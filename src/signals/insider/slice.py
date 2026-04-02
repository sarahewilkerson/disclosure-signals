from __future__ import annotations

import hashlib
from datetime import datetime

from signals.core.dto import NormalizedTransaction, SignalResult
from signals.core.enums import ReasonCode
from signals.core.versioning import (
    INSIDER_SCORE_METHOD_VERSION,
    NORMALIZATION_METHOD_VERSION,
    RESOLUTION_METHOD_VERSION,
)
from signals.insider.legacy_bridge import (
    classify_role,
    classify_transaction_type,
    compute_pct_holdings_changed,
    detect_planned_trade,
    parse_form4_xml,
    resolve_issuer,
    score_transaction,
)


def run_insider_vertical_slice(xml_path: str, run_id: str) -> tuple[list[NormalizedTransaction], list[SignalResult]]:
    parsed = parse_form4_xml(xml_path)
    if parsed["parse_error"]:
        raise ValueError(parsed["parse_error"])

    filing = parsed["filing"]
    filing_id = f"{filing['cik_issuer']}:{filing['cik_owner']}:{filing['period_of_report']}"
    entity_key, ticker, issuer_name = resolve_issuer(filing["cik_issuer"])
    normalized_rows: list[NormalizedTransaction] = []
    signal_rows: list[SignalResult] = []
    scored_values: list[float] = []

    for idx, txn in enumerate(parsed["transactions"], start=1):
        role_class, exclusion = classify_role(
            filing.get("officer_title"),
            filing.get("owner_name"),
            bool(filing.get("is_officer")),
            bool(filing.get("is_director")),
            bool(filing.get("is_ten_pct_owner")),
            bool(filing.get("is_other")),
        )
        transaction_class = classify_transaction_type(txn.get("transaction_code"))
        planned = detect_planned_trade(txn.get("footnotes"))
        pct_changed = compute_pct_holdings_changed(txn.get("shares"), txn.get("shares_after"))
        include = exclusion is None and txn.get("transaction_code") in ("P", "S")
        reason_code = None if include else ReasonCode.ENTITY_ROLE_EXCLUDED.value
        detail = exclusion

        source_record_id = f"{filing_id}:{idx}"
        normalized = NormalizedTransaction(
            source="insider",
            source_record_id=source_record_id,
            source_filing_id=filing_id,
            actor_id=filing.get("cik_owner"),
            actor_name=filing.get("owner_name"),
            actor_type=role_class,
            owner_type="self",
            entity_key=entity_key,
            instrument_key=ticker,
            ticker=ticker,
            issuer_name=issuer_name,
            instrument_type=txn.get("security_title"),
            transaction_type=transaction_class,
            direction="BUY" if txn.get("transaction_code") == "P" else "SELL",
            execution_date=txn.get("transaction_date"),
            disclosure_date=filing.get("period_of_report"),
            amount_low=txn.get("total_value"),
            amount_high=txn.get("total_value"),
            amount_estimate=txn.get("total_value"),
            currency="USD",
            units_low=txn.get("shares"),
            units_high=txn.get("shares"),
            price_low=txn.get("price_per_share"),
            price_high=txn.get("price_per_share"),
            quality_score=1.0,
            parse_confidence=1.0,
            resolution_event_id=None,
            resolution_confidence=1.0 if ticker else 0.0,
            resolution_method_version=RESOLUTION_METHOD_VERSION,
            include_in_signal=include,
            exclusion_reason_code=reason_code,
            exclusion_reason_detail=detail,
            provenance_payload={
                "source_system": "legacy-insider",
                "raw_record_id": source_record_id,
                "raw_filing_id": filing_id,
                "source_values": {
                    "transaction_code": txn.get("transaction_code"),
                    "officer_title": filing.get("officer_title"),
                },
                "method_versions": {
                    "normalization": NORMALIZATION_METHOD_VERSION,
                    "resolution": RESOLUTION_METHOD_VERSION,
                },
            },
            normalization_method_version=NORMALIZATION_METHOD_VERSION,
            run_id=run_id,
        )
        normalized_rows.append(normalized)

        if include:
            score_detail = score_transaction(
                {
                    "transaction_code": txn.get("transaction_code"),
                    "role_class": role_class,
                    "is_likely_planned": 1 if planned else 0,
                    "ownership_nature": txn.get("ownership_nature"),
                    "pct_holdings_changed": pct_changed,
                    "transaction_date": txn.get("transaction_date"),
                },
                datetime.strptime(txn["transaction_date"], "%Y-%m-%d"),
            )
            scored_values.append(score_detail["transaction_signal"])

    if scored_values:
        total_score = float(sum(scored_values))
        label = "bullish" if total_score > 0 else "bearish" if total_score < 0 else "neutral"
        signal_rows.append(
            SignalResult(
                source="insider",
                scope="entity",
                subject_key=entity_key or filing["cik_issuer"],
                score=total_score,
                label=label,
                confidence=1.0,
                as_of_date=normalized_rows[0].execution_date or "",
                lookback_window=90,
                input_count=len(normalized_rows),
                included_count=len(scored_values),
                excluded_count=len(normalized_rows) - len(scored_values),
                explanation=f"{len(scored_values)} qualifying insider transaction(s) for {ticker or filing['cik_issuer']}",
                method_version=INSIDER_SCORE_METHOD_VERSION,
                code_version="workspace",
                run_id=run_id,
                provenance_refs={
                    "normalized_row_ids": [row.source_record_id for row in normalized_rows],
                    "resolution_event_ids": [],
                    "input_fingerprint_basis": "source_record_ids",
                },
            )
        )

    return normalized_rows, signal_rows


def fingerprint_for_rows(rows: list[NormalizedTransaction]) -> str:
    basis = "|".join(sorted(row.source_record_id for row in rows))
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()
