from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

from signals.congress.legacy_bridge import entity_resolver_class, score_transaction, senate_connector_class
from signals.core.dto import NormalizedTransaction, SignalResult
from signals.core.enums import ReasonCode
from signals.core.versioning import (
    CONGRESS_SCORE_METHOD_VERSION,
    NORMALIZATION_METHOD_VERSION,
    RESOLUTION_METHOD_VERSION,
)


def _owner_type(owner: str) -> str:
    owner_lower = owner.lower()
    if "spouse" in owner_lower:
        return "spouse"
    return "self"


def _direction(txn_type: str) -> str:
    txn_lower = txn_type.lower()
    if "purchase" in txn_lower:
        return "BUY"
    if "sale" in txn_lower:
        return "SELL"
    return "NEUTRAL"


def run_congress_vertical_slice(html_path: str, run_id: str) -> tuple[list[NormalizedTransaction], list[SignalResult]]:
    connector = senate_connector_class()(cache_dir=Path(html_path).parent.parent, request_delay=0)
    resolver = entity_resolver_class()()
    parsed = connector.parse_ptr_transactions(Path(html_path))
    normalized_rows: list[NormalizedTransaction] = []
    signal_rows: list[SignalResult] = []

    for idx, txn in enumerate(parsed, start=1):
        resolved = resolver.resolve(txn.asset_name, ticker=txn.ticker)
        include = bool(resolved.include_in_signal)
        source_record_id = f"{Path(html_path).stem}:{idx}"
        normalized = NormalizedTransaction(
            source="congress",
            source_record_id=source_record_id,
            source_filing_id=Path(html_path).stem,
            actor_id=None,
            actor_name=txn.owner,
            actor_type="member",
            owner_type=_owner_type(txn.owner),
            entity_key=f"entity:{(resolved.resolved_ticker or '').lower()}" if resolved.resolved_ticker else None,
            instrument_key=resolved.resolved_ticker,
            ticker=resolved.resolved_ticker,
            issuer_name=resolved.resolved_company or txn.asset_name,
            instrument_type=txn.asset_type,
            transaction_type=txn.transaction_type,
            direction=_direction(txn.transaction_type),
            execution_date=txn.transaction_date.strftime("%Y-%m-%d") if txn.transaction_date else None,
            disclosure_date=txn.transaction_date.strftime("%Y-%m-%d") if txn.transaction_date else None,
            amount_low=float(txn.amount_min) if hasattr(txn, "amount_min") and txn.amount_min is not None else None,
            amount_high=float(txn.amount_max) if hasattr(txn, "amount_max") and txn.amount_max is not None else None,
            amount_estimate=None,
            currency="USD",
            units_low=None,
            units_high=None,
            price_low=None,
            price_high=None,
            quality_score=1.0,
            parse_confidence=1.0,
            resolution_event_id=None,
            resolution_confidence=resolved.resolution_confidence,
            resolution_method_version=RESOLUTION_METHOD_VERSION,
            include_in_signal=include,
            exclusion_reason_code=resolved.exclusion_reason or (ReasonCode.MISSING_TICKER.value if not resolved.resolved_ticker else None),
            exclusion_reason_detail=resolved.exclusion_reason,
            provenance_payload={
                "source_system": "legacy-congress",
                "raw_record_id": source_record_id,
                "raw_filing_id": Path(html_path).stem,
                "source_values": {"owner": txn.owner, "asset_name": txn.asset_name},
                "method_versions": {
                    "normalization": NORMALIZATION_METHOD_VERSION,
                    "resolution": RESOLUTION_METHOD_VERSION,
                },
            },
            normalization_method_version=NORMALIZATION_METHOD_VERSION,
            run_id=run_id,
        )
        # Parse amount range through scoring engine later; normalize here for derived storage.
        if txn.amount_range == "$1,001 - $15,000":
            normalized.amount_low = 1001.0
            normalized.amount_high = 15000.0
            normalized.amount_estimate = (1001.0 + 15000.0) / 2
        elif txn.amount_range == "$15,001 - $50,000":
            normalized.amount_low = 15001.0
            normalized.amount_high = 50000.0
            normalized.amount_estimate = (15001.0 + 50000.0) / 2
        normalized_rows.append(normalized)

        if include and normalized.amount_low is not None and normalized.amount_high is not None:
            tx_score = score_transaction(
                member_id=source_record_id,
                ticker=normalized.ticker,
                transaction_type=normalized.transaction_type.lower().replace(" ", "_").replace("(partial)", "partial").replace("__", "_"),
                execution_date=datetime.strptime(normalized.execution_date, "%Y-%m-%d") if normalized.execution_date else None,
                amount_min=int(normalized.amount_low),
                amount_max=int(normalized.amount_high),
                owner_type=normalized.owner_type,
                resolution_confidence=normalized.resolution_confidence or 0.0,
                signal_weight=1.0,
                reference_date=datetime.strptime(normalized.execution_date, "%Y-%m-%d") if normalized.execution_date else datetime.utcnow(),
            )
            signal_rows.append(
                SignalResult(
                    source="congress",
                    scope="entity",
                    subject_key=normalized.entity_key or f"unresolved:{source_record_id}",
                    score=float(tx_score.final_score),
                    label="bullish" if tx_score.final_score > 0 else "bearish" if tx_score.final_score < 0 else "neutral",
                    confidence=max(normalized.resolution_confidence or 0.0, 0.95 if normalized.ticker else 0.0),
                    as_of_date=normalized.execution_date or "",
                    lookback_window=90,
                    input_count=1,
                    included_count=1,
                    excluded_count=0,
                    explanation=f"1 qualifying congress transaction for {normalized.ticker or normalized.issuer_name}",
                    method_version=CONGRESS_SCORE_METHOD_VERSION,
                    code_version="workspace",
                    run_id=run_id,
                    provenance_refs={
                        "normalized_row_ids": [source_record_id],
                        "resolution_event_ids": [],
                        "input_fingerprint_basis": "source_record_ids",
                    },
                )
            )

    return normalized_rows, signal_rows


def fingerprint_for_rows(rows: list[NormalizedTransaction]) -> str:
    basis = "|".join(sorted(row.source_record_id for row in rows))
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()
