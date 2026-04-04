from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from signals.core.derived_db import (
    get_connection,
    init_db,
    insert_normalized,
    insert_resolution_event,
    insert_run,
    insert_signal_result,
    update_run_status,
)
from signals.core.dto import NormalizedTransaction, SignalResult
from signals.core.enums import ReasonCode
from signals.core.git import git_sha
from signals.core.resolution import resolve_entity
from signals.core.runs import make_run, utcnow_iso
from signals.core.versioning import INSIDER_SCORE_METHOD_VERSION, NORMALIZATION_METHOD_VERSION, RESOLUTION_METHOD_VERSION
from signals.insider.engine import (
    ANALYSIS_WINDOWS_DAYS,
    aggregate_company_signal,
    classify_role,
    classify_transaction_type,
    compute_pct_holdings_changed,
    detect_planned_trade,
    score_transaction,
)
from signals.insider.parser import parse_form4_xml


@dataclass
class DirectInsiderRunResult:
    run_id: str
    xml_count: int
    imported_normalized_count: int
    imported_result_count: int
    xml_dir: str

    def to_dict(self) -> dict:
        return asdict(self)


def _subject_key(ticker: str | None, cik: str | None) -> str:
    return f"entity:{ticker.lower()}" if ticker else f"cik:{cik}"


def _fingerprint(source_record_ids: list[str], method_version: str, as_of_date: str, lookback_window: int) -> str:
    basis = "|".join(sorted(source_record_ids) + [method_version, as_of_date, str(lookback_window)])
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def run_direct_xml_into_derived(repo_root: Path, derived_db_path: str, xml_dir: str, reference_date: datetime) -> DirectInsiderRunResult:
    init_db(derived_db_path)
    xml_root = Path(xml_dir)
    xml_files = sorted(xml_root.rglob("*.xml"))
    code_version = git_sha(repo_root)
    run = make_run(
        "direct_xml_score",
        "insider",
        code_version,
        {"xml_dir": str(xml_root), "reference_date": reference_date.strftime("%Y-%m-%d")},
        {"normalization": NORMALIZATION_METHOD_VERSION, "resolution": RESOLUTION_METHOD_VERSION, "score": INSIDER_SCORE_METHOD_VERSION},
    )
    with get_connection(derived_db_path) as conn:
        insert_run(conn, run)

    normalized_rows: list[NormalizedTransaction] = []
    resolution_events: dict[str, object] = {}
    scored_transactions_by_entity: dict[str, list[dict]] = defaultdict(list)
    record_ids_by_entity: dict[str, list[str]] = defaultdict(list)
    all_entities: set[str] = set()
    result_rows: list[tuple[SignalResult, str]] = []

    try:
        for xml_file in xml_files:
            parsed = parse_form4_xml(xml_file)
            filing = parsed.get("filing") or {}
            if parsed.get("parse_error") or not filing:
                continue
            filing_id = f"{filing.get('cik_issuer')}:{filing.get('cik_owner')}:{filing.get('period_of_report')}:{xml_file.stem}"
            for idx, txn in enumerate(parsed.get("transactions", []), start=1):
                source_record_id = f"{filing_id}:{idx}"
                resolution_event = resolve_entity(
                    source="insider",
                    source_record_id=source_record_id,
                    source_filing_id=filing_id,
                    ticker=filing.get("ticker_issuer"),
                    cik=filing.get("cik_issuer"),
                    issuer_name=filing.get("issuer_name"),
                    instrument_type=txn.get("security_title"),
                    run_id=run.run_id,
                )
                resolution_events[source_record_id] = resolution_event
                role_class, exclusion = classify_role(
                    filing.get("officer_title"),
                    filing.get("owner_name"),
                    bool(filing.get("is_officer")),
                    bool(filing.get("is_director")),
                    bool(filing.get("is_ten_pct_owner")),
                    bool(filing.get("is_other")),
                )
                include = exclusion is None and txn.get("transaction_code") in {"P", "S"}
                normalized = NormalizedTransaction(
                    source="insider",
                    source_record_id=source_record_id,
                    source_filing_id=filing_id,
                    actor_id=filing.get("cik_owner"),
                    actor_name=filing.get("owner_name"),
                    actor_type=role_class,
                    owner_type="direct" if txn.get("ownership_nature") == "D" else "indirect",
                    entity_key=resolution_event.entity_key or _subject_key(resolution_event.ticker, filing.get("cik_issuer")),
                    instrument_key=resolution_event.instrument_key,
                    ticker=resolution_event.ticker,
                    issuer_name=resolution_event.issuer_name or filing.get("issuer_name"),
                    instrument_type=txn.get("security_title"),
                    transaction_type=classify_transaction_type(txn.get("transaction_code")),
                    direction="BUY" if txn.get("transaction_code") == "P" else "SELL" if txn.get("transaction_code") == "S" else "NEUTRAL",
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
                    resolution_event_id=resolution_event.event_id,
                    resolution_confidence=resolution_event.resolution_confidence,
                    resolution_method_version=RESOLUTION_METHOD_VERSION,
                    include_in_signal=include,
                    exclusion_reason_code=None if include else ReasonCode.ENTITY_ROLE_EXCLUDED.value,
                    exclusion_reason_detail=exclusion,
                    provenance_payload={
                        "source_system": "direct-insider-xml",
                        "raw_record_id": source_record_id,
                        "raw_filing_id": filing_id,
                        "xml_path": str(xml_file),
                        "resolver_evidence": resolution_event.evidence_payload,
                        "method_versions": {
                            "normalization": NORMALIZATION_METHOD_VERSION,
                            "resolution": RESOLUTION_METHOD_VERSION,
                            "score": INSIDER_SCORE_METHOD_VERSION,
                        },
                    },
                    normalization_method_version=NORMALIZATION_METHOD_VERSION,
                    run_id=run.run_id,
                )
                normalized_rows.append(normalized)
                subject_key = _subject_key(normalized.ticker, filing.get("cik_issuer"))
                all_entities.add(subject_key)
                if include:
                    scored_txn = {
                        "transaction_code": txn.get("transaction_code"),
                        "role_class": role_class,
                        "is_likely_planned": 1 if detect_planned_trade(txn.get("footnotes")) else 0,
                        "ownership_nature": txn.get("ownership_nature"),
                        "pct_holdings_changed": compute_pct_holdings_changed(txn.get("shares"), txn.get("shares_after")),
                        "transaction_date": txn.get("transaction_date"),
                        "cik_owner": filing.get("cik_owner"),
                        "total_value": txn.get("total_value"),
                        "accession_number": filing_id,
                    }
                    scored_txn.update(score_transaction(scored_txn, reference_date))
                    scored_transactions_by_entity[subject_key].append(scored_txn)
                    record_ids_by_entity[subject_key].append(source_record_id)

        for subject_key in sorted(all_entities):
            scored_transactions = scored_transactions_by_entity.get(subject_key, [])
            txns_by_date = [txn for txn in scored_transactions if txn.get("transaction_date")]
            for window in ANALYSIS_WINDOWS_DAYS:
                window_txns = []
                since = (reference_date.date()).toordinal() - window
                for txn in txns_by_date:
                    try:
                        txn_dt = datetime.strptime(txn["transaction_date"][:10], "%Y-%m-%d")
                    except (ValueError, TypeError):
                        continue
                    if txn_dt.date().toordinal() >= since:
                        window_txns.append(txn)
                aggregate = aggregate_company_signal(window_txns, window)
                ids = []
                source_ids = record_ids_by_entity.get(subject_key, [])
                for rid, txn in zip(source_ids, scored_transactions, strict=False):
                    if txn in window_txns:
                        ids.append(rid)
                signal = SignalResult(
                    source="insider",
                    scope="entity",
                    subject_key=subject_key,
                    score=float(aggregate["score"]),
                    label=aggregate["signal"],
                    confidence=float(aggregate["confidence"]),
                    as_of_date=reference_date.strftime("%Y-%m-%d"),
                    lookback_window=window,
                    input_count=len(window_txns),
                    included_count=aggregate["buy_count"] + aggregate["sell_count"],
                    excluded_count=0,
                    explanation=aggregate["explanation"],
                    method_version=INSIDER_SCORE_METHOD_VERSION,
                    code_version=code_version,
                    run_id=run.run_id,
                    provenance_refs={
                        "normalized_row_ids": ids,
                        "resolution_event_ids": [normalized.resolution_event_id for normalized in normalized_rows if normalized.source_record_id in ids and normalized.resolution_event_id],
                        "path": "direct_xml",
                    },
                )
                result_rows.append((signal, _fingerprint(ids, INSIDER_SCORE_METHOD_VERSION, signal.as_of_date, signal.lookback_window)))

        with get_connection(derived_db_path) as conn:
            for row in normalized_rows:
                if row.resolution_event_id:
                    insert_resolution_event(conn, resolution_events[row.source_record_id])
                insert_normalized(conn, row)
            for signal, fingerprint in result_rows:
                insert_signal_result(conn, signal, fingerprint)
            update_run_status(conn, run.run_id, "SUCCEEDED", utcnow_iso(), {"normalized_count": len(normalized_rows), "score_count": len(result_rows), "xml_count": len(xml_files)})
    except Exception as exc:
        with get_connection(derived_db_path) as conn:
            update_run_status(
                conn,
                run.run_id,
                "FAILED",
                utcnow_iso(),
                {"normalized_count": len(normalized_rows), "score_count": len(result_rows), "xml_count": len(xml_files), "error": str(exc)},
            )
        raise

    return DirectInsiderRunResult(
        run_id=run.run_id,
        xml_count=len(xml_files),
        imported_normalized_count=len(normalized_rows),
        imported_result_count=len(result_rows),
        xml_dir=str(xml_root),
    )
