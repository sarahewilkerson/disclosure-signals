from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass, asdict
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
from signals.core.versioning import (
    INSIDER_SCORE_METHOD_VERSION,
    NORMALIZATION_METHOD_VERSION,
    RESOLUTION_METHOD_VERSION,
)
from signals.insider.legacy_bridge import (
    db_module as legacy_db_module,
    reporting_service_module as legacy_reporting_service_module,
    scoring_service_module as legacy_scoring_service_module,
    status_service_module as legacy_status_service_module,
)
def _normalize_direction(transaction_code: str | None) -> str:
    if transaction_code == "P":
        return "BUY"
    if transaction_code == "S":
        return "SELL"
    return "NEUTRAL"


def _subject_key(ticker: str | None, cik: str) -> str:
    return f"entity:{ticker.lower()}" if ticker else f"cik:{cik}"


def _fingerprint(source_record_ids: list[str], method_version: str, as_of_date: str, lookback_window: int) -> str:
    basis = "|".join(sorted(source_record_ids) + [method_version, as_of_date, str(lookback_window)])
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


@dataclass
class InsiderDerivedRunResult:
    run_id: str
    reference_date: str
    imported_normalized_count: int
    imported_result_count: int
    legacy_status: dict
    parity: dict
    artifact_paths: dict[str, str]

    def to_dict(self) -> dict:
        return asdict(self)


def run_legacy_score_into_derived(
    repo_root: Path,
    derived_db_path: str,
    legacy_db_path: str,
    reference_date: datetime,
) -> InsiderDerivedRunResult:
    init_db(derived_db_path)
    legacy_db = legacy_db_module()
    legacy_db.init_db(legacy_db_path)
    legacy_scoring = legacy_scoring_service_module()
    legacy_status = legacy_status_service_module()
    legacy_scoring.compute_scores(reference_date=reference_date, db_path=legacy_db_path)

    code_version = git_sha(repo_root)
    run = make_run(
        "score_import",
        "insider",
        code_version,
        {"reference_date": reference_date.strftime("%Y-%m-%d"), "legacy_db_path": legacy_db_path},
        {
            "normalization": NORMALIZATION_METHOD_VERSION,
            "resolution": RESOLUTION_METHOD_VERSION,
            "score": INSIDER_SCORE_METHOD_VERSION,
        },
    )

    with legacy_db.get_connection(legacy_db_path) as legacy_conn:
        normalized_rows = []
        resolution_events = {}
        raw_rows = legacy_conn.execute(
            """
            SELECT
                t.id,
                t.accession_number,
                t.cik_issuer,
                t.cik_owner,
                t.owner_name,
                t.security_title,
                t.transaction_date,
                t.transaction_code,
                t.shares,
                t.price_per_share,
                t.total_value,
                t.ownership_nature,
                t.role_class,
                t.include_in_signal,
                t.exclusion_reason,
                f.filing_date,
                c.ticker,
                c.company_name
            FROM transactions t
            JOIN filings f ON f.accession_number = t.accession_number
            LEFT JOIN companies c ON c.cik = t.cik_issuer
            ORDER BY t.id
            """
        ).fetchall()
        for row in raw_rows:
            source_record_id = f"insider-txn:{row['id']}"
            resolution_event = resolve_entity(
                source="insider",
                source_record_id=source_record_id,
                source_filing_id=row["accession_number"],
                ticker=row["ticker"],
                cik=row["cik_issuer"],
                issuer_name=row["company_name"],
                instrument_type=row["security_title"],
                run_id=run.run_id,
            )
            resolution_events[source_record_id] = resolution_event
            normalized_rows.append(
                NormalizedTransaction(
                    source="insider",
                    source_record_id=source_record_id,
                    source_filing_id=row["accession_number"],
                    actor_id=row["cik_owner"],
                    actor_name=row["owner_name"],
                    actor_type=row["role_class"] or "unknown",
                    owner_type="direct" if row["ownership_nature"] == "D" else "indirect",
                    entity_key=resolution_event.entity_key or _subject_key(row["ticker"], row["cik_issuer"]),
                    instrument_key=resolution_event.instrument_key,
                    ticker=resolution_event.ticker,
                    issuer_name=resolution_event.issuer_name,
                    instrument_type=row["security_title"],
                    transaction_type=row["transaction_code"] or "unknown",
                    direction=_normalize_direction(row["transaction_code"]),
                    execution_date=row["transaction_date"],
                    disclosure_date=row["filing_date"],
                    amount_low=float(row["total_value"]) if row["total_value"] is not None else None,
                    amount_high=float(row["total_value"]) if row["total_value"] is not None else None,
                    amount_estimate=float(row["total_value"]) if row["total_value"] is not None else None,
                    currency="USD",
                    units_low=float(row["shares"]) if row["shares"] is not None else None,
                    units_high=float(row["shares"]) if row["shares"] is not None else None,
                    price_low=float(row["price_per_share"]) if row["price_per_share"] is not None else None,
                    price_high=float(row["price_per_share"]) if row["price_per_share"] is not None else None,
                    quality_score=1.0,
                    parse_confidence=1.0,
                    resolution_event_id=resolution_event.event_id,
                    resolution_confidence=resolution_event.resolution_confidence,
                    resolution_method_version=RESOLUTION_METHOD_VERSION,
                    include_in_signal=bool(row["include_in_signal"]),
                    exclusion_reason_code=None if row["include_in_signal"] else (ReasonCode.ENTITY_ROLE_EXCLUDED.value if row["exclusion_reason"] else ReasonCode.UNSUPPORTED_TRANSACTION_TYPE.value),
                    exclusion_reason_detail=row["exclusion_reason"],
                    provenance_payload={
                        "source_system": "legacy-insider",
                        "raw_record_id": row["id"],
                        "raw_filing_id": row["accession_number"],
                        "stage_timestamps": {"imported_at": utcnow_iso()},
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
            )

        score_rows = legacy_conn.execute(
            "SELECT * FROM company_scores ORDER BY window_days, ticker"
        ).fetchall()
        signal_txn_count = int(
            legacy_conn.execute(
                "SELECT COUNT(*) AS c FROM transactions WHERE include_in_signal = 1"
            ).fetchone()["c"]
        )

    result_rows: list[tuple[SignalResult, str]] = []
    grouped_record_ids: dict[tuple[str, int], list[str]] = defaultdict(list)
    for row in normalized_rows:
        if row.include_in_signal:
            subject_key = _subject_key(row.ticker, row.source_filing_id)
            for window in (30, 90, 180):
                grouped_record_ids[(subject_key, window)].append(row.source_record_id)

    for row in score_rows:
        subject_key = _subject_key(row["ticker"], row["cik"])
        ids = grouped_record_ids.get((subject_key, row["window_days"]), [])
        signal = SignalResult(
            source="insider",
            scope="entity",
            subject_key=subject_key,
            score=float(row["score"]),
            label=row["signal"],
            confidence=float(row["confidence"]),
            as_of_date=reference_date.strftime("%Y-%m-%d"),
            lookback_window=int(row["window_days"]),
            input_count=int(row["buy_count"]) + int(row["sell_count"]),
            included_count=int(row["buy_count"]) + int(row["sell_count"]),
            excluded_count=0,
            explanation=row["explanation"],
            method_version=INSIDER_SCORE_METHOD_VERSION,
            code_version=code_version,
            run_id=run.run_id,
            provenance_refs={
                "legacy_table": "company_scores",
                "legacy_subject": {"cik": row["cik"], "ticker": row["ticker"]},
                "legacy_computed_at": row["computed_at"],
                "normalized_row_ids": ids,
                "resolution_event_ids": [
                    item.resolution_event_id
                    for item in normalized_rows
                    if item.source_record_id in ids and item.resolution_event_id
                ],
            },
        )
        result_rows.append(
            (
                signal,
                _fingerprint(ids, INSIDER_SCORE_METHOD_VERSION, signal.as_of_date, signal.lookback_window),
            )
        )

    with get_connection(derived_db_path) as conn:
        insert_run(conn, run)
        for row in normalized_rows:
            if row.resolution_event_id:
                insert_resolution_event(conn, resolution_events[row.source_record_id])
            insert_normalized(conn, row)
        for signal, fingerprint in result_rows:
            insert_signal_result(conn, signal, fingerprint)
        update_run_status(
            conn,
            run.run_id,
            "SUCCEEDED",
            utcnow_iso(),
            {
                "normalized_count": len(normalized_rows),
                "score_count": len(result_rows),
            },
        )

    status = legacy_status.get_status(db_path=legacy_db_path)
    parity = {
        "legacy_signal_transactions": signal_txn_count,
        "imported_normalized": len(normalized_rows),
        "legacy_company_scores": len(score_rows),
        "imported_results": len(result_rows),
        "normalized_match": signal_txn_count == sum(1 for row in normalized_rows if row.include_in_signal),
        "result_match": len(score_rows) == len(result_rows),
    }
    return InsiderDerivedRunResult(
        run_id=run.run_id,
        reference_date=reference_date.strftime("%Y-%m-%d"),
        imported_normalized_count=len(normalized_rows),
        imported_result_count=len(result_rows),
        legacy_status=asdict(status),
        parity=parity,
        artifact_paths={},
    )


def get_legacy_status(legacy_db_path: str) -> dict:
    status = legacy_status_service_module().get_status(db_path=legacy_db_path)
    return asdict(status)


def run_legacy_report(legacy_db_path: str, output_dir: str | None = None) -> dict:
    result = legacy_reporting_service_module().generate_reports(
        db_path=legacy_db_path,
        output_dir=output_dir,
    )
    return asdict(result)
