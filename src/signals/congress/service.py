from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path

from signals.congress.legacy_bridge import (
    compute_aggregate,
    compute_confidence_score,
    db_module as legacy_db_module,
    reporting_service_module as legacy_reporting_service_module,
    score_transaction,
    scoring_service_module as legacy_scoring_service_module,
    status_service_module as legacy_status_service_module,
)
from signals.congress.engine import compute_entity_signal
from signals.core.derived_db import (
    get_connection,
    init_db,
    insert_resolution_event,
    insert_normalized,
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
    CONGRESS_SCORE_METHOD_VERSION,
    NORMALIZATION_METHOD_VERSION,
    RESOLUTION_METHOD_VERSION,
)


def _subject_key(ticker: str | None, asset_name: str, row_id: int) -> str:
    if ticker:
        return f"entity:{ticker.lower()}"
    return f"unresolved:{row_id}:{asset_name.lower()}"


def _direction(transaction_type: str) -> str:
    lower = transaction_type.lower()
    if "purchase" in lower:
        return "BUY"
    if "sale" in lower:
        return "SELL"
    return "NEUTRAL"


def _fingerprint(source_record_ids: list[str], method_version: str, as_of_date: str, lookback_window: int) -> str:
    basis = "|".join(sorted(source_record_ids) + [method_version, as_of_date, str(lookback_window)])
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


@dataclass
class CongressDerivedRunResult:
    run_id: str
    reference_date: str
    window_days: int
    imported_normalized_count: int
    imported_result_count: int
    legacy_status: dict
    parity: dict

    def to_dict(self) -> dict:
        return asdict(self)


def run_legacy_score_into_derived(
    repo_root: Path,
    derived_db_path: str,
    legacy_db_path: str,
    window_days: int,
    reference_date: datetime | None = None,
) -> CongressDerivedRunResult:
    init_db(derived_db_path)
    reference_date = reference_date or datetime.now()
    legacy_db = legacy_db_module()
    legacy_db.init_db(legacy_db_path)
    legacy_scoring = legacy_scoring_service_module()
    legacy_status = legacy_status_service_module()
    legacy_scoring.compute_and_store_score(window=window_days, db_path=legacy_db_path)

    code_version = git_sha(repo_root)
    run = make_run(
        "score_import",
        "congress",
        code_version,
        {
            "reference_date": reference_date.strftime("%Y-%m-%d"),
            "legacy_db_path": legacy_db_path,
            "window_days": window_days,
        },
        {
            "normalization": NORMALIZATION_METHOD_VERSION,
            "resolution": RESOLUTION_METHOD_VERSION,
            "score": CONGRESS_SCORE_METHOD_VERSION,
        },
    )

    cutoff_date = (reference_date - timedelta(days=window_days)).strftime("%Y-%m-%d")
    normalized_rows: list[NormalizedTransaction] = []
    scored_by_subject: dict[str, list] = defaultdict(list)
    record_ids_by_subject: dict[str, list[str]] = defaultdict(list)

    with legacy_db.get_connection(legacy_db_path) as legacy_conn:
        legacy_window_count = int(
            legacy_conn.execute(
                "SELECT COUNT(*) AS c FROM transactions WHERE execution_date >= ?",
                (cutoff_date,),
            ).fetchone()["c"]
        )
        legacy_signal_count = int(
            legacy_conn.execute(
                "SELECT COUNT(*) AS c FROM transactions WHERE execution_date >= ? AND include_in_signal = 1 AND resolved_ticker IS NOT NULL",
                (cutoff_date,),
            ).fetchone()["c"]
        )
        rows = legacy_conn.execute(
            """
            SELECT
                t.id,
                t.filing_id,
                t.bioguide_id,
                t.owner_type,
                t.asset_name_raw,
                t.asset_type,
                t.resolved_ticker,
                t.resolved_company,
                t.resolution_method,
                t.resolution_confidence,
                t.transaction_type,
                t.execution_date,
                t.disclosure_date,
                t.amount_min,
                t.amount_max,
                t.include_in_signal,
                t.exclusion_reason,
                t.extraction_confidence,
                f.filer_name
            FROM transactions t
            JOIN filings f ON f.filing_id = t.filing_id
            WHERE t.execution_date >= ?
            ORDER BY t.id
            """,
            (cutoff_date,),
        ).fetchall()

        resolution_events = {}
        for row in rows:
            subject_key = _subject_key(row["resolved_ticker"], row["asset_name_raw"], row["id"])
            source_record_id = f"congress-txn:{row['id']}"
            resolution_event = resolve_entity(
                source="congress",
                source_record_id=source_record_id,
                source_filing_id=row["filing_id"],
                ticker=row["resolved_ticker"],
                cik=None,
                issuer_name=row["resolved_company"] or row["asset_name_raw"],
                instrument_type=row["asset_type"],
                run_id=run.run_id,
            )
            resolution_events[source_record_id] = resolution_event
            normalized = NormalizedTransaction(
                source="congress",
                source_record_id=source_record_id,
                source_filing_id=row["filing_id"],
                actor_id=row["bioguide_id"],
                actor_name=row["filer_name"],
                actor_type="member",
                owner_type=row["owner_type"],
                entity_key=resolution_event.entity_key or (subject_key if row["resolved_ticker"] else None),
                instrument_key=resolution_event.instrument_key,
                ticker=resolution_event.ticker,
                issuer_name=resolution_event.issuer_name,
                instrument_type=row["asset_type"],
                transaction_type=row["transaction_type"],
                direction=_direction(row["transaction_type"]),
                execution_date=row["execution_date"],
                disclosure_date=row["disclosure_date"],
                amount_low=float(row["amount_min"]) if row["amount_min"] is not None else None,
                amount_high=float(row["amount_max"]) if row["amount_max"] is not None else None,
                amount_estimate=((float(row["amount_min"]) + float(row["amount_max"])) / 2.0) if row["amount_min"] is not None and row["amount_max"] is not None else None,
                currency="USD",
                units_low=None,
                units_high=None,
                price_low=None,
                price_high=None,
                quality_score=float(row["extraction_confidence"]) if row["extraction_confidence"] is not None else 1.0,
                parse_confidence=float(row["extraction_confidence"]) if row["extraction_confidence"] is not None else 1.0,
                resolution_event_id=resolution_event.event_id,
                resolution_confidence=resolution_event.resolution_confidence,
                resolution_method_version=RESOLUTION_METHOD_VERSION,
                include_in_signal=bool(row["include_in_signal"]),
                exclusion_reason_code=None if row["include_in_signal"] else (ReasonCode.NON_SIGNAL_ASSET.value if row["exclusion_reason"] else ReasonCode.MISSING_TICKER.value),
                exclusion_reason_detail=row["exclusion_reason"],
                provenance_payload={
                    "source_system": "legacy-congress",
                    "raw_record_id": row["id"],
                    "raw_filing_id": row["filing_id"],
                    "stage_timestamps": {"imported_at": utcnow_iso()},
                    "resolver_evidence": resolution_event.evidence_payload,
                    "method_versions": {
                        "normalization": NORMALIZATION_METHOD_VERSION,
                        "resolution": RESOLUTION_METHOD_VERSION,
                        "score": CONGRESS_SCORE_METHOD_VERSION,
                    },
                },
                normalization_method_version=NORMALIZATION_METHOD_VERSION,
                run_id=run.run_id,
            )
            normalized_rows.append(normalized)

            if not normalized.include_in_signal or not normalized.ticker:
                continue

            scored = score_transaction(
                member_id=row["bioguide_id"] or row["filer_name"],
                ticker=row["resolved_ticker"],
                transaction_type=row["transaction_type"],
                execution_date=datetime.strptime(row["execution_date"], "%Y-%m-%d") if row["execution_date"] else None,
                amount_min=row["amount_min"],
                amount_max=row["amount_max"],
                owner_type=row["owner_type"] or "self",
                resolution_confidence=float(row["resolution_confidence"]) if row["resolution_confidence"] is not None else 0.0,
                signal_weight=1.0,
                reference_date=reference_date,
            )
            scored_by_subject[subject_key].append(scored)
            record_ids_by_subject[subject_key].append(source_record_id)

    results: list[tuple[SignalResult, str]] = []
    for subject_key, scored_transactions in scored_by_subject.items():
        aggregate = compute_aggregate(scored_transactions)
        total = aggregate.volume_buy + aggregate.volume_sell
        resolution_rate = 1.0 if aggregate.transactions_included else 0.0
        confidence = compute_confidence_score(aggregate, resolution_rate)["composite_score"]
        net_score = aggregate.volume_net / total if total else 0.0
        ids = record_ids_by_subject[subject_key]
        signal = compute_entity_signal(
            subject_key=subject_key,
            score=float(net_score),
            confidence=float(confidence),
            as_of_date=reference_date.strftime("%Y-%m-%d"),
            lookback_window=window_days,
            input_count=len(ids),
            included_count=aggregate.transactions_included,
            excluded_count=aggregate.transactions_excluded,
            explanation=f"{aggregate.transactions_included} qualifying congress transaction(s) across {aggregate.unique_members} member(s)",
            method_version=CONGRESS_SCORE_METHOD_VERSION,
            code_version=code_version,
            run_id=run.run_id,
            provenance_refs={
                "normalized_row_ids": ids,
                "resolution_event_ids": [
                    resolution_events[item].event_id
                    for item in ids
                    if item in resolution_events
                ],
                "aggregate": {
                    "buyers": aggregate.buyers,
                    "sellers": aggregate.sellers,
                    "volume_net": aggregate.volume_net,
                },
            },
        )
        results.append(
            (
                signal,
                _fingerprint(ids, CONGRESS_SCORE_METHOD_VERSION, reference_date.strftime("%Y-%m-%d"), window_days),
            )
        )

    with get_connection(derived_db_path) as conn:
        insert_run(conn, run)
        for row in normalized_rows:
            if row.resolution_event_id:
                insert_resolution_event(conn, resolution_events[row.source_record_id])
            insert_normalized(conn, row)
        for signal, fingerprint in results:
            insert_signal_result(conn, signal, fingerprint)
        update_run_status(
            conn,
            run.run_id,
            "SUCCEEDED",
            utcnow_iso(),
            {
                "normalized_count": len(normalized_rows),
                "score_count": len(results),
            },
        )

    status = legacy_status.get_status(db_path=legacy_db_path)
    parity = {
        "legacy_window_transactions": legacy_window_count,
        "legacy_signal_transactions": legacy_signal_count,
        "imported_normalized": len(normalized_rows),
        "imported_results": len(results),
        "normalized_match": legacy_window_count == len(normalized_rows),
        "result_match": len(results) <= legacy_signal_count and len(results) > 0,
    }
    return CongressDerivedRunResult(
        run_id=run.run_id,
        reference_date=reference_date.strftime("%Y-%m-%d"),
        window_days=window_days,
        imported_normalized_count=len(normalized_rows),
        imported_result_count=len(results),
        legacy_status=asdict(status),
        parity=parity,
    )


def get_legacy_status(legacy_db_path: str) -> dict:
    status = legacy_status_service_module().get_status(db_path=legacy_db_path)
    return asdict(status)


def run_legacy_report(legacy_db_path: str, output: str, report_format: str, window_days: int) -> dict | None:
    result = legacy_reporting_service_module().build_report(
        window=window_days,
        output=output,
        report_format=report_format,
        db_path=legacy_db_path,
    )
    return asdict(result) if result is not None else None
