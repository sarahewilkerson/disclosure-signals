from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from signals.congress.engine import (
    compute_aggregate,
    compute_confidence_score,
    compute_entity_signal,
    score_transaction,
)
from signals.congress.parser import parse_house_pdf_text_only
from signals.congress.resolution import resolve_transaction
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
from signals.core.enums import ReasonCode, ResolutionStatus
from signals.core.git import git_sha
from signals.core.resolution import resolve_entity
from signals.core.runs import make_run, utcnow_iso
from signals.core.versioning import (
    CONGRESS_SCORE_METHOD_VERSION,
    NORMALIZATION_METHOD_VERSION,
    RESOLUTION_METHOD_VERSION,
)


@dataclass
class DirectCongressRunResult:
    run_id: str
    pdf_count: int
    imported_normalized_count: int
    imported_result_count: int
    skipped_count: int
    skip_reasons: dict[str, int]
    pdf_dir: str

    def to_dict(self) -> dict:
        return asdict(self)


def _subject_key(ticker: str | None, asset_name: str, source_id: str) -> str:
    if ticker:
        return f"entity:{ticker.lower()}"
    return f"unresolved:{source_id}:{asset_name.lower()}"


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


def run_direct_house_pdfs_into_derived(
    *,
    repo_root: Path,
    derived_db_path: str,
    pdf_dir: str,
    reference_date: datetime,
    window_days: int,
    max_files: int | None = None,
) -> DirectCongressRunResult:
    init_db(derived_db_path)
    pdf_root = Path(pdf_dir)
    pdf_files = sorted(pdf_root.glob("*.pdf"))
    if max_files is not None:
        pdf_files = pdf_files[:max_files]

    code_version = git_sha(repo_root)
    run = make_run(
        "direct_house_score",
        "congress",
        code_version,
        {
            "pdf_dir": str(pdf_root),
            "reference_date": reference_date.strftime("%Y-%m-%d"),
            "window_days": window_days,
            "max_files": max_files,
        },
        {
            "normalization": NORMALIZATION_METHOD_VERSION,
            "resolution": RESOLUTION_METHOD_VERSION,
            "score": CONGRESS_SCORE_METHOD_VERSION,
        },
    )

    normalized_rows: list[NormalizedTransaction] = []
    resolution_events: dict[str, object] = {}
    scored_by_subject: dict[str, list] = defaultdict(list)
    record_ids_by_subject: dict[str, list[str]] = defaultdict(list)
    skipped_count = 0
    skip_reasons: dict[str, int] = defaultdict(int)

    for pdf_path in pdf_files:
        filing, skip_reason = parse_house_pdf_text_only(repo_root, pdf_path)
        if filing is None:
            skipped_count += 1
            skip_reasons[skip_reason or "unknown"] += 1
            continue
        if skip_reason and not filing.transactions:
            skipped_count += 1
            skip_reasons[skip_reason] += 1
            continue
        for idx, txn in enumerate(filing.transactions, start=1):
            source_record_id = f"congress-house:{filing.filing_id or pdf_path.stem}:{idx}"
            resolution_event = resolve_entity(
                source="congress",
                source_record_id=source_record_id,
                source_filing_id=filing.filing_id or pdf_path.stem,
                ticker=txn.ticker,
                cik=None,
                issuer_name=txn.asset_name,
                instrument_type=txn.asset_type,
                run_id=run.run_id,
            )
            asset_resolution = resolve_transaction(
                asset_name=txn.asset_name,
                ticker=resolution_event.ticker or txn.ticker,
                asset_type_code=txn.asset_type,
            )
            resolution_events[source_record_id] = resolution_event
            include = (
                asset_resolution.include_in_signal
                and resolution_event.resolution_status == ResolutionStatus.RESOLVED.value
                and bool(resolution_event.ticker)
                and txn.transaction_type in {"purchase", "sale", "sale_partial"}
            )
            if include:
                exclusion_reason_code = None
            elif not asset_resolution.include_in_signal:
                exclusion_reason_code = ReasonCode.NON_SIGNAL_ASSET.value
            elif not resolution_event.ticker:
                exclusion_reason_code = ReasonCode.MISSING_TICKER.value
            elif resolution_event.resolution_status != ResolutionStatus.RESOLVED.value:
                exclusion_reason_code = ReasonCode.LOW_RESOLUTION_CONFIDENCE.value
            else:
                exclusion_reason_code = ReasonCode.NON_SIGNAL_ASSET.value
            normalized = NormalizedTransaction(
                source="congress",
                source_record_id=source_record_id,
                source_filing_id=filing.filing_id or pdf_path.stem,
                actor_id=filing.filing_id or pdf_path.stem,
                actor_name=filing.filer_name,
                actor_type="member",
                owner_type=txn.owner or "self",
                entity_key=resolution_event.entity_key or _subject_key(resolution_event.ticker, txn.asset_name, source_record_id),
                instrument_key=resolution_event.instrument_key,
                ticker=resolution_event.ticker,
                issuer_name=resolution_event.issuer_name or txn.asset_name,
                instrument_type=txn.asset_type,
                transaction_type=txn.transaction_type,
                direction=_direction(txn.transaction_type),
                execution_date=txn.transaction_date.strftime("%Y-%m-%d") if txn.transaction_date else None,
                disclosure_date=txn.notification_date.strftime("%Y-%m-%d") if txn.notification_date else None,
                amount_low=float(txn.amount_min) if txn.amount_min is not None else None,
                amount_high=float(txn.amount_max) if txn.amount_max is not None else None,
                amount_estimate=((float(txn.amount_min) + float(txn.amount_max)) / 2.0) if txn.amount_min is not None and txn.amount_max is not None else None,
                currency="USD",
                units_low=None,
                units_high=None,
                price_low=None,
                price_high=None,
                quality_score=1.0,
                parse_confidence=1.0,
                resolution_event_id=resolution_event.event_id,
                resolution_confidence=resolution_event.resolution_confidence,
                resolution_method_version=RESOLUTION_METHOD_VERSION,
                include_in_signal=include,
                exclusion_reason_code=exclusion_reason_code,
                exclusion_reason_detail=skip_reason,
                provenance_payload={
                    "source_system": "direct-congress-house-pdf",
                    "raw_record_id": source_record_id,
                    "raw_filing_id": filing.filing_id or pdf_path.stem,
                    "pdf_path": str(pdf_path),
                    "page_number": txn.page_number,
                    "raw_line": txn.raw_line,
                    "resolver_evidence": resolution_event.evidence_payload,
                    "asset_resolution": {
                        "resolved_ticker": asset_resolution.resolved_ticker,
                        "resolved_company": asset_resolution.resolved_company,
                        "category": asset_resolution.category.value,
                        "resolution_method": asset_resolution.resolution_method,
                        "resolution_confidence": asset_resolution.resolution_confidence,
                        "include_in_signal": asset_resolution.include_in_signal,
                        "exclusion_reason": asset_resolution.exclusion_reason,
                        "signal_relevance_weight": asset_resolution.signal_relevance_weight,
                    },
                    "method_versions": {
                        "normalization": NORMALIZATION_METHOD_VERSION,
                        "resolution": RESOLUTION_METHOD_VERSION,
                        "score": CONGRESS_SCORE_METHOD_VERSION,
                    },
                    "imported_at": utcnow_iso(),
                },
                normalization_method_version=NORMALIZATION_METHOD_VERSION,
                run_id=run.run_id,
            )
            normalized_rows.append(normalized)

            if not include or not normalized.ticker:
                continue

            scored = score_transaction(
                member_id=filing.filer_name or filing.filing_id or pdf_path.stem,
                ticker=normalized.ticker,
                transaction_type=txn.transaction_type,
                execution_date=txn.transaction_date,
                amount_min=txn.amount_min,
                amount_max=txn.amount_max,
                owner_type=txn.owner or "self",
                resolution_confidence=resolution_event.resolution_confidence,
                signal_weight=1.0,
                reference_date=reference_date,
            )
            subject_key = f"entity:{normalized.ticker.lower()}"
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
            explanation=f"{aggregate.transactions_included} qualifying direct House transaction(s) across {aggregate.unique_members} member(s)",
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
                "path": "direct_house_pdf",
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
                "pdf_count": len(pdf_files),
                "skipped_count": skipped_count,
                "skip_reasons": dict(skip_reasons),
            },
        )

    return DirectCongressRunResult(
        run_id=run.run_id,
        pdf_count=len(pdf_files),
        imported_normalized_count=len(normalized_rows),
        imported_result_count=len(results),
        skipped_count=skipped_count,
        skip_reasons=dict(skip_reasons),
        pdf_dir=str(pdf_root),
    )
