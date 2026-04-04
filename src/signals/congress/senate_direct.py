from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

from bs4 import BeautifulSoup

from signals.congress.constants import AMOUNT_RANGES
from signals.congress.engine import (
    compute_aggregate,
    compute_confidence_score,
    compute_entity_signal,
    score_transaction,
)
from signals.congress.resolution import resolve_transaction
from signals.congress.senate_connector import SenateConnector, SenateFiling
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
    CONGRESS_SCORE_METHOD_VERSION,
    NORMALIZATION_METHOD_VERSION,
    RESOLUTION_METHOD_VERSION,
)

MINIMUM_CONGRESS_TRADE_AMOUNT = 15_000


@dataclass
class DirectSenateIngestResult:
    searched_count: int
    downloaded_ptr_count: int
    skipped_paper_count: int
    failed_count: int
    cache_dir: str
    html_dir: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DirectSenateRunResult:
    run_id: str
    html_count: int
    imported_normalized_count: int
    imported_result_count: int
    skipped_count: int
    html_dir: str

    def to_dict(self) -> dict:
        return asdict(self)


def _fingerprint(source_record_ids: list[str], method_version: str, as_of_date: str, lookback_window: int) -> str:
    basis = "|".join(sorted(source_record_ids) + [method_version, as_of_date, str(lookback_window)])
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _direction(transaction_type: str) -> str:
    lower = transaction_type.lower()
    if "purchase" in lower:
        return "BUY"
    if "sale" in lower:
        return "SELL"
    return "NEUTRAL"


def _normalize_owner(owner: str | None) -> str:
    owner = (owner or "self").strip().lower()
    mapping = {
        "self": "self",
        "spouse": "spouse",
        "joint": "joint",
        "dependent child": "dependent",
        "child": "dependent",
    }
    return mapping.get(owner, owner or "self")


def _normalize_txn_type(raw: str | None) -> str:
    lower = (raw or "").lower()
    if "purchase" in lower:
        return "purchase"
    if "sale" in lower and "partial" in lower:
        return "sale_partial"
    if "sale" in lower:
        return "sale"
    if "exchange" in lower:
        return "exchange"
    return "unknown"


def _parse_amount_range(repo_root: Path, amount_range: str | None) -> tuple[int | None, int | None]:
    if not amount_range:
        return None, None
    for range_text, bounds in AMOUNT_RANGES.items():
        if range_text in amount_range:
            return bounds
    return None, None


def ingest_senate_ptrs_direct(
    *,
    repo_root: Path,
    cache_dir: str,
    days: int,
    max_filings: int | None = None,
    force: bool = False,
) -> DirectSenateIngestResult:
    senate = SenateConnector(cache_dir=Path(cache_dir), request_delay=0.25)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    filings = _search_senate_ptrs_live(senate, start_date=start_date, end_date=end_date)
    if max_filings is not None:
        filings = filings[:max_filings]

    downloaded_ptr = 0
    skipped_paper = 0
    failed = 0
    for filing in filings:
        if filing.is_paper:
            skipped_paper += 1
            continue
        result = senate.download_ptr(filing.filing_id, force=force)
        if result is None:
            failed += 1
        else:
            downloaded_ptr += 1

    return DirectSenateIngestResult(
        searched_count=len(filings),
        downloaded_ptr_count=downloaded_ptr,
        skipped_paper_count=skipped_paper,
        failed_count=failed,
        cache_dir=str(Path(cache_dir)),
        html_dir=str(Path(senate.cache_dir)),
    )


def _search_senate_ptrs_live(senate, *, start_date: datetime, end_date: datetime):
    if not senate.ensure_session():
        return []
    search_page = senate._get(senate.SEARCH_URL)
    search_page.raise_for_status()
    soup = BeautifulSoup(search_page.text, "lxml")
    csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
    csrf_token = csrf_input.get("value", "") if csrf_input else ""
    endpoint = f"{senate.BASE_URL}/search/report/data/"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": senate.SEARCH_URL,
        "X-CSRFToken": senate.session.cookies.get("csrftoken", ""),
    }
    filings = []
    start = 0
    page_size = 100
    while True:
        data = {
            "draw": "1",
            "start": str(start),
            "length": str(page_size),
            "csrfmiddlewaretoken": csrf_token,
            "report_types": "[11]",
            "filer_types": "[]",
            "submitted_start_date": start_date.strftime("%m/%d/%Y 00:00:00"),
            "submitted_end_date": end_date.strftime("%m/%d/%Y 23:59:59"),
            "candidate_state": "",
            "senator_state": "",
            "office_id": "",
            "first_name": "",
            "last_name": "",
        }
        response = senate._post(endpoint, data=data, headers=headers)
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data", [])
        if not rows:
            break
        filings.extend(_parse_senate_json_rows(rows))
        start += len(rows)
        if start >= int(payload.get("recordsFiltered", 0)):
            break
    return filings


def _parse_senate_json_rows(rows: list[list[str]]):
    filings = []
    for row in rows:
        if len(row) < 5:
            continue
        report_html = row[3]
        match = re.search(r'/search/view/(ptr|paper)/([a-f0-9-]+)/', report_html, re.I)
        if not match:
            continue
        kind = match.group(1).lower()
        filing_id = match.group(2)
        is_paper = kind == "paper"
        filer_name = row[2] or f"{row[0]} {row[1]}".strip()
        filing_date = None
        try:
            filing_date = datetime.strptime(row[4], "%m/%d/%Y")
        except Exception:
            pass
        report_url = f"https://efdsearch.senate.gov/search/view/{kind}/{filing_id}/"
        filings.append(
            SenateFiling(
                filing_id=filing_id,
                filer_name=filer_name,
                state=None,
                filing_date=filing_date,
                report_url=report_url,
                is_paper=is_paper,
            )
        )
    return filings


def run_direct_senate_html_into_derived(
    *,
    repo_root: Path,
    derived_db_path: str,
    html_dir: str,
    reference_date: datetime,
    window_days: int,
    max_files: int | None = None,
) -> DirectSenateRunResult:
    init_db(derived_db_path)
    html_root = Path(html_dir)
    html_files = sorted(html_root.glob("ptr_*.html"))
    if max_files is not None:
        html_files = html_files[:max_files]

    code_version = git_sha(repo_root)
    run = make_run(
        "direct_senate_score",
        "congress",
        code_version,
        {
            "html_dir": str(html_root),
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
    with get_connection(derived_db_path) as conn:
        insert_run(conn, run)

    senate = SenateConnector(cache_dir=html_root.parent.parent, request_delay=0.0)

    normalized_rows: list[NormalizedTransaction] = []
    resolution_events: dict[str, object] = {}
    scored_by_subject: dict[str, list] = defaultdict(list)
    record_ids_by_subject: dict[str, list[str]] = defaultdict(list)
    results: list[tuple[SignalResult, str]] = []
    skipped_count = 0

    try:
        for html_path in html_files:
            ptr_id = html_path.stem.replace("ptr_", "")
            transactions = senate.parse_ptr_transactions(html_path)
            if not transactions:
                skipped_count += 1
                continue
            filing_id = f"senate:{ptr_id}"
            for idx, txn in enumerate(transactions, start=1):
                source_record_id = f"{filing_id}:{idx}"
                native_res = resolve_transaction(
                    asset_name=txn.asset_name or "",
                    ticker=txn.ticker,
                    asset_type_code=txn.asset_type,
                )
                resolution_event = resolve_entity(
                    source="congress",
                    source_record_id=source_record_id,
                    source_filing_id=filing_id,
                    ticker=native_res.resolved_ticker or txn.ticker,
                    cik=None,
                    issuer_name=native_res.resolved_company or txn.asset_name,
                    instrument_type=txn.asset_type,
                    run_id=run.run_id,
                )
                resolution_events[source_record_id] = resolution_event
                txn_type = _normalize_txn_type(txn.transaction_type)
                amount_min, amount_max = _parse_amount_range(repo_root, txn.amount_range)
                include = bool(native_res.include_in_signal and resolution_event.ticker and txn_type in {"purchase", "sale", "sale_partial"})
                if include and amount_max is not None and amount_max <= MINIMUM_CONGRESS_TRADE_AMOUNT:
                    include = False
                    exclusion_reason_code = ReasonCode.BELOW_MINIMUM_VALUE.value
                elif include:
                    exclusion_reason_code = None
                elif not native_res.include_in_signal:
                    exclusion_reason_code = ReasonCode.NON_SIGNAL_ASSET.value
                elif not resolution_event.ticker:
                    exclusion_reason_code = ReasonCode.MISSING_TICKER.value
                else:
                    exclusion_reason_code = ReasonCode.LOW_RESOLUTION_CONFIDENCE.value
                normalized = NormalizedTransaction(
                    source="congress",
                    source_record_id=source_record_id,
                    source_filing_id=filing_id,
                    actor_id=ptr_id,
                    actor_name=None,
                    actor_type="senator",
                    owner_type=_normalize_owner(txn.owner),
                    entity_key=resolution_event.entity_key,
                    instrument_key=resolution_event.instrument_key,
                    ticker=resolution_event.ticker,
                    issuer_name=resolution_event.issuer_name or txn.asset_name,
                    instrument_type=txn.asset_type,
                    transaction_type=txn_type,
                    direction=_direction(txn_type),
                    execution_date=txn.transaction_date.strftime("%Y-%m-%d") if txn.transaction_date else None,
                    disclosure_date=reference_date.strftime("%Y-%m-%d"),
                    amount_low=float(amount_min) if amount_min is not None else None,
                    amount_high=float(amount_max) if amount_max is not None else None,
                    amount_estimate=((float(amount_min) + float(amount_max)) / 2.0) if amount_min is not None and amount_max is not None else None,
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
                    exclusion_reason_detail=native_res.exclusion_reason,
                    provenance_payload={
                        "source_system": "direct-senate-html",
                        "raw_record_id": source_record_id,
                        "raw_filing_id": filing_id,
                        "html_path": str(html_path),
                        "amount_range": txn.amount_range,
                        "comment": txn.comment,
                        "resolver_evidence": resolution_event.evidence_payload,
                        "native_resolution": {
                            "method": native_res.resolution_method,
                            "confidence": native_res.resolution_confidence,
                            "include_in_signal": native_res.include_in_signal,
                            "exclusion_reason": native_res.exclusion_reason,
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
                    member_id=ptr_id,
                    ticker=normalized.ticker,
                    transaction_type=txn_type,
                    execution_date=txn.transaction_date,
                    amount_min=amount_min,
                    amount_max=amount_max,
                    owner_type=_normalize_owner(txn.owner),
                    resolution_confidence=resolution_event.resolution_confidence,
                    signal_weight=1.0,
                    reference_date=reference_date,
                    disclosure_date=reference_date,
                )
                subject_key = f"entity:{normalized.ticker.lower()}"
                scored_by_subject[subject_key].append(scored)
                record_ids_by_subject[subject_key].append(source_record_id)

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
                explanation=f"{aggregate.transactions_included} qualifying direct Senate transaction(s) across {aggregate.unique_members} member(s)",
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
                    "path": "direct_senate_html",
                },
            )
            results.append((signal, _fingerprint(ids, CONGRESS_SCORE_METHOD_VERSION, reference_date.strftime("%Y-%m-%d"), window_days)))

        with get_connection(derived_db_path) as conn:
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
                    "html_count": len(html_files),
                    "skipped_count": skipped_count,
                },
            )
    except Exception as exc:
        with get_connection(derived_db_path) as conn:
            update_run_status(
                conn,
                run.run_id,
                "FAILED",
                utcnow_iso(),
                {
                    "normalized_count": len(normalized_rows),
                    "score_count": len(results),
                    "html_count": len(html_files),
                    "skipped_count": skipped_count,
                    "error": str(exc),
                },
            )
        raise

    return DirectSenateRunResult(
        run_id=run.run_id,
        html_count=len(html_files),
        imported_normalized_count=len(normalized_rows),
        imported_result_count=len(results),
        skipped_count=skipped_count,
        html_dir=str(html_root),
    )
