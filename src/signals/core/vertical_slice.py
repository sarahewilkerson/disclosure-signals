from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from signals.combined.overlay import build_overlay, fingerprint_for_combined
from signals.congress.constants import AMOUNT_RANGES
from signals.congress.engine import score_transaction as congress_score_transaction
from signals.congress.senate_connector import SenateConnector
from signals.core.artifacts import ensure_dir, write_json, write_text
from signals.core.derived_db import (
    count_rows,
    fetch_all,
    fetch_combined_block_events,
    fetch_failed_runs,
    fetch_signal_results_by_source,
    get_connection,
    init_db,
    insert_combined_block_event,
    insert_combined_result,
    insert_normalized,
    insert_run,
    insert_signal_result,
    update_run_status,
)
from signals.core.dto import CombinedResult, NormalizedTransaction, SignalResult
from signals.core.enums import ReasonCode
from signals.core.parity import compare_expected
from signals.core.resolution import resolve_entity
from signals.core.runs import make_run, utcnow_iso
from signals.core.versioning import (
    COMBINE_METHOD_VERSION,
    CONGRESS_SCORE_METHOD_VERSION,
    INSIDER_SCORE_METHOD_VERSION,
    NORMALIZATION_METHOD_VERSION,
    RESOLUTION_METHOD_VERSION,
)
from signals.insider.engine import (
    aggregate_company_signal,
    classify_role,
    classify_transaction_type,
    compute_pct_holdings_changed,
    detect_planned_trade,
    score_transaction as insider_score_transaction,
)
from signals.insider.parser import parse_form4_xml
from signals.reporting.formatters import render_json, render_text


@dataclass
class VerticalSliceResult:
    runs: list[dict]
    normalized: list[dict]
    source_results: list[dict]
    combined_results: list[dict]
    blocked_combined: list[dict]
    parity: dict
    artifact_paths: dict[str, str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DomainFixtureResult:
    source: str
    runs: list[dict]
    normalized: list[dict]
    source_results: list[dict]
    blocked_combined: list[dict]
    artifact_paths: dict[str, str]

    def to_dict(self) -> dict:
        return asdict(self)


def _fingerprint_for_rows(rows: list[NormalizedTransaction]) -> str:
    basis = "|".join(sorted(row.source_record_id for row in rows))
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _run_insider_vertical_slice(xml_path: str, run_id: str) -> tuple[list[NormalizedTransaction], list[SignalResult]]:
    parsed = parse_form4_xml(Path(xml_path))
    if parsed["parse_error"]:
        raise ValueError(parsed["parse_error"])

    filing = parsed["filing"]
    filing_id = f"{filing['cik_issuer']}:{filing['cik_owner']}:{filing['period_of_report']}"
    normalized_rows: list[NormalizedTransaction] = []
    scored_transactions: list[dict] = []
    record_ids: list[str] = []
    subject_key = f"cik:{filing['cik_issuer']}"
    as_of_date = filing.get("period_of_report") or ""
    for idx, txn in enumerate(parsed["transactions"], start=1):
        role_class, exclusion = classify_role(
            filing.get("officer_title"),
            filing.get("owner_name"),
            bool(filing.get("is_officer")),
            bool(filing.get("is_director")),
            bool(filing.get("is_ten_pct_owner")),
            bool(filing.get("is_other")),
        )
        source_record_id = f"{filing_id}:{idx}"
        resolution_event = resolve_entity(
            source="insider",
            source_record_id=source_record_id,
            source_filing_id=filing_id,
            ticker=None,
            cik=filing.get("cik_issuer"),
            issuer_name=None,
            instrument_type=txn.get("security_title"),
            run_id=run_id,
        )
        include = exclusion is None and txn.get("transaction_code") in {"P", "S"}
        normalized = NormalizedTransaction(
            source="insider",
            source_record_id=source_record_id,
            source_filing_id=filing_id,
            actor_id=filing.get("cik_owner"),
            actor_name=filing.get("owner_name"),
            actor_type=role_class,
            owner_type="self",
            entity_key=resolution_event.entity_key,
            instrument_key=resolution_event.instrument_key,
            ticker=resolution_event.ticker,
            issuer_name=resolution_event.issuer_name,
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
            resolution_event_id=None,
            resolution_confidence=resolution_event.resolution_confidence,
            resolution_method_version=RESOLUTION_METHOD_VERSION,
            include_in_signal=include,
            exclusion_reason_code=None if include else ReasonCode.ENTITY_ROLE_EXCLUDED.value,
            exclusion_reason_detail=exclusion,
            provenance_payload={
                "source_system": "vertical-slice-direct-insider",
                "raw_record_id": source_record_id,
                "raw_filing_id": filing_id,
                "resolver_evidence": resolution_event.evidence_payload,
                "method_versions": {
                    "normalization": NORMALIZATION_METHOD_VERSION,
                    "resolution": RESOLUTION_METHOD_VERSION,
                    "score": INSIDER_SCORE_METHOD_VERSION,
                },
            },
            normalization_method_version=NORMALIZATION_METHOD_VERSION,
            run_id=run_id,
        )
        normalized_rows.append(normalized)
        if resolution_event.ticker:
            subject_key = f"entity:{resolution_event.ticker.lower()}"
        elif resolution_event.entity_key:
            subject_key = resolution_event.entity_key
        if include:
            as_of_date = txn.get("transaction_date") or as_of_date
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
            scored_txn.update(
                insider_score_transaction(
                    scored_txn,
                    datetime.strptime(txn["transaction_date"], "%Y-%m-%d"),
                )
            )
            scored_transactions.append(scored_txn)
            record_ids.append(source_record_id)

    signal_rows: list[SignalResult] = []
    if scored_transactions:
        aggregate = aggregate_company_signal(scored_transactions, 90)
        signal_rows.append(
            SignalResult(
                source="insider",
                scope="entity",
                subject_key=subject_key,
                score=float(aggregate["score"]),
                label=aggregate["signal"],
                confidence=float(aggregate["confidence"]),
                as_of_date=as_of_date,
                lookback_window=90,
                input_count=len(normalized_rows),
                included_count=aggregate["buy_count"] + aggregate["sell_count"],
                excluded_count=len(normalized_rows) - (aggregate["buy_count"] + aggregate["sell_count"]),
                explanation=aggregate["explanation"],
                method_version=INSIDER_SCORE_METHOD_VERSION,
                code_version="workspace",
                run_id=run_id,
                provenance_refs={
                    "normalized_row_ids": record_ids,
                    "resolution_event_ids": [],
                    "input_fingerprint_basis": "source_record_ids",
                },
            )
        )

    return normalized_rows, signal_rows


def _run_congress_vertical_slice(html_path: str, run_id: str) -> tuple[list[NormalizedTransaction], list[SignalResult]]:
    fixture_path = Path(html_path)
    connector = SenateConnector(cache_dir=fixture_path.parent.parent)
    parsed = connector.parse_ptr_transactions(fixture_path)
    filing_id = fixture_path.stem
    normalized_rows: list[NormalizedTransaction] = []
    signal_rows: list[SignalResult] = []

    for idx, txn in enumerate(parsed, start=1):
        source_record_id = f"{filing_id}:{idx}"
        resolution_event = resolve_entity(
            source="congress",
            source_record_id=source_record_id,
            source_filing_id=filing_id,
            ticker=txn.ticker,
            cik=None,
            issuer_name=txn.asset_name,
            instrument_type=txn.asset_type,
            run_id=run_id,
        )
        amount_min, amount_max = AMOUNT_RANGES.get(txn.amount_range, (None, None))
        include = bool(
            resolution_event.ticker
            and amount_min is not None
            and amount_max is not None
            and txn.transaction_type.lower().replace(" (partial)", "_partial") in {"purchase", "sale", "sale_partial"}
        )
        normalized = NormalizedTransaction(
            source="congress",
            source_record_id=source_record_id,
            source_filing_id=filing_id,
            actor_id=None,
            actor_name=txn.owner,
            actor_type="member",
            owner_type=txn.owner.lower(),
            entity_key=resolution_event.entity_key,
            instrument_key=resolution_event.instrument_key,
            ticker=resolution_event.ticker,
            issuer_name=resolution_event.issuer_name or txn.asset_name,
            instrument_type=txn.asset_type,
            transaction_type=txn.transaction_type.lower().replace(" (partial)", "_partial"),
            direction="BUY" if "purchase" in txn.transaction_type.lower() else "SELL" if "sale" in txn.transaction_type.lower() else "NEUTRAL",
            execution_date=txn.transaction_date.strftime("%Y-%m-%d") if txn.transaction_date else None,
            disclosure_date=txn.transaction_date.strftime("%Y-%m-%d") if txn.transaction_date else None,
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
            resolution_event_id=None,
            resolution_confidence=resolution_event.resolution_confidence,
            resolution_method_version=RESOLUTION_METHOD_VERSION,
            include_in_signal=include,
            exclusion_reason_code=None if include else ReasonCode.MISSING_TICKER.value,
            exclusion_reason_detail=None if include else ReasonCode.MISSING_TICKER.value,
            provenance_payload={
                "source_system": "vertical-slice-direct-congress",
                "raw_record_id": source_record_id,
                "raw_filing_id": filing_id,
                "source_values": {"owner": txn.owner, "asset_name": txn.asset_name},
                "resolver_evidence": resolution_event.evidence_payload,
                "method_versions": {
                    "normalization": NORMALIZATION_METHOD_VERSION,
                    "resolution": RESOLUTION_METHOD_VERSION,
                    "score": CONGRESS_SCORE_METHOD_VERSION,
                },
            },
            normalization_method_version=NORMALIZATION_METHOD_VERSION,
            run_id=run_id,
        )
        normalized_rows.append(normalized)
        if include and txn.transaction_date is not None:
            tx_score = congress_score_transaction(
                member_id=source_record_id,
                ticker=normalized.ticker,
                transaction_type=normalized.transaction_type,
                execution_date=txn.transaction_date,
                amount_min=amount_min,
                amount_max=amount_max,
                owner_type=normalized.owner_type,
                resolution_confidence=normalized.resolution_confidence or 0.0,
                signal_weight=1.0,
                reference_date=txn.transaction_date,
            )
            signal_rows.append(
                SignalResult(
                    source="congress",
                    scope="entity",
                    subject_key=f"entity:{normalized.ticker.lower()}" if normalized.ticker else (normalized.entity_key or f"unresolved:{source_record_id}"),
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


def git_sha(repo_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip() or "workspace"
    except Exception:
        return "workspace"


def parity_summary(payload: dict, fixture_dir: Path) -> dict:
    expected = json.loads((fixture_dir / "expected_vertical_slice.json").read_text())
    actual = {
        "normalized_count": len(payload["normalized"]),
        "source_scores": {
            f"{row['source']}:{row['subject_key']}": {"label": row["label"], "score": row["score"]}
            for row in payload["source_results"]
        },
        "combined_summary": {
            "combined_count": len(payload["combined_results"]),
            "blocked_reasons": sorted(row["reason_code"] for row in payload["blocked_combined"]),
        },
    }
    return compare_expected(actual, expected).to_dict()


def _typed_rows(payload: dict) -> tuple[list[SignalResult], list[CombinedResult]]:
    source_results = [
        SignalResult(
            **{
                **{k: row[k] for k in SignalResult.__dataclass_fields__.keys()},
                "provenance_refs": json.loads(row["provenance_refs"]),
            }
        )
        for row in payload["source_results"]
    ]
    combined_results = [
        CombinedResult(
            **{
                **{k: row[k] for k in CombinedResult.__dataclass_fields__.keys()},
                "provenance_refs": json.loads(row["provenance_refs"]),
            }
        )
        for row in payload["combined_results"]
    ]
    return source_results, combined_results


def emit_vertical_slice_artifacts(
    repo_root: Path,
    db_path: str,
    payload: dict,
    parity: dict,
    output_dir: Path | None = None,
) -> dict[str, str]:
    artifact_dir = ensure_dir(output_dir or (repo_root / "data" / "artifacts" / "vertical_slice"))
    source_results, combined_results = _typed_rows(payload)
    report_json = render_json(source_results, combined_results, payload["blocked_combined"], parity)
    report_text = render_text(source_results, combined_results, payload["blocked_combined"], parity)

    run_summary = {
        "db_path": db_path,
        "run_count": len(payload["runs"]),
        "normalized_count": len(payload["normalized"]),
        "source_result_count": len(payload["source_results"]),
        "combined_result_count": len(payload["combined_results"]),
        "blocked_combined_count": len(payload["blocked_combined"]),
    }
    blocked_histogram: dict[str, int] = {}
    for row in payload["blocked_combined"]:
        code = row["reason_code"]
        blocked_histogram[code] = blocked_histogram.get(code, 0) + 1

    unresolved_entities = [
        {
            "source_record_id": row["source_record_id"],
            "source": row["source"],
            "issuer_name": row["issuer_name"],
            "ticker": row["ticker"],
            "exclusion_reason_code": row["exclusion_reason_code"],
        }
        for row in payload["normalized"]
        if not row["entity_key"] or not row["ticker"]
    ]

    paths = {
        "run_summary": str(write_json(artifact_dir / "run_summary.json", run_summary)),
        "parity_report": str(write_json(artifact_dir / "parity_report.json", parity)),
        "exclusion_histogram": str(write_json(artifact_dir / "exclusion_histogram.json", blocked_histogram)),
        "unresolved_entities": str(write_json(artifact_dir / "unresolved_entities.json", {"rows": unresolved_entities})),
        "combined_block_report": str(write_json(artifact_dir / "combined_block_report.json", {"rows": payload["blocked_combined"]})),
        "report_json": str(write_json(artifact_dir / "report.json", report_json)),
        "report_text": str(write_text(artifact_dir / "report.txt", report_text)),
    }
    return paths


def _emit_domain_artifacts(
    source: str,
    repo_root: Path,
    db_path: str,
    payload: dict,
    output_dir: Path | None = None,
) -> dict[str, str]:
    artifact_dir = ensure_dir(output_dir or (repo_root / "data" / "artifacts" / source))
    source_results = [
        SignalResult(
            **{
                **{k: row[k] for k in SignalResult.__dataclass_fields__.keys()},
                "provenance_refs": json.loads(row["provenance_refs"]),
            }
        )
        for row in payload["source_results"]
    ]
    report_json = render_json(source_results, [], payload["blocked_combined"], None)
    report_text = render_text(source_results, [], payload["blocked_combined"], None)
    run_summary = {
        "source": source,
        "db_path": db_path,
        "run_count": len(payload["runs"]),
        "normalized_count": len(payload["normalized"]),
        "source_result_count": len(payload["source_results"]),
    }
    return {
        "run_summary": str(write_json(artifact_dir / "run_summary.json", run_summary)),
        "report_json": str(write_json(artifact_dir / "report.json", report_json)),
        "report_text": str(write_text(artifact_dir / "report.txt", report_text)),
    }


def _method_versions() -> dict[str, str]:
    return {
        "normalization": NORMALIZATION_METHOD_VERSION,
        "resolution": RESOLUTION_METHOD_VERSION,
        "insider_score": INSIDER_SCORE_METHOD_VERSION,
        "congress_score": CONGRESS_SCORE_METHOD_VERSION,
        "combine": COMBINE_METHOD_VERSION,
    }


def run_insider_fixture(repo_root: Path, db_path: str, fixture_dir: Path, artifact_dir: Path | None = None) -> DomainFixtureResult:
    init_db(db_path)
    code_version = git_sha(repo_root)
    insider_run = make_run(
        "fixture_insider",
        "insider",
        code_version,
        {"fixture": "insider_form4_simple_buy.xml"},
        _method_versions(),
    )
    with get_connection(db_path) as conn:
        insert_run(conn, insider_run)
        normalized, scores = _run_insider_vertical_slice(
            str(fixture_dir / "insider_form4_simple_buy.xml"),
            insider_run.run_id,
        )
        for row in normalized:
            insert_normalized(conn, row)
        fingerprint = _fingerprint_for_rows(normalized)
        for row in scores:
            insert_signal_result(conn, row, fingerprint)
        update_run_status(
            conn,
            insider_run.run_id,
            "SUCCEEDED",
            utcnow_iso(),
            {"normalized_count": len(normalized), "score_count": len(scores)},
        )
        payload = {
            "runs": [dict(row) for row in conn.execute("SELECT * FROM runs WHERE run_id = ?", (insider_run.run_id,))],
            "normalized": [dict(row) for row in conn.execute("SELECT * FROM normalized_transactions WHERE source = 'insider' ORDER BY id")],
            "source_results": fetch_signal_results_by_source(conn, "insider"),
            "blocked_combined": [],
        }
    artifact_paths = _emit_domain_artifacts("insider", repo_root, db_path, payload, artifact_dir)
    return DomainFixtureResult(
        source="insider",
        runs=payload["runs"],
        normalized=payload["normalized"],
        source_results=payload["source_results"],
        blocked_combined=[],
        artifact_paths=artifact_paths,
    )


def run_congress_fixture(repo_root: Path, db_path: str, fixture_dir: Path, artifact_dir: Path | None = None) -> DomainFixtureResult:
    init_db(db_path)
    code_version = git_sha(repo_root)
    congress_run = make_run(
        "fixture_congress",
        "congress",
        code_version,
        {"fixture": "congress_ptr_sample.html"},
        _method_versions(),
    )
    with get_connection(db_path) as conn:
        insert_run(conn, congress_run)
        normalized, scores = _run_congress_vertical_slice(
            str(fixture_dir / "congress_ptr_sample.html"),
            congress_run.run_id,
        )
        for row in normalized:
            insert_normalized(conn, row)
        fingerprint = _fingerprint_for_rows(normalized)
        for row in scores:
            insert_signal_result(conn, row, fingerprint)
        update_run_status(
            conn,
            congress_run.run_id,
            "SUCCEEDED",
            utcnow_iso(),
            {"normalized_count": len(normalized), "score_count": len(scores)},
        )
        payload = {
            "runs": [dict(row) for row in conn.execute("SELECT * FROM runs WHERE run_id = ?", (congress_run.run_id,))],
            "normalized": [dict(row) for row in conn.execute("SELECT * FROM normalized_transactions WHERE source = 'congress' ORDER BY id")],
            "source_results": fetch_signal_results_by_source(conn, "congress"),
            "blocked_combined": [],
        }
    artifact_paths = _emit_domain_artifacts("congress", repo_root, db_path, payload, artifact_dir)
    return DomainFixtureResult(
        source="congress",
        runs=payload["runs"],
        normalized=payload["normalized"],
        source_results=payload["source_results"],
        blocked_combined=[],
        artifact_paths=artifact_paths,
    )


def build_combined_fixture(repo_root: Path, db_path: str, artifact_dir: Path | None = None) -> DomainFixtureResult:
    init_db(db_path)
    code_version = git_sha(repo_root)
    combined_run = make_run(
        "fixture_combined",
        "combined",
        code_version,
        {"mode": "overlay_from_db"},
        _method_versions(),
    )
    with get_connection(db_path) as conn:
        insider_rows = fetch_signal_results_by_source(conn, "insider")
        congress_rows = fetch_signal_results_by_source(conn, "congress")
        insider_results = [
            SignalResult(
                **{
                    **{k: row[k] for k in SignalResult.__dataclass_fields__.keys()},
                    "provenance_refs": json.loads(row["provenance_refs"]),
                }
            )
            for row in insider_rows
        ]
        congress_results = [
            SignalResult(
                **{
                    **{k: row[k] for k in SignalResult.__dataclass_fields__.keys()},
                    "provenance_refs": json.loads(row["provenance_refs"]),
                }
            )
            for row in congress_rows
        ]
        insert_run(conn, combined_run)
        combined_rows, blocked = build_overlay(insider_results, congress_results, {}, combined_run.run_id)
        for row in combined_rows:
            insert_combined_result(conn, row, fingerprint_for_combined(row))
        for row in blocked:
            insert_combined_block_event(conn, row)
        update_run_status(
            conn,
            combined_run.run_id,
            "SUCCEEDED",
            utcnow_iso(),
            {"combined_count": len(combined_rows), "blocked_count": len(blocked)},
        )
        payload = {
            "runs": [dict(row) for row in conn.execute("SELECT * FROM runs WHERE run_id = ?", (combined_run.run_id,))],
            "normalized": [],
            "source_results": [],
            "blocked_combined": fetch_combined_block_events(conn, combined_run.run_id),
            "combined_results": fetch_all(conn, "combined_results"),
        }
    artifact_paths = emit_vertical_slice_artifacts(
        repo_root,
        db_path,
        {
            "runs": payload["runs"],
            "normalized": [],
            "source_results": insider_rows + congress_rows,
            "combined_results": payload["combined_results"],
            "blocked_combined": payload["blocked_combined"],
        },
        {
            "structural_ok": True,
            "analytical_ok": True,
            "reporting_ok": True,
            "tolerated_deltas": {},
            "unexpected_divergences": [],
        },
        output_dir=artifact_dir or (repo_root / "data" / "artifacts" / "combined"),
    )
    return DomainFixtureResult(
        source="combined",
        runs=payload["runs"],
        normalized=[],
        source_results=[],
        blocked_combined=payload["blocked_combined"],
        artifact_paths=artifact_paths,
    )


def run_vertical_slice(repo_root: Path, db_path: str, fixture_dir: Path, artifact_dir: Path | None = None) -> VerticalSliceResult:
    init_db(db_path)
    code_version = git_sha(repo_root)
    method_versions = {
        "normalization": NORMALIZATION_METHOD_VERSION,
        "resolution": RESOLUTION_METHOD_VERSION,
        "insider_score": INSIDER_SCORE_METHOD_VERSION,
        "congress_score": CONGRESS_SCORE_METHOD_VERSION,
        "combine": COMBINE_METHOD_VERSION,
    }
    insider_run = make_run("vertical_slice", "insider", code_version, {"fixture": "insider_form4_simple_buy.xml"}, method_versions)
    congress_run = make_run("vertical_slice", "congress", code_version, {"fixture": "congress_ptr_sample.html"}, method_versions)
    combined_run = make_run("vertical_slice", "combined", code_version, {"mode": "overlay"}, method_versions)

    with get_connection(db_path) as conn:
        insert_run(conn, insider_run)
        insider_normalized, insider_scores = _run_insider_vertical_slice(
            str(fixture_dir / "insider_form4_simple_buy.xml"),
            insider_run.run_id,
        )
        for row in insider_normalized:
            insert_normalized(conn, row)
        insider_fp = _fingerprint_for_rows(insider_normalized)
        for row in insider_scores:
            insert_signal_result(conn, row, insider_fp)
        update_run_status(
            conn,
            insider_run.run_id,
            "SUCCEEDED",
            utcnow_iso(),
            {"normalized_count": len(insider_normalized), "score_count": len(insider_scores)},
        )

        insert_run(conn, congress_run)
        congress_normalized, congress_scores = _run_congress_vertical_slice(
            str(fixture_dir / "congress_ptr_sample.html"),
            congress_run.run_id,
        )
        for row in congress_normalized:
            insert_normalized(conn, row)
        congress_fp = _fingerprint_for_rows(congress_normalized)
        for row in congress_scores:
            insert_signal_result(conn, row, congress_fp)
        update_run_status(
            conn,
            congress_run.run_id,
            "SUCCEEDED",
            utcnow_iso(),
            {"normalized_count": len(congress_normalized), "score_count": len(congress_scores)},
        )

        insert_run(conn, combined_run)
        combined_rows, blocked = build_overlay(insider_scores, congress_scores, {}, combined_run.run_id)
        for row in combined_rows:
            insert_combined_result(conn, row, fingerprint_for_combined(row))
        for row in blocked:
            insert_combined_block_event(conn, row)
        update_run_status(
            conn,
            combined_run.run_id,
            "SUCCEEDED",
            utcnow_iso(),
            {"combined_count": len(combined_rows), "blocked_count": len(blocked)},
        )

        payload = {
            "runs": fetch_all(conn, "runs"),
            "normalized": fetch_all(conn, "normalized_transactions"),
            "source_results": fetch_all(conn, "signal_results"),
            "combined_results": fetch_all(conn, "combined_results"),
            "blocked_combined": fetch_combined_block_events(conn, combined_run.run_id),
        }

    parity = parity_summary(payload, fixture_dir)
    artifact_paths = emit_vertical_slice_artifacts(
        repo_root=repo_root,
        db_path=db_path,
        payload=payload,
        parity=parity,
        output_dir=artifact_dir,
    )
    return VerticalSliceResult(
        runs=payload["runs"],
        normalized=payload["normalized"],
        source_results=payload["source_results"],
        combined_results=payload["combined_results"],
        blocked_combined=payload["blocked_combined"],
        parity=parity,
        artifact_paths=artifact_paths,
    )


def derived_status(db_path: str) -> dict:
    init_db(db_path)
    with get_connection(db_path) as conn:
        return {
            "runs": count_rows(conn, "runs"),
            "normalized": count_rows(conn, "normalized_transactions"),
            "source_results": count_rows(conn, "signal_results"),
            "combined_results": count_rows(conn, "combined_results"),
            "failed_runs": len(fetch_failed_runs(conn)),
            "source_result_counts": {
                "insider": len(fetch_signal_results_by_source(conn, "insider")),
                "congress": len(fetch_signal_results_by_source(conn, "congress")),
            },
            "recent_runs": [
                {
                    "run_id": row["run_id"],
                    "source": row["source"],
                    "run_type": row["run_type"],
                    "status": row["status"],
                }
                for row in conn.execute(
                    "SELECT run_id, source, run_type, status FROM runs ORDER BY started_at DESC LIMIT 10"
                ).fetchall()
            ],
        }
