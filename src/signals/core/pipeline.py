from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from signals.combined.diagnostics import build_overlay_diagnostics
from signals.combined.service import build_from_derived
from signals.congress.direct_service import run_direct_house_pdfs_into_derived
from signals.congress.senate_direct import ingest_senate_ptrs_direct, run_direct_senate_html_into_derived
from signals.congress.ingest import ingest_house_ptrs_direct
from signals.congress.service import run_legacy_score_into_derived as run_congress_legacy_score_into_derived
from signals.core.artifacts import ensure_dir, write_json, write_text
from signals.core.derived_db import get_connection, init_db
from signals.core.dto import SignalResult
from signals.core.parity import ParityReport
from signals.insider.direct_service import run_direct_xml_into_derived
from signals.insider.ingest import ingest_universe_direct
from signals.insider.service import run_legacy_score_into_derived as run_insider_legacy_score_into_derived
from signals.reporting.service import build_combined_report, build_source_report


@dataclass
class UnifiedRunResult:
    insider: dict
    congress: dict
    combined: dict
    reports: dict
    artifact_paths: dict[str, str]

    def to_dict(self) -> dict:
        return asdict(self)


def _to_dict(result):
    return result.to_dict() if hasattr(result, "to_dict") else result


def _typed_signal_results(rows: list[dict]) -> list[SignalResult]:
    return [SignalResult(**row) for row in rows]


def _build_unified_observability(conn, insider_run_id: str, congress_run_ids: list[str], combined_run_id: str, blocked_rows: list[dict]) -> tuple[dict, dict, dict, dict, dict]:
    congress_ids = list(congress_run_ids)
    normalized_run_ids = [insider_run_id, *congress_ids]
    placeholders = ", ".join("?" for _ in normalized_run_ids)
    normalized_rows = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT * FROM normalized_transactions
            WHERE run_id IN ({placeholders})
            ORDER BY id
            """,
            normalized_run_ids,
        ).fetchall()
    ]

    unresolved_entities = [
        {
            "source": row["source"],
            "source_record_id": row["source_record_id"],
            "issuer_name": row["issuer_name"],
            "ticker": row["ticker"],
            "exclusion_reason_code": row["exclusion_reason_code"],
        }
        for row in normalized_rows
        if not row["entity_key"] or not row["ticker"]
    ]

    exclusion_histogram: dict[str, int] = {}
    for row in normalized_rows:
        code = row["exclusion_reason_code"]
        if code:
            exclusion_histogram[code] = exclusion_histogram.get(code, 0) + 1

    blocked_histogram: dict[str, int] = {}
    for row in blocked_rows:
        code = row["reason_code"]
        blocked_histogram[code] = blocked_histogram.get(code, 0) + 1

    source_counts = {
        "insider": int(conn.execute("SELECT COUNT(*) AS c FROM signal_results WHERE source = 'insider' AND run_id = ?", (insider_run_id,)).fetchone()["c"]),
        "congress": int(
            conn.execute(
                f"SELECT COUNT(*) AS c FROM signal_results WHERE source = 'congress' AND run_id IN ({', '.join('?' for _ in congress_ids)})",
                congress_ids,
            ).fetchone()["c"]
        ) if congress_ids else 0,
        "combined": int(conn.execute("SELECT COUNT(*) AS c FROM combined_results WHERE run_id = ?", (combined_run_id,)).fetchone()["c"]),
    }

    run_ids = [*congress_ids, combined_run_id, insider_run_id]
    run_rows = [
        {
            "run_id": row["run_id"],
            "source": row["source"],
            "run_type": row["run_type"],
            "status": row["status"],
        }
        for row in conn.execute(
            f"SELECT run_id, source, run_type, status FROM runs WHERE run_id IN ({', '.join('?' for _ in run_ids)}) ORDER BY started_at DESC",
            run_ids,
        ).fetchall()
    ]

    run_summary = {
        "run_count": len(run_rows),
        "normalized_count": len(normalized_rows),
        "source_counts": source_counts,
        "failed_run_count": int(
            conn.execute(
                f"SELECT COUNT(*) AS c FROM runs WHERE run_id IN ({', '.join('?' for _ in run_ids)}) AND status = 'FAILED'",
                run_ids,
            ).fetchone()["c"]
        ),
        "recent_runs": run_rows,
    }

    parity = ParityReport(
        structural_ok=source_counts["insider"] > 0 and source_counts["congress"] > 0,
        analytical_ok=source_counts["insider"] > 0 and source_counts["congress"] > 0,
        reporting_ok=source_counts["combined"] >= 0,
        tolerated_deltas={},
        unexpected_divergences=[],
    ).to_dict()

    return (
        run_summary,
        parity,
        exclusion_histogram,
        {"rows": unresolved_entities},
        {"rows": blocked_rows, "counts": blocked_histogram},
    )


def run_unified_pipeline(
    repo_root: Path,
    derived_db_path: str,
    insider_legacy_db_path: str,
    congress_legacy_db_path: str,
    reference_date: datetime,
    lookback_window: int,
    artifact_dir: Path | None = None,
) -> UnifiedRunResult:
    insider = run_insider_legacy_score_into_derived(
        repo_root=repo_root,
        derived_db_path=derived_db_path,
        legacy_db_path=insider_legacy_db_path,
        reference_date=reference_date,
    )
    congress = run_congress_legacy_score_into_derived(
        repo_root=repo_root,
        derived_db_path=derived_db_path,
        legacy_db_path=congress_legacy_db_path,
        window_days=lookback_window,
        reference_date=reference_date,
    )
    combined = build_from_derived(repo_root, derived_db_path, lookback_window=lookback_window)

    init_db(derived_db_path)
    with get_connection(derived_db_path) as conn:
        insider_text, insider_payload = build_source_report(conn, "insider", run_id=insider.run_id)
        congress_text, congress_payload = build_source_report(conn, "congress", run_id=congress.run_id)
        combined_text, combined_payload = build_combined_report(conn, run_id=combined.run_id, blocked=combined.blocked_rows)
        overlay_diagnostics = build_overlay_diagnostics(
            _typed_signal_results(insider_payload["source_results"]),
            _typed_signal_results(congress_payload["source_results"]),
            combined.blocked_rows,
        )
        (
            run_summary,
            parity_report,
            exclusion_histogram,
            unresolved_entities,
            combined_block_report,
        ) = _build_unified_observability(
            conn,
            insider.run_id,
            [congress.run_id],
            combined.run_id,
            combined.blocked_rows,
        )

    reporting_ok = (
        len(insider_payload["source_results"]) == insider.imported_result_count
        and len(congress_payload["source_results"]) == congress.imported_result_count
        and len(combined_payload["combined_results"]) == combined.combined_count
    )
    parity_report["structural_ok"] = insider.parity["normalized_match"] and congress.parity["normalized_match"]
    parity_report["analytical_ok"] = insider.parity["result_match"] and congress.parity["result_match"]
    parity_report["reporting_ok"] = reporting_ok
    if not reporting_ok:
        parity_report["unexpected_divergences"].append("run-scoped report counts do not match imported result counts")

    artifact_paths: dict[str, str] = {}
    if artifact_dir is not None:
        base = ensure_dir(artifact_dir)
        artifact_paths = {
            "run_summary": str(write_json(base / "run_summary.json", {
                **run_summary,
                "insider": insider.to_dict(),
                "congress": congress.to_dict(),
                "combined": combined.to_dict(),
            })),
            "parity_report": str(write_json(base / "parity_report.json", {
                **parity_report,
                "legacy_parity": {
                    "insider": insider.parity,
                    "congress": congress.parity,
                },
            })),
            "exclusion_histogram": str(write_json(base / "exclusion_histogram.json", exclusion_histogram)),
            "unresolved_entities": str(write_json(base / "unresolved_entities.json", unresolved_entities)),
            "combined_block_report": str(write_json(base / "combined_block_report.json", combined_block_report)),
            "overlay_diagnostics": str(write_json(base / "overlay_diagnostics.json", overlay_diagnostics)),
            "insider_report_json": str(write_json(base / "insider_report.json", insider_payload)),
            "congress_report_json": str(write_json(base / "congress_report.json", congress_payload)),
            "combined_report_json": str(write_json(base / "combined_report.json", combined_payload)),
            "insider_report_text": str(write_text(base / "insider_report.txt", insider_text)),
            "congress_report_text": str(write_text(base / "congress_report.txt", congress_text)),
            "combined_report_text": str(write_text(base / "combined_report.txt", combined_text)),
        }

    return UnifiedRunResult(
        insider=insider.to_dict(),
        congress=congress.to_dict(),
        combined=combined.to_dict(),
        reports={
            "insider": insider_payload,
            "congress": congress_payload,
            "combined": combined_payload,
        },
        artifact_paths=artifact_paths,
    )


def run_direct_pipeline(
    *,
    repo_root: Path,
    derived_db_path: str,
    insider_csv_path: str,
    insider_user_agent: str,
    insider_cache_dir: str,
    congress_cache_dir: str,
    reference_date: datetime,
    lookback_window: int,
    insider_max_filings: int | None = None,
    house_days: int = 90,
    house_max_filings: int | None = None,
    senate_days: int = 365,
    senate_max_filings: int | None = None,
    artifact_dir: Path | None = None,
) -> UnifiedRunResult:
    def _run_insider_branch():
        insider_ingest = ingest_universe_direct(
            csv_path=insider_csv_path,
            user_agent=insider_user_agent,
            cache_dir=insider_cache_dir,
            max_filings_per_company=insider_max_filings,
            start_date=None,
            end_date=None,
        )
        insider = run_direct_xml_into_derived(
            repo_root=repo_root,
            derived_db_path=derived_db_path,
            xml_dir=insider_ingest["filings_dir"],
            reference_date=reference_date,
        )
        return insider_ingest, insider

    def _run_house_branch():
        house_ingest = ingest_house_ptrs_direct(
            repo_root=repo_root,
            cache_dir=congress_cache_dir,
            days=house_days,
            max_filings=house_max_filings,
            force=False,
        )
        house = run_direct_house_pdfs_into_derived(
            repo_root=repo_root,
            derived_db_path=derived_db_path,
            pdf_dir=house_ingest.pdf_dir,
            reference_date=reference_date,
            window_days=lookback_window,
            max_files=house_max_filings,
        )
        return house_ingest, house

    def _run_senate_branch():
        senate_ingest = ingest_senate_ptrs_direct(
            repo_root=repo_root,
            cache_dir=congress_cache_dir,
            days=senate_days,
            max_filings=senate_max_filings,
            force=False,
        )
        senate = run_direct_senate_html_into_derived(
            repo_root=repo_root,
            derived_db_path=derived_db_path,
            html_dir=senate_ingest.html_dir,
            reference_date=reference_date,
            window_days=lookback_window,
            max_files=senate_max_filings,
        )
        return senate_ingest, senate

    with ThreadPoolExecutor(max_workers=3) as executor:
        insider_future = executor.submit(_run_insider_branch)
        house_future = executor.submit(_run_house_branch)
        senate_future = executor.submit(_run_senate_branch)
        insider_ingest, insider = insider_future.result()
        house_ingest, house = house_future.result()
        senate_ingest, senate = senate_future.result()

    combined = build_from_derived(repo_root, derived_db_path, lookback_window=lookback_window)

    init_db(derived_db_path)
    with get_connection(derived_db_path) as conn:
        insider_text, insider_payload = build_source_report(conn, "insider", run_id=insider.run_id)
        congress_text, congress_payload = build_source_report(conn, "congress", run_ids=[house.run_id, senate.run_id])
        combined_text, combined_payload = build_combined_report(conn, run_id=combined.run_id, blocked=combined.blocked_rows)
        overlay_diagnostics = build_overlay_diagnostics(
            _typed_signal_results(insider_payload["source_results"]),
            _typed_signal_results(congress_payload["source_results"]),
            combined.blocked_rows,
        )
        (
            run_summary,
            parity_report,
            exclusion_histogram,
            unresolved_entities,
            combined_block_report,
        ) = _build_unified_observability(
            conn,
            insider.run_id,
            [house.run_id, senate.run_id],
            combined.run_id,
            combined.blocked_rows,
        )

    artifact_paths: dict[str, str] = {}
    if artifact_dir is not None:
        base = ensure_dir(artifact_dir)
        artifact_paths = {
            "run_summary": str(write_json(base / "run_summary.json", {
                **run_summary,
                "insider_ingest": insider_ingest,
                "house_ingest": house_ingest.to_dict(),
                "senate_ingest": senate_ingest.to_dict(),
                "insider": insider.to_dict(),
                "house": house.to_dict(),
                "senate": senate.to_dict(),
                "combined": combined.to_dict(),
            })),
            "parity_report": str(write_json(base / "parity_report.json", parity_report)),
            "exclusion_histogram": str(write_json(base / "exclusion_histogram.json", exclusion_histogram)),
            "unresolved_entities": str(write_json(base / "unresolved_entities.json", unresolved_entities)),
            "combined_block_report": str(write_json(base / "combined_block_report.json", combined_block_report)),
            "overlay_diagnostics": str(write_json(base / "overlay_diagnostics.json", overlay_diagnostics)),
            "insider_report_json": str(write_json(base / "insider_report.json", insider_payload)),
            "congress_report_json": str(write_json(base / "congress_report.json", congress_payload)),
            "combined_report_json": str(write_json(base / "combined_report.json", combined_payload)),
            "insider_report_text": str(write_text(base / "insider_report.txt", insider_text)),
            "congress_report_text": str(write_text(base / "congress_report.txt", congress_text)),
            "combined_report_text": str(write_text(base / "combined_report.txt", combined_text)),
        }

    return UnifiedRunResult(
        insider={
            "ingest": insider_ingest,
            "score": insider.to_dict(),
        },
        congress={
            "house_ingest": house_ingest.to_dict(),
            "house_score": house.to_dict(),
            "senate_ingest": senate_ingest.to_dict(),
            "senate_score": senate.to_dict(),
            "imported_result_count": house.imported_result_count + senate.imported_result_count,
            "imported_normalized_count": house.imported_normalized_count + senate.imported_normalized_count,
        },
        combined=combined.to_dict(),
        reports={
            "insider": insider_payload,
            "congress": congress_payload,
            "combined": combined_payload,
        },
        artifact_paths=artifact_paths,
    )
