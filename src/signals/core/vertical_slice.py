from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

from signals.combined.overlay import build_overlay, fingerprint_for_combined
from signals.congress.slice import fingerprint_for_rows as congress_fingerprint
from signals.congress.slice import run_congress_vertical_slice
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
from signals.core.dto import CombinedResult, SignalResult
from signals.core.dto import EntityResolutionEvent
from signals.core.enums import OverlayOutcome
from signals.core.parity import compare_expected
from signals.core.runs import make_run, utcnow_iso
from signals.core.versioning import (
    COMBINE_METHOD_VERSION,
    CONGRESS_SCORE_METHOD_VERSION,
    INSIDER_SCORE_METHOD_VERSION,
    NORMALIZATION_METHOD_VERSION,
    RESOLUTION_METHOD_VERSION,
)
from signals.insider.slice import fingerprint_for_rows as insider_fingerprint
from signals.insider.slice import run_insider_vertical_slice
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
        normalized, scores = run_insider_vertical_slice(
            str(fixture_dir / "insider_form4_simple_buy.xml"),
            insider_run.run_id,
        )
        for row in normalized:
            insert_normalized(conn, row)
        fingerprint = insider_fingerprint(normalized)
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
        normalized, scores = run_congress_vertical_slice(
            str(fixture_dir / "congress_ptr_sample.html"),
            congress_run.run_id,
        )
        for row in normalized:
            insert_normalized(conn, row)
        fingerprint = congress_fingerprint(normalized)
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
        insider_normalized, insider_scores = run_insider_vertical_slice(
            str(fixture_dir / "insider_form4_simple_buy.xml"),
            insider_run.run_id,
        )
        for row in insider_normalized:
            insert_normalized(conn, row)
        insider_fp = insider_fingerprint(insider_normalized)
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
        congress_normalized, congress_scores = run_congress_vertical_slice(
            str(fixture_dir / "congress_ptr_sample.html"),
            congress_run.run_id,
        )
        for row in congress_normalized:
            insert_normalized(conn, row)
        congress_fp = congress_fingerprint(congress_normalized)
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
