from __future__ import annotations

from signals.core.read_model import load_combined_block_events, load_combined_results, load_recent_runs, load_signal_results
from signals.reporting.formatters import render_json, render_persisted_report_text


def build_source_report(conn, source: str, run_id: str | None = None) -> tuple[str, dict]:
    results = load_signal_results(conn, source, run_id=run_id)
    runs = load_recent_runs(conn, source if run_id is None else None, run_id=run_id)
    text = render_persisted_report_text(
        title=f"{source.upper()} PERSISTED REPORT",
        source_results=results,
        combined_results=[],
        blocked=[],
        runs=runs,
    )
    payload = render_json(results, [], [], None)
    payload["runs"] = runs
    return text, payload


def build_combined_report(conn, run_id: str | None = None, blocked: list[dict] | None = None) -> tuple[str, dict]:
    combined = load_combined_results(conn, run_id=run_id)
    blocked_events = blocked if blocked is not None else load_combined_block_events(conn, run_id=run_id)
    runs = load_recent_runs(conn, "combined" if run_id is None else None, run_id=run_id)
    text = render_persisted_report_text(
        title="COMBINED PERSISTED REPORT",
        source_results=[],
        combined_results=combined,
        blocked=blocked_events,
        runs=runs,
    )
    payload = render_json([], combined, blocked_events, None)
    payload["runs"] = runs
    return text, payload
