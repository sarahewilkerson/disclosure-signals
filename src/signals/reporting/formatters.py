from __future__ import annotations

from signals.core.dto import CombinedResult, SignalResult


def render_text(source_results: list[SignalResult], combined_results: list[CombinedResult], blocked: list[dict], parity: dict | None = None) -> str:
    lines = [
        "DISCLOSURE SIGNALS VERTICAL SLICE",
        "",
        "Source Results:",
    ]
    for row in source_results:
        lines.append(
            f"- {row.source}:{row.subject_key} label={row.label} score={row.score:.4f} conf={row.confidence:.2f}"
        )
    lines.append("")
    lines.append("Combined Results:")
    for row in combined_results:
        lines.append(
            f"- {row.subject_key} outcome={row.overlay_outcome or row.agreement_state} label={row.label} score={row.score:.4f} conflict={row.conflict_score or 0.0:.2f}"
        )
    if blocked:
        lines.append("")
        lines.append("Blocked Combined Rows:")
        for row in blocked:
            lines.append(f"- {row['subject_key']} outcome={row.get('overlay_outcome')} reason={row['reason_code']} detail={row.get('reason_detail') or row.get('detail')}")
    if parity:
        lines.append("")
        lines.append(
            f"Parity: structural={parity['structural_ok']} analytical={parity['analytical_ok']} reporting={parity['reporting_ok']}"
        )
    return "\n".join(lines)


def render_json(source_results: list[SignalResult], combined_results: list[CombinedResult], blocked: list[dict], parity: dict | None = None) -> dict:
    return {
        "source_results": [row.to_dict() for row in source_results],
        "combined_results": [row.to_dict() for row in combined_results],
        "blocked_combined": blocked,
        "parity": parity,
    }


def render_persisted_report_text(
    title: str,
    source_results: list[SignalResult],
    combined_results: list[CombinedResult],
    blocked: list[dict],
    runs: list[dict],
) -> str:
    lines = [title, ""]
    if runs:
        lines.append("Recent Runs:")
        for run in runs:
            lines.append(f"- {run['run_type']} status={run['status']} run_id={run['run_id']}")
        lines.append("")
    if source_results:
        lines.append("Source Results:")
        for row in source_results:
            lines.append(
                f"- {row.subject_key} label={row.label} score={row.score:.4f} conf={row.confidence:.2f}"
            )
        lines.append("")
    if combined_results:
        lines.append("Combined Results:")
        for row in combined_results:
            lines.append(
                f"- {row.subject_key} outcome={row.overlay_outcome or row.agreement_state} label={row.label} score={row.score:.4f} conflict={row.conflict_score or 0.0:.2f}"
            )
        lines.append("")
    if blocked:
        lines.append("Blocked:")
        for row in blocked:
            lines.append(f"- {row['subject_key']} outcome={row.get('overlay_outcome')} reason={row['reason_code']}")
    return "\n".join(lines).rstrip()
