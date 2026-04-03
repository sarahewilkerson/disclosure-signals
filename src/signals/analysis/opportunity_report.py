from __future__ import annotations

from typing import Any

from signals.core.dto import SignalResult


def build_overlap_opportunity_report(
    insider_results: list[SignalResult],
    congress_results: list[SignalResult],
    combined_results: list[dict],
    blocked_rows: list[dict],
) -> dict[str, Any]:
    insider_by_subject = {row.subject_key: row for row in insider_results if row.scope == "entity"}
    congress_by_subject = {row.subject_key: row for row in congress_results if row.scope == "entity"}
    combined_by_subject = {row["subject_key"]: row for row in combined_results if isinstance(row, dict) and "subject_key" in row}
    blocked_by_subject = {row["subject_key"]: row for row in blocked_rows if isinstance(row, dict) and "subject_key" in row}

    overlap_subjects = sorted(set(insider_by_subject) & set(congress_by_subject))

    aligned: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    low_confidence: list[dict[str, Any]] = []
    insider_insufficient: list[dict[str, Any]] = []
    neutral_or_mixed: list[dict[str, Any]] = []
    blocked_ambiguity: list[dict[str, Any]] = []

    for subject in overlap_subjects:
        insider = insider_by_subject[subject]
        congress = congress_by_subject[subject]
        combined = combined_by_subject.get(subject)
        blocked = blocked_by_subject.get(subject)

        detail = {
            "subject_key": subject,
            "insider_label": insider.label,
            "insider_score": insider.score,
            "insider_confidence": insider.confidence,
            "congress_label": congress.label,
            "congress_score": congress.score,
            "congress_confidence": congress.confidence,
        }

        if combined:
            outcome = combined.get("overlay_outcome")
            detail["combined_label"] = combined.get("label")
            detail["combined_score"] = combined.get("score")
            detail["combined_confidence"] = combined.get("confidence")
            if outcome == "TRUE_CONFLICT":
                conflicts.append(detail)
            elif outcome == "LOW_CONFIDENCE_ALIGNMENT":
                low_confidence.append(detail)
            else:
                aligned.append(detail)
            continue

        if blocked and blocked.get("reason_code") == "AMBIGUOUS_ENTITY_MATCH":
            detail["block_reason"] = blocked["reason_code"]
            blocked_ambiguity.append(detail)
            continue

        if insider.label == "insufficient":
            insider_insufficient.append(detail)
            continue

        if insider.label == "neutral" or congress.label == "neutral":
            neutral_or_mixed.append(detail)
            continue

        low_confidence.append(detail)

    return {
        "overlap_subject_count": len(overlap_subjects),
        "aligned_count": len(aligned),
        "conflict_count": len(conflicts),
        "low_confidence_count": len(low_confidence),
        "insider_insufficient_count": len(insider_insufficient),
        "neutral_or_mixed_count": len(neutral_or_mixed),
        "blocked_ambiguity_count": len(blocked_ambiguity),
        "aligned_subjects": aligned[:20],
        "conflict_subjects": conflicts[:20],
        "low_confidence_subjects": low_confidence[:20],
        "insider_insufficient_subjects": insider_insufficient[:20],
        "neutral_or_mixed_subjects": neutral_or_mixed[:20],
        "blocked_ambiguity_subjects": blocked_ambiguity[:20],
    }


def render_overlap_opportunity_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Overlap Opportunity Report",
        "",
        "## Summary",
        f"- Overlap subjects: {report['overlap_subject_count']}",
        f"- Aligned subjects: {report['aligned_count']}",
        f"- Conflict subjects: {report['conflict_count']}",
        f"- Low-confidence subjects: {report['low_confidence_count']}",
        f"- Insider-insufficient subjects: {report['insider_insufficient_count']}",
        f"- Neutral/mixed subjects: {report['neutral_or_mixed_count']}",
        f"- Ambiguity-blocked subjects: {report['blocked_ambiguity_count']}",
        "",
        "## Insider-Insufficient",
        f"- {report['insider_insufficient_subjects']}",
        "",
        "## Neutral Or Mixed",
        f"- {report['neutral_or_mixed_subjects']}",
        "",
        "## Ambiguity-Blocked",
        f"- {report['blocked_ambiguity_subjects']}",
    ]
    return "\n".join(lines)
