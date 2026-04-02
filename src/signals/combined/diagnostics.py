from __future__ import annotations

from signals.core.dto import SignalResult


def build_overlay_diagnostics(
    insider_results: list[SignalResult],
    congress_results: list[SignalResult],
    blocked_rows: list[dict],
) -> dict:
    insider_subjects = sorted({row.subject_key for row in insider_results if row.scope == "entity"})
    congress_subjects = sorted({row.subject_key for row in congress_results if row.scope == "entity"})
    insider_set = set(insider_subjects)
    congress_set = set(congress_subjects)
    overlap = sorted(insider_set & congress_set)
    insider_only = sorted(insider_set - congress_set)
    congress_only = sorted(congress_set - insider_set)

    blocked_reason_counts: dict[str, int] = {}
    blocked_outcome_counts: dict[str, int] = {}
    for row in blocked_rows:
        blocked_reason_counts[row["reason_code"]] = blocked_reason_counts.get(row["reason_code"], 0) + 1
        outcome = row.get("overlay_outcome") or "UNKNOWN"
        blocked_outcome_counts[outcome] = blocked_outcome_counts.get(outcome, 0) + 1

    insider_by_subject = {row.subject_key: row for row in insider_results}
    congress_by_subject = {row.subject_key: row for row in congress_results}
    overlap_details = [
        {
            "subject_key": subject,
            "insider_label": insider_by_subject[subject].label,
            "insider_score": insider_by_subject[subject].score,
            "congress_label": congress_by_subject[subject].label,
            "congress_score": congress_by_subject[subject].score,
        }
        for subject in overlap
    ]

    return {
        "insider_subject_count": len(insider_subjects),
        "congress_subject_count": len(congress_subjects),
        "overlap_subject_count": len(overlap),
        "insider_only_count": len(insider_only),
        "congress_only_count": len(congress_only),
        "overlap_subjects": overlap,
        "overlap_details": overlap_details,
        "blocked_reason_counts": blocked_reason_counts,
        "blocked_outcome_counts": blocked_outcome_counts,
        "sample_insider_only": insider_only[:20],
        "sample_congress_only": congress_only[:20],
    }
