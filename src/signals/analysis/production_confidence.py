from __future__ import annotations

from typing import Any


def _primary_constraint(summary: dict[str, Any], quality: dict[str, Any], blocked_reason_counts: dict[str, int]) -> str:
    overlap = summary["overlap_subject_count"]
    combined = summary["combined_count"]
    blocked_total = summary["blocked_count"]
    missing_counterpart = blocked_reason_counts.get("MISSING_COUNTERPART", 0)
    ambiguous = blocked_reason_counts.get("AMBIGUOUS_ENTITY_MATCH", 0)
    if overlap == 0:
        return "no_cross_source_overlap"
    if missing_counterpart and (missing_counterpart >= max(5, int(blocked_total * 0.6)) or missing_counterpart >= overlap):
        return "sparse_counterpart_coverage"
    if ambiguous and ambiguous >= max(2, int(blocked_total * 0.25)):
        return "entity_resolution_ambiguity"
    if quality["house_candidate_count"] > 2 or quality["senate_candidate_count"] > 2 or quality["insider_candidate_count"] > 2:
        return "candidate_backlog"
    if quality["house_scored_signal_rate"] < 0.1:
        return "low_house_signal_extraction_rate"
    return "combined_layer_ready_for_strategy_evaluation"


def _readiness(constraint: str, summary: dict[str, Any], quality: dict[str, Any]) -> str:
    if constraint in {"no_cross_source_overlap", "sparse_counterpart_coverage"}:
        return "pipeline_correct_but_signal_sparse"
    if constraint in {"entity_resolution_ambiguity", "candidate_backlog", "low_house_signal_extraction_rate"}:
        return "pipeline_operational_needs_more_data_quality_work"
    if summary["combined_count"] > 0 and quality["house_candidate_count"] == 0 and quality["senate_candidate_count"] == 0:
        return "ready_for_broader_live_strategy_validation"
    return "operationally_ready"


def build_production_confidence_report(result: dict[str, Any]) -> dict[str, Any]:
    insider = result["insider"]
    congress = result["congress"]
    combined = result["combined"]
    reports = result["reports"]

    overlay = reports["overlay_diagnostics"]
    insider_candidates = insider["candidate_discovery"]
    house_quality = congress["house_quality_metrics"]
    house_candidates = congress["house_candidate_discovery"]
    senate_candidates = congress["senate_candidate_discovery"]

    summary = {
        "insider_result_count": insider["score"]["imported_result_count"],
        "house_result_count": congress["house_score"]["imported_result_count"],
        "senate_result_count": congress["senate_score"]["imported_result_count"],
        "congress_result_count": congress["imported_result_count"],
        "combined_count": combined["combined_count"],
        "blocked_count": combined["blocked_count"],
        "overlap_subject_count": overlay["overlap_subject_count"],
        "insider_subject_count": overlay["insider_subject_count"],
        "congress_subject_count": overlay["congress_subject_count"],
    }

    overlap = overlay["overlap_subject_count"]
    congress_count = max(1, congress["imported_result_count"])
    quality = {
        "combined_yield_rate_vs_overlap": round((combined["combined_count"] / overlap) if overlap else 0.0, 4),
        "combined_yield_rate_vs_congress": round(combined["combined_count"] / congress_count, 4),
        "house_scored_signal_rate": house_quality.get("scored_signal_rate", 0.0),
        "house_resolved_entity_rate": house_quality.get("resolved_entity_rate", 0.0),
        "house_included_rate": house_quality.get("included_rate", 0.0),
        "house_skip_count": house_quality.get("skipped_count", 0),
        "insider_candidate_count": insider_candidates["candidate_count"],
        "house_candidate_count": house_candidates["candidate_count"],
        "senate_candidate_count": senate_candidates["candidate_count"],
    }

    blocked_reason_counts = overlay["blocked_reason_counts"]
    blocked_outcome_counts = overlay["blocked_outcome_counts"]
    diagnostics = {
        "blocked_reason_counts": blocked_reason_counts,
        "blocked_outcome_counts": blocked_outcome_counts,
        "overlap_subjects": overlay["overlap_subjects"],
        "overlap_details": overlay["overlap_details"][:10],
        "top_house_signal_like_unresolved_issuers": house_quality.get("top_signal_like_unresolved_issuers", []),
        "top_house_non_signal_unresolved_issuers": house_quality.get("top_non_signal_unresolved_issuers", []),
        "top_house_recovered_issuers": house_quality.get("top_recovered_issuers", []),
    }

    primary_constraint = _primary_constraint(summary, quality, blocked_reason_counts)
    readiness = _readiness(primary_constraint, summary, quality)

    assessment = {
        "primary_constraint": primary_constraint,
        "readiness": readiness,
        "recommendation": (
            "focus_on_strategy_validation"
            if readiness == "ready_for_broader_live_strategy_validation"
            else "focus_on_overlap_and_signal_quality_analysis"
        ),
    }

    return {
        "summary": summary,
        "quality": quality,
        "diagnostics": diagnostics,
        "assessment": assessment,
    }


def render_production_confidence_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    quality = report["quality"]
    diagnostics = report["diagnostics"]
    assessment = report["assessment"]
    blocked = diagnostics["blocked_reason_counts"]
    lines = [
        "# Production Confidence Report",
        "",
        "## Summary",
        f"- Insider results: {summary['insider_result_count']}",
        f"- House results: {summary['house_result_count']}",
        f"- Senate results: {summary['senate_result_count']}",
        f"- Congress total results: {summary['congress_result_count']}",
        f"- Overlap subjects: {summary['overlap_subject_count']}",
        f"- Combined overlays: {summary['combined_count']}",
        f"- Blocked overlays: {summary['blocked_count']}",
        "",
        "## Quality",
        f"- Combined yield vs overlap: {quality['combined_yield_rate_vs_overlap']}",
        f"- Combined yield vs congress: {quality['combined_yield_rate_vs_congress']}",
        f"- House scored signal rate: {quality['house_scored_signal_rate']}",
        f"- House resolved entity rate: {quality['house_resolved_entity_rate']}",
        f"- House skip count: {quality['house_skip_count']}",
        f"- Insider candidate backlog: {quality['insider_candidate_count']}",
        f"- House candidate backlog: {quality['house_candidate_count']}",
        f"- Senate candidate backlog: {quality['senate_candidate_count']}",
        "",
        "## Diagnostics",
        f"- Blocked reasons: {blocked}",
        f"- Overlap subjects: {diagnostics['overlap_subjects']}",
        f"- Top House recovered issuers: {diagnostics['top_house_recovered_issuers']}",
        "",
        "## Assessment",
        f"- Primary constraint: {assessment['primary_constraint']}",
        f"- Readiness: {assessment['readiness']}",
        f"- Recommendation: {assessment['recommendation']}",
    ]
    return "\n".join(lines)
