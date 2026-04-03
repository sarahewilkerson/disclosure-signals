from __future__ import annotations

from signals.analysis.production_confidence import (
    build_production_confidence_report,
    render_production_confidence_markdown,
)


def test_build_production_confidence_report_identifies_sparse_overlap():
    result = {
        "insider": {
            "score": {"imported_result_count": 6},
            "candidate_discovery": {"candidate_count": 0, "candidates": []},
        },
        "congress": {
            "house_score": {"imported_result_count": 10},
            "senate_score": {"imported_result_count": 12},
            "imported_result_count": 22,
            "house_quality_metrics": {
                "scored_signal_rate": 0.15,
                "resolved_entity_rate": 0.6,
                "included_rate": 0.2,
                "skipped_count": 4,
                "top_signal_like_unresolved_issuers": [],
                "top_non_signal_unresolved_issuers": [{"issuer_name": "Municipal Bond", "count": 3}],
                "top_recovered_issuers": [{"issuer_name": "Apple Inc.", "count": 2}],
            },
            "house_candidate_discovery": {"candidate_count": 0, "candidates": []},
            "senate_candidate_discovery": {"candidate_count": 0, "candidates": []},
        },
        "combined": {"combined_count": 0, "blocked_count": 18},
        "reports": {
            "overlay_diagnostics": {
                "insider_subject_count": 4,
                "congress_subject_count": 18,
                "overlap_subject_count": 2,
                "overlap_subjects": ["entity:aapl", "entity:msft"],
                "overlap_details": [{"subject_key": "entity:aapl"}],
                "blocked_reason_counts": {"MISSING_COUNTERPART": 18},
                "blocked_outcome_counts": {"SINGLE_SOURCE_ONLY": 18},
            }
        },
    }

    report = build_production_confidence_report(result)

    assert report["summary"]["overlap_subject_count"] == 2
    assert report["quality"]["combined_yield_rate_vs_overlap"] == 0.0
    assert report["assessment"]["primary_constraint"] == "sparse_counterpart_coverage"
    assert report["assessment"]["readiness"] == "pipeline_correct_but_signal_sparse"


def test_build_production_confidence_report_prefers_sparse_counterparts_over_minor_ambiguity():
    result = {
        "insider": {
            "score": {"imported_result_count": 21},
            "candidate_discovery": {"candidate_count": 1, "candidates": []},
        },
        "congress": {
            "house_score": {"imported_result_count": 25},
            "senate_score": {"imported_result_count": 31},
            "imported_result_count": 56,
            "house_quality_metrics": {
                "scored_signal_rate": 0.085,
                "resolved_entity_rate": 0.915,
                "included_rate": 0.1395,
                "skipped_count": 0,
                "top_signal_like_unresolved_issuers": [],
                "top_non_signal_unresolved_issuers": [],
                "top_recovered_issuers": [],
            },
            "house_candidate_discovery": {"candidate_count": 0, "candidates": []},
            "senate_candidate_discovery": {"candidate_count": 0, "candidates": []},
        },
        "combined": {"combined_count": 1, "blocked_count": 49},
        "reports": {
            "overlay_diagnostics": {
                "insider_subject_count": 7,
                "congress_subject_count": 50,
                "overlap_subject_count": 3,
                "overlap_subjects": ["entity:aapl", "entity:amzn", "entity:msft"],
                "overlap_details": [],
                "blocked_reason_counts": {"MISSING_COUNTERPART": 47, "AMBIGUOUS_ENTITY_MATCH": 2},
                "blocked_outcome_counts": {"SINGLE_SOURCE_ONLY": 47, "BLOCKED_AMBIGUOUS": 2},
            }
        },
    }

    report = build_production_confidence_report(result)

    assert report["assessment"]["primary_constraint"] == "sparse_counterpart_coverage"


def test_render_production_confidence_markdown_contains_assessment():
    report = {
        "summary": {
            "insider_result_count": 1,
            "house_result_count": 2,
            "senate_result_count": 3,
            "congress_result_count": 5,
            "combined_count": 1,
            "blocked_count": 4,
            "overlap_subject_count": 2,
        },
        "quality": {
            "combined_yield_rate_vs_overlap": 0.5,
            "combined_yield_rate_vs_congress": 0.2,
            "house_scored_signal_rate": 0.2,
            "house_resolved_entity_rate": 0.7,
            "house_included_rate": 0.2,
            "house_skip_count": 1,
            "insider_candidate_count": 0,
            "house_candidate_count": 0,
            "senate_candidate_count": 0,
        },
        "diagnostics": {
            "blocked_reason_counts": {"MISSING_COUNTERPART": 4},
            "overlap_subjects": ["entity:aapl", "entity:msft"],
            "top_house_recovered_issuers": [{"issuer_name": "Apple Inc.", "count": 2}],
        },
        "assessment": {
            "primary_constraint": "combined_layer_ready_for_strategy_evaluation",
            "readiness": "ready_for_broader_live_strategy_validation",
            "recommendation": "focus_on_strategy_validation",
        },
    }

    markdown = render_production_confidence_markdown(report)

    assert "# Production Confidence Report" in markdown
    assert "Primary constraint: combined_layer_ready_for_strategy_evaluation" in markdown
    assert "Readiness: ready_for_broader_live_strategy_validation" in markdown
