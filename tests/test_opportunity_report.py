from __future__ import annotations

from signals.analysis.opportunity_report import (
    build_overlap_opportunity_report,
    render_overlap_opportunity_markdown,
)
from signals.core.dto import SignalResult


def _row(source: str, subject: str, label: str, score: float, confidence: float = 0.5) -> SignalResult:
    return SignalResult(
        source=source,
        scope="entity",
        subject_key=subject,
        score=score,
        label=label,
        confidence=confidence,
        as_of_date="2026-04-03",
        lookback_window=90,
        input_count=1,
        included_count=1,
        excluded_count=0,
        explanation="test",
        method_version="test",
        code_version="test",
        run_id=f"{source}-run",
        provenance_refs={},
    )


def test_build_overlap_opportunity_report_buckets_overlap_subjects():
    insider = [
        _row("insider", "entity:aapl", "insufficient", 0.0),
        _row("insider", "entity:amzn", "neutral", -0.06),
        _row("insider", "entity:msft", "bullish", 0.3),
    ]
    congress = [
        _row("congress", "entity:aapl", "bearish", -1.0),
        _row("congress", "entity:amzn", "bullish", 1.0),
        _row("congress", "entity:msft", "bullish", 0.8),
    ]
    combined = [
        {"subject_key": "entity:msft", "overlay_outcome": "ALIGNED_BULLISH", "label": "bullish", "score": 0.55, "confidence": 0.7},
    ]
    blocked = [
        {"subject_key": "entity:aapl", "reason_code": "AMBIGUOUS_ENTITY_MATCH"},
    ]

    report = build_overlap_opportunity_report(insider, congress, combined, blocked)

    assert report["overlap_subject_count"] == 3
    assert report["aligned_count"] == 1
    assert report["blocked_ambiguity_count"] == 1
    assert report["neutral_or_mixed_count"] == 1


def test_render_overlap_opportunity_markdown_contains_summary():
    markdown = render_overlap_opportunity_markdown(
        {
            "overlap_subject_count": 3,
            "aligned_count": 1,
            "conflict_count": 0,
            "low_confidence_count": 0,
            "insider_insufficient_count": 1,
            "neutral_or_mixed_count": 1,
            "blocked_ambiguity_count": 1,
            "insider_insufficient_subjects": [{"subject_key": "entity:aapl"}],
            "neutral_or_mixed_subjects": [{"subject_key": "entity:amzn"}],
            "blocked_ambiguity_subjects": [{"subject_key": "entity:msft"}],
        }
    )

    assert "# Overlap Opportunity Report" in markdown
    assert "Overlap subjects: 3" in markdown
    assert "Insider-insufficient subjects: 1" in markdown
