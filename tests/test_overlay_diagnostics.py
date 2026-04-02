from __future__ import annotations

from signals.combined.diagnostics import build_overlay_diagnostics
from signals.core.dto import SignalResult


def _signal(source: str, subject_key: str, label: str, score: float) -> SignalResult:
    return SignalResult(
        source=source,
        scope="entity",
        subject_key=subject_key,
        score=score,
        label=label,
        confidence=0.9,
        as_of_date="2026-04-02",
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


def test_overlay_diagnostics_reports_overlap_and_blocked_reasons():
    insider = [_signal("insider", "entity:aapl", "bullish", 0.4), _signal("insider", "entity:msft", "bearish", -0.2)]
    congress = [_signal("congress", "entity:aapl", "bullish", 0.3), _signal("congress", "entity:amzn", "bullish", 0.5)]
    blocked = [
        {"subject_key": "entity:amzn", "reason_code": "MISSING_COUNTERPART", "overlay_outcome": "SINGLE_SOURCE_ONLY"},
        {"subject_key": "entity:msft", "reason_code": "LOW_RESOLUTION_CONFIDENCE", "overlay_outcome": "BLOCKED_LOW_CONFIDENCE"},
    ]

    payload = build_overlay_diagnostics(insider, congress, blocked)

    assert payload["overlap_subject_count"] == 1
    assert payload["insider_only_count"] == 1
    assert payload["congress_only_count"] == 1
    assert payload["blocked_reason_counts"]["MISSING_COUNTERPART"] == 1
    assert payload["blocked_outcome_counts"]["SINGLE_SOURCE_ONLY"] == 1
    assert payload["overlap_details"][0]["subject_key"] == "entity:aapl"
