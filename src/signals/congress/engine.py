from __future__ import annotations

from signals.core.dto import SignalResult
from signals.core.enums import OverlayOutcome


def label_from_score(score: float, confidence: float) -> str:
    if confidence < 0.25:
        return "insufficient"
    if score > 0.05:
        return "bullish"
    if score < -0.05:
        return "bearish"
    return "neutral"


def compute_entity_signal(
    *,
    subject_key: str,
    score: float,
    confidence: float,
    as_of_date: str,
    lookback_window: int,
    input_count: int,
    included_count: int,
    excluded_count: int,
    explanation: str,
    method_version: str,
    code_version: str,
    run_id: str,
    provenance_refs: dict,
) -> SignalResult:
    return SignalResult(
        source="congress",
        scope="entity",
        subject_key=subject_key,
        score=float(score),
        label=label_from_score(score, confidence),
        confidence=float(confidence),
        as_of_date=as_of_date,
        lookback_window=lookback_window,
        input_count=input_count,
        included_count=included_count,
        excluded_count=excluded_count,
        explanation=explanation,
        method_version=method_version,
        code_version=code_version,
        run_id=run_id,
        provenance_refs=provenance_refs,
    )
