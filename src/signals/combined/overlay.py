from __future__ import annotations

import hashlib

from signals.core.dto import CombinedBlockEvent, CombinedResult, EntityResolutionEvent, SignalResult
from signals.core.enums import OverlayOutcome, ResolutionStatus
from signals.core.resolution import make_eligibility_decision
from signals.core.versioning import COMBINE_METHOD_VERSION


def _classify_strength(insider_confidence: float, congress_confidence: float, score_magnitude: float) -> str:
    if insider_confidence >= 0.7 and congress_confidence >= 0.7 and score_magnitude > 0.3:
        return "strong"
    if insider_confidence >= 0.4 and congress_confidence >= 0.4:
        return "moderate"
    return "weak"


def _fallback_resolution_event(row: SignalResult | None) -> EntityResolutionEvent | None:
    if row is None:
        return None
    if row.subject_key.startswith("entity:"):
        ticker = row.subject_key.split(":", 1)[1].upper()
        return EntityResolutionEvent(
            event_id=f"synthetic:{row.run_id}:{row.subject_key}",
            source=row.source,
            source_record_id=row.subject_key,
            source_filing_id=row.run_id,
            entity_key=row.subject_key,
            instrument_key=None,
            ticker=ticker,
            issuer_name=None,
            instrument_type=None,
            resolution_status=ResolutionStatus.RESOLVED.value,
            resolution_confidence=1.0,
            evidence_payload={"synthetic": True, "reason": "subject_key_entity_fallback"},
            resolution_method_version=COMBINE_METHOD_VERSION,
            run_id=row.run_id,
        )
    return None


def build_overlay(
    insider_results: list[SignalResult],
    congress_results: list[SignalResult],
    resolution_events: dict[str, EntityResolutionEvent],
    run_id: str,
    resolution_threshold: float = 0.8,
    lookback_window: int = 90,
) -> tuple[list[CombinedResult], list[CombinedBlockEvent]]:
    insider_by_key = {row.subject_key: row for row in insider_results}
    combined: list[CombinedResult] = []
    blocked: list[CombinedBlockEvent] = []

    for congress_row in congress_results:
        match = insider_by_key.get(congress_row.subject_key)
        congress_event_id = (congress_row.provenance_refs.get("resolution_event_ids") or [None])[0]
        insider_event_id = (match.provenance_refs.get("resolution_event_ids") or [None])[0] if match else None
        congress_event = resolution_events.get(congress_event_id) if congress_event_id else _fallback_resolution_event(congress_row)
        insider_event = resolution_events.get(insider_event_id) if insider_event_id else _fallback_resolution_event(match)
        decision = make_eligibility_decision(
            insider_event,
            congress_event,
            match,
            congress_row,
            resolution_threshold=resolution_threshold,
        )
        if not decision.eligible:
            blocked.append(
                CombinedBlockEvent(
                    source="combined",
                    scope="entity",
                    subject_key=congress_row.subject_key,
                    lookback_window=lookback_window,
                    run_id=run_id,
                    overlay_outcome=decision.outcome,
                    reason_code=decision.reason_code or "UNKNOWN",
                    reason_detail=decision.reason_detail,
                    insider_result_ref=None if match is None else {
                        "run_id": match.run_id,
                        "subject_key": match.subject_key,
                        "method_version": match.method_version,
                    },
                    congress_result_ref={
                        "run_id": congress_row.run_id,
                        "subject_key": congress_row.subject_key,
                        "method_version": congress_row.method_version,
                    },
                    insider_resolution_event_id=insider_event_id,
                    congress_resolution_event_id=congress_event_id,
                    combine_method_version=COMBINE_METHOD_VERSION,
                    conflict_score=decision.conflict_score,
                    provenance_refs={
                        "insider_result_run_id": match.run_id if match else None,
                        "congress_result_run_id": congress_row.run_id,
                    },
                )
            )
            continue

        resolution_conf = min(insider_event.resolution_confidence if insider_event else 0.0, congress_event.resolution_confidence)

        net_score = (match.score + congress_row.score) / 2
        strength_tier = _classify_strength(match.confidence, congress_row.confidence, abs(net_score))
        if decision.outcome == OverlayOutcome.ALIGNED_BULLISH.value:
            state = decision.outcome
            label = "bullish"
        elif decision.outcome == OverlayOutcome.ALIGNED_BEARISH.value:
            state = decision.outcome
            label = "bearish"
        elif decision.outcome == OverlayOutcome.TRUE_CONFLICT.value:
            state = decision.outcome
            label = "mixed"
        else:
            state = decision.outcome
            label = "mixed"

        combined.append(
            CombinedResult(
                source="combined",
                scope="entity",
                subject_key=congress_row.subject_key,
                score=net_score,
                label=label,
                confidence=resolution_conf,
                as_of_date=max(match.as_of_date, congress_row.as_of_date),
                lookback_window=lookback_window,
                input_count=match.input_count + congress_row.input_count,
                included_count=match.included_count + congress_row.included_count,
                excluded_count=match.excluded_count + congress_row.excluded_count,
                explanation=f"Overlay from insider={match.label} and congress={congress_row.label}",
                method_version=COMBINE_METHOD_VERSION,
                code_version=match.code_version if match.code_version == congress_row.code_version else f"{match.code_version}+{congress_row.code_version}",
                run_id=run_id,
                provenance_refs={
                    "insider_run_id": match.run_id,
                    "congress_run_id": congress_row.run_id,
                    "insider_resolution_event_id": insider_event_id,
                    "congress_resolution_event_id": congress_event_id,
                },
                overlay_outcome=decision.outcome,
                agreement_state=state,
                conflict_score=decision.conflict_score,
                insider_score=match.score,
                congress_score=congress_row.score,
                insider_confidence=match.confidence,
                congress_confidence=congress_row.confidence,
                entity_resolution_confidence=resolution_conf,
                combine_method_version=COMBINE_METHOD_VERSION,
                do_not_combine_reason_code=None,
                do_not_combine_reason_detail=None,
                strength_tier=strength_tier,
            )
        )

    return combined, blocked


def fingerprint_for_combined(row: CombinedResult) -> str:
    basis = "|".join(
        [
            row.subject_key,
            row.as_of_date,
            row.combine_method_version,
            str(row.insider_score),
            str(row.congress_score),
        ]
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()
