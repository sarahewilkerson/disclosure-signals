from __future__ import annotations

import re
import uuid

from signals.core.dto import CombineEligibilityDecision, EntityResolutionEvent, SignalResult
from signals.core.enums import OverlayOutcome, ReasonCode, ResolutionStatus
from signals.core.versioning import RESOLUTION_METHOD_VERSION


CANONICAL_BY_TICKER = {
    "AAPL": {
        "entity_key": "entity:apple",
        "instrument_key": "instrument:aapl:common",
        "ticker": "AAPL",
        "issuer_name": "Apple Inc.",
    },
    "MSFT": {
        "entity_key": "entity:microsoft",
        "instrument_key": "instrument:msft:common",
        "ticker": "MSFT",
        "issuer_name": "Microsoft Corporation",
    },
    "AMZN": {
        "entity_key": "entity:amazon",
        "instrument_key": "instrument:amzn:common",
        "ticker": "AMZN",
        "issuer_name": "Amazon.com Inc.",
    },
}

CANONICAL_BY_CIK = {
    "0000320193": CANONICAL_BY_TICKER["AAPL"],
    "0000789019": CANONICAL_BY_TICKER["MSFT"],
    "0001018724": CANONICAL_BY_TICKER["AMZN"],
}

NAME_ALIASES = {
    "apple inc": [CANONICAL_BY_TICKER["AAPL"]],
    "apple inc.": [CANONICAL_BY_TICKER["AAPL"]],
    "microsoft corporation": [CANONICAL_BY_TICKER["MSFT"]],
    "amazon.com inc.": [CANONICAL_BY_TICKER["AMZN"]],
}


def _normalize_name(name: str | None) -> str | None:
    if not name:
        return None
    normalized = re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()
    return normalized or None


def resolve_entity(
    *,
    source: str,
    source_record_id: str,
    source_filing_id: str,
    ticker: str | None,
    cik: str | None,
    issuer_name: str | None,
    instrument_type: str | None,
    run_id: str,
) -> EntityResolutionEvent:
    candidate = None
    confidence = 0.0
    status = ResolutionStatus.UNRESOLVED.value
    evidence: dict = {
        "inputs": {
            "ticker": ticker,
            "cik": cik,
            "issuer_name": issuer_name,
            "instrument_type": instrument_type,
        }
    }

    if ticker and ticker.upper() in CANONICAL_BY_TICKER:
        candidate = CANONICAL_BY_TICKER[ticker.upper()]
        confidence = 0.99
        status = ResolutionStatus.RESOLVED.value
        evidence["match_type"] = "ticker"
    elif cik and cik in CANONICAL_BY_CIK:
        candidate = CANONICAL_BY_CIK[cik]
        confidence = 0.97
        status = ResolutionStatus.RESOLVED.value
        evidence["match_type"] = "cik"
    else:
        normalized_name = _normalize_name(issuer_name)
        matches = NAME_ALIASES.get(normalized_name or "", [])
        if len(matches) == 1:
            candidate = matches[0]
            confidence = 0.90
            status = ResolutionStatus.RESOLVED.value
            evidence["match_type"] = "name"
        elif len(matches) > 1:
            status = ResolutionStatus.AMBIGUOUS.value
            confidence = 0.40
            evidence["match_type"] = "ambiguous_name"
            evidence["candidate_entity_keys"] = [item["entity_key"] for item in matches]
        else:
            status = ResolutionStatus.UNRESOLVED.value
            confidence = 0.0
            evidence["match_type"] = "none"

    return EntityResolutionEvent(
        event_id=str(uuid.uuid4()),
        source=source,
        source_record_id=source_record_id,
        source_filing_id=source_filing_id,
        entity_key=candidate["entity_key"] if candidate else None,
        instrument_key=candidate["instrument_key"] if candidate else None,
        ticker=candidate["ticker"] if candidate else ticker,
        issuer_name=candidate["issuer_name"] if candidate else issuer_name,
        instrument_type=instrument_type,
        resolution_status=status,
        resolution_confidence=confidence,
        evidence_payload=evidence,
        resolution_method_version=RESOLUTION_METHOD_VERSION,
        run_id=run_id,
    )


def make_eligibility_decision(
    insider_event: EntityResolutionEvent | None,
    congress_event: EntityResolutionEvent | None,
    insider_result: SignalResult | None,
    congress_result: SignalResult,
    *,
    resolution_threshold: float = 0.8,
    conflict_confidence_threshold: float = 0.5,
) -> CombineEligibilityDecision:
    if congress_event is None:
        return CombineEligibilityDecision(
            eligible=False,
            outcome=OverlayOutcome.BLOCKED_AMBIGUOUS.value,
            reason_code=ReasonCode.AMBIGUOUS_ENTITY_MATCH.value,
            reason_detail="Congress result could not be resolved to a canonical entity",
            conflict_score=0.0,
        )

    if insider_result is None or insider_event is None:
        return CombineEligibilityDecision(
            eligible=False,
            outcome=OverlayOutcome.SINGLE_SOURCE_ONLY.value,
            reason_code=ReasonCode.MISSING_COUNTERPART.value,
            reason_detail="No insider counterpart in derived results",
            conflict_score=0.0,
        )

    if insider_event.resolution_status != ResolutionStatus.RESOLVED.value or congress_event.resolution_status != ResolutionStatus.RESOLVED.value:
        return CombineEligibilityDecision(
            eligible=False,
            outcome=OverlayOutcome.BLOCKED_AMBIGUOUS.value,
            reason_code=ReasonCode.AMBIGUOUS_ENTITY_MATCH.value,
            reason_detail="At least one source did not resolve cleanly",
            conflict_score=0.0,
        )

    if insider_event.entity_key != congress_event.entity_key:
        return CombineEligibilityDecision(
            eligible=False,
            outcome=OverlayOutcome.BLOCKED_AMBIGUOUS.value,
            reason_code=ReasonCode.AMBIGUOUS_ENTITY_MATCH.value,
            reason_detail="Resolved entity keys do not align",
            conflict_score=0.0,
        )

    if insider_event.instrument_key and congress_event.instrument_key and insider_event.instrument_key != congress_event.instrument_key:
        return CombineEligibilityDecision(
            eligible=False,
            outcome=OverlayOutcome.BLOCKED_INSTRUMENT_MISMATCH.value,
            reason_code=ReasonCode.INSTRUMENT_MISMATCH.value,
            reason_detail="Resolved instruments do not match",
            conflict_score=0.0,
        )

    confidence = min(
        insider_event.resolution_confidence,
        congress_event.resolution_confidence,
    )
    if confidence < resolution_threshold:
        return CombineEligibilityDecision(
            eligible=False,
            outcome=OverlayOutcome.BLOCKED_LOW_CONFIDENCE.value,
            reason_code=ReasonCode.LOW_RESOLUTION_CONFIDENCE.value,
            reason_detail=f"resolution confidence {confidence:.2f} below threshold",
            conflict_score=0.0,
        )

    labels = {insider_result.label, congress_result.label}
    min_signal_conf = min(insider_result.confidence, congress_result.confidence)
    if insider_result.label == congress_result.label == "bullish":
        outcome = OverlayOutcome.ALIGNED_BULLISH.value if min_signal_conf >= conflict_confidence_threshold else OverlayOutcome.LOW_CONFIDENCE_ALIGNMENT.value
        return CombineEligibilityDecision(True, outcome, None, None, 0.0)
    if insider_result.label == congress_result.label == "bearish":
        outcome = OverlayOutcome.ALIGNED_BEARISH.value if min_signal_conf >= conflict_confidence_threshold else OverlayOutcome.LOW_CONFIDENCE_ALIGNMENT.value
        return CombineEligibilityDecision(True, outcome, None, None, 0.0)

    if labels == {"bullish", "bearish"} and min_signal_conf >= conflict_confidence_threshold:
        conflict_score = min(
            1.0,
            (abs(insider_result.score - congress_result.score) / 2.0) * min_signal_conf,
        )
        return CombineEligibilityDecision(
            eligible=True,
            outcome=OverlayOutcome.TRUE_CONFLICT.value,
            reason_code=None,
            reason_detail=None,
            conflict_score=conflict_score,
        )

    return CombineEligibilityDecision(
        eligible=True,
        outcome=OverlayOutcome.LOW_CONFIDENCE_ALIGNMENT.value,
        reason_code=None,
        reason_detail=None,
        conflict_score=0.0,
    )
