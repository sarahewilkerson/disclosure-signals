from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class NormalizedTransaction:
    source: str
    source_record_id: str
    source_filing_id: str
    actor_id: str | None
    actor_name: str | None
    actor_type: str
    owner_type: str
    entity_key: str | None
    instrument_key: str | None
    ticker: str | None
    issuer_name: str | None
    instrument_type: str | None
    transaction_type: str
    direction: str
    execution_date: str | None
    disclosure_date: str | None
    amount_low: float | None
    amount_high: float | None
    amount_estimate: float | None
    currency: str | None
    units_low: float | None
    units_high: float | None
    price_low: float | None
    price_high: float | None
    quality_score: float
    parse_confidence: float | None
    resolution_event_id: str | None
    resolution_confidence: float | None
    resolution_method_version: str
    include_in_signal: bool
    exclusion_reason_code: str | None
    exclusion_reason_detail: str | None
    provenance_payload: dict[str, Any]
    normalization_method_version: str
    run_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SignalResult:
    source: str
    scope: str
    subject_key: str
    score: float
    label: str
    confidence: float
    as_of_date: str
    lookback_window: int
    input_count: int
    included_count: int
    excluded_count: int
    explanation: str
    method_version: str
    code_version: str
    run_id: str
    provenance_refs: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CombinedResult:
    source: str
    scope: str
    subject_key: str
    score: float
    label: str
    confidence: float
    as_of_date: str
    lookback_window: int
    input_count: int
    included_count: int
    excluded_count: int
    explanation: str
    method_version: str
    code_version: str
    run_id: str
    provenance_refs: dict[str, Any]
    overlay_outcome: str | None
    agreement_state: str | None
    conflict_score: float | None
    insider_score: float | None
    congress_score: float | None
    insider_confidence: float | None
    congress_confidence: float | None
    entity_resolution_confidence: float | None
    combine_method_version: str
    do_not_combine_reason_code: str | None
    do_not_combine_reason_detail: str | None
    strength_tier: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EntityResolutionEvent:
    event_id: str
    source: str
    source_record_id: str
    source_filing_id: str
    entity_key: str | None
    instrument_key: str | None
    ticker: str | None
    issuer_name: str | None
    instrument_type: str | None
    resolution_status: str
    resolution_confidence: float
    evidence_payload: dict[str, Any]
    resolution_method_version: str
    run_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CombineEligibilityDecision:
    eligible: bool
    outcome: str
    reason_code: str | None
    reason_detail: str | None
    conflict_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CombinedBlockEvent:
    source: str
    scope: str
    subject_key: str
    lookback_window: int
    run_id: str
    overlay_outcome: str
    reason_code: str
    reason_detail: str | None
    insider_result_ref: dict[str, Any] | None
    congress_result_ref: dict[str, Any] | None
    insider_resolution_event_id: str | None
    congress_resolution_event_id: str | None
    combine_method_version: str
    conflict_score: float | None
    provenance_refs: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
