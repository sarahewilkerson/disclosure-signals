from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

from signals.core.dto import SignalResult


_DEFAULT_OWNER_WEIGHTS = {
    "self": 1.0,
    "spouse": 0.8,
    "joint": 0.9,
    "dependent": 0.5,
    "managed": 0.0,
}


@dataclass
class ScoredTransaction:
    member_id: str
    ticker: str | None
    transaction_type: str
    execution_date: datetime | None
    amount_min: int | None
    amount_max: int | None
    owner_type: str
    base_value: float
    direction: float
    staleness_penalty: float
    owner_weight: float
    resolution_confidence: float
    signal_weight: float
    raw_score: float
    final_score: float


@dataclass
class AggregateResult:
    breadth_pct: float
    unique_members: int
    buyers: int
    sellers: int
    neutral: int
    volume_net: float
    volume_buy: float
    volume_sell: float
    concentration_top5: float
    is_concentrated: bool
    members_capped: int
    mean_staleness: float
    transactions_included: int
    transactions_excluded: int


def get_owner_weight(owner_type: str) -> float:
    return _DEFAULT_OWNER_WEIGHTS.get(owner_type, 0.3)


STALENESS_HALF_LIFE_DAYS = 60


def staleness_penalty(execution_date: datetime | None, reference_date: datetime) -> float:
    if execution_date is None:
        return 0.5
    lag_days = (reference_date - execution_date).days
    if lag_days < 0:
        return 0.9
    return math.exp(-0.693 * lag_days / STALENESS_HALF_LIFE_DAYS)


def disclosure_lag_penalty(execution_date: datetime | None, disclosure_date: datetime | None) -> float:
    if execution_date is None or disclosure_date is None:
        return 0.7
    lag_days = (disclosure_date - execution_date).days
    if lag_days <= 30:
        return 1.0
    if lag_days <= 60:
        return 0.85
    if lag_days <= 120:
        return 0.6
    return 0.3


def estimate_amount(amount_min: int | None, amount_max: int | None, method: str = "geometric_mean") -> float:
    if amount_min is None or amount_max is None or amount_min <= 0 or amount_max <= 0:
        return 0.0
    if method == "lower_bound":
        return float(amount_min)
    if method == "midpoint":
        return (amount_min + amount_max) / 2.0
    if method == "log_uniform_ev":
        if amount_max == amount_min:
            return float(amount_min)
        return (amount_max - amount_min) / math.log(amount_max / amount_min)
    return math.sqrt(amount_min * amount_max)


def score_transaction(
    *,
    member_id: str,
    ticker: str | None,
    transaction_type: str,
    execution_date: datetime | None,
    amount_min: int | None,
    amount_max: int | None,
    owner_type: str,
    resolution_confidence: float,
    signal_weight: float,
    reference_date: datetime,
    disclosure_date: datetime | None = None,
    amount_method: str = "geometric_mean",
    use_log_scaling: bool = False,
) -> ScoredTransaction:
    base_value = estimate_amount(amount_min, amount_max, amount_method)
    if use_log_scaling and base_value > 0:
        base_value = math.log(1 + base_value)
    if transaction_type == "purchase":
        direction = 1.0
    elif transaction_type in {"sale", "sale_partial"}:
        direction = -1.0
    else:
        direction = 0.0
    stale = staleness_penalty(execution_date, reference_date)
    owner_weight = get_owner_weight(owner_type)
    lag_penalty = disclosure_lag_penalty(execution_date, disclosure_date)
    raw_score = base_value * direction * stale * owner_weight
    final_score = raw_score * resolution_confidence * signal_weight * lag_penalty
    return ScoredTransaction(
        member_id=member_id,
        ticker=ticker,
        transaction_type=transaction_type,
        execution_date=execution_date,
        amount_min=amount_min,
        amount_max=amount_max,
        owner_type=owner_type,
        base_value=base_value,
        direction=direction,
        staleness_penalty=stale,
        owner_weight=owner_weight,
        resolution_confidence=resolution_confidence,
        signal_weight=signal_weight,
        raw_score=raw_score,
        final_score=final_score,
    )


def winsorize_transactions(scored: list[ScoredTransaction], percentile: float = 0.95) -> list[ScoredTransaction]:
    if not scored:
        return scored
    abs_scores = sorted(abs(t.final_score) for t in scored if t.final_score != 0)
    if not abs_scores:
        return scored
    idx = min(int(len(abs_scores) * percentile), len(abs_scores) - 1)
    threshold = abs_scores[idx]
    result = []
    for t in scored:
        if abs(t.final_score) > threshold:
            sign = 1 if t.final_score > 0 else -1
            result.append(
                ScoredTransaction(
                    member_id=t.member_id,
                    ticker=t.ticker,
                    transaction_type=t.transaction_type,
                    execution_date=t.execution_date,
                    amount_min=t.amount_min,
                    amount_max=t.amount_max,
                    owner_type=t.owner_type,
                    base_value=t.base_value,
                    direction=t.direction,
                    staleness_penalty=t.staleness_penalty,
                    owner_weight=t.owner_weight,
                    resolution_confidence=t.resolution_confidence,
                    signal_weight=t.signal_weight,
                    raw_score=t.raw_score,
                    final_score=sign * threshold,
                )
            )
        else:
            result.append(t)
    return result


def compute_aggregate(
    scored_transactions: list[ScoredTransaction],
    member_cap_pct: float = 0.05,
    winsorize_pct: float = 0.95,
) -> AggregateResult:
    if not scored_transactions:
        return AggregateResult(0.0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, False, 0, 0.0, 0, 0)
    winsorized = winsorize_transactions(scored_transactions, winsorize_pct)
    by_member: dict[str, list[ScoredTransaction]] = {}
    for t in winsorized:
        by_member.setdefault(t.member_id, []).append(t)
    member_raw_scores = {member: sum(t.final_score for t in txns) for member, txns in by_member.items()}
    total_abs = sum(abs(s) for s in member_raw_scores.values())
    member_scores: dict[str, float] = {}
    members_capped = 0
    if total_abs > 0:
        max_contribution = total_abs * member_cap_pct
        for member, raw_score in member_raw_scores.items():
            if abs(raw_score) > max_contribution:
                member_scores[member] = (1 if raw_score > 0 else -1) * max_contribution
                members_capped += 1
            else:
                member_scores[member] = raw_score
    else:
        member_scores = member_raw_scores
    buyers = sum(1 for s in member_scores.values() if s > 0)
    sellers = sum(1 for s in member_scores.values() if s < 0)
    neutral = sum(1 for s in member_scores.values() if s == 0)
    unique_members = len(member_scores)
    breadth_pct = (buyers - sellers) / unique_members if unique_members else 0.0
    volume_net = sum(member_scores.values())
    volume_buy = sum(s for s in member_scores.values() if s > 0)
    volume_sell = abs(sum(s for s in member_scores.values() if s < 0))
    sorted_abs = sorted((abs(s) for s in member_scores.values()), reverse=True)
    top_5_abs = sum(sorted_abs[:5]) if len(sorted_abs) >= 5 else sum(sorted_abs)
    total_abs_capped = sum(abs(s) for s in member_scores.values())
    concentration_top5 = (top_5_abs / total_abs_capped) if total_abs_capped else 0.0
    mean_staleness = sum(t.staleness_penalty for t in winsorized) / len(winsorized) if winsorized else 0.0
    return AggregateResult(
        breadth_pct=breadth_pct,
        unique_members=unique_members,
        buyers=buyers,
        sellers=sellers,
        neutral=neutral,
        volume_net=volume_net,
        volume_buy=volume_buy,
        volume_sell=volume_sell,
        concentration_top5=concentration_top5,
        is_concentrated=concentration_top5 > 0.5,
        members_capped=members_capped,
        mean_staleness=mean_staleness,
        transactions_included=len(winsorized),
        transactions_excluded=0,
    )


def compute_confidence_score(aggregate: AggregateResult, resolution_rate: float, chamber_balance: float = 0.5) -> dict:
    factors = {
        "member_coverage": min(1.0, aggregate.unique_members / 50),
        "transaction_volume": min(1.0, aggregate.transactions_included / 200),
        "resolution_quality": resolution_rate,
        "timeliness": aggregate.mean_staleness,
        "balance": 1.0 - abs(chamber_balance - 0.5) * 2,
        "concentration": 1.0 - aggregate.concentration_top5,
    }
    weights = {
        "member_coverage": 0.25,
        "resolution_quality": 0.20,
        "timeliness": 0.20,
        "concentration": 0.15,
        "transaction_volume": 0.10,
        "balance": 0.10,
    }
    composite = sum(factors[k] * weights[k] for k in weights)
    tier = "HIGH" if composite > 0.7 else "MODERATE" if composite > 0.4 else "LOW"
    return {"composite_score": composite, "tier": tier, "factors": factors, "weights": weights}


MIN_TRANSACTIONS_FOR_SIGNAL = 2


def label_from_score(score: float, confidence: float, transaction_count: int = 0) -> str:
    if confidence < 0.25 or transaction_count < MIN_TRANSACTIONS_FOR_SIGNAL:
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
        label=label_from_score(score, confidence, included_count),
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
