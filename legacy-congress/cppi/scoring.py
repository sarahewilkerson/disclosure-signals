"""Scoring engine for Congressional Positioning Index.

Implements:
- Three-timestamp model with staleness penalties
- Amount estimation (geometric mean default)
- Owner type weights
- Anti-dominance controls (member cap, winsorization)
- Aggregate positioning calculation (breadth + volume signals)
"""

import logging
import math
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from cppi import config

logger = logging.getLogger(__name__)


def get_owner_weight(owner_type: str) -> float:
    """Get weight for owner type from config."""
    weights = getattr(config, "OWNER_WEIGHTS", _DEFAULT_OWNER_WEIGHTS)
    default = getattr(config, "OWNER_WEIGHT_DEFAULT", 0.3)
    return weights.get(owner_type, default)


class AmountMethod(Enum):
    """Methods for estimating amount from range."""

    LOWER_BOUND = "lower_bound"
    MIDPOINT = "midpoint"
    GEOMETRIC_MEAN = "geometric_mean"
    LOG_UNIFORM_EV = "log_uniform_ev"


# Owner type weights - imported from config for tunability
# Local fallback for when config is not available (e.g., testing)
_DEFAULT_OWNER_WEIGHTS = {
    "self": 1.0,
    "spouse": 0.8,
    "joint": 0.9,
    "dependent": 0.5,
    "managed": 0.3,
}


@dataclass
class ScoredTransaction:
    """A transaction with computed score."""

    member_id: str
    ticker: Optional[str]
    transaction_type: str  # 'purchase', 'sale', 'sale_partial', 'exchange'
    execution_date: Optional[datetime]
    amount_min: Optional[int]
    amount_max: Optional[int]
    owner_type: str
    base_value: float
    direction: float
    staleness_penalty: float
    owner_weight: float
    resolution_confidence: float
    signal_weight: float
    raw_score: float
    final_score: float  # After all adjustments


@dataclass
class AggregateResult:
    """Aggregate positioning result."""

    # Breadth signal
    breadth_pct: float  # (buyers - sellers) / total as percentage
    unique_members: int
    buyers: int
    sellers: int
    neutral: int

    # Volume signal
    volume_net: float
    volume_buy: float
    volume_sell: float

    # Concentration
    concentration_top5: float
    is_concentrated: bool  # True if top 5 > 50%
    members_capped: int  # How many members hit the cap

    # Quality metrics
    mean_staleness: float
    transactions_included: int
    transactions_excluded: int


def staleness_penalty(
    execution_date: Optional[datetime],
    reference_date: datetime,
) -> float:
    """Calculate staleness penalty based on lag from execution to reference.

    Uses STALENESS_PENALTIES from config for tunability.

    Args:
        execution_date: When the trade was executed
        reference_date: Reference date for scoring (usually "now")

    Returns:
        Penalty multiplier from 0.2 to 1.0
    """
    if execution_date is None:
        return 0.5  # Moderate penalty for unknown dates

    lag_days = (reference_date - execution_date).days

    if lag_days < 0:
        # Future date - likely data error, minimal penalty
        return 0.9

    # Get penalties from config
    penalties = getattr(config, "STALENESS_PENALTIES", {45: 1.0, 60: 0.9, 90: 0.7, 180: 0.4})
    default = getattr(config, "STALENESS_DEFAULT", 0.2)

    # Find applicable penalty based on lag days
    for threshold in sorted(penalties.keys()):
        if lag_days <= threshold:
            return penalties[threshold]

    return default


def estimate_amount(
    amount_min: Optional[int],
    amount_max: Optional[int],
    method: str = "geometric_mean",
) -> float:
    """Estimate transaction amount from range bounds.

    Args:
        amount_min: Lower bound of range
        amount_max: Upper bound of range
        method: Estimation method (geometric_mean, midpoint, lower_bound, log_uniform_ev)

    Returns:
        Estimated amount value
    """
    if amount_min is None or amount_max is None:
        return 0.0

    if amount_min <= 0 or amount_max <= 0:
        return 0.0

    if method == "lower_bound":
        return float(amount_min)
    elif method == "midpoint":
        return (amount_min + amount_max) / 2.0
    elif method == "geometric_mean":
        return math.sqrt(amount_min * amount_max)
    elif method == "log_uniform_ev":
        # Expected value assuming uniform distribution in log-space
        if amount_max == amount_min:
            return float(amount_min)
        return (amount_max - amount_min) / math.log(amount_max / amount_min)
    else:
        # Default to geometric mean
        return math.sqrt(amount_min * amount_max)


def score_transaction(
    member_id: str,
    ticker: Optional[str],
    transaction_type: str,
    execution_date: Optional[datetime],
    amount_min: Optional[int],
    amount_max: Optional[int],
    owner_type: str,
    resolution_confidence: float,
    signal_weight: float,
    reference_date: datetime,
    amount_method: str = "geometric_mean",
    use_log_scaling: bool = False,
) -> ScoredTransaction:
    """Score a single transaction.

    Args:
        member_id: Identifier for the member
        ticker: Resolved ticker (if any)
        transaction_type: Type of transaction
        execution_date: When trade was executed
        amount_min: Lower bound of amount range
        amount_max: Upper bound of amount range
        owner_type: Owner type (self, spouse, etc.)
        resolution_confidence: Confidence in entity resolution
        signal_weight: Relevance weight for asset class
        reference_date: Reference date for staleness calculation
        amount_method: Method for amount estimation
        use_log_scaling: Whether to apply log scaling

    Returns:
        ScoredTransaction with all scoring components
    """
    # Base value
    base_value = estimate_amount(amount_min, amount_max, amount_method)

    # Apply log scaling if enabled
    if use_log_scaling and base_value > 0:
        base_value = math.log(1 + base_value)

    # Direction
    if transaction_type in ("purchase",):
        direction = 1.0
    elif transaction_type in ("sale", "sale_partial"):
        direction = -1.0
    else:  # exchange, unknown
        direction = 0.0

    # Staleness penalty
    stale_penalty = staleness_penalty(execution_date, reference_date)

    # Owner weight (from config)
    owner_weight = get_owner_weight(owner_type)

    # Raw score before confidence adjustments
    raw_score = base_value * direction * stale_penalty * owner_weight

    # Final score with confidence factors
    final_score = raw_score * resolution_confidence * signal_weight

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
        staleness_penalty=stale_penalty,
        owner_weight=owner_weight,
        resolution_confidence=resolution_confidence,
        signal_weight=signal_weight,
        raw_score=raw_score,
        final_score=final_score,
    )


def winsorize_transactions(
    scored: list[ScoredTransaction],
    percentile: float = 0.95,
) -> list[ScoredTransaction]:
    """Clip transaction scores at the specified percentile.

    Args:
        scored: List of scored transactions
        percentile: Percentile to clip at (default 95th)

    Returns:
        List of transactions with clipped scores
    """
    if not scored:
        return scored

    # Get absolute values for percentile calculation
    abs_scores = sorted(abs(t.final_score) for t in scored if t.final_score != 0)
    if not abs_scores:
        return scored

    # Find percentile threshold
    idx = int(len(abs_scores) * percentile)
    idx = min(idx, len(abs_scores) - 1)
    threshold = abs_scores[idx]

    # Clip scores
    result = []
    for t in scored:
        if abs(t.final_score) > threshold:
            # Clip to threshold, preserving sign
            sign = 1 if t.final_score > 0 else -1
            clipped = ScoredTransaction(
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
            result.append(clipped)
        else:
            result.append(t)

    return result


def compute_aggregate(
    scored_transactions: list[ScoredTransaction],
    member_cap_pct: Optional[float] = None,
    winsorize_pct: Optional[float] = None,
) -> AggregateResult:
    """Compute aggregate positioning from scored transactions.

    Args:
        scored_transactions: List of scored transactions
        member_cap_pct: Maximum percentage any single member can contribute
        winsorize_pct: Percentile for winsorization

    Returns:
        AggregateResult with breadth, volume, and quality metrics
    """
    # Use config defaults if not specified
    if member_cap_pct is None:
        member_cap_pct = getattr(config, "MEMBER_CAP_PCT", 0.05)
    if winsorize_pct is None:
        winsorize_pct = getattr(config, "WINSORIZE_PERCENTILE", 0.95)

    if not scored_transactions:
        return AggregateResult(
            breadth_pct=0.0,
            unique_members=0,
            buyers=0,
            sellers=0,
            neutral=0,
            volume_net=0.0,
            volume_buy=0.0,
            volume_sell=0.0,
            concentration_top5=0.0,
            is_concentrated=False,
            members_capped=0,
            mean_staleness=0.0,
            transactions_included=0,
            transactions_excluded=0,
        )

    # Apply winsorization first
    winsorized = winsorize_transactions(scored_transactions, winsorize_pct)

    # Group by member
    by_member: dict[str, list[ScoredTransaction]] = {}
    for t in winsorized:
        if t.member_id not in by_member:
            by_member[t.member_id] = []
        by_member[t.member_id].append(t)

    # Calculate raw member scores
    member_raw_scores: dict[str, float] = {}
    for member, txns in by_member.items():
        member_raw_scores[member] = sum(t.final_score for t in txns)

    # Calculate total absolute score for capping
    total_abs = sum(abs(s) for s in member_raw_scores.values())

    # Apply member cap
    member_scores: dict[str, float] = {}
    members_capped = 0

    if total_abs > 0:
        max_contribution = total_abs * member_cap_pct
        for member, raw_score in member_raw_scores.items():
            if abs(raw_score) > max_contribution:
                # Clip to max, preserving sign
                sign = 1 if raw_score > 0 else -1
                member_scores[member] = sign * max_contribution
                members_capped += 1
            else:
                member_scores[member] = raw_score
    else:
        member_scores = member_raw_scores

    # Breadth signal
    buyers = sum(1 for s in member_scores.values() if s > 0)
    sellers = sum(1 for s in member_scores.values() if s < 0)
    neutral = sum(1 for s in member_scores.values() if s == 0)
    unique_members = len(member_scores)

    if unique_members > 0:
        breadth_pct = (buyers - sellers) / unique_members
    else:
        breadth_pct = 0.0

    # Volume signal
    volume_net = sum(member_scores.values())
    volume_buy = sum(s for s in member_scores.values() if s > 0)
    volume_sell = abs(sum(s for s in member_scores.values() if s < 0))

    # Concentration
    sorted_abs = sorted(abs(s) for s in member_scores.values())[::-1]
    top_5_abs = sum(sorted_abs[:5]) if len(sorted_abs) >= 5 else sum(sorted_abs)
    total_abs_capped = sum(abs(s) for s in member_scores.values())

    if total_abs_capped > 0:
        concentration_top5 = top_5_abs / total_abs_capped
    else:
        concentration_top5 = 0.0

    is_concentrated = concentration_top5 > 0.5

    # Quality metrics
    staleness_values = [t.staleness_penalty for t in winsorized]
    mean_staleness = sum(staleness_values) / len(staleness_values) if staleness_values else 0.0

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
        is_concentrated=is_concentrated,
        members_capped=members_capped,
        mean_staleness=mean_staleness,
        transactions_included=len(winsorized),
        transactions_excluded=0,
    )


@dataclass
class SectorPositioning:
    """Positioning metrics for a single sector."""

    sector: str
    breadth_pct: float  # (buyers - sellers) / total for this sector
    buyers: int
    sellers: int
    volume_net: float
    volume_buy: float
    volume_sell: float
    member_count: int  # Members with exposure to this sector
    total_exposure: float  # Sum of member exposure scores

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "sector": self.sector,
            "breadth_pct": self.breadth_pct,
            "buyers": self.buyers,
            "sellers": self.sellers,
            "volume_net": self.volume_net,
            "volume_buy": self.volume_buy,
            "volume_sell": self.volume_sell,
            "member_count": self.member_count,
            "total_exposure": self.total_exposure,
        }


def compute_sector_positioning(
    scored_transactions: list[ScoredTransaction],
    member_sector_exposures: dict[str, list[dict]],
) -> list[SectorPositioning]:
    """Compute positioning metrics broken down by economic sector.

    Each transaction is attributed to sectors based on the member's
    committee exposure. A member on the Finance committee has their
    transactions weighted toward the Financial sector.

    Args:
        scored_transactions: List of scored transactions
        member_sector_exposures: Dict mapping member_id to list of
            sector exposures (from compute_member_sector_exposures)

    Returns:
        List of SectorPositioning objects, one per sector with activity
    """
    if not scored_transactions:
        return []

    # Import here to avoid circular dependency
    from cppi.enrichment.sector_mapping import get_all_sectors

    # Group transactions by member first
    by_member: dict[str, list[ScoredTransaction]] = {}
    for t in scored_transactions:
        if t.member_id not in by_member:
            by_member[t.member_id] = []
        by_member[t.member_id].append(t)

    # Calculate member-level net positions
    member_net: dict[str, float] = {}
    for member_id, txns in by_member.items():
        member_net[member_id] = sum(t.final_score for t in txns)

    # Initialize sector tracking
    all_sectors = get_all_sectors()
    sector_data: dict[str, dict] = {}
    for sector in all_sectors:
        sector_data[sector] = {
            "volume_buy": 0.0,
            "volume_sell": 0.0,
            "buyers": set(),
            "sellers": set(),
            "members": set(),
            "total_exposure": 0.0,
        }

    # Attribute member positions to sectors based on exposure
    for member_id, net_position in member_net.items():
        exposures = member_sector_exposures.get(member_id, [])

        if not exposures:
            # No sector exposure data - skip this member
            continue

        # Sum total exposure for normalization
        total_exposure = sum(e.get("score", 0) for e in exposures)
        if total_exposure <= 0:
            continue

        # Distribute position across sectors proportionally
        for exposure in exposures:
            sector = exposure.get("sector", "")
            score = exposure.get("score", 0)

            if not sector or sector not in sector_data:
                continue

            # Weight by exposure proportion
            weight = score / total_exposure
            weighted_position = net_position * weight

            # Track in sector
            sector_data[sector]["members"].add(member_id)
            sector_data[sector]["total_exposure"] += score

            if weighted_position > 0:
                sector_data[sector]["volume_buy"] += weighted_position
                sector_data[sector]["buyers"].add(member_id)
            elif weighted_position < 0:
                sector_data[sector]["volume_sell"] += abs(weighted_position)
                sector_data[sector]["sellers"].add(member_id)

    # Build result list
    results = []
    for sector in all_sectors:
        data = sector_data[sector]

        # Skip sectors with no activity
        if not data["members"]:
            continue

        buyers = len(data["buyers"])
        sellers = len(data["sellers"])
        member_count = len(data["members"])

        if member_count > 0:
            breadth_pct = (buyers - sellers) / member_count
        else:
            breadth_pct = 0.0

        results.append(SectorPositioning(
            sector=sector,
            breadth_pct=breadth_pct,
            buyers=buyers,
            sellers=sellers,
            volume_net=data["volume_buy"] - data["volume_sell"],
            volume_buy=data["volume_buy"],
            volume_sell=data["volume_sell"],
            member_count=member_count,
            total_exposure=data["total_exposure"],
        ))

    # Sort by absolute volume for most significant sectors first
    results.sort(key=lambda x: abs(x.volume_net), reverse=True)

    return results


def compute_confidence_score(
    aggregate: AggregateResult,
    resolution_rate: float,
    chamber_balance: float = 0.5,
) -> dict:
    """Compute composite confidence score.

    Args:
        aggregate: Aggregate positioning result
        resolution_rate: Percentage of transactions resolved (0-1)
        chamber_balance: House vs Senate balance (0.5 = perfect)

    Returns:
        Dictionary with composite score and factor breakdown
    """
    factors = {
        "member_coverage": min(1.0, aggregate.unique_members / 50),
        "transaction_volume": min(1.0, aggregate.transactions_included / 200),
        "resolution_quality": resolution_rate,
        "timeliness": aggregate.mean_staleness,  # Already 0-1
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

    if composite > 0.7:
        tier = "HIGH"
    elif composite > 0.4:
        tier = "MODERATE"
    else:
        tier = "LOW"

    return {
        "composite_score": composite,
        "tier": tier,
        "factors": factors,
        "weights": weights,
    }
