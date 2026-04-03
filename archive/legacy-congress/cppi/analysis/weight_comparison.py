"""
Equal-weight vs dollar-weight signal comparison for CPPI.

Compares breadth signal (equal weight per member) vs volume signal
(dollar-weighted) to identify divergence periods and correlations.
"""

import logging
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from cppi.scoring import ScoredTransaction, compute_aggregate

logger = logging.getLogger(__name__)


@dataclass
class WeightComparison:
    """Comparison between breadth and volume signals."""

    breadth_signal: float  # -1 to +1 (buyers - sellers / total)
    volume_signal: float  # Normalized net volume
    breadth_direction: str  # "bullish", "bearish", "neutral"
    volume_direction: str  # "bullish", "bearish", "neutral"
    is_divergent: bool  # True if signals disagree
    divergence_magnitude: float  # How much they disagree (0-2)
    correlation: Optional[float]  # If time series provided

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "breadth_signal": self.breadth_signal,
            "volume_signal": self.volume_signal,
            "breadth_direction": self.breadth_direction,
            "volume_direction": self.volume_direction,
            "is_divergent": self.is_divergent,
            "divergence_magnitude": self.divergence_magnitude,
            "correlation": self.correlation,
        }


def signal_direction(value: float, threshold: float = 0.1) -> str:
    """Classify signal direction."""
    if value > threshold:
        return "bullish"
    elif value < -threshold:
        return "bearish"
    else:
        return "neutral"


def normalize_volume_signal(volume_net: float, volume_total: float) -> float:
    """
    Normalize volume signal to -1 to +1 range.

    Args:
        volume_net: Net buy - sell volume
        volume_total: Total absolute volume

    Returns:
        Normalized signal from -1 to +1
    """
    if volume_total <= 0:
        return 0.0
    return volume_net / volume_total


def compare_weighting_methods(
    transactions: list[ScoredTransaction],
    divergence_threshold: float = 0.5,
) -> WeightComparison:
    """
    Compare breadth (equal-weight) vs volume (dollar-weight) signals.

    Args:
        transactions: List of scored transactions
        divergence_threshold: Threshold for considering signals divergent

    Returns:
        WeightComparison with both signals and divergence analysis
    """
    if not transactions:
        return WeightComparison(
            breadth_signal=0.0,
            volume_signal=0.0,
            breadth_direction="neutral",
            volume_direction="neutral",
            is_divergent=False,
            divergence_magnitude=0.0,
            correlation=None,
        )

    # Compute aggregate to get both signals
    agg = compute_aggregate(transactions)

    # Breadth signal: (buyers - sellers) / total members
    breadth = agg.breadth_pct

    # Volume signal: normalized net volume
    volume_total = agg.volume_buy + agg.volume_sell
    volume_norm = normalize_volume_signal(agg.volume_net, volume_total)

    # Determine directions
    breadth_dir = signal_direction(breadth)
    volume_dir = signal_direction(volume_norm)

    # Check for divergence
    # Divergence occurs when:
    # 1. Signals point opposite directions (bearish vs bullish)
    # 2. One is strong and other is neutral
    is_divergent = False
    divergence_mag = 0.0

    if breadth_dir != volume_dir:
        if (breadth_dir == "bullish" and volume_dir == "bearish") or \
           (breadth_dir == "bearish" and volume_dir == "bullish"):
            # Opposite directions - strong divergence
            is_divergent = True
            divergence_mag = abs(breadth - volume_norm)
        elif breadth_dir == "neutral" or volume_dir == "neutral":
            # One neutral, one directional - mild divergence
            divergence_mag = abs(breadth - volume_norm)
            is_divergent = divergence_mag > divergence_threshold

    return WeightComparison(
        breadth_signal=breadth,
        volume_signal=volume_norm,
        breadth_direction=breadth_dir,
        volume_direction=volume_dir,
        is_divergent=is_divergent,
        divergence_magnitude=divergence_mag,
        correlation=None,  # Single-point comparison, no correlation
    )


def compute_time_series_correlation(
    breadth_series: list[float],
    volume_series: list[float],
) -> float:
    """
    Compute Pearson correlation between breadth and volume time series.

    Args:
        breadth_series: Time series of breadth signals
        volume_series: Time series of volume signals

    Returns:
        Correlation coefficient from -1 to +1
    """
    if len(breadth_series) != len(volume_series):
        raise ValueError("Series must have same length")

    n = len(breadth_series)
    if n < 2:
        return 0.0

    # Calculate means
    mean_b = sum(breadth_series) / n
    mean_v = sum(volume_series) / n

    # Calculate covariance and variances
    cov = sum((b - mean_b) * (v - mean_v) for b, v in zip(breadth_series, volume_series))
    var_b = sum((b - mean_b) ** 2 for b in breadth_series)
    var_v = sum((v - mean_v) ** 2 for v in volume_series)

    # Handle edge cases
    if var_b <= 0 or var_v <= 0:
        return 0.0

    return cov / math.sqrt(var_b * var_v)


def detect_divergence_periods(
    breadth_series: list[float],
    volume_series: list[float],
    dates: list[datetime],
    threshold: float = 0.3,
) -> list[dict]:
    """
    Detect periods where breadth and volume signals diverge.

    Args:
        breadth_series: Time series of breadth signals
        volume_series: Time series of normalized volume signals
        dates: Corresponding dates
        threshold: Divergence threshold

    Returns:
        List of divergence periods with start/end dates
    """
    if len(breadth_series) != len(volume_series) or len(breadth_series) != len(dates):
        raise ValueError("All series must have same length")

    divergences = []
    in_divergence = False
    start_idx = None

    for i, (b, v, d) in enumerate(zip(breadth_series, volume_series, dates)):
        breadth_dir = signal_direction(b)
        volume_dir = signal_direction(v)

        # Check for divergence
        is_div = (
            (breadth_dir == "bullish" and volume_dir == "bearish") or
            (breadth_dir == "bearish" and volume_dir == "bullish") or
            abs(b - v) > threshold
        )

        if is_div and not in_divergence:
            # Start of divergence period
            in_divergence = True
            start_idx = i
        elif not is_div and in_divergence:
            # End of divergence period
            in_divergence = False
            divergences.append({
                "start_date": dates[start_idx].isoformat(),
                "end_date": dates[i - 1].isoformat(),
                "duration_days": (dates[i - 1] - dates[start_idx]).days,
                "max_divergence": max(
                    abs(breadth_series[j] - volume_series[j])
                    for j in range(start_idx, i)
                ),
            })

    # Handle ongoing divergence
    if in_divergence and start_idx is not None:
        divergences.append({
            "start_date": dates[start_idx].isoformat(),
            "end_date": dates[-1].isoformat(),
            "duration_days": (dates[-1] - dates[start_idx]).days,
            "max_divergence": max(
                abs(breadth_series[j] - volume_series[j])
                for j in range(start_idx, len(dates))
            ),
            "ongoing": True,
        })

    return divergences


def format_weight_comparison_report(
    comparison: WeightComparison,
    divergence_periods: Optional[list[dict]] = None,
) -> str:
    """
    Format weight comparison as text report.

    Args:
        comparison: WeightComparison result
        divergence_periods: Optional list of historical divergence periods

    Returns:
        Formatted text report
    """
    lines = [
        "=" * 77,
        "BREADTH VS VOLUME SIGNAL COMPARISON",
        "=" * 77,
        "",
        "CURRENT SIGNALS",
        "-" * 77,
        f"Breadth Signal:      {comparison.breadth_signal:+.1%} ({comparison.breadth_direction})",
        f"Volume Signal:       {comparison.volume_signal:+.1%} ({comparison.volume_direction})",
        "",
    ]

    if comparison.is_divergent:
        lines.extend([
            "*** DIVERGENCE DETECTED ***",
            f"Magnitude: {comparison.divergence_magnitude:.2f}",
            "",
            "Note: Breadth and volume signals are pointing different directions.",
            "This may indicate:",
            "  - Large trades by few members skewing volume",
            "  - Broad but small positioning changes",
            "  - Sector-specific activity",
            "",
        ])
    else:
        lines.extend([
            "Signals are ALIGNED",
            f"Divergence magnitude: {comparison.divergence_magnitude:.2f}",
            "",
        ])

    if comparison.correlation is not None:
        lines.extend([
            "CORRELATION ANALYSIS",
            "-" * 77,
            f"Breadth-Volume Correlation: {comparison.correlation:.2f}",
            "",
        ])

    if divergence_periods:
        lines.extend([
            "HISTORICAL DIVERGENCE PERIODS",
            "-" * 77,
        ])
        for period in divergence_periods:
            ongoing = " (ongoing)" if period.get("ongoing") else ""
            lines.append(
                f"  {period['start_date']} to {period['end_date']}{ongoing}"
                f" ({period['duration_days']} days, max: {period['max_divergence']:.2f})"
            )
        lines.append("")

    lines.extend([
        "INTERPRETATION",
        "-" * 77,
        "- Breadth signal: Equal-weight across members (democracy)",
        "- Volume signal: Dollar-weighted (capital flow)",
        "- Divergence often indicates concentrated activity",
        "",
    ])

    return "\n".join(lines)
