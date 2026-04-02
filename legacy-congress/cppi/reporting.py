"""
Report generation for CPPI.

Generates text reports matching the format in design plan Section 15.
"""

import logging
from dataclasses import dataclass
from datetime import datetime

from cppi.scoring import AggregateResult

logger = logging.getLogger(__name__)


@dataclass
class DataQuality:
    """Data quality metrics for reporting."""

    included_transactions: int
    excluded_asset_class: int
    excluded_unparseable: int
    resolution_rate_count: float
    resolution_rate_value: float
    mean_disclosure_lag: float
    min_disclosure_lag: int
    max_disclosure_lag: int


@dataclass
class ReportData:
    """All data needed to generate a report."""

    # Metadata
    generated_at: datetime
    window_days: int
    window_end: datetime

    # Aggregate positioning
    aggregate: AggregateResult

    # Confidence
    confidence_score: float
    confidence_tier: str
    confidence_factors: dict

    # Data quality
    quality: DataQuality

    # Chamber breakdown (optional for MVP)
    house_pct: float = 0.5
    house_breadth: float = 0.0
    senate_pct: float = 0.5
    senate_breadth: float = 0.0

    # Sector breakdown (Phase 2)
    sector_positioning: list = None  # List of SectorPositioning objects


def format_currency(value: float) -> str:
    """Format currency value with magnitude suffix."""
    abs_val = abs(value)
    sign = "-" if value < 0 else "+"

    if abs_val >= 1_000_000_000:
        return f"{sign}${abs_val / 1_000_000_000:.1f}B"
    elif abs_val >= 1_000_000:
        return f"{sign}${abs_val / 1_000_000:.0f}M"
    elif abs_val >= 1_000:
        return f"{sign}${abs_val / 1_000:.0f}K"
    else:
        return f"{sign}${abs_val:.0f}"


def generate_text_report(data: ReportData) -> str:
    """Generate the full text report.

    Args:
        data: ReportData containing all metrics

    Returns:
        Formatted text report string
    """
    agg = data.aggregate
    qual = data.quality

    # Determine breadth signal description
    if agg.breadth_pct > 0.1:
        breadth_signal = f"NET BUYERS ({agg.breadth_pct:.0%} buy-biased members)"
    elif agg.breadth_pct < -0.1:
        breadth_signal = f"NET SELLERS ({abs(agg.breadth_pct):.0%} sell-biased members)"
    else:
        breadth_signal = f"NEUTRAL ({agg.breadth_pct:+.0%} bias)"

    # Volume tilt description
    volume_tilt = f"Estimated {format_currency(agg.volume_net)} equivalent (range-based, lag-adjusted)"

    # Concentration warning
    if agg.is_concentrated:
        conc_status = "(signal is CONCENTRATED - interpret with caution)"
    elif agg.concentration_top5 > 0.4:
        conc_status = "(signal is moderately concentrated)"
    else:
        conc_status = "(signal is well-dispersed)"

    lines = [
        "=" * 77,
        "CONGRESSIONAL DISCLOSED POSITIONING INDEX",
        f"Generated: {data.generated_at.strftime('%Y-%m-%d')}",
        f"Window: {data.window_days} days ending {data.window_end.strftime('%Y-%m-%d')}",
        "=" * 77,
        "",
        "POSITIONING SUMMARY",
        "-" * 77,
        f"Breadth Signal:      {breadth_signal}",
        f"Volume Tilt:         {volume_tilt}",
        f"Confidence:          {data.confidence_tier} (score: {data.confidence_score:.2f})",
        "",
        "BREADTH METRICS",
        "-" * 77,
        f"Active Members:      {agg.unique_members} (of ~535 in Congress)",
        f"Net Buyers:          {agg.buyers} members ({agg.buyers / agg.unique_members:.0%})" if agg.unique_members > 0 else "Net Buyers:          0 members",
        f"Net Sellers:         {agg.sellers} members ({agg.sellers / agg.unique_members:.0%})" if agg.unique_members > 0 else "Net Sellers:         0 members",
        f"Breadth Direction:   {agg.buyers - agg.sellers:+d} (buyers - sellers)",
        "",
        "VOLUME METRICS (Estimates based on disclosed ranges)",
        "-" * 77,
        f"Estimated Buy Volume:     ~{format_currency(agg.volume_buy).lstrip('+')} equivalent",
        f"Estimated Sell Volume:    ~{format_currency(agg.volume_sell).lstrip('+')} equivalent",
        f"Estimated Net:            ~{format_currency(agg.volume_net)} equivalent",
        "",
        "CONCENTRATION WARNING" if agg.is_concentrated else "CONCENTRATION",
        "-" * 77,
        f"Top 5 members:       {agg.concentration_top5:.0%} of total signal volume",
        f"                     {conc_status}",
    ]

    # Chamber breakdown (if available)
    if data.house_pct > 0 or data.senate_pct > 0:
        lines.extend([
            "",
            "CHAMBER BREAKDOWN",
            "-" * 77,
            f"House:               {data.house_pct:.0%} of signal  |  Breadth: {data.house_breadth:.0%} net buyers",
            f"Senate:              {data.senate_pct:.0%} of signal  |  Breadth: {data.senate_breadth:.0%} net buyers",
        ])

    # Sector breakdown (Phase 2)
    if data.sector_positioning:
        lines.extend([
            "",
            "SECTOR BREAKDOWN",
            "-" * 77,
            "Sector               Members  Buyers  Sellers  Breadth   Net Volume",
            "-" * 77,
        ])
        for sp in data.sector_positioning[:10]:  # Top 10 sectors by volume
            lines.append(
                f"{sp.sector:20s} {sp.member_count:5d}   {sp.buyers:5d}   "
                f"{sp.sellers:5d}   {sp.breadth_pct:+6.0%}   {format_currency(sp.volume_net)}"
            )

    # Confidence factors
    lines.extend([
        "",
        "CONFIDENCE FACTOR BREAKDOWN",
        "-" * 77,
    ])

    # Format each factor
    factor_labels = {
        "member_coverage": "Member coverage",
        "transaction_volume": "Transaction volume",
        "resolution_quality": "Resolution quality",
        "timeliness": "Timeliness",
        "balance": "Chamber balance",
        "concentration": "Concentration",
    }

    for key, label in factor_labels.items():
        if key in data.confidence_factors:
            value = data.confidence_factors[key]
            lines.append(f"{label:20s} {value:.2f}")

    # Data quality
    lines.extend([
        "",
        "DATA QUALITY",
        "-" * 77,
        f"Included transactions:    {qual.included_transactions:,}",
        f"Excluded (asset class):     {qual.excluded_asset_class:,} (mutual funds, broad ETFs)",
        f"Excluded (unparseable):      {qual.excluded_unparseable:,}",
        f"Resolution rate:          {qual.resolution_rate_count:.1%} by count, {qual.resolution_rate_value:.1%} by estimated value",
        f"Mean disclosure lag:      {qual.mean_disclosure_lag:.0f} days (range: {qual.min_disclosure_lag}-{qual.max_disclosure_lag} days)",
        "",
        "LIMITATIONS",
        "-" * 77,
        "- All amounts are ESTIMATES based on disclosed ranges",
        "- Disclosure lag means data reflects positions 6-12 weeks old",
        f"- {1 - qual.resolution_rate_count:.0%} of transactions unresolved to tickers",
        "- Signal reflects disclosed activity, not intent or prediction",
        "",
        "METHODOLOGY: See methodology.md",
        "DISCLAIMER: Aggregate positioning data only. Not financial advice.",
        "           Does not measure or imply ethics, compliance, or intent.",
        "",
    ])

    return "\n".join(lines)


def generate_json_report(data: ReportData) -> dict:
    """Generate JSON report for programmatic use.

    Args:
        data: ReportData containing all metrics

    Returns:
        Dictionary with all report data
    """
    return {
        "metadata": {
            "generated_at": data.generated_at.isoformat(),
            "window_days": data.window_days,
            "window_end": data.window_end.isoformat(),
        },
        "positioning": {
            "breadth_pct": data.aggregate.breadth_pct,
            "buyers": data.aggregate.buyers,
            "sellers": data.aggregate.sellers,
            "neutral": data.aggregate.neutral,
            "unique_members": data.aggregate.unique_members,
            "volume_net": data.aggregate.volume_net,
            "volume_buy": data.aggregate.volume_buy,
            "volume_sell": data.aggregate.volume_sell,
            "concentration_top5": data.aggregate.concentration_top5,
            "is_concentrated": data.aggregate.is_concentrated,
            "members_capped": data.aggregate.members_capped,
            "mean_staleness": data.aggregate.mean_staleness,
        },
        "confidence": {
            "score": data.confidence_score,
            "tier": data.confidence_tier,
            "factors": data.confidence_factors,
        },
        "quality": {
            "included_transactions": data.quality.included_transactions,
            "excluded_asset_class": data.quality.excluded_asset_class,
            "excluded_unparseable": data.quality.excluded_unparseable,
            "resolution_rate_count": data.quality.resolution_rate_count,
            "resolution_rate_value": data.quality.resolution_rate_value,
            "mean_disclosure_lag": data.quality.mean_disclosure_lag,
        },
        "chamber_breakdown": {
            "house_pct": data.house_pct,
            "house_breadth": data.house_breadth,
            "senate_pct": data.senate_pct,
            "senate_breadth": data.senate_breadth,
        },
        "sector_positioning": [
            sp.to_dict() if hasattr(sp, "to_dict") else sp
            for sp in (data.sector_positioning or [])
        ],
    }
