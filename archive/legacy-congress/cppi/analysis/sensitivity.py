"""
Sensitivity analysis for CPPI signal parameters.

Tests how the positioning signal changes across different parameter configurations
to understand signal stability and robustness.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from cppi.scoring import ScoredTransaction, compute_aggregate

logger = logging.getLogger(__name__)


@dataclass
class SensitivityResult:
    """Result of a single parameter sensitivity test."""

    parameter_name: str
    parameter_values: list
    breadth_values: list[float]
    volume_values: list[float]
    breadth_range: float  # max - min breadth
    volume_range: float  # max - min volume
    is_stable: bool  # True if range is within acceptable bounds

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "parameter_name": self.parameter_name,
            "parameter_values": self.parameter_values,
            "breadth_values": self.breadth_values,
            "volume_values": self.volume_values,
            "breadth_range": self.breadth_range,
            "volume_range": self.volume_range,
            "is_stable": self.is_stable,
        }


# Default parameter sweep configurations
DEFAULT_SWEEPS = {
    "member_cap_pct": [0.03, 0.05, 0.07, 0.10, 0.15],
    "winsorize_percentile": [0.90, 0.95, 0.97, 0.99],
}

# Stability thresholds
BREADTH_STABILITY_THRESHOLD = 0.10  # Max 10% breadth change
VOLUME_STABILITY_THRESHOLD = 0.20  # Max 20% volume change (relative)


def sweep_parameter(
    transactions: list[ScoredTransaction],
    parameter_name: str,
    values: list,
    base_config: Optional[dict] = None,
) -> SensitivityResult:
    """
    Sweep a single parameter across values and measure signal change.

    Args:
        transactions: List of scored transactions to analyze
        parameter_name: Name of parameter to sweep
        values: List of values to test
        base_config: Base configuration (parameter will be overridden)

    Returns:
        SensitivityResult with breadth/volume at each value
    """
    base_config = base_config or {}
    breadth_values = []
    volume_values = []

    for value in values:
        # Create config with this parameter value
        config = base_config.copy()
        config[parameter_name] = value

        # Compute aggregate with this config
        agg = compute_aggregate(
            transactions,
            member_cap_pct=config.get("member_cap_pct"),
            winsorize_pct=config.get("winsorize_percentile"),
        )

        breadth_values.append(agg.breadth_pct)
        volume_values.append(agg.volume_net)

    # Calculate ranges
    breadth_range = max(breadth_values) - min(breadth_values) if breadth_values else 0
    volume_range_abs = max(volume_values) - min(volume_values) if volume_values else 0

    # Relative volume range (normalized by mean)
    mean_volume = sum(abs(v) for v in volume_values) / len(volume_values) if volume_values else 1
    volume_range_rel = volume_range_abs / mean_volume if mean_volume > 0 else 0

    # Check stability
    is_stable = (
        abs(breadth_range) <= BREADTH_STABILITY_THRESHOLD
        and volume_range_rel <= VOLUME_STABILITY_THRESHOLD
    )

    return SensitivityResult(
        parameter_name=parameter_name,
        parameter_values=values,
        breadth_values=breadth_values,
        volume_values=volume_values,
        breadth_range=breadth_range,
        volume_range=volume_range_abs,
        is_stable=is_stable,
    )


def run_sensitivity_analysis(
    transactions: list[ScoredTransaction],
    parameter_sweeps: Optional[dict] = None,
) -> list[SensitivityResult]:
    """
    Run full sensitivity analysis across multiple parameters.

    Args:
        transactions: List of scored transactions
        parameter_sweeps: Dict mapping parameter names to value lists.
                         Defaults to DEFAULT_SWEEPS.

    Returns:
        List of SensitivityResult objects, one per parameter
    """
    sweeps = parameter_sweeps or DEFAULT_SWEEPS
    results = []

    for param_name, values in sweeps.items():
        logger.info(f"Sweeping parameter: {param_name}")
        result = sweep_parameter(transactions, param_name, values)
        results.append(result)

        if result.is_stable:
            logger.info(f"  {param_name}: STABLE (breadth range: {result.breadth_range:.3f})")
        else:
            logger.warning(
                f"  {param_name}: UNSTABLE (breadth range: {result.breadth_range:.3f})"
            )

    return results


def format_sensitivity_report(results: list[SensitivityResult]) -> str:
    """
    Format sensitivity analysis results as text report.

    Args:
        results: List of SensitivityResult objects

    Returns:
        Formatted text report
    """
    lines = [
        "=" * 77,
        "SENSITIVITY ANALYSIS REPORT",
        "=" * 77,
        "",
    ]

    stable_count = sum(1 for r in results if r.is_stable)
    total = len(results)

    lines.extend([
        f"Parameters tested: {total}",
        f"Stable: {stable_count}/{total}",
        "",
        "-" * 77,
    ])

    for result in results:
        status = "STABLE" if result.is_stable else "UNSTABLE"
        lines.extend([
            "",
            f"Parameter: {result.parameter_name} [{status}]",
            f"  Values tested: {result.parameter_values}",
            f"  Breadth range: {result.breadth_range:.3f} ({result.breadth_range*100:.1f}%)",
            f"  Volume range: ${result.volume_range:,.0f}",
            "",
            "  Value       Breadth    Net Volume",
            "  " + "-" * 40,
        ])

        for i, val in enumerate(result.parameter_values):
            lines.append(
                f"  {val:<10} {result.breadth_values[i]:+7.1%}  ${result.volume_values[i]:>12,.0f}"
            )

    lines.extend([
        "",
        "-" * 77,
        "Stability thresholds:",
        f"  Breadth: +/- {BREADTH_STABILITY_THRESHOLD*100:.0f}%",
        f"  Volume: +/- {VOLUME_STABILITY_THRESHOLD*100:.0f}% (relative)",
        "",
    ])

    return "\n".join(lines)


def get_custom_sweep(
    param_name: str,
    start: float,
    end: float,
    steps: int = 5,
) -> dict:
    """
    Generate a custom parameter sweep configuration.

    Args:
        param_name: Parameter name
        start: Start value
        end: End value
        steps: Number of steps

    Returns:
        Dict suitable for parameter_sweeps argument
    """
    if steps < 2:
        steps = 2

    step_size = (end - start) / (steps - 1)
    values = [start + i * step_size for i in range(steps)]

    return {param_name: values}
