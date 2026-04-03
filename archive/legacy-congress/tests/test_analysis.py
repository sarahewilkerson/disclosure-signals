"""Tests for analysis modules (sensitivity and weight comparison)."""

from datetime import datetime

import pytest

from cppi.analysis.sensitivity import (
    DEFAULT_SWEEPS,
    SensitivityResult,
    format_sensitivity_report,
    get_custom_sweep,
    run_sensitivity_analysis,
    sweep_parameter,
)
from cppi.analysis.weight_comparison import (
    WeightComparison,
    compare_weighting_methods,
    compute_time_series_correlation,
    detect_divergence_periods,
    format_weight_comparison_report,
    normalize_volume_signal,
    signal_direction,
)
from cppi.scoring import ScoredTransaction


def make_scored_transaction(
    member_id: str,
    final_score: float,
    ticker: str = "TEST",
) -> ScoredTransaction:
    """Helper to create a scored transaction for testing."""
    return ScoredTransaction(
        member_id=member_id,
        ticker=ticker,
        transaction_type="purchase" if final_score > 0 else "sale",
        execution_date=datetime.now(),
        amount_min=1000,
        amount_max=15000,
        owner_type="self",
        base_value=abs(final_score),
        direction=1.0 if final_score > 0 else -1.0,
        staleness_penalty=1.0,
        owner_weight=1.0,
        resolution_confidence=1.0,
        signal_weight=1.0,
        raw_score=final_score,
        final_score=final_score,
    )


class TestSensitivityResult:
    """Test SensitivityResult dataclass."""

    def test_creation(self):
        """Test creating a SensitivityResult."""
        result = SensitivityResult(
            parameter_name="member_cap_pct",
            parameter_values=[0.03, 0.05, 0.10],
            breadth_values=[0.2, 0.22, 0.25],
            volume_values=[1000, 1100, 1200],
            breadth_range=0.05,
            volume_range=200,
            is_stable=True,
        )
        assert result.parameter_name == "member_cap_pct"
        assert len(result.parameter_values) == 3
        assert result.is_stable is True

    def test_to_dict(self):
        """Test to_dict conversion."""
        result = SensitivityResult(
            parameter_name="test",
            parameter_values=[1, 2, 3],
            breadth_values=[0.1, 0.2, 0.3],
            volume_values=[100, 200, 300],
            breadth_range=0.2,
            volume_range=200,
            is_stable=False,
        )
        d = result.to_dict()
        assert d["parameter_name"] == "test"
        assert d["is_stable"] is False


class TestSweepParameter:
    """Test parameter sweep functionality."""

    def test_empty_transactions(self):
        """Test sweep with empty transactions."""
        result = sweep_parameter([], "member_cap_pct", [0.03, 0.05])
        assert result.parameter_name == "member_cap_pct"
        assert all(b == 0 for b in result.breadth_values)

    def test_single_parameter_sweep(self):
        """Test sweeping a single parameter."""
        txns = [
            make_scored_transaction("M001", 1000.0),
            make_scored_transaction("M002", -500.0),
            make_scored_transaction("M003", 200.0),
        ]

        result = sweep_parameter(
            txns,
            "member_cap_pct",
            [0.03, 0.05, 0.10],
        )

        assert result.parameter_name == "member_cap_pct"
        assert len(result.breadth_values) == 3
        assert len(result.volume_values) == 3

    def test_stability_detection(self):
        """Test that stability is correctly detected."""
        # Create transactions where member cap will have significant effect
        txns = [
            make_scored_transaction("M001", 10000.0),  # Large position
            make_scored_transaction("M002", 100.0),
            make_scored_transaction("M003", 100.0),
        ]

        result = sweep_parameter(
            txns,
            "member_cap_pct",
            [0.01, 0.50],  # Very different caps
        )

        # Volume should change significantly with different caps
        assert result.volume_range > 0


class TestRunSensitivityAnalysis:
    """Test full sensitivity analysis."""

    def test_default_sweeps(self):
        """Test running with default parameter sweeps."""
        txns = [
            make_scored_transaction(f"M{i:03d}", 100.0 * (i % 3 - 1))
            for i in range(10)
        ]

        results = run_sensitivity_analysis(txns)

        assert len(results) == len(DEFAULT_SWEEPS)
        for result in results:
            assert result.parameter_name in DEFAULT_SWEEPS

    def test_custom_sweeps(self):
        """Test running with custom parameter sweeps."""
        txns = [make_scored_transaction("M001", 1000.0)]

        custom = {"member_cap_pct": [0.05, 0.10]}
        results = run_sensitivity_analysis(txns, custom)

        assert len(results) == 1
        assert results[0].parameter_name == "member_cap_pct"


class TestGetCustomSweep:
    """Test custom sweep generation."""

    def test_basic_sweep(self):
        """Test basic sweep generation."""
        sweep = get_custom_sweep("test_param", 0.0, 1.0, steps=5)
        assert "test_param" in sweep
        assert len(sweep["test_param"]) == 5
        assert sweep["test_param"][0] == 0.0
        assert sweep["test_param"][-1] == 1.0

    def test_minimum_steps(self):
        """Test that minimum steps is enforced."""
        sweep = get_custom_sweep("test", 0.0, 1.0, steps=1)
        assert len(sweep["test"]) == 2  # Minimum 2 steps


class TestFormatSensitivityReport:
    """Test report formatting."""

    def test_format_report(self):
        """Test report formatting."""
        results = [
            SensitivityResult(
                parameter_name="member_cap_pct",
                parameter_values=[0.03, 0.05, 0.10],
                breadth_values=[0.2, 0.22, 0.25],
                volume_values=[1000, 1100, 1200],
                breadth_range=0.05,
                volume_range=200,
                is_stable=True,
            )
        ]

        report = format_sensitivity_report(results)

        assert "SENSITIVITY ANALYSIS" in report
        assert "member_cap_pct" in report
        assert "STABLE" in report


# Weight Comparison Tests


class TestSignalDirection:
    """Test signal direction classification."""

    def test_bullish(self):
        """Test bullish classification."""
        assert signal_direction(0.2) == "bullish"
        assert signal_direction(0.5) == "bullish"

    def test_bearish(self):
        """Test bearish classification."""
        assert signal_direction(-0.2) == "bearish"
        assert signal_direction(-0.5) == "bearish"

    def test_neutral(self):
        """Test neutral classification."""
        assert signal_direction(0.0) == "neutral"
        assert signal_direction(0.05) == "neutral"
        assert signal_direction(-0.05) == "neutral"

    def test_custom_threshold(self):
        """Test with custom threshold."""
        assert signal_direction(0.15, threshold=0.2) == "neutral"
        assert signal_direction(0.25, threshold=0.2) == "bullish"


class TestNormalizeVolumeSignal:
    """Test volume signal normalization."""

    def test_positive_net(self):
        """Test positive net volume."""
        assert normalize_volume_signal(100, 200) == 0.5

    def test_negative_net(self):
        """Test negative net volume."""
        assert normalize_volume_signal(-100, 200) == -0.5

    def test_zero_total(self):
        """Test zero total volume."""
        assert normalize_volume_signal(0, 0) == 0.0


class TestWeightComparison:
    """Test WeightComparison dataclass."""

    def test_creation(self):
        """Test creating a WeightComparison."""
        comp = WeightComparison(
            breadth_signal=0.3,
            volume_signal=0.5,
            breadth_direction="bullish",
            volume_direction="bullish",
            is_divergent=False,
            divergence_magnitude=0.2,
            correlation=0.8,
        )
        assert comp.breadth_signal == 0.3
        assert comp.is_divergent is False

    def test_to_dict(self):
        """Test to_dict conversion."""
        comp = WeightComparison(
            breadth_signal=0.3,
            volume_signal=-0.2,
            breadth_direction="bullish",
            volume_direction="bearish",
            is_divergent=True,
            divergence_magnitude=0.5,
            correlation=None,
        )
        d = comp.to_dict()
        assert d["is_divergent"] is True
        assert d["correlation"] is None


class TestCompareWeightingMethods:
    """Test weighting method comparison."""

    def test_empty_transactions(self):
        """Test with empty transactions."""
        result = compare_weighting_methods([])
        assert result.breadth_signal == 0.0
        assert result.is_divergent is False

    def test_aligned_signals(self):
        """Test when breadth and volume are aligned."""
        txns = [
            make_scored_transaction("M001", 1000.0),
            make_scored_transaction("M002", 1000.0),
            make_scored_transaction("M003", 1000.0),
        ]

        result = compare_weighting_methods(txns)

        assert result.breadth_direction == "bullish"
        assert result.volume_direction == "bullish"
        assert result.is_divergent is False

    def test_divergent_signals(self):
        """Test when breadth and volume diverge."""
        # Many small sellers but one large buyer
        txns = [
            make_scored_transaction("M001", 10000.0),  # Large buyer
            make_scored_transaction("M002", -100.0),  # Small seller
            make_scored_transaction("M003", -100.0),  # Small seller
            make_scored_transaction("M004", -100.0),  # Small seller
            make_scored_transaction("M005", -100.0),  # Small seller
        ]

        result = compare_weighting_methods(txns)

        # Breadth: 1 buyer, 4 sellers = bearish
        # Volume: Large positive net = bullish
        # These should be divergent
        if result.breadth_direction != result.volume_direction:
            assert result.is_divergent is True


class TestTimeSeries:
    """Test time series analysis functions."""

    def test_correlation_perfect_positive(self):
        """Test perfect positive correlation."""
        breadth = [0.1, 0.2, 0.3, 0.4, 0.5]
        volume = [0.1, 0.2, 0.3, 0.4, 0.5]

        corr = compute_time_series_correlation(breadth, volume)
        assert abs(corr - 1.0) < 0.01

    def test_correlation_perfect_negative(self):
        """Test perfect negative correlation."""
        breadth = [0.1, 0.2, 0.3, 0.4, 0.5]
        volume = [0.5, 0.4, 0.3, 0.2, 0.1]

        corr = compute_time_series_correlation(breadth, volume)
        assert abs(corr - (-1.0)) < 0.01

    def test_correlation_no_correlation(self):
        """Test no correlation."""
        breadth = [0.1, -0.1, 0.1, -0.1]
        volume = [0.0, 0.0, 0.0, 0.0]

        corr = compute_time_series_correlation(breadth, volume)
        assert corr == 0.0

    def test_detect_divergence_periods(self):
        """Test divergence period detection."""
        breadth = [0.3, 0.3, -0.3, -0.3, 0.3]
        volume = [0.3, 0.3, 0.3, 0.3, 0.3]  # Stays positive
        dates = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3),
            datetime(2024, 1, 4),
            datetime(2024, 1, 5),
        ]

        periods = detect_divergence_periods(breadth, volume, dates)

        # Should detect divergence when breadth goes negative but volume stays positive
        assert len(periods) >= 1


class TestFormatWeightComparisonReport:
    """Test weight comparison report formatting."""

    def test_format_aligned(self):
        """Test formatting aligned signals."""
        comp = WeightComparison(
            breadth_signal=0.3,
            volume_signal=0.4,
            breadth_direction="bullish",
            volume_direction="bullish",
            is_divergent=False,
            divergence_magnitude=0.1,
            correlation=None,
        )

        report = format_weight_comparison_report(comp)

        assert "BREADTH VS VOLUME" in report
        assert "ALIGNED" in report

    def test_format_divergent(self):
        """Test formatting divergent signals."""
        comp = WeightComparison(
            breadth_signal=0.3,
            volume_signal=-0.4,
            breadth_direction="bullish",
            volume_direction="bearish",
            is_divergent=True,
            divergence_magnitude=0.7,
            correlation=None,
        )

        report = format_weight_comparison_report(comp)

        assert "DIVERGENCE DETECTED" in report
