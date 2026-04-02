"""Tests for scoring engine module."""

import math
from datetime import datetime, timedelta

import pytest

from cppi.scoring import (
    AggregateResult,
    ScoredTransaction,
    compute_aggregate,
    compute_confidence_score,
    estimate_amount,
    get_owner_weight,
    score_transaction,
    staleness_penalty,
    winsorize_transactions,
)


class TestStalenessPenalty:
    """Tests for staleness penalty calculation."""

    def test_fresh_trade_no_penalty(self):
        """Test trades within 45 days have no penalty."""
        ref = datetime(2024, 3, 1)
        exec_date = datetime(2024, 2, 15)  # 15 days ago
        assert staleness_penalty(exec_date, ref) == 1.0

    def test_45_day_boundary(self):
        """Test exact 45-day boundary."""
        ref = datetime(2024, 3, 15)
        exec_date = datetime(2024, 1, 30)  # Exactly 45 days
        assert staleness_penalty(exec_date, ref) == 1.0

    def test_slightly_stale_60_days(self):
        """Test 45-60 day range has slight penalty."""
        ref = datetime(2024, 3, 15)
        exec_date = datetime(2024, 1, 20)  # 55 days
        assert staleness_penalty(exec_date, ref) == 0.9

    def test_moderate_staleness_90_days(self):
        """Test 60-90 day range has moderate penalty."""
        ref = datetime(2024, 3, 15)
        exec_date = datetime(2023, 12, 25)  # ~80 days
        assert staleness_penalty(exec_date, ref) == 0.7

    def test_very_stale_180_days(self):
        """Test 90-180 day range has significant penalty."""
        ref = datetime(2024, 6, 1)
        exec_date = datetime(2024, 1, 1)  # ~150 days
        assert staleness_penalty(exec_date, ref) == 0.4

    def test_extremely_stale_over_180(self):
        """Test >180 days has maximum penalty."""
        ref = datetime(2024, 6, 1)
        exec_date = datetime(2023, 6, 1)  # 365 days
        assert staleness_penalty(exec_date, ref) == 0.2

    def test_unknown_date_moderate_penalty(self):
        """Test None date gets moderate penalty."""
        ref = datetime(2024, 3, 1)
        assert staleness_penalty(None, ref) == 0.5

    def test_future_date_minimal_penalty(self):
        """Test future dates (data errors) get minimal penalty."""
        ref = datetime(2024, 3, 1)
        exec_date = datetime(2024, 3, 15)  # Future
        assert staleness_penalty(exec_date, ref) == 0.9


class TestAmountEstimation:
    """Tests for amount estimation from ranges."""

    def test_geometric_mean(self):
        """Test geometric mean calculation."""
        result = estimate_amount(1001, 15000, "geometric_mean")
        expected = math.sqrt(1001 * 15000)
        assert abs(result - expected) < 0.01

    def test_midpoint(self):
        """Test midpoint calculation."""
        result = estimate_amount(1001, 15000, "midpoint")
        expected = (1001 + 15000) / 2
        assert result == expected

    def test_lower_bound(self):
        """Test lower bound returns minimum."""
        result = estimate_amount(1001, 15000, "lower_bound")
        assert result == 1001.0

    def test_log_uniform_ev(self):
        """Test log-uniform expected value."""
        result = estimate_amount(1001, 15000, "log_uniform_ev")
        expected = (15000 - 1001) / math.log(15000 / 1001)
        assert abs(result - expected) < 0.01

    def test_log_uniform_equal_bounds(self):
        """Test log-uniform with equal bounds."""
        result = estimate_amount(5000, 5000, "log_uniform_ev")
        assert result == 5000.0

    def test_none_bounds_return_zero(self):
        """Test None bounds return zero."""
        assert estimate_amount(None, 15000, "geometric_mean") == 0.0
        assert estimate_amount(1001, None, "geometric_mean") == 0.0
        assert estimate_amount(None, None, "geometric_mean") == 0.0

    def test_zero_bounds_return_zero(self):
        """Test zero bounds return zero."""
        assert estimate_amount(0, 15000, "geometric_mean") == 0.0
        assert estimate_amount(1001, 0, "geometric_mean") == 0.0

    def test_default_is_geometric_mean(self):
        """Test default method is geometric mean."""
        result = estimate_amount(1001, 15000)
        expected = math.sqrt(1001 * 15000)
        assert abs(result - expected) < 0.01

    def test_invalid_method_defaults_to_geometric(self):
        """Test invalid method falls back to geometric mean."""
        result = estimate_amount(1001, 15000, "invalid_method")
        expected = math.sqrt(1001 * 15000)
        assert abs(result - expected) < 0.01


class TestOwnerWeights:
    """Tests for owner type weights."""

    def test_self_full_weight(self):
        """Test self trades have full weight."""
        assert get_owner_weight("self") == 1.0

    def test_spouse_weight(self):
        """Test spouse trades have 0.8 weight."""
        assert get_owner_weight("spouse") == 0.8

    def test_joint_weight(self):
        """Test joint trades have 0.9 weight."""
        assert get_owner_weight("joint") == 0.9

    def test_dependent_weight(self):
        """Test dependent trades have 0.5 weight."""
        assert get_owner_weight("dependent") == 0.5

    def test_managed_weight(self):
        """Test managed account trades have 0.3 weight."""
        assert get_owner_weight("managed") == 0.3

    def test_unknown_owner_default(self):
        """Test unknown owner gets default weight."""
        assert get_owner_weight("unknown") == 0.3


class TestTransactionScoring:
    """Tests for individual transaction scoring."""

    def test_purchase_positive_direction(self):
        """Test purchase has positive direction."""
        ref = datetime(2024, 3, 1)
        result = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2024, 2, 15),
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
        )
        assert result.direction == 1.0
        assert result.final_score > 0

    def test_sale_negative_direction(self):
        """Test sale has negative direction."""
        ref = datetime(2024, 3, 1)
        result = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="sale",
            execution_date=datetime(2024, 2, 15),
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
        )
        assert result.direction == -1.0
        assert result.final_score < 0

    def test_partial_sale_negative(self):
        """Test partial sale has negative direction."""
        ref = datetime(2024, 3, 1)
        result = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="sale_partial",
            execution_date=datetime(2024, 2, 15),
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
        )
        assert result.direction == -1.0

    def test_exchange_neutral(self):
        """Test exchange has zero direction."""
        ref = datetime(2024, 3, 1)
        result = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="exchange",
            execution_date=datetime(2024, 2, 15),
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
        )
        assert result.direction == 0.0
        assert result.final_score == 0.0

    def test_staleness_reduces_score(self):
        """Test stale trades have reduced scores."""
        ref = datetime(2024, 6, 1)

        fresh = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2024, 5, 15),  # 17 days
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
        )

        stale = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2024, 1, 1),  # ~150 days
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
        )

        assert fresh.final_score > stale.final_score
        assert fresh.staleness_penalty == 1.0
        assert stale.staleness_penalty < 1.0

    def test_owner_weight_applied(self):
        """Test owner type weight is applied."""
        ref = datetime(2024, 3, 1)

        self_trade = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2024, 2, 15),
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
        )

        spouse_trade = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2024, 2, 15),
            amount_min=1001,
            amount_max=15000,
            owner_type="spouse",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
        )

        assert self_trade.owner_weight == 1.0
        assert spouse_trade.owner_weight == 0.8
        assert self_trade.final_score > spouse_trade.final_score

    def test_resolution_confidence_applied(self):
        """Test resolution confidence reduces score."""
        ref = datetime(2024, 3, 1)

        high_conf = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2024, 2, 15),
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
        )

        low_conf = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2024, 2, 15),
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=0.5,
            signal_weight=1.0,
            reference_date=ref,
        )

        assert high_conf.final_score > low_conf.final_score

    def test_log_scaling(self):
        """Test log scaling compresses large amounts."""
        ref = datetime(2024, 3, 1)

        no_log = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2024, 2, 15),
            amount_min=1_000_001,
            amount_max=5_000_000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
            use_log_scaling=False,
        )

        with_log = score_transaction(
            member_id="M001",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2024, 2, 15),
            amount_min=1_000_001,
            amount_max=5_000_000,
            owner_type="self",
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=ref,
            use_log_scaling=True,
        )

        # Log scaling should compress large values
        assert with_log.base_value < no_log.base_value


class TestWinsorization:
    """Tests for transaction winsorization."""

    def test_empty_list(self):
        """Test empty list returns empty."""
        assert winsorize_transactions([]) == []

    def test_no_clipping_needed(self):
        """Test normal transactions unchanged."""
        txns = [
            _create_scored_txn("M001", 100.0),
            _create_scored_txn("M002", 200.0),
            _create_scored_txn("M003", 150.0),
        ]
        result = winsorize_transactions(txns, 0.95)
        # With only 3 values, 95th percentile is the max
        for orig, clipped in zip(txns, result):
            assert orig.final_score == clipped.final_score

    def test_outlier_clipped(self):
        """Test extreme outliers are clipped."""
        # Create 100 transactions with one extreme outlier
        # At 90th percentile, the top 10% (10 values) should be clipped
        txns = [_create_scored_txn(f"M{i:02d}", 100.0) for i in range(99)]
        txns.append(_create_scored_txn("M99", 10000.0))  # Outlier

        result = winsorize_transactions(txns, 0.90)

        # Find the outlier in results
        outlier_result = [t for t in result if t.member_id == "M99"][0]
        # Should be clipped to 90th percentile (100.0, same as others)
        assert outlier_result.final_score == 100.0

    def test_negative_scores_handled(self):
        """Test negative scores are clipped properly."""
        txns = [
            _create_scored_txn("M001", 100.0),
            _create_scored_txn("M002", -100.0),
            _create_scored_txn("M003", -5000.0),  # Negative outlier
        ]

        result = winsorize_transactions(txns, 0.5)  # 50th percentile

        # The negative outlier should be clipped (less negative)
        outlier = [t for t in result if t.member_id == "M003"][0]
        assert outlier.final_score > -5000.0
        assert outlier.final_score < 0  # Still negative


class TestAggregateComputation:
    """Tests for aggregate positioning computation."""

    def test_empty_transactions(self):
        """Test empty transactions return zero aggregate."""
        result = compute_aggregate([])
        assert result.breadth_pct == 0.0
        assert result.unique_members == 0
        assert result.volume_net == 0.0

    def test_all_buyers(self):
        """Test all buyers produces positive breadth."""
        txns = [
            _create_scored_txn("M001", 100.0),
            _create_scored_txn("M002", 200.0),
            _create_scored_txn("M003", 150.0),
        ]
        result = compute_aggregate(txns)

        assert result.breadth_pct == 1.0  # 100% buyers
        assert result.buyers == 3
        assert result.sellers == 0
        assert result.volume_net > 0

    def test_all_sellers(self):
        """Test all sellers produces negative breadth."""
        txns = [
            _create_scored_txn("M001", -100.0),
            _create_scored_txn("M002", -200.0),
            _create_scored_txn("M003", -150.0),
        ]
        result = compute_aggregate(txns)

        assert result.breadth_pct == -1.0  # 100% sellers
        assert result.buyers == 0
        assert result.sellers == 3
        assert result.volume_net < 0

    def test_mixed_buyers_sellers(self):
        """Test mixed signal produces partial breadth."""
        txns = [
            _create_scored_txn("M001", 100.0),
            _create_scored_txn("M002", 200.0),
            _create_scored_txn("M003", -100.0),
        ]
        result = compute_aggregate(txns)

        # 2 buyers, 1 seller: (2-1)/3 = 0.333...
        assert abs(result.breadth_pct - 0.333) < 0.01
        assert result.buyers == 2
        assert result.sellers == 1

    def test_member_aggregation(self):
        """Test multiple transactions per member are aggregated."""
        txns = [
            _create_scored_txn("M001", 100.0),
            _create_scored_txn("M001", 200.0),  # Same member
            _create_scored_txn("M002", -50.0),
        ]
        result = compute_aggregate(txns)

        assert result.unique_members == 2
        # M001 is net buyer (300), M002 is seller (-50)
        assert result.buyers == 1
        assert result.sellers == 1

    def test_member_cap_applied(self):
        """Test member cap limits dominant traders."""
        # One member with 90% of volume
        txns = [
            _create_scored_txn("M001", 9000.0),  # Dominant
            _create_scored_txn("M002", 500.0),
            _create_scored_txn("M003", 500.0),
        ]

        # With 5% cap, M001's contribution should be limited
        result = compute_aggregate(txns, member_cap_pct=0.05)
        assert result.members_capped >= 1

    def test_concentration_calculation(self):
        """Test concentration metric."""
        txns = [
            _create_scored_txn("M001", 500.0),
            _create_scored_txn("M002", 300.0),
            _create_scored_txn("M003", 100.0),
            _create_scored_txn("M004", 50.0),
            _create_scored_txn("M005", 50.0),
        ]
        result = compute_aggregate(txns, member_cap_pct=1.0)  # No cap for this test

        # All 5 members = top 5, so concentration should be 1.0
        assert result.concentration_top5 == 1.0

    def test_is_concentrated_flag(self):
        """Test concentration flag set when top 5 > 50%."""
        # With member cap disabled, check concentration
        txns = [
            _create_scored_txn("M001", 600.0),
            _create_scored_txn("M002", 200.0),
            _create_scored_txn("M003", 100.0),
            _create_scored_txn("M004", 50.0),
            _create_scored_txn("M005", 50.0),
        ]
        result = compute_aggregate(txns, member_cap_pct=1.0)

        # Top 5 is everyone, so concentration = 100%
        assert result.is_concentrated is True

    def test_volume_metrics(self):
        """Test volume buy/sell metrics."""
        txns = [
            _create_scored_txn("M001", 300.0),
            _create_scored_txn("M002", -100.0),
            _create_scored_txn("M003", 200.0),
        ]
        result = compute_aggregate(txns, member_cap_pct=1.0)

        assert result.volume_buy == 500.0  # 300 + 200
        assert result.volume_sell == 100.0  # abs(-100)
        assert result.volume_net == 400.0  # 500 - 100


class TestConfidenceScore:
    """Tests for composite confidence scoring."""

    def test_high_confidence(self):
        """Test high-quality data produces high confidence."""
        agg = AggregateResult(
            breadth_pct=0.5,
            unique_members=100,
            buyers=75,
            sellers=25,
            neutral=0,
            volume_net=1000000.0,
            volume_buy=750000.0,
            volume_sell=250000.0,
            concentration_top5=0.3,
            is_concentrated=False,
            members_capped=0,
            mean_staleness=0.9,
            transactions_included=300,
            transactions_excluded=20,
        )

        result = compute_confidence_score(agg, resolution_rate=0.95)

        assert result["tier"] == "HIGH"
        assert result["composite_score"] > 0.7

    def test_low_confidence_few_members(self):
        """Test few members produces low confidence."""
        agg = AggregateResult(
            breadth_pct=0.5,
            unique_members=5,  # Very few
            buyers=3,
            sellers=2,
            neutral=0,
            volume_net=10000.0,
            volume_buy=7000.0,
            volume_sell=3000.0,
            concentration_top5=0.9,
            is_concentrated=True,
            members_capped=0,
            mean_staleness=0.3,  # Very stale
            transactions_included=10,
            transactions_excluded=5,
        )

        result = compute_confidence_score(agg, resolution_rate=0.3)

        assert result["tier"] == "LOW"
        assert result["composite_score"] < 0.4

    def test_factor_breakdown_included(self):
        """Test all factors are included in result."""
        agg = AggregateResult(
            breadth_pct=0.5,
            unique_members=50,
            buyers=35,
            sellers=15,
            neutral=0,
            volume_net=500000.0,
            volume_buy=350000.0,
            volume_sell=150000.0,
            concentration_top5=0.4,
            is_concentrated=False,
            members_capped=0,
            mean_staleness=0.7,
            transactions_included=150,
            transactions_excluded=10,
        )

        result = compute_confidence_score(agg, resolution_rate=0.8)

        assert "factors" in result
        assert "member_coverage" in result["factors"]
        assert "resolution_quality" in result["factors"]
        assert "timeliness" in result["factors"]
        assert "concentration" in result["factors"]


# Helper function to create scored transactions for testing
def _create_scored_txn(member_id: str, final_score: float) -> ScoredTransaction:
    """Create a scored transaction for testing."""
    return ScoredTransaction(
        member_id=member_id,
        ticker="TEST",
        transaction_type="purchase" if final_score > 0 else "sale",
        execution_date=datetime(2024, 2, 15),
        amount_min=1001,
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
