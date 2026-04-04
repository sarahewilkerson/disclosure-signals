from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

from signals.congress import engine as direct_congress_engine
from signals.insider import engine as direct_insider_engine


def _load_fixture(name: str) -> dict:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "expected_parity" / f"{name}.json"
    return json.loads(fixture_path.read_text())


def test_insider_transaction_scoring_parity():
    expected = _load_fixture("insider_engine_single")
    reference_date = datetime(2026, 4, 2)
    txn = {
        "transaction_code": "P",
        "role_class": "officer",
        "is_likely_planned": 0,
        "ownership_nature": "D",
        "pct_holdings_changed": 0.05,
        "transaction_date": "2026-03-01",
        "cik_owner": "owner-1",
        "total_value": 50000.0,
    }

    direct = direct_insider_engine.score_transaction(dict(txn), reference_date)

    assert direct.keys() == expected.keys()
    for key in direct:
        assert math.isclose(direct[key], expected[key], rel_tol=1e-9, abs_tol=1e-9)


def test_insider_company_aggregate_parity():
    expected = _load_fixture("insider_engine_agg")
    reference_date = datetime(2026, 4, 2)
    
    txn_template = {
        "transaction_code": "P",
        "role_class": "officer",
        "is_likely_planned": 0,
        "ownership_nature": "D",
        "pct_holdings_changed": 0.05,
        "transaction_date": "2026-03-01",
        "cik_owner": "owner-1",
        "total_value": 50000.0,
    }
    insider_txns = [
        {**txn_template, "transaction_date": "2026-03-01", "total_value": 10000.0, "transaction_code": "P", "cik_owner": "owner-1"},
        {**txn_template, "transaction_date": "2026-03-02", "total_value": 20000.0, "transaction_code": "S", "cik_owner": "owner-2"},
    ]
    scored_txns = [{**t, **direct_insider_engine.score_transaction(t, reference_date)} for t in insider_txns]
    
    direct_result = direct_insider_engine.aggregate_company_signal(scored_txns, 90)

    assert math.isclose(direct_result["score"], expected["score"], abs_tol=1e-9)
    assert math.isclose(direct_result["confidence"], expected["confidence"], abs_tol=1e-9)
    assert direct_result["signal"] == expected["signal"]
    assert direct_result["explanation"] == expected["explanation"]


def test_congress_transaction_and_aggregate_parity():
    expected_root = _load_fixture("congress_engine")
    expected_single = expected_root["single"]
    expected_agg = expected_root["aggregate"]
    expected_conf = expected_root["confidence"]
    
    reference_date = datetime(2026, 4, 2)
    congress_txn = {
        "member_id": "member-1",
        "ticker": "AAPL",
        "transaction_type": "purchase",
        "execution_date": datetime(2026, 3, 1),
        "amount_min": 15001,
        "amount_max": 50000,
        "owner_type": "self",
        "resolution_confidence": 1.0,
        "signal_weight": 1.0,
    }

    direct_scored = direct_congress_engine.score_transaction(reference_date=reference_date, **congress_txn)

    assert math.isclose(direct_scored.final_score, expected_single["final_score"], rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(direct_scored.raw_score, expected_single["raw_score"], rel_tol=1e-9, abs_tol=1e-9)

    direct_aggregate = direct_congress_engine.compute_aggregate([direct_scored])
    assert math.isclose(direct_aggregate.volume_net, expected_agg["volume_net"], rel_tol=1e-9, abs_tol=1e-9)
    assert direct_aggregate.unique_members == expected_agg["unique_members"]
    assert direct_aggregate.transactions_included == expected_agg["transactions_included"]

    direct_conf = direct_congress_engine.compute_confidence_score(direct_aggregate, 1.0)
    assert math.isclose(direct_conf["composite_score"], expected_conf["composite_score"], rel_tol=1e-9, abs_tol=1e-9)


def test_single_transaction_returns_insufficient():
    """Single insider transaction should produce 'insufficient' signal."""
    reference_date = datetime(2026, 4, 2)
    txn = {
        "transaction_code": "P",
        "role_class": "ceo",
        "is_likely_planned": 0,
        "ownership_nature": "D",
        "pct_holdings_changed": 0.05,
        "transaction_date": "2026-03-01",
        "cik_owner": "owner-1",
        "total_value": 50000.0,
    }
    scored = [{**txn, **direct_insider_engine.score_transaction(txn, reference_date)}]
    result = direct_insider_engine.aggregate_company_signal(scored, 90)
    assert result["signal"] == "insufficient"
    assert result["score"] == 0.0
    assert result["confidence"] == 0.0


def test_planned_trade_near_zero_signal():
    """10b5-1 planned trades should produce ~5% of non-planned signal."""
    reference_date = datetime(2026, 4, 2)
    base_txn = {
        "transaction_code": "S",
        "role_class": "ceo",
        "is_likely_planned": 0,
        "ownership_nature": "D",
        "pct_holdings_changed": 0.05,
        "transaction_date": "2026-03-01",
        "cik_owner": "owner-1",
        "total_value": 100000.0,
    }
    planned_txn = {**base_txn, "is_likely_planned": 1}

    normal_result = direct_insider_engine.score_transaction(base_txn, reference_date)
    planned_result = direct_insider_engine.score_transaction(planned_txn, reference_date)

    ratio = abs(planned_result["transaction_signal"]) / abs(normal_result["transaction_signal"])
    assert math.isclose(ratio, 0.05, rel_tol=1e-9)


def test_mixed_direction_no_bonus():
    """Mixed buy/sell should NOT get a confidence bonus over single-direction."""
    # Unanimous direction is higher conviction — mixed should not be rewarded
    conf_mixed = direct_insider_engine._compute_confidence(4, 2, has_buys=True, has_sells=True)
    conf_single = direct_insider_engine._compute_confidence(4, 2, has_buys=True, has_sells=False)
    assert conf_mixed == conf_single


def test_managed_account_zero_weight():
    """Managed account trades should have zero weight (member not making decisions)."""
    assert direct_congress_engine.get_owner_weight("managed") == 0.0
    assert direct_congress_engine.get_owner_weight("self") == 1.0
    assert direct_congress_engine.get_owner_weight("spouse") == 0.8


def test_congress_single_txn_insufficient():
    """Congress label_from_score with single transaction should return 'insufficient'."""
    assert direct_congress_engine.label_from_score(0.5, 0.8, transaction_count=1) == "insufficient"
    assert direct_congress_engine.label_from_score(0.5, 0.8, transaction_count=0) == "insufficient"
    assert direct_congress_engine.label_from_score(0.5, 0.8, transaction_count=2) == "bullish"
    assert direct_congress_engine.label_from_score(-0.5, 0.8, transaction_count=2) == "bearish"


def test_disclosure_lag_penalty():
    """Disclosure lag penalty should decay with time between execution and disclosure."""
    from signals.congress.engine import disclosure_lag_penalty

    # Same day or close: no penalty
    assert disclosure_lag_penalty(datetime(2026, 3, 1), datetime(2026, 3, 2)) == 1.0
    # 30 days: still no penalty
    assert disclosure_lag_penalty(datetime(2026, 3, 1), datetime(2026, 3, 31)) == 1.0
    # 45 days: moderate penalty
    assert disclosure_lag_penalty(datetime(2026, 3, 1), datetime(2026, 4, 15)) == 0.85
    # 90 days: significant penalty
    assert disclosure_lag_penalty(datetime(2026, 3, 1), datetime(2026, 5, 30)) == 0.6
    # 180 days: heavy penalty
    assert disclosure_lag_penalty(datetime(2026, 3, 1), datetime(2026, 8, 28)) == 0.3
    # Unknown: moderate default
    assert disclosure_lag_penalty(None, datetime(2026, 3, 1)) == 0.7
    assert disclosure_lag_penalty(datetime(2026, 3, 1), None) == 0.7


def test_minimum_trade_value_insider_exclusion():
    """Insider trades below $10K should be excluded with BELOW_MINIMUM_VALUE."""
    from signals.insider.direct_service import MINIMUM_INSIDER_TRADE_VALUE
    assert MINIMUM_INSIDER_TRADE_VALUE == 10_000


def test_minimum_trade_value_congress_exclusion():
    """Congress trades in lowest bracket should be excluded."""
    from signals.congress.direct_service import MINIMUM_CONGRESS_TRADE_AMOUNT
    from signals.congress.senate_direct import MINIMUM_CONGRESS_TRADE_AMOUNT as SENATE_MIN
    assert MINIMUM_CONGRESS_TRADE_AMOUNT == 15_000
    assert SENATE_MIN == 15_000


def test_staleness_continuous_decay():
    """Staleness penalty should be continuous with no cliffs."""
    from signals.congress.engine import staleness_penalty

    ref = datetime(2026, 4, 2)
    # At 0 days: 1.0
    assert math.isclose(staleness_penalty(datetime(2026, 4, 2), ref), 1.0)
    # At 60 days (half-life): ~0.5
    assert math.isclose(staleness_penalty(datetime(2026, 2, 1), ref), 0.5, abs_tol=0.01)
    # Monotonically decreasing: no cliffs
    prev = 1.0
    for days in range(1, 365):
        from datetime import timedelta
        val = staleness_penalty(ref - timedelta(days=days), ref)
        assert val <= prev, f"Staleness increased at day {days}: {prev} -> {val}"
        prev = val


def test_strength_tier_classification():
    """Signal strength tiers should classify based on confidence and score magnitude."""
    from signals.combined.overlay import _classify_strength

    # Strong: both high confidence + large score
    assert _classify_strength(0.8, 0.75, 0.5) == "strong"
    # Moderate: both reasonable confidence
    assert _classify_strength(0.5, 0.5, 0.1) == "moderate"
    # Weak: one side below threshold
    assert _classify_strength(0.3, 0.8, 0.5) == "weak"
    assert _classify_strength(0.8, 0.2, 0.5) == "weak"
    # Edge: high confidence but low score magnitude → moderate (not strong)
    assert _classify_strength(0.8, 0.8, 0.2) == "moderate"
