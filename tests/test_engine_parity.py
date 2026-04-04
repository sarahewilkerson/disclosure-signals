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
