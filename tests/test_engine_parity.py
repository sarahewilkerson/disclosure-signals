from __future__ import annotations

import importlib
import math
import sys
from datetime import datetime
from pathlib import Path

from signals.congress import engine as direct_congress_engine
from signals.insider import engine as direct_insider_engine


def _import_legacy_insider_scoring():
    repo_root = Path(__file__).resolve().parents[1]
    legacy_root = str(repo_root / "legacy-insider")
    if legacy_root not in sys.path:
        sys.path.insert(0, legacy_root)
    return importlib.import_module("scoring")


def _import_legacy_congress_scoring():
    repo_root = Path(__file__).resolve().parents[1]
    legacy_root = str(repo_root / "legacy-congress")
    if legacy_root not in sys.path:
        sys.path.insert(0, legacy_root)
    return importlib.import_module("cppi.scoring")


def test_insider_transaction_scoring_parity():
    legacy = _import_legacy_insider_scoring()
    reference_date = datetime(2026, 4, 2)
    txn = {
        "transaction_code": "P",
        "role_class": "ceo",
        "is_likely_planned": 0,
        "ownership_nature": "D",
        "pct_holdings_changed": 0.08,
        "transaction_date": "2026-03-20",
        "cik_owner": "owner-1",
    }

    direct = direct_insider_engine.score_transaction(dict(txn), reference_date)
    legacy_row = legacy.score_transaction(dict(txn), reference_date)

    assert direct.keys() == legacy_row.keys()
    for key in direct:
        assert math.isclose(direct[key], legacy_row[key], rel_tol=1e-9, abs_tol=1e-9)


def test_insider_company_aggregate_parity():
    legacy = _import_legacy_insider_scoring()
    reference_date = datetime(2026, 4, 2)
    txns = [
        {
            "transaction_code": "P",
            "role_class": "ceo",
            "is_likely_planned": 0,
            "ownership_nature": "D",
            "pct_holdings_changed": 0.10,
            "transaction_date": "2026-03-28",
            "cik_owner": "owner-1",
        },
        {
            "transaction_code": "S",
            "role_class": "cfo",
            "is_likely_planned": 1,
            "ownership_nature": "I",
            "pct_holdings_changed": 0.03,
            "transaction_date": "2026-03-25",
            "cik_owner": "owner-2",
        },
    ]
    direct_scored = [{**txn, **direct_insider_engine.score_transaction(txn, reference_date)} for txn in txns]
    legacy_scored = [{**txn, **legacy.score_transaction(txn, reference_date)} for txn in txns]

    direct_result = direct_insider_engine.aggregate_company_signal(direct_scored, 30)
    legacy_score, legacy_contrib = legacy._aggregate_with_saturation(legacy_scored)
    legacy_conf = legacy._compute_confidence(2, 2, True, True)
    legacy_signal = legacy._label_signal(legacy_score, legacy_conf)

    assert math.isclose(direct_result["score"], round(legacy_score, 4), abs_tol=1e-9)
    assert math.isclose(direct_result["confidence"], round(legacy_conf, 4), abs_tol=1e-9)
    assert direct_result["signal"] == legacy_signal
    assert set(direct_result["explanation"])  # non-empty
    assert set(legacy_contrib.keys()) == {"owner-1", "owner-2"}


def test_congress_transaction_and_aggregate_parity():
    legacy = _import_legacy_congress_scoring()
    reference_date = datetime(2026, 4, 2)
    txns = [
        dict(
            member_id="member-1",
            ticker="AAPL",
            transaction_type="purchase",
            execution_date=datetime(2026, 3, 15),
            amount_min=1001,
            amount_max=15000,
            owner_type="self",
            resolution_confidence=0.99,
            signal_weight=1.0,
        ),
        dict(
            member_id="member-2",
            ticker="MSFT",
            transaction_type="sale",
            execution_date=datetime(2026, 3, 10),
            amount_min=15001,
            amount_max=50000,
            owner_type="spouse",
            resolution_confidence=0.9,
            signal_weight=1.0,
        ),
    ]

    direct_scored = [
        direct_congress_engine.score_transaction(reference_date=reference_date, **txn)
        for txn in txns
    ]
    legacy_scored = [
        legacy.score_transaction(reference_date=reference_date, **txn)
        for txn in txns
    ]

    for direct_row, legacy_row in zip(direct_scored, legacy_scored, strict=True):
        assert math.isclose(direct_row.final_score, legacy_row.final_score, rel_tol=1e-9, abs_tol=1e-9)
        assert math.isclose(direct_row.raw_score, legacy_row.raw_score, rel_tol=1e-9, abs_tol=1e-9)

    direct_aggregate = direct_congress_engine.compute_aggregate(direct_scored)
    legacy_aggregate = legacy.compute_aggregate(legacy_scored)
    assert math.isclose(direct_aggregate.volume_net, legacy_aggregate.volume_net, rel_tol=1e-9, abs_tol=1e-9)
    assert direct_aggregate.unique_members == legacy_aggregate.unique_members
    assert direct_aggregate.transactions_included == legacy_aggregate.transactions_included

    direct_conf = direct_congress_engine.compute_confidence_score(direct_aggregate, 1.0)
    legacy_conf = legacy.compute_confidence_score(legacy_aggregate, 1.0)
    assert math.isclose(direct_conf["composite_score"], legacy_conf["composite_score"], rel_tol=1e-9, abs_tol=1e-9)
