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


def test_daily_brief_structure():
    """Daily brief should produce all expected sections from an in-memory DB."""
    import sqlite3
    import tempfile
    from signals.core.derived_db import init_db, get_connection, insert_normalized, insert_signal_result, insert_run
    from signals.core.dto import NormalizedTransaction, SignalResult
    from signals.core.runs import make_run

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db_path = tmp.name
        init_db(db_path)
        run = make_run("test", "insider", "test", {}, {})
        with get_connection(db_path) as conn:
            insert_run(conn, run)
            # Insert 2 insider BUY transactions for the same ticker from different actors
            for i, actor in enumerate(["CEO Alpha", "CFO Beta"], start=1):
                insert_normalized(conn, NormalizedTransaction(
                    source="insider", source_record_id=f"test:{i}", source_filing_id="f1",
                    actor_id=f"cik-{i}", actor_name=actor, actor_type="ceo",
                    owner_type="direct", entity_key="entity:test", instrument_key=None,
                    ticker="TEST", issuer_name="Test Corp", instrument_type="ST",
                    transaction_type="open_market_buy", direction="BUY",
                    execution_date="2026-04-01", disclosure_date="2026-04-02",
                    amount_low=50000.0, amount_high=50000.0, amount_estimate=50000.0,
                    currency="USD", units_low=100.0, units_high=100.0,
                    price_low=500.0, price_high=500.0,
                    quality_score=1.0, parse_confidence=1.0,
                    resolution_event_id=None, resolution_confidence=0.99,
                    resolution_method_version="test",
                    include_in_signal=True, exclusion_reason_code=None,
                    exclusion_reason_detail=None,
                    provenance_payload={}, normalization_method_version="test",
                    run_id=run.run_id,
                ))
            # Insert a bullish signal
            insert_signal_result(conn, SignalResult(
                source="insider", scope="entity", subject_key="entity:test",
                score=0.7, label="bullish", confidence=0.8,
                as_of_date="2026-04-04", lookback_window=30,
                input_count=2, included_count=2, excluded_count=0,
                explanation="test", method_version="test", code_version="test",
                run_id=run.run_id, provenance_refs={},
            ), "fp-test")

        from signals.analysis.daily_brief import build_daily_brief
        brief = build_daily_brief(db_path, reference_date=datetime(2026, 4, 4))

        assert brief["as_of_date"] == "2026-04-04"
        assert "cluster_buy_alerts" in brief
        assert "strong_insider_buys" in brief
        assert "cross_source_signals" in brief
        assert "stats" in brief
        # Should find the cluster buy (2 unique buyers for TEST)
        assert len(brief["cluster_buy_alerts"]) == 1
        assert brief["cluster_buy_alerts"][0]["ticker"] == "TEST"
        assert brief["cluster_buy_alerts"][0]["unique_buyers"] == 2
        # Should find the strong insider buy
        assert len(brief["strong_insider_buys"]) == 1
        assert brief["strong_insider_buys"][0]["ticker"] == "TEST"


def test_daily_brief_filters_sells():
    """Daily brief should not include sell-driven signals."""
    import tempfile
    from signals.core.derived_db import init_db, get_connection, insert_signal_result, insert_run
    from signals.core.dto import SignalResult
    from signals.core.runs import make_run

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db_path = tmp.name
        init_db(db_path)
        run = make_run("test", "insider", "test", {}, {})
        with get_connection(db_path) as conn:
            insert_run(conn, run)
            # Insert a bearish signal — should NOT appear in brief
            insert_signal_result(conn, SignalResult(
                source="insider", scope="entity", subject_key="entity:sell",
                score=-0.5, label="bearish", confidence=0.8,
                as_of_date="2026-04-04", lookback_window=90,
                input_count=5, included_count=5, excluded_count=0,
                explanation="test", method_version="test", code_version="test",
                run_id=run.run_id, provenance_refs={},
            ), "fp-sell")

        from signals.analysis.daily_brief import build_daily_brief
        brief = build_daily_brief(db_path)

        assert len(brief["strong_insider_buys"]) == 0
        assert len(brief["cluster_buy_alerts"]) == 0


def test_daily_brief_anomaly_detection():
    """Anomaly detection should flag first-time insider buying."""
    import tempfile
    from signals.core.derived_db import init_db, get_connection, insert_normalized, insert_run
    from signals.core.dto import NormalizedTransaction
    from signals.core.runs import make_run

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db_path = tmp.name
        init_db(db_path)
        run = make_run("test", "insider", "test", {}, {})
        with get_connection(db_path) as conn:
            insert_run(conn, run)
            # Insert one recent buy with NO historical buys
            insert_normalized(conn, NormalizedTransaction(
                source="insider", source_record_id="anomaly:1", source_filing_id="f1",
                actor_id="cik-1", actor_name="CEO Anomaly", actor_type="ceo",
                owner_type="direct", entity_key="entity:anomtest", instrument_key=None,
                ticker="ANOMTEST", issuer_name="Anomaly Corp", instrument_type="ST",
                transaction_type="open_market_buy", direction="BUY",
                execution_date="2026-04-01", disclosure_date="2026-04-02",
                amount_low=50000.0, amount_high=50000.0, amount_estimate=50000.0,
                currency="USD", units_low=100.0, units_high=100.0,
                price_low=500.0, price_high=500.0,
                quality_score=1.0, parse_confidence=1.0,
                resolution_event_id=None, resolution_confidence=0.99,
                resolution_method_version="test",
                include_in_signal=True, exclusion_reason_code=None,
                exclusion_reason_detail=None,
                provenance_payload={}, normalization_method_version="test",
                run_id=run.run_id,
            ))

        from signals.analysis.daily_brief import build_daily_brief
        brief = build_daily_brief(db_path, reference_date=datetime(2026, 4, 4))

        assert len(brief["anomaly_alerts"]) == 1
        assert brief["anomaly_alerts"][0]["ticker"] == "ANOMTEST"
        assert brief["anomaly_alerts"][0]["alert_type"] == "first_buy_in_period"


def test_sector_cache_logic(monkeypatch, tmp_path):
    """Sector cache should store and retrieve without re-fetching."""
    import signals.analysis.sectors as sectors_mod

    # Mock yfinance
    call_count = {"n": 0}
    class FakeTicker:
        def __init__(self, ticker):
            call_count["n"] += 1
            self.info = {"sector": "Technology", "industry": "Software"}

    monkeypatch.setattr(sectors_mod, "HAS_YFINANCE", True)
    monkeypatch.setattr(sectors_mod.yf, "Ticker", FakeTicker)
    monkeypatch.setattr(sectors_mod, "_CACHE_DB", tmp_path / "test_cache.db")

    # First call: fetches from yfinance
    result1 = sectors_mod.get_sector_map(["AAPL"])
    assert result1["AAPL"]["sector"] == "Technology"
    assert call_count["n"] == 1

    # Second call: should use cache, not re-fetch
    result2 = sectors_mod.get_sector_map(["AAPL"])
    assert result2["AAPL"]["sector"] == "Technology"
    assert call_count["n"] == 1  # no new yfinance call


def test_baseline_comparison_no_yfinance(monkeypatch):
    """Baseline comparison should return error when yfinance unavailable."""
    import signals.analysis.validation as val_mod
    monkeypatch.setattr(val_mod, "HAS_YFINANCE", False)
    result = val_mod.run_baseline_comparison("/nonexistent.db")
    assert "error" in result


def test_regime_analysis_no_yfinance(monkeypatch):
    """Regime analysis should return error when yfinance unavailable."""
    import signals.analysis.validation as val_mod
    monkeypatch.setattr(val_mod, "HAS_YFINANCE", False)
    result = val_mod.run_regime_analysis("/nonexistent.db")
    assert "error" in result


def test_senate_filing_metadata_sidecar(tmp_path):
    """Sidecar JSON should round-trip filing metadata."""
    from signals.congress.senate_direct import _write_filing_metadata, _read_filing_metadata
    from signals.congress.senate_connector import SenateFiling

    html_path = tmp_path / "ptr_abcdef12.html"
    html_path.write_text("<html></html>")

    filing = SenateFiling(
        filing_id="abcdef12-abcd-1234-abcd-abcdef123456",
        filer_name="Senator Test",
        state=None,
        filing_date=datetime(2026, 3, 15),
        report_url="https://example.com",
        is_paper=False,
    )
    _write_filing_metadata(html_path, filing)

    meta = _read_filing_metadata(html_path)
    assert meta is not None
    assert meta["filing_date"] == "2026-03-15"
    assert meta["filer_name"] == "Senator Test"
    assert meta["is_paper"] is False

    # Missing sidecar returns None
    other_path = tmp_path / "ptr_noexist.html"
    other_path.write_text("<html></html>")
    assert _read_filing_metadata(other_path) is None
