"""Tests for market regime computation and scoring integration."""

from __future__ import annotations

import math
from datetime import datetime
from unittest.mock import MagicMock

import pytest


def test_compute_regime_bear(monkeypatch):
    """Bear market (SPY < -2%) should produce regime_weight_buy = 1.1."""
    import signals.core.regime as regime_mod
    import pandas as pd

    # Mock yfinance: SPY drops 5% over lookback
    dates = pd.date_range("2025-01-01", periods=60, freq="B")
    prices = [500 - i * (500 * 0.05 / 60) for i in range(60)]
    mock_data = pd.DataFrame({"Close": prices}, index=dates)

    monkeypatch.setattr(regime_mod, "HAS_YFINANCE", True)
    monkeypatch.setattr(regime_mod.yf, "download", lambda *a, **kw: mock_data)

    result = regime_mod.compute_regime(datetime(2025, 4, 1))
    assert result.regime == "bear"
    assert result.regime_weight_buy == 1.1
    assert result.spy_trailing_return < 0


def test_compute_regime_bull(monkeypatch):
    """Bull market (SPY > +8%) should produce regime_weight_buy = 0.95."""
    import signals.core.regime as regime_mod
    import pandas as pd

    dates = pd.date_range("2025-01-01", periods=60, freq="B")
    prices = [500 + i * (500 * 0.10 / 60) for i in range(60)]
    mock_data = pd.DataFrame({"Close": prices}, index=dates)

    monkeypatch.setattr(regime_mod, "HAS_YFINANCE", True)
    monkeypatch.setattr(regime_mod.yf, "download", lambda *a, **kw: mock_data)

    result = regime_mod.compute_regime(datetime(2025, 4, 1))
    assert result.regime == "bull"
    assert result.regime_weight_buy == 0.95
    assert result.spy_trailing_return > 0.08


def test_compute_regime_neutral(monkeypatch):
    """Neutral market should produce regime_weight_buy = 1.0."""
    import signals.core.regime as regime_mod
    import pandas as pd

    dates = pd.date_range("2025-01-01", periods=60, freq="B")
    prices = [500 + i * (500 * 0.03 / 60) for i in range(60)]  # +3%, neutral range
    mock_data = pd.DataFrame({"Close": prices}, index=dates)

    monkeypatch.setattr(regime_mod, "HAS_YFINANCE", True)
    monkeypatch.setattr(regime_mod.yf, "download", lambda *a, **kw: mock_data)

    result = regime_mod.compute_regime(datetime(2025, 4, 1))
    assert result.regime == "neutral"
    assert result.regime_weight_buy == 1.0


def test_compute_regime_yfinance_failure(monkeypatch):
    """Should return unknown regime with weight=1.0 when yfinance fails."""
    import signals.core.regime as regime_mod

    monkeypatch.setattr(regime_mod, "HAS_YFINANCE", True)
    monkeypatch.setattr(regime_mod.yf, "download", MagicMock(side_effect=Exception("network error")))

    result = regime_mod.compute_regime(datetime(2025, 4, 1))
    assert result.regime == "unknown"
    assert result.regime_weight_buy == 1.0
    assert result.spy_trailing_return is None


def test_compute_regime_no_yfinance(monkeypatch):
    """Should return unknown when yfinance not installed."""
    import signals.core.regime as regime_mod

    monkeypatch.setattr(regime_mod, "HAS_YFINANCE", False)

    result = regime_mod.compute_regime(datetime(2025, 4, 1))
    assert result.regime == "unknown"
    assert result.regime_weight_buy == 1.0


def test_regime_weight_flows_through_insider_scoring():
    """Regime weight should multiply into insider transaction_signal."""
    from signals.insider.engine import score_transaction

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
    ref = datetime(2026, 4, 2)

    base = score_transaction(txn, ref)
    regime_boosted = score_transaction(txn, ref, regime_weight=1.1)

    assert math.isclose(regime_boosted["transaction_signal"], base["transaction_signal"] * 1.1, rel_tol=1e-9)


def test_regime_weight_flows_through_congress_scoring():
    """Regime weight should multiply into congress final_score."""
    from signals.congress.engine import score_transaction

    ref = datetime(2026, 4, 2)

    base = score_transaction(
        member_id="m1", ticker="AAPL", transaction_type="purchase",
        execution_date=datetime(2026, 3, 1), amount_min=15001, amount_max=50000,
        owner_type="self", resolution_confidence=1.0, signal_weight=1.0,
        reference_date=ref,
    )
    boosted = score_transaction(
        member_id="m1", ticker="AAPL", transaction_type="purchase",
        execution_date=datetime(2026, 3, 1), amount_min=15001, amount_max=50000,
        owner_type="self", resolution_confidence=1.0, signal_weight=1.0,
        reference_date=ref, regime_weight=1.1,
    )

    assert math.isclose(boosted.final_score, base.final_score * 1.1, rel_tol=1e-9)


def test_regime_default_preserves_existing_behavior():
    """Default regime_weight=1.0 should produce identical results to no-regime scoring."""
    from signals.insider.engine import score_transaction

    txn = {
        "transaction_code": "P", "role_class": "officer", "is_likely_planned": 0,
        "ownership_nature": "D", "pct_holdings_changed": 0.05,
        "transaction_date": "2026-03-01", "cik_owner": "o1", "total_value": 50000.0,
    }
    ref = datetime(2026, 4, 2)

    result = score_transaction(txn, ref, regime_weight=1.0)
    assert result == score_transaction(txn, ref)
