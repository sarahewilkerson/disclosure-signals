"""Backtesting infrastructure for CPPI signals."""

from cppi.backtest.data import (
    PriceCache,
    fetch_historical_prices,
    get_price_returns,
)
from cppi.backtest.engine import (
    BacktestConfig,
    BacktestResult,
    run_backtest,
    format_backtest_report,
    store_historical_scores,
)

__all__ = [
    "PriceCache",
    "fetch_historical_prices",
    "get_price_returns",
    "BacktestConfig",
    "BacktestResult",
    "run_backtest",
    "format_backtest_report",
    "store_historical_scores",
]
