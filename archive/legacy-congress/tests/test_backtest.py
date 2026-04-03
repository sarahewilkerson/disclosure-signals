"""Tests for backtesting modules."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from cppi.backtest.data import (
    PriceCache,
    PricePoint,
    fetch_historical_prices,
    get_price_returns,
)
from cppi.backtest.engine import (
    BacktestConfig,
    BacktestResult,
    SignalPoint,
    _compute_correlation,
    format_backtest_report,
    run_backtest,
    store_historical_scores,
)


class TestPricePoint:
    """Test PricePoint dataclass."""

    def test_creation(self):
        """Test creating a PricePoint."""
        point = PricePoint(
            date=datetime(2024, 1, 15),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            adj_close=103.0,
        )
        assert point.close == 103.0
        assert point.volume == 1000000

    def test_to_dict(self):
        """Test to_dict conversion."""
        point = PricePoint(
            date=datetime(2024, 1, 15),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            adj_close=103.0,
        )
        d = point.to_dict()
        assert d["close"] == 103.0
        assert "2024-01-15" in d["date"]


class TestPriceCache:
    """Test PriceCache."""

    def test_cache_creation(self, tmp_path):
        """Test cache directory creation."""
        cache_dir = tmp_path / "price_cache"
        cache = PriceCache(str(cache_dir))
        assert cache.cache_dir.exists()

    def test_cache_miss(self, tmp_path):
        """Test cache miss returns None."""
        cache = PriceCache(str(tmp_path / "cache"))
        result = cache.get("AAPL", "2024-01-01", "2024-03-01")
        assert result is None

    def test_cache_set_get(self, tmp_path):
        """Test cache set and get."""
        cache = PriceCache(str(tmp_path / "cache"))
        prices = [
            PricePoint(
                date=datetime(2024, 1, 15),
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000,
                adj_close=103.0,
            )
        ]

        cache.set("AAPL", "2024-01-01", "2024-03-01", prices)
        result = cache.get("AAPL", "2024-01-01", "2024-03-01")

        assert result is not None
        assert len(result) == 1
        assert result[0].close == 103.0


class TestGetPriceReturns:
    """Test price return calculations."""

    def test_empty_prices(self):
        """Test empty price list."""
        returns = get_price_returns([], 30)
        assert returns == {}

    def test_single_price(self):
        """Test single price point."""
        prices = [
            PricePoint(
                date=datetime(2024, 1, 1),
                open=100.0, high=100.0, low=100.0,
                close=100.0, volume=1000, adj_close=100.0,
            )
        ]
        returns = get_price_returns(prices, 30)
        assert returns == {}

    def test_compute_returns(self):
        """Test forward return computation."""
        prices = [
            PricePoint(
                date=datetime(2024, 1, 1),
                open=100.0, high=100.0, low=100.0,
                close=100.0, volume=1000, adj_close=100.0,
            ),
            PricePoint(
                date=datetime(2024, 2, 1),
                open=110.0, high=110.0, low=110.0,
                close=110.0, volume=1000, adj_close=110.0,
            ),
        ]
        returns = get_price_returns(prices, 30)

        # Should have return for Jan 1 (looking forward to Feb 1)
        assert "2024-01-01" in returns
        assert abs(returns["2024-01-01"] - 0.10) < 0.001  # 10% return


class TestFetchHistoricalPrices:
    """Test fetching historical prices."""

    def test_no_yfinance(self):
        """Test error when yfinance not available."""
        with patch("cppi.backtest.data.HAS_YFINANCE", False):
            with pytest.raises(ImportError):
                fetch_historical_prices("AAPL", "2024-01-01", "2024-03-01")

    @patch("cppi.backtest.data.HAS_YFINANCE", True)
    @patch("cppi.backtest.data.yf")
    def test_fetch_with_cache_hit(self, mock_yf, tmp_path):
        """Test fetch with cache hit."""
        cache = PriceCache(str(tmp_path / "cache"))
        prices = [
            PricePoint(
                date=datetime(2024, 1, 15),
                open=100.0, high=105.0, low=99.0,
                close=103.0, volume=1000000, adj_close=103.0,
            )
        ]
        cache.set("AAPL", "2024-01-01", "2024-03-01", prices)

        result = fetch_historical_prices(
            "AAPL", "2024-01-01", "2024-03-01", cache
        )

        # Should use cache, not call yfinance
        mock_yf.download.assert_not_called()
        assert len(result) == 1


class TestBacktestConfig:
    """Test BacktestConfig dataclass."""

    def test_creation(self):
        """Test creating a BacktestConfig."""
        config = BacktestConfig(
            start_date="2024-01-01",
            end_date="2024-06-30",
            window_days=90,
            forward_return_days=30,
        )
        assert config.start_date == "2024-01-01"
        assert config.window_days == 90

    def test_defaults(self):
        """Test default values."""
        config = BacktestConfig(
            start_date="2024-01-01",
            end_date="2024-06-30",
        )
        assert config.window_days == 90
        assert config.forward_return_days == 30
        assert config.benchmark_ticker == "SPY"


class TestSignalPoint:
    """Test SignalPoint dataclass."""

    def test_creation(self):
        """Test creating a SignalPoint."""
        point = SignalPoint(
            as_of_date="2024-01-15",
            breadth_pct=0.65,
            net_positioning=0.15,
            confidence_score=0.75,
            transaction_count=50,
            forward_return=0.05,
        )
        assert point.breadth_pct == 0.65
        assert point.forward_return == 0.05


class TestComputeCorrelation:
    """Test correlation computation."""

    def test_perfect_positive(self):
        """Test perfect positive correlation."""
        xs = [1, 2, 3, 4, 5]
        ys = [2, 4, 6, 8, 10]
        corr = _compute_correlation(xs, ys)
        assert corr is not None
        assert abs(corr - 1.0) < 0.001

    def test_perfect_negative(self):
        """Test perfect negative correlation."""
        xs = [1, 2, 3, 4, 5]
        ys = [10, 8, 6, 4, 2]
        corr = _compute_correlation(xs, ys)
        assert corr is not None
        assert abs(corr - (-1.0)) < 0.001

    def test_no_correlation(self):
        """Test uncorrelated data."""
        xs = [1, 2, 3, 4, 5]
        ys = [1, -1, 1, -1, 1]  # Alternating
        corr = _compute_correlation(xs, ys)
        assert corr is not None
        # Should be close to zero
        assert abs(corr) < 0.3

    def test_insufficient_data(self):
        """Test with insufficient data."""
        xs = [1, 2]
        ys = [3, 4]
        corr = _compute_correlation(xs, ys)
        assert corr is None

    def test_mismatched_lengths(self):
        """Test with mismatched lengths."""
        xs = [1, 2, 3, 4, 5]
        ys = [1, 2, 3]
        corr = _compute_correlation(xs, ys)
        assert corr is None


class TestBacktestResult:
    """Test BacktestResult dataclass."""

    def test_creation(self):
        """Test creating a BacktestResult."""
        config = BacktestConfig(
            start_date="2024-01-01",
            end_date="2024-06-30",
        )
        result = BacktestResult(
            config=config,
            signal_points=[],
            correlation_breadth_vs_return=0.15,
            correlation_net_vs_return=0.20,
            correlation_confidence_vs_return=0.10,
            signal_positive_hit_rate=0.55,
            signal_negative_hit_rate=0.52,
            benchmark_total_return=0.08,
            average_forward_return=0.03,
            warnings=["Test warning"],
        )
        assert result.correlation_net_vs_return == 0.20
        assert result.signal_positive_hit_rate == 0.55

    def test_to_dict(self):
        """Test to_dict conversion."""
        config = BacktestConfig(
            start_date="2024-01-01",
            end_date="2024-06-30",
        )
        result = BacktestResult(
            config=config,
            signal_points=[],
            correlation_breadth_vs_return=0.15,
            correlation_net_vs_return=None,
            correlation_confidence_vs_return=None,
            signal_positive_hit_rate=None,
            signal_negative_hit_rate=None,
            benchmark_total_return=None,
            average_forward_return=None,
        )
        d = result.to_dict()
        assert d["config"]["start_date"] == "2024-01-01"
        assert d["metrics"]["correlation_breadth_vs_return"] == 0.15


class TestFormatBacktestReport:
    """Test report formatting."""

    def test_format_report(self):
        """Test report formatting."""
        config = BacktestConfig(
            start_date="2024-01-01",
            end_date="2024-06-30",
        )
        result = BacktestResult(
            config=config,
            signal_points=[
                SignalPoint(
                    as_of_date="2024-01-15",
                    breadth_pct=0.65,
                    net_positioning=0.15,
                    confidence_score=0.75,
                    transaction_count=50,
                    forward_return=0.05,
                )
            ],
            correlation_breadth_vs_return=0.15,
            correlation_net_vs_return=0.20,
            correlation_confidence_vs_return=0.10,
            signal_positive_hit_rate=0.55,
            signal_negative_hit_rate=0.52,
            benchmark_total_return=0.08,
            average_forward_return=0.03,
            warnings=["Test warning"],
        )

        report = format_backtest_report(result)

        assert "BACKTEST REPORT" in report
        assert "2024-01-01" in report
        assert "CORRELATION ANALYSIS" in report
        assert "HIT RATES" in report
        assert "DISCLAIMER" in report

    def test_format_with_none_values(self):
        """Test formatting with None values."""
        config = BacktestConfig(
            start_date="2024-01-01",
            end_date="2024-06-30",
        )
        result = BacktestResult(
            config=config,
            signal_points=[],
            correlation_breadth_vs_return=None,
            correlation_net_vs_return=None,
            correlation_confidence_vs_return=None,
            signal_positive_hit_rate=None,
            signal_negative_hit_rate=None,
            benchmark_total_return=None,
            average_forward_return=None,
        )

        report = format_backtest_report(result)

        # Should have N/A placeholders
        assert "N/A" in report


class TestRunBacktest:
    """Test the backtest runner."""

    @patch("cppi.backtest.engine.fetch_index_prices")
    @patch("cppi.backtest.engine._compute_historical_positioning")
    def test_run_backtest_empty_db(
        self, mock_positioning, mock_prices, tmp_path
    ):
        """Test running backtest with empty database."""
        # Mock no positioning data
        mock_positioning.return_value = None
        mock_prices.return_value = []

        # Create empty database
        import sqlite3
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS filings (
                filing_id TEXT PRIMARY KEY
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY
            )
        """)
        conn.close()

        config = BacktestConfig(
            start_date="2024-01-01",
            end_date="2024-02-01",
        )

        result = run_backtest(db_path, config)

        # Should complete without error
        assert result is not None
        assert len(result.warnings) >= 3  # Standard warnings

    @patch("cppi.backtest.engine.fetch_index_prices")
    @patch("cppi.backtest.engine._compute_historical_positioning")
    def test_run_backtest_with_data(
        self, mock_positioning, mock_prices, tmp_path
    ):
        """Test running backtest with mocked data."""
        # Mock positioning data
        mock_positioning.return_value = {
            "breadth_pct": 0.65,
            "net_positioning": 0.15,
            "confidence_score": 0.75,
            "transaction_count": 50,
        }

        # Mock price data with returns
        mock_prices.return_value = [
            PricePoint(
                date=datetime(2024, 1, 1),
                open=100.0, high=100.0, low=100.0,
                close=100.0, volume=1000, adj_close=100.0,
            ),
            PricePoint(
                date=datetime(2024, 2, 1),
                open=105.0, high=105.0, low=105.0,
                close=105.0, volume=1000, adj_close=105.0,
            ),
        ]

        # Create empty database
        import sqlite3
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS filings (
                filing_id TEXT PRIMARY KEY,
                chamber TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY,
                filing_id TEXT,
                resolved_ticker TEXT,
                transaction_type TEXT,
                execution_date TEXT
            )
        """)
        conn.close()

        config = BacktestConfig(
            start_date="2024-01-01",
            end_date="2024-01-15",  # Short period
            rebalance_frequency_days=7,
        )

        result = run_backtest(db_path, config)

        assert result is not None
        assert len(result.signal_points) > 0


class TestStoreHistoricalScores:
    """Test storing historical scores."""

    def test_store_scores(self, tmp_path):
        """Test storing scores to database."""
        import sqlite3
        db_path = str(tmp_path / "test.db")

        # Create database with schema
        conn = sqlite3.connect(db_path)
        conn.close()

        signal_points = [
            SignalPoint(
                as_of_date="2024-01-15",
                breadth_pct=0.65,
                net_positioning=0.15,
                confidence_score=0.75,
                transaction_count=50,
            ),
            SignalPoint(
                as_of_date="2024-01-22",
                breadth_pct=0.60,
                net_positioning=0.10,
                confidence_score=0.70,
                transaction_count=45,
            ),
        ]

        count = store_historical_scores(
            db_path, signal_points, "all", 90
        )

        assert count == 2

        # Verify stored data
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT COUNT(*) FROM historical_scores"
        )
        assert cursor.fetchone()[0] == 2
        conn.close()

    def test_store_empty_list(self, tmp_path):
        """Test storing empty list."""
        import sqlite3
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.close()

        count = store_historical_scores(db_path, [], "all", 90)

        assert count == 0
