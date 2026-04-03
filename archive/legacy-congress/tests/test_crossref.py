"""Tests for cross-reference analysis module."""

import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cppi.analysis.crossref import (
    CrossRefMatch,
    CrossRefReport,
    TickerSignal,
    check_data_freshness,
    compute_crossref_signals,
    format_crossref_report,
    get_cppi_signals,
    get_insider_signals,
    normalize_ticker,
    run_crossref_analysis,
)


class TestTickerNormalization:
    """Tests for ticker normalization."""

    def test_normalize_goog_to_googl(self):
        assert normalize_ticker("GOOG") == "GOOGL"

    def test_normalize_brk_variants(self):
        assert normalize_ticker("BRK.A") == "BRK-B"
        assert normalize_ticker("BRK/A") == "BRK-B"

    def test_normalize_passes_through_unknown(self):
        assert normalize_ticker("AAPL") == "AAPL"
        assert normalize_ticker("MSFT") == "MSFT"

    def test_normalize_empty_string(self):
        assert normalize_ticker("") == ""

    def test_normalize_lowercase(self):
        assert normalize_ticker("aapl") == "AAPL"


class TestTickerSignal:
    """Tests for TickerSignal dataclass."""

    def test_from_transactions_bullish(self):
        sig = TickerSignal.from_transactions("AAPL", buys=100000, sells=20000, count=10)
        assert sig.signal == "BULLISH"
        assert sig.net_value == 80000
        assert sig.transaction_count == 10

    def test_from_transactions_bearish(self):
        sig = TickerSignal.from_transactions("AAPL", buys=20000, sells=100000, count=5)
        assert sig.signal == "BEARISH"
        assert sig.net_value == -80000

    def test_from_transactions_neutral(self):
        sig = TickerSignal.from_transactions("AAPL", buys=50000, sells=50000, count=2)
        assert sig.signal == "NEUTRAL"
        assert sig.net_value == 0


class TestCrossRefMatch:
    """Tests for CrossRefMatch classification."""

    def test_both_bullish_is_convergent(self):
        congress = TickerSignal("AAPL", 100000, 10, "BULLISH")
        insider = TickerSignal("AAPL", 50000, 5, "BULLISH")
        match = CrossRefMatch("AAPL", congress, insider, False, "")
        assert match.is_convergent is True
        assert match.match_type == "BOTH_BULLISH"

    def test_both_bearish_is_convergent(self):
        congress = TickerSignal("AAPL", -100000, 10, "BEARISH")
        insider = TickerSignal("AAPL", -50000, 5, "BEARISH")
        match = CrossRefMatch("AAPL", congress, insider, False, "")
        assert match.is_convergent is True
        assert match.match_type == "BOTH_BEARISH"

    def test_opposing_signals_is_divergent(self):
        congress = TickerSignal("AAPL", 100000, 10, "BULLISH")
        insider = TickerSignal("AAPL", -50000, 5, "BEARISH")
        match = CrossRefMatch("AAPL", congress, insider, False, "")
        assert match.is_convergent is False
        assert match.match_type == "DIVERGENT"

    def test_one_neutral_is_mixed(self):
        congress = TickerSignal("AAPL", 100000, 10, "BULLISH")
        insider = TickerSignal("AAPL", 0, 5, "NEUTRAL")
        match = CrossRefMatch("AAPL", congress, insider, False, "")
        assert match.is_convergent is False
        assert match.match_type == "MIXED"


class TestGetInsiderSignals:
    """Tests for get_insider_signals function."""

    def test_returns_empty_dict_when_db_not_found(self):
        result = get_insider_signals("/nonexistent/path.db", ["AAPL"], 90)
        assert result == {}

    def test_returns_empty_dict_for_empty_tickers(self):
        result = get_insider_signals("/tmp/test.db", [], 90)
        assert result == {}

    @patch("cppi.analysis.crossref.sqlite3.connect")
    def test_returns_dict_structure(self, mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("AAPL", "P", 100000.0, 5),  # ticker, code, value, count
            ("AAPL", "S", 20000.0, 2),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        with patch("os.path.exists", return_value=True):
            result = get_insider_signals("/tmp/test.db", ["AAPL"], 90)

        assert "AAPL" in result
        assert isinstance(result["AAPL"], TickerSignal)
        assert result["AAPL"].signal == "BULLISH"


class TestGetCppiSignals:
    """Tests for get_cppi_signals function."""

    def test_returns_empty_dict_when_db_not_found(self):
        result = get_cppi_signals("/nonexistent/path.db", 90)
        assert result == {}


class TestCrossRefReport:
    """Tests for CrossRefReport dataclass."""

    def test_agreement_rate_calculation(self):
        report = CrossRefReport(
            window_days=90,
            cppi_ticker_count=100,
            insider_ticker_count=50,
            overlapping_ticker_count=30,
            convergent_bullish=[MagicMock()] * 10,
            convergent_bearish=[MagicMock()] * 5,
            divergent=[MagicMock()] * 15,
        )
        # (10 + 5) / (10 + 5 + 15) = 15/30 = 0.5
        assert report.agreement_rate == 0.5

    def test_agreement_rate_zero_overlap(self):
        report = CrossRefReport(
            window_days=90,
            cppi_ticker_count=100,
            insider_ticker_count=50,
            overlapping_ticker_count=0,
        )
        assert report.agreement_rate == 0.0

    def test_to_dict(self):
        report = CrossRefReport(
            window_days=90,
            cppi_ticker_count=100,
            insider_ticker_count=50,
            overlapping_ticker_count=30,
            warnings=["test warning"],
        )
        d = report.to_dict()
        assert d["window_days"] == 90
        assert d["cppi_ticker_count"] == 100
        assert "test warning" in d["warnings"]


class TestComputeCrossrefSignals:
    """Tests for compute_crossref_signals function."""

    def test_handles_missing_cppi_db(self):
        report = compute_crossref_signals(
            cppi_db_path="/nonexistent/cppi.db",
            insider_db_path="/nonexistent/insider.db",
            window_days=90,
        )
        assert report.cppi_ticker_count == 0
        assert "No CPPI transactions in window" in report.warnings

    def test_handles_no_overlapping_tickers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create empty CPPI DB
            cppi_db = Path(tmpdir) / "cppi.db"
            conn = sqlite3.connect(cppi_db)
            conn.execute("""
                CREATE TABLE transactions (
                    resolved_ticker TEXT,
                    transaction_type TEXT,
                    amount_min INTEGER,
                    amount_max INTEGER,
                    amount_midpoint REAL,
                    execution_date TEXT,
                    include_in_signal INTEGER
                )
            """)
            # Insert a transaction
            conn.execute("""
                INSERT INTO transactions VALUES ('AAPL', 'purchase', 1000, 15000, NULL, '2026-03-01', 1)
            """)
            conn.commit()
            conn.close()

            report = compute_crossref_signals(
                cppi_db_path=str(cppi_db),
                insider_db_path="/nonexistent/insider.db",
                window_days=90,
            )

            assert report.cppi_ticker_count >= 1
            assert report.overlapping_ticker_count == 0


class TestFormatCrossrefReport:
    """Tests for format_crossref_report function."""

    def test_formats_report_with_data(self):
        congress = TickerSignal("NVDA", 1000000, 10, "BULLISH")
        insider = TickerSignal("NVDA", -500000, 20, "BEARISH")
        match = CrossRefMatch("NVDA", congress, insider, False, "DIVERGENT")

        report = CrossRefReport(
            window_days=90,
            cppi_ticker_count=100,
            insider_ticker_count=50,
            overlapping_ticker_count=10,
            divergent=[match],
        )

        formatted = format_crossref_report(report)
        assert "CONGRESSIONAL / INSIDER CROSS-REFERENCE" in formatted
        assert "Window: 90 days" in formatted
        assert "DIVERGENT: 1 tickers" in formatted
        assert "NVDA" in formatted

    def test_formats_report_no_overlap(self):
        report = CrossRefReport(
            window_days=90,
            cppi_ticker_count=100,
            insider_ticker_count=0,
            overlapping_ticker_count=0,
        )

        formatted = format_crossref_report(report)
        assert "No overlapping tickers found" in formatted

    def test_includes_warnings(self):
        report = CrossRefReport(
            window_days=90,
            cppi_ticker_count=0,
            insider_ticker_count=0,
            overlapping_ticker_count=0,
            warnings=["Test warning message"],
        )

        formatted = format_crossref_report(report)
        assert "Test warning message" in formatted


class TestCheckDataFreshness:
    """Tests for check_data_freshness function."""

    def test_returns_false_when_db_not_found(self):
        is_fresh, msg = check_data_freshness(
            "/nonexistent/db.sqlite", "transactions", "date", 90
        )
        assert is_fresh is False
        assert "not found" in msg

    def test_returns_true_for_fresh_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE transactions (date TEXT)")
            conn.execute(
                "INSERT INTO transactions VALUES (?)",
                (datetime.now().strftime("%Y-%m-%d"),),
            )
            conn.commit()
            conn.close()

            is_fresh, msg = check_data_freshness(str(db_path), "transactions", "date", 90)
            assert is_fresh is True
            assert msg is None


class TestRunCrossrefAnalysis:
    """Integration tests for run_crossref_analysis."""

    def test_uses_default_insider_db_path(self):
        with patch("cppi.analysis.crossref.compute_crossref_signals") as mock:
            mock.return_value = CrossRefReport(
                window_days=90,
                cppi_ticker_count=0,
                insider_ticker_count=0,
                overlapping_ticker_count=0,
            )

            run_crossref_analysis("/tmp/cppi.db", window_days=90)

            call_args = mock.call_args
            # Should use default path when not specified
            assert call_args.kwargs.get("insider_db_path") is not None


class TestCLICrossrefMissingDB:
    """Tests for CLI handling of missing database."""

    def test_cli_handles_missing_db_gracefully(self):
        report = compute_crossref_signals(
            cppi_db_path="/nonexistent/cppi.db",
            insider_db_path="/nonexistent/insider.db",
            window_days=90,
        )
        # Should not raise, should return report with warnings
        assert report is not None
        assert len(report.warnings) > 0
