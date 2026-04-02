"""Tests for universe management (CIK resolution, CSV loading)."""

import csv
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import init_db, get_connection
from universe import (
    _index_by_ticker,
    _normalize_ticker,
    resolve_cik,
    load_universe_csv,
)


class TestIndexByTicker(unittest.TestCase):
    """Tests for SEC tickers JSON indexing."""

    def test_basic_indexing(self):
        raw = {
            "0": {"cik_str": "320193", "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": "789019", "ticker": "MSFT", "title": "Microsoft Corp."},
        }
        result = _index_by_ticker(raw)

        self.assertIn("AAPL", result)
        self.assertIn("MSFT", result)
        self.assertEqual(result["AAPL"]["cik_str"], "0000320193")
        self.assertEqual(result["MSFT"]["title"], "Microsoft Corp.")

    def test_zero_padded_cik(self):
        raw = {"0": {"cik_str": "123", "ticker": "TEST", "title": "Test Co"}}
        result = _index_by_ticker(raw)
        self.assertEqual(result["TEST"]["cik_str"], "0000000123")

    def test_uppercase_ticker(self):
        raw = {"0": {"cik_str": "123", "ticker": "aapl", "title": "Apple"}}
        result = _index_by_ticker(raw)
        self.assertIn("AAPL", result)
        self.assertNotIn("aapl", result)

    def test_empty_ticker_skipped(self):
        raw = {
            "0": {"cik_str": "123", "ticker": "", "title": "No Ticker"},
            "1": {"cik_str": "456", "ticker": "VALID", "title": "Valid Co"},
        }
        result = _index_by_ticker(raw)
        self.assertEqual(len(result), 1)
        self.assertIn("VALID", result)


class TestNormalizeTicker(unittest.TestCase):
    """Tests for ticker normalization."""

    def test_uppercase(self):
        self.assertEqual(_normalize_ticker("aapl"), "AAPL")

    def test_strip_whitespace(self):
        self.assertEqual(_normalize_ticker("  AAPL  "), "AAPL")

    def test_dots_to_hyphens(self):
        self.assertEqual(_normalize_ticker("BRK.B"), "BRK-B")


class TestResolveCik(unittest.TestCase):
    """Tests for CIK resolution with ticker variants."""

    def setUp(self):
        self.tickers_map = {
            "AAPL": {"cik_str": "0000320193", "ticker": "AAPL", "title": "Apple"},
            "BRK-B": {"cik_str": "0001067983", "ticker": "BRK-B", "title": "Berkshire B"},
            "BRKB": {"cik_str": "0001067984", "ticker": "BRKB", "title": "Alt Berkshire"},
        }

    def test_exact_match(self):
        cik = resolve_cik("AAPL", self.tickers_map)
        self.assertEqual(cik, "0000320193")

    def test_case_insensitive(self):
        cik = resolve_cik("aapl", self.tickers_map)
        self.assertEqual(cik, "0000320193")

    def test_dot_to_hyphen_conversion(self):
        cik = resolve_cik("BRK.B", self.tickers_map)
        self.assertEqual(cik, "0001067983")

    def test_hyphen_to_dot_conversion(self):
        # Add a dot variant to test the reverse conversion
        tickers_map = {"BRK.A": {"cik_str": "0001067985", "ticker": "BRK.A"}}
        cik = resolve_cik("BRK-A", tickers_map)
        self.assertEqual(cik, "0001067985")

    def test_stripped_variant(self):
        cik = resolve_cik("BRK-B", self.tickers_map)
        # Should match BRK-B directly
        self.assertEqual(cik, "0001067983")

    def test_unresolvable_returns_none(self):
        cik = resolve_cik("UNKNOWN", self.tickers_map)
        self.assertIsNone(cik)


class TestLoadUniverseCsv(unittest.TestCase):
    """Tests for loading universe CSV into database."""

    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        init_db(self.db_path)

        # Create test CSV
        self.csv_fd, self.csv_path = tempfile.mkstemp(suffix=".csv")
        with open(self.csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ticker", "company_name", "sector", "rank", "revenue", "cik"])
            writer.writerow(["AAPL", "Apple Inc.", "Technology", "1", "394328", "320193"])
            writer.writerow(["MSFT", "Microsoft", "Technology", "2", "211915", "789019"])

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)
        os.close(self.csv_fd)
        os.unlink(self.csv_path)

    @patch("universe.load_company_tickers_map")
    def test_load_with_provided_cik(self, mock_tickers):
        mock_tickers.return_value = {}  # Not needed when CIK is in CSV

        companies = load_universe_csv(self.csv_path, self.db_path)

        self.assertEqual(len(companies), 2)
        self.assertEqual(companies[0]["ticker"], "AAPL")
        self.assertEqual(companies[0]["cik"], "0000320193")
        self.assertEqual(companies[0]["fortune_rank"], 1)
        self.assertEqual(companies[0]["revenue"], 394328.0)

    @patch("universe.load_company_tickers_map")
    def test_cik_resolved_when_missing(self, mock_tickers):
        mock_tickers.return_value = {
            "GOOG": {"cik_str": "0001652044", "ticker": "GOOG", "title": "Alphabet"}
        }

        # Create CSV without CIK
        csv_path = self.csv_path + ".nocik"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ticker", "company_name", "sector", "rank", "revenue"])
            writer.writerow(["GOOG", "Alphabet Inc.", "Technology", "3", "280522"])

        try:
            companies = load_universe_csv(csv_path, self.db_path)
            self.assertEqual(len(companies), 1)
            self.assertEqual(companies[0]["cik"], "0001652044")
        finally:
            os.unlink(csv_path)

    @patch("universe.load_company_tickers_map")
    def test_unresolvable_ticker_skipped(self, mock_tickers):
        mock_tickers.return_value = {}  # No tickers available

        # Create CSV without CIK
        csv_path = self.csv_path + ".unresolvable"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ticker", "company_name", "sector"])
            writer.writerow(["UNKNOWN", "Unknown Co", "Tech"])

        try:
            companies = load_universe_csv(csv_path, self.db_path)
            self.assertEqual(len(companies), 0)
        finally:
            os.unlink(csv_path)

    def test_file_not_found_raises(self):
        with self.assertRaises(FileNotFoundError):
            load_universe_csv("/nonexistent/file.csv", self.db_path)

    @patch("universe.load_company_tickers_map")
    def test_companies_persisted_to_db(self, mock_tickers):
        mock_tickers.return_value = {}

        load_universe_csv(self.csv_path, self.db_path)

        with get_connection(self.db_path) as conn:
            rows = conn.execute("SELECT * FROM companies ORDER BY ticker").fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["ticker"], "AAPL")
            self.assertEqual(rows[1]["ticker"], "MSFT")


class TestLoadUniverseCsvEdgeCases(unittest.TestCase):
    """Edge case tests for CSV loading."""

    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        init_db(self.db_path)

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)

    @patch("universe.load_company_tickers_map")
    def test_non_numeric_rank_handled(self, mock_tickers):
        mock_tickers.return_value = {}

        csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ticker", "company_name", "rank", "cik"])
            writer.writerow(["TEST", "Test Co", "N/A", "123456"])

        try:
            companies = load_universe_csv(csv_path, self.db_path)
            self.assertEqual(len(companies), 1)
            self.assertIsNone(companies[0]["fortune_rank"])
        finally:
            os.close(csv_fd)
            os.unlink(csv_path)

    @patch("universe.load_company_tickers_map")
    def test_revenue_with_formatting(self, mock_tickers):
        mock_tickers.return_value = {}

        csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ticker", "company_name", "revenue", "cik"])
            writer.writerow(["TEST", "Test Co", "$1,234,567", "123456"])

        try:
            companies = load_universe_csv(csv_path, self.db_path)
            self.assertEqual(companies[0]["revenue"], 1234567.0)
        finally:
            os.close(csv_fd)
            os.unlink(csv_path)


if __name__ == "__main__":
    unittest.main()
