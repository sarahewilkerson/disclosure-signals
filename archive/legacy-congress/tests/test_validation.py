"""Tests for validation modules."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from cppi.validation.quiver import (
    QuiverClient,
    QuiverTransaction,
    fetch_quiver_transactions,
    parse_amount_range,
)
from cppi.validation.validator import (
    MatchResult,
    ValidationReport,
    compare_amounts,
    format_validation_report,
    match_transactions,
    normalize_ticker,
    normalize_transaction_type,
    validate_against_source,
)


class TestParseAmountRange:
    """Test amount range parsing."""

    def test_standard_range(self):
        """Test standard amount range."""
        min_val, max_val = parse_amount_range("$1,001 - $15,000")
        assert min_val == 1001
        assert max_val == 15000

    def test_large_range(self):
        """Test large amount range."""
        min_val, max_val = parse_amount_range("$50,001 - $100,000")
        assert min_val == 50001
        assert max_val == 100000

    def test_single_value(self):
        """Test single value (no range)."""
        min_val, max_val = parse_amount_range("$15,000")
        assert min_val == 15000
        assert max_val == 15000

    def test_empty_string(self):
        """Test empty string."""
        min_val, max_val = parse_amount_range("")
        assert min_val is None
        assert max_val is None

    def test_invalid_format(self):
        """Test invalid format."""
        min_val, max_val = parse_amount_range("not a number")
        assert min_val is None
        assert max_val is None


class TestQuiverTransaction:
    """Test QuiverTransaction dataclass."""

    def test_creation(self):
        """Test creating a QuiverTransaction."""
        txn = QuiverTransaction(
            ticker="AAPL",
            representative="Pelosi, Nancy",
            transaction_type="Purchase",
            transaction_date=datetime(2024, 1, 15),
            disclosure_date=datetime(2024, 2, 1),
            amount_range="$50,001 - $100,000",
            amount_min=50001,
            amount_max=100000,
            house_senate="House",
        )
        assert txn.ticker == "AAPL"
        assert txn.representative == "Pelosi, Nancy"

    def test_to_dict(self):
        """Test to_dict conversion."""
        txn = QuiverTransaction(
            ticker="MSFT",
            representative="Test Rep",
            transaction_type="Sale",
            transaction_date=datetime(2024, 1, 15),
            disclosure_date=None,
            amount_range="$1,001 - $15,000",
            amount_min=1001,
            amount_max=15000,
            house_senate="Senate",
        )
        d = txn.to_dict()
        assert d["ticker"] == "MSFT"
        assert d["transaction_type"] == "Sale"


class TestQuiverClient:
    """Test QuiverClient."""

    def test_client_no_key(self):
        """Test client without API key."""
        with patch.dict("os.environ", {}, clear=True):
            import os
            os.environ.pop("QUIVER_API_KEY", None)

            client = QuiverClient(api_key=None)
            assert client.api_key is None

    @patch("requests.Session")
    def test_get_house_trading(self, mock_session_class):
        """Test fetching House trading data."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "Ticker": "AAPL",
                "Representative": "Test Rep",
                "Transaction": "Purchase",
                "TransactionDate": "2024-01-15",
                "DisclosureDate": "2024-02-01",
                "Range": "$1,001 - $15,000",
            }
        ]
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        client = QuiverClient(api_key="test_key")
        client.session = mock_session
        txns = client.get_house_trading(limit=10)

        assert len(txns) == 1
        assert txns[0].ticker == "AAPL"
        assert txns[0].house_senate == "House"


class TestNormalizeTransactionType:
    """Test transaction type normalization."""

    def test_purchase(self):
        """Test purchase variations."""
        assert normalize_transaction_type("Purchase") == "purchase"
        assert normalize_transaction_type("purchase") == "purchase"
        assert normalize_transaction_type("Buy") == "purchase"

    def test_sale(self):
        """Test sale variations."""
        assert normalize_transaction_type("Sale") == "sale"
        assert normalize_transaction_type("sale") == "sale"
        assert normalize_transaction_type("Sell") == "sale"
        assert normalize_transaction_type("Sale (Full)") == "sale"

    def test_exchange(self):
        """Test exchange."""
        assert normalize_transaction_type("Exchange") == "exchange"

    def test_empty(self):
        """Test empty string."""
        assert normalize_transaction_type("") == "unknown"
        assert normalize_transaction_type(None) == "unknown"


class TestNormalizeTicker:
    """Test ticker normalization."""

    def test_uppercase(self):
        """Test uppercase conversion."""
        assert normalize_ticker("aapl") == "AAPL"

    def test_whitespace(self):
        """Test whitespace trimming."""
        assert normalize_ticker("  MSFT  ") == "MSFT"

    def test_empty(self):
        """Test empty string."""
        assert normalize_ticker("") == ""
        assert normalize_ticker(None) == ""


class TestCompareAmounts:
    """Test amount comparison."""

    def test_exact_match(self):
        """Test exact amount match."""
        match, disc = compare_amounts(1001, 15000, 1001, 15000)
        assert match is True
        assert disc == ""

    def test_overlapping_ranges(self):
        """Test overlapping amount ranges with similar midpoints."""
        # Ranges that overlap and have similar midpoints (realistic match)
        match, disc = compare_amounts(1001, 15000, 1001, 16000)
        assert match is True
        assert disc == ""

    def test_overlapping_but_different_midpoints(self):
        """Test overlapping ranges with significantly different midpoints."""
        # Ranges overlap but midpoints differ by >50%
        match, disc = compare_amounts(1001, 15000, 5000, 20000)
        assert match is False
        assert "Large amount difference" in disc

    def test_no_overlap(self):
        """Test non-overlapping ranges."""
        match, disc = compare_amounts(1001, 15000, 50001, 100000)
        assert match is False
        assert "mismatch" in disc.lower()

    def test_missing_cppi(self):
        """Test missing CPPI amount."""
        match, disc = compare_amounts(None, None, 1001, 15000)
        assert match is False
        assert "CPPI" in disc

    def test_both_missing(self):
        """Test both missing amounts."""
        match, disc = compare_amounts(None, None, None, None)
        assert match is True


class TestMatchTransactions:
    """Test transaction matching."""

    def test_perfect_match(self):
        """Test perfect transaction match."""
        cppi = [
            {
                "filing_id": "F001",
                "resolved_ticker": "AAPL",
                "transaction_type": "purchase",
                "execution_date": "2024-01-15",
                "amount_min": 1001,
                "amount_max": 15000,
            }
        ]
        external = [
            QuiverTransaction(
                ticker="AAPL",
                representative="Test",
                transaction_type="Purchase",
                transaction_date=datetime(2024, 1, 15),
                disclosure_date=None,
                amount_range="$1,001 - $15,000",
                amount_min=1001,
                amount_max=15000,
                house_senate="House",
            )
        ]

        matched, unmatched_cppi, unmatched_ext = match_transactions(cppi, external)

        assert len(matched) == 1
        assert len(unmatched_cppi) == 0
        assert len(unmatched_ext) == 0
        assert matched[0].is_matched is True
        assert matched[0].match_score >= 0.5

    def test_no_match_different_ticker(self):
        """Test no match with different ticker."""
        cppi = [
            {
                "filing_id": "F001",
                "resolved_ticker": "AAPL",
                "transaction_type": "purchase",
                "execution_date": "2024-01-15",
                "amount_min": 1001,
                "amount_max": 15000,
            }
        ]
        external = [
            QuiverTransaction(
                ticker="MSFT",  # Different ticker
                representative="Test",
                transaction_type="Purchase",
                transaction_date=datetime(2024, 1, 15),
                disclosure_date=None,
                amount_range="$1,001 - $15,000",
                amount_min=1001,
                amount_max=15000,
                house_senate="House",
            )
        ]

        matched, unmatched_cppi, unmatched_ext = match_transactions(cppi, external)

        assert len(matched) == 0
        assert len(unmatched_cppi) == 1
        assert len(unmatched_ext) == 1

    def test_date_tolerance(self):
        """Test date tolerance in matching."""
        cppi = [
            {
                "filing_id": "F001",
                "resolved_ticker": "AAPL",
                "transaction_type": "purchase",
                "execution_date": "2024-01-15",
                "amount_min": 1001,
                "amount_max": 15000,
            }
        ]
        external = [
            QuiverTransaction(
                ticker="AAPL",
                representative="Test",
                transaction_type="Purchase",
                transaction_date=datetime(2024, 1, 18),  # 3 days later
                disclosure_date=None,
                amount_range="$1,001 - $15,000",
                amount_min=1001,
                amount_max=15000,
                house_senate="House",
            )
        ]

        matched, _, _ = match_transactions(cppi, external, date_tolerance_days=7)
        assert len(matched) == 1  # Should match within tolerance


class TestValidationReport:
    """Test ValidationReport."""

    def test_creation(self):
        """Test creating a ValidationReport."""
        report = ValidationReport(
            source="quiver",
            validated_at=datetime.now(),
            cppi_transaction_count=100,
            external_transaction_count=90,
            matched_count=80,
            unmatched_cppi_count=20,
            unmatched_external_count=10,
            match_rate=0.8,
            ticker_match_rate=0.95,
            amount_match_rate=0.9,
            discrepancy_summary={"Ticker": 5},
        )
        assert report.source == "quiver"
        assert report.match_rate == 0.8

    def test_to_dict(self):
        """Test to_dict conversion."""
        report = ValidationReport(
            source="quiver",
            validated_at=datetime(2024, 1, 15),
            cppi_transaction_count=100,
            external_transaction_count=90,
            matched_count=80,
            unmatched_cppi_count=20,
            unmatched_external_count=10,
            match_rate=0.8,
            ticker_match_rate=0.95,
            amount_match_rate=0.9,
            discrepancy_summary={},
        )
        d = report.to_dict()
        assert d["source"] == "quiver"
        assert d["match_rate"] == 0.8


class TestValidateAgainstSource:
    """Test validate_against_source function."""

    def test_empty_transactions(self):
        """Test with empty transaction lists."""
        report = validate_against_source([], [], "test")
        assert report.cppi_transaction_count == 0
        assert report.match_rate == 0.0

    def test_basic_validation(self):
        """Test basic validation flow."""
        cppi = [
            {
                "filing_id": "F001",
                "resolved_ticker": "AAPL",
                "transaction_type": "purchase",
                "execution_date": "2024-01-15",
                "amount_min": 1001,
                "amount_max": 15000,
            }
        ]
        external = [
            QuiverTransaction(
                ticker="AAPL",
                representative="Test",
                transaction_type="Purchase",
                transaction_date=datetime(2024, 1, 15),
                disclosure_date=None,
                amount_range="$1,001 - $15,000",
                amount_min=1001,
                amount_max=15000,
                house_senate="House",
            )
        ]

        report = validate_against_source(cppi, external, "quiver")

        assert report.source == "quiver"
        assert report.cppi_transaction_count == 1
        assert report.external_transaction_count == 1
        assert report.match_rate >= 0.5


class TestFormatValidationReport:
    """Test report formatting."""

    def test_format_report(self):
        """Test report formatting."""
        report = ValidationReport(
            source="quiver",
            validated_at=datetime.now(),
            cppi_transaction_count=100,
            external_transaction_count=90,
            matched_count=80,
            unmatched_cppi_count=20,
            unmatched_external_count=10,
            match_rate=0.8,
            ticker_match_rate=0.95,
            amount_match_rate=0.9,
            discrepancy_summary={"Ticker": 5, "Amount": 3},
        )

        text = format_validation_report(report)

        assert "VALIDATION REPORT" in text
        assert "QUIVER" in text
        assert "80%" in text
        assert "MATCH RATES" in text
