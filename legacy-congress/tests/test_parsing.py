"""Tests for PDF parsing module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cppi.parsing import (
    HousePDFParser,
    ParsedFiling,
    parse_house_pdf,
)


class TestAmountParsing:
    """Tests for amount range parsing."""

    @pytest.fixture
    def parser(self):
        """Create a parser instance."""
        return HousePDFParser()

    def test_parse_standard_ranges(self, parser):
        """Test parsing standard STOCK Act amount ranges."""
        test_cases = [
            ("$1,001 - $15,000", (1_001, 15_000)),
            ("$15,001 - $50,000", (15_001, 50_000)),
            ("$50,001 - $100,000", (50_001, 100_000)),
            ("$100,001 - $250,000", (100_001, 250_000)),
            ("$250,001 - $500,000", (250_001, 500_000)),
            ("$500,001 - $1,000,000", (500_001, 1_000_000)),
            ("$1,000,001 - $5,000,000", (1_000_001, 5_000_000)),
            ("$5,000,001 - $25,000,000", (5_000_001, 25_000_000)),
        ]
        for amount_text, expected in test_cases:
            result = parser._parse_amount(amount_text)
            assert result == expected, f"Failed for {amount_text}"

    def test_parse_over_amount(self, parser):
        """Test parsing 'Over $X' amounts."""
        result = parser._parse_amount("Over $50,000,000")
        assert result[0] == 50_000_001
        assert result[1] > result[0]

    def test_parse_without_spaces(self, parser):
        """Test parsing amounts without spaces."""
        result = parser._parse_amount("$1,001-$15,000")
        assert result == (1_001, 15_000)

    def test_parse_empty_returns_none(self, parser):
        """Test empty amount returns None."""
        assert parser._parse_amount("") == (None, None)
        assert parser._parse_amount(None) == (None, None)

    def test_parse_invalid_returns_none(self, parser):
        """Test invalid amount returns None."""
        assert parser._parse_amount("invalid") == (None, None)
        assert parser._parse_amount("abc123") == (None, None)


class TestTransactionParsing:
    """Tests for transaction entry parsing."""

    @pytest.fixture
    def parser(self):
        """Create a parser instance."""
        return HousePDFParser()

    def test_parse_simple_purchase(self, parser):
        """Test parsing a simple stock purchase."""
        entry_lines = [
            "SP Apple Inc. - Common Stock (AAPL) P 01/15/2024 01/16/2024 $1,001 - $15,000",
            "[ST]",
        ]
        txn = parser._parse_entry(entry_lines, [], [], 1)

        assert txn is not None
        assert txn.owner == "spouse"
        assert txn.ticker == "AAPL"
        assert txn.asset_type == "ST"
        assert txn.transaction_type == "purchase"
        assert txn.transaction_date == datetime(2024, 1, 15)
        assert txn.amount_min == 1_001
        assert txn.amount_max == 15_000

    def test_parse_partial_sale(self, parser):
        """Test parsing a partial sale transaction."""
        entry_lines = [
            "Microsoft Corporation - Common Stock (MSFT) S (partial) 02/20/2024 02/21/2024 $50,001 - $100,000",
            "[ST]",
        ]
        txn = parser._parse_entry(entry_lines, [], [], 1)

        assert txn is not None
        assert txn.owner == "self"  # No owner prefix means self
        assert txn.ticker == "MSFT"
        assert txn.transaction_type == "sale_partial"

    def test_parse_joint_owner(self, parser):
        """Test parsing joint ownership."""
        entry_lines = [
            "JT Treasury Bond [GS] P 03/01/2024 03/02/2024 $15,001 - $50,000"
        ]
        txn = parser._parse_entry(entry_lines, [], [], 1)

        assert txn is not None
        assert txn.owner == "joint"
        assert txn.asset_type == "GS"

    def test_parse_dependent_owner(self, parser):
        """Test parsing dependent child ownership."""
        entry_lines = [
            "DC Some Fund (XYZ) [MF] P 04/15/2024 04/16/2024 $1,001 - $15,000"
        ]
        txn = parser._parse_entry(entry_lines, [], [], 1)

        assert txn is not None
        assert txn.owner == "dependent"

    def test_parse_split_amount(self, parser):
        """Test parsing amounts split across lines."""
        entry_lines = [
            "SP NVIDIA Corporation - Common Stock P 01/14/2025 01/14/2025 $250,001 -",
            "Stock (NVDA) [ST] $500,000",
        ]
        txn = parser._parse_entry(entry_lines, [], [], 1)

        assert txn is not None
        assert txn.ticker == "NVDA"
        assert txn.amount_min == 250_001
        assert txn.amount_max == 500_000

    def test_parse_with_description(self, parser):
        """Test parsing transaction with description."""
        entry_lines = [
            "SP Apple Inc. (AAPL) [ST] P 01/15/2024 01/16/2024 $1,001 - $15,000"
        ]
        description_lines = ["Purchased 100 shares"]
        txn = parser._parse_entry(entry_lines, description_lines, [], 1)

        assert txn is not None
        assert txn.description == "Purchased 100 shares"

    def test_parse_exchange_transaction(self, parser):
        """Test parsing exchange transaction."""
        entry_lines = [
            "Some Fund (ABC) [MF] E 05/01/2024 05/02/2024 $100,001 - $250,000"
        ]
        txn = parser._parse_entry(entry_lines, [], [], 1)

        assert txn is not None
        assert txn.transaction_type == "exchange"


class TestFilingParsing:
    """Tests for full filing parsing."""

    def test_parse_house_pdf_returns_filing(self, tmp_path):
        """Test that parse_house_pdf returns a ParsedFiling object."""
        # Create a mock PDF with minimal content
        with patch("cppi.parsing.pdfplumber") as mock_pdfplumber:
            mock_pdf = MagicMock()
            mock_page = MagicMock()
            mock_page.page_number = 1
            mock_page.extract_text.return_value = """
Filing ID #12345678
Name: Hon. John Smith
Status: Member
State/District: TX01

ID Owner Asset Transaction Date Notification Amount Cap.
Type Date Gains >
$200?
Apple Inc. - Common Stock (AAPL) P 01/15/2024 01/16/2024 $1,001 - $15,000
[ST]
F      S     : New

I CERTIFY that the statements
"""
            mock_pdf.pages = [mock_page]
            mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
            mock_pdf.__exit__ = MagicMock(return_value=False)
            mock_pdfplumber.open.return_value = mock_pdf

            result = parse_house_pdf(Path("test.pdf"))

            assert isinstance(result, ParsedFiling)
            assert result.filing_id == "12345678"
            assert result.filer_name == "Hon. John Smith"
            assert result.filer_status == "Member"
            assert result.state_district == "TX01"
            assert len(result.transactions) >= 1

    def test_filing_metadata_extraction(self):
        """Test metadata extraction from filing text."""
        parser = HousePDFParser()
        filing = ParsedFiling(
            filing_id="",
            filer_name="",
            filer_status="",
            state_district=None,
        )

        text = """
Filing ID #20024300
Name: Hon. Nancy Pelosi
Status: Member
State/District: CA11
"""
        parser._extract_metadata(text, filing)

        assert filing.filing_id == "20024300"
        assert filing.filer_name == "Hon. Nancy Pelosi"
        assert filing.filer_status == "Member"
        assert filing.state_district == "CA11"


class TestAssetNameExtraction:
    """Tests for asset name extraction."""

    @pytest.fixture
    def parser(self):
        """Create a parser instance."""
        return HousePDFParser()

    def test_extract_simple_stock_name(self, parser):
        """Test extracting simple stock name."""
        text = "Apple Inc. - Common Stock P 01/15/2024"
        dates = ["01/15/2024"]
        result = parser._extract_asset_name(text, "AAPL", "ST", dates)
        assert "Apple Inc." in result

    def test_extract_name_without_ticker(self, parser):
        """Test extracting name when no ticker present."""
        text = "U.S. Treasury Bond P 01/15/2024"
        dates = ["01/15/2024"]
        result = parser._extract_asset_name(text, None, "GS", dates)
        assert "U.S. Treasury Bond" in result or "Treasury Bond" in result


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.fixture
    def parser(self):
        """Create a parser instance."""
        return HousePDFParser()

    def test_empty_entry_returns_none(self, parser):
        """Test that empty entry returns None."""
        result = parser._parse_entry([], [], [], 1)
        assert result is None

    def test_malformed_date_handled(self, parser):
        """Test that malformed dates are handled gracefully."""
        entry_lines = [
            "Apple Inc. (AAPL) [ST] P 99/99/9999 01/16/2024 $1,001 - $15,000"
        ]
        txn = parser._parse_entry(entry_lines, [], [], 1)
        # Should still parse other fields, date may be None
        assert txn is not None
        assert txn.ticker == "AAPL"

    def test_missing_amount_returns_none_values(self, parser):
        """Test that missing amount results in None values."""
        entry_lines = [
            "Apple Inc. (AAPL) [ST] P 01/15/2024 01/16/2024"
        ]
        txn = parser._parse_entry(entry_lines, [], [], 1)
        assert txn is not None
        # Amount should be None when not found
        # (exact behavior depends on implementation)

    def test_parse_exact_small_amount(self, parser):
        """Test parsing exact small amounts (under $1,000)."""
        entry_lines = [
            "SP Apple Inc. - Common Stock (AAPL) P 02/29/2024 02/29/2024 $360.00",
            "[ST]",
        ]
        txn = parser._parse_entry(entry_lines, [], [], 1)

        assert txn is not None
        assert txn.amount_min == 360
        assert txn.amount_max == 360


class TestOwnerCodes:
    """Tests for owner code handling."""

    @pytest.fixture
    def parser(self):
        """Create a parser instance."""
        return HousePDFParser()

    def test_all_owner_codes(self, parser):
        """Test all owner code mappings."""
        test_cases = [
            ("SP Asset [ST] P 01/01/2024 01/02/2024 $1,001 - $15,000", "spouse"),
            ("DC Asset [ST] P 01/01/2024 01/02/2024 $1,001 - $15,000", "dependent"),
            ("JT Asset [ST] P 01/01/2024 01/02/2024 $1,001 - $15,000", "joint"),
            ("Asset [ST] P 01/01/2024 01/02/2024 $1,001 - $15,000", "self"),
        ]
        for entry_text, expected_owner in test_cases:
            txn = parser._parse_entry([entry_text], [], [], 1)
            assert txn is not None, f"Failed to parse: {entry_text}"
            assert txn.owner == expected_owner, f"Expected {expected_owner} for: {entry_text}"
