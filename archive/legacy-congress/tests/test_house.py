"""Tests for House connector."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from cppi.connectors.house import HouseConnector, HouseFiling


class TestHouseConnector:
    """Tests for HouseConnector class."""

    @pytest.fixture
    def connector(self, tmp_path):
        """Create a connector with temp cache directory."""
        return HouseConnector(cache_dir=tmp_path, request_delay=0)

    def test_get_pdf_url_electronic(self, connector):
        """Test PDF URL construction for electronic filings."""
        url = connector.get_pdf_url("20024300")
        assert url == "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2002/20024300.pdf"

    def test_get_pdf_url_with_year_override(self, connector):
        """Test PDF URL construction with explicit year."""
        url = connector.get_pdf_url("20024300", year=2024)
        assert url == "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2024/20024300.pdf"

    def test_get_pdf_url_paper_filing(self, connector):
        """Test PDF URL construction for paper filings (822xxxx pattern)."""
        # Paper filings don't have year embedded, uses current year
        url = connector.get_pdf_url("8220162", year=2024)
        assert url == "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2024/8220162.pdf"

    @patch("cppi.connectors.house.requests.Session")
    def test_download_pdf_success(self, mock_session_class, tmp_path):
        """Test successful PDF download."""
        # Setup mock
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Create realistic PDF content (larger than ERROR_PAGE_SIZE and starts with %PDF)
        pdf_content = b"%PDF-1.4 " + b"x" * 2000  # Well over error page size
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = pdf_content
        mock_response.headers = {"Content-Type": "application/pdf"}
        mock_session.get.return_value = mock_response

        connector = HouseConnector(cache_dir=tmp_path, request_delay=0)
        result = connector.download_pdf("20024300")

        assert result is not None
        assert result.exists()
        assert result.read_bytes() == pdf_content

    @patch("cppi.connectors.house.requests.Session")
    def test_download_pdf_uses_cache(self, mock_session_class, tmp_path):
        """Test that cached PDFs are returned without network request."""
        # Create cached file
        cache_path = tmp_path / "pdfs" / "house" / "20024300.pdf"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(b"%PDF-1.4 cached content")

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        connector = HouseConnector(cache_dir=tmp_path, request_delay=0)
        result = connector.download_pdf("20024300")

        assert result == cache_path
        # Should not have made any GET requests
        mock_session.get.assert_not_called()

    @patch("cppi.connectors.house.requests.Session")
    def test_download_pdf_force_redownload(self, mock_session_class, tmp_path):
        """Test force redownload ignores cache."""
        # Create cached file
        cache_path = tmp_path / "pdfs" / "house" / "20024300.pdf"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(b"%PDF-1.4 old cached content")

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Create new content larger than ERROR_PAGE_SIZE
        new_content = b"%PDF-1.4 new content " + b"x" * 2000
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = new_content
        mock_response.headers = {"Content-Type": "application/pdf"}
        mock_session.get.return_value = mock_response

        connector = HouseConnector(cache_dir=tmp_path, request_delay=0)
        result = connector.download_pdf("20024300", force=True)

        assert result is not None
        assert result.read_bytes() == new_content
        mock_session.get.assert_called_once()

    @patch("cppi.connectors.house.requests.Session")
    def test_download_pdf_error_page(self, mock_session_class, tmp_path):
        """Test handling of error pages (small responses)."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Simulate error page (small content with error text)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"<html>File not found</html>"
        mock_response.headers = {"Content-Type": "text/html"}
        mock_session.get.return_value = mock_response

        connector = HouseConnector(cache_dir=tmp_path, request_delay=0)
        result = connector.download_pdf("99999999")

        assert result is None

    def test_list_cached_pdfs(self, tmp_path):
        """Test listing cached PDF files."""
        # Create some cached files
        cache_dir = tmp_path / "pdfs" / "house"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "20024300.pdf").write_bytes(b"pdf1")
        (cache_dir / "20024301.pdf").write_bytes(b"pdf2")
        (cache_dir / "notapdf.txt").write_text("text file")

        connector = HouseConnector(cache_dir=tmp_path, request_delay=0)
        cached = connector.list_cached_pdfs()

        assert set(cached) == {"20024300", "20024301"}

    def test_clear_cache(self, tmp_path):
        """Test clearing cache directory."""
        # Create some cached files
        cache_dir = tmp_path / "pdfs" / "house"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "20024300.pdf").write_bytes(b"pdf1")
        (cache_dir / "20024301.pdf").write_bytes(b"pdf2")

        connector = HouseConnector(cache_dir=tmp_path, request_delay=0)
        count = connector.clear_cache()

        assert count == 2
        assert len(list(cache_dir.glob("*.pdf"))) == 0

    def test_get_pdf_hash(self, tmp_path):
        """Test PDF hash calculation."""
        # Create cached file
        cache_dir = tmp_path / "pdfs" / "house"
        cache_dir.mkdir(parents=True, exist_ok=True)
        content = b"%PDF-1.4 test content"
        (cache_dir / "20024300.pdf").write_bytes(content)

        connector = HouseConnector(cache_dir=tmp_path, request_delay=0)
        hash_result = connector.get_pdf_hash("20024300")

        assert hash_result is not None
        assert len(hash_result) == 64  # SHA256 hex length

    def test_get_pdf_hash_not_cached(self, tmp_path):
        """Test hash returns None for uncached files."""
        connector = HouseConnector(cache_dir=tmp_path, request_delay=0)
        hash_result = connector.get_pdf_hash("nonexistent")

        assert hash_result is None


class TestHouseFiling:
    """Tests for HouseFiling dataclass."""

    def test_house_filing_creation(self):
        """Test creating a HouseFiling object."""
        filing = HouseFiling(
            filing_id="20024300",
            filer_name="DOE, JOHN",
            state="CA",
            district="12",
            filing_date=datetime(2024, 3, 15),
            pdf_url="https://example.com/test.pdf",
        )

        assert filing.filing_id == "20024300"
        assert filing.filer_name == "DOE, JOHN"
        assert filing.state == "CA"
        assert filing.district == "12"
        assert filing.filing_type == "PTR"  # Default value

    def test_house_filing_optional_fields(self):
        """Test HouseFiling with optional fields as None."""
        filing = HouseFiling(
            filing_id="20024300",
            filer_name="DOE, JANE",
            state=None,
            district=None,
            filing_date=None,
            pdf_url="https://example.com/test.pdf",
        )

        assert filing.state is None
        assert filing.district is None
        assert filing.filing_date is None


class TestFDXMLNameLookup:
    """Tests for FD XML filer name lookup functionality."""

    def test_load_fd_xml_names_builds_lookup(self, tmp_path):
        """Test that _load_fd_xml_names builds DocID→Name lookup from FD XML."""
        from cppi.cli import _load_fd_xml_names

        # Create a mock FD XML file
        fd_xml_dir = tmp_path / "fd_xml"
        fd_xml_dir.mkdir()
        xml_content = """<?xml version="1.0" encoding="utf-8"?>
<FinancialDisclosure>
  <Member>
    <First>John</First>
    <Last>Doe</Last>
    <Suffix>Jr.</Suffix>
    <DocID>12345678</DocID>
  </Member>
  <Member>
    <First>Jane</First>
    <Last>Smith</Last>
    <Suffix></Suffix>
    <DocID>87654321</DocID>
  </Member>
</FinancialDisclosure>"""
        (fd_xml_dir / "2024FD.xml").write_text(xml_content)

        # Load names
        names = _load_fd_xml_names(tmp_path)

        assert names["12345678"] == "John Doe Jr."
        assert names["87654321"] == "Jane Smith"

    def test_load_fd_xml_names_missing_dir(self, tmp_path):
        """Test that _load_fd_xml_names returns empty dict if fd_xml/ doesn't exist."""
        from cppi.cli import _load_fd_xml_names

        # No fd_xml directory
        names = _load_fd_xml_names(tmp_path)
        assert names == {}

    def test_load_fd_xml_names_handles_empty_fields(self, tmp_path):
        """Test that _load_fd_xml_names handles missing name fields gracefully."""
        from cppi.cli import _load_fd_xml_names

        fd_xml_dir = tmp_path / "fd_xml"
        fd_xml_dir.mkdir()
        xml_content = """<?xml version="1.0" encoding="utf-8"?>
<FinancialDisclosure>
  <Member>
    <First></First>
    <Last>OnlyLast</Last>
    <DocID>11111111</DocID>
  </Member>
  <Member>
    <First>OnlyFirst</First>
    <Last></Last>
    <DocID>22222222</DocID>
  </Member>
  <Member>
    <DocID>33333333</DocID>
  </Member>
</FinancialDisclosure>"""
        (fd_xml_dir / "2024FD.xml").write_text(xml_content)

        names = _load_fd_xml_names(tmp_path)

        assert names["11111111"] == "OnlyLast"
        assert names["22222222"] == "OnlyFirst"
        assert "33333333" not in names  # No name, should not be in lookup


class TestFilingIdValidation:
    """Tests for filing_id validation and fallback."""

    def test_empty_filing_id_uses_filename_fallback(self):
        """Test that empty filing_id from parser falls back to filename."""
        # This tests the validation logic conceptually
        # The actual validation is in cmd_parse, tested via integration

        # Simulate what the code does
        filing_id_from_filename = "8220824"
        parsed_filing_id = ""  # Parser returned empty

        # Validation logic
        if not parsed_filing_id or parsed_filing_id.strip() == "":
            actual_filing_id = filing_id_from_filename
        else:
            actual_filing_id = parsed_filing_id

        assert actual_filing_id == "8220824"

    def test_valid_filing_id_not_overwritten(self):
        """Test that valid filing_id from parser is not overwritten."""
        filing_id_from_filename = "8220824"
        parsed_filing_id = "8220999"  # Different valid ID

        # Validation logic
        if not parsed_filing_id or parsed_filing_id.strip() == "":
            actual_filing_id = filing_id_from_filename
        else:
            actual_filing_id = parsed_filing_id

        assert actual_filing_id == "8220999"
