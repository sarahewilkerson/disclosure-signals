"""Tests for Senate connector."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from cppi.connectors.senate import SenateConnector, SenateFiling, SenateTransaction


class TestSenateConnector:
    """Tests for SenateConnector class."""

    @pytest.fixture
    def connector(self, tmp_path):
        """Create a connector with temp cache directory."""
        return SenateConnector(cache_dir=tmp_path, request_delay=0)

    def test_get_ptr_url(self, connector):
        """Test PTR URL construction."""
        url = connector.get_ptr_url("0068462f-1234-5678-9abc-def012345678")
        assert url == "https://efdsearch.senate.gov/search/view/ptr/0068462f-1234-5678-9abc-def012345678/"

    def test_get_ptr_url_uppercase(self, connector):
        """Test PTR URL normalizes UUID to lowercase."""
        url = connector.get_ptr_url("0068462F-1234-5678-9ABC-DEF012345678")
        assert url == "https://efdsearch.senate.gov/search/view/ptr/0068462f-1234-5678-9abc-def012345678/"

    def test_get_paper_url(self, connector):
        """Test paper filing URL construction."""
        url = connector.get_paper_url("00181DE2-1234-5678-9ABC-DEF012345678")
        assert url == "https://efdsearch.senate.gov/search/view/paper/00181DE2-1234-5678-9ABC-DEF012345678/"

    @patch("cppi.connectors.senate.requests.Session")
    def test_establish_session_success(self, mock_session_class, tmp_path):
        """Test successful session establishment."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock GET response with CSRF token
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.text = """
        <html>
        <form>
            <input name="csrfmiddlewaretoken" value="test_csrf_token_123">
        </form>
        </html>
        """

        # Mock POST response (redirect to search)
        mock_post_response = MagicMock()
        mock_post_response.status_code = 302
        mock_post_response.url = "https://efdsearch.senate.gov/search/"

        mock_session.get.return_value = mock_get_response
        mock_session.post.return_value = mock_post_response
        mock_session.cookies = {"search_agreement": "true"}

        connector = SenateConnector(cache_dir=tmp_path, request_delay=0)
        result = connector.establish_session()

        assert result is True
        assert connector._session_established is True

    @patch("cppi.connectors.senate.requests.Session")
    def test_establish_session_no_csrf(self, mock_session_class, tmp_path):
        """Test session establishment fails without CSRF token."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock GET response without CSRF token
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.text = "<html><body>No form here</body></html>"
        mock_session.get.return_value = mock_get_response

        connector = SenateConnector(cache_dir=tmp_path, request_delay=0)
        result = connector.establish_session()

        assert result is False
        assert connector._session_established is False

    @patch("cppi.connectors.senate.requests.Session")
    def test_download_ptr_success(self, mock_session_class, tmp_path):
        """Test successful PTR download."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Setup for session establishment
        mock_get_home = MagicMock()
        mock_get_home.status_code = 200
        mock_get_home.text = '<input name="csrfmiddlewaretoken" value="token123">'
        mock_get_home.url = "https://efdsearch.senate.gov/search/home/"

        mock_post = MagicMock()
        mock_post.status_code = 302
        mock_post.url = "https://efdsearch.senate.gov/search/"

        mock_session.cookies = {"search_agreement": "true"}

        # Setup for PTR download - must be > 1000 chars to pass size check
        ptr_html = """
        <html>
        <head><title>Senator Financial Disclosure</title></head>
        <body>
        <div class="container">
        <h1>Periodic Transaction Report</h1>
        <p>Filer: SENATOR NAME</p>
        <table class="table table-striped">
            <thead><tr><th>Date</th><th>Owner</th><th>Ticker</th><th>Asset</th><th>Type</th><th>Transaction</th><th>Amount</th></tr></thead>
            <tbody>
                <tr>
                    <td>04/24/2024</td>
                    <td>Self</td>
                    <td><a href="https://finance.yahoo.com/quote/AAPL">AAPL</a></td>
                    <td>Apple Inc.</td>
                    <td>Stock</td>
                    <td>Purchase</td>
                    <td>$1,001 - $15,000</td>
                </tr>
            </tbody>
        </table>
        <footer>This is footer content to make the page larger than the minimum size check</footer>
        </div>
        </body>
        </html>
        """ + " " * 500  # Pad to ensure > 1000 chars
        mock_get_ptr = MagicMock()
        mock_get_ptr.status_code = 200
        mock_get_ptr.text = ptr_html
        mock_get_ptr.url = "https://efdsearch.senate.gov/search/view/ptr/testid/"

        # Return different responses based on URL
        def mock_get(url, **kwargs):
            if "home" in url:
                return mock_get_home
            else:
                return mock_get_ptr

        mock_session.get.side_effect = mock_get
        mock_session.post.return_value = mock_post

        connector = SenateConnector(cache_dir=tmp_path, request_delay=0)
        result = connector.download_ptr("0068462f-1234-5678-9abc-def012345678")

        assert result is not None
        assert result.exists()
        assert "table" in result.read_text()

    @patch("cppi.connectors.senate.requests.Session")
    def test_download_ptr_uses_cache(self, mock_session_class, tmp_path):
        """Test that cached PTRs are returned without network request."""
        # Create cached file
        cache_dir = tmp_path / "pdfs" / "senate"
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / "ptr_0068462f.html"
        cache_path.write_text("<html>cached content</html>")

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        connector = SenateConnector(cache_dir=tmp_path, request_delay=0)
        connector._session_established = True  # Skip session establishment
        result = connector.download_ptr("0068462f-1234-5678-9abc-def012345678")

        assert result == cache_path
        # Should not have made any GET requests
        mock_session.get.assert_not_called()

    def test_parse_ptr_transactions(self, tmp_path):
        """Test parsing transactions from PTR HTML."""
        # Create test HTML file
        cache_dir = tmp_path / "pdfs" / "senate"
        cache_dir.mkdir(parents=True, exist_ok=True)
        html_path = cache_dir / "ptr_test.html"
        html_path.write_text("""
        <html>
        <table class="table table-striped">
            <thead>
                <tr><th>#</th><th>Transaction Date</th><th>Owner</th><th>Ticker</th>
                    <th>Asset Name</th><th>Asset Type</th><th>Type</th><th>Amount</th></tr>
            </thead>
            <tbody>
                <tr>
                    <td>1</td>
                    <td>04/24/2024</td>
                    <td>Self</td>
                    <td><a href="https://finance.yahoo.com/quote/AAPL">AAPL</a></td>
                    <td>Apple Inc.</td>
                    <td>Stock</td>
                    <td>Purchase</td>
                    <td>$1,001 - $15,000</td>
                </tr>
                <tr>
                    <td>2</td>
                    <td>04/25/2024</td>
                    <td>Spouse</td>
                    <td><a href="https://finance.yahoo.com/quote/MSFT">MSFT</a></td>
                    <td>Microsoft Corporation</td>
                    <td>Stock</td>
                    <td>Sale (Partial)</td>
                    <td>$15,001 - $50,000</td>
                </tr>
            </tbody>
        </table>
        </html>
        """)

        connector = SenateConnector(cache_dir=tmp_path, request_delay=0)
        transactions = connector.parse_ptr_transactions(html_path)

        assert len(transactions) == 2
        assert transactions[0].ticker == "AAPL"
        assert transactions[0].owner == "Self"
        assert transactions[0].transaction_type == "Purchase"
        assert transactions[1].ticker == "MSFT"
        assert transactions[1].owner == "Spouse"
        assert transactions[1].transaction_type == "Sale (Partial)"

    def test_list_cached_files(self, tmp_path):
        """Test listing cached files."""
        # Create some cached files
        cache_dir = tmp_path / "pdfs" / "senate"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "ptr_0068462f.html").write_text("ptr1")
        (cache_dir / "ptr_00f9f9ee.html").write_text("ptr2")
        (cache_dir / "paper_00181DE2.html").write_text("paper1")
        (cache_dir / "paper_00181DE2_page1.gif").write_bytes(b"gif")

        connector = SenateConnector(cache_dir=tmp_path, request_delay=0)
        cached = connector.list_cached_files()

        assert "0068462f" in cached["ptr"]
        assert "00f9f9ee" in cached["ptr"]
        assert "00181DE2" in cached["paper"]

    def test_clear_cache(self, tmp_path):
        """Test clearing cache directory."""
        # Create some cached files
        cache_dir = tmp_path / "pdfs" / "senate"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "ptr_test1.html").write_text("html1")
        (cache_dir / "ptr_test2.html").write_text("html2")
        (cache_dir / "paper_test.gif").write_bytes(b"gif")

        connector = SenateConnector(cache_dir=tmp_path, request_delay=0)
        count = connector.clear_cache()

        assert count == 3
        assert len(list(cache_dir.glob("*"))) == 0


class TestSenateFiling:
    """Tests for SenateFiling dataclass."""

    def test_senate_filing_creation(self):
        """Test creating a SenateFiling object."""
        filing = SenateFiling(
            filing_id="0068462f-1234-5678-9abc-def012345678",
            filer_name="SMITH, JOHN",
            state="TX",
            filing_date=datetime(2024, 4, 15),
            report_url="https://efdsearch.senate.gov/search/view/ptr/0068462f/",
        )

        assert filing.filing_id == "0068462f-1234-5678-9abc-def012345678"
        assert filing.filer_name == "SMITH, JOHN"
        assert filing.state == "TX"
        assert filing.filing_type == "PTR"  # Default value
        assert filing.is_paper is False  # Default value

    def test_senate_filing_paper(self):
        """Test SenateFiling for paper filing."""
        filing = SenateFiling(
            filing_id="00181DE2-1234-5678-9abc-def012345678",
            filer_name="DOE, JANE",
            state="CA",
            filing_date=datetime(2024, 3, 10),
            report_url="https://efdsearch.senate.gov/search/view/paper/00181DE2/",
            is_paper=True,
        )

        assert filing.is_paper is True


class TestSenateTransaction:
    """Tests for SenateTransaction dataclass."""

    def test_senate_transaction_creation(self):
        """Test creating a SenateTransaction object."""
        txn = SenateTransaction(
            transaction_date=datetime(2024, 4, 24),
            owner="Self",
            ticker="AAPL",
            asset_name="Apple Inc.",
            asset_type="Stock",
            transaction_type="Purchase",
            amount_range="$1,001 - $15,000",
            comment=None,
        )

        assert txn.ticker == "AAPL"
        assert txn.owner == "Self"
        assert txn.transaction_type == "Purchase"
        assert txn.amount_range == "$1,001 - $15,000"

    def test_senate_transaction_with_comment(self):
        """Test SenateTransaction with comment."""
        txn = SenateTransaction(
            transaction_date=datetime(2024, 4, 25),
            owner="Spouse",
            ticker="MSFT",
            asset_name="Microsoft Corporation",
            asset_type="Stock",
            transaction_type="Sale (Partial)",
            amount_range="$15,001 - $50,000",
            comment="Rebalancing portfolio",
        )

        assert txn.comment == "Rebalancing portfolio"
