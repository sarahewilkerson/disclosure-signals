"""Tests for SEC EDGAR Form 4 ingestion."""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock, AsyncMock, PropertyMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import init_db, get_connection
from ingestion import (
    EdgarClient,
    AsyncEdgarClient,
    search_form4_filings,
    search_form4_filings_async,
    resolve_filing_xml_url,
    resolve_filing_xml_url_async,
    download_filing_xml,
    download_filing_xml_async,
    ingest_company,
    ingest_company_async,
)


class TestEdgarClient(unittest.TestCase):
    """Tests for the rate-limited EDGAR HTTP client."""

    @patch("ingestion.requests.Session")
    def test_client_sets_user_agent(self, mock_session_class):
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        client = EdgarClient()

        mock_session.headers.update.assert_called_once()
        call_args = mock_session.headers.update.call_args[0][0]
        self.assertIn("User-Agent", call_args)

    @patch("ingestion.time.sleep")
    @patch("ingestion.time.time")
    @patch("ingestion.requests.Session")
    def test_throttling_enforced(self, mock_session_class, mock_time, mock_sleep):
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_response = MagicMock()
        mock_session.get.return_value = mock_response

        # Simulate rapid requests
        mock_time.side_effect = [0.0, 0.0, 0.05, 0.05, 0.2]

        client = EdgarClient()
        client.get("http://example.com")
        client.get("http://example.com")

        # Should have slept to enforce rate limit
        mock_sleep.assert_called()

    @patch("ingestion.time.sleep")
    @patch("ingestion.requests.Session")
    def test_retry_on_failure(self, mock_session_class, mock_sleep):
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # First two calls fail, third succeeds
        import requests
        mock_session.get.side_effect = [
            requests.RequestException("Connection error"),
            requests.RequestException("Timeout"),
            MagicMock(status_code=200),
        ]

        client = EdgarClient()
        client._last_request_time = 0

        with patch("ingestion.time.time", return_value=1.0):
            response = client.get("http://example.com", retries=3)

        self.assertEqual(mock_session.get.call_count, 3)

    @patch("ingestion.requests.Session")
    def test_raises_after_max_retries(self, mock_session_class):
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        import requests
        mock_session.get.side_effect = requests.RequestException("Always fails")

        client = EdgarClient()
        client._last_request_time = 0

        with patch("ingestion.time.time", return_value=1.0):
            with patch("ingestion.time.sleep"):
                with self.assertRaises(requests.RequestException):
                    client.get("http://example.com", retries=2)


class TestSearchForm4Filings(unittest.TestCase):
    """Tests for EFTS API search."""

    def setUp(self):
        self.mock_client = MagicMock()

    def test_search_parses_response(self):
        efts_response = {
            "hits": {
                "total": {"value": 2},
                "hits": [
                    {
                        "_source": {
                            "adsh": "0001234567-24-000001",
                            "file_date": "2024-01-15",
                            "root_form": "4",
                            "ciks": [320193, 1234567],
                            "display_names": ["DOE JOHN"],
                        }
                    },
                    {
                        "_source": {
                            "adsh": "0001234567-24-000002",
                            "file_date": "2024-01-16",
                            "root_form": "4/A",
                            "ciks": [320193, 1234567],
                            "display_names": ["DOE JOHN"],
                        }
                    },
                ]
            }
        }

        mock_response = MagicMock()
        mock_response.json.return_value = efts_response
        self.mock_client.get.return_value = mock_response

        filings = search_form4_filings(self.mock_client, "0000320193", max_results=100)

        self.assertEqual(len(filings), 2)
        self.assertEqual(filings[0]["accession_number"], "0001234567-24-000001")
        self.assertEqual(filings[0]["is_amendment"], False)
        self.assertEqual(filings[1]["is_amendment"], True)

    def test_search_filters_by_issuer_cik(self):
        # Filing that doesn't include our issuer CIK should be filtered
        efts_response = {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {
                        "_source": {
                            "adsh": "0001234567-24-000001",
                            "file_date": "2024-01-15",
                            "root_form": "4",
                            "ciks": [999999],  # Different CIK
                        }
                    },
                ]
            }
        }

        mock_response = MagicMock()
        mock_response.json.return_value = efts_response
        self.mock_client.get.return_value = mock_response

        filings = search_form4_filings(self.mock_client, "0000320193")

        self.assertEqual(len(filings), 0)

    def test_search_handles_empty_response(self):
        efts_response = {"hits": {"total": {"value": 0}, "hits": []}}

        mock_response = MagicMock()
        mock_response.json.return_value = efts_response
        self.mock_client.get.return_value = mock_response

        filings = search_form4_filings(self.mock_client, "0000320193")

        self.assertEqual(len(filings), 0)

    def test_search_handles_api_error(self):
        self.mock_client.get.side_effect = Exception("API error")

        filings = search_form4_filings(self.mock_client, "0000320193")

        self.assertEqual(len(filings), 0)


class TestResolveFilingXmlUrl(unittest.TestCase):
    """Tests for XML URL resolution."""

    def setUp(self):
        self.mock_client = MagicMock()

    def test_resolve_from_index(self):
        index_response = {
            "directory": {
                "item": [
                    {"name": "primary_doc.xml", "type": "file"},
                    {"name": "0001234567-24-000001-index.json", "type": "file"},
                ]
            }
        }

        mock_response = MagicMock()
        mock_response.json.return_value = index_response
        self.mock_client.get.return_value = mock_response

        xml_url, filer_cik = resolve_filing_xml_url(
            self.mock_client, "0001234567-24-000001"
        )

        self.assertIsNotNone(xml_url)
        self.assertIn("primary_doc.xml", xml_url)
        self.assertEqual(filer_cik, "0001234567")

    def test_resolve_extracts_filer_cik(self):
        index_response = {"directory": {"item": []}}

        mock_response = MagicMock()
        mock_response.json.return_value = index_response
        self.mock_client.get.return_value = mock_response

        xml_url, filer_cik = resolve_filing_xml_url(
            self.mock_client, "0007654321-24-000001"
        )

        self.assertEqual(filer_cik, "0007654321")

    def test_resolve_invalid_accession_format(self):
        xml_url, filer_cik = resolve_filing_xml_url(
            self.mock_client, "invalid-format"
        )

        self.assertIsNone(xml_url)
        self.assertIsNone(filer_cik)


class TestDownloadFilingXml(unittest.TestCase):
    """Tests for XML download and caching."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("ingestion.config.FILINGS_CACHE_DIR")
    def test_download_creates_file(self, mock_cache_dir):
        mock_cache_dir.__str__ = lambda x: self.temp_dir
        # Need to actually set the value
        with patch("ingestion.config.FILINGS_CACHE_DIR", self.temp_dir):
            mock_response = MagicMock()
            mock_response.content = b"<ownershipDocument>test</ownershipDocument>"
            self.mock_client.get.return_value = mock_response

            path = download_filing_xml(
                self.mock_client,
                "http://example.com/doc.xml",
                "0001234567-24-000001"
            )

            self.assertIsNotNone(path)
            self.assertTrue(os.path.exists(path))
            with open(path, "rb") as f:
                content = f.read()
            self.assertIn(b"ownershipDocument", content)

    @patch("ingestion.config.FILINGS_CACHE_DIR")
    def test_download_uses_cache(self, mock_cache_dir):
        with patch("ingestion.config.FILINGS_CACHE_DIR", self.temp_dir):
            # Create cached file
            cached_path = os.path.join(self.temp_dir, "0001234567_24_000001.xml")
            os.makedirs(os.path.dirname(cached_path), exist_ok=True)
            with open(cached_path, "w") as f:
                f.write("<cached>content</cached>")

            path = download_filing_xml(
                self.mock_client,
                "http://example.com/doc.xml",
                "0001234567-24-000001"
            )

            # Should not have made HTTP request
            self.mock_client.get.assert_not_called()
            self.assertEqual(path, cached_path)


class TestIngestCompany(unittest.TestCase):
    """Tests for company ingestion pipeline."""

    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        init_db(self.db_path)
        self.temp_dir = tempfile.mkdtemp()

        # Insert company for FK constraint
        with get_connection(self.db_path) as conn:
            conn.execute("""
                INSERT INTO companies (cik, ticker, company_name, fortune_rank, revenue, sector)
                VALUES ('0000320193', 'AAPL', 'Apple Inc.', 1, 394328, 'Technology')
            """)

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("ingestion.download_filing_xml")
    @patch("ingestion.resolve_filing_xml_url")
    @patch("ingestion.search_form4_filings")
    def test_ingest_stores_filings(self, mock_search, mock_resolve, mock_download):
        mock_search.return_value = [
            {
                "accession_number": "0001234567-24-000001",
                "filing_date": "2024-01-15",
                "form_type": "4",
                "is_amendment": False,
                "issuer_cik": "0000320193",
            }
        ]
        mock_resolve.return_value = ("http://example.com/doc.xml", "0001234567")
        mock_download.return_value = "/tmp/test.xml"

        mock_client = MagicMock()
        results = ingest_company(
            "320193", client=mock_client, db_path=self.db_path
        )

        self.assertEqual(len(results), 1)

        # Verify filing stored in DB
        with get_connection(self.db_path) as conn:
            filings = conn.execute("SELECT * FROM filings").fetchall()
            self.assertEqual(len(filings), 1)
            self.assertEqual(filings[0]["accession_number"], "0001234567-24-000001")

    @patch("ingestion.download_filing_xml")
    @patch("ingestion.resolve_filing_xml_url")
    @patch("ingestion.search_form4_filings")
    def test_ingest_skips_existing(self, mock_search, mock_resolve, mock_download):
        # Pre-insert a filing
        with get_connection(self.db_path) as conn:
            conn.execute("""
                INSERT INTO filings (accession_number, cik_issuer, is_officer,
                    is_director, is_ten_pct_owner, is_other, is_amendment, aff10b5one,
                    additional_owners)
                VALUES ('0001234567-24-000001', '0000320193', 0, 0, 0, 0, 0, 0, NULL)
            """)

        mock_search.return_value = [
            {
                "accession_number": "0001234567-24-000001",
                "filing_date": "2024-01-15",
                "form_type": "4",
                "is_amendment": False,
                "issuer_cik": "0000320193",
            }
        ]

        mock_client = MagicMock()
        results = ingest_company(
            "320193", client=mock_client, db_path=self.db_path
        )

        # Should not download existing filing
        mock_resolve.assert_not_called()
        mock_download.assert_not_called()
        self.assertEqual(len(results), 0)

    @patch("ingestion.download_filing_xml")
    @patch("ingestion.resolve_filing_xml_url")
    @patch("ingestion.search_form4_filings")
    def test_ingest_respects_max_filings(self, mock_search, mock_resolve, mock_download):
        mock_search.return_value = [
            {"accession_number": f"0001-24-{i:06d}", "filing_date": "2024-01-15",
             "form_type": "4", "is_amendment": False, "issuer_cik": "0000320193"}
            for i in range(10)
        ]
        mock_resolve.return_value = ("http://example.com/doc.xml", "0001")
        mock_download.return_value = "/tmp/test.xml"

        mock_client = MagicMock()
        results = ingest_company(
            "320193", client=mock_client, db_path=self.db_path, max_filings=3
        )

        self.assertEqual(len(results), 3)


class TestEdgarClient4xxFastFail(unittest.TestCase):
    """Tests for 4xx client error fast-fail behavior (no retries)."""

    @patch("ingestion.time.sleep")
    @patch("ingestion.requests.Session")
    def test_404_raises_immediately_no_retry(self, mock_session_class, mock_sleep):
        """404 Not Found should raise immediately without retrying."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        import requests
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_error = requests.HTTPError("404 Not Found")
        http_error.response = mock_response
        mock_session.get.return_value.raise_for_status.side_effect = http_error

        client = EdgarClient()
        client._last_request_time = 0

        with patch("ingestion.time.time", return_value=1.0):
            with self.assertRaises(requests.HTTPError) as context:
                client.get("http://example.com/not-found", retries=3)

        # Should NOT have retried - only one attempt
        self.assertEqual(mock_session.get.call_count, 1)
        # Should NOT have slept for retry backoff
        mock_sleep.assert_not_called()

    @patch("ingestion.time.sleep")
    @patch("ingestion.requests.Session")
    def test_400_raises_immediately_no_retry(self, mock_session_class, mock_sleep):
        """400 Bad Request should raise immediately without retrying."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        import requests
        mock_response = MagicMock()
        mock_response.status_code = 400
        http_error = requests.HTTPError("400 Bad Request")
        http_error.response = mock_response
        mock_session.get.return_value.raise_for_status.side_effect = http_error

        client = EdgarClient()
        client._last_request_time = 0

        with patch("ingestion.time.time", return_value=1.0):
            with self.assertRaises(requests.HTTPError):
                client.get("http://example.com/bad-request", retries=3)

        self.assertEqual(mock_session.get.call_count, 1)
        mock_sleep.assert_not_called()

    @patch("ingestion.time.sleep")
    @patch("ingestion.requests.Session")
    def test_500_retries_with_backoff(self, mock_session_class, mock_sleep):
        """500 Server Error should retry with exponential backoff."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        import requests
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_error = requests.HTTPError("500 Server Error")
        http_error.response = mock_response
        mock_session.get.return_value.raise_for_status.side_effect = http_error

        client = EdgarClient()
        client._last_request_time = 0

        with patch("ingestion.time.time", return_value=1.0):
            with self.assertRaises(requests.HTTPError):
                client.get("http://example.com/server-error", retries=3)

        # Should have retried 3 times (all 3 attempts made)
        self.assertEqual(mock_session.get.call_count, 3)
        # Should have slept (includes throttle + retry backoff sleeps)
        # At minimum 2 retry sleeps for 3 attempts
        self.assertGreaterEqual(mock_sleep.call_count, 2)


class TestAsyncEdgarClient4xxFastFail(unittest.TestCase):
    """Tests for async 4xx client error fast-fail behavior (no retries)."""

    def test_async_404_raises_immediately_no_retry(self):
        """404 Not Found should raise immediately without retrying in async client."""
        async def test():
            import httpx

            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_response.request = MagicMock()

            http_error = httpx.HTTPStatusError(
                "404 Not Found",
                request=mock_response.request,
                response=mock_response
            )
            mock_client.get = AsyncMock(side_effect=http_error)

            client = AsyncEdgarClient()
            client._client = mock_client
            client._last_request_time = 0

            with patch("ingestion.time.time", return_value=1.0):
                with patch("ingestion.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                    with self.assertRaises(httpx.HTTPStatusError):
                        await client.get("http://example.com/not-found", retries=3)

                    # Should NOT have retried - only one attempt
                    self.assertEqual(mock_client.get.call_count, 1)
                    # Should NOT have slept for retry backoff
                    mock_sleep.assert_not_called()

        asyncio.run(test())

    def test_async_500_retries_with_backoff(self):
        """500 Server Error should retry with exponential backoff in async client."""
        async def test():
            import httpx

            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.request = MagicMock()

            http_error = httpx.HTTPStatusError(
                "500 Server Error",
                request=mock_response.request,
                response=mock_response
            )
            mock_client.get = AsyncMock(side_effect=http_error)

            client = AsyncEdgarClient()
            client._client = mock_client
            client._last_request_time = 0

            with patch("ingestion.time.time", return_value=1.0):
                with patch("ingestion.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                    with self.assertRaises(httpx.HTTPStatusError):
                        await client.get("http://example.com/server-error", retries=3)

                    # Should have retried 3 times (all 3 attempts made)
                    self.assertEqual(mock_client.get.call_count, 3)
                    # Should have slept (includes throttle + retry backoff sleeps)
                    # At minimum 2 retry sleeps for 3 attempts
                    self.assertGreaterEqual(mock_sleep.call_count, 2)

        asyncio.run(test())


class TestAsyncEdgarClient(unittest.TestCase):
    """Tests for the async rate-limited EDGAR HTTP client."""

    def test_client_initializes(self):
        client = AsyncEdgarClient()
        self.assertIsNone(client._client)  # Lazy initialization
        self.assertEqual(client._last_request_time, 0.0)

    def test_async_context_manager(self):
        async def test():
            async with AsyncEdgarClient() as client:
                self.assertIsNotNone(client)
            # After exit, client should be closed
            self.assertIsNone(client._client)

        asyncio.run(test())

    @patch("ingestion.httpx.AsyncClient")
    def test_async_throttling(self, mock_client_class):
        async def test():
            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client_class.return_value = mock_client

            client = AsyncEdgarClient()
            client._client = mock_client

            # Provide enough time values for throttle checks and request timestamps
            with patch("ingestion.time.time", side_effect=[0.0, 0.01, 0.02, 0.03, 0.04, 0.05]):
                with patch("ingestion.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                    await client.get("http://example.com")
                    await client.get("http://example.com")
                    # Should have made both requests
                    self.assertEqual(mock_client.get.call_count, 2)

        asyncio.run(test())


class TestAsyncSearchForm4Filings(unittest.TestCase):
    """Tests for async EFTS API search."""

    def test_async_search_parses_response(self):
        async def test():
            mock_client = AsyncMock(spec=AsyncEdgarClient)

            efts_response = {
                "hits": {
                    "total": {"value": 2},
                    "hits": [
                        {
                            "_source": {
                                "adsh": "0001234567-24-000001",
                                "file_date": "2024-01-15",
                                "root_form": "4",
                                "ciks": [320193, 1234567],
                                "display_names": ["DOE JOHN"],
                            }
                        },
                        {
                            "_source": {
                                "adsh": "0001234567-24-000002",
                                "file_date": "2024-01-16",
                                "root_form": "4/A",
                                "ciks": [320193, 1234567],
                                "display_names": ["DOE JOHN"],
                            }
                        },
                    ]
                }
            }

            mock_response = MagicMock()
            mock_response.json.return_value = efts_response
            mock_client.get = AsyncMock(return_value=mock_response)

            filings = await search_form4_filings_async(
                mock_client, "0000320193", max_results=100
            )

            self.assertEqual(len(filings), 2)
            self.assertEqual(filings[0]["accession_number"], "0001234567-24-000001")
            self.assertEqual(filings[0]["is_amendment"], False)
            self.assertEqual(filings[1]["is_amendment"], True)

        asyncio.run(test())

    def test_async_search_handles_empty_response(self):
        async def test():
            mock_client = AsyncMock(spec=AsyncEdgarClient)
            efts_response = {"hits": {"total": {"value": 0}, "hits": []}}

            mock_response = MagicMock()
            mock_response.json.return_value = efts_response
            mock_client.get = AsyncMock(return_value=mock_response)

            filings = await search_form4_filings_async(mock_client, "0000320193")

            self.assertEqual(len(filings), 0)

        asyncio.run(test())


class TestAsyncResolveFilingXmlUrl(unittest.TestCase):
    """Tests for async XML URL resolution."""

    def test_async_resolve_from_index(self):
        async def test():
            mock_client = AsyncMock(spec=AsyncEdgarClient)

            index_response = {
                "directory": {
                    "item": [
                        {"name": "primary_doc.xml", "type": "file"},
                        {"name": "0001234567-24-000001-index.json", "type": "file"},
                    ]
                }
            }

            mock_response = MagicMock()
            mock_response.json.return_value = index_response
            mock_client.get = AsyncMock(return_value=mock_response)

            xml_url, filer_cik = await resolve_filing_xml_url_async(
                mock_client, "0001234567-24-000001"
            )

            self.assertIsNotNone(xml_url)
            self.assertIn("primary_doc.xml", xml_url)
            self.assertEqual(filer_cik, "0001234567")

        asyncio.run(test())

    def test_async_resolve_invalid_format(self):
        async def test():
            mock_client = AsyncMock(spec=AsyncEdgarClient)

            xml_url, filer_cik = await resolve_filing_xml_url_async(
                mock_client, "invalid-format"
            )

            self.assertIsNone(xml_url)
            self.assertIsNone(filer_cik)

        asyncio.run(test())


class TestAsyncDownloadFilingXml(unittest.TestCase):
    """Tests for async XML download."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_async_download_creates_file(self):
        async def test():
            with patch("ingestion.config.FILINGS_CACHE_DIR", self.temp_dir):
                mock_client = AsyncMock(spec=AsyncEdgarClient)
                mock_response = MagicMock()
                mock_response.content = b"<ownershipDocument>test</ownershipDocument>"
                mock_client.get = AsyncMock(return_value=mock_response)

                path = await download_filing_xml_async(
                    mock_client,
                    "http://example.com/doc.xml",
                    "0001234567-24-000001"
                )

                self.assertIsNotNone(path)
                self.assertTrue(os.path.exists(path))
                with open(path, "rb") as f:
                    content = f.read()
                self.assertIn(b"ownershipDocument", content)

        asyncio.run(test())

    def test_async_download_uses_cache(self):
        async def test():
            with patch("ingestion.config.FILINGS_CACHE_DIR", self.temp_dir):
                # Create cached file
                cached_path = os.path.join(self.temp_dir, "0001234567_24_000001.xml")
                os.makedirs(os.path.dirname(cached_path), exist_ok=True)
                with open(cached_path, "w") as f:
                    f.write("<cached>content</cached>")

                mock_client = AsyncMock(spec=AsyncEdgarClient)

                path = await download_filing_xml_async(
                    mock_client,
                    "http://example.com/doc.xml",
                    "0001234567-24-000001"
                )

                # Should not have made HTTP request
                mock_client.get.assert_not_called()
                self.assertEqual(path, cached_path)

        asyncio.run(test())


class TestAsyncIngestCompany(unittest.TestCase):
    """Tests for async company ingestion pipeline."""

    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        init_db(self.db_path)
        self.temp_dir = tempfile.mkdtemp()

        # Insert company for FK constraint
        with get_connection(self.db_path) as conn:
            conn.execute("""
                INSERT INTO companies (cik, ticker, company_name, fortune_rank, revenue, sector)
                VALUES ('0000320193', 'AAPL', 'Apple Inc.', 1, 394328, 'Technology')
            """)

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("ingestion.download_filing_xml_async")
    @patch("ingestion.resolve_filing_xml_url_async")
    @patch("ingestion.search_form4_filings_async")
    def test_async_ingest_stores_filings(self, mock_search, mock_resolve, mock_download):
        async def test():
            mock_search.return_value = [
                {
                    "accession_number": "0001234567-24-000001",
                    "filing_date": "2024-01-15",
                    "form_type": "4",
                    "is_amendment": False,
                    "issuer_cik": "0000320193",
                }
            ]
            mock_resolve.return_value = ("http://example.com/doc.xml", "0001234567")
            mock_download.return_value = "/tmp/test.xml"

            mock_client = AsyncMock(spec=AsyncEdgarClient)
            mock_client.close = AsyncMock()

            results = await ingest_company_async(
                "320193", client=mock_client, db_path=self.db_path
            )

            self.assertEqual(len(results), 1)

            # Verify filing stored in DB
            with get_connection(self.db_path) as conn:
                filings = conn.execute("SELECT * FROM filings").fetchall()
                self.assertEqual(len(filings), 1)
                self.assertEqual(filings[0]["accession_number"], "0001234567-24-000001")

        asyncio.run(test())

    @patch("ingestion.download_filing_xml_async")
    @patch("ingestion.resolve_filing_xml_url_async")
    @patch("ingestion.search_form4_filings_async")
    def test_async_ingest_skips_existing(self, mock_search, mock_resolve, mock_download):
        async def test():
            # Pre-insert a filing
            with get_connection(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO filings (accession_number, cik_issuer, is_officer,
                        is_director, is_ten_pct_owner, is_other, is_amendment, aff10b5one,
                        additional_owners)
                    VALUES ('0001234567-24-000001', '0000320193', 0, 0, 0, 0, 0, 0, NULL)
                """)

            mock_search.return_value = [
                {
                    "accession_number": "0001234567-24-000001",
                    "filing_date": "2024-01-15",
                    "form_type": "4",
                    "is_amendment": False,
                    "issuer_cik": "0000320193",
                }
            ]

            mock_client = AsyncMock(spec=AsyncEdgarClient)
            mock_client.close = AsyncMock()

            results = await ingest_company_async(
                "320193", client=mock_client, db_path=self.db_path
            )

            # Should not download existing filing
            mock_resolve.assert_not_called()
            mock_download.assert_not_called()
            self.assertEqual(len(results), 0)

        asyncio.run(test())


if __name__ == "__main__":
    unittest.main()
