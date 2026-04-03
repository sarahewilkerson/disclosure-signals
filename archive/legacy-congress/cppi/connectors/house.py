"""House of Representatives Financial Disclosure Connector.

Downloads Periodic Transaction Reports (PTRs) from the House Clerk's
disclosure website at disclosures-clerk.house.gov.

URL Patterns:
- Electronic PDFs: https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{YEAR}/{ID}.pdf
- Filing IDs: Electronic (2002xxxx), Paper (822xxxx)

Access Constraints:
- No authentication required
- No CAPTCHAs detected
- Rate limiting implemented as best practice (1 req/sec default)
"""

import hashlib
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from cppi.config import CACHE_DIR, REQUEST_DELAY, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)


@dataclass
class HouseFiling:
    """Represents a House PTR filing."""

    filing_id: str
    filer_name: str
    state: Optional[str]
    district: Optional[str]
    filing_date: Optional[datetime]
    pdf_url: str
    filing_type: str = "PTR"


class HouseConnector:
    """Connector for House financial disclosure site."""

    BASE_URL = "https://disclosures-clerk.house.gov"
    SEARCH_URL = "https://disclosures-clerk.house.gov/FinancialDisclosure/Search"
    PTR_PDF_PATTERN = "/public_disc/ptr-pdfs/{year}/{filing_id}.pdf"

    # Error page signature (404 pages return 200 with specific content)
    ERROR_PAGE_SIZE = 1245  # Approximate size of error page

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        request_delay: float = REQUEST_DELAY,
        timeout: int = REQUEST_TIMEOUT,
    ):
        """Initialize the House connector.

        Args:
            cache_dir: Directory for caching downloaded PDFs
            request_delay: Minimum seconds between requests (rate limiting)
            timeout: Request timeout in seconds
        """
        self.cache_dir = Path(cache_dir or CACHE_DIR) / "pdfs" / "house"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.request_delay = request_delay
        self.timeout = timeout
        self._last_request_time: float = 0

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "CPPI/0.1 (Congressional Policy Research)",
                "Accept": "text/html,application/pdf,*/*",
            }
        )

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, url: str, **kwargs) -> requests.Response:
        """Make a rate-limited GET request."""
        self._rate_limit()
        logger.debug(f"GET {url}")
        response = self.session.get(url, timeout=self.timeout, **kwargs)
        return response

    def _post(self, url: str, data: dict, **kwargs) -> requests.Response:
        """Make a rate-limited POST request."""
        self._rate_limit()
        logger.debug(f"POST {url}")
        response = self.session.post(url, data=data, timeout=self.timeout, **kwargs)
        return response

    def get_pdf_url(self, filing_id: str, year: Optional[int] = None) -> str:
        """Construct PDF URL for a filing ID.

        Args:
            filing_id: The filing ID (e.g., "20024300" or "8220162")
            year: Optional year override. If not provided, inferred from ID.

        Returns:
            Full URL to the PDF file
        """
        if year is None:
            # Infer year from filing ID prefix
            if filing_id.startswith("822"):
                # Paper filings - year not embedded, use current year as default
                year = datetime.now().year
            else:
                # Electronic filings have year in first 4 digits (2002-2026)
                year = int(filing_id[:4])
                # Normalize years like 2002 -> 2002, 2024 -> 2024
                if year < 2000:
                    year = 2000 + year

        pdf_path = self.PTR_PDF_PATTERN.format(year=year, filing_id=filing_id)
        return urljoin(self.BASE_URL, pdf_path)

    def download_pdf(
        self,
        filing_id: str,
        year: Optional[int] = None,
        force: bool = False,
    ) -> Optional[Path]:
        """Download a PTR PDF to the cache.

        Args:
            filing_id: The filing ID
            year: Optional year override
            force: If True, re-download even if cached

        Returns:
            Path to cached PDF, or None if download failed
        """
        cache_path = self.cache_dir / f"{filing_id}.pdf"

        # Return cached file if exists and not forcing
        if cache_path.exists() and not force:
            logger.debug(f"Using cached PDF: {cache_path}")
            return cache_path

        url = self.get_pdf_url(filing_id, year)

        try:
            response = self._get(url)

            # Check for error page (returns 200 but with error content)
            if (
                response.status_code == 200
                and len(response.content) < self.ERROR_PAGE_SIZE + 100
            ):
                # Small response likely indicates error page
                if b"not found" in response.content.lower() or b"error" in response.content.lower():
                    logger.warning(f"Filing not found: {filing_id}")
                    return None

            response.raise_for_status()

            # Verify we got a PDF
            content_type = response.headers.get("Content-Type", "")
            if "pdf" not in content_type.lower() and not response.content.startswith(b"%PDF"):
                logger.warning(f"Response is not a PDF for {filing_id}: {content_type}")
                return None

            # Write to cache
            cache_path.write_bytes(response.content)
            logger.info(f"Downloaded PDF: {filing_id} ({len(response.content)} bytes)")
            return cache_path

        except requests.RequestException as e:
            logger.error(f"Failed to download {filing_id}: {e}")
            return None

    def get_pdf_hash(self, filing_id: str) -> Optional[str]:
        """Get SHA256 hash of a cached PDF.

        Args:
            filing_id: The filing ID

        Returns:
            Hex digest of SHA256 hash, or None if not cached
        """
        cache_path = self.cache_dir / f"{filing_id}.pdf"
        if not cache_path.exists():
            return None
        return hashlib.sha256(cache_path.read_bytes()).hexdigest()

    def search_ptrs(
        self,
        year: int,
        filing_type: str = "P",  # P = Periodic Transaction Report
        state: Optional[str] = None,
    ) -> list[HouseFiling]:
        """Search for PTR filings.

        Note: The House disclosure search requires JavaScript for full functionality.
        This method scrapes the initial search results page. For comprehensive
        listing, consider using the XML feeds if available.

        Args:
            year: Filing year to search
            filing_type: Filing type code (P = PTR, A = Annual)
            state: Optional state filter (two-letter code)

        Returns:
            List of HouseFiling objects found
        """
        # The House search form uses POST with specific parameters
        search_data = {
            "LastName": "",
            "FilingYear": str(year),
            "State": state or "",
            "District": "",
            "ReportType": filing_type,
        }

        try:
            response = self._post(self.SEARCH_URL, data=search_data)
            response.raise_for_status()

            return self._parse_search_results(response.text, year)

        except requests.RequestException as e:
            logger.error(f"Search failed: {e}")
            return []

    def _parse_search_results(self, html: str, year: int) -> list[HouseFiling]:
        """Parse search results HTML to extract filing information.

        Args:
            html: Raw HTML response
            year: The year being searched

        Returns:
            List of parsed HouseFiling objects
        """
        filings = []
        soup = BeautifulSoup(html, "lxml")

        # Look for result rows in the table
        # Structure varies but typically has a results table
        results_table = soup.find("table", {"class": re.compile(r"result|data|report", re.I)})
        if not results_table:
            # Try finding by ID or other patterns
            results_table = soup.find("table", {"id": re.compile(r"result|data|report", re.I)})

        if not results_table:
            logger.debug("No results table found in search response")
            return filings

        rows = results_table.find_all("tr")
        for row in rows[1:]:  # Skip header row
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            try:
                # Extract filing ID from PDF link
                pdf_link = row.find("a", href=re.compile(r"\.pdf", re.I))
                if not pdf_link:
                    continue

                href = pdf_link.get("href", "")
                filing_id_match = re.search(r"/(\d+)\.pdf", href)
                if not filing_id_match:
                    continue

                filing_id = filing_id_match.group(1)

                # Extract filer name
                filer_name = cells[0].get_text(strip=True)

                # Extract state/district if available
                state = None
                district = None
                if len(cells) > 1:
                    location = cells[1].get_text(strip=True)
                    if "-" in location:
                        parts = location.split("-")
                        state = parts[0].strip()
                        district = parts[1].strip() if len(parts) > 1 else None
                    else:
                        state = location

                # Extract filing date if available
                filing_date = None
                if len(cells) > 2:
                    date_text = cells[2].get_text(strip=True)
                    try:
                        filing_date = datetime.strptime(date_text, "%m/%d/%Y")
                    except ValueError:
                        pass

                filing = HouseFiling(
                    filing_id=filing_id,
                    filer_name=filer_name,
                    state=state,
                    district=district,
                    filing_date=filing_date,
                    pdf_url=self.get_pdf_url(filing_id, year),
                )
                filings.append(filing)

            except Exception as e:
                logger.debug(f"Error parsing row: {e}")
                continue

        logger.info(f"Found {len(filings)} filings in search results")
        return filings

    def list_cached_pdfs(self) -> list[str]:
        """List all cached PDF filing IDs.

        Returns:
            List of filing IDs with cached PDFs
        """
        return [f.stem for f in self.cache_dir.glob("*.pdf")]

    def clear_cache(self) -> int:
        """Clear all cached PDFs.

        Returns:
            Number of files deleted
        """
        count = 0
        for pdf_file in self.cache_dir.glob("*.pdf"):
            pdf_file.unlink()
            count += 1
        logger.info(f"Cleared {count} cached PDFs")
        return count
