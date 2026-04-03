"""Senate Electronic Financial Disclosure Connector.

Downloads Periodic Transaction Reports (PTRs) from the Senate's
EFD Search website at efdsearch.senate.gov.

URL Patterns:
- Search home: https://efdsearch.senate.gov/search/home/
- Electronic PTR: https://efdsearch.senate.gov/search/view/ptr/{UUID}/
- Paper filing: https://efdsearch.senate.gov/search/view/paper/{UUID}/
- Paper media: https://efd-media-public.senate.gov/media/{YEAR}/{MONTH}/000/{PATH}/{ID}.gif

Access Constraints:
- Session agreement required before accessing reports
- CSRF token required for POST requests
- No CAPTCHAs detected
"""

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

from cppi.config import CACHE_DIR, REQUEST_DELAY, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)


@dataclass
class SenateFiling:
    """Represents a Senate PTR filing."""

    filing_id: str  # UUID
    filer_name: str
    state: Optional[str]
    filing_date: Optional[datetime]
    report_url: str
    filing_type: str = "PTR"
    is_paper: bool = False


@dataclass
class SenateTransaction:
    """Represents a parsed transaction from Senate electronic filing."""

    transaction_date: Optional[datetime]
    owner: str
    ticker: Optional[str]
    asset_name: str
    asset_type: Optional[str]
    transaction_type: str
    amount_range: str
    comment: Optional[str]


class SenateConnector:
    """Connector for Senate financial disclosure site."""

    BASE_URL = "https://efdsearch.senate.gov"
    HOME_URL = "https://efdsearch.senate.gov/search/home/"
    SEARCH_URL = "https://efdsearch.senate.gov/search/"
    PTR_URL_PATTERN = "https://efdsearch.senate.gov/search/view/ptr/{uuid}/"
    PAPER_URL_PATTERN = "https://efdsearch.senate.gov/search/view/paper/{uuid}/"
    MEDIA_BASE = "https://efd-media-public.senate.gov"

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        request_delay: float = REQUEST_DELAY,
        timeout: int = REQUEST_TIMEOUT,
    ):
        """Initialize the Senate connector.

        Args:
            cache_dir: Directory for caching downloaded files
            request_delay: Minimum seconds between requests (rate limiting)
            timeout: Request timeout in seconds
        """
        self.cache_dir = Path(cache_dir or CACHE_DIR) / "pdfs" / "senate"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.request_delay = request_delay
        self.timeout = timeout
        self._last_request_time: float = 0
        self._session_established = False

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "CPPI/0.1 (Congressional Policy Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
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

    def establish_session(self) -> bool:
        """Establish session by accepting the prohibition agreement.

        The Senate EFD site requires users to accept an agreement before
        viewing any disclosure reports.

        Returns:
            True if session established successfully
        """
        if self._session_established:
            return True

        try:
            # Step 1: GET the home page to get CSRF token and cookies
            response = self._get(self.HOME_URL)
            response.raise_for_status()

            # Extract CSRF token from the form
            soup = BeautifulSoup(response.text, "lxml")
            csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
            if not csrf_input:
                logger.error("Could not find CSRF token in home page")
                return False

            csrf_token = csrf_input.get("value", "")

            # Step 2: POST agreement acceptance
            agreement_data = {
                "csrfmiddlewaretoken": csrf_token,
                "prohibition_agreement": "1",
            }

            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": self.HOME_URL,
            }

            response = self._post(self.HOME_URL, data=agreement_data, headers=headers)

            # Check if we're redirected to the search page (success)
            if response.status_code in (200, 302) and (
                "/search/" in response.url or "search_agreement" in str(self.session.cookies)
            ):
                self._session_established = True
                logger.info("Senate session established successfully")
                return True

            # If not redirected, check if we can access search
            test_response = self._get(self.SEARCH_URL)
            if test_response.status_code == 200 and "search" in test_response.url.lower():
                self._session_established = True
                logger.info("Senate session established (verified via search page)")
                return True

            logger.warning("Session establishment may have failed")
            return False

        except requests.RequestException as e:
            logger.error(f"Failed to establish session: {e}")
            return False

    def ensure_session(self) -> bool:
        """Ensure session is established, establishing if needed."""
        if not self._session_established:
            return self.establish_session()
        return True

    def get_ptr_url(self, uuid: str) -> str:
        """Construct PTR URL for a UUID."""
        return self.PTR_URL_PATTERN.format(uuid=uuid.lower())

    def get_paper_url(self, uuid: str) -> str:
        """Construct paper filing URL for a UUID."""
        return self.PAPER_URL_PATTERN.format(uuid=uuid)

    def download_ptr(
        self,
        uuid: str,
        force: bool = False,
    ) -> Optional[Path]:
        """Download a PTR filing (HTML) to the cache.

        Args:
            uuid: The filing UUID
            force: If True, re-download even if cached

        Returns:
            Path to cached HTML file, or None if download failed
        """
        cache_path = self.cache_dir / f"ptr_{uuid[:8]}.html"

        # Return cached file if exists and not forcing
        if cache_path.exists() and not force:
            logger.debug(f"Using cached PTR: {cache_path}")
            return cache_path

        if not self.ensure_session():
            logger.error("Cannot download PTR: session not established")
            return None

        url = self.get_ptr_url(uuid)

        try:
            response = self._get(url)

            # Check if redirected to home (session expired or invalid)
            if "home" in response.url:
                logger.warning("Session may have expired, re-establishing...")
                self._session_established = False
                if not self.establish_session():
                    return None
                response = self._get(url)

            response.raise_for_status()

            # Verify we got actual content (not error page)
            if len(response.text) < 1000:
                logger.warning(f"PTR content too small, may be error: {uuid}")
                return None

            # Write to cache
            cache_path.write_text(response.text, encoding="utf-8")
            logger.info(f"Downloaded PTR: {uuid[:8]} ({len(response.text)} bytes)")
            return cache_path

        except requests.RequestException as e:
            logger.error(f"Failed to download PTR {uuid}: {e}")
            return None

    def download_paper_filing(
        self,
        uuid: str,
        force: bool = False,
    ) -> Optional[Path]:
        """Download a paper filing page and its GIF images.

        Paper filings are served as GIF images. This method downloads
        the filing page and extracts/caches the image URLs.

        Args:
            uuid: The filing UUID
            force: If True, re-download even if cached

        Returns:
            Path to cached HTML file with image references, or None if failed
        """
        cache_path = self.cache_dir / f"paper_{uuid[:8]}.html"

        if cache_path.exists() and not force:
            logger.debug(f"Using cached paper filing: {cache_path}")
            return cache_path

        if not self.ensure_session():
            logger.error("Cannot download paper filing: session not established")
            return None

        url = self.get_paper_url(uuid)

        try:
            response = self._get(url)

            if "home" in response.url:
                self._session_established = False
                if not self.establish_session():
                    return None
                response = self._get(url)

            response.raise_for_status()

            cache_path.write_text(response.text, encoding="utf-8")
            logger.info(f"Downloaded paper filing page: {uuid[:8]}")

            # Extract and download GIF images
            self._download_paper_images(response.text, uuid)

            return cache_path

        except requests.RequestException as e:
            logger.error(f"Failed to download paper filing {uuid}: {e}")
            return None

    def _download_paper_images(self, html: str, uuid: str) -> list[Path]:
        """Extract and download GIF images from paper filing page.

        Args:
            html: The paper filing page HTML
            uuid: The filing UUID

        Returns:
            List of paths to downloaded GIF files
        """
        downloaded = []
        soup = BeautifulSoup(html, "lxml")

        # Find all image tags pointing to the media server
        images = soup.find_all("img", src=re.compile(r"efd-media-public\.senate\.gov"))

        for i, img in enumerate(images):
            img_url = img.get("src", "")
            if not img_url:
                continue

            # Ensure full URL
            if not img_url.startswith("http"):
                img_url = f"https:{img_url}"

            cache_path = self.cache_dir / f"paper_{uuid[:8]}_page{i + 1}.gif"

            if cache_path.exists():
                downloaded.append(cache_path)
                continue

            try:
                # Media server doesn't require session
                self._rate_limit()
                response = requests.get(img_url, timeout=self.timeout)
                response.raise_for_status()

                cache_path.write_bytes(response.content)
                downloaded.append(cache_path)
                logger.debug(f"Downloaded paper image: {cache_path.name}")

            except requests.RequestException as e:
                logger.warning(f"Failed to download image: {e}")

        return downloaded

    def parse_ptr_transactions(self, html_path: Path) -> list[SenateTransaction]:
        """Parse transactions from a cached PTR HTML file.

        Args:
            html_path: Path to cached HTML file

        Returns:
            List of parsed transactions
        """
        transactions = []

        try:
            html = html_path.read_text(encoding="utf-8")
            soup = BeautifulSoup(html, "lxml")

            # Find the transactions table
            table = soup.find("table", {"class": re.compile(r"table.*striped", re.I)})
            if not table:
                logger.warning(f"No transaction table found in {html_path}")
                return transactions

            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip header
                cells = row.find_all("td")
                if len(cells) < 7:
                    continue

                try:
                    # Determine if first column is row number (#) or date
                    # Row numbers are typically just digits
                    first_cell_text = cells[0].get_text(strip=True)
                    offset = 0
                    if first_cell_text.isdigit():
                        # First column is row number, shift all indices by 1
                        offset = 1
                        if len(cells) < 8:  # Need at least 8 cells with # column
                            continue

                    # Parse transaction date
                    date_text = cells[offset + 0].get_text(strip=True)
                    transaction_date = None
                    if date_text:
                        try:
                            transaction_date = datetime.strptime(date_text, "%m/%d/%Y")
                        except ValueError:
                            pass

                    # Parse owner
                    owner = cells[offset + 1].get_text(strip=True) or "Self"

                    # Parse ticker (may be in a link)
                    ticker_cell = cells[offset + 2]
                    ticker_link = ticker_cell.find("a")
                    ticker = None
                    if ticker_link:
                        ticker = ticker_link.get_text(strip=True)
                    else:
                        ticker_text = ticker_cell.get_text(strip=True)
                        if ticker_text and ticker_text != "--":
                            ticker = ticker_text

                    # Parse asset name
                    asset_name = cells[offset + 3].get_text(strip=True)

                    # Parse asset type
                    asset_type = cells[offset + 4].get_text(strip=True) if len(cells) > offset + 4 else None

                    # Parse transaction type
                    transaction_type = cells[offset + 5].get_text(strip=True) if len(cells) > offset + 5 else ""

                    # Parse amount range
                    amount_range = cells[offset + 6].get_text(strip=True) if len(cells) > offset + 6 else ""

                    # Parse comment if present
                    comment = cells[offset + 7].get_text(strip=True) if len(cells) > offset + 7 else None

                    transaction = SenateTransaction(
                        transaction_date=transaction_date,
                        owner=owner,
                        ticker=ticker,
                        asset_name=asset_name,
                        asset_type=asset_type,
                        transaction_type=transaction_type,
                        amount_range=amount_range,
                        comment=comment,
                    )
                    transactions.append(transaction)

                except Exception as e:
                    logger.debug(f"Error parsing transaction row: {e}")
                    continue

            logger.info(f"Parsed {len(transactions)} transactions from {html_path.name}")

        except Exception as e:
            logger.error(f"Failed to parse PTR: {e}")

        return transactions

    def search_ptrs(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        filer_name: Optional[str] = None,
    ) -> list[SenateFiling]:
        """Search for PTR filings.

        Note: The Senate search requires JavaScript for full functionality.
        This method handles basic search. For comprehensive listing, may need
        to paginate through results.

        Args:
            start_date: Start of date range
            end_date: End of date range
            filer_name: Optional filer name filter

        Returns:
            List of SenateFiling objects found
        """
        if not self.ensure_session():
            logger.error("Cannot search: session not established")
            return []

        # Build search parameters
        params = {
            "report_types": "[PTR]",  # Periodic Transaction Reports
        }

        if start_date:
            params["submitted_start_date"] = start_date.strftime("%m/%d/%Y")
        if end_date:
            params["submitted_end_date"] = end_date.strftime("%m/%d/%Y")
        if filer_name:
            params["filer_name"] = filer_name

        try:
            response = self._get(self.SEARCH_URL, params=params)
            response.raise_for_status()

            return self._parse_search_results(response.text)

        except requests.RequestException as e:
            logger.error(f"Search failed: {e}")
            return []

    def _parse_search_results(self, html: str) -> list[SenateFiling]:
        """Parse search results HTML to extract filing information.

        Args:
            html: Raw HTML response

        Returns:
            List of parsed SenateFiling objects
        """
        filings = []
        soup = BeautifulSoup(html, "lxml")

        # Look for result rows
        result_rows = soup.find_all("tr", {"class": re.compile(r"result", re.I)})
        if not result_rows:
            # Try finding all table rows in results section
            results_div = soup.find("div", {"id": re.compile(r"result", re.I)})
            if results_div:
                result_rows = results_div.find_all("tr")

        for row in result_rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            try:
                # Find link to the filing
                link = row.find("a", href=re.compile(r"/view/(ptr|paper)/"))
                if not link:
                    continue

                href = link.get("href", "")
                is_paper = "paper" in href

                # Extract UUID from URL
                uuid_match = re.search(r"/(ptr|paper)/([a-f0-9-]+)", href, re.I)
                if not uuid_match:
                    continue

                uuid = uuid_match.group(2)

                # Extract filer name
                filer_name = cells[0].get_text(strip=True)

                # Extract state if available
                state = None
                if len(cells) > 1:
                    state = cells[1].get_text(strip=True)

                # Extract filing date if available
                filing_date = None
                if len(cells) > 2:
                    date_text = cells[2].get_text(strip=True)
                    try:
                        filing_date = datetime.strptime(date_text, "%m/%d/%Y")
                    except ValueError:
                        pass

                report_url = (
                    self.get_paper_url(uuid) if is_paper else self.get_ptr_url(uuid)
                )

                filing = SenateFiling(
                    filing_id=uuid,
                    filer_name=filer_name,
                    state=state,
                    filing_date=filing_date,
                    report_url=report_url,
                    is_paper=is_paper,
                )
                filings.append(filing)

            except Exception as e:
                logger.debug(f"Error parsing result row: {e}")
                continue

        logger.info(f"Found {len(filings)} filings in search results")
        return filings

    def list_cached_files(self) -> dict[str, list[str]]:
        """List all cached files by type.

        Returns:
            Dict with 'ptr' and 'paper' keys containing lists of cached IDs
        """
        ptrs = [f.stem.replace("ptr_", "") for f in self.cache_dir.glob("ptr_*.html")]
        papers = [
            f.stem.replace("paper_", "").split("_")[0]
            for f in self.cache_dir.glob("paper_*.html")
        ]
        return {"ptr": ptrs, "paper": list(set(papers))}

    def clear_cache(self) -> int:
        """Clear all cached files.

        Returns:
            Number of files deleted
        """
        count = 0
        for f in self.cache_dir.glob("*"):
            if f.is_file():
                f.unlink()
                count += 1
        logger.info(f"Cleared {count} cached Senate files")
        return count
