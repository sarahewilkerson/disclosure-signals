#!/usr/bin/env python3
"""
Senate EFD PTR Scraper

Scrapes Periodic Transaction Reports from efdsearch.senate.gov using Playwright
for JavaScript rendering. Outputs UUIDs to JSON for subsequent download.

Usage:
    # Scrape UUIDs:
    python scrape_senate.py scrape --from-date 2024-01-01 --to-date 2026-03-31 --output senate_ptrs.json

    # Download PTRs from scraped JSON:
    python scrape_senate.py download --input senate_ptrs.json --cache-dir /path/to/cache

Requires:
    pip install playwright requests beautifulsoup4
    playwright install chromium
"""

import argparse
import asyncio
import json
import logging
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import Page, async_playwright
from playwright.async_api import TimeoutError as PlaywrightTimeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Selectors discovered via reconnaissance
SELECTORS = {
    "agreement_checkbox": "input#agree_statement",
    "submit_button": "button[type=submit]",
    "ptr_checkbox": "input[name='report_type'][value='11']",
    "from_date": "input#fromDate",
    "to_date": "input#toDate",
    "results_table": "#filedReports",
    "table_rows": "#filedReports tbody tr",
    "datatables_info": ".dataTables_info",
    "next_button": ".paginate_button.next:not(.disabled)",
    "current_page": ".paginate_button.current",
}

# URL patterns
BASE_URL = "https://efdsearch.senate.gov"
HOME_URL = f"{BASE_URL}/search/home/"
SEARCH_URL = f"{BASE_URL}/search/"

# UUID regex for extraction from URLs
UUID_PATTERN = re.compile(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}")


@dataclass
class PTRRecord:
    """A discovered PTR record from search results."""
    uuid: str
    first_name: str
    last_name: str
    office: str
    report_type: str
    date_filed: str
    url_type: str  # 'ptr' or 'paper'
    full_url: str
    discovered_at: str

    def to_dict(self) -> dict:
        return asdict(self)


async def establish_session(page: Page) -> bool:
    """
    Navigate to Senate EFD and accept the prohibition agreement.

    Returns:
        True if session established successfully
    """
    logger.info("Establishing session at %s", HOME_URL)

    try:
        await page.goto(HOME_URL)
        await page.wait_for_load_state("networkidle")

        # Check for and click agreement checkbox
        checkbox = await page.query_selector(SELECTORS["agreement_checkbox"])
        if not checkbox:
            logger.warning("Agreement checkbox not found - may already be accepted")
            return True

        await checkbox.click()
        await page.wait_for_timeout(300)

        # Click submit/agree button
        submit_btn = await page.query_selector(SELECTORS["submit_button"])
        if submit_btn:
            await submit_btn.click()
            await page.wait_for_load_state("networkidle")
            logger.info("Agreement accepted, session established")
            return True

        logger.error("Submit button not found")
        return False

    except PlaywrightTimeout:
        logger.error("Timeout establishing session")
        return False
    except Exception as e:
        logger.error("Error establishing session: %s", e)
        return False


async def submit_ptr_search(page: Page, from_date: str, to_date: str) -> bool:
    """
    Submit a search for PTRs within the date range.

    Args:
        page: Playwright page
        from_date: Start date (MM/DD/YYYY format)
        to_date: End date (MM/DD/YYYY format)

    Returns:
        True if search submitted successfully
    """
    logger.info("Submitting PTR search: %s to %s", from_date, to_date)

    try:
        # Wait for search form
        await page.wait_for_selector(SELECTORS["ptr_checkbox"], timeout=10000)

        # Check PTR checkbox
        ptr_cb = await page.query_selector(SELECTORS["ptr_checkbox"])
        if ptr_cb:
            await ptr_cb.click()
            logger.debug("PTR checkbox checked")
        else:
            logger.error("PTR checkbox not found")
            return False

        # Fill date range
        from_input = await page.query_selector(SELECTORS["from_date"])
        to_input = await page.query_selector(SELECTORS["to_date"])

        if from_input and to_input:
            await from_input.fill(from_date)
            await to_input.fill(to_date)
            logger.debug("Date range set")
        else:
            logger.error("Date inputs not found")
            return False

        # Submit search
        submit_btn = await page.query_selector(SELECTORS["submit_button"])
        if submit_btn:
            await submit_btn.click()
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)  # Extra wait for JS rendering
            logger.info("Search submitted")
            return True

        logger.error("Search submit button not found")
        return False

    except PlaywrightTimeout:
        logger.error("Timeout submitting search")
        return False
    except Exception as e:
        logger.error("Error submitting search: %s", e)
        return False


async def get_total_results(page: Page) -> Optional[int]:
    """
    Extract total result count from DataTables info.

    Returns:
        Total number of results, or None if unable to parse
    """
    try:
        info_elem = await page.query_selector(SELECTORS["datatables_info"])
        if info_elem:
            text = await info_elem.inner_text()
            # Parse "Showing X to Y of Z entries"
            match = re.search(r"of\s+([\d,]+)\s+entries", text)
            if match:
                total = int(match.group(1).replace(",", ""))
                logger.info("Total results: %d", total)
                return total
        return None
    except Exception as e:
        logger.warning("Could not parse total results: %s", e)
        return None


async def extract_page_results(page: Page) -> list[PTRRecord]:
    """
    Extract PTR records from the current results page.

    Returns:
        List of PTRRecord objects
    """
    records = []
    now = datetime.now().isoformat()

    try:
        rows = await page.query_selector_all(SELECTORS["table_rows"])
        logger.debug("Found %d rows on current page", len(rows))

        for row in rows:
            try:
                cells = await row.query_selector_all("td")
                if len(cells) < 5:
                    continue

                first_name = (await cells[0].inner_text()).strip()
                last_name = (await cells[1].inner_text()).strip()
                office = (await cells[2].inner_text()).strip()
                report_type = (await cells[3].inner_text()).strip()
                date_filed = (await cells[4].inner_text()).strip()

                # Extract link and UUID
                link = await row.query_selector("a")
                if link:
                    href = await link.get_attribute("href") or ""
                    uuid_match = UUID_PATTERN.search(href)
                    if uuid_match:
                        uuid = uuid_match.group(0)

                        # Determine URL type (ptr or paper)
                        url_type = "ptr"
                        if "/paper/" in href:
                            url_type = "paper"

                        records.append(PTRRecord(
                            uuid=uuid,
                            first_name=first_name,
                            last_name=last_name,
                            office=office,
                            report_type=report_type,
                            date_filed=date_filed,
                            url_type=url_type,
                            full_url=f"{BASE_URL}{href}",
                            discovered_at=now,
                        ))

            except Exception as e:
                logger.warning("Error extracting row: %s", e)
                continue

    except Exception as e:
        logger.error("Error extracting page results: %s", e)

    return records


async def paginate_all(page: Page) -> list[PTRRecord]:
    """
    Iterate through all pages and collect PTR records.

    Returns:
        Complete list of PTRRecord objects
    """
    all_records = []
    page_num = 1

    while True:
        logger.info("Extracting page %d...", page_num)

        # Extract current page
        records = await extract_page_results(page)
        all_records.extend(records)
        logger.info("  Found %d records (total: %d)", len(records), len(all_records))

        # Check for next button
        next_btn = await page.query_selector(SELECTORS["next_button"])
        if not next_btn:
            logger.info("No more pages (Next button disabled/missing)")
            break

        # Click next
        try:
            await next_btn.click()
            await page.wait_for_timeout(1500)  # Wait for table refresh
            page_num += 1
        except Exception as e:
            logger.warning("Error clicking next: %s", e)
            break

        # Safety limit
        if page_num > 100:
            logger.warning("Reached page limit (100), stopping")
            break

    return all_records


def save_results(records: list[PTRRecord], output_path: Path) -> None:
    """Save records to JSON file."""
    data = {
        "scraped_at": datetime.now().isoformat(),
        "total_count": len(records),
        "records": [r.to_dict() for r in records],
    }

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info("Saved %d records to %s", len(records), output_path)


def load_existing_uuids(output_path: Path) -> set[str]:
    """Load UUIDs from existing output file for resume support."""
    if not output_path.exists():
        return set()

    try:
        with open(output_path) as f:
            data = json.load(f)
            return {r["uuid"] for r in data.get("records", [])}
    except Exception as e:
        logger.warning("Could not load existing file: %s", e)
        return set()


async def run_scrape(
    from_date: str,
    to_date: str,
    output_path: Path,
    headless: bool = True,
) -> int:
    """
    Main scraping workflow.

    Args:
        from_date: Start date (YYYY-MM-DD, will be converted to MM/DD/YYYY)
        to_date: End date (YYYY-MM-DD)
        output_path: Path to output JSON file
        headless: Run browser in headless mode

    Returns:
        Number of records scraped
    """
    # Convert date format
    from_dt = datetime.strptime(from_date, "%Y-%m-%d")
    to_dt = datetime.strptime(to_date, "%Y-%m-%d")
    from_str = from_dt.strftime("%m/%d/%Y")
    to_str = to_dt.strftime("%m/%d/%Y")

    # Load existing UUIDs for dedup
    existing_uuids = load_existing_uuids(output_path)
    if existing_uuids:
        logger.info("Loaded %d existing UUIDs for deduplication", len(existing_uuids))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            # Establish session
            if not await establish_session(page):
                logger.error("Failed to establish session")
                return 0

            # Submit search
            if not await submit_ptr_search(page, from_str, to_str):
                logger.error("Failed to submit search")
                return 0

            # Get total count
            total = await get_total_results(page)
            if total:
                logger.info("Expecting approximately %d results", total)

            # Paginate and collect all records
            records = await paginate_all(page)

            # Deduplicate
            new_records = [r for r in records if r.uuid not in existing_uuids]
            logger.info("New records after dedup: %d (skipped %d existing)",
                       len(new_records), len(records) - len(new_records))

            # Save results
            if new_records:
                save_results(new_records, output_path)

            return len(new_records)

        finally:
            await browser.close()


# ============================================================================
# DOWNLOAD FUNCTIONALITY
# ============================================================================

# Session configuration for downloads
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 1.0  # Seconds between requests (be respectful)
AGREEMENT_URL = "https://efdsearch.senate.gov/search/home/"


class SenateDownloader:
    """Downloads PTR HTML files using requests session."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir / "pdfs" / "senate"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self._session_established = False

    def establish_session(self) -> bool:
        """Establish session by accepting the prohibition agreement."""
        if self._session_established:
            return True

        try:
            # Get initial page for CSRF token
            resp = self.session.get(AGREEMENT_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            # Extract CSRF token
            soup = BeautifulSoup(resp.text, "html.parser")
            csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
            if not csrf_input:
                logger.error("CSRF token not found")
                return False

            csrf_token = csrf_input.get("value")

            # Submit agreement
            data = {
                "csrfmiddlewaretoken": csrf_token,
                "prohibition_agreement": "1",
            }
            headers = {
                "Referer": AGREEMENT_URL,
            }

            resp = self.session.post(
                AGREEMENT_URL,
                data=data,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )

            if "/search/" in resp.url:
                self._session_established = True
                logger.info("Download session established")
                return True

            logger.error("Session establishment failed: redirected to %s", resp.url)
            return False

        except Exception as e:
            logger.error("Error establishing session: %s", e)
            return False

    def download_ptr(self, uuid: str, url_type: str, force: bool = False) -> Optional[Path]:
        """
        Download a PTR or paper filing HTML.

        Args:
            uuid: Filing UUID
            url_type: 'ptr' or 'paper'
            force: Re-download even if cached

        Returns:
            Path to cached file, or None on failure
        """
        prefix = "ptr" if url_type == "ptr" else "paper"
        cache_path = self.cache_dir / f"{prefix}_{uuid[:8]}.html"

        if cache_path.exists() and not force:
            logger.debug("Using cached: %s", cache_path.name)
            return cache_path

        if not self.establish_session():
            return None

        url = f"{BASE_URL}/search/view/{url_type}/{uuid}/"

        try:
            time.sleep(REQUEST_DELAY)
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            # Check for redirect to home (session expired)
            if "/search/home/" in resp.url:
                logger.warning("Session expired, re-establishing...")
                self._session_established = False
                if not self.establish_session():
                    return None
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()

            # Save to cache
            cache_path.write_text(resp.text, encoding="utf-8")
            logger.debug("Downloaded: %s -> %s", uuid[:8], cache_path.name)
            return cache_path

        except Exception as e:
            logger.error("Failed to download %s: %s", uuid[:8], e)
            return None

    def extract_gif_urls_from_html(self, html_path: Path) -> list[str]:
        """
        Extract embedded GIF URLs from a paper filing HTML file.

        Args:
            html_path: Path to the paper filing HTML

        Returns:
            List of GIF URLs found in the HTML
        """
        try:
            html_content = html_path.read_text(encoding="utf-8")
            # Pattern for Senate paper filing GIF URLs
            gif_pattern = r'https://efd-media-public\.senate\.gov/[^"\']+\.gif'
            return list(set(re.findall(gif_pattern, html_content)))
        except Exception as e:
            logger.error("Failed to extract GIF URLs from %s: %s", html_path.name, e)
            return []

    def download_paper_gifs(self, html_path: Path, force: bool = False) -> list[Path]:
        """
        Download all GIF images from a paper filing HTML.

        Args:
            html_path: Path to the paper filing HTML
            force: Re-download existing files

        Returns:
            List of paths to downloaded GIF files
        """
        gif_urls = self.extract_gif_urls_from_html(html_path)
        if not gif_urls:
            logger.debug("No GIF URLs found in %s", html_path.name)
            return []

        # Extract filing ID from HTML filename (paper_XXXXXXXX.html)
        filing_id = html_path.stem.replace("paper_", "")

        downloaded = []
        for i, gif_url in enumerate(gif_urls):
            # Create filename: paper_XXXXXXXX_pageN.gif
            if len(gif_urls) == 1:
                gif_path = self.cache_dir / f"paper_{filing_id}.gif"
            else:
                gif_path = self.cache_dir / f"paper_{filing_id}_page{i + 1}.gif"

            if gif_path.exists() and not force:
                logger.debug("Using cached: %s", gif_path.name)
                downloaded.append(gif_path)
                continue

            try:
                time.sleep(REQUEST_DELAY)
                resp = self.session.get(gif_url, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()

                gif_path.write_bytes(resp.content)
                logger.debug("Downloaded GIF: %s -> %s", gif_url.split("/")[-1], gif_path.name)
                downloaded.append(gif_path)

            except Exception as e:
                logger.error("Failed to download GIF %s: %s", gif_url, e)

        return downloaded


def run_download_gifs(
    cache_dir: Path,
    force: bool = False,
) -> tuple[int, int]:
    """
    Download GIF images from already-cached paper filing HTML files.

    Args:
        cache_dir: Cache directory containing paper_*.html files
        force: Re-download existing GIFs

    Returns:
        Tuple of (successful, failed) download counts
    """
    senate_cache = cache_dir / "pdfs" / "senate"
    if not senate_cache.exists():
        logger.error("Senate cache directory not found: %s", senate_cache)
        return 0, 0

    # Find all paper filing HTML files
    paper_html_files = list(senate_cache.glob("paper_*.html"))
    logger.info("Found %d paper filing HTML files", len(paper_html_files))

    if not paper_html_files:
        return 0, 0

    # Pass base cache_dir - SenateDownloader adds pdfs/senate internally
    downloader = SenateDownloader(cache_dir)
    total_downloaded = 0
    total_failed = 0

    for i, html_path in enumerate(paper_html_files, 1):
        gif_urls = downloader.extract_gif_urls_from_html(html_path)
        if not gif_urls:
            continue

        logger.info("[%d/%d] Processing %s (%d GIFs)...",
                   i, len(paper_html_files), html_path.name, len(gif_urls))

        gifs = downloader.download_paper_gifs(html_path, force=force)
        downloaded = len(gifs)
        failed = len(gif_urls) - downloaded

        total_downloaded += downloaded
        total_failed += failed

        if downloaded > 0:
            logger.info("  Downloaded %d GIFs", downloaded)
        if failed > 0:
            logger.warning("  Failed to download %d GIFs", failed)

    return total_downloaded, total_failed


def run_download(
    input_path: Path,
    cache_dir: Path,
    limit: Optional[int] = None,
    force: bool = False,
) -> tuple[int, int]:
    """
    Download PTRs from a scraped JSON file.

    Args:
        input_path: Path to scraped JSON file
        cache_dir: Cache directory
        limit: Max files to download (None for all)
        force: Re-download existing files

    Returns:
        Tuple of (successful, failed) download counts
    """
    # Load scraped data
    with open(input_path) as f:
        data = json.load(f)

    records = data.get("records", [])
    if limit:
        records = records[:limit]

    logger.info("Downloading %d PTRs to %s", len(records), cache_dir)

    downloader = SenateDownloader(cache_dir)
    success = 0
    failed = 0

    for i, record in enumerate(records, 1):
        uuid = record["uuid"]
        url_type = record.get("url_type", "ptr")
        name = f"{record.get('first_name', '')} {record.get('last_name', '')}".strip()

        logger.info("[%d/%d] Downloading %s (%s)...", i, len(records), name, url_type)

        result = downloader.download_ptr(uuid, url_type, force=force)
        if result:
            success += 1
            # For paper filings, also download the embedded GIF images
            if url_type == "paper":
                gifs = downloader.download_paper_gifs(result, force=force)
                if gifs:
                    logger.info("  Downloaded %d GIF images for paper filing", len(gifs))
        else:
            failed += 1

        # Progress checkpoint every 50
        if i % 50 == 0:
            logger.info("Progress: %d/%d (success: %d, failed: %d)",
                       i, len(records), success, failed)

    return success, failed


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape and download Senate PTRs from efdsearch.senate.gov"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Scrape subcommand
    scrape_parser = subparsers.add_parser("scrape", help="Scrape PTR UUIDs")
    scrape_parser.add_argument(
        "--from-date",
        required=True,
        help="Start date (YYYY-MM-DD)"
    )
    scrape_parser.add_argument(
        "--to-date",
        required=True,
        help="End date (YYYY-MM-DD)"
    )
    scrape_parser.add_argument(
        "--output",
        default="senate_ptrs.json",
        help="Output JSON file path (default: senate_ptrs.json)"
    )
    scrape_parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser in visible mode (for debugging)"
    )

    # Download subcommand
    download_parser = subparsers.add_parser("download", help="Download PTR HTML files")
    download_parser.add_argument(
        "--input",
        required=True,
        help="Input JSON file from scrape"
    )
    download_parser.add_argument(
        "--cache-dir",
        default="/tmp/congressional_positioning/cache",
        help="Cache directory (default: /tmp/congressional_positioning/cache)"
    )
    download_parser.add_argument(
        "--limit",
        type=int,
        help="Max files to download"
    )
    download_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download existing files"
    )

    # Download GIFs subcommand
    gifs_parser = subparsers.add_parser(
        "download-gifs",
        help="Download GIF images from cached paper filing HTML files"
    )
    gifs_parser.add_argument(
        "--cache-dir",
        default="/tmp/congressional_positioning/cache",
        help="Cache directory (default: /tmp/congressional_positioning/cache)"
    )
    gifs_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download existing GIFs"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.command == "scrape":
        output_path = Path(args.output)
        count = asyncio.run(run_scrape(
            from_date=args.from_date,
            to_date=args.to_date,
            output_path=output_path,
            headless=not args.no_headless,
        ))
        if count > 0:
            logger.info("Scrape complete: %d new PTRs discovered", count)
        else:
            logger.info("Scrape complete: no new PTRs found")

    elif args.command == "download":
        input_path = Path(args.input)
        cache_dir = Path(args.cache_dir)

        success, failed = run_download(
            input_path=input_path,
            cache_dir=cache_dir,
            limit=args.limit,
            force=args.force,
        )
        logger.info("Download complete: %d success, %d failed", success, failed)

    elif args.command == "download-gifs":
        cache_dir = Path(args.cache_dir)

        success, failed = run_download_gifs(
            cache_dir=cache_dir,
            force=args.force,
        )
        logger.info("GIF download complete: %d success, %d failed", success, failed)


if __name__ == "__main__":
    main()
