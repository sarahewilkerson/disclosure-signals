"""
SEC EDGAR Form 4 ingestion.

Uses the EDGAR Full-Text Search (EFTS) API to find Form 4 filings by issuer,
then downloads the raw XML and inserts filing metadata into the database.

Key design notes:
- The EDGAR submissions API (data.sec.gov/submissions/CIK{cik}.json) indexes
  filings by the FILER's CIK. For Form 4, the filer is the reporting owner
  (insider), NOT the issuer (company). So we cannot use that API to find
  Form 4 filings about a specific company.
- Instead, we use the EFTS API (efts.sec.gov/LATEST/search-index) which
  supports searching by form type and can match on issuer CIK within the
  filing content.
- As a fallback / complement, we also use the company's filing index at
  www.sec.gov/cgi-bin/browse-edgar to find Form 4 filings where the company
  is the subject (issuer).
"""

import asyncio
import json
import logging
import os
import re
import time
from typing import Optional

import httpx
import requests

import config
from db import get_connection, upsert_filing, get_filing_accession_numbers

logger = logging.getLogger(__name__)


class EdgarClient:
    """Rate-limited HTTP client for SEC EDGAR."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": config.SEC_USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        })
        self._last_request_time = 0.0

    def _throttle(self):
        """Enforce SEC rate limits."""
        elapsed = time.time() - self._last_request_time
        if elapsed < config.SEC_RATE_LIMIT_DELAY:
            time.sleep(config.SEC_RATE_LIMIT_DELAY - elapsed)

    def get(self, url: str, retries: int = None) -> requests.Response:
        """GET with throttling and retries.

        Retries on 5xx server errors and network errors.
        Does NOT retry on 4xx client errors (permanent failures like 404).
        """
        config.validate_runtime_config()
        if retries is None:
            retries = config.SEC_MAX_RETRIES
        last_error = None
        for attempt in range(retries):
            self._throttle()
            try:
                resp = self.session.get(url, timeout=30)
                self._last_request_time = time.time()
                resp.raise_for_status()
                return resp
            except requests.HTTPError as e:
                self._last_request_time = time.time()
                # Don't retry client errors (4xx) - they're permanent
                if e.response is not None and 400 <= e.response.status_code < 500:
                    raise
                # Retry server errors (5xx) - they may be transient
                last_error = e
                wait = 2 ** (attempt + 1)
                logger.warning(f"Request failed ({url}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            except requests.RequestException as e:
                # Retry network errors (timeout, connection reset)
                self._last_request_time = time.time()
                last_error = e
                wait = 2 ** (attempt + 1)
                logger.warning(f"Request failed ({url}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
        raise last_error


class AsyncEdgarClient:
    """Async rate-limited HTTP client for SEC EDGAR using httpx."""

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        self._last_request_time = 0.0

    async def _get_client(self) -> httpx.AsyncClient:
        """Lazily create the async client."""
        if self._client is None:
            config.validate_runtime_config()
            self._client = httpx.AsyncClient(
                headers={
                    "User-Agent": config.SEC_USER_AGENT,
                    "Accept-Encoding": "gzip, deflate",
                },
                timeout=30.0,
            )
        return self._client

    async def _throttle(self):
        """Enforce SEC rate limits (async)."""
        elapsed = time.time() - self._last_request_time
        if elapsed < config.SEC_RATE_LIMIT_DELAY:
            await asyncio.sleep(config.SEC_RATE_LIMIT_DELAY - elapsed)

    async def get(self, url: str, retries: int = None) -> httpx.Response:
        """Async GET with throttling and retries.

        Retries on 5xx server errors and network errors.
        Does NOT retry on 4xx client errors (permanent failures like 404).
        """
        if retries is None:
            retries = config.SEC_MAX_RETRIES
        client = await self._get_client()
        last_error = None
        for attempt in range(retries):
            await self._throttle()
            try:
                resp = await client.get(url)
                self._last_request_time = time.time()
                resp.raise_for_status()
                return resp
            except httpx.HTTPStatusError as e:
                self._last_request_time = time.time()
                # Don't retry client errors (4xx) - they're permanent
                if 400 <= e.response.status_code < 500:
                    raise
                # Retry server errors (5xx) - they may be transient
                last_error = e
                wait = 2 ** (attempt + 1)
                logger.warning(f"Async request failed ({url}): {e}. Retrying in {wait}s...")
                await asyncio.sleep(wait)
            except httpx.HTTPError as e:
                # Retry network errors (timeout, connection reset)
                self._last_request_time = time.time()
                last_error = e
                wait = 2 ** (attempt + 1)
                logger.warning(f"Async request failed ({url}): {e}. Retrying in {wait}s...")
                await asyncio.sleep(wait)
        raise last_error

    async def close(self):
        """Close the async client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# ---------------------------------------------------------------------------
# EDGAR EFTS API for finding Form 4 filings by issuer
# ---------------------------------------------------------------------------
def search_form4_filings(client: EdgarClient, issuer_cik: str,
                         start_date: str = None, end_date: str = None,
                         max_results: int = 100) -> list[dict]:
    """
    Search for Form 4 filings via the EDGAR EFTS API.

    The EFTS search indexes filing content, so searching for the issuer CIK
    finds Form 4 filings where that CIK appears as the issuer — regardless
    of who filed it.

    Args:
        issuer_cik: The company's CIK (zero-padded or not).
        start_date: Start date YYYY-MM-DD (optional).
        end_date: End date YYYY-MM-DD (optional).
        max_results: Maximum number of results to fetch.

    Returns list of filing metadata dicts.
    """
    # EFTS requires zero-padded 10-digit CIK format for search
    cik_padded = str(issuer_cik).lstrip("0").zfill(10)
    cik_clean = str(issuer_cik).lstrip("0") or "0"  # for comparison
    filings = []
    page_size = min(max_results, 100)

    # Build EFTS query — search for the issuer CIK within Form 4 filings
    base_url = f"{config.SEC_EFTS_URL}/search-index"
    params = {
        "q": f'"{cik_padded}"',  # exact match on zero-padded CIK
        "forms": "4,4/A",
        "from": 0,
    }
    if start_date and end_date:
        params["dateRange"] = "custom"
        params["startdt"] = start_date
        params["enddt"] = end_date

    fetched = 0
    while fetched < max_results:
        params["from"] = fetched
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())

        try:
            resp = client.get(url)
            data = resp.json()
        except Exception as e:
            logger.error(f"EFTS search failed for CIK {issuer_cik}: {e}")
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            source = hit.get("_source", {})
            accession = source.get("adsh", "")
            if not accession:
                continue

            # The EFTS result includes CIKs of all entities in the filing
            # Verify our issuer CIK is actually in this filing
            filing_ciks = source.get("ciks", [])
            if cik_clean not in [str(c).lstrip("0") for c in filing_ciks]:
                continue

            form_type = source.get("root_form", "4")
            filing = {
                "accession_number": accession,
                "filing_date": source.get("file_date", ""),
                "form_type": form_type,
                "is_amendment": form_type == "4/A" or "/A" in str(form_type),
                "issuer_cik": str(issuer_cik).zfill(10),
                "display_names": source.get("display_names", []),
            }
            filings.append(filing)

        fetched += len(hits)
        total_available = data.get("hits", {}).get("total", {}).get("value", 0)
        if fetched >= total_available:
            break

    logger.info(f"EFTS found {len(filings)} Form 4 filings for CIK {issuer_cik}")
    return filings


def resolve_filing_xml_url(client: EdgarClient, accession_number: str,
                           issuer_cik: str = None) -> tuple[str | None, str | None]:
    """
    Resolve the primary XML document URL for a filing.

    Uses the EDGAR filing index page to find the ownership XML document.
    SEC archives are organized by issuer CIK (the company), not filer CIK.

    Args:
        client: HTTP client
        accession_number: Filing accession number
        issuer_cik: The issuer (company) CIK - required for correct URL construction

    Returns (xml_url, filer_cik) or (None, None) on failure.
    """
    accession_no_dashes = accession_number.replace("-", "")

    # The filing index JSON is at:
    # https://www.sec.gov/Archives/edgar/data/{filer_cik}/{accession_no_dashes}/{accession}.json
    # But we don't know filer_cik. Use the index URL that works without it:
    index_url = f"{config.SEC_ARCHIVES_URL}/{accession_no_dashes[0:10]}/{accession_no_dashes}/{accession_number}-index.json"

    # Alternative: use the filing index at a known path pattern
    # The EDGAR filing index is also available at:
    # https://www.sec.gov/Archives/edgar/data/{any_cik}/{accession_no_dashes}/
    # But we need a CIK. Instead, use the accession-number-based lookup.

    # Try the EDGAR viewer API which doesn't require CIK
    viewer_url = f"{config.SEC_BASE_URL}/cgi-bin/viewer?action=view&cik=&type=4&dateb=&owner=include&count=1&search_text=&accession={accession_number}&xbrl_type=v"

    # Simpler approach: construct the filing directory URL using EFTS data
    # The EFTS hit._id often contains the path
    # But the most reliable method is the Archives path with the accession number

    # Use the EDGAR filing index endpoint
    # Format: https://www.sec.gov/Archives/edgar/data/{issuer_cik}/{accession_no_dashes}/{filename}
    # SEC archives are organized by issuer (company) CIK, not filer (person) CIK

    # Extract filer CIK from accession number for return value
    # Accession format: {filer_cik}-{yy}-{sequence}
    parts = accession_number.split("-")
    if len(parts) >= 3:
        filer_cik = parts[0].zfill(10)
    else:
        return None, None

    # Use issuer CIK for URL path (required for SEC archive structure)
    if not issuer_cik:
        logger.warning(f"No issuer_cik provided for {accession_number}, falling back to filer_cik")
        path_cik = filer_cik
    else:
        path_cik = str(issuer_cik).lstrip("0") or "0"

    # Get the filing index to find the XML document
    index_url = (
        f"{config.SEC_ARCHIVES_URL}/"
        f"{path_cik}/"
        f"{accession_no_dashes}/"
        f"{accession_number}-index.json"
    )

    try:
        resp = client.get(index_url)
        index_data = resp.json()
    except Exception:
        # Index.json doesn't exist - parse directory listing HTML instead
        dir_url = (
            f"{config.SEC_ARCHIVES_URL}/"
            f"{path_cik}/"
            f"{accession_no_dashes}/"
        )
        try:
            resp = client.get(dir_url, retries=1)
            html = resp.text
            # Find XML files in directory listing (exclude -index files)
            xml_matches = re.findall(r'href="[^"]*?([^/"]+\.xml)"', html, re.IGNORECASE)
            for filename in xml_matches:
                if "-index" in filename.lower():
                    continue
                xml_url = f"{dir_url}{filename}"
                try:
                    xml_resp = client.get(xml_url, retries=1)
                    if xml_resp.status_code == 200 and b"ownershipDocument" in xml_resp.content:
                        return xml_url, filer_cik
                except Exception:
                    continue
        except Exception:
            pass
        return None, filer_cik

    # Find the XML document in the filing index
    directory = index_data.get("directory", {})
    items = directory.get("item", [])
    xml_filename = None
    for item in items:
        name = item.get("name", "")
        # Form 4 XML files are typically named like: primary_doc.xml, doc4.xml,
        # or have .xml extension and contain ownership data
        if name.endswith(".xml") and name != f"{accession_number}-index.json":
            # Prefer files that look like ownership docs
            if any(kw in name.lower() for kw in ["primary", "doc4", "ownership", "form4"]):
                xml_filename = name
                break
            elif xml_filename is None:
                xml_filename = name  # fallback to first XML

    if not xml_filename:
        # Last resort: try the most common pattern
        xml_filename = "primary_doc.xml"

    xml_url = (
        f"{config.SEC_ARCHIVES_URL}/"
        f"{path_cik}/"
        f"{accession_no_dashes}/{xml_filename}"
    )
    return xml_url, filer_cik


def download_filing_xml(client: EdgarClient, xml_url: str,
                        accession_number: str) -> str | None:
    """
    Download a Form 4 XML and cache it locally.

    Returns the local file path, or None on failure.
    """
    os.makedirs(config.FILINGS_CACHE_DIR, exist_ok=True)

    safe_accession = accession_number.replace("-", "_")
    local_path = os.path.join(config.FILINGS_CACHE_DIR, f"{safe_accession}.xml")

    # Skip if already cached
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        logger.debug(f"Cache hit: {local_path}")
        return local_path

    try:
        resp = client.get(xml_url)
        with open(local_path, "wb") as f:
            f.write(resp.content)
        logger.debug(f"Downloaded: {xml_url} -> {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download {xml_url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Main ingestion pipeline
# ---------------------------------------------------------------------------
def ingest_company(cik: str, client: EdgarClient = None,
                   db_path: str = None, max_filings: int = None,
                   start_date: str = None, end_date: str = None) -> list[dict]:
    """
    Ingest Form 4 filings for a single company (issuer CIK).

    Uses the EFTS API to find Form 4 filings where this CIK is the issuer,
    downloads the XML, and inserts filing metadata into the database.

    Args:
        cik: The company's CIK.
        client: Optional EdgarClient instance to reuse.
        db_path: Optional database path.
        max_filings: Maximum number of filings to fetch.
        start_date: Start date for historical backfill (YYYY-MM-DD).
        end_date: End date for historical backfill (YYYY-MM-DD).

    Returns list of filing metadata dicts with local XML paths.
    """
    if client is None:
        client = EdgarClient()

    cik_padded = str(cik).zfill(10)

    logger.info(f"Searching for Form 4 filings for CIK {cik_padded}...")
    form4s = search_form4_filings(
        client, cik_padded,
        start_date=start_date,
        end_date=end_date,
        max_results=max_filings or 200,
    )

    if max_filings:
        form4s = form4s[:max_filings]

    # Download XMLs and insert filing metadata into DB
    results = []
    with get_connection(db_path) as conn:
        known_accessions = get_filing_accession_numbers(conn, cik_padded)

        for filing_meta in form4s:
            accession = filing_meta["accession_number"]

            # Skip if already in DB
            if accession in known_accessions:
                logger.debug(f"Skipping already-ingested filing: {accession}")
                continue

            # Resolve XML URL (use issuer CIK for correct SEC archive path)
            xml_url, filer_cik = resolve_filing_xml_url(client, accession, issuer_cik=cik_padded)
            if not xml_url:
                logger.warning(f"Could not resolve XML URL for {accession}")
                continue

            # Download XML
            local_path = download_filing_xml(client, xml_url, accession)

            # Insert filing metadata into DB so parse_all_pending can find it
            filing_row = {
                "accession_number": accession,
                "cik_issuer": cik_padded,
                "cik_owner": filer_cik,  # will be refined during parsing
                "owner_name": None,      # populated during parsing
                "officer_title": None,   # populated during parsing
                "is_officer": 0,
                "is_director": 0,
                "is_ten_pct_owner": 0,
                "is_other": 0,
                "is_amendment": 1 if filing_meta.get("is_amendment") else 0,
                "amendment_type": None,
                "period_of_report": None,  # populated during parsing
                "aff10b5one": 0,           # populated during parsing
                "additional_owners": None,  # populated during parsing
                "filing_date": filing_meta.get("filing_date", ""),
                "xml_url": xml_url,
                "raw_xml_path": local_path,
                "parsed_at": None,  # NULL = not yet parsed
                "parse_error": None,
            }
            upsert_filing(conn, filing_row)

            filing_meta["xml_url"] = xml_url
            filing_meta["raw_xml_path"] = local_path
            results.append(filing_meta)

    logger.info(f"Ingested {len(results)} new filings for CIK {cik_padded}")
    return results


def ingest_universe(db_path: str = None, max_filings_per_company: int = None,
                    start_date: str = None, end_date: str = None) -> dict:
    """
    Ingest Form 4 filings for all companies in the universe.

    Args:
        db_path: Optional database path.
        max_filings_per_company: Maximum filings to fetch per company.
        start_date: Start date for historical backfill (YYYY-MM-DD).
        end_date: End date for historical backfill (YYYY-MM-DD).

    Returns summary stats.
    """
    from db import get_companies

    with get_connection(db_path) as conn:
        companies = get_companies(conn)

    # Share a single client across all companies (connection reuse)
    client = EdgarClient()

    total_filings = 0
    errors = []

    for company in companies:
        cik = company["cik"]
        ticker = company["ticker"]
        try:
            results = ingest_company(
                cik, client, db_path, max_filings_per_company,
                start_date=start_date, end_date=end_date,
            )
            total_filings += len(results)
            logger.info(f"  {ticker}: {len(results)} new filings")
        except Exception as e:
            logger.error(f"  {ticker}: ingestion failed: {e}")
            errors.append({"ticker": ticker, "cik": cik, "error": str(e)})

    return {
        "total_new_filings": total_filings,
        "companies_processed": len(companies),
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Async versions of ingestion functions
# ---------------------------------------------------------------------------
async def search_form4_filings_async(client: AsyncEdgarClient, issuer_cik: str,
                                     start_date: str = None, end_date: str = None,
                                     max_results: int = 100) -> list[dict]:
    """
    Async version of search_form4_filings.

    Search for Form 4 filings via the EDGAR EFTS API using async HTTP.
    """
    # EFTS requires zero-padded 10-digit CIK format for search
    cik_padded = str(issuer_cik).lstrip("0").zfill(10)
    cik_clean = str(issuer_cik).lstrip("0") or "0"  # for comparison
    filings = []

    base_url = f"{config.SEC_EFTS_URL}/search-index"
    params = {
        "q": f'"{cik_padded}"',  # exact match on zero-padded CIK
        "forms": "4,4/A",
        "from": 0,
    }
    if start_date and end_date:
        params["dateRange"] = "custom"
        params["startdt"] = start_date
        params["enddt"] = end_date

    fetched = 0
    while fetched < max_results:
        params["from"] = fetched
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())

        try:
            resp = await client.get(url)
            data = resp.json()
        except Exception as e:
            logger.error(f"EFTS async search failed for CIK {issuer_cik}: {e}")
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            source = hit.get("_source", {})
            accession = source.get("adsh", "")
            if not accession:
                continue

            filing_ciks = source.get("ciks", [])
            if cik_clean not in [str(c).lstrip("0") for c in filing_ciks]:
                continue

            form_type = source.get("root_form", "4")
            filing = {
                "accession_number": accession,
                "filing_date": source.get("file_date", ""),
                "form_type": form_type,
                "is_amendment": form_type == "4/A" or "/A" in str(form_type),
                "issuer_cik": str(issuer_cik).zfill(10),
                "display_names": source.get("display_names", []),
            }
            filings.append(filing)

        fetched += len(hits)
        total_available = data.get("hits", {}).get("total", {}).get("value", 0)
        if fetched >= total_available:
            break

    logger.info(f"EFTS async found {len(filings)} Form 4 filings for CIK {issuer_cik}")
    return filings


async def resolve_filing_xml_url_async(client: AsyncEdgarClient,
                                       accession_number: str,
                                       issuer_cik: str = None) -> tuple[str | None, str | None]:
    """
    Async version of resolve_filing_xml_url.

    Resolve the primary XML document URL for a filing using async HTTP.
    SEC archives are organized by issuer CIK (the company), not filer CIK.
    """
    accession_no_dashes = accession_number.replace("-", "")

    # Extract filer CIK from accession number for return value
    parts = accession_number.split("-")
    if len(parts) >= 3:
        filer_cik = parts[0].zfill(10)
    else:
        return None, None

    # Use issuer CIK for URL path (required for SEC archive structure)
    if not issuer_cik:
        logger.warning(f"No issuer_cik provided for {accession_number}, falling back to filer_cik")
        path_cik = filer_cik.lstrip("0") or "0"
    else:
        path_cik = str(issuer_cik).lstrip("0") or "0"

    index_url = (
        f"{config.SEC_ARCHIVES_URL}/"
        f"{path_cik}/"
        f"{accession_no_dashes}/"
        f"{accession_number}-index.json"
    )

    try:
        resp = await client.get(index_url)
        index_data = resp.json()
    except Exception:
        # Index.json doesn't exist - parse directory listing HTML instead
        dir_url = (
            f"{config.SEC_ARCHIVES_URL}/"
            f"{path_cik}/"
            f"{accession_no_dashes}/"
        )
        try:
            resp = await client.get(dir_url)
            html = resp.text
            # Find XML files in directory listing (exclude -index files)
            xml_matches = re.findall(r'href="[^"]*?([^/"]+\.xml)"', html, re.IGNORECASE)
            for filename in xml_matches:
                if "-index" in filename.lower():
                    continue
                xml_url = f"{dir_url}{filename}"
                try:
                    xml_resp = await client.get(xml_url)
                    if xml_resp.status_code == 200 and b"ownershipDocument" in xml_resp.content:
                        return xml_url, filer_cik
                except Exception:
                    continue
        except Exception:
            pass
        return None, filer_cik

    # Find the XML document in the filing index
    directory = index_data.get("directory", {})
    items = directory.get("item", [])
    xml_filename = None
    for item in items:
        name = item.get("name", "")
        if name.endswith(".xml") and name != f"{accession_number}-index.json":
            if any(kw in name.lower() for kw in ["primary", "doc4", "ownership", "form4"]):
                xml_filename = name
                break
            elif xml_filename is None:
                xml_filename = name

    if not xml_filename:
        xml_filename = "primary_doc.xml"

    xml_url = (
        f"{config.SEC_ARCHIVES_URL}/"
        f"{path_cik}/"
        f"{accession_no_dashes}/{xml_filename}"
    )
    return xml_url, filer_cik


async def download_filing_xml_async(client: AsyncEdgarClient, xml_url: str,
                                    accession_number: str) -> str | None:
    """
    Async version of download_filing_xml.

    Download a Form 4 XML and cache it locally using async HTTP.
    """
    os.makedirs(config.FILINGS_CACHE_DIR, exist_ok=True)

    safe_accession = accession_number.replace("-", "_")
    local_path = os.path.join(config.FILINGS_CACHE_DIR, f"{safe_accession}.xml")

    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        logger.debug(f"Cache hit: {local_path}")
        return local_path

    try:
        resp = await client.get(xml_url)
        with open(local_path, "wb") as f:
            f.write(resp.content)
        logger.debug(f"Async downloaded: {xml_url} -> {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Async failed to download {xml_url}: {e}")
        return None


async def ingest_company_async(cik: str, client: AsyncEdgarClient = None,
                               db_path: str = None, max_filings: int = None,
                               start_date: str = None, end_date: str = None) -> list[dict]:
    """
    Async version of ingest_company.

    Ingest Form 4 filings for a single company using async HTTP.
    """
    close_client = False
    if client is None:
        client = AsyncEdgarClient()
        close_client = True

    try:
        cik_padded = str(cik).zfill(10)

        logger.info(f"Async searching for Form 4 filings for CIK {cik_padded}...")
        form4s = await search_form4_filings_async(
            client, cik_padded,
            start_date=start_date,
            end_date=end_date,
            max_results=max_filings or 200,
        )

        if max_filings:
            form4s = form4s[:max_filings]

        results = []
        with get_connection(db_path) as conn:
            known_accessions = get_filing_accession_numbers(conn, cik_padded)

            for filing_meta in form4s:
                accession = filing_meta["accession_number"]

                if accession in known_accessions:
                    logger.debug(f"Skipping already-ingested filing: {accession}")
                    continue

                # Resolve XML URL (use issuer CIK for correct SEC archive path)
                xml_url, filer_cik = await resolve_filing_xml_url_async(client, accession, issuer_cik=cik_padded)
                if not xml_url:
                    logger.warning(f"Could not resolve XML URL for {accession}")
                    continue

                local_path = await download_filing_xml_async(client, xml_url, accession)

                filing_row = {
                    "accession_number": accession,
                    "cik_issuer": cik_padded,
                    "cik_owner": filer_cik,
                    "owner_name": None,
                    "officer_title": None,
                    "is_officer": 0,
                    "is_director": 0,
                    "is_ten_pct_owner": 0,
                    "is_other": 0,
                    "is_amendment": 1 if filing_meta.get("is_amendment") else 0,
                    "amendment_type": None,
                    "period_of_report": None,
                    "aff10b5one": 0,
                    "additional_owners": None,
                    "filing_date": filing_meta.get("filing_date", ""),
                    "xml_url": xml_url,
                    "raw_xml_path": local_path,
                    "parsed_at": None,
                    "parse_error": None,
                }
                upsert_filing(conn, filing_row)

                filing_meta["xml_url"] = xml_url
                filing_meta["raw_xml_path"] = local_path
                results.append(filing_meta)

        logger.info(f"Async ingested {len(results)} new filings for CIK {cik_padded}")
        return results
    finally:
        if close_client:
            await client.close()


async def ingest_universe_async(db_path: str = None, max_filings_per_company: int = None,
                                start_date: str = None, end_date: str = None,
                                concurrency: int = 5) -> dict:
    """
    Async version of ingest_universe.

    Ingest Form 4 filings for all companies using async HTTP with controlled concurrency.

    Args:
        db_path: Optional database path.
        max_filings_per_company: Maximum filings to fetch per company.
        start_date: Start date for historical backfill (YYYY-MM-DD).
        end_date: End date for historical backfill (YYYY-MM-DD).
        concurrency: Maximum number of concurrent company ingestions.
                     Keep low to respect SEC rate limits.

    Returns summary stats.
    """
    from db import get_companies

    with get_connection(db_path) as conn:
        companies = get_companies(conn)

    total_filings = 0
    errors = []

    # Use semaphore to limit concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def ingest_with_semaphore(company, client):
        async with semaphore:
            cik = company["cik"]
            ticker = company["ticker"]
            try:
                results = await ingest_company_async(
                    cik, client, db_path, max_filings_per_company,
                    start_date=start_date, end_date=end_date,
                )
                logger.info(f"  {ticker}: {len(results)} new filings")
                return len(results), None
            except Exception as e:
                logger.error(f"  {ticker}: async ingestion failed: {e}")
                return 0, {"ticker": ticker, "cik": cik, "error": str(e)}

    async with AsyncEdgarClient() as client:
        tasks = [ingest_with_semaphore(company, client) for company in companies]
        results = await asyncio.gather(*tasks)

    for count, error in results:
        total_filings += count
        if error:
            errors.append(error)

    return {
        "total_new_filings": total_filings,
        "companies_processed": len(companies),
        "errors": errors,
    }
