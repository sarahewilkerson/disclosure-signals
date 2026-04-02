"""
Command-line interface for CPPI.

Usage:
    python -m cppi.cli ingest --days 90
    python -m cppi.cli parse
    python -m cppi.cli score --window 90
    python -m cppi.cli report --output output/cppi_report.txt
    python -m cppi.cli enrich --members --committees
"""

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from cppi import __version__
from cppi.config import LOG_LEVEL, MIN_MEMBERS, MIN_TRANSACTIONS
from cppi.db import get_connection, init_db

logger = logging.getLogger(__name__)


def setup_logging():
    """Configure logging for CLI usage."""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_init(args):
    """Initialize the database."""
    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized successfully")


def _download_fd_xml_ptrs(years: list[int], cache_dir: Path) -> list[dict]:
    """Download FD ZIP files and extract PTR document IDs.

    The House Clerk provides bulk XML files at:
    https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{YEAR}FD.ZIP

    These contain ALL House financial disclosures with FilingType=P for PTRs.

    Args:
        years: List of years to download (e.g., [2024, 2025, 2026])
        cache_dir: Directory to cache downloads

    Returns:
        List of PTR dicts with doc_id, name, state_district, filing_date, year
    """
    import io
    import zipfile
    from xml.etree import ElementTree as ET

    import requests

    ptrs = []
    fd_cache = cache_dir / "fd_xml"
    fd_cache.mkdir(parents=True, exist_ok=True)

    for year in years:
        xml_cache = fd_cache / f"{year}FD.xml"

        # Check cache first
        if xml_cache.exists():
            logger.info(f"Using cached {year}FD.xml")
            xml_content = xml_cache.read_text()
        else:
            # Download the ZIP file
            url = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.ZIP"
            logger.info(f"Downloading {url}...")

            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()

                # Extract XML from ZIP
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    xml_filename = f"{year}FD.xml"
                    if xml_filename in zf.namelist():
                        xml_content = zf.read(xml_filename).decode("utf-8")
                        # Cache for future use
                        xml_cache.write_text(xml_content)
                        logger.info(f"Cached {xml_filename}")
                    else:
                        logger.warning(f"No {xml_filename} in ZIP")
                        continue
            except Exception as e:
                logger.error(f"Failed to download {year}FD.ZIP: {e}")
                continue

        # Parse XML to extract PTRs
        try:
            root = ET.fromstring(xml_content)
            for member in root.findall(".//Member"):
                filing_type = member.findtext("FilingType", "")
                if filing_type != "P":  # P = Periodic Transaction Report
                    continue

                doc_id = member.findtext("DocID", "")
                if not doc_id:
                    continue

                name_parts = []
                first = member.findtext("First", "")
                last = member.findtext("Last", "")
                suffix = member.findtext("Suffix", "")
                if first:
                    name_parts.append(first)
                if last:
                    name_parts.append(last)
                if suffix:
                    name_parts.append(suffix)
                name = " ".join(name_parts)

                state = member.findtext("StateDst", "")
                filing_date = member.findtext("FilingDate", "")

                ptrs.append({
                    "doc_id": doc_id,
                    "name": name,
                    "state_district": state,
                    "filing_date": filing_date,
                    "year": year,
                })

            logger.info(f"Found {len([p for p in ptrs if p['year'] == year])} PTRs in {year}FD.xml")

        except ET.ParseError as e:
            logger.error(f"Failed to parse {year}FD.xml: {e}")
            continue

    return ptrs


def _load_fd_xml_names(cache_dir: Path) -> dict[str, str]:
    """Load DocID→Name mapping from cached FD XML files.

    Used during parsing to populate filer_name for House filings,
    since the PDF itself often doesn't contain the filer's name.

    Args:
        cache_dir: Root cache directory containing fd_xml/ subdirectory

    Returns:
        Dict mapping doc_id (str) → filer name (str)
    """
    from xml.etree import ElementTree as ET

    docid_to_name: dict[str, str] = {}
    fd_cache = cache_dir / "fd_xml"

    if not fd_cache.exists():
        logger.warning("No FD XML cache found - House filer names will be Unknown")
        return docid_to_name

    for xml_file in fd_cache.glob("*.xml"):
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            for member in root.findall(".//Member"):
                doc_id = member.findtext("DocID", "").strip()
                if not doc_id:
                    continue

                # Build name from components
                first = member.findtext("First", "").strip()
                last = member.findtext("Last", "").strip()
                suffix = member.findtext("Suffix", "").strip()

                name_parts = []
                if first:
                    name_parts.append(first)
                if last:
                    name_parts.append(last)
                if suffix:
                    name_parts.append(suffix)

                name = " ".join(name_parts)
                if name:
                    docid_to_name[doc_id] = name

            logger.debug(f"Loaded {len([k for k in docid_to_name if k.startswith(xml_file.stem[:4])])} names from {xml_file.name}")

        except ET.ParseError as e:
            logger.error(f"Failed to parse {xml_file}: {e}")
            continue

    logger.info(f"Loaded {len(docid_to_name)} House filer names from FD XML cache")
    return docid_to_name


def cmd_ingest(args):
    """Ingest filings from House and Senate disclosure sites."""
    from cppi.connectors.house import HouseConnector
    from cppi.connectors.senate import SenateConnector

    days = args.days
    since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    logger.info(f"Ingesting filings from the last {days} days (since {since_date})")

    house_count = 0
    senate_count = 0

    # Ingest House filings
    if not args.senate_only:
        logger.info("Ingesting House filings...")
        house = HouseConnector()

        if args.bulk:
            # Bulk download from FD XML files
            current_year = datetime.now().year
            years = list(range(2024, current_year + 1))

            logger.info(f"Bulk ingestion: downloading FD XML for years {years}")
            ptrs = _download_fd_xml_ptrs(years, house.cache_dir.parent.parent)

            # Filter by date if requested
            if days < 365 * 10:  # Only filter if not requesting all time
                from datetime import datetime as dt
                cutoff = dt.now() - timedelta(days=days)
                filtered = []
                for ptr in ptrs:
                    try:
                        fd = ptr.get("filing_date", "")
                        if "/" in fd:
                            fd_date = dt.strptime(fd, "%m/%d/%Y")
                        elif "-" in fd:
                            fd_date = dt.strptime(fd, "%Y-%m-%d")
                        else:
                            filtered.append(ptr)  # Include if can't parse
                            continue
                        if fd_date >= cutoff:
                            filtered.append(ptr)
                    except ValueError:
                        filtered.append(ptr)
                ptrs = filtered
                logger.info(f"Filtered to {len(ptrs)} PTRs from last {days} days")

            # Download PDFs
            logger.info(f"Downloading {len(ptrs)} House PTR PDFs...")
            downloaded = 0
            skipped = 0
            failed = 0

            for i, ptr in enumerate(ptrs):
                doc_id = ptr["doc_id"]
                year = ptr["year"]

                # Check if already cached
                cache_path = house.cache_dir / f"{doc_id}.pdf"
                if cache_path.exists():
                    skipped += 1
                    continue

                # Download
                result = house.download_pdf(doc_id, year=year)
                if result:
                    downloaded += 1
                else:
                    failed += 1

                # Progress every 50
                if (i + 1) % 50 == 0:
                    logger.info(f"Progress: {i+1}/{len(ptrs)} (downloaded: {downloaded}, skipped: {skipped}, failed: {failed})")

            house_count = len(house.list_cached_pdfs())
            logger.info(f"House bulk ingest: {downloaded} downloaded, {skipped} already cached, {failed} failed")
            logger.info(f"Total House PDFs in cache: {house_count}")
        else:
            # Legacy: just list cached files
            cached = house.list_cached_pdfs()
            house_count = len(cached)
            logger.info(f"Found {house_count} cached House PDFs (use --bulk to download more)")

    # Ingest Senate filings
    if not args.house_only:
        logger.info("Ingesting Senate filings...")
        senate = SenateConnector()
        # Note: Session not required for listing cached files
        cached = senate.list_cached_files()
        senate_count = sum(len(v) for v in cached.values())
        logger.info(f"Found {senate_count} cached Senate files")

    logger.info(f"Ingest complete: {house_count} House, {senate_count} Senate")


def _parse_senate_ocr_text(text: str) -> list[dict]:
    """Parse transactions from OCR'd Senate paper filing text.

    Senate paper filings have a similar structure to electronic filings
    but may have OCR artifacts. This parser is more lenient.

    Args:
        text: OCR'd text from paper filing images

    Returns:
        List of transaction dictionaries with keys:
        - owner, asset_name, ticker, asset_type
        - transaction_type, transaction_date
        - amount_min, amount_max
    """
    import re

    transactions = []
    lines = text.split("\n")

    # Patterns for Senate filings
    date_pattern = re.compile(r"(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})")
    amount_pattern = re.compile(r"\$[\d,]+(?:\s*-\s*\$[\d,]+)?")

    current_entry = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Check if this line looks like a transaction entry
        has_date = bool(date_pattern.search(line))
        has_amount = bool(amount_pattern.search(line))

        # Accumulate lines until we have enough to parse
        if has_date or has_amount:
            if current_entry and has_date:
                # Parse previous entry
                txn = _parse_senate_entry("\n".join(current_entry))
                if txn:
                    transactions.append(txn)
                current_entry = [line]
            else:
                current_entry.append(line)
        elif current_entry:
            current_entry.append(line)

    # Parse final entry
    if current_entry:
        txn = _parse_senate_entry("\n".join(current_entry))
        if txn:
            transactions.append(txn)

    return transactions


def _parse_senate_entry(entry_text: str) -> dict | None:
    """Parse a single transaction entry from Senate OCR text.

    Args:
        entry_text: Text for a single transaction

    Returns:
        Transaction dict or None if parsing fails
    """
    import re
    from datetime import datetime

    from cppi.parsing import AMOUNT_RANGES

    if not entry_text or len(entry_text) < 10:
        return None

    # Initialize transaction dict
    txn = {
        "owner": "self",
        "asset_name": None,
        "ticker": None,
        "asset_type": None,
        "transaction_type": "unknown",
        "transaction_date": None,
        "amount_min": None,
        "amount_max": None,
    }

    # Extract ticker
    ticker_match = re.search(r"\(([A-Z]{1,5})\)", entry_text)
    if ticker_match:
        txn["ticker"] = ticker_match.group(1)

    # Extract date
    date_patterns = [
        (r"(\d{1,2})/(\d{1,2})/(\d{4})", "%m/%d/%Y"),
        (r"(\d{1,2})-(\d{1,2})-(\d{4})", "%m-%d-%Y"),
        (r"(\d{1,2})/(\d{1,2})/(\d{2})", "%m/%d/%y"),
    ]
    for pattern, fmt in date_patterns:
        match = re.search(pattern, entry_text)
        if match:
            try:
                date_str = match.group(0)
                txn["transaction_date"] = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                pass

    # Extract amount
    amount_match = re.search(r"\$(\d{1,3}(?:,\d{3})*)\s*[-—]\s*\$(\d{1,3}(?:,\d{3})*)", entry_text)
    if amount_match:
        try:
            txn["amount_min"] = int(amount_match.group(1).replace(",", ""))
            txn["amount_max"] = int(amount_match.group(2).replace(",", ""))
        except ValueError:
            pass

    # Also check for standard amount range text
    if not txn["amount_min"]:
        for range_text, (min_val, max_val) in AMOUNT_RANGES.items():
            if range_text.lower() in entry_text.lower():
                txn["amount_min"] = min_val
                txn["amount_max"] = max_val
                break

    # Extract transaction type
    text_lower = entry_text.lower()
    if any(kw in text_lower for kw in ["purchase", "buy", "bought", "acquire"]):
        txn["transaction_type"] = "purchase"
    elif any(kw in text_lower for kw in ["sale", "sell", "sold", "dispos"]):
        txn["transaction_type"] = "sale"
    elif "exchange" in text_lower:
        txn["transaction_type"] = "exchange"

    # Extract owner type
    if "spouse" in text_lower or " sp " in text_lower:
        txn["owner"] = "spouse"
    elif "joint" in text_lower or " jt " in text_lower:
        txn["owner"] = "joint"
    elif "child" in text_lower or " dc " in text_lower:
        txn["owner"] = "dependent"

    # Extract asset name (everything before the ticker or date)
    asset_name = entry_text
    # Remove ticker
    if txn["ticker"]:
        asset_name = re.sub(rf"\({txn['ticker']}\)", "", asset_name)
    # Remove dates
    asset_name = re.sub(r"\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}", "", asset_name)
    # Remove amounts
    asset_name = re.sub(r"\$[\d,]+(?:\s*[-—]\s*\$[\d,]+)?", "", asset_name)
    # Clean up
    asset_name = re.sub(r"\s+", " ", asset_name).strip()
    if asset_name and len(asset_name) > 3:
        txn["asset_name"] = asset_name[:200]  # Truncate if too long

    # Only return if we have meaningful data
    if txn["ticker"] or txn["asset_name"] or (txn["amount_min"] and txn["amount_max"]):
        return txn

    return None


def cmd_parse(args):
    """Parse downloaded filings to extract transactions."""
    from cppi.connectors.house import HouseConnector
    from cppi.connectors.senate import SenateConnector
    from cppi.ocr import is_tesseract_available, ocr_image
    from cppi.parsing import AMOUNT_RANGES, parse_house_pdf
    from cppi.resolution import resolve_transaction

    logger.info("Parsing downloaded filings...")

    total_txns = 0
    included = 0
    excluded = 0
    skipped = 0  # Track filings skipped due to unchanged source_hash
    zero_txn_filings = []  # Track filings that yield 0 transactions
    force = getattr(args, "force", False)  # --force flag to bypass hash check

    # ============================================
    # HOUSE FILINGS (PDFs)
    # ============================================
    house = HouseConnector()
    filing_ids = house.list_cached_pdfs()
    logger.info(f"Found {len(filing_ids)} cached House PDFs to parse")

    # Load FD XML for filer name lookup (DocID → Name)
    # FD XML is in cache/fd_xml/, house.cache_dir is cache/pdfs/house/
    docid_to_name = _load_fd_xml_names(house.cache_dir.parent.parent)

    with get_connection() as conn:
        for filing_id in filing_ids:
            # Construct full path from filing ID
            pdf_path = house.cache_dir / f"{filing_id}.pdf"

            # Check if already parsed and unchanged (skip unless --force)
            current_hash = house.get_pdf_hash(filing_id)
            if not force and current_hash:
                existing = conn.execute(
                    "SELECT source_hash FROM filings WHERE filing_id = ?", (filing_id,)
                ).fetchone()
                if existing and existing[0] == current_hash:
                    skipped += 1
                    continue  # Already parsed and unchanged

            try:
                filing = parse_house_pdf(pdf_path)
                logger.debug(f"Parsed {filing_id}: {len(filing.transactions)} transactions")

                # Validate filing_id - use filename if parser returned empty
                if not filing.filing_id or filing.filing_id.strip() == "":
                    logger.warning(f"Empty filing_id from parser, using filename: {filing_id}")
                    filing.filing_id = filing_id

                # Track filings with 0 transactions
                if len(filing.transactions) == 0:
                    is_paper = filing_id.startswith("822")
                    filing_type = "paper" if is_paper else "electronic"
                    logger.warning(
                        f"No transactions extracted from House {filing_type} filing {filing_id}"
                    )
                    zero_txn_filings.append(("house", filing_id, filing_type))

                # Construct source URL
                year = datetime.now().year
                if filing_id.startswith("822"):
                    source_url = f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{filing_id}.pdf"
                else:
                    # Electronic filings have year in first 4 digits
                    try:
                        year = int(filing_id[:4])
                    except ValueError:
                        pass
                    source_url = f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{filing_id}.pdf"

                # Insert filing record FIRST (foreign key constraint)
                # Use FD XML lookup for filer name, fall back to PDF extraction, then "Unknown"
                filer_name = docid_to_name.get(filing_id, filing.filer_name) or "Unknown"

                conn.execute("""
                    INSERT OR REPLACE INTO filings (
                        filing_id, bioguide_id, chamber, filer_name, filing_type,
                        disclosure_date, source_url, source_format, raw_path, parsed_at,
                        source_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    filing.filing_id,
                    None,
                    "house",
                    filer_name,
                    "PTR",
                    datetime.now().strftime("%Y-%m-%d"),
                    source_url,
                    "pdf_electronic",
                    str(pdf_path),
                    datetime.now().isoformat(),
                    current_hash,
                ))

                # Then insert transactions
                for txn in filing.transactions:
                    # Resolve entity
                    res = resolve_transaction(
                        asset_name=txn.asset_name or "",
                        ticker=txn.ticker,
                        asset_type_code=txn.asset_type,
                    )

                    # Insert into database
                    conn.execute("""
                        INSERT OR REPLACE INTO transactions (
                            filing_id, bioguide_id, owner_type, asset_name_raw,
                            asset_type, resolved_ticker, resolved_company,
                            resolution_method, resolution_confidence,
                            transaction_type, execution_date, disclosure_date,
                            ingestion_date, amount_min, amount_max,
                            include_in_signal, exclusion_reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        filing.filing_id,
                        None,  # bioguide_id not available from PDF
                        txn.owner,
                        txn.asset_name,
                        txn.asset_type,
                        res.resolved_ticker,
                        res.resolved_company,
                        res.resolution_method,
                        res.resolution_confidence,
                        txn.transaction_type,
                        txn.transaction_date.strftime("%Y-%m-%d") if txn.transaction_date else datetime.now().strftime("%Y-%m-%d"),
                        txn.notification_date.strftime("%Y-%m-%d") if txn.notification_date else datetime.now().strftime("%Y-%m-%d"),
                        datetime.now().strftime("%Y-%m-%d"),
                        txn.amount_min,
                        txn.amount_max,
                        1 if res.include_in_signal else 0,
                        res.exclusion_reason,
                    ))

                    total_txns += 1
                    if res.include_in_signal:
                        included += 1
                    else:
                        excluded += 1

                conn.commit()

            except Exception as e:
                logger.error(f"Error parsing House PDF {pdf_path}: {e}")

        # ============================================
        # SENATE FILINGS (HTML + GIF paper filings)
        # ============================================
        senate = SenateConnector()
        cached = senate.list_cached_files()
        ptr_ids = cached.get("ptr", [])
        paper_ids = cached.get("paper", [])

        # Load senator names from senate_ptrs.json metadata
        # Keys are first 8 chars of UUID (no dashes) to match ptr_id format from list_cached_files()
        senate_metadata: dict[str, str] = {}
        senate_ptrs_path = Path("senate_ptrs.json")
        if senate_ptrs_path.exists():
            try:
                with open(senate_ptrs_path) as f:
                    data = json.load(f)
                    for record in data.get("records", []):
                        uuid = record.get("uuid", "").replace("-", "")
                        first_name = record.get("first_name", "")
                        last_name = record.get("last_name", "")
                        name = f"{first_name} {last_name}".strip()
                        if uuid and name:
                            # Use first 8 chars to match ptr_id/paper_id format
                            senate_metadata[uuid[:8]] = name
                logger.info(f"Loaded {len(senate_metadata)} senator names from senate_ptrs.json")
            except Exception as e:
                logger.warning(f"Could not load senate_ptrs.json: {e}")

        logger.info(f"Found {len(ptr_ids)} Senate electronic PTRs and {len(paper_ids)} paper filings")

        # Parse electronic PTRs (HTML)
        for ptr_id in ptr_ids:
            html_path = senate.cache_dir / f"ptr_{ptr_id}.html"
            if not html_path.exists():
                continue

            # Hash HTML content for skip check
            html_hash = hashlib.sha256(html_path.read_bytes()).hexdigest()
            filing_id_senate = f"senate_{ptr_id}"

            # Check if already parsed and unchanged (skip unless --force)
            if not force:
                existing = conn.execute(
                    "SELECT source_hash FROM filings WHERE filing_id = ?", (filing_id_senate,)
                ).fetchone()
                if existing and existing[0] == html_hash:
                    skipped += 1
                    continue  # Already parsed and unchanged

            try:
                transactions = senate.parse_ptr_transactions(html_path)
                logger.debug(f"Parsed Senate PTR {ptr_id}: {len(transactions)} transactions")

                # Track filings with 0 transactions
                if len(transactions) == 0:
                    logger.warning(f"No transactions extracted from Senate PTR {ptr_id}")
                    zero_txn_filings.append(("senate", ptr_id, "electronic"))

                # Insert filing record
                conn.execute("""
                    INSERT OR REPLACE INTO filings (
                        filing_id, bioguide_id, chamber, filer_name, filing_type,
                        disclosure_date, source_url, source_format, raw_path, parsed_at,
                        source_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    filing_id_senate,
                    None,
                    "senate",
                    senate_metadata.get(ptr_id, "Unknown"),
                    "PTR",
                    datetime.now().strftime("%Y-%m-%d"),
                    f"https://efdsearch.senate.gov/search/view/ptr/{ptr_id}/",
                    "html_electronic",
                    str(html_path),
                    datetime.now().isoformat(),
                    html_hash,
                ))

                # Insert transactions
                for txn in transactions:
                    # Parse amount range
                    amount_min, amount_max = None, None
                    if txn.amount_range:
                        for range_text, (min_val, max_val) in AMOUNT_RANGES.items():
                            if range_text in txn.amount_range:
                                amount_min, amount_max = min_val, max_val
                                break

                    # Resolve entity
                    res = resolve_transaction(
                        asset_name=txn.asset_name or "",
                        ticker=txn.ticker,
                        asset_type_code=txn.asset_type,
                    )

                    # Normalize transaction type
                    txn_type = "purchase" if "purchase" in (txn.transaction_type or "").lower() else \
                               "sale" if "sale" in (txn.transaction_type or "").lower() else \
                               "exchange" if "exchange" in (txn.transaction_type or "").lower() else "unknown"

                    conn.execute("""
                        INSERT OR REPLACE INTO transactions (
                            filing_id, bioguide_id, owner_type, asset_name_raw,
                            asset_type, resolved_ticker, resolved_company,
                            resolution_method, resolution_confidence,
                            transaction_type, execution_date, disclosure_date,
                            ingestion_date, amount_min, amount_max,
                            include_in_signal, exclusion_reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        f"senate_{ptr_id}",
                        None,
                        txn.owner.lower() if txn.owner else "self",
                        txn.asset_name,
                        txn.asset_type,
                        res.resolved_ticker,
                        res.resolved_company,
                        res.resolution_method,
                        res.resolution_confidence,
                        txn_type,
                        txn.transaction_date.strftime("%Y-%m-%d") if txn.transaction_date else None,
                        datetime.now().strftime("%Y-%m-%d"),  # Use current date for disclosure_date
                        datetime.now().strftime("%Y-%m-%d"),
                        amount_min,
                        amount_max,
                        1 if res.include_in_signal else 0,
                        res.exclusion_reason,
                    ))

                    total_txns += 1
                    if res.include_in_signal:
                        included += 1
                    else:
                        excluded += 1

                conn.commit()

            except Exception as e:
                logger.error(f"Error parsing Senate PTR {ptr_id}: {e}")

        # Parse paper filings (GIF images via OCR)
        # Find all paper GIF files directly (they may be named paper_ID.gif or paper_ID_pageN.gif)
        all_paper_gifs = list(senate.cache_dir.glob("paper_*.gif"))

        # Group by filing ID (extract ID from filename)
        paper_filings: dict[str, list] = {}
        for gif_path in all_paper_gifs:
            # Extract ID: paper_XXXXXXXX.gif or paper_XXXXXXXX_page1.gif
            name = gif_path.stem  # paper_XXXXXXXX or paper_XXXXXXXX_page1
            parts = name.replace("paper_", "").split("_page")
            filing_id = parts[0]
            if filing_id not in paper_filings:
                paper_filings[filing_id] = []
            paper_filings[filing_id].append(gif_path)

        logger.info(f"Found {len(paper_filings)} Senate paper filings with {len(all_paper_gifs)} GIF images")

        if not is_tesseract_available():
            if paper_filings:
                logger.warning(
                    f"Skipping {len(paper_filings)} Senate paper filings: "
                    "Tesseract OCR not installed. Run: brew install tesseract"
                )
        else:
            for paper_id, gif_files in paper_filings.items():
                gif_files = sorted(gif_files)  # Ensure page order

                # Hash all GIF files for skip check (combine hashes)
                combined_hash = hashlib.sha256()
                for gif_path in gif_files:
                    combined_hash.update(gif_path.read_bytes())
                paper_hash = combined_hash.hexdigest()
                filing_id_paper = f"senate_paper_{paper_id}"

                # Check if already parsed and unchanged (skip unless --force)
                if not force:
                    existing = conn.execute(
                        "SELECT source_hash FROM filings WHERE filing_id = ?", (filing_id_paper,)
                    ).fetchone()
                    if existing and existing[0] == paper_hash:
                        skipped += 1
                        continue  # Already parsed and unchanged

                try:
                    # OCR all pages and combine text
                    all_text = []
                    for gif_path in gif_files:
                        result = ocr_image(gif_path)
                        if result.text:
                            all_text.append(result.text)

                    combined_text = "\n".join(all_text)
                    if not combined_text.strip():
                        logger.warning(f"OCR produced no text for Senate paper filing {paper_id}")
                        continue

                    # Parse transactions from OCR text
                    ocr_transactions = _parse_senate_ocr_text(combined_text)
                    logger.debug(f"Parsed Senate paper {paper_id}: {len(ocr_transactions)} transactions via OCR")

                    # Track filings with 0 transactions
                    if len(ocr_transactions) == 0:
                        logger.warning(f"No transactions extracted from Senate paper filing {paper_id}")
                        zero_txn_filings.append(("senate", paper_id, "paper"))

                    # Insert filing record
                    conn.execute("""
                        INSERT OR REPLACE INTO filings (
                            filing_id, bioguide_id, chamber, filer_name, filing_type,
                            disclosure_date, source_url, source_format, raw_path, parsed_at,
                            source_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        filing_id_paper,
                        None,
                        "senate",
                        senate_metadata.get(paper_id, "Unknown"),
                        "PTR",
                        datetime.now().strftime("%Y-%m-%d"),
                        f"https://efdsearch.senate.gov/search/view/paper/{paper_id}/",
                        "gif_paper_ocr",
                        str(gif_files[0]),
                        datetime.now().isoformat(),
                        paper_hash,
                    ))

                    # Insert transactions
                    for txn in ocr_transactions:
                        res = resolve_transaction(
                            asset_name=txn.get("asset_name", ""),
                            ticker=txn.get("ticker"),
                            asset_type_code=txn.get("asset_type"),
                        )

                        # Use execution_date if available, otherwise use disclosure_date as fallback
                        exec_date = txn.get("transaction_date") or datetime.now().strftime("%Y-%m-%d")

                        conn.execute("""
                            INSERT OR REPLACE INTO transactions (
                                filing_id, bioguide_id, owner_type, asset_name_raw,
                                asset_type, resolved_ticker, resolved_company,
                                resolution_method, resolution_confidence,
                                transaction_type, execution_date, disclosure_date,
                                ingestion_date, amount_min, amount_max,
                                include_in_signal, exclusion_reason
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            f"senate_paper_{paper_id}",
                            None,
                            txn.get("owner", "self"),
                            txn.get("asset_name"),
                            txn.get("asset_type"),
                            res.resolved_ticker,
                            res.resolved_company,
                            res.resolution_method,
                            res.resolution_confidence,
                            txn.get("transaction_type", "unknown"),
                            exec_date,
                            datetime.now().strftime("%Y-%m-%d"),  # Use current date for disclosure_date
                            datetime.now().strftime("%Y-%m-%d"),
                            txn.get("amount_min"),
                            txn.get("amount_max"),
                            1 if res.include_in_signal else 0,
                            res.exclusion_reason,
                        ))

                        total_txns += 1
                        if res.include_in_signal:
                            included += 1
                        else:
                            excluded += 1

                    conn.commit()

                except Exception as e:
                    logger.error(f"Error parsing Senate paper filing {paper_id}: {e}")

    # Report filings with 0 transactions
    if zero_txn_filings:
        logger.warning(
            f"{len(zero_txn_filings)} filings yielded 0 transactions (may need OCR or manual review)"
        )
        # Group by chamber and type
        house_paper = sum(1 for c, _, t in zero_txn_filings if c == "house" and t == "paper")
        house_electronic = sum(1 for c, _, t in zero_txn_filings if c == "house" and t == "electronic")
        senate_electronic = sum(1 for c, _, t in zero_txn_filings if c == "senate" and t == "electronic")
        senate_paper = sum(1 for c, _, t in zero_txn_filings if c == "senate" and t == "paper")

        details = []
        if house_paper:
            details.append(f"{house_paper} House paper")
        if house_electronic:
            details.append(f"{house_electronic} House electronic")
        if senate_electronic:
            details.append(f"{senate_electronic} Senate electronic")
        if senate_paper:
            details.append(f"{senate_paper} Senate paper")

        logger.warning(f"  Breakdown: {', '.join(details)}")

    if skipped > 0:
        logger.info(f"Parse complete: {total_txns} transactions ({included} included, {excluded} excluded), {skipped} filings skipped (unchanged)")
    else:
        logger.info(f"Parse complete: {total_txns} transactions ({included} included, {excluded} excluded)")


def cmd_score(args):
    """Compute positioning scores."""
    from cppi.services.scoring_service import compute_and_store_score

    result = compute_and_store_score(args.window)
    if result is None:
        return

    print(f"\n{'=' * 50}")
    print("POSITIONING SCORE COMPUTED")
    print(f"{'=' * 50}")
    print(f"Window:       {result.window} days")
    print(f"Transactions: {result.transaction_count}")
    print(f"Members:      {result.unique_members}")
    print(f"Breadth:      {result.breadth_pct:+.1%} ({result.buyers} buyers, {result.sellers} sellers)")
    print(f"Net Volume:   ${result.net_volume:,.0f}")
    print(f"Confidence:   {result.confidence_tier} ({result.confidence_score:.2f})")
    print()


def cmd_report(args):
    """Generate positioning report."""
    from cppi.services.reporting_service import build_report

    result = build_report(args.window, args.output, args.format)
    if result is None:
        return

    if args.stdout:
        print(result.content)


def cmd_enrich(args):
    """Enrich member data from external sources."""
    # Lazy import to avoid dependency on requests at import time
    from cppi.enrichment.congress_gov import CongressGovClient

    if args.members:
        logger.info("Enriching member data from Congress.gov API...")

        client = CongressGovClient()

        # Try to load from cache first if --no-cache not specified
        members = None
        if not args.no_cache:
            members = client.load_from_cache()
            if members:
                logger.info(f"Loaded {len(members)} members from cache")

        # Fetch from API if no cache
        if not members:
            if not client.api_key:
                logger.error(
                    "No Congress.gov API key found. Set CONGRESS_API_KEY environment variable "
                    "or get a key at https://api.data.gov/signup/"
                )
                return

            members = client.get_current_members()
            if not members:
                logger.error("Failed to fetch members from Congress.gov API")
                return

            # Optionally enrich with committee data
            if args.committees:
                logger.info("Fetching committee assignments...")
                members = client.enrich_members_with_committees(members)

            # Save to cache
            client.save_to_cache(members)

        # Insert into database
        with get_connection() as conn:
            inserted = 0
            for member in members:
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO members (
                            bioguide_id, name, party, state, chamber, in_office, committees
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        member.bioguide_id,
                        member.name,
                        member.party,
                        member.state,
                        member.chamber,
                        1 if member.in_office else 0,
                        json.dumps(member.committees) if member.committees else None,
                    ))
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Error inserting member {member.bioguide_id}: {e}")

            conn.commit()

        print("\nMember Enrichment Complete")
        print("=" * 50)
        print(f"  Total members: {len(members)}")
        print(f"  Inserted/updated: {inserted}")
        print(f"  House: {sum(1 for m in members if m.chamber == 'house')}")
        print(f"  Senate: {sum(1 for m in members if m.chamber == 'senate')}")
        if args.committees:
            with_committees = sum(1 for m in members if m.committees)
            print(f"  With committee data: {with_committees}")
        print()

    else:
        logger.error("No enrichment target specified. Use --members to enrich member data.")


def cmd_analyze(args):
    """Run analysis commands."""
    from cppi.scoring import score_transaction

    window = args.window
    reference_date = datetime.now()
    cutoff_date = reference_date - timedelta(days=window)

    with get_connection() as conn:
        # Get transactions within window
        rows = conn.execute("""
            SELECT
                id, filing_id, owner_type, asset_name_raw, asset_type,
                resolved_ticker, transaction_type, execution_date,
                amount_min, amount_max, include_in_signal,
                resolution_confidence
            FROM transactions
            WHERE include_in_signal = 1
              AND execution_date >= ?
            ORDER BY execution_date DESC
        """, (cutoff_date.strftime("%Y-%m-%d"),)).fetchall()

        if not rows:
            logger.error("No transactions found in window. Run 'cppi parse' first.")
            return

        # Score transactions
        scored = []
        for row in rows:
            exec_date = None
            if row["execution_date"]:
                try:
                    exec_date = datetime.strptime(row["execution_date"], "%Y-%m-%d")
                except ValueError:
                    pass

            txn = score_transaction(
                member_id=row["filing_id"],
                ticker=row["resolved_ticker"],
                transaction_type=row["transaction_type"] or "purchase",
                execution_date=exec_date,
                amount_min=row["amount_min"],
                amount_max=row["amount_max"],
                owner_type=row["owner_type"] or "self",
                resolution_confidence=row["resolution_confidence"] or 1.0,
                signal_weight=1.0,
                reference_date=reference_date,
            )
            scored.append(txn)

        if args.analysis_type == "sensitivity":
            # Lazy import
            from cppi.analysis.sensitivity import (
                format_sensitivity_report,
                run_sensitivity_analysis,
            )

            logger.info(f"Running sensitivity analysis on {len(scored)} transactions...")
            results = run_sensitivity_analysis(scored)
            report = format_sensitivity_report(results)
            print(report)

        elif args.analysis_type == "weights":
            # Lazy import
            from cppi.analysis.weight_comparison import (
                compare_weighting_methods,
                format_weight_comparison_report,
            )

            logger.info(f"Comparing weighting methods on {len(scored)} transactions...")
            comparison = compare_weighting_methods(scored)
            report = format_weight_comparison_report(comparison)
            print(report)

        elif args.analysis_type == "crossref":
            # Lazy import
            from cppi.analysis.crossref import (
                format_crossref_report,
                run_crossref_analysis,
            )
            from cppi.config import DB_PATH

            insider_db = getattr(args, "insider_db", None)
            logger.info(f"Running cross-reference analysis (window={window} days)...")

            report_data = run_crossref_analysis(
                cppi_db_path=DB_PATH,
                insider_db_path=insider_db,
                window_days=window,
                reference_date=reference_date,
            )

            report = format_crossref_report(report_data)
            print(report)

            # Print JSON summary if requested
            if getattr(args, "json", False):
                import json as json_module
                print("\n" + json_module.dumps(report_data.to_dict(), indent=2))

        else:
            logger.error(f"Unknown analysis type: {args.analysis_type}")


def cmd_validate(args):
    """Validate CPPI data against external sources."""
    source = args.source
    window = args.window

    logger.info(f"Validating against {source}...")

    from datetime import timedelta

    cutoff_date = datetime.now() - timedelta(days=window)

    with get_connection() as conn:
        # Get CPPI transactions
        rows = conn.execute("""
            SELECT
                filing_id, bioguide_id, resolved_ticker, transaction_type,
                execution_date, amount_min, amount_max
            FROM transactions
            WHERE execution_date >= ?
              AND include_in_signal = 1
            ORDER BY execution_date DESC
        """, (cutoff_date.strftime("%Y-%m-%d"),)).fetchall()

        if not rows:
            logger.error("No CPPI transactions found. Run 'cppi parse' first.")
            return

        cppi_txns = [dict(row) for row in rows]
        logger.info(f"Found {len(cppi_txns)} CPPI transactions")

        if source == "quiver":
            # Lazy import
            from cppi.validation.quiver import fetch_quiver_transactions
            from cppi.validation.validator import (
                format_validation_report,
                validate_against_source,
            )

            external_txns = fetch_quiver_transactions(limit=1000)

            if not external_txns:
                logger.error(
                    "Failed to fetch Quiver data. Check your QUIVER_API_KEY "
                    "environment variable."
                )
                return

            logger.info(f"Fetched {len(external_txns)} Quiver transactions")

            report = validate_against_source(cppi_txns, external_txns, "quiver")
            print(format_validation_report(report))

            # Store in database
            conn.execute("""
                INSERT INTO validation_results (
                    source, validated_at, match_rate, total_compared, discrepancies
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                source,
                datetime.now().isoformat(),
                report.match_rate,
                report.matched_count,
                json.dumps(report.discrepancy_summary),
            ))

        else:
            logger.error(f"Unknown validation source: {source}")
            logger.info("Available sources: quiver")


def cmd_diagnose(args):
    """Run member diagnostics."""
    bioguide_id = args.bioguide_id

    with get_connection() as conn:
        # Get member info
        member = conn.execute(
            "SELECT * FROM members WHERE bioguide_id = ?",
            (bioguide_id,)
        ).fetchone()

        if not member:
            # Try to find by filing_id pattern
            logger.info(f"Member {bioguide_id} not found in members table, searching filings...")
            member = None

        # Get transactions for this member/filing
        txns = conn.execute("""
            SELECT t.*, f.filer_name, f.chamber
            FROM transactions t
            JOIN filings f ON t.filing_id = f.filing_id
            WHERE t.filing_id LIKE ? OR t.bioguide_id = ?
            ORDER BY t.execution_date DESC
        """, (f"%{bioguide_id}%", bioguide_id)).fetchall()

        if not txns:
            logger.error(f"No transactions found for {bioguide_id}")
            return

        # Print diagnostic report
        print(f"\n{'=' * 60}")
        print("MEMBER DIAGNOSTIC REPORT")
        print(f"{'=' * 60}")

        if member:
            print(f"\nMember: {member['name']}")
            print(f"Bioguide ID: {member['bioguide_id']}")
            print(f"Party: {member['party']}")
            print(f"State: {member['state']}")
            print(f"Chamber: {member['chamber']}")
            if member['committees']:
                committees = json.loads(member['committees'])
                print(f"Committees: {len(committees)}")
                for c in committees[:5]:
                    name = c.get('name', c.get('code', 'Unknown'))
                    print(f"  - {name}")
        else:
            filer = txns[0] if txns else None
            if filer:
                print(f"\nFiler: {filer['filer_name']}")
                print(f"Chamber: {filer['chamber']}")

        print(f"\n{'-' * 60}")
        print("TRANSACTION SUMMARY")
        print(f"{'-' * 60}")
        print(f"Total transactions: {len(txns)}")

        # Count by type
        buys = sum(1 for t in txns if t['transaction_type'] == 'purchase')
        sells = sum(1 for t in txns if t['transaction_type'] in ('sale', 'sale_partial'))
        print(f"Purchases: {buys}")
        print(f"Sales: {sells}")

        # Included vs excluded
        included = sum(1 for t in txns if t['include_in_signal'])
        print(f"Included in signal: {included}")
        print(f"Excluded: {len(txns) - included}")

        # Date range
        dates = [t['execution_date'] for t in txns if t['execution_date']]
        if dates:
            print(f"Date range: {min(dates)} to {max(dates)}")

        # Top tickers
        tickers = {}
        for t in txns:
            ticker = t['resolved_ticker'] or 'UNRESOLVED'
            tickers[ticker] = tickers.get(ticker, 0) + 1

        print(f"\n{'-' * 60}")
        print("TOP SECURITIES")
        print(f"{'-' * 60}")
        for ticker, count in sorted(tickers.items(), key=lambda x: -x[1])[:10]:
            print(f"  {ticker}: {count} transactions")

        # Estimate total volume
        total_buy = 0
        total_sell = 0
        for t in txns:
            if t['amount_min'] and t['amount_max']:
                mid = (t['amount_min'] + t['amount_max']) / 2
                if t['transaction_type'] == 'purchase':
                    total_buy += mid
                elif t['transaction_type'] in ('sale', 'sale_partial'):
                    total_sell += mid

        print(f"\n{'-' * 60}")
        print("ESTIMATED VOLUME")
        print(f"{'-' * 60}")
        print(f"Buy volume: ~${total_buy:,.0f}")
        print(f"Sell volume: ~${total_sell:,.0f}")
        print(f"Net: ~${total_buy - total_sell:+,.0f}")
        print()


def cmd_status(args):
    """Show database status and statistics."""
    from cppi.services.status_service import get_status

    status = get_status()
    print(f"\nCPPI Database Status (v{__version__})")
    print("=" * 50)
    print(f"  members: {status.members:,} records")
    print(f"  filings: {status.filings:,} records")
    print(f"  transactions: {status.transactions:,} records")
    print(f"  positioning_scores: {status.positioning_scores:,} records")

    if status.latest_score_scope:
        print(f"\nLatest Score ({status.latest_score_scope}, {status.latest_score_window_days}d):")
        print(f"  Breadth: {status.latest_score_breadth_pct:.1%}")
        print(f"  Confidence: {status.latest_score_confidence_tier}")
        print(f"  Computed: {status.latest_score_computed_at}")
    else:
        print("\nNo scores computed yet")

    print()


def cmd_backtest(args):
    """Run historical backtest of CPPI signals."""
    from cppi.backtest import (
        BacktestConfig,
        format_backtest_report,
        run_backtest,
        store_historical_scores,
    )
    from cppi.config import DB_PATH

    logger.info(f"Running backtest from {args.start} to {args.end}")

    config = BacktestConfig(
        start_date=args.start,
        end_date=args.end,
        window_days=args.window,
        forward_return_days=args.forward_days,
        rebalance_frequency_days=args.rebalance_days,
        benchmark_ticker=args.benchmark,
        scope=args.scope,
        use_cache=not args.no_cache,
    )

    try:
        result = run_backtest(DB_PATH, config)
    except ImportError as e:
        print(f"\nError: {e}")
        print("\nBacktesting requires yfinance. Install with:")
        print("  pip install yfinance")
        return

    # Format and output report
    report = format_backtest_report(result)

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w") as f:
            f.write(report)
        print(f"Backtest report written to {args.output}")

    if args.stdout or not args.output:
        print(report)

    # Optionally store historical scores
    if args.store_scores:
        count = store_historical_scores(
            DB_PATH,
            result.signal_points,
            config.scope,
            config.window_days,
        )
        print(f"Stored {count} historical scores to database")

    # Print summary metrics
    if result.correlation_net_vs_return is not None:
        print(f"\nKey metric: Net positioning correlation = {result.correlation_net_vs_return:+.3f}")


def main():
    """Main entry point for CLI."""
    setup_logging()

    parser = argparse.ArgumentParser(
        prog="cppi",
        description="Congressional Policy Positioning Index",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # init command
    init_parser = subparsers.add_parser("init", help="Initialize database")
    init_parser.set_defaults(func=cmd_init)

    # ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Ingest filings")
    ingest_parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back (default: 90)",
    )
    ingest_parser.add_argument(
        "--house-only",
        action="store_true",
        help="Only ingest House filings",
    )
    ingest_parser.add_argument(
        "--senate-only",
        action="store_true",
        help="Only ingest Senate filings",
    )
    ingest_parser.add_argument(
        "--bulk",
        action="store_true",
        help="Download ALL PTRs from House FD XML bulk files (1000+ documents)",
    )
    ingest_parser.set_defaults(func=cmd_ingest)

    # parse command
    parse_parser = subparsers.add_parser("parse", help="Parse downloaded filings")
    parse_parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-parsing of all filings, ignoring cached hashes",
    )
    parse_parser.set_defaults(func=cmd_parse)

    # score command
    score_parser = subparsers.add_parser("score", help="Compute positioning scores")
    score_parser.add_argument(
        "--window",
        type=int,
        default=90,
        help="Lookback window in days (default: 90)",
    )
    score_parser.set_defaults(func=cmd_score)

    # report command
    report_parser = subparsers.add_parser("report", help="Generate report")
    report_parser.add_argument(
        "--output",
        type=str,
        default="output/cppi_report.txt",
        help="Output file path (default: output/cppi_report.txt)",
    )
    report_parser.add_argument(
        "--window",
        type=int,
        default=90,
        help="Lookback window in days (default: 90)",
    )
    report_parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    report_parser.add_argument(
        "--stdout",
        action="store_true",
        help="Also print report to stdout",
    )
    report_parser.set_defaults(func=cmd_report)

    # status command
    status_parser = subparsers.add_parser("status", help="Show database status")
    status_parser.set_defaults(func=cmd_status)

    # enrich command
    enrich_parser = subparsers.add_parser("enrich", help="Enrich data from external sources")
    enrich_parser.add_argument(
        "--members",
        action="store_true",
        help="Enrich member data from Congress.gov API",
    )
    enrich_parser.add_argument(
        "--committees",
        action="store_true",
        help="Also fetch committee assignments (slower, requires many API calls)",
    )
    enrich_parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Ignore cached data and fetch fresh from API",
    )
    enrich_parser.set_defaults(func=cmd_enrich)

    # analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Run analysis tools")
    analyze_parser.add_argument(
        "analysis_type",
        choices=["sensitivity", "weights", "crossref"],
        help="Type of analysis to run",
    )
    analyze_parser.add_argument(
        "--window",
        type=int,
        default=90,
        help="Lookback window in days (default: 90)",
    )
    analyze_parser.add_argument(
        "--insider-db",
        type=str,
        help="Path to insider trading database (for crossref analysis)",
    )
    analyze_parser.add_argument(
        "--json",
        action="store_true",
        help="Also output JSON summary (for crossref analysis)",
    )
    analyze_parser.set_defaults(func=cmd_analyze)

    # validate command
    validate_parser = subparsers.add_parser("validate", help="Validate against external sources")
    validate_parser.add_argument(
        "--source",
        choices=["quiver"],
        required=True,
        help="External source to validate against",
    )
    validate_parser.add_argument(
        "--window",
        type=int,
        default=90,
        help="Lookback window in days (default: 90)",
    )
    validate_parser.set_defaults(func=cmd_validate)

    # diagnose command
    diagnose_parser = subparsers.add_parser("diagnose", help="Run diagnostics")
    diagnose_subparsers = diagnose_parser.add_subparsers(dest="diagnose_type")

    # diagnose member
    diagnose_member_parser = diagnose_subparsers.add_parser(
        "member",
        help="Diagnose a specific member",
    )
    diagnose_member_parser.add_argument(
        "bioguide_id",
        help="Bioguide ID or filing ID pattern to diagnose",
    )
    diagnose_member_parser.set_defaults(func=cmd_diagnose)

    # backtest command
    backtest_parser = subparsers.add_parser(
        "backtest",
        help="Run historical backtest of CPPI signals",
    )
    backtest_parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    backtest_parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    backtest_parser.add_argument(
        "--window",
        type=int,
        default=90,
        help="Signal lookback window in days (default: 90)",
    )
    backtest_parser.add_argument(
        "--forward-days",
        type=int,
        default=30,
        help="Forward return period in days (default: 30)",
    )
    backtest_parser.add_argument(
        "--rebalance-days",
        type=int,
        default=7,
        help="Days between signal observations (default: 7)",
    )
    backtest_parser.add_argument(
        "--benchmark",
        type=str,
        default="SPY",
        help="Benchmark ticker (default: SPY)",
    )
    backtest_parser.add_argument(
        "--scope",
        choices=["house", "senate", "all"],
        default="all",
        help="Scope of transactions (default: all)",
    )
    backtest_parser.add_argument(
        "--output",
        type=str,
        help="Output file path for report",
    )
    backtest_parser.add_argument(
        "--stdout",
        action="store_true",
        help="Also print report to stdout",
    )
    backtest_parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Don't use price cache",
    )
    backtest_parser.add_argument(
        "--store-scores",
        action="store_true",
        help="Store historical scores to database",
    )
    backtest_parser.set_defaults(func=cmd_backtest)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    try:
        # Ensure database is initialized before any command
        init_db()
        args.func(args)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
