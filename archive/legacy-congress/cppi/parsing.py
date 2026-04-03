"""PDF parsing module for House financial disclosure PDFs.

Extracts transaction data from Periodic Transaction Reports (PTRs)
downloaded from disclosures-clerk.house.gov.

Transaction line format:
    [Owner] Asset Name (TICKER) [TYPE] TxnType MM/DD/YYYY MM/DD/YYYY $X - $Y

Owner codes: SP (Spouse), DC (Dependent Child), JT (Joint), blank (Self)
Transaction types: P (Purchase), S (Sale), S (partial), E (Exchange)
Asset types: ST (Stock), OP (Option), GS (Gov Securities), MF (Mutual Fund),
             EF (ETF), OT (Other), BD (Corporate Bond), CS (Crypto)
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import pdfplumber

from cppi.ocr import is_paper_filing, is_tesseract_available, ocr_pdf

logger = logging.getLogger(__name__)


@dataclass
class ParsedTransaction:
    """Represents a parsed transaction from a House PTR PDF."""

    owner: str  # 'self', 'spouse', 'dependent', 'joint'
    asset_name: str
    ticker: Optional[str]
    asset_type: Optional[str]  # ST, OP, GS, MF, EF, etc.
    transaction_type: str  # 'purchase', 'sale', 'sale_partial', 'exchange'
    transaction_date: Optional[datetime]
    notification_date: Optional[datetime]
    amount_min: Optional[int]
    amount_max: Optional[int]
    amount_text: str
    cap_gains_over_200: Optional[bool]
    description: Optional[str] = None
    subowner: Optional[str] = None  # For trusts, etc.
    page_number: int = 1
    raw_line: str = ""


@dataclass
class ParsedFiling:
    """Represents a complete parsed PTR filing."""

    filing_id: str
    filer_name: str
    filer_status: str  # Member, Candidate, etc.
    state_district: Optional[str]
    transactions: list[ParsedTransaction] = field(default_factory=list)
    pdf_path: str = ""
    page_count: int = 0
    parse_errors: list[str] = field(default_factory=list)


# Owner code mapping
OWNER_CODES = {
    "SP": "spouse",
    "DC": "dependent",
    "JT": "joint",
    "": "self",
}

# Transaction type mapping
TRANSACTION_TYPES = {
    "P": "purchase",
    "S": "sale",
    "S (partial)": "sale_partial",
    "E": "exchange",
}

# Amount range parsing
AMOUNT_RANGES = {
    "$1,001 - $15,000": (1_001, 15_000),
    "$15,001 - $50,000": (15_001, 50_000),
    "$50,001 - $100,000": (50_001, 100_000),
    "$100,001 - $250,000": (100_001, 250_000),
    "$250,001 - $500,000": (250_001, 500_000),
    "$500,001 - $1,000,000": (500_001, 1_000_000),
    "$1,000,001 - $5,000,000": (1_000_001, 5_000_000),
    "$5,000,001 - $25,000,000": (5_000_001, 25_000_000),
    "$25,000,001 - $50,000,000": (25_000_001, 50_000_000),
    "Over $50,000,000": (50_000_001, 100_000_000),
}

# Regex patterns
FILING_ID_PATTERN = re.compile(r"Filing ID #?(\d+)")
FILER_NAME_PATTERN = re.compile(r"Name:\s*(.+?)(?:\n|$)")
FILER_STATUS_PATTERN = re.compile(r"Status:\s*(.+?)(?:\n|$)")
STATE_DISTRICT_PATTERN = re.compile(r"State/District:\s*(.+?)(?:\n|$)")

# Transaction line pattern - matches owner code at start
OWNER_PATTERN = re.compile(r"^(SP|DC|JT)?\s*")

# Ticker pattern - (TICKER) or (TICKER.X)
TICKER_PATTERN = re.compile(r"\(([A-Z]{1,5}(?:\.[A-Z])?)\)")

# Asset type pattern - [XX]
ASSET_TYPE_PATTERN = re.compile(r"\[([A-Z]{2})\]")

# Date pattern - MM/DD/YYYY
DATE_PATTERN = re.compile(r"(\d{2}/\d{2}/\d{4})")

# Amount pattern - various forms (ranges and exact amounts)
AMOUNT_PATTERN = re.compile(
    r"(\$[\d,]+\s*-\s*\$[\d,]+|Over \$[\d,]+)"
)

# Exact amount pattern - for amounts under $1,000 reported as exact values
EXACT_AMOUNT_PATTERN = re.compile(r"\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*$")

# Split amount patterns - for when amount range is split across wrapped lines
AMOUNT_LOWER_PATTERN = re.compile(r"\$(\d{1,3}(?:,\d{3})*)\s*-\s*$")  # "$250,001 -" at end
AMOUNT_UPPER_PATTERN = re.compile(r"^\s*\$(\d{1,3}(?:,\d{3})*)(?:\s|$)")  # "$500,000" at start

# Transaction type pattern - P, S, S (partial), E at end of asset section
TXN_TYPE_PATTERN = re.compile(r"\b(P|S \(partial\)|S|E)\b")

# Paper filing patterns (OCR'd scanned forms)
# Date pattern for paper filings - MM/DD/YY format
PAPER_DATE_PATTERN = re.compile(r"(\d{1,2}/\d{1,2}/\d{2})\b")

# Paper filing transaction line - starts with owner code or asset name
PAPER_TXN_PATTERN = re.compile(
    r"^(?P<owner>JT|SP|DC|st)?\s*\|?\s*"  # Optional owner
    r"(?P<asset>.+?)"  # Asset name
    r"(?:\s*[-–]\s*(?P<ticker>[A-Z]{1,5}))?"  # Optional ticker after dash
    r"\s*"
    r"(?P<txn_type>[PSEXxpse]|\d)?\s*\|?\s*"  # Transaction type or checkbox
    r"(?P<date1>\d{1,2}/\d{1,2}/\d{2,4})?\s*\|?\s*"  # Transaction date
    r"(?P<date2>\d{1,2}/\d{1,2}/\d{2,4})?"  # Notification date
)


class HousePDFParser:
    """Parser for House PTR PDF documents."""

    def __init__(self):
        """Initialize the parser."""
        self.errors: list[str] = []

    def parse(self, pdf_path: Path) -> ParsedFiling:
        """Parse a House PTR PDF and extract transactions.

        Args:
            pdf_path: Path to the PDF file

        Returns:
            ParsedFiling object with extracted data
        """
        self.errors = []
        filing = ParsedFiling(
            filing_id="",
            filer_name="",
            filer_status="",
            state_district=None,
            pdf_path=str(pdf_path),
        )

        try:
            with pdfplumber.open(pdf_path) as pdf:
                filing.page_count = len(pdf.pages)

                # Combine all page text
                full_text = ""
                page_texts = []
                needs_ocr = False

                for page_num, page in enumerate(pdf.pages):
                    text = page.extract_text() or ""
                    if not text.strip() and page.images:
                        # Page has images but no text - likely scanned
                        needs_ocr = True
                        logger.debug(
                            f"Page {page_num + 1} of {pdf_path} has no text but {len(page.images)} images"
                        )
                    page_texts.append(text)
                    full_text += text + "\n"

                # If we need OCR, try to get text via OCR
                if needs_ocr:
                    full_text, page_texts = self._try_ocr_fallback(pdf_path, filing)

                # Extract filing metadata
                self._extract_metadata(full_text, filing)

                # Extract transactions from each page
                for page_num, text in enumerate(page_texts, start=1):
                    transactions = self._extract_transactions(text, page_num)
                    filing.transactions.extend(transactions)

        except Exception as e:
            error_msg = f"Failed to parse {pdf_path}: {e}"
            logger.error(error_msg)
            self.errors.append(error_msg)

        filing.parse_errors = self.errors.copy()
        return filing

    def _try_ocr_fallback(
        self, pdf_path: Path, filing: ParsedFiling
    ) -> tuple[str, list[str]]:
        """Attempt OCR on a PDF when text extraction fails.

        Args:
            pdf_path: Path to the PDF file
            filing: Filing object to record warnings

        Returns:
            Tuple of (full_text, page_texts_list)
        """
        if not is_tesseract_available():
            warning = (
                f"PDF {pdf_path.name} appears to be scanned but Tesseract OCR "
                "is not installed. Run: brew install tesseract poppler"
            )
            logger.warning(warning)
            self.errors.append(warning)
            return "", [""] * filing.page_count

        logger.info(f"Running OCR on scanned PDF: {pdf_path.name}")

        try:
            ocr_result = ocr_pdf(pdf_path)

            if not ocr_result.is_successful:
                warning = (
                    f"OCR failed for {pdf_path.name}: "
                    f"confidence={ocr_result.confidence:.2f}, "
                    f"warnings={ocr_result.warnings}"
                )
                logger.warning(warning)
                self.errors.append(warning)
                return "", [""] * filing.page_count

            # Validate OCR output
            if not self._validate_ocr_output(ocr_result.text):
                warning = (
                    f"OCR output for {pdf_path.name} failed validation "
                    "(may be garbled or low quality)"
                )
                logger.warning(warning)
                self.errors.append(warning)
                return "", [""] * filing.page_count

            logger.info(
                f"OCR successful for {pdf_path.name}: "
                f"{len(ocr_result.text)} chars, "
                f"confidence={ocr_result.confidence:.2f}"
            )

            # Split OCR result into pages
            page_texts = ocr_result.text.split("\n\n--- PAGE BREAK ---\n\n")

            # Pad page_texts if OCR produced fewer pages than expected
            while len(page_texts) < filing.page_count:
                page_texts.append("")

            return ocr_result.text, page_texts

        except Exception as e:
            warning = f"OCR error for {pdf_path.name}: {e}"
            logger.warning(warning)
            self.errors.append(warning)
            return "", [""] * filing.page_count

    def _validate_ocr_output(self, text: str) -> bool:
        """Validate that OCR output appears to be valid financial disclosure text.

        Args:
            text: OCR'd text to validate

        Returns:
            True if text appears valid, False if it looks like garbage
        """
        if not text or len(text) < 50:
            return False

        # Check for excessive special characters (OCR garbage indicator)
        allowed_special = set(" .,()-$%:/\n\t'\"")
        special_chars = sum(
            1 for c in text if not c.isalnum() and c not in allowed_special
        )
        if len(text) > 0:
            special_ratio = special_chars / len(text)
            if special_ratio > 0.3:  # >30% unusual special chars = garbage
                logger.debug(f"OCR validation failed: special_ratio={special_ratio:.2f}")
                return False

        # Check for expected patterns in financial disclosures
        has_date = bool(re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", text))
        has_amount = bool(re.search(r"\$[\d,]+", text))

        if not (has_date or has_amount):
            logger.debug("OCR validation failed: no dates or amounts found")
            return False

        return True

    def _extract_metadata(self, text: str, filing: ParsedFiling) -> None:
        """Extract filing metadata from text.

        Args:
            text: Full PDF text
            filing: Filing object to populate
        """
        # Filing ID
        match = FILING_ID_PATTERN.search(text)
        if match:
            filing.filing_id = match.group(1)

        # Filer name
        match = FILER_NAME_PATTERN.search(text)
        if match:
            filing.filer_name = match.group(1).strip()

        # Status
        match = FILER_STATUS_PATTERN.search(text)
        if match:
            filing.filer_status = match.group(1).strip()

        # State/District
        match = STATE_DISTRICT_PATTERN.search(text)
        if match:
            filing.state_district = match.group(1).strip()

    def _extract_transactions(
        self, page_text: str, page_number: int
    ) -> list[ParsedTransaction]:
        """Extract transactions from a single page.

        Args:
            page_text: Text content of the page
            page_number: Page number (1-indexed)

        Returns:
            List of parsed transactions
        """
        transactions = []
        lines = page_text.split("\n")

        # Find the transaction table header
        in_transactions = False
        current_entry_lines: list[str] = []
        description_lines: list[str] = []
        subowner_lines: list[str] = []

        for i, line in enumerate(lines):
            line_stripped = line.strip()

            # Skip empty lines
            if not line_stripped:
                continue

            # Detect table header
            if "Owner" in line and "Asset" in line and "Transaction" in line:
                in_transactions = True
                continue

            # Skip secondary header line
            if in_transactions and ("Type" in line and "Date" in line and "Gains" in line):
                continue

            # Stop at certification section
            if "CERTIFY" in line or "certify" in line:
                # Process any pending entry
                if current_entry_lines:
                    txn = self._parse_entry(
                        current_entry_lines, description_lines, subowner_lines, page_number
                    )
                    if txn:
                        transactions.append(txn)
                break

            # Stop at asset codes reference
            if "asset type abbreviations" in line.lower():
                if current_entry_lines:
                    txn = self._parse_entry(
                        current_entry_lines, description_lines, subowner_lines, page_number
                    )
                    if txn:
                        transactions.append(txn)
                break

            if not in_transactions:
                continue

            # Check for metadata lines first
            is_filing_status = line_stripped.startswith("F") and "S" in line_stripped[:20] and ":" in line_stripped
            is_subowner = line_stripped.startswith("S") and "O" in line_stripped[:20] and ":" in line_stripped
            is_description = line_stripped.startswith("D") and ":" in line_stripped[:20]
            is_location = line_stripped.startswith("L") and ":" in line_stripped[:20]

            # Skip metadata lines early
            if is_filing_status or is_location:
                continue

            # Check if this is a new transaction entry
            # New entries have these characteristics:
            # 1. Start with owner code (SP, DC, JT) - definitely new
            # 2. Contain TWO dates (MM/DD/YYYY patterns) - transaction + notification date
            # 3. Contain amount range pattern - has full transaction data

            has_owner_prefix = line_stripped.startswith(("SP ", "DC ", "JT "))
            dates_in_line = DATE_PATTERN.findall(line_stripped)
            has_amount = bool(AMOUNT_PATTERN.search(line_stripped))

            # A line is a new entry if:
            # - Starts with owner code, OR
            # - Has 2 dates (transaction and notification), OR
            # - Has 1 date AND an amount (complete entry on one line for self)
            is_new_entry = (
                has_owner_prefix
                or len(dates_in_line) >= 2
                or (len(dates_in_line) == 1 and has_amount)
            )

            if is_subowner:
                # Extract subowner info
                subowner_match = re.search(r":\s*(.+)", line_stripped)
                if subowner_match:
                    subowner_lines.append(subowner_match.group(1))
                continue

            if is_description:
                # Extract description
                desc_match = re.search(r":\s*(.+)", line_stripped)
                if desc_match:
                    description_lines.append(desc_match.group(1))
                continue

            # Check if this looks like an amount continuation
            # Amount continuations start with $ or contain just the amount range
            is_amount_continuation = (
                line_stripped.startswith("$")
                or re.match(r"^\$[\d,]+$", line_stripped)
            )

            if is_new_entry and current_entry_lines:
                # Process the previous entry before starting new one
                txn = self._parse_entry(
                    current_entry_lines, description_lines, subowner_lines, page_number
                )
                if txn:
                    transactions.append(txn)
                current_entry_lines = [line_stripped]
                description_lines = []
                subowner_lines = []
            elif is_amount_continuation and current_entry_lines:
                # Append amount to previous line
                current_entry_lines[-1] += " " + line_stripped
            elif current_entry_lines:
                # Continuation of current entry
                current_entry_lines.append(line_stripped)
            elif is_new_entry:
                # Start of first entry
                current_entry_lines = [line_stripped]

        return transactions

    def _parse_entry(
        self,
        entry_lines: list[str],
        description_lines: list[str],
        subowner_lines: list[str],
        page_number: int,
    ) -> Optional[ParsedTransaction]:
        """Parse a transaction entry from collected lines.

        Args:
            entry_lines: Lines comprising the transaction entry
            description_lines: Description metadata lines
            subowner_lines: Subowner metadata lines
            page_number: Page number

        Returns:
            ParsedTransaction or None if parsing fails
        """
        if not entry_lines:
            return None

        # Join lines for parsing
        full_text = " ".join(entry_lines)
        raw_line = full_text

        try:
            # Extract owner
            owner_match = OWNER_PATTERN.match(full_text)
            owner_code = owner_match.group(1) if owner_match and owner_match.group(1) else ""
            owner = OWNER_CODES.get(owner_code, "self")

            # Remove owner code from text
            if owner_code:
                full_text = full_text[len(owner_code):].strip()

            # Extract ticker
            ticker_match = TICKER_PATTERN.search(full_text)
            ticker = ticker_match.group(1) if ticker_match else None

            # Extract asset type
            asset_type_match = ASSET_TYPE_PATTERN.search(full_text)
            asset_type = asset_type_match.group(1) if asset_type_match else None

            # Extract dates
            dates = DATE_PATTERN.findall(full_text)
            transaction_date = None
            notification_date = None
            if len(dates) >= 1:
                try:
                    transaction_date = datetime.strptime(dates[0], "%m/%d/%Y")
                except ValueError:
                    pass
            if len(dates) >= 2:
                try:
                    notification_date = datetime.strptime(dates[1], "%m/%d/%Y")
                except ValueError:
                    pass

            # Extract amount
            amount_min: Optional[int] = None
            amount_max: Optional[int] = None

            amount_match = AMOUNT_PATTERN.search(full_text)
            amount_text = amount_match.group(1) if amount_match else ""

            # Handle split amounts where text appears between lower and upper bounds
            # e.g., "$250,001 - Stock (GOOGL) [OP] $500,000"
            if not amount_text:
                # Look for split pattern across the entry lines
                for i, line in enumerate(entry_lines):
                    # Check for lower bound ending with "-"
                    lower_match = AMOUNT_LOWER_PATTERN.search(line)
                    if lower_match:
                        amount_min = int(lower_match.group(1).replace(",", ""))
                        # Look for upper bound in remaining lines
                        for remaining_line in entry_lines[i + 1:]:
                            upper_matches = re.findall(r"\$(\d{1,3}(?:,\d{3})*)", remaining_line)
                            if upper_matches:
                                amount_max = int(upper_matches[-1].replace(",", ""))
                                amount_text = f"${amount_min:,} - ${amount_max:,}"
                                break
                        if amount_max is not None:
                            break

                    # Check for both amounts on same line with text between
                    # e.g., "$250,001 - [ST] $500,000" (all on one line)
                    all_amounts = re.findall(r"\$([\d,]+)", line)
                    if len(all_amounts) >= 2 and amount_min is None:
                        # First and last amounts are the range bounds
                        amount_min = int(all_amounts[0].replace(",", ""))
                        amount_max = int(all_amounts[-1].replace(",", ""))
                        if amount_max > amount_min:  # Sanity check
                            amount_text = f"${amount_min:,} - ${amount_max:,}"
                            break
                        else:
                            amount_min, amount_max = None, None

            # Handle exact amounts (small amounts reported without range)
            if not amount_text:
                for line in entry_lines:
                    exact_match = EXACT_AMOUNT_PATTERN.search(line)
                    if exact_match:
                        # Convert cents if present (e.g., $360.00 -> 360)
                        if "." in exact_match.group(1):
                            exact_val = int(float(exact_match.group(1).replace(",", "")))
                        else:
                            exact_val = int(exact_match.group(1).replace(",", ""))
                        amount_min, amount_max = exact_val, exact_val
                        amount_text = f"${exact_val:,} (exact)"
                        break

            # Normalize and parse amount if not already extracted
            if amount_min is None and amount_text:
                amount_text_normalized = amount_text.replace(" ", "").replace("\n", "")
                amount_min, amount_max = self._parse_amount(amount_text_normalized)

            # Extract transaction type
            # Look for P, S, S (partial), E between asset type and first date
            txn_type_str = "purchase"  # default

            # Find position of first date
            first_date_match = DATE_PATTERN.search(full_text)
            if first_date_match:
                pre_date_text = full_text[:first_date_match.start()]
                # Look for transaction type code
                if " S (partial)" in pre_date_text or "S (partial)" in pre_date_text:
                    txn_type_str = "sale_partial"
                elif re.search(r"\bS\b", pre_date_text):
                    txn_type_str = "sale"
                elif re.search(r"\bE\b", pre_date_text):
                    txn_type_str = "exchange"
                elif re.search(r"\bP\b", pre_date_text):
                    txn_type_str = "purchase"

            # Extract asset name
            # Asset name is everything from start to ticker/type/date, excluding owner
            asset_name = self._extract_asset_name(full_text, ticker, asset_type, dates)

            # Combine descriptions
            description = " ".join(description_lines) if description_lines else None
            subowner = " | ".join(subowner_lines) if subowner_lines else None

            return ParsedTransaction(
                owner=owner,
                asset_name=asset_name,
                ticker=ticker,
                asset_type=asset_type,
                transaction_type=txn_type_str,
                transaction_date=transaction_date,
                notification_date=notification_date,
                amount_min=amount_min,
                amount_max=amount_max,
                amount_text=amount_text,
                cap_gains_over_200=None,  # TODO: parse this
                description=description,
                subowner=subowner,
                page_number=page_number,
                raw_line=raw_line,
            )

        except Exception as e:
            error_msg = f"Failed to parse entry on page {page_number}: {e}"
            logger.warning(error_msg)
            self.errors.append(error_msg)
            return None

    def _parse_amount(self, amount_text: str) -> tuple[Optional[int], Optional[int]]:
        """Parse amount range text into min/max values.

        Args:
            amount_text: Amount text like "$1,001 - $15,000" or "Over $50,000,000"

        Returns:
            Tuple of (min, max) or (None, None) if unparseable
        """
        if not amount_text:
            return None, None

        # Normalize
        normalized = amount_text.replace(" ", "").replace("\n", "")

        # Try exact match first
        for range_text, (min_val, max_val) in AMOUNT_RANGES.items():
            range_normalized = range_text.replace(" ", "")
            if normalized == range_normalized:
                return min_val, max_val

        # Try parsing manually
        if normalized.lower().startswith("over"):
            # "Over $50,000,000"
            match = re.search(r"\$([\d,]+)", normalized)
            if match:
                min_val = int(match.group(1).replace(",", "")) + 1
                return min_val, min_val * 2  # Estimate max as 2x

        # "$X-$Y" pattern
        match = re.match(r"\$([\d,]+)-\$([\d,]+)", normalized)
        if match:
            min_val = int(match.group(1).replace(",", ""))
            max_val = int(match.group(2).replace(",", ""))
            return min_val, max_val

        # Couldn't parse
        logger.debug(f"Unparseable amount: {amount_text}")
        return None, None

    def _extract_asset_name(
        self,
        text: str,
        ticker: Optional[str],
        asset_type: Optional[str],
        dates: list[str],
    ) -> str:
        """Extract the asset name from transaction text.

        Args:
            text: Full transaction text (without owner code)
            ticker: Extracted ticker
            asset_type: Extracted asset type
            dates: Extracted dates

        Returns:
            Asset name string
        """
        # Find the end of the asset name
        # It ends before the transaction type code (P, S, E) before the date

        # Find position of first date
        first_date_pos = len(text)
        if dates:
            for date in dates:
                pos = text.find(date)
                if pos != -1 and pos < first_date_pos:
                    first_date_pos = pos

        # Asset name is before the transaction type code and date
        asset_text = text[:first_date_pos].strip()

        # Remove trailing transaction type codes
        asset_text = re.sub(r"\s+[PSE]\s*$", "", asset_text)
        asset_text = re.sub(r"\s+S \(partial\)\s*$", "", asset_text)

        # Remove asset type code at end
        if asset_type:
            asset_text = re.sub(rf"\s*\[{asset_type}\]\s*$", "", asset_text)

        # Clean up ticker from name if it appears at the end
        if ticker:
            asset_text = re.sub(rf"\s*\({ticker}\)\s*$", "", asset_text)

        return asset_text.strip()


class PaperFilingParser:
    """Parser for OCR'd paper House PTR filings.

    Paper filings have a different format than electronic ones:
    - Handwritten or typed into physical forms
    - Dates in MM/DD/YY format
    - Amount ranges indicated by checkboxes (often garbled by OCR)
    - Transaction type indicated by position or letter codes
    """

    # Paper filing amount column mapping (columns A-J on the form)
    AMOUNT_COLUMNS = {
        "A": (1_001, 15_000),
        "B": (15_001, 50_000),
        "C": (50_001, 100_000),
        "D": (100_001, 250_000),
        "E": (250_001, 500_000),
        "F": (500_001, 1_000_000),
        "G": (1_000_001, 5_000_000),
        "H": (5_000_001, 25_000_000),
        "I": (25_000_001, 50_000_000),
        "J": (50_000_001, 100_000_000),
    }

    def __init__(self):
        """Initialize the parser."""
        self.errors: list[str] = []

    def parse_ocr_text(
        self, ocr_text: str, pdf_path: Path, page_count: int
    ) -> ParsedFiling:
        """Parse OCR'd text from a paper House PTR.

        Args:
            ocr_text: Full OCR text from the PDF
            pdf_path: Path to the source PDF
            page_count: Number of pages in PDF

        Returns:
            ParsedFiling object with extracted data
        """
        self.errors = []
        filing = ParsedFiling(
            filing_id="",
            filer_name="",
            filer_status="",
            state_district=None,
            pdf_path=str(pdf_path),
            page_count=page_count,
        )

        # Extract filer name from paper filing
        # Usually appears as "NAME: Name Here" or similar
        name_match = re.search(r"NAME[;:]?\s*([A-Za-z\s\.]+?)(?:\s+OFFICE|\s+Member|\n)", ocr_text)
        if name_match:
            filing.filer_name = name_match.group(1).strip()

        # Extract filing ID if present
        filing_id_match = re.search(r"(?:Filing\s*ID|Doc\s*ID)[#:\s]*(\d+)", ocr_text, re.I)
        if filing_id_match:
            filing.filing_id = filing_id_match.group(1)
        else:
            # Use filename as fallback
            filing.filing_id = pdf_path.stem

        # Set status for paper filings
        if "Member" in ocr_text:
            filing.filer_status = "Member"

        # Parse transactions from OCR text
        transactions = self._extract_paper_transactions(ocr_text)
        filing.transactions = transactions

        filing.parse_errors = self.errors.copy()
        return filing

    def _extract_paper_transactions(self, text: str) -> list[ParsedTransaction]:
        """Extract transactions from paper filing OCR text.

        Paper filings have transactions in a table format with:
        - Asset name (sometimes with ticker after dash)
        - Transaction type (P/S/E or checkbox marker)
        - Transaction date (MM/DD/YY)
        - Notification date (MM/DD/YY)
        - Amount indicated by column checkbox (A-J)
        """
        transactions = []
        lines = text.split("\n")

        for i, line in enumerate(lines):
            line = line.strip()
            if not line or len(line) < 10:
                continue

            # Skip header/instruction lines
            if any(skip in line.lower() for skip in [
                "example", "provide full", "mega corp", "ticker symbol",
                "type of", "date of", "amount of", "transaction",
                "action", "certified", "penalty"
            ]):
                continue

            # Look for transaction lines
            # Pattern: [Owner] | Asset Name [- TICKER] | [TxnType] | Date1 | Date2 | [Amount markers]
            txn = self._parse_paper_transaction_line(line, i + 1)
            if txn:
                transactions.append(txn)

        return transactions

    def _parse_paper_transaction_line(
        self, line: str, line_num: int
    ) -> Optional[ParsedTransaction]:
        """Parse a single transaction line from paper filing OCR.

        Args:
            line: Line text
            line_num: Line number for error reporting

        Returns:
            ParsedTransaction or None if not a valid transaction
        """
        # Clean up common OCR artifacts
        line = line.replace("|", " | ")
        line = re.sub(r"\s+", " ", line)

        # Extract dates (MM/DD/YY or MM/DD/YYYY format)
        date_pattern = r"(\d{1,2}/\d{1,2}/\d{2,4})"
        dates = re.findall(date_pattern, line)

        # Need at least one date to be a transaction
        if not dates:
            return None

        # Extract owner code (at start of line)
        owner = "self"
        owner_match = re.match(r"^(JT|SP|DC|st)\s*\|?\s*", line, re.I)
        if owner_match:
            owner_code = owner_match.group(1).upper()
            owner = OWNER_CODES.get(owner_code, "self")
            line = line[owner_match.end():]

        # Try to extract ticker (appears after dash in asset name)
        ticker = None
        ticker_match = re.search(r"[-–]\s*([A-Z]{1,5})\b", line)
        if ticker_match:
            ticker = ticker_match.group(1)

        # Extract asset name (everything before dates)
        first_date_pos = line.find(dates[0])
        if first_date_pos > 0:
            asset_section = line[:first_date_pos]
            # Clean up asset name
            asset_name = re.sub(r"\s*[-–]\s*[A-Z]{1,5}\s*$", "", asset_section)
            asset_name = re.sub(r"\s*\|+\s*$", "", asset_name)
            asset_name = re.sub(r"^\s*\|+\s*", "", asset_name)
            asset_name = re.sub(r"\s*[PSE]\s*$", "", asset_name)  # Remove trailing P/S/E
            asset_name = asset_name.strip()
        else:
            # Dates at start - skip
            return None

        # Skip if asset name is too short or looks like garbage
        if len(asset_name) < 3:
            return None
        if re.match(r"^[\d\s\|]+$", asset_name):
            return None

        # Determine transaction type
        # Look for P/S/E before the dates, or infer from checkbox markers
        txn_type = "purchase"  # default
        pre_date = line[:first_date_pos]
        if re.search(r"\bS\b", pre_date):
            txn_type = "sale"
        elif re.search(r"\bE\b", pre_date):
            txn_type = "exchange"
        elif re.search(r"\bP\b", pre_date):
            txn_type = "purchase"

        # Parse dates
        transaction_date = None
        notification_date = None
        for j, date_str in enumerate(dates[:2]):
            try:
                # Try MM/DD/YYYY first
                if len(date_str.split("/")[-1]) == 4:
                    parsed = datetime.strptime(date_str, "%m/%d/%Y")
                else:
                    # MM/DD/YY - assume 2000s
                    parsed = datetime.strptime(date_str, "%m/%d/%y")
                if j == 0:
                    transaction_date = parsed
                else:
                    notification_date = parsed
            except ValueError:
                pass

        # Try to extract amount from column markers
        # Paper filings have columns A-J for amounts
        amount_min, amount_max, amount_text = self._extract_paper_amount(line, dates)

        return ParsedTransaction(
            owner=owner,
            asset_name=asset_name,
            ticker=ticker,
            asset_type=None,  # Paper filings don't have [XX] codes reliably
            transaction_type=txn_type,
            transaction_date=transaction_date,
            notification_date=notification_date,
            amount_min=amount_min,
            amount_max=amount_max,
            amount_text=amount_text,
            cap_gains_over_200=None,
            description=None,
            subowner=None,
            page_number=1,  # Paper forms usually 1 page
            raw_line=line,
        )

    def _extract_paper_amount(
        self, line: str, dates: list[str]
    ) -> tuple[Optional[int], Optional[int], str]:
        """Extract amount from paper filing line.

        Paper filings use column checkboxes (A-J) for amounts.
        OCR often garbles these, so we look for patterns.
        """
        # Find text after the last date
        after_dates = line
        if dates:
            last_date_pos = line.rfind(dates[-1])
            if last_date_pos >= 0:
                after_dates = line[last_date_pos + len(dates[-1]):]

        # Look for explicit amount range text (sometimes OCR gets it)
        amount_match = re.search(r"\$[\d,]+\s*[-–]\s*\$[\d,]+", after_dates)
        if amount_match:
            text = amount_match.group(0)
            # Parse the range
            nums = re.findall(r"\$([\d,]+)", text)
            if len(nums) >= 2:
                try:
                    min_val = int(nums[0].replace(",", ""))
                    max_val = int(nums[1].replace(",", ""))
                    return min_val, max_val, text
                except ValueError:
                    pass

        # Look for column letter markers (x followed by or near A-J)
        # These are often OCR'd as "x", "X", or just letters
        column_match = re.search(r"[xX]\s*([A-J])|([A-J])\s*[xX]", after_dates)
        if column_match:
            col = (column_match.group(1) or column_match.group(2)).upper()
            if col in self.AMOUNT_COLUMNS:
                min_val, max_val = self.AMOUNT_COLUMNS[col]
                return min_val, max_val, f"Column {col}"

        # Look for just an X marker followed by context suggesting amount
        if "x" in after_dates.lower():
            # Default to moderate amount range if X found but column unclear
            return 1_001, 15_000, "$1,001 - $15,000 (inferred)"

        # No amount found - common for OCR'd forms
        return None, None, ""


def parse_house_pdf(pdf_path: Path) -> ParsedFiling:
    """Convenience function to parse a House PTR PDF.

    Automatically detects paper vs electronic filings and uses
    the appropriate parser.

    Args:
        pdf_path: Path to the PDF file

    Returns:
        ParsedFiling object
    """
    # Check if this is a paper filing (822xxxxx IDs)
    if is_paper_filing(pdf_path.stem):
        logger.info(f"Detected paper filing: {pdf_path.name}")
        # Use electronic parser first - it will OCR if needed
        # The OCR result will be used by paper parser
        parser = HousePDFParser()
        filing = parser.parse(pdf_path)

        # If electronic parser got 0 transactions but we have OCR text,
        # try the paper filing parser
        if len(filing.transactions) == 0:
            # Re-run OCR to get the text for paper parsing
            if is_tesseract_available():
                ocr_result = ocr_pdf(pdf_path)
                if ocr_result.is_successful:
                    paper_parser = PaperFilingParser()
                    paper_filing = paper_parser.parse_ocr_text(
                        ocr_result.text, pdf_path, ocr_result.page_count
                    )
                    # Use paper parser result if it found more transactions
                    if len(paper_filing.transactions) > 0:
                        logger.info(
                            f"Paper parser found {len(paper_filing.transactions)} "
                            f"transactions in {pdf_path.name}"
                        )
                        return paper_filing

        return filing

    # Standard electronic filing
    parser = HousePDFParser()
    return parser.parse(pdf_path)
