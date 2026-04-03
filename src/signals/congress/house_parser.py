from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import pdfplumber

from signals.congress.constants import AMOUNT_RANGES
from signals.congress.ocr import is_tesseract_available, ocr_pdf


@dataclass
class ParsedTransaction:
    owner: str
    asset_name: str
    ticker: Optional[str]
    asset_type: Optional[str]
    transaction_type: str
    transaction_date: Optional[datetime]
    notification_date: Optional[datetime]
    amount_min: Optional[int]
    amount_max: Optional[int]
    amount_text: str
    cap_gains_over_200: Optional[bool]
    description: Optional[str] = None
    subowner: Optional[str] = None
    page_number: int = 1
    raw_line: str = ""


@dataclass
class ParsedFiling:
    filing_id: str
    filer_name: str
    filer_status: str
    state_district: Optional[str]
    transactions: list[ParsedTransaction] = field(default_factory=list)
    pdf_path: str = ""
    page_count: int = 0
    parse_errors: list[str] = field(default_factory=list)


OWNER_CODES = {"SP": "spouse", "DC": "dependent", "JT": "joint", "": "self"}
FILING_ID_PATTERN = re.compile(r"Filing ID #?(\d+)")
FILER_NAME_PATTERN = re.compile(r"Name:\s*(.+?)(?:\n|$)")
FILER_STATUS_PATTERN = re.compile(r"Status:\s*(.+?)(?:\n|$)")
STATE_DISTRICT_PATTERN = re.compile(r"State/District:\s*(.+?)(?:\n|$)")
OWNER_PATTERN = re.compile(r"^(SP|DC|JT)?\s*")
TICKER_PATTERN = re.compile(r"\(([A-Z]{1,5}(?:\.[A-Z])?)\)")
ASSET_TYPE_PATTERN = re.compile(r"\[([A-Z]{2})\]")
DATE_PATTERN = re.compile(r"(\d{2}/\d{2}/\d{4})")
AMOUNT_PATTERN = re.compile(r"(\$[\d,]+\s*-\s*\$[\d,]+|Over \$[\d,]+)")
EXACT_AMOUNT_PATTERN = re.compile(r"\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*$")
AMOUNT_LOWER_PATTERN = re.compile(r"\$(\d{1,3}(?:,\d{3})*)\s*-\s*$")
PAPER_OWNER_PATTERN = re.compile(r"^(JT|SP|DC|st)\s*\|?\s*", re.I)


def pdf_has_extractable_text(pdf_path: Path) -> bool:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return any((page.extract_text() or "").strip() for page in pdf.pages)
    except Exception:
        return False


class HousePDFParser:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def parse(self, pdf_path: Path) -> ParsedFiling:
        self.errors = []
        filing = ParsedFiling("", "", "", None, pdf_path=str(pdf_path))
        try:
            with pdfplumber.open(pdf_path) as pdf:
                filing.page_count = len(pdf.pages)
                full_text = ""
                page_texts = []
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    page_texts.append(text)
                    full_text += text + "\n"
                self._extract_metadata(full_text, filing)
                for page_num, text in enumerate(page_texts, start=1):
                    filing.transactions.extend(self._extract_transactions(text, page_num))
        except Exception as exc:
            self.errors.append(f"Failed to parse {pdf_path}: {exc}")
        filing.parse_errors = self.errors.copy()
        return filing

    def _extract_metadata(self, text: str, filing: ParsedFiling) -> None:
        for pattern, attr in [
            (FILING_ID_PATTERN, "filing_id"),
            (FILER_NAME_PATTERN, "filer_name"),
            (FILER_STATUS_PATTERN, "filer_status"),
            (STATE_DISTRICT_PATTERN, "state_district"),
        ]:
            match = pattern.search(text)
            if match:
                setattr(filing, attr, match.group(1).strip())

    def _extract_transactions(self, page_text: str, page_number: int) -> list[ParsedTransaction]:
        transactions = []
        lines = page_text.split("\n")
        in_transactions = False
        current_entry_lines: list[str] = []
        description_lines: list[str] = []
        subowner_lines: list[str] = []
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue
            if "Owner" in line and "Asset" in line and "Transaction" in line:
                in_transactions = True
                continue
            if in_transactions and ("Type" in line and "Date" in line and "Gains" in line):
                continue
            if "CERTIFY" in line or "certify" in line or "asset type abbreviations" in line.lower():
                if current_entry_lines:
                    txn = self._parse_entry(current_entry_lines, description_lines, subowner_lines, page_number)
                    if txn:
                        transactions.append(txn)
                break
            if not in_transactions:
                continue
            is_filing_status = line_stripped.startswith("F") and "S" in line_stripped[:20] and ":" in line_stripped
            is_subowner = line_stripped.startswith("S") and "O" in line_stripped[:20] and ":" in line_stripped
            is_description = line_stripped.startswith("D") and ":" in line_stripped[:20]
            is_location = line_stripped.startswith("L") and ":" in line_stripped[:20]
            if is_filing_status or is_location:
                continue
            has_owner_prefix = line_stripped.startswith(("SP ", "DC ", "JT "))
            dates_in_line = DATE_PATTERN.findall(line_stripped)
            has_amount = bool(AMOUNT_PATTERN.search(line_stripped))
            is_new_entry = has_owner_prefix or len(dates_in_line) >= 2 or (len(dates_in_line) == 1 and has_amount)
            if is_subowner:
                subowner_match = re.search(r":\s*(.+)", line_stripped)
                if subowner_match:
                    subowner_lines.append(subowner_match.group(1))
                continue
            if is_description:
                desc_match = re.search(r":\s*(.+)", line_stripped)
                if desc_match:
                    description_lines.append(desc_match.group(1))
                continue
            is_amount_continuation = line_stripped.startswith("$") or re.match(r"^\$[\d,]+$", line_stripped)
            if is_new_entry and current_entry_lines:
                txn = self._parse_entry(current_entry_lines, description_lines, subowner_lines, page_number)
                if txn:
                    transactions.append(txn)
                current_entry_lines = [line_stripped]
                description_lines = []
                subowner_lines = []
            elif is_amount_continuation and current_entry_lines:
                current_entry_lines[-1] += " " + line_stripped
            elif current_entry_lines:
                current_entry_lines.append(line_stripped)
            elif is_new_entry:
                current_entry_lines = [line_stripped]
        return transactions

    def _parse_entry(self, entry_lines: list[str], description_lines: list[str], subowner_lines: list[str], page_number: int) -> ParsedTransaction | None:
        if not entry_lines:
            return None
        full_text = " ".join(entry_lines)
        raw_line = full_text
        try:
            owner_match = OWNER_PATTERN.match(full_text)
            owner_code = owner_match.group(1) if owner_match and owner_match.group(1) else ""
            owner = OWNER_CODES.get(owner_code, "self")
            if owner_code:
                full_text = full_text[len(owner_code):].strip()
            ticker_match = TICKER_PATTERN.search(full_text)
            ticker = ticker_match.group(1) if ticker_match else None
            asset_type_match = ASSET_TYPE_PATTERN.search(full_text)
            asset_type = asset_type_match.group(1) if asset_type_match else None
            dates = DATE_PATTERN.findall(full_text)
            transaction_date = datetime.strptime(dates[0], "%m/%d/%Y") if len(dates) >= 1 else None
            notification_date = datetime.strptime(dates[1], "%m/%d/%Y") if len(dates) >= 2 else None
            amount_min = None
            amount_max = None
            amount_match = AMOUNT_PATTERN.search(full_text)
            amount_text = amount_match.group(1) if amount_match else ""
            if not amount_text:
                for i, line in enumerate(entry_lines):
                    lower_match = AMOUNT_LOWER_PATTERN.search(line)
                    if lower_match:
                        amount_min = int(lower_match.group(1).replace(",", ""))
                        for remaining_line in entry_lines[i + 1:]:
                            upper_matches = re.findall(r"\$(\d{1,3}(?:,\d{3})*)", remaining_line)
                            if upper_matches:
                                amount_max = int(upper_matches[-1].replace(",", ""))
                                amount_text = f"${amount_min:,} - ${amount_max:,}"
                                break
                        if amount_max is not None:
                            break
                    all_amounts = re.findall(r"\$([\d,]+)", line)
                    if len(all_amounts) >= 2 and amount_min is None:
                        amount_min = int(all_amounts[0].replace(",", ""))
                        amount_max = int(all_amounts[-1].replace(",", ""))
                        if amount_max > amount_min:
                            amount_text = f"${amount_min:,} - ${amount_max:,}"
                            break
                        amount_min, amount_max = None, None
            if not amount_text:
                for line in entry_lines:
                    exact_match = EXACT_AMOUNT_PATTERN.search(line)
                    if exact_match:
                        exact_val = int(float(exact_match.group(1).replace(",", "")))
                        amount_min = exact_val
                        amount_max = exact_val
                        amount_text = f"${exact_val:,} (exact)"
                        break
            if amount_min is None and amount_text:
                amount_min, amount_max = self._parse_amount(amount_text)
            txn_type_str = "purchase"
            first_date_match = DATE_PATTERN.search(full_text)
            if first_date_match:
                pre_date_text = full_text[:first_date_match.start()]
                if " S (partial)" in pre_date_text or "S (partial)" in pre_date_text:
                    txn_type_str = "sale_partial"
                elif re.search(r"\bS\b", pre_date_text):
                    txn_type_str = "sale"
                elif re.search(r"\bE\b", pre_date_text):
                    txn_type_str = "exchange"
                elif re.search(r"\bP\b", pre_date_text):
                    txn_type_str = "purchase"
            asset_name = self._extract_asset_name(full_text, ticker, asset_type, dates)
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
                cap_gains_over_200=None,
                description=" ".join(description_lines) if description_lines else None,
                subowner=" | ".join(subowner_lines) if subowner_lines else None,
                page_number=page_number,
                raw_line=raw_line,
            )
        except Exception as exc:
            self.errors.append(f"Failed to parse entry on page {page_number}: {exc}")
            return None

    def _parse_amount(self, amount_text: str) -> tuple[int | None, int | None]:
        normalized = amount_text.replace(" ", "").replace("\n", "")
        for range_text, bounds in AMOUNT_RANGES.items():
            if normalized == range_text.replace(" ", ""):
                return bounds
        if normalized.lower().startswith("over"):
            match = re.search(r"\$([\d,]+)", normalized)
            if match:
                min_val = int(match.group(1).replace(",", "")) + 1
                return min_val, min_val * 2
        match = re.match(r"\$([\d,]+)-\$([\d,]+)", normalized)
        if match:
            return int(match.group(1).replace(",", "")), int(match.group(2).replace(",", ""))
        return None, None

    def _extract_asset_name(self, text: str, ticker: str | None, asset_type: str | None, dates: list[str]) -> str:
        first_date_pos = len(text)
        if dates:
            for date in dates:
                pos = text.find(date)
                if pos != -1 and pos < first_date_pos:
                    first_date_pos = pos
        asset_text = text[:first_date_pos].strip()
        asset_text = re.sub(r"\s+[PSE]\s*$", "", asset_text)
        asset_text = re.sub(r"\s+S \(partial\)\s*$", "", asset_text)
        if asset_type:
            asset_text = re.sub(rf"\s*\[{asset_type}\]\s*$", "", asset_text)
        if ticker:
            asset_text = re.sub(rf"\s*\({re.escape(ticker)}\)\s*$", "", asset_text)
        return asset_text.strip()


class PaperHouseFilingParser:
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

    def __init__(self) -> None:
        self.errors: list[str] = []

    def parse_ocr_text(self, ocr_text: str, pdf_path: Path, page_count: int) -> ParsedFiling:
        self.errors = []
        filing = ParsedFiling(
            filing_id="",
            filer_name="",
            filer_status="",
            state_district=None,
            pdf_path=str(pdf_path),
            page_count=page_count,
        )

        name_match = re.search(r"NAME[;:]?\s*([A-Za-z\s\.]+?)(?:\s+OFFICE|\s+Member|\n)", ocr_text)
        if name_match:
            filing.filer_name = name_match.group(1).strip()

        filing_id_match = re.search(r"(?:Filing\s*ID|Doc\s*ID)[#:\s]*(\d+)", ocr_text, re.I)
        filing.filing_id = filing_id_match.group(1) if filing_id_match else pdf_path.stem

        if "Member" in ocr_text:
            filing.filer_status = "Member"

        filing.transactions = self._extract_paper_transactions(ocr_text)
        filing.parse_errors = self.errors.copy()
        return filing

    def _extract_paper_transactions(self, text: str) -> list[ParsedTransaction]:
        transactions = []
        lines = text.split("\n")
        for i, line in enumerate(lines):
            line = line.strip()
            if not line or len(line) < 10:
                continue
            if any(skip in line.lower() for skip in [
                "example", "provide full", "mega corp", "ticker symbol",
                "type of", "date of", "amount of", "transaction",
                "action", "certified", "penalty",
            ]):
                continue
            txn = self._parse_paper_transaction_line(line, i + 1)
            if txn:
                transactions.append(txn)
        return transactions

    def _parse_paper_transaction_line(self, line: str, line_num: int) -> ParsedTransaction | None:
        del line_num
        line = line.replace("|", " | ")
        line = re.sub(r"\s+", " ", line)
        dates = re.findall(r"(\d{1,2}/\d{1,2}/\d{2,4})", line)
        if not dates:
            return None

        owner = "self"
        owner_match = PAPER_OWNER_PATTERN.match(line)
        if owner_match:
            owner_code = owner_match.group(1).upper()
            owner = OWNER_CODES.get(owner_code, "self")
            line = line[owner_match.end():]

        ticker = None
        ticker_match = re.search(r"[-–]\s*([A-Z]{1,5}(?:\.[A-Z])?)\b", line)
        if ticker_match:
            ticker = ticker_match.group(1)

        first_date_pos = line.find(dates[0])
        if first_date_pos <= 0:
            return None
        asset_section = line[:first_date_pos]
        asset_name = re.sub(r"\s*[-–]\s*[A-Z]{1,5}(?:\.[A-Z])?\s*$", "", asset_section)
        asset_name = re.sub(r"\s*\|+\s*$", "", asset_name)
        asset_name = re.sub(r"^\s*\|+\s*", "", asset_name)
        asset_name = re.sub(r"\s*[PSE]\s*$", "", asset_name)
        asset_name = re.sub(r"\s+[xX]+\s*$", "", asset_name)
        asset_name = re.sub(r"\s+[~'\"`.,;:]+\s*$", "", asset_name)
        asset_name = asset_name.strip()
        if not self._looks_like_asset_name(asset_name):
            return None

        pre_date = line[:first_date_pos]
        if re.search(r"\bS\b", pre_date):
            txn_type = "sale"
        elif re.search(r"\bE\b", pre_date):
            txn_type = "exchange"
        elif re.search(r"\bP\b", pre_date):
            txn_type = "purchase"
        elif "x" in line.lower():
            txn_type = "purchase"
        else:
            return None

        parsed_dates: list[datetime | None] = []
        for date_str in dates[:2]:
            try:
                fmt = "%m/%d/%Y" if len(date_str.split("/")[-1]) == 4 else "%m/%d/%y"
                parsed_dates.append(datetime.strptime(date_str, fmt))
            except ValueError:
                parsed_dates.append(None)
        transaction_date = parsed_dates[0] if parsed_dates else None
        notification_date = parsed_dates[1] if len(parsed_dates) > 1 else None
        amount_min, amount_max, amount_text = self._extract_paper_amount(line, dates)
        if amount_min is None and amount_max is None:
            return None

        return ParsedTransaction(
            owner=owner,
            asset_name=asset_name,
            ticker=ticker,
            asset_type=None,
            transaction_type=txn_type,
            transaction_date=transaction_date,
            notification_date=notification_date,
            amount_min=amount_min,
            amount_max=amount_max,
            amount_text=amount_text,
            cap_gains_over_200=None,
            description=None,
            subowner=None,
            page_number=1,
            raw_line=line,
        )

    def _looks_like_asset_name(self, asset_name: str) -> bool:
        if len(asset_name) < 3 or re.match(r"^[\d\s\|]+$", asset_name):
            return False
        letters = sum(1 for c in asset_name if c.isalpha())
        if letters < 4:
            return False
        alpha_ratio = letters / max(1, len(asset_name))
        if alpha_ratio < 0.45:
            return False
        return any(len(token) >= 3 for token in re.findall(r"[A-Za-z]+", asset_name))

    def _extract_paper_amount(self, line: str, dates: list[str]) -> tuple[int | None, int | None, str]:
        after_dates = line
        if dates:
            last_date_pos = line.rfind(dates[-1])
            if last_date_pos >= 0:
                after_dates = line[last_date_pos + len(dates[-1]):]

        amount_match = re.search(r"\$[\d,]+\s*[-–]\s*\$[\d,]+", after_dates)
        if amount_match:
            text = amount_match.group(0)
            nums = re.findall(r"\$([\d,]+)", text)
            if len(nums) >= 2:
                try:
                    return int(nums[0].replace(",", "")), int(nums[1].replace(",", "")), text
                except ValueError:
                    pass

        column_match = re.search(r"[xX]\s*([A-J])|([A-J])\s*[xX]", after_dates)
        if column_match:
            col = (column_match.group(1) or column_match.group(2)).upper()
            if col in self.AMOUNT_COLUMNS:
                lo, hi = self.AMOUNT_COLUMNS[col]
                return lo, hi, f"Column {col}"

        if "x" in after_dates.lower():
            return 1_001, 15_000, "$1,001 - $15,000 (inferred)"

        return None, None, ""


def validate_ocr_output(text: str) -> bool:
    if not text or len(text) < 50:
        return False
    allowed_special = set(" .,()-$%:/\n\t'\"")
    special_chars = sum(1 for c in text if not c.isalnum() and c not in allowed_special)
    if len(text) > 0 and (special_chars / len(text)) > 0.3:
        return False
    has_date = bool(re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", text))
    has_amount = bool(re.search(r"\$[\d,]+", text))
    return has_date or has_amount


def parse_house_pdf(pdf_path: Path) -> tuple[ParsedFiling | None, str | None]:
    parser = HousePDFParser()
    filing = parser.parse(pdf_path)

    if filing.transactions:
        return filing, "parse_errors" if filing.parse_errors else None

    needs_ocr = pdf_path.stem.startswith("822") or not pdf_has_extractable_text(pdf_path)
    if not needs_ocr:
        return filing, "parse_errors" if filing.parse_errors else "no_transactions"
    if not is_tesseract_available():
        return None, "ocr_unavailable"

    ocr_result = ocr_pdf(pdf_path)
    if not ocr_result.is_successful:
        return None, "ocr_failed"
    if not validate_ocr_output(ocr_result.text):
        return None, "ocr_invalid"

    paper_parser = PaperHouseFilingParser()
    paper_filing = paper_parser.parse_ocr_text(ocr_result.text, pdf_path, ocr_result.page_count)
    if paper_filing.transactions:
        return paper_filing, None
    return paper_filing, "no_transactions"


def parse_house_pdf_text_only(pdf_path: Path):
    return parse_house_pdf(pdf_path)
