"""
Form 4 XML parser.

Parses SEC EDGAR Form 4 ownership XML into canonical filing and transaction
records. Handles both non-derivative and derivative transactions, amendments,
footnotes, and ownership nature.
"""

import json
import logging
import os
import time
import xml.etree.ElementTree as ET

from db import (
    get_connection,
    upsert_filing,
    insert_transactions_batch,
    clear_transactions_for_filing,
    get_amendment_candidates,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------
def _strip_namespace(root):
    """
    Remove all XML namespace prefixes from the element tree in-place.

    This is the standard approach for SEC EDGAR XML, which sometimes uses
    xmlns and sometimes doesn't. Stripping makes all lookups uniform.
    """
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]
        # Also strip namespace from attributes
        new_attrib = {}
        for key, val in elem.attrib.items():
            if "}" in key:
                key = key.split("}", 1)[1]
            new_attrib[key] = val
        elem.attrib = new_attrib


def _text(element, tag: str, default: str = None) -> str | None:
    """Extract text from a child element, or return default."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return default


def _bool(element, tag: str) -> bool:
    """Extract a boolean (0/1/true/false) from a child element."""
    val = _text(element, tag, "0")
    return val in ("1", "true", "True", "TRUE")


def _float(element, tag: str) -> float | None:
    """Extract a float from a child element."""
    val = _text(element, tag)
    if val is None:
        return None
    try:
        return float(val.replace(",", ""))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Footnote extraction
# ---------------------------------------------------------------------------
def _extract_footnotes(root) -> dict:
    """
    Extract all footnotes from the XML as a dict {id: text}.

    Namespace-stripped tree expected (call _strip_namespace first).
    """
    footnotes = {}
    fn_section = root.find(".//footnotes")
    if fn_section is not None:
        for fn in fn_section.findall("footnote"):
            fn_id = fn.get("id", "")
            # Get all text content including nested elements
            fn_text = ET.tostring(fn, encoding="unicode", method="text").strip()
            if fn_text:
                footnotes[fn_id] = fn_text
    return footnotes


def _collect_footnote_ids(element) -> set[str]:
    """
    Collect footnote IDs referenced by an element or its children.

    In Form 4 XML, footnote references are <footnoteId id="F1"/> elements
    that appear as siblings of <value> elements. Returns a deduplicated set.
    """
    ids = set()
    for child in element.iter():
        # <footnoteId id="F1"/> — the footnoteId is the tag, id is the attribute
        if child.tag == "footnoteId":
            ref_id = child.get("id")
            if ref_id:
                ids.add(ref_id)
    return ids


# ---------------------------------------------------------------------------
# Transaction parsing
# ---------------------------------------------------------------------------
def _parse_transaction(txn_elem, is_derivative: bool, footnotes: dict) -> dict | None:
    """
    Parse a single transaction element (nonDerivativeTransaction or
    derivativeTransaction) into a canonical dict.
    """
    # Security title
    security_title = _text(txn_elem, ".//securityTitle/value")

    # Transaction date
    txn_date = _text(txn_elem, ".//transactionDate/value")

    # Transaction coding
    coding = txn_elem.find(".//transactionCoding")
    txn_code = None
    equity_swap = False
    if coding is not None:
        txn_code = _text(coding, "transactionCode")
        equity_swap = _bool(coding, "equitySwapInvolved")

    # Transaction amounts
    amounts = txn_elem.find(".//transactionAmounts")
    shares = None
    price = None
    if amounts is not None:
        shares = _float(amounts, ".//transactionShares/value")
        price = _float(amounts, ".//transactionPricePerShare/value")

    # Post-transaction amounts
    post = txn_elem.find(".//postTransactionAmounts")
    shares_after = None
    if post is not None:
        shares_after = _float(post, ".//sharesOwnedFollowingTransaction/value")

    # Ownership nature
    ownership = txn_elem.find(".//ownershipNature")
    ownership_nature = None
    indirect_entity = None
    if ownership is not None:
        ownership_nature = _text(ownership, "directOrIndirectOwnership/value")
        indirect_entity = _text(ownership, "natureOfOwnership/value")

    # Derivative-specific: underlying security
    underlying = None
    if is_derivative:
        underlying = _text(txn_elem, ".//underlyingSecurity/underlyingSecurityTitle/value")

    # Collect footnote text
    fn_ids = _collect_footnote_ids(txn_elem)
    fn_texts = [footnotes.get(fid, "") for fid in fn_ids if fid in footnotes]
    footnote_text = " | ".join(fn_texts) if fn_texts else None

    # Compute total value
    total_value = None
    if shares is not None and price is not None:
        total_value = round(shares * price, 2)

    return {
        "security_title": security_title,
        "transaction_date": txn_date,
        "transaction_code": txn_code,
        "equity_swap": 1 if equity_swap else 0,
        "shares": shares,
        "price_per_share": price,
        "total_value": total_value,
        "shares_after": shares_after,
        "ownership_nature": ownership_nature,
        "indirect_entity": indirect_entity,
        "is_derivative": 1 if is_derivative else 0,
        "underlying_security": underlying,
        "footnotes": footnote_text,
    }


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------
def parse_form4_xml(xml_path: str) -> dict:
    """
    Parse a Form 4 XML file into a structured dict.

    Returns:
        {
            "filing": {filing-level fields},
            "transactions": [list of transaction dicts],
            "parse_error": None or error string,
        }
    """
    result = {"filing": None, "transactions": [], "parse_error": None}

    if not xml_path or not os.path.exists(xml_path):
        result["parse_error"] = f"File not found: {xml_path}"
        return result

    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        result["parse_error"] = f"XML parse error: {e}"
        return result

    # Strip all XML namespaces so all lookups use bare element names
    _strip_namespace(root)

    # --- Issuer ---
    issuer = root.find("issuer")
    cik_issuer = None
    if issuer is not None:
        cik_issuer = _text(issuer, "issuerCik")
        if cik_issuer:
            cik_issuer = cik_issuer.zfill(10)

    # --- Reporting Owners ---
    # Parse ALL reporting owners. The first is treated as primary; additional
    # owners are stored in a JSON field for multi-owner filings.
    owner_elements = root.findall("reportingOwner")
    all_owners = []

    for owner_elem in owner_elements:
        owner_data = {
            "cik": None,
            "name": None,
            "officer_title": None,
            "is_officer": False,
            "is_director": False,
            "is_ten_pct_owner": False,
            "is_other": False,
        }

        owner_id = owner_elem.find("reportingOwnerId")
        if owner_id is not None:
            owner_cik = _text(owner_id, "rptOwnerCik")
            if owner_cik:
                owner_data["cik"] = owner_cik.zfill(10)
            owner_data["name"] = _text(owner_id, "rptOwnerName")

        relationship = owner_elem.find("reportingOwnerRelationship")
        if relationship is not None:
            owner_data["is_officer"] = _bool(relationship, "isOfficer")
            owner_data["is_director"] = _bool(relationship, "isDirector")
            owner_data["is_ten_pct_owner"] = _bool(relationship, "isTenPercentOwner")
            owner_data["is_other"] = _bool(relationship, "isOther")
            owner_data["officer_title"] = _text(relationship, "officerTitle")

        all_owners.append(owner_data)

    # Primary owner is the first; use for main filing fields
    primary_owner = all_owners[0] if all_owners else {}
    owner_cik = primary_owner.get("cik")
    owner_name = primary_owner.get("name")
    officer_title = primary_owner.get("officer_title")
    is_officer = primary_owner.get("is_officer", False)
    is_director = primary_owner.get("is_director", False)
    is_ten_pct = primary_owner.get("is_ten_pct_owner", False)
    is_other = primary_owner.get("is_other", False)

    # Additional owners stored as JSON
    additional_owners = all_owners[1:] if len(all_owners) > 1 else None
    if additional_owners:
        logger.info(
            f"Multi-owner filing in {xml_path}: {len(all_owners)} owners. "
            f"Primary: {owner_name}, Additional: {[o.get('name') for o in additional_owners]}"
        )

    # --- Amendment info ---
    is_amendment = False
    amendment_type = None
    is_amend_elem = root.find("isAmendment")
    if is_amend_elem is not None and is_amend_elem.text:
        is_amendment = is_amend_elem.text.strip() in ("1", "true", "True", "TRUE")
    amend_type_elem = root.find("amendmentType")
    if amend_type_elem is not None and amend_type_elem.text:
        amendment_type = amend_type_elem.text.strip()

    # --- Period of report ---
    period_of_report = _text(root, "periodOfReport")

    # --- Rule 10b5-1 structured indicator (post-April 2023 filings) ---
    aff10b5_elem = root.find("aff10b5One")
    aff10b5one = False
    if aff10b5_elem is not None and aff10b5_elem.text:
        aff10b5one = aff10b5_elem.text.strip() in ("1", "true", "True", "TRUE")

    # --- Footnotes ---
    footnotes = _extract_footnotes(root)

    # --- Build filing record ---
    filing = {
        "cik_issuer": cik_issuer,
        "cik_owner": owner_cik,
        "owner_name": owner_name,
        "officer_title": officer_title,
        "is_officer": 1 if is_officer else 0,
        "is_director": 1 if is_director else 0,
        "is_ten_pct_owner": 1 if is_ten_pct else 0,
        "is_other": 1 if is_other else 0,
        "is_amendment": 1 if is_amendment else 0,
        "amendment_type": amendment_type,
        "period_of_report": period_of_report,
        "aff10b5one": 1 if aff10b5one else 0,
        "additional_owners": json.dumps(additional_owners) if additional_owners else None,
        "parsed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "parse_error": None,
    }
    result["filing"] = filing

    # --- Non-derivative transactions ---
    nd_table = root.find("nonDerivativeTable")
    if nd_table is not None:
        for txn_elem in nd_table.findall("nonDerivativeTransaction"):
            txn = _parse_transaction(txn_elem, is_derivative=False, footnotes=footnotes)
            if txn:
                result["transactions"].append(txn)

    # --- Derivative transactions ---
    d_table = root.find("derivativeTable")
    if d_table is not None:
        for txn_elem in d_table.findall("derivativeTransaction"):
            txn = _parse_transaction(txn_elem, is_derivative=True, footnotes=footnotes)
            if txn:
                result["transactions"].append(txn)

    return result


def parse_and_store_filing(
    accession_number: str,
    xml_path: str,
    filing_date: str,
    xml_url: str,
    issuer_cik: str,
    is_amendment: bool = False,
    db_path: str = None,
) -> dict:
    """
    Parse a Form 4 XML and store the filing + transactions in the database.

    Handles amendment deduplication: if this is an amendment, it finds and
    replaces the original filing's transactions.

    Returns the parse result dict.
    """
    parsed = parse_form4_xml(xml_path)

    filing_data = parsed.get("filing") or {}
    filing_data.update({
        "accession_number": accession_number,
        "filing_date": filing_date,
        "xml_url": xml_url,
        "raw_xml_path": xml_path,
    })

    # Override issuer CIK if not parsed from XML
    if not filing_data.get("cik_issuer"):
        filing_data["cik_issuer"] = issuer_cik

    # Mark amendment status from metadata if XML didn't indicate
    if is_amendment and not filing_data.get("is_amendment"):
        filing_data["is_amendment"] = 1

    if parsed["parse_error"]:
        filing_data["parse_error"] = parsed["parse_error"]

    with get_connection(db_path) as conn:
        upsert_filing(conn, filing_data)

        # Handle amendments: remove transactions from the original filing
        if filing_data.get("is_amendment") and filing_data.get("cik_owner") and filing_data.get("period_of_report"):
            originals = get_amendment_candidates(
                conn,
                filing_data["cik_issuer"],
                filing_data["cik_owner"],
                filing_data["period_of_report"],
                exclude_accession=accession_number,
            )
            for orig in originals:
                logger.info(
                    f"Amendment {accession_number} supersedes {orig['accession_number']}. "
                    f"Clearing old transactions."
                )
                clear_transactions_for_filing(conn, orig["accession_number"])

        # Clear any existing transactions for this filing (re-parse safety)
        clear_transactions_for_filing(conn, accession_number)

        # Prepare transactions for batch insert
        txns_to_insert = []
        for txn in parsed["transactions"]:
            txn.update({
                "accession_number": accession_number,
                "cik_issuer": filing_data.get("cik_issuer"),
                "cik_owner": filing_data.get("cik_owner"),
                "owner_name": filing_data.get("owner_name"),
                "officer_title": filing_data.get("officer_title"),
                # Classification fields — populated later by classification step
                "role_class": None,
                "transaction_class": None,
                "is_likely_planned": 0,
                "is_discretionary": 0,
                "pct_holdings_changed": None,
                "include_in_signal": 0,
                "exclusion_reason": None,
            })
            txns_to_insert.append(txn)

        # Batch insert all transactions
        insert_transactions_batch(conn, txns_to_insert)

    logger.info(
        f"Parsed filing {accession_number}: "
        f"{len(parsed['transactions'])} transactions"
        f"{' (AMENDMENT)' if filing_data.get('is_amendment') else ''}"
        f"{' ERROR: ' + parsed['parse_error'] if parsed['parse_error'] else ''}"
    )

    return parsed


def parse_all_pending(db_path: str = None):
    """
    Parse all downloaded but not-yet-parsed filings.

    Looks for filings in the DB with raw_xml_path set but parsed_at is NULL.
    """
    with get_connection(db_path) as conn:
        rows = conn.execute("""
            SELECT accession_number, raw_xml_path, filing_date, xml_url,
                   cik_issuer, is_amendment
            FROM filings
            WHERE raw_xml_path IS NOT NULL AND parsed_at IS NULL
        """).fetchall()

    logger.info(f"Found {len(rows)} unparsed filings.")

    for row in rows:
        try:
            parse_and_store_filing(
                accession_number=row["accession_number"],
                xml_path=row["raw_xml_path"],
                filing_date=row["filing_date"],
                xml_url=row["xml_url"],
                issuer_cik=row["cik_issuer"],
                is_amendment=bool(row["is_amendment"]),
                db_path=db_path,
            )
        except Exception as e:
            logger.error(f"Failed to parse {row['accession_number']}: {e}")
