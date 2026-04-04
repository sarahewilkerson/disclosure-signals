from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path


def _strip_namespace(root: ET.Element) -> None:
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]
        new_attrib = {}
        for key, val in elem.attrib.items():
            if "}" in key:
                key = key.split("}", 1)[1]
            new_attrib[key] = val
        elem.attrib = new_attrib


def _text(element: ET.Element | None, tag: str, default: str | None = None) -> str | None:
    if element is None:
        return default
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return default


def _bool(element: ET.Element | None, tag: str) -> bool:
    val = _text(element, tag, "0")
    return val in ("1", "true", "True", "TRUE")


def _float(element: ET.Element | None, tag: str) -> float | None:
    val = _text(element, tag)
    if val is None:
        return None
    try:
        return float(val.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _extract_footnotes(root: ET.Element) -> dict[str, str]:
    footnotes: dict[str, str] = {}
    fn_section = root.find(".//footnotes")
    if fn_section is not None:
        for fn in fn_section.findall("footnote"):
            fn_id = fn.get("id", "")
            fn_text = ET.tostring(fn, encoding="unicode", method="text").strip()
            if fn_id and fn_text:
                footnotes[fn_id] = fn_text
    return footnotes


def _collect_footnote_ids(element: ET.Element) -> set[str]:
    ids: set[str] = set()
    for child in element.iter():
        if child.tag == "footnoteId":
            ref_id = child.get("id")
            if ref_id:
                ids.add(ref_id)
    return ids


def _parse_transaction(txn_elem: ET.Element, is_derivative: bool, footnotes: dict[str, str]) -> dict | None:
    security_title = _text(txn_elem, ".//securityTitle/value")
    txn_date = _text(txn_elem, ".//transactionDate/value")

    coding = txn_elem.find(".//transactionCoding")
    txn_code = _text(coding, "transactionCode")
    equity_swap = _bool(coding, "equitySwapInvolved")

    amounts = txn_elem.find(".//transactionAmounts")
    shares = _float(amounts, ".//transactionShares/value")
    price = _float(amounts, ".//transactionPricePerShare/value")

    post = txn_elem.find(".//postTransactionAmounts")
    shares_after = _float(post, ".//sharesOwnedFollowingTransaction/value")

    ownership = txn_elem.find(".//ownershipNature")
    ownership_nature = _text(ownership, "directOrIndirectOwnership/value")
    indirect_entity = _text(ownership, "natureOfOwnership/value")

    underlying = _text(txn_elem, ".//underlyingSecurity/underlyingSecurityTitle/value") if is_derivative else None

    fn_ids = _collect_footnote_ids(txn_elem)
    fn_texts = [footnotes.get(fid, "") for fid in fn_ids if fid in footnotes]
    footnote_text = " | ".join(fn_texts) if fn_texts else None

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


def parse_form4_xml(xml_path: str | Path) -> dict:
    path = Path(xml_path)
    result = {"filing": None, "transactions": [], "parse_error": None, "xml_path": str(path)}
    if not path.exists():
        result["parse_error"] = f"File not found: {path}"
        return result

    try:
        tree = ET.parse(path)
        root = tree.getroot()
    except ET.ParseError as exc:
        result["parse_error"] = f"XML parse error: {exc}"
        return result

    _strip_namespace(root)

    issuer = root.find("issuer")
    cik_issuer = _text(issuer, "issuerCik")
    if cik_issuer:
        cik_issuer = cik_issuer.zfill(10)
    issuer_name = _text(issuer, "issuerName")
    ticker_issuer = _text(issuer, "issuerTradingSymbol")

    owner_elements = root.findall("reportingOwner")
    all_owners = []
    for owner_elem in owner_elements:
        owner_id = owner_elem.find("reportingOwnerId")
        relationship = owner_elem.find("reportingOwnerRelationship")
        owner_cik = _text(owner_id, "rptOwnerCik")
        all_owners.append(
            {
                "cik": owner_cik.zfill(10) if owner_cik else None,
                "name": _text(owner_id, "rptOwnerName"),
                "officer_title": _text(relationship, "officerTitle"),
                "is_officer": _bool(relationship, "isOfficer"),
                "is_director": _bool(relationship, "isDirector"),
                "is_ten_pct_owner": _bool(relationship, "isTenPercentOwner"),
                "is_other": _bool(relationship, "isOther"),
            }
        )

    primary_owner = all_owners[0] if all_owners else {}
    additional_owners = all_owners[1:] if len(all_owners) > 1 else None

    is_amendment = _bool(root, "isAmendment")
    amendment_type = _text(root, "amendmentType")
    period_of_report = _text(root, "periodOfReport")
    aff10b5one = _bool(root, "aff10b5One")
    footnotes = _extract_footnotes(root)

    filing = {
        "cik_issuer": cik_issuer,
        "issuer_name": issuer_name,
        "ticker_issuer": ticker_issuer,
        "cik_owner": primary_owner.get("cik"),
        "owner_name": primary_owner.get("name"),
        "officer_title": primary_owner.get("officer_title"),
        "is_officer": 1 if primary_owner.get("is_officer") else 0,
        "is_director": 1 if primary_owner.get("is_director") else 0,
        "is_ten_pct_owner": 1 if primary_owner.get("is_ten_pct_owner") else 0,
        "is_other": 1 if primary_owner.get("is_other") else 0,
        "is_amendment": 1 if is_amendment else 0,
        "amendment_type": amendment_type,
        "period_of_report": period_of_report,
        "aff10b5one": 1 if aff10b5one else 0,
        "additional_owners": json.dumps(additional_owners) if additional_owners else None,
        "parse_error": None,
    }
    result["filing"] = filing

    nd_table = root.find("nonDerivativeTable")
    if nd_table is not None:
        for txn_elem in nd_table.findall("nonDerivativeTransaction"):
            txn = _parse_transaction(txn_elem, is_derivative=False, footnotes=footnotes)
            if txn:
                result["transactions"].append(txn)

    d_table = root.find("derivativeTable")
    if d_table is not None:
        for txn_elem in d_table.findall("derivativeTransaction"):
            txn = _parse_transaction(txn_elem, is_derivative=True, footnotes=footnotes)
            if txn:
                result["transactions"].append(txn)

    return result
