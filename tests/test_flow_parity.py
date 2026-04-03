from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

import pytest
from PIL import Image, ImageDraw, ImageFont

from signals.congress.engine import compute_aggregate as direct_compute_aggregate
from signals.congress.engine import compute_confidence_score as direct_compute_confidence_score
from signals.congress.engine import score_transaction as direct_congress_score_transaction
from signals.congress.house_parser import PaperHouseFilingParser, parse_house_pdf_text_only
from signals.congress.senate_connector import SenateConnector as DirectSenateConnector
from signals.insider.engine import (
    aggregate_company_signal as direct_aggregate_company_signal,
    classify_role as direct_classify_role,
    compute_pct_holdings_changed as direct_compute_pct_holdings_changed,
    detect_planned_trade as direct_detect_planned_trade,
    score_transaction as direct_insider_score_transaction,
)
from signals.insider.parser import parse_form4_xml as direct_parse_form4_xml


def _load_parity_fixture(name: str) -> dict:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "expected_parity" / f"{name}.json"
    return json.loads(fixture_path.read_text())


def _insider_scored_rows(parsed: dict, scorer, role_classifier, planned_detector, pct_fn, reference_date: datetime) -> list[dict]:
    filing = parsed["filing"]
    rows = []
    for txn in parsed["transactions"]:
        role_class, exclusion = role_classifier(
            filing.get("officer_title"),
            filing.get("owner_name"),
            bool(filing.get("is_officer")),
            bool(filing.get("is_director")),
            bool(filing.get("is_ten_pct_owner")),
            bool(filing.get("is_other")),
        )
        if exclusion is not None or txn.get("transaction_code") not in {"P", "S"}:
            continue
        row = {
            "transaction_code": txn.get("transaction_code"),
            "role_class": role_class,
            "is_likely_planned": 1 if planned_detector(txn.get("footnotes")) else 0,
            "ownership_nature": txn.get("ownership_nature"),
            "pct_holdings_changed": pct_fn(txn.get("shares"), txn.get("shares_after")),
            "transaction_date": txn.get("transaction_date"),
            "cik_owner": filing.get("cik_owner"),
            "total_value": txn.get("total_value"),
            "accession_number": "fixture",
        }
        row.update(scorer(row, reference_date))
        rows.append(row)
    return rows


def test_insider_form4_parse_to_score_parity():
    fixture = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "insider" / "form4_simple_buy.xml"
    expected = _load_parity_fixture("insider_flow")
    
    direct = direct_parse_form4_xml(fixture)

    assert direct["filing"]["cik_issuer"] == "0000320193"
    assert direct["filing"]["owner_name"] == "DOE JOHN"
    assert len(direct["transactions"]) == 1

    reference_date = datetime(2024, 6, 15)
    direct_rows = _insider_scored_rows(
        direct,
        direct_insider_score_transaction,
        direct_classify_role,
        direct_detect_planned_trade,
        direct_compute_pct_holdings_changed,
        reference_date,
    )
    
    if direct_rows:
        row = direct_rows[0]
        for key in ["direction", "role_weight", "planned_discount", "final_weight"]:
            if key in expected:
                assert math.isclose(row[key], expected[key], rel_tol=1e-9, abs_tol=1e-9)


def test_senate_html_parse_to_score_parity():
    fixture = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "vertical_slice" / "congress_ptr_sample.html"
    expected = _load_parity_fixture("congress_flow")
    
    direct_connector = DirectSenateConnector(cache_dir=fixture.parent.parent)
    direct_rows = direct_connector.parse_ptr_transactions(fixture)

    assert len(direct_rows) == 2
    assert direct_rows[0].ticker == "AAPL"
    assert direct_rows[1].ticker == "MSFT"

    reference_date = datetime(2026, 4, 2)
    direct_scored = [
        direct_congress_score_transaction(
            member_id="fixture",
            ticker=row.ticker,
            transaction_type=row.transaction_type.lower().replace(" (partial)", "_partial"),
            execution_date=row.transaction_date,
            amount_min=1001 if row.ticker == "AAPL" else 15001,
            amount_max=15000 if row.ticker == "AAPL" else 50000,
            owner_type=row.owner.lower(),
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=reference_date,
        )
        for row in direct_rows
    ]

    assert math.isclose(direct_scored[0].final_score, expected["final_score"], rel_tol=1e-9, abs_tol=1e-9)


def test_house_paper_ocr_text_parse_parity():
    fixture = Path(__file__).resolve().parent / "fixtures" / "house_paper_ocr_sample.txt"
    text = fixture.read_text()

    direct_parser = PaperHouseFilingParser()
    direct_filing = direct_parser.parse_ocr_text(text, Path("8229999.pdf"), 1)

    assert len(direct_filing.transactions) == 2
    assert direct_filing.transactions[0].ticker == "AAPL"
    assert direct_filing.transactions[1].ticker == "MSFT"

    reference_date = datetime(2026, 4, 2)
    direct_scored = [
        direct_congress_score_transaction(
            member_id="fixture",
            ticker=row.ticker,
            transaction_type=row.transaction_type,
            execution_date=row.transaction_date,
            amount_min=row.amount_min,
            amount_max=row.amount_max,
            owner_type=row.owner,
            resolution_confidence=1.0,
            signal_weight=1.0,
            reference_date=reference_date,
        )
        for row in direct_filing.transactions
    ]
    direct_agg = direct_compute_aggregate(direct_scored)
    assert direct_agg.unique_members == 1
    assert direct_agg.transactions_included == 2


@pytest.mark.skipif(not Path("/opt/homebrew/bin/tesseract").exists() or not Path("/opt/homebrew/bin/pdftoppm").exists(), reason="OCR tools not installed")
def test_house_scanned_pdf_native_ocr_path(tmp_path):
    pdf_path = tmp_path / "8229999.pdf"
    img = Image.new("RGB", (2200, 2800), "white")
    draw = ImageDraw.Draw(img)
    for candidate in ["Arial.ttf", "DejaVuSans.ttf"]:
        try:
            font = ImageFont.truetype(candidate, 56)
            break
        except Exception:
            font = ImageFont.load_default()
    lines = [
        "NAME: JOHN DOE MEMBER",
        "Filing ID: 8229999",
        "SP | Apple Inc - AAPL P 03/01/2026 03/02/2026 x A",
        "Self | Microsoft Corp - MSFT S 03/03/2026 03/04/2026 x C",
    ]
    y = 120
    for line in lines:
        draw.text((120, y), line, fill="black", font=font)
        y += 120
    img.save(pdf_path, "PDF", resolution=300.0)

    filing, skip_reason = parse_house_pdf_text_only(pdf_path)
    assert filing is not None
    assert skip_reason is None
    assert len(filing.transactions) >= 1
    assert any(txn.asset_name for txn in filing.transactions)
