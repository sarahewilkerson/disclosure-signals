from __future__ import annotations

import importlib
import math
import sys
import tempfile
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


def _import_legacy_insider_modules():
    repo_root = Path(__file__).resolve().parents[1]
    legacy_root = str(repo_root / "legacy-insider")
    if legacy_root not in sys.path:
        sys.path.insert(0, legacy_root)
    return importlib.import_module("parsing"), importlib.import_module("scoring")


def _import_legacy_congress_modules():
    repo_root = Path(__file__).resolve().parents[1]
    legacy_root = str(repo_root / "legacy-congress")
    if legacy_root not in sys.path:
        sys.path.insert(0, legacy_root)
    return (
        importlib.import_module("cppi.connectors.senate"),
        importlib.import_module("cppi.scoring"),
        importlib.import_module("cppi.parsing"),
    )


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
    fixture = Path(__file__).resolve().parents[1] / "legacy-insider" / "tests" / "fixtures" / "form4_simple_buy.xml"
    legacy_parsing, legacy_scoring = _import_legacy_insider_modules()
    direct = direct_parse_form4_xml(fixture)
    legacy = legacy_parsing.parse_form4_xml(str(fixture))

    assert direct["filing"]["cik_issuer"] == legacy["filing"]["cik_issuer"]
    assert direct["filing"]["owner_name"] == legacy["filing"]["owner_name"]
    assert len(direct["transactions"]) == len(legacy["transactions"]) == 1

    reference_date = datetime(2024, 6, 15)
    direct_rows = _insider_scored_rows(
        direct,
        direct_insider_score_transaction,
        direct_classify_role,
        direct_detect_planned_trade,
        direct_compute_pct_holdings_changed,
        reference_date,
    )
    legacy_rows = _insider_scored_rows(
        legacy,
        legacy_scoring.score_transaction,
        importlib.import_module("classification").classify_role,
        importlib.import_module("classification").detect_planned_trade,
        importlib.import_module("classification").compute_pct_holdings_changed,
        reference_date,
    )

    direct_result = direct_aggregate_company_signal(direct_rows, 90)
    legacy_score, _legacy_contrib = legacy_scoring._aggregate_with_saturation(legacy_rows)
    legacy_conf = legacy_scoring._compute_confidence(
        len(legacy_rows),
        len({row["cik_owner"] for row in legacy_rows}),
        any(row["direction"] > 0 for row in legacy_rows),
        any(row["direction"] < 0 for row in legacy_rows),
    )
    legacy_signal = legacy_scoring._label_signal(legacy_score, legacy_conf)
    assert direct_result["signal"] == legacy_signal
    assert math.isclose(direct_result["score"], round(legacy_score, 4), rel_tol=1e-9, abs_tol=1e-9)
    assert math.isclose(direct_result["confidence"], round(legacy_conf, 4), rel_tol=1e-9, abs_tol=1e-9)


def test_senate_html_parse_to_score_parity():
    fixture = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "vertical_slice" / "congress_ptr_sample.html"
    legacy_senate_mod, legacy_scoring, _legacy_parsing = _import_legacy_congress_modules()
    direct_connector = DirectSenateConnector(cache_dir=fixture.parent.parent)
    legacy_connector = legacy_senate_mod.SenateConnector(cache_dir=fixture.parent.parent)

    direct_rows = direct_connector.parse_ptr_transactions(fixture)
    legacy_rows = legacy_connector.parse_ptr_transactions(fixture)

    assert len(direct_rows) == len(legacy_rows) == 2
    assert direct_rows[0].ticker == legacy_rows[0].ticker == "AAPL"
    assert direct_rows[1].ticker == legacy_rows[1].ticker == "MSFT"

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
    legacy_scored = [
        legacy_scoring.score_transaction(
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
        for row in legacy_rows
    ]

    direct_agg = direct_compute_aggregate(direct_scored)
    legacy_agg = legacy_scoring.compute_aggregate(legacy_scored)
    assert math.isclose(direct_agg.volume_net, legacy_agg.volume_net, rel_tol=1e-9, abs_tol=1e-9)
    direct_conf = direct_compute_confidence_score(direct_agg, 1.0)
    legacy_conf = legacy_scoring.compute_confidence_score(legacy_agg, 1.0)
    assert math.isclose(direct_conf["composite_score"], legacy_conf["composite_score"], rel_tol=1e-9, abs_tol=1e-9)


def test_house_paper_ocr_text_parse_parity():
    fixture = Path(__file__).resolve().parent / "fixtures" / "house_paper_ocr_sample.txt"
    _legacy_senate, legacy_scoring, legacy_parsing = _import_legacy_congress_modules()
    text = fixture.read_text()

    direct_parser = PaperHouseFilingParser()
    legacy_parser = legacy_parsing.PaperFilingParser()
    direct_filing = direct_parser.parse_ocr_text(text, Path("8229999.pdf"), 1)
    legacy_filing = legacy_parser.parse_ocr_text(text, Path("8229999.pdf"), 1)

    assert len(direct_filing.transactions) == len(legacy_filing.transactions) == 2
    assert direct_filing.transactions[0].ticker == legacy_filing.transactions[0].ticker == "AAPL"
    assert direct_filing.transactions[1].ticker == legacy_filing.transactions[1].ticker == "MSFT"

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
    legacy_scored = [
        legacy_scoring.score_transaction(
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
        for row in legacy_filing.transactions
    ]
    direct_agg = direct_compute_aggregate(direct_scored)
    legacy_agg = legacy_scoring.compute_aggregate(legacy_scored)
    assert math.isclose(direct_agg.volume_net, legacy_agg.volume_net, rel_tol=1e-9, abs_tol=1e-9)


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
