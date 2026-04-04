from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from signals.congress.direct_service import run_direct_house_pdfs_into_derived
from signals.congress.house_parser import PaperHouseFilingParser, parse_house_pdf
from signals.congress.ocr import OCRResult, ocr_pdf
from signals.core.derived_db import get_connection


def test_direct_house_rewrite_persists_rows(tmp_path, monkeypatch):
    pdf_dir = tmp_path / "pdfs"
    pdf_dir.mkdir()
    (pdf_dir / "20030001.pdf").write_bytes(b"%PDF-1.4 fake")

    filing = SimpleNamespace(
        filing_id="20030001",
        filer_name="Hon. Test Member",
        transactions=[
            SimpleNamespace(
                owner="self",
                asset_name="Apple Inc. - Common Stock",
                ticker="AAPL",
                asset_type="ST",
                transaction_type="purchase",
                transaction_date=datetime(2026, 3, 1),
                notification_date=datetime(2026, 3, 2),
                amount_min=15001,
                amount_max=50000,
                page_number=1,
                raw_line="Apple Inc. - Common Stock (AAPL) P 03/01/2026 03/02/2026 $15,001 - $50,000",
            )
        ],
        parse_errors=[],
    )

    monkeypatch.setattr(
        "signals.congress.direct_service.parse_house_pdf_text_only",
        lambda repo_root, pdf_path: (filing, None),
    )

    db_path = tmp_path / "derived.db"
    result = run_direct_house_pdfs_into_derived(
        repo_root=Path(__file__).resolve().parents[1],
        derived_db_path=str(db_path),
        pdf_dir=str(pdf_dir),
        reference_date=datetime(2026, 4, 2),
        window_days=90,
        max_files=None,
    )

    assert result.pdf_count == 1
    assert result.imported_normalized_count == 1
    assert result.imported_result_count == 1
    assert result.skip_reasons == {}


def test_direct_house_rewrite_marks_treasuries_as_non_signal(tmp_path, monkeypatch):
    pdf_dir = tmp_path / "pdfs"
    pdf_dir.mkdir()
    (pdf_dir / "20030002.pdf").write_bytes(b"%PDF-1.4 fake")

    filing = SimpleNamespace(
        filing_id="20030002",
        filer_name="Hon. Test Member",
        transactions=[
            SimpleNamespace(
                owner="self",
                asset_name="US Treasury Note 4% DUE 7/31/29",
                ticker=None,
                asset_type="GS",
                transaction_type="purchase",
                transaction_date=datetime(2026, 3, 1),
                notification_date=datetime(2026, 3, 2),
                amount_min=50001,
                amount_max=100000,
                page_number=1,
                raw_line="US Treasury Note 4% DUE 7/31/29 P 03/01/2026 03/02/2026 $50,001 - $100,000",
            )
        ],
        parse_errors=[],
    )

    monkeypatch.setattr(
        "signals.congress.direct_service.parse_house_pdf_text_only",
        lambda repo_root, pdf_path: (filing, None),
    )

    db_path = tmp_path / "derived.db"
    result = run_direct_house_pdfs_into_derived(
        repo_root=Path(__file__).resolve().parents[1],
        derived_db_path=str(db_path),
        pdf_dir=str(pdf_dir),
        reference_date=datetime(2026, 4, 2),
        window_days=90,
        max_files=None,
    )

    assert result.imported_normalized_count == 1
    assert result.imported_result_count == 0
    with get_connection(str(db_path)) as conn:
        reason = conn.execute(
            "select exclusion_reason_code from normalized_transactions where source_filing_id = ?",
            ("20030002",),
        ).fetchone()[0]
    assert reason == "NON_SIGNAL_ASSET"


def test_house_parser_classifies_nothing_to_report(tmp_path, monkeypatch):
    pdf_path = tmp_path / "8221310.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake")

    class FakeOCR:
        text = "Nothing to report for December 2025"
        confidence = 0.9
        page_count = 1

        @property
        def is_successful(self):
            return True

    monkeypatch.setattr("signals.congress.house_parser.pdf_has_extractable_text", lambda path: False)
    monkeypatch.setattr("signals.congress.house_parser.is_tesseract_available", lambda: True)
    monkeypatch.setattr("signals.congress.house_parser.ocr_pdf", lambda path: FakeOCR())

    filing, reason = parse_house_pdf(pdf_path)

    assert filing is not None
    assert filing.transactions == []
    assert reason == "nothing_to_report"


def test_ocr_pdf_caches_result(tmp_path, monkeypatch):
    pdf_path = tmp_path / "8221310.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake pdf")
    cache_dir = tmp_path / "ocr-cache"
    calls = {"count": 0}

    def fake_uncached(path, language="eng", dpi=300):
        del path, language, dpi
        calls["count"] += 1
        return OCRResult(
            text="Purchase $1,001 03/01/2026",
            confidence=0.95,
            source_format="pdf",
            page_count=1,
            warnings=[],
        )

    monkeypatch.setattr("signals.congress.ocr._ocr_pdf_uncached", fake_uncached)

    first = ocr_pdf(pdf_path, cache_dir=cache_dir)
    second = ocr_pdf(pdf_path, cache_dir=cache_dir)

    assert calls["count"] == 1
    assert first.text == second.text
    assert list(cache_dir.glob("*.json"))


def test_house_parser_reuses_cached_ocr_result(tmp_path, monkeypatch):
    pdf_path = tmp_path / "8221310.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake pdf")
    calls = {"count": 0}

    def fake_uncached(path, language="eng", dpi=300):
        del path, language, dpi
        calls["count"] += 1
        return OCRResult(
            text="Nothing to report for December 2025. This filing contains no reportable transactions or assets.",
            confidence=0.9,
            source_format="pdf",
            page_count=1,
            warnings=[],
        )

    monkeypatch.setattr("signals.congress.house_parser.pdf_has_extractable_text", lambda path: False)
    monkeypatch.setattr("signals.congress.house_parser.is_tesseract_available", lambda: True)
    monkeypatch.setattr("signals.congress.ocr._ocr_pdf_uncached", fake_uncached)

    first_filing, first_reason = parse_house_pdf(pdf_path)
    second_filing, second_reason = parse_house_pdf(pdf_path)

    assert calls["count"] == 1
    assert first_reason == "nothing_to_report"
    assert second_reason == "nothing_to_report"
    assert first_filing is not None
    assert second_filing is not None


def test_house_parser_parses_amendment_letter_line():
    parser = PaperHouseFilingParser()
    filing = parser.parse_ocr_text(
        "Dynatrace, Inc. $1,0001 - Sale 03/27/2024 Spouse\n"
        "Common Stock | $15,000\n"
        "Humana Inc. $1,0001 - Sale 03/27/2024 Self\n"
        "Common Stock | $15,000\n"
        "Synopsys, Inc. $1,0001 - Purchase 03/27/2024 Self\n"
        "Common Stock | $15,000",
        Path("8221334.pdf"),
        1,
    )

    assert [txn.asset_name for txn in filing.transactions] == [
        "Dynatrace, Inc.",
        "Humana Inc.",
        "Synopsys, Inc.",
    ]
    assert [txn.transaction_type for txn in filing.transactions] == [
        "sale",
        "sale",
        "purchase",
    ]
    assert [txn.amount_min for txn in filing.transactions] == [1001, 1001, 1001]
