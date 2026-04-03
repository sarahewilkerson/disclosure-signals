from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from signals.congress.direct_service import run_direct_house_pdfs_into_derived
from signals.core.derived_db import get_connection


def test_house_direct_service_persists_run_rows_and_skip_reasons(tmp_path, monkeypatch):
    pdf_dir = tmp_path / "pdfs"
    pdf_dir.mkdir()
    for name in ["good.pdf", "treasury.pdf", "noise.pdf", "nothing.pdf", "ocr_failed.pdf"]:
        (pdf_dir / name).write_bytes(b"%PDF-1.4 fake")

    def fake_parse(repo_root, pdf_path):
        del repo_root
        if pdf_path.name == "ocr_failed.pdf":
            return None, "ocr_failed"
        if pdf_path.name == "nothing.pdf":
            return SimpleNamespace(
                filing_id="nothing",
                filer_name="Hon. Nobody",
                transactions=[],
                parse_errors=[],
            ), "nothing_to_report"
        if pdf_path.name == "good.pdf":
            return SimpleNamespace(
                filing_id="good",
                filer_name="Hon. Good",
                transactions=[
                    SimpleNamespace(
                        owner="self",
                        asset_name="Walmart Inc CMN",
                        ticker=None,
                        asset_type=None,
                        transaction_type="purchase",
                        transaction_date=datetime(2026, 3, 1),
                        notification_date=datetime(2026, 3, 2),
                        amount_min=1001,
                        amount_max=15000,
                        page_number=1,
                        raw_line="Walmart Inc CMN x 03/01/26 03/02/26 x",
                    )
                ],
                parse_errors=[],
            ), None
        if pdf_path.name == "treasury.pdf":
            return SimpleNamespace(
                filing_id="treasury",
                filer_name="Hon. Bond",
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
            ), None
        return SimpleNamespace(
            filing_id="noise",
            filer_name="Hon. Noise",
            transactions=[
                SimpleNamespace(
                    owner="self",
                    asset_name="pc _ | USD",
                    ticker=None,
                    asset_type=None,
                    transaction_type="purchase",
                    transaction_date=datetime(2026, 3, 1),
                    notification_date=datetime(2026, 3, 2),
                    amount_min=1001,
                    amount_max=15000,
                    page_number=1,
                    raw_line="pc _ | USD x } 12-16-2025 - 01/04/26 x",
                )
            ],
            parse_errors=[],
        ), None

    monkeypatch.setattr("signals.congress.direct_service.parse_house_pdf_text_only", fake_parse)

    db_path = tmp_path / "derived.db"
    result = run_direct_house_pdfs_into_derived(
        repo_root=Path(__file__).resolve().parents[1],
        derived_db_path=str(db_path),
        pdf_dir=str(pdf_dir),
        reference_date=datetime(2026, 4, 2),
        window_days=90,
        max_files=None,
    )

    assert result.pdf_count == 5
    assert result.imported_normalized_count == 3
    assert result.imported_result_count == 1
    assert result.skipped_count == 2
    assert result.skip_reasons == {"nothing_to_report": 1, "ocr_failed": 1}

    with get_connection(str(db_path)) as conn:
        runs = conn.execute("select run_type, status from runs").fetchall()
        normalized = conn.execute(
            "select source_filing_id, exclusion_reason_code, include_in_signal from normalized_transactions order by source_filing_id"
        ).fetchall()
        results = conn.execute("select subject_key from signal_results order by subject_key").fetchall()

    assert len(runs) == 1
    assert runs[0]["run_type"] == "direct_house_score"
    assert runs[0]["status"] == "SUCCEEDED"
    assert [(row["source_filing_id"], row["exclusion_reason_code"], row["include_in_signal"]) for row in normalized] == [
        ("good", None, 1),
        ("noise", "NON_SIGNAL_ASSET", 0),
        ("treasury", "NON_SIGNAL_ASSET", 0),
    ]
    assert [row["subject_key"] for row in results] == ["entity:wmt"]
