from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from signals.congress.direct_service import run_direct_house_pdfs_into_derived


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
                amount_min=1001,
                amount_max=15000,
                page_number=1,
                raw_line="Apple Inc. - Common Stock (AAPL) P 03/01/2026 03/02/2026 $1,001 - $15,000",
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
