from __future__ import annotations

from pathlib import Path

from signals.congress.ingest import ingest_house_ptrs_direct


def test_direct_house_ingest_uses_downloaded_ptr_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "signals.congress.ingest._download_fd_xml_ptrs",
        lambda years, cache_dir: [
            {"doc_id": "20030001", "filing_date": "2026-03-15", "year": 2026, "name": "Test Member", "state_district": "CA12"},
            {"doc_id": "20030002", "filing_date": "2026-03-16", "year": 2026, "name": "Test Member 2", "state_district": "CA13"},
        ],
    )

    class FakeConnector:
        def __init__(self, cache_dir, request_delay):
            self.cache_dir = Path(cache_dir) / "pdfs" / "house"
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        def download_pdf(self, filing_id, year=None, force=False):
            path = self.cache_dir / f"{filing_id}.pdf"
            path.write_bytes(b"%PDF-1.4 fake")
            return path

    monkeypatch.setattr("signals.congress.ingest.HouseConnector", FakeConnector)

    result = ingest_house_ptrs_direct(
        repo_root=Path(__file__).resolve().parents[1],
        cache_dir=str(tmp_path),
        days=90,
        max_filings=1,
        force=False,
    )

    assert result.ptr_count == 1
    assert result.downloaded_count == 1
    assert result.failed_count == 0
