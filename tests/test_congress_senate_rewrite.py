from __future__ import annotations

from datetime import datetime
from pathlib import Path

from signals.congress.senate_direct import ingest_senate_ptrs_direct, run_direct_senate_html_into_derived


def test_direct_senate_ingest_downloads_only_electronic(monkeypatch, tmp_path):
    class Filing:
        def __init__(self, filing_id, is_paper):
            self.filing_id = filing_id
            self.is_paper = is_paper

    class FakeConnector:
        def __init__(self, cache_dir, request_delay):
            self.cache_dir = Path(cache_dir) / "pdfs" / "senate"
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        def download_ptr(self, filing_id, force=False):
            path = self.cache_dir / f"ptr_{filing_id[:8]}.html"
            path.write_text("<html></html>")
            return path

    monkeypatch.setattr("signals.congress.senate_direct._senate_connector_class", lambda repo_root: FakeConnector)
    monkeypatch.setattr(
        "signals.congress.senate_direct._search_senate_ptrs_live",
        lambda senate, start_date, end_date: [Filing("abc", False), Filing("def", True)],
    )
    result = ingest_senate_ptrs_direct(
        repo_root=Path(__file__).resolve().parents[1],
        cache_dir=str(tmp_path),
        days=90,
        max_filings=None,
        force=False,
    )
    assert result.searched_count == 2
    assert result.downloaded_ptr_count == 1
    assert result.skipped_paper_count == 1


def test_direct_senate_score_persists_rows(monkeypatch, tmp_path):
    html_dir = tmp_path / "pdfs" / "senate"
    html_dir.mkdir(parents=True)
    (html_dir / "ptr_abcdef12.html").write_text("<html></html>")

    class Txn:
        def __init__(self):
            self.transaction_date = datetime(2026, 3, 1)
            self.owner = "Self"
            self.ticker = "AAPL"
            self.asset_name = "Apple Inc."
            self.asset_type = "Stock"
            self.transaction_type = "Purchase"
            self.amount_range = "$1,001 - $15,000"
            self.comment = None

    class FakeConnector:
        def __init__(self, cache_dir, request_delay):
            self.cache_dir = html_dir

        def parse_ptr_transactions(self, html_path):
            return [Txn()]

    class FakeLegacyResolution:
        @staticmethod
        def resolve_transaction(asset_name, ticker=None, asset_type_code=None):
            class Result:
                resolved_ticker = "AAPL"
                resolved_company = "Apple Inc."
                resolution_method = "extracted"
                resolution_confidence = 0.99
                include_in_signal = True
                exclusion_reason = None
            return Result()

    monkeypatch.setattr("signals.congress.senate_direct._senate_connector_class", lambda repo_root: FakeConnector)
    monkeypatch.setattr("signals.congress.senate_direct._legacy_resolution_module", lambda repo_root: FakeLegacyResolution)

    db_path = tmp_path / "derived.db"
    result = run_direct_senate_html_into_derived(
        repo_root=Path(__file__).resolve().parents[1],
        derived_db_path=str(db_path),
        html_dir=str(html_dir),
        reference_date=datetime(2026, 4, 2),
        window_days=90,
        max_files=None,
    )
    assert result.html_count == 1
    assert result.imported_normalized_count == 1
    assert result.imported_result_count == 1
