from __future__ import annotations

import json
from argparse import Namespace

from signals.cli import cmd_congress_rewrite_run_house


def test_congress_rewrite_run_outputs_combined_payload(monkeypatch, capsys):
    class IngestResult:
        ptr_count = 3
        downloaded_count = 2
        skipped_cached_count = 1
        failed_count = 0
        cache_dir = "/tmp/cache"
        pdf_dir = "/tmp/cache/pdfs/house"
        years = [2026]

        def to_dict(self):
            return {
                "ptr_count": self.ptr_count,
                "downloaded_count": self.downloaded_count,
                "skipped_cached_count": self.skipped_cached_count,
                "failed_count": self.failed_count,
                "cache_dir": self.cache_dir,
                "pdf_dir": self.pdf_dir,
                "years": self.years,
            }

    class ScoreResult:
        run_id = "run-2"
        pdf_count = 2
        imported_normalized_count = 10
        imported_result_count = 4
        skipped_count = 0
        pdf_dir = "/tmp/cache/pdfs/house"

        def to_dict(self):
            return {
                "run_id": self.run_id,
                "pdf_count": self.pdf_count,
                "imported_normalized_count": self.imported_normalized_count,
                "imported_result_count": self.imported_result_count,
                "skipped_count": self.skipped_count,
                "pdf_dir": self.pdf_dir,
            }

    monkeypatch.setattr("signals.cli.ingest_house_ptrs_direct", lambda **kwargs: IngestResult())
    monkeypatch.setattr("signals.cli.run_direct_house_pdfs_into_derived", lambda **kwargs: ScoreResult())

    args = Namespace(
        cache_dir="/tmp/cache",
        days=90,
        max_filings=5,
        force=False,
        date="2026-04-02",
        window=90,
        score_max_files=2,
        db="/tmp/derived.db",
        format="json",
    )
    cmd_congress_rewrite_run_house(args)
    payload = json.loads(capsys.readouterr().out)
    assert payload["ingest"]["downloaded_count"] == 2
    assert payload["score"]["imported_result_count"] == 4
