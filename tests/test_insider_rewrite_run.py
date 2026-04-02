from __future__ import annotations

import json
from argparse import Namespace

from signals.cli import cmd_insider_rewrite_run


def test_insider_rewrite_run_outputs_combined_payload(monkeypatch, capsys):
    monkeypatch.setattr(
        "signals.cli.ingest_universe_direct",
        lambda **kwargs: {
            "companies_processed": 1,
            "total_new_filings": 2,
            "cache_dir": "/tmp/cache",
            "filings_dir": "/tmp/cache/filings",
        },
    )

    class Result:
        run_id = "run-1"
        xml_count = 2
        imported_normalized_count = 3
        imported_result_count = 4

        def to_dict(self):
            return {
                "run_id": self.run_id,
                "xml_count": self.xml_count,
                "imported_normalized_count": self.imported_normalized_count,
                "imported_result_count": self.imported_result_count,
                "xml_dir": "/tmp/cache/filings",
            }

    monkeypatch.setattr("signals.cli.run_direct_xml_into_derived", lambda **kwargs: Result())
    args = Namespace(
        csv="/tmp/u.csv",
        sec_user_agent="DisclosureSignals/1.0 (test@example.com)",
        cache_dir="/tmp/cache",
        max_filings=2,
        start_date=None,
        end_date=None,
        date="2026-04-02",
        db="/tmp/derived.db",
        format="json",
    )
    cmd_insider_rewrite_run(args)
    payload = json.loads(capsys.readouterr().out)
    assert payload["ingest"]["total_new_filings"] == 2
    assert payload["score"]["imported_result_count"] == 4
