from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from signals.core.derived_db import count_rows, get_connection, init_db, insert_run, insert_signal_result, update_run_status
from signals.core.pipeline import run_direct_pipeline
from signals.core.runs import make_run, utcnow_iso
from signals.core.dto import SignalResult


@dataclass
class _Obj:
    run_id: str
    imported_result_count: int
    imported_normalized_count: int

    def to_dict(self):
        return {
            "run_id": self.run_id,
            "imported_result_count": self.imported_result_count,
            "imported_normalized_count": self.imported_normalized_count,
        }


@dataclass
class _IngestObj:
    pdf_dir: str = "/tmp/pdfs/house"
    html_dir: str = "/tmp/pdfs/senate"
    ptr_count: int = 1
    downloaded_count: int = 1
    skipped_cached_count: int = 0
    failed_count: int = 0
    searched_count: int = 1
    downloaded_ptr_count: int = 1
    skipped_paper_count: int = 0
    cache_dir: str = "/tmp/cache"

    def to_dict(self):
        return self.__dict__.copy()


def _write_source_result(db_path: str, source: str, subject_key: str, score: float, run_type: str) -> str:
    run = make_run(run_type, source, "stress-test", {}, {"score": "stress"})
    with get_connection(db_path) as conn:
        insert_run(conn, run)
        insert_signal_result(
            conn,
            SignalResult(
                source=source,
                scope="entity",
                subject_key=subject_key,
                score=score,
                label="bullish" if score > 0 else "bearish",
                confidence=0.9,
                as_of_date="2026-04-02",
                lookback_window=90,
                input_count=1,
                included_count=1,
                excluded_count=0,
                explanation=f"{source} stress result",
                method_version="stress",
                code_version="stress",
                run_id=run.run_id,
                provenance_refs={"normalized_row_ids": [str(uuid.uuid4())], "resolution_event_ids": []},
            ),
            f"{source}:{subject_key}:{run.run_id}",
        )
        update_run_status(conn, run.run_id, "SUCCEEDED", utcnow_iso(), {"score_count": 1})
    return run.run_id


def test_parallel_direct_pipeline_repeated_runs_same_db(tmp_path, monkeypatch):
    repo_root = Path(__file__).resolve().parents[1]
    db_path = tmp_path / "derived.db"
    init_db(str(db_path))

    monkeypatch.setattr(
        "signals.core.pipeline.ingest_universe_direct",
        lambda **kwargs: {
            "companies_processed": 1,
            "total_new_filings": 1,
            "cache_dir": "/tmp/insider-cache",
            "filings_dir": "/tmp/insider-cache/filings",
        },
    )
    monkeypatch.setattr("signals.core.pipeline.ingest_house_ptrs_direct", lambda **kwargs: _IngestObj())
    monkeypatch.setattr("signals.core.pipeline.ingest_senate_ptrs_direct", lambda **kwargs: _IngestObj())
    monkeypatch.setattr(
        "signals.core.pipeline.run_direct_xml_into_derived",
        lambda repo_root, derived_db_path, xml_dir, reference_date, **kwargs: _Obj(
            run_id=_write_source_result(derived_db_path, "insider", "entity:aapl", 0.4, "stress_insider"),
            imported_result_count=1,
            imported_normalized_count=0,
        ),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.run_direct_house_pdfs_into_derived",
        lambda repo_root, derived_db_path, pdf_dir, reference_date, window_days, max_files=None, **kwargs: _Obj(
            run_id=_write_source_result(derived_db_path, "congress", "entity:aapl", 0.2, "stress_house"),
            imported_result_count=1,
            imported_normalized_count=0,
        ),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.run_direct_senate_html_into_derived",
        lambda repo_root, derived_db_path, html_dir, reference_date, window_days, max_files=None, **kwargs: _Obj(
            run_id=_write_source_result(derived_db_path, "congress", "entity:msft", -0.3, "stress_senate"),
            imported_result_count=1,
            imported_normalized_count=0,
        ),
    )

    for _ in range(5):
        result = run_direct_pipeline(
            repo_root=repo_root,
            derived_db_path=str(db_path),
            insider_csv_path="ignored.csv",
            insider_user_agent="DisclosureSignals/1.0 (test@example.com)",
            insider_cache_dir=str(tmp_path / "insider"),
            congress_cache_dir=str(tmp_path / "congress"),
            reference_date=datetime(2026, 4, 2),
            lookback_window=90,
            artifact_dir=None,
        )
        assert result.congress["imported_result_count"] == 2
        assert result.insider["score"]["imported_result_count"] == 1

    with get_connection(str(db_path)) as conn:
        assert count_rows(conn, "runs") == 20
        assert count_rows(conn, "signal_results") == 15
