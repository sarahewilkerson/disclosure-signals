from __future__ import annotations

from datetime import datetime
from pathlib import Path

from signals.core.pipeline import run_direct_pipeline


class _Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        return dict(self.__dict__)


def _signal_row(source: str, subject_key: str, score: float) -> dict:
    return {
        "source": source,
        "scope": "entity",
        "subject_key": subject_key,
        "score": score,
        "label": "bullish" if score > 0 else "bearish",
        "confidence": 0.9,
        "as_of_date": "2026-04-02",
        "lookback_window": 90,
        "input_count": 1,
        "included_count": 1,
        "excluded_count": 0,
        "explanation": "test",
        "method_version": "test",
        "code_version": "test",
        "run_id": f"{source}-run",
        "provenance_refs": {},
    }


def test_run_direct_pipeline_composes_direct_flows(tmp_path, monkeypatch):
    repo_root = Path(__file__).resolve().parents[1]

    monkeypatch.setattr(
        "signals.core.pipeline.ingest_universe_direct",
        lambda **kwargs: {
            "companies_processed": 1,
            "total_new_filings": 2,
            "cache_dir": "/tmp/insider-cache",
            "filings_dir": "/tmp/insider-cache/filings",
        },
    )
    monkeypatch.setattr(
        "signals.core.pipeline.run_direct_xml_into_derived",
        lambda **kwargs: _Obj(run_id="insider-run", imported_result_count=3, imported_normalized_count=4),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.ingest_house_ptrs_direct",
        lambda **kwargs: _Obj(ptr_count=2, downloaded_count=2, skipped_cached_count=0, failed_count=0, cache_dir="/tmp/congress", pdf_dir="/tmp/congress/pdfs/house"),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.run_direct_house_pdfs_into_derived",
        lambda **kwargs: _Obj(
            run_id="house-run",
            imported_result_count=5,
            imported_normalized_count=6,
            skipped_count=0,
            skip_reasons={},
            to_dict=lambda: {
                "run_id": "house-run",
                "imported_result_count": 5,
                "imported_normalized_count": 6,
                "skipped_count": 0,
                "skip_reasons": {},
            },
        ),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.ingest_senate_ptrs_direct",
        lambda **kwargs: _Obj(searched_count=2, downloaded_ptr_count=2, skipped_paper_count=0, failed_count=0, cache_dir="/tmp/congress", html_dir="/tmp/congress/pdfs/senate"),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.run_direct_senate_html_into_derived",
        lambda **kwargs: _Obj(run_id="senate-run", imported_result_count=7, imported_normalized_count=8),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.build_from_derived",
        lambda *args, **kwargs: _Obj(run_id="combined-run", combined_count=1, blocked_rows=[], to_dict=lambda: {"run_id": "combined-run", "combined_count": 1, "blocked_count": 0, "blocked_rows": [], "lookback_window": 90}),
    )
    monkeypatch.setattr("signals.core.pipeline.init_db", lambda path: None)
    monkeypatch.setattr(
        "signals.core.pipeline.build_house_quality_metrics",
        lambda conn, run_id, skipped_count, skip_reasons: {
            "run_id": run_id,
            "normalized_count": 6,
            "scored_result_count": 5,
            "skipped_count": skipped_count,
            "skip_reasons": skip_reasons,
        },
    )
    monkeypatch.setattr(
        "signals.core.pipeline.build_insider_candidate_discovery",
        lambda conn, run_id: {
            "run_id": run_id,
            "candidate_count": 1,
            "candidates": [{"normalized_name": "unknown issuer", "count": 1, "raw_examples": ["Unknown Issuer"], "filing_ids": ["f1"], "instrument_types": {"Common Stock": 1}, "reason_codes": {"<included>": 1}}],
        },
    )
    monkeypatch.setattr(
        "signals.core.pipeline.build_house_candidate_discovery",
        lambda conn, run_id: {
            "run_id": run_id,
            "candidate_count": 1,
            "candidates": [{"normalized_name": "dirty ocr common stock", "count": 1, "asset_categories": {"common_stock": 1}, "raw_examples": ["Dirty OCR Common Stock"], "reason_codes": {"MISSING_TICKER": 1}}],
        },
    )

    class _ConnCtx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=()):
            class _Row:
                def __getitem__(self, key):
                    return 0
            class _Cursor:
                def fetchall(self_inner):
                    return []
                def fetchone(self_inner):
                    return _Row()
            return _Cursor()

    monkeypatch.setattr("signals.core.pipeline.get_connection", lambda path: _ConnCtx())
    monkeypatch.setattr(
        "signals.core.pipeline.build_source_report",
        lambda conn, source, run_id=None, run_ids=None: (
            "text",
            {"source_results": [_signal_row("insider", "entity:aapl", 0.4)] if source == "insider" else [_signal_row("congress", "entity:aapl", 0.3)]},
        ),
    )
    monkeypatch.setattr("signals.core.pipeline.build_combined_report", lambda conn, run_id=None, blocked=None: ("text", {"combined_results": [1]}))

    result = run_direct_pipeline(
        repo_root=repo_root,
        derived_db_path=str(tmp_path / "derived.db"),
        insider_csv_path="u.csv",
        insider_user_agent="DisclosureSignals/1.0 (test@example.com)",
        insider_cache_dir=str(tmp_path / "insider-cache"),
        congress_cache_dir=str(tmp_path / "congress-cache"),
        reference_date=datetime(2026, 4, 2),
        lookback_window=90,
        insider_max_filings=2,
        house_days=90,
        house_max_filings=2,
        senate_days=365,
        senate_max_filings=2,
        artifact_dir=None,
    )

    assert result.insider["score"]["imported_result_count"] == 3
    assert result.insider["candidate_discovery"]["candidate_count"] == 1
    assert result.congress["imported_result_count"] == 12
    assert result.combined["combined_count"] == 1
    assert result.congress["house_quality_metrics"]["run_id"] == "house-run"
    assert result.congress["house_candidate_discovery"]["candidate_count"] == 1


def test_run_direct_pipeline_writes_house_quality_artifact(tmp_path, monkeypatch):
    repo_root = Path(__file__).resolve().parents[1]

    monkeypatch.setattr("signals.core.pipeline.ingest_universe_direct", lambda **kwargs: {"filings_dir": "/tmp/insider", "companies_processed": 1, "total_new_filings": 1, "cache_dir": "/tmp/insider"})
    monkeypatch.setattr("signals.core.pipeline.run_direct_xml_into_derived", lambda **kwargs: _Obj(run_id="insider-run", imported_result_count=1, imported_normalized_count=1))
    monkeypatch.setattr(
        "signals.core.pipeline.ingest_house_ptrs_direct",
        lambda **kwargs: _Obj(ptr_count=2, downloaded_count=2, skipped_cached_count=0, failed_count=0, cache_dir="/tmp/congress", pdf_dir="/tmp/congress/pdfs/house"),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.run_direct_house_pdfs_into_derived",
        lambda **kwargs: _Obj(run_id="house-run", imported_result_count=2, imported_normalized_count=3, skipped_count=1, skip_reasons={"nothing_to_report": 1}, to_dict=lambda: {"run_id": "house-run", "imported_result_count": 2, "imported_normalized_count": 3, "skipped_count": 1, "skip_reasons": {"nothing_to_report": 1}}),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.ingest_senate_ptrs_direct",
        lambda **kwargs: _Obj(searched_count=1, downloaded_ptr_count=1, skipped_paper_count=0, failed_count=0, cache_dir="/tmp/congress", html_dir="/tmp/congress/pdfs/senate"),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.run_direct_senate_html_into_derived",
        lambda **kwargs: _Obj(run_id="senate-run", imported_result_count=1, imported_normalized_count=1, to_dict=lambda: {"run_id": "senate-run", "imported_result_count": 1, "imported_normalized_count": 1}),
    )
    monkeypatch.setattr(
        "signals.core.pipeline.build_from_derived",
        lambda *args, **kwargs: _Obj(run_id="combined-run", combined_count=0, blocked_rows=[], to_dict=lambda: {"run_id": "combined-run", "combined_count": 0, "blocked_count": 0, "blocked_rows": [], "lookback_window": 90}),
    )
    monkeypatch.setattr("signals.core.pipeline.init_db", lambda path: None)
    monkeypatch.setattr(
        "signals.core.pipeline.build_house_quality_metrics",
        lambda conn, run_id, skipped_count, skip_reasons: {
            "run_id": run_id,
            "normalized_count": 3,
            "scored_result_count": 2,
            "included_count": 2,
            "unresolved_count": 1,
            "resolved_entity_count": 2,
            "scored_signal_rate": 0.6667,
            "resolved_entity_rate": 0.6667,
            "included_rate": 0.6667,
            "skipped_count": skipped_count,
            "skip_reasons": skip_reasons,
            "exclusion_reason_counts": {"NON_SIGNAL_ASSET": 1},
            "asset_category_counts": {"common_stock": 2, "unknown": 1},
            "unresolved_asset_category_counts": {"unknown": 1},
            "non_signal_asset_category_counts": {"unknown": 1},
            "top_unresolved_issuers": [{"issuer_name": "Noise Asset", "count": 1}],
            "top_signal_like_unresolved_issuers": [{"issuer_name": "Dirty OCR Common Stock", "count": 1}],
            "top_non_signal_unresolved_issuers": [{"issuer_name": "Noise Asset", "count": 1}],
            "top_recovered_issuers": [{"issuer_name": "Walmart Inc.", "count": 2}],
            "top_scored_subjects": [{"subject_key": "entity:wmt", "label": "bullish", "score": 1.0, "confidence": 0.5, "input_count": 2}],
        },
    )
    monkeypatch.setattr(
        "signals.core.pipeline.build_insider_candidate_discovery",
        lambda conn, run_id: {
            "run_id": run_id,
            "candidate_count": 0,
            "candidates": [],
        },
    )
    monkeypatch.setattr(
        "signals.core.pipeline.build_house_candidate_discovery",
        lambda conn, run_id: {
            "run_id": run_id,
            "candidate_count": 1,
            "candidates": [{"normalized_name": "dirty ocr common stock", "count": 1, "asset_categories": {"common_stock": 1}, "raw_examples": ["Dirty OCR Common Stock"], "reason_codes": {"MISSING_TICKER": 1}}],
        },
    )

    class _ConnCtx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=()):
            class _Row:
                def __getitem__(self, key):
                    return 0

            class _Cursor:
                def fetchall(self_inner):
                    return []

                def fetchone(self_inner):
                    return _Row()

            return _Cursor()

    monkeypatch.setattr("signals.core.pipeline.get_connection", lambda path: _ConnCtx())
    monkeypatch.setattr(
        "signals.core.pipeline.build_source_report",
        lambda conn, source, run_id=None, run_ids=None: ("text", {"source_results": []}),
    )
    monkeypatch.setattr("signals.core.pipeline.build_combined_report", lambda conn, run_id=None, blocked=None: ("text", {"combined_results": []}))

    artifacts = tmp_path / "artifacts"
    result = run_direct_pipeline(
        repo_root=repo_root,
        derived_db_path=str(tmp_path / "derived.db"),
        insider_csv_path="u.csv",
        insider_user_agent="DisclosureSignals/1.0 (test@example.com)",
        insider_cache_dir=str(tmp_path / "insider-cache"),
        congress_cache_dir=str(tmp_path / "congress-cache"),
        reference_date=datetime(2026, 4, 2),
        lookback_window=90,
        artifact_dir=artifacts,
    )

    assert result.artifact_paths["house_quality_metrics"].endswith("house_quality_metrics.json")
    assert result.artifact_paths["insider_candidate_discovery"].endswith("insider_candidate_discovery.json")
    assert result.artifact_paths["house_candidate_discovery"].endswith("house_candidate_discovery.json")
    assert (artifacts / "insider_candidate_discovery.json").exists()
    assert (artifacts / "house_quality_metrics.json").exists()
    assert (artifacts / "house_candidate_discovery.json").exists()
    assert result.congress["house_quality_metrics"]["top_signal_like_unresolved_issuers"] == [
        {"issuer_name": "Dirty OCR Common Stock", "count": 1}
    ]
    assert result.congress["house_candidate_discovery"]["candidates"] == [
        {
            "normalized_name": "dirty ocr common stock",
            "count": 1,
            "asset_categories": {"common_stock": 1},
            "raw_examples": ["Dirty OCR Common Stock"],
            "reason_codes": {"MISSING_TICKER": 1},
        }
    ]
