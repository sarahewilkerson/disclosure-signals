from pathlib import Path

from signals.core.vertical_slice import (
    build_combined_fixture,
    run_congress_fixture,
    run_insider_fixture,
    run_vertical_slice,
)


def test_vertical_slice_runs(tmp_path):
    fixture_dir = Path(__file__).parent / "fixtures" / "vertical_slice"
    db_path = tmp_path / "derived.db"
    result = run_vertical_slice(
        repo_root=Path(__file__).resolve().parents[1],
        db_path=str(db_path),
        fixture_dir=fixture_dir,
        artifact_dir=tmp_path / "artifacts",
    )
    assert len(result.runs) == 3
    assert len(result.normalized) == 3
    assert len(result.combined_results) == 1
    assert result.blocked_combined[0]["reason_code"] == "MISSING_COUNTERPART"
    assert Path(result.artifact_paths["parity_report"]).exists()
    assert Path(result.artifact_paths["report_text"]).exists()


def test_vertical_slice_parity(tmp_path):
    fixture_dir = Path(__file__).parent / "fixtures" / "vertical_slice"
    db_path = tmp_path / "derived.db"
    result = run_vertical_slice(
        repo_root=Path(__file__).resolve().parents[1],
        db_path=str(db_path),
        fixture_dir=fixture_dir,
        artifact_dir=tmp_path / "artifacts",
    )
    assert result.parity["structural_ok"] is True
    assert result.parity["analytical_ok"] is True
    assert result.parity["reporting_ok"] is True


def test_vertical_slice_rerun_is_idempotent(tmp_path):
    fixture_dir = Path(__file__).parent / "fixtures" / "vertical_slice"
    db_path = tmp_path / "derived.db"
    first = run_vertical_slice(
        repo_root=Path(__file__).resolve().parents[1],
        db_path=str(db_path),
        fixture_dir=fixture_dir,
        artifact_dir=tmp_path / "artifacts",
    )
    second = run_vertical_slice(
        repo_root=Path(__file__).resolve().parents[1],
        db_path=str(db_path),
        fixture_dir=fixture_dir,
        artifact_dir=tmp_path / "artifacts",
    )
    assert len(first.normalized) == len(second.normalized) == 3
    assert len(first.source_results) == len(second.source_results) == 3
    assert len(first.combined_results) == len(second.combined_results) == 1


def test_domain_fixture_commands(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    fixture_dir = Path(__file__).parent / "fixtures" / "vertical_slice"
    db_path = tmp_path / "derived.db"
    insider = run_insider_fixture(repo_root, str(db_path), fixture_dir, tmp_path / "insider-artifacts")
    congress = run_congress_fixture(repo_root, str(db_path), fixture_dir, tmp_path / "congress-artifacts")
    combined = build_combined_fixture(repo_root, str(db_path), tmp_path / "combined-artifacts")
    assert insider.source == "insider"
    assert congress.source == "congress"
    assert combined.source == "combined"
    assert len(insider.source_results) == 1
    assert len(congress.source_results) == 2
    assert len(combined.blocked_combined) == 1


def test_persisted_reports_read_from_db(tmp_path):
    import json
    import os
    import subprocess
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    db_path = tmp_path / "derived.db"
    artifacts = tmp_path / "artifacts"

    subprocess.run(
        [str(repo_root / ".venv" / "bin" / "python"), "-m", "signals.cli", "--db", str(db_path), "--artifacts-dir", str(artifacts), "slice", "run"],
        cwd=repo_root,
        env={**os.environ},
        check=True,
        capture_output=True,
        text=True,
    )

    insider_report = subprocess.run(
        [str(repo_root / ".venv" / "bin" / "python"), "-m", "signals.cli", "--db", str(db_path), "--format", "json", "insider", "report"],
        cwd=repo_root,
        env={**os.environ},
        check=True,
        capture_output=True,
        text=True,
    )
    combined_report = subprocess.run(
        [str(repo_root / ".venv" / "bin" / "python"), "-m", "signals.cli", "--db", str(db_path), "--format", "json", "combined", "report"],
        cwd=repo_root,
        env={**os.environ},
        check=True,
        capture_output=True,
        text=True,
    )
    insider_payload = json.loads(insider_report.stdout)
    combined_payload = json.loads(combined_report.stdout)
    assert len(insider_payload["source_results"]) == 1
    assert insider_payload["source_results"][0]["subject_key"] == "entity:aapl"
    assert len(combined_payload["combined_results"]) == 1
    assert combined_payload["combined_results"][0]["subject_key"] == "entity:aapl"
