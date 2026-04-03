"""Tests for extracted legacy insider application services."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import tempfile

from db import init_db
from services.reporting_service import generate_reports
from services.scoring_service import compute_scores
from services.status_service import get_status


def test_status_service_on_empty_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        init_db(db_path)
        status = get_status(db_path)
        assert status.companies == 0
        assert status.filings == 0
        assert status.transactions == 0
        assert status.signal_transactions == 0
        assert status.company_scores == 0
        assert status.filing_date_oldest is None
        assert status.filing_date_latest is None
        assert status.parse_errors == 0
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_scoring_service_on_empty_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        init_db(db_path)
        result = compute_scores(datetime(2024, 6, 15), db_path)
        assert result.company_score_count == 0
        assert result.aggregate_index_count == 3
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_reporting_service_writes_outputs():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        output_dir = str(Path(td) / "out")
        init_db(db_path)
        result = generate_reports(db_path=db_path, output_dir=output_dir)
        assert Path(result.cli_report_path).exists()
        assert Path(result.html_dashboard_path).exists()
        assert "INSIDER TRADING SIGNAL ENGINE" in result.cli_report
