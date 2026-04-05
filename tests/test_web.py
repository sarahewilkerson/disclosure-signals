"""Tests for the FastAPI web dashboard."""

from __future__ import annotations

import os
import tempfile

from fastapi.testclient import TestClient

from signals.core.derived_db import init_db, get_connection, insert_signal_result, insert_run
from signals.core.dto import SignalResult
from signals.core.runs import make_run


def _setup_test_db() -> str:
    """Create a temporary DB with test data."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = tmp.name
    tmp.close()
    init_db(db_path)
    run = make_run("test", "insider", "test", {}, {})
    with get_connection(db_path) as conn:
        insert_run(conn, run)
        insert_signal_result(conn, SignalResult(
            source="insider", scope="entity", subject_key="entity:aapl",
            score=0.5, label="bullish", confidence=0.8,
            as_of_date="2026-04-05", lookback_window=90,
            input_count=3, included_count=3, excluded_count=0,
            explanation="test signal", method_version="test", code_version="test",
            run_id=run.run_id, provenance_refs={},
        ), "fp-test")
    return db_path


def test_index_returns_html():
    """GET / should return HTML with the daily brief."""
    db_path = _setup_test_db()
    os.environ["SIGNALS_DB_PATH"] = db_path

    from signals.web.app import app
    client = TestClient(app)
    response = client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Market Intelligence Brief" in response.text
    assert "<h1>" in response.text

    os.unlink(db_path)


def test_api_brief_returns_json():
    """GET /api/brief should return JSON with expected keys."""
    db_path = _setup_test_db()
    os.environ["SIGNALS_DB_PATH"] = db_path

    import importlib
    import signals.web.app as web_mod
    importlib.reload(web_mod)
    client = TestClient(web_mod.app)
    response = client.get("/api/brief")

    assert response.status_code == 200
    data = response.json()
    assert "as_of_date" in data
    assert "cluster_buy_alerts" in data
    assert "strong_insider_buys" in data
    assert "stats" in data
    assert "participation_index" in data

    os.unlink(db_path)


def test_api_signals_returns_filtered():
    """GET /api/signals with filters should return matching results."""
    db_path = _setup_test_db()
    os.environ["SIGNALS_DB_PATH"] = db_path

    import importlib
    import signals.web.app as web_mod
    importlib.reload(web_mod)
    client = TestClient(web_mod.app)

    # Unfiltered
    response = client.get("/api/signals")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] >= 1

    # Filtered by source
    response = client.get("/api/signals?source=insider&label=bullish")
    data = response.json()
    assert all(s["source"] == "insider" for s in data["signals"])

    os.unlink(db_path)
