"""Tests for historical backtesting framework."""

from __future__ import annotations

from datetime import datetime

from signals.analysis.backtest import generate_backtest_dates


def test_generate_monthly_dates():
    dates = generate_backtest_dates(
        datetime(2025, 1, 1), datetime(2025, 6, 1), "monthly"
    )
    assert len(dates) == 6
    assert dates[0] == datetime(2025, 1, 1)
    assert dates[-1] == datetime(2025, 6, 1)
    # All should be 1st of month
    for d in dates:
        assert d.day == 1


def test_generate_biweekly_dates():
    dates = generate_backtest_dates(
        datetime(2025, 1, 1), datetime(2025, 2, 28), "biweekly"
    )
    assert len(dates) == 5  # Jan 1, 15, 29, Feb 12, 26
    assert (dates[1] - dates[0]).days == 14


def test_signal_stability_with_synthetic_data():
    """Test stability computation with a temporary DB."""
    import sqlite3
    import tempfile

    from signals.core.derived_db import init_db, get_connection, insert_signal_result, insert_run
    from signals.core.dto import SignalResult
    from signals.core.runs import make_run
    from signals.analysis.timeseries import compute_signal_stability

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db_path = tmp.name
        init_db(db_path)

        run_ids_by_date = {}
        for i, date in enumerate(["2025-01-01", "2025-02-01", "2025-03-01"]):
            run = make_run(f"bt-{i}", "insider", "test", {}, {})
            with get_connection(db_path) as conn:
                insert_run(conn, run)
                # AAPL: bullish in all 3 dates (stable)
                insert_signal_result(conn, SignalResult(
                    source="insider", scope="entity", subject_key="entity:aapl",
                    score=0.5, label="bullish", confidence=0.8,
                    as_of_date=date, lookback_window=90,
                    input_count=3, included_count=3, excluded_count=0,
                    explanation="test", method_version="test", code_version="test",
                    run_id=run.run_id, provenance_refs={},
                ), f"fp-{date}-aapl")
                # GOOG: flips between bullish and bearish
                label = "bullish" if i % 2 == 0 else "bearish"
                insert_signal_result(conn, SignalResult(
                    source="insider", scope="entity", subject_key="entity:goog",
                    score=0.3 if label == "bullish" else -0.3, label=label, confidence=0.6,
                    as_of_date=date, lookback_window=90,
                    input_count=2, included_count=2, excluded_count=0,
                    explanation="test", method_version="test", code_version="test",
                    run_id=run.run_id, provenance_refs={},
                ), f"fp-{date}-goog")

            run_ids_by_date[date] = [run.run_id]

        stability = compute_signal_stability(db_path, run_ids_by_date)
        assert stability["ticker_count"] == 2
        assert stability["date_count"] == 3
        # AAPL: stable (no flips)
        assert stability["tickers"]["AAPL"]["flip_rate"] == 0.0
        assert stability["tickers"]["AAPL"]["dominant_label"] == "bullish"
        # GOOG: flips every date
        assert stability["tickers"]["GOOG"]["flip_rate"] == 1.0


def test_signal_turnover_with_synthetic_data():
    """Test turnover computation."""
    import tempfile

    from signals.core.derived_db import init_db, get_connection, insert_signal_result, insert_run
    from signals.core.dto import SignalResult
    from signals.core.runs import make_run
    from signals.analysis.timeseries import compute_signal_turnover

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db_path = tmp.name
        init_db(db_path)

        run_ids_by_date = {}
        # Date 1: AAPL bullish only
        run1 = make_run("bt-1", "insider", "test", {}, {})
        with get_connection(db_path) as conn:
            insert_run(conn, run1)
            insert_signal_result(conn, SignalResult(
                source="insider", scope="entity", subject_key="entity:aapl",
                score=0.5, label="bullish", confidence=0.8,
                as_of_date="2025-01-01", lookback_window=90,
                input_count=3, included_count=3, excluded_count=0,
                explanation="test", method_version="test", code_version="test",
                run_id=run1.run_id, provenance_refs={},
            ), "fp1")
        run_ids_by_date["2025-01-01"] = [run1.run_id]

        # Date 2: AAPL bullish + GOOG bearish (partial overlap)
        run2 = make_run("bt-2", "insider", "test", {}, {})
        with get_connection(db_path) as conn:
            insert_run(conn, run2)
            insert_signal_result(conn, SignalResult(
                source="insider", scope="entity", subject_key="entity:aapl",
                score=0.5, label="bullish", confidence=0.8,
                as_of_date="2025-02-01", lookback_window=90,
                input_count=3, included_count=3, excluded_count=0,
                explanation="test", method_version="test", code_version="test",
                run_id=run2.run_id, provenance_refs={},
            ), "fp2a")
            insert_signal_result(conn, SignalResult(
                source="insider", scope="entity", subject_key="entity:goog",
                score=-0.3, label="bearish", confidence=0.6,
                as_of_date="2025-02-01", lookback_window=90,
                input_count=2, included_count=2, excluded_count=0,
                explanation="test", method_version="test", code_version="test",
                run_id=run2.run_id, provenance_refs={},
            ), "fp2b")
        run_ids_by_date["2025-02-01"] = [run2.run_id]

        turnover = compute_signal_turnover(db_path, run_ids_by_date)
        assert turnover["date_pairs"] == 1
        # Jaccard: {aapl:bullish} ∩ {aapl:bullish, goog:bearish} = 1/2 → turnover = 0.5
        assert turnover["turnovers"][0]["turnover"] == 0.5
