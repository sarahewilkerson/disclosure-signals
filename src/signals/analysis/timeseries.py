"""Time-series analysis for backtested signals.

Analyzes signal stability, turnover, and persistence across
multiple reference dates from a backtest run.

Usage:
    from signals.analysis.timeseries import compute_signal_stability, compute_signal_turnover
    stability = compute_signal_stability(db_path, run_ids_by_date)
    turnover = compute_signal_turnover(db_path, run_ids_by_date)
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict


def compute_signal_stability(
    db_path: str,
    run_ids_by_date: dict[str, list[str]],
) -> dict:
    """Compute per-ticker signal stability across dates.

    Stability = fraction of dates where the ticker has a non-neutral/non-insufficient signal.
    Flip rate = fraction of consecutive date pairs where the label changes.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Collect signals by ticker and date
    ticker_signals: dict[str, dict[str, str]] = defaultdict(dict)  # ticker → {date → label}
    dates_sorted = sorted(run_ids_by_date.keys())

    for date, run_ids in sorted(run_ids_by_date.items()):
        if not run_ids:
            continue
        placeholders = ",".join("?" for _ in run_ids)
        rows = conn.execute(
            f"SELECT subject_key, label FROM signal_results WHERE run_id IN ({placeholders}) AND source = 'insider'",
            run_ids,
        ).fetchall()
        for row in rows:
            ticker = row["subject_key"].replace("entity:", "").upper()
            ticker_signals[ticker][date] = row["label"]

    conn.close()

    # Compute stability metrics
    results = {}
    for ticker, date_labels in ticker_signals.items():
        labels_in_order = [date_labels.get(d, "absent") for d in dates_sorted]
        active = [l for l in labels_in_order if l in ("bullish", "bearish")]
        total_dates = len(dates_sorted)

        # Flip rate: consecutive label changes
        flips = 0
        comparisons = 0
        for i in range(1, len(labels_in_order)):
            if labels_in_order[i] != "absent" and labels_in_order[i - 1] != "absent":
                comparisons += 1
                if labels_in_order[i] != labels_in_order[i - 1]:
                    flips += 1

        results[ticker] = {
            "active_dates": len(active),
            "total_dates": total_dates,
            "stability": len(active) / total_dates if total_dates else 0,
            "flip_rate": flips / comparisons if comparisons else 0,
            "dominant_label": max(set(active), key=active.count) if active else "insufficient",
        }

    return {
        "ticker_count": len(results),
        "date_count": len(dates_sorted),
        "tickers": results,
        "summary": {
            "mean_stability": sum(r["stability"] for r in results.values()) / len(results) if results else 0,
            "mean_flip_rate": sum(r["flip_rate"] for r in results.values()) / len(results) if results else 0,
        },
    }


def compute_signal_turnover(
    db_path: str,
    run_ids_by_date: dict[str, list[str]],
) -> dict:
    """Compute signal set turnover between consecutive dates.

    Turnover = 1 - Jaccard similarity of bullish/bearish ticker sets.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    date_signals: dict[str, set[str]] = {}
    for date, run_ids in sorted(run_ids_by_date.items()):
        if not run_ids:
            date_signals[date] = set()
            continue
        placeholders = ",".join("?" for _ in run_ids)
        rows = conn.execute(
            f"SELECT subject_key, label FROM signal_results WHERE run_id IN ({placeholders}) AND label IN ('bullish', 'bearish')",
            run_ids,
        ).fetchall()
        date_signals[date] = {f"{row['subject_key']}:{row['label']}" for row in rows}

    conn.close()

    dates_sorted = sorted(date_signals.keys())
    turnovers = []
    for i in range(1, len(dates_sorted)):
        prev = date_signals[dates_sorted[i - 1]]
        curr = date_signals[dates_sorted[i]]
        union = prev | curr
        intersection = prev & curr
        jaccard = len(intersection) / len(union) if union else 1.0
        turnover = 1.0 - jaccard
        turnovers.append({
            "from_date": dates_sorted[i - 1],
            "to_date": dates_sorted[i],
            "prev_count": len(prev),
            "curr_count": len(curr),
            "turnover": round(turnover, 4),
        })

    return {
        "date_pairs": len(turnovers),
        "mean_turnover": round(sum(t["turnover"] for t in turnovers) / len(turnovers), 4) if turnovers else 0,
        "turnovers": turnovers,
    }


def render_timeseries_markdown(stability: dict, turnover: dict) -> str:
    """Render time-series analysis as markdown."""
    lines = [
        "# Time-Series Analysis",
        "",
        f"**Tickers tracked:** {stability['ticker_count']}",
        f"**Dates:** {stability['date_count']}",
        f"**Mean stability:** {stability['summary']['mean_stability']:.2%}",
        f"**Mean flip rate:** {stability['summary']['mean_flip_rate']:.2%}",
        f"**Mean turnover:** {turnover['mean_turnover']:.2%}",
        "",
    ]

    if turnover.get("turnovers"):
        lines.extend([
            "## Turnover by Date Pair",
            "",
            "| From | To | Prev Signals | Curr Signals | Turnover |",
            "|------|----|-------------|-------------|----------|",
        ])
        for t in turnover["turnovers"]:
            lines.append(f"| {t['from_date']} | {t['to_date']} | {t['prev_count']} | {t['curr_count']} | {t['turnover']:.1%} |")

    return "\n".join(lines) + "\n"
