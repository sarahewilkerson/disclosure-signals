"""Forward-return validation for signal quality assessment.

Fetches price data via yfinance and computes directional accuracy,
score-return correlation, and per-window analysis for signal results.

This module is measurement-only — it does NOT change scoring logic.

Usage:
    from signals.analysis.validation import run_validation_report
    report = run_validation_report(db_path, forward_days=[5, 10, 20, 60])
"""

from __future__ import annotations

import json
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta

try:
    import yfinance as yf

    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


@dataclass
class ValidationResult:
    ticker: str
    source: str
    signal_label: str
    signal_score: float
    signal_confidence: float
    as_of_date: str
    lookback_window: int
    forward_days: int
    forward_return: float | None
    direction_correct: bool | None

    def to_dict(self) -> dict:
        from dataclasses import asdict

        return asdict(self)


def _fetch_forward_returns(
    tickers: list[str],
    as_of_dates: list[str],
    forward_days_list: list[int],
) -> dict[tuple[str, str, int], float | None]:
    """Fetch forward returns for ticker/date/window combinations.

    Returns dict keyed by (ticker, as_of_date, forward_days) → return as decimal.
    """
    if not HAS_YFINANCE:
        return {}

    results: dict[tuple[str, str, int], float | None] = {}
    unique_tickers = sorted(set(tickers))
    max_forward = max(forward_days_list) if forward_days_list else 60

    all_dates = [datetime.strptime(d, "%Y-%m-%d") for d in as_of_dates if d]
    if not all_dates:
        return results
    earliest = min(all_dates) - timedelta(days=5)
    latest = max(all_dates) + timedelta(days=max_forward + 10)

    for ticker in unique_tickers:
        try:
            data = yf.download(
                ticker,
                start=earliest.strftime("%Y-%m-%d"),
                end=latest.strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=True,
            )
            if data.empty:
                continue
            # Handle MultiIndex columns from yfinance
            if hasattr(data.columns, "levels"):
                data.columns = data.columns.get_level_values(0)
            close = data["Close"]
        except Exception:
            continue

        for as_of in as_of_dates:
            try:
                base_date = datetime.strptime(as_of, "%Y-%m-%d")
            except (ValueError, TypeError):
                continue

            # Find the closest trading day on or after the signal date
            base_idx = close.index.searchsorted(base_date)
            if base_idx >= len(close):
                continue
            base_price = close.iloc[base_idx]

            for fwd in forward_days_list:
                target_date = base_date + timedelta(days=fwd)
                target_idx = close.index.searchsorted(target_date)
                if target_idx >= len(close):
                    results[(ticker, as_of, fwd)] = None
                    continue
                target_price = close.iloc[target_idx]
                if base_price > 0:
                    results[(ticker, as_of, fwd)] = float(
                        (target_price - base_price) / base_price
                    )
                else:
                    results[(ticker, as_of, fwd)] = None

    return results


def run_validation_report(
    db_path: str,
    forward_days: list[int] | None = None,
    source_filter: str | None = None,
) -> dict:
    """Run forward-return validation against persisted signal results.

    Args:
        db_path: Path to derived SQLite database.
        forward_days: List of forward return windows (default: [5, 10, 20, 60]).
        source_filter: Optional source filter ('insider', 'congress', or None for all).

    Returns:
        Validation report dict with summary statistics and per-signal details.
    """
    if not HAS_YFINANCE:
        return {"error": "yfinance not installed. Run: pip install yfinance"}

    if forward_days is None:
        forward_days = [5, 10, 20, 60]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT source, subject_key, score, label, confidence, as_of_date, lookback_window
        FROM signal_results
        WHERE label IN ('bullish', 'bearish')
          AND confidence > 0
    """
    params: list = []
    if source_filter:
        query += " AND source = ?"
        params.append(source_filter)

    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()

    if not rows:
        return {"error": "No qualifying signal results found", "row_count": 0}

    # Extract tickers and dates
    tickers = []
    as_of_dates = []
    for row in rows:
        ticker = row["subject_key"].replace("entity:", "").upper()
        tickers.append(ticker)
        as_of_dates.append(row["as_of_date"])

    # Fetch returns
    returns = _fetch_forward_returns(tickers, as_of_dates, forward_days)

    # Build validation results
    results: list[ValidationResult] = []
    for row in rows:
        ticker = row["subject_key"].replace("entity:", "").upper()
        for fwd in forward_days:
            fwd_return = returns.get((ticker, row["as_of_date"], fwd))
            if fwd_return is not None:
                if row["label"] == "bullish":
                    direction_correct = fwd_return > 0
                elif row["label"] == "bearish":
                    direction_correct = fwd_return < 0
                else:
                    direction_correct = None
            else:
                direction_correct = None

            results.append(
                ValidationResult(
                    ticker=ticker,
                    source=row["source"],
                    signal_label=row["label"],
                    signal_score=row["score"],
                    signal_confidence=row["confidence"],
                    as_of_date=row["as_of_date"],
                    lookback_window=row["lookback_window"],
                    forward_days=fwd,
                    forward_return=fwd_return,
                    direction_correct=direction_correct,
                )
            )

    # Compute summary statistics
    summary = _compute_summary(results, forward_days)

    return {
        "signal_count": len(rows),
        "validation_count": len(results),
        "forward_windows": forward_days,
        "summary": summary,
        "details": [r.to_dict() for r in results],
    }


def _compute_summary(
    results: list[ValidationResult], forward_days: list[int]
) -> dict:
    summary: dict = {}

    for fwd in forward_days:
        window_results = [r for r in results if r.forward_days == fwd]
        with_returns = [r for r in window_results if r.forward_return is not None]
        with_direction = [r for r in with_returns if r.direction_correct is not None]

        if not with_direction:
            summary[f"{fwd}d"] = {
                "total": len(window_results),
                "with_data": len(with_returns),
                "directional_accuracy": None,
                "mean_return": None,
                "by_source": {},
            }
            continue

        correct = sum(1 for r in with_direction if r.direction_correct)
        accuracy = correct / len(with_direction) if with_direction else 0.0

        returns_list = [r.forward_return for r in with_returns if r.forward_return is not None]
        mean_return = statistics.mean(returns_list) if returns_list else None

        # By source
        by_source: dict = {}
        for source in ("insider", "congress"):
            source_results = [r for r in with_direction if r.source == source]
            if source_results:
                source_correct = sum(1 for r in source_results if r.direction_correct)
                source_returns = [r.forward_return for r in source_results if r.forward_return is not None]
                by_source[source] = {
                    "count": len(source_results),
                    "directional_accuracy": source_correct / len(source_results),
                    "mean_return": statistics.mean(source_returns) if source_returns else None,
                }

        # By label
        by_label: dict = {}
        for label in ("bullish", "bearish"):
            label_results = [r for r in with_returns if r.signal_label == label]
            if label_results:
                label_returns = [r.forward_return for r in label_results if r.forward_return is not None]
                label_direction = [r for r in label_results if r.direction_correct is not None]
                label_correct = sum(1 for r in label_direction if r.direction_correct)
                by_label[label] = {
                    "count": len(label_results),
                    "directional_accuracy": label_correct / len(label_direction) if label_direction else None,
                    "mean_return": statistics.mean(label_returns) if label_returns else None,
                }

        summary[f"{fwd}d"] = {
            "total": len(window_results),
            "with_data": len(with_returns),
            "directional_accuracy": round(accuracy, 4),
            "mean_return": round(mean_return, 6) if mean_return is not None else None,
            "by_source": by_source,
            "by_label": by_label,
        }

    return summary


def run_transaction_validation(
    db_path: str,
    forward_days: list[int] | None = None,
    source_filter: str | None = None,
    min_date: str | None = None,
    max_date: str | None = None,
) -> dict:
    """Validate at the transaction level: for each included buy/sell, check forward returns.

    This is the most direct test of signal quality — does insider/congress
    buying predict positive returns, and selling predict negative returns?
    """
    if not HAS_YFINANCE:
        return {"error": "yfinance not installed. Run: pip install yfinance"}

    if forward_days is None:
        forward_days = [5, 10, 20, 60]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT source, ticker, direction, execution_date, amount_estimate,
               actor_type, owner_type
        FROM normalized_transactions
        WHERE include_in_signal = 1
          AND ticker IS NOT NULL
          AND execution_date IS NOT NULL
          AND direction IN ('BUY', 'SELL')
    """
    params: list = []
    if source_filter:
        query += " AND source = ?"
        params.append(source_filter)
    if min_date:
        query += " AND execution_date >= ?"
        params.append(min_date)
    if max_date:
        query += " AND execution_date <= ?"
        params.append(max_date)

    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()

    if not rows:
        return {"error": "No qualifying transactions found", "row_count": 0}

    tickers = [r["ticker"].upper() for r in rows]
    dates = [r["execution_date"] for r in rows]
    returns = _fetch_forward_returns(tickers, dates, forward_days)

    # Compute directional accuracy per window
    summary: dict = {}
    for fwd in forward_days:
        buys_correct = 0
        buys_total = 0
        sells_correct = 0
        sells_total = 0
        buy_returns: list[float] = []
        sell_returns: list[float] = []

        for row in rows:
            ticker = row["ticker"].upper()
            fwd_return = returns.get((ticker, row["execution_date"], fwd))
            if fwd_return is None:
                continue

            if row["direction"] == "BUY":
                buys_total += 1
                buy_returns.append(fwd_return)
                if fwd_return > 0:
                    buys_correct += 1
            elif row["direction"] == "SELL":
                sells_total += 1
                sell_returns.append(fwd_return)
                if fwd_return < 0:
                    sells_correct += 1

        total = buys_total + sells_total
        correct = buys_correct + sells_correct
        summary[f"{fwd}d"] = {
            "total_transactions": total,
            "directional_accuracy": round(correct / total, 4) if total else None,
            "buys": {
                "count": buys_total,
                "accuracy": round(buys_correct / buys_total, 4) if buys_total else None,
                "mean_return": round(statistics.mean(buy_returns), 6) if buy_returns else None,
            },
            "sells": {
                "count": sells_total,
                "accuracy": round(sells_correct / sells_total, 4) if sells_total else None,
                "mean_return": round(statistics.mean(sell_returns), 6) if sell_returns else None,
            },
        }

    return {
        "transaction_count": len(rows),
        "forward_windows": forward_days,
        "date_range": {"min": min(dates), "max": max(dates)},
        "summary": summary,
    }


def render_transaction_validation_markdown(report: dict) -> str:
    """Render transaction-level validation as markdown."""
    if "error" in report:
        return f"# Transaction Validation\n\n**Error:** {report['error']}\n"

    lines = [
        "# Transaction-Level Validation Report",
        "",
        f"**Transactions evaluated:** {report['transaction_count']}",
        f"**Date range:** {report['date_range']['min']} to {report['date_range']['max']}",
        f"**Forward windows:** {report['forward_windows']}",
        "",
        "## Directional Accuracy",
        "",
        "| Window | Transactions | Overall Accuracy | Buy Accuracy | Sell Accuracy | Buy Mean Return | Sell Mean Return |",
        "|--------|-------------|-----------------|--------------|---------------|-----------------|------------------|",
    ]

    for key, stats in report["summary"].items():
        acc = f"{stats['directional_accuracy']:.1%}" if stats["directional_accuracy"] is not None else "N/A"
        b_acc = f"{stats['buys']['accuracy']:.1%}" if stats["buys"]["accuracy"] is not None else "N/A"
        s_acc = f"{stats['sells']['accuracy']:.1%}" if stats["sells"]["accuracy"] is not None else "N/A"
        b_ret = f"{stats['buys']['mean_return']:.4%}" if stats["buys"]["mean_return"] is not None else "N/A"
        s_ret = f"{stats['sells']['mean_return']:.4%}" if stats["sells"]["mean_return"] is not None else "N/A"
        lines.append(f"| {key} | {stats['total_transactions']} | {acc} | {b_acc} ({stats['buys']['count']}) | {s_acc} ({stats['sells']['count']}) | {b_ret} | {s_ret} |")

    return "\n".join(lines) + "\n"


def render_validation_markdown(report: dict) -> str:
    """Render a validation report as markdown."""
    if "error" in report:
        return f"# Validation Report\n\n**Error:** {report['error']}\n"

    lines = [
        "# Signal Validation Report",
        "",
        f"**Signals evaluated:** {report['signal_count']}",
        f"**Forward windows:** {report['forward_windows']}",
        "",
        "## Directional Accuracy",
        "",
        "| Window | Signals | With Data | Accuracy | Mean Return |",
        "|--------|---------|-----------|----------|-------------|",
    ]

    for key, stats in report["summary"].items():
        acc = f"{stats['directional_accuracy']:.1%}" if stats["directional_accuracy"] is not None else "N/A"
        ret = f"{stats['mean_return']:.4%}" if stats["mean_return"] is not None else "N/A"
        lines.append(f"| {key} | {stats['total']} | {stats['with_data']} | {acc} | {ret} |")

    lines.extend(["", "## By Source", ""])
    for key, stats in report["summary"].items():
        if stats.get("by_source"):
            lines.append(f"### {key}")
            for source, s in stats["by_source"].items():
                acc = f"{s['directional_accuracy']:.1%}" if s["directional_accuracy"] is not None else "N/A"
                lines.append(f"- **{source}:** {s['count']} signals, accuracy={acc}")

    lines.extend(["", "## By Label", ""])
    for key, stats in report["summary"].items():
        if stats.get("by_label"):
            lines.append(f"### {key}")
            for label, s in stats["by_label"].items():
                acc = f"{s['directional_accuracy']:.1%}" if s["directional_accuracy"] is not None else "N/A"
                ret = f"{s['mean_return']:.4%}" if s["mean_return"] is not None else "N/A"
                lines.append(f"- **{label}:** {s['count']} signals, accuracy={acc}, mean_return={ret}")

    return "\n".join(lines) + "\n"
