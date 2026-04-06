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


def run_baseline_comparison(
    db_path: str,
    forward_days: list[int] | None = None,
    min_date: str | None = None,
    max_date: str | None = None,
) -> dict:
    """Compare scored model vs trivial baseline (predict bullish if any insider buy exists).

    The trivial baseline: for each ticker with at least 1 insider buy in the period,
    predict the stock will go up. No scoring, no weighting — just "did someone buy?"

    If our scoring model doesn't outperform this, the multiplicative weights add no value.
    """
    if not HAS_YFINANCE:
        return {"error": "yfinance not installed"}

    if forward_days is None:
        forward_days = [5, 20, 60]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT ticker, execution_date, amount_estimate, actor_type
        FROM normalized_transactions
        WHERE source = 'insider' AND include_in_signal = 1
          AND direction = 'BUY' AND ticker IS NOT NULL AND execution_date IS NOT NULL
    """
    params: list = []
    if min_date:
        query += " AND execution_date >= ?"
        params.append(min_date)
    if max_date:
        query += " AND execution_date <= ?"
        params.append(max_date)

    buy_rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()

    if not buy_rows:
        return {"error": "No insider buys found", "buy_count": 0}

    tickers = [r["ticker"].upper() for r in buy_rows]
    dates = [r["execution_date"] for r in buy_rows]
    returns = _fetch_forward_returns(tickers, dates, forward_days)

    baseline_results: dict[str, dict] = {}
    for fwd in forward_days:
        correct = 0
        total = 0
        fwd_returns: list[float] = []
        for row in buy_rows:
            ticker = row["ticker"].upper()
            ret = returns.get((ticker, row["execution_date"], fwd))
            if ret is not None:
                total += 1
                fwd_returns.append(ret)
                if ret > 0:
                    correct += 1

        baseline_results[f"{fwd}d"] = {
            "baseline_accuracy": round(correct / total, 4) if total else None,
            "baseline_mean_return": round(statistics.mean(fwd_returns), 6) if fwd_returns else None,
            "baseline_count": total,
        }

    return {
        "buy_transactions": len(buy_rows),
        "forward_windows": forward_days,
        "comparison": baseline_results,
    }


def run_regime_analysis(
    db_path: str,
    forward_days: list[int] | None = None,
    min_date: str | None = None,
    max_date: str | None = None,
    regime_lookback_days: int = 60,
) -> dict:
    """Compute insider buy accuracy conditional on market regime.

    Regime defined by SPY trailing return over `regime_lookback_days`:
    bull (SPY > 0) vs bear (SPY <= 0).
    """
    if not HAS_YFINANCE:
        return {"error": "yfinance not installed"}

    if forward_days is None:
        forward_days = [5, 20, 60]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT ticker, execution_date
        FROM normalized_transactions
        WHERE source = 'insider' AND include_in_signal = 1
          AND direction = 'BUY' AND ticker IS NOT NULL AND execution_date IS NOT NULL
    """
    params: list = []
    if min_date:
        query += " AND execution_date >= ?"
        params.append(min_date)
    if max_date:
        query += " AND execution_date <= ?"
        params.append(max_date)

    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()

    if not rows:
        return {"error": "No insider buys found"}

    all_dates = [datetime.strptime(r["execution_date"], "%Y-%m-%d") for r in rows]
    earliest = min(all_dates) - timedelta(days=regime_lookback_days + 10)
    latest = max(all_dates) + timedelta(days=max(forward_days) + 10)

    try:
        spy_data = yf.download(
            "SPY", start=earliest.strftime("%Y-%m-%d"),
            end=latest.strftime("%Y-%m-%d"), progress=False, auto_adjust=True,
        )
        if hasattr(spy_data.columns, "levels"):
            spy_data.columns = spy_data.columns.get_level_values(0)
        spy_close = spy_data["Close"]
    except Exception:
        return {"error": "Could not fetch SPY data"}

    def _get_regime(exec_date: datetime) -> str | None:
        lookback_start = exec_date - timedelta(days=regime_lookback_days)
        start_idx = spy_close.index.searchsorted(lookback_start)
        end_idx = spy_close.index.searchsorted(exec_date)
        if start_idx >= len(spy_close) or end_idx >= len(spy_close) or start_idx == end_idx:
            return None
        spy_return = (spy_close.iloc[end_idx] - spy_close.iloc[start_idx]) / spy_close.iloc[start_idx]
        return "bull" if float(spy_return) > 0 else "bear"

    tickers = [r["ticker"].upper() for r in rows]
    dates = [r["execution_date"] for r in rows]
    returns = _fetch_forward_returns(tickers, dates, forward_days)

    regime_results: dict[str, dict] = {}
    for fwd in forward_days:
        bull_correct, bull_total = 0, 0
        bear_correct, bear_total = 0, 0
        bull_returns: list[float] = []
        bear_returns: list[float] = []

        for row in rows:
            exec_dt = datetime.strptime(row["execution_date"], "%Y-%m-%d")
            regime = _get_regime(exec_dt)
            if regime is None:
                continue
            ticker = row["ticker"].upper()
            ret = returns.get((ticker, row["execution_date"], fwd))
            if ret is None:
                continue

            if regime == "bull":
                bull_total += 1
                bull_returns.append(ret)
                if ret > 0:
                    bull_correct += 1
            else:
                bear_total += 1
                bear_returns.append(ret)
                if ret > 0:
                    bear_correct += 1

        regime_results[f"{fwd}d"] = {
            "bull": {
                "count": bull_total,
                "accuracy": round(bull_correct / bull_total, 4) if bull_total else None,
                "mean_return": round(statistics.mean(bull_returns), 6) if bull_returns else None,
            },
            "bear": {
                "count": bear_total,
                "accuracy": round(bear_correct / bear_total, 4) if bear_total else None,
                "mean_return": round(statistics.mean(bear_returns), 6) if bear_returns else None,
            },
        }

    return {
        "transaction_count": len(rows),
        "regime_lookback_days": regime_lookback_days,
        "forward_windows": forward_days,
        "regime_analysis": regime_results,
    }


def render_baseline_comparison_markdown(report: dict) -> str:
    if "error" in report:
        return f"# Baseline Comparison\n\n**Error:** {report['error']}\n"

    lines = [
        "# Model vs Trivial Baseline",
        "",
        f"**Insider buy transactions:** {report['buy_transactions']}",
        "",
        "Baseline: predict bullish for any ticker with an insider buy (no scoring).",
        "",
        "| Window | Baseline Accuracy | N | Mean Return |",
        "|--------|-------------------|---|-------------|",
    ]
    for key, stats in report["comparison"].items():
        b_acc = f"{stats['baseline_accuracy']:.1%}" if stats["baseline_accuracy"] is not None else "N/A"
        b_ret = f"{stats['baseline_mean_return']:.4%}" if stats["baseline_mean_return"] is not None else "N/A"
        lines.append(f"| {key} | {b_acc} | {stats['baseline_count']} | {b_ret} |")

    return "\n".join(lines) + "\n"


def render_regime_analysis_markdown(report: dict) -> str:
    if "error" in report:
        return f"# Regime Analysis\n\n**Error:** {report['error']}\n"

    lines = [
        "# Insider Buy Accuracy by Market Regime",
        "",
        f"**Transactions:** {report['transaction_count']}",
        f"**Regime:** SPY {report['regime_lookback_days']}-day trailing return (>0 = bull, <=0 = bear)",
        "",
        "| Window | Bull Accuracy | Bull N | Bear Accuracy | Bear N | Bull Mean Ret | Bear Mean Ret |",
        "|--------|--------------|--------|--------------|--------|---------------|---------------|",
    ]
    for key, data in report["regime_analysis"].items():
        b = data["bull"]
        r = data["bear"]
        b_acc = f"{b['accuracy']:.1%}" if b["accuracy"] is not None else "N/A"
        r_acc = f"{r['accuracy']:.1%}" if r["accuracy"] is not None else "N/A"
        b_ret = f"{b['mean_return']:.4%}" if b["mean_return"] is not None else "N/A"
        r_ret = f"{r['mean_return']:.4%}" if r["mean_return"] is not None else "N/A"
        lines.append(f"| {key} | {b_acc} | {b['count']} | {r_acc} | {r['count']} | {b_ret} | {r_ret} |")

    return "\n".join(lines) + "\n"


# Sector ETF mapping for sector-relative validation
SECTOR_ETF_MAP = {
    "Technology": "XLK",
    "Health Care": "XLV",
    "Financials": "XLF",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Industrials": "XLI",
    "Materials": "XLB",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}


def run_sector_relative_validation(
    db_path: str,
    forward_days: list[int] | None = None,
    min_date: str | None = None,
    max_date: str | None = None,
) -> dict:
    """Validate insider buy signals using sector-relative returns.

    For each insider buy, computes stock_return - sector_ETF_return.
    If sector-adjusted accuracy < 55%, signals are mostly beta, not alpha.
    """
    if not HAS_YFINANCE:
        return {"error": "yfinance not installed"}

    if forward_days is None:
        forward_days = [5, 20, 60]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = """
        SELECT ticker, execution_date
        FROM normalized_transactions
        WHERE source = 'insider' AND include_in_signal = 1
          AND direction = 'BUY' AND ticker IS NOT NULL AND execution_date IS NOT NULL
    """
    params: list = []
    if min_date:
        query += " AND execution_date >= ?"
        params.append(min_date)
    if max_date:
        query += " AND execution_date <= ?"
        params.append(max_date)

    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()

    if not rows:
        return {"error": "No insider buys found"}

    try:
        from signals.analysis.sectors import get_sector_map
        tickers = list({r["ticker"].upper() for r in rows})
        sector_map = get_sector_map(tickers)
    except Exception:
        return {"error": "Could not fetch sector data"}

    etf_tickers = set()
    ticker_to_etf = {}
    skipped = 0
    for r in rows:
        ticker = r["ticker"].upper()
        sector = sector_map.get(ticker, {}).get("sector")
        etf = SECTOR_ETF_MAP.get(sector) if sector else None
        if etf:
            etf_tickers.add(etf)
            ticker_to_etf[ticker] = etf
        else:
            skipped += 1

    all_tickers = [r["ticker"].upper() for r in rows if r["ticker"].upper() in ticker_to_etf]
    all_dates = [r["execution_date"] for r in rows if r["ticker"].upper() in ticker_to_etf]

    stock_returns = _fetch_forward_returns(all_tickers, all_dates, forward_days)
    etf_returns = _fetch_forward_returns(list(etf_tickers), list(set(all_dates)), forward_days)

    summary: dict = {}
    for fwd in forward_days:
        outperform = 0
        underperform = 0
        excess_returns: list[float] = []

        for r in rows:
            ticker = r["ticker"].upper()
            etf = ticker_to_etf.get(ticker)
            if not etf:
                continue
            stock_ret = stock_returns.get((ticker, r["execution_date"], fwd))
            sector_ret = etf_returns.get((etf, r["execution_date"], fwd))
            if stock_ret is None or sector_ret is None:
                continue
            excess = stock_ret - sector_ret
            excess_returns.append(excess)
            if excess > 0:
                outperform += 1
            else:
                underperform += 1

        total = outperform + underperform
        summary[f"{fwd}d"] = {
            "total": total,
            "outperform": outperform,
            "underperform": underperform,
            "sector_adjusted_accuracy": round(outperform / total, 4) if total else None,
            "mean_excess_return": round(statistics.mean(excess_returns), 6) if excess_returns else None,
            "is_alpha": (outperform / total > 0.55) if total else None,
        }

    return {
        "transaction_count": len(rows),
        "with_sector": len(rows) - skipped,
        "skipped_no_sector": skipped,
        "forward_windows": forward_days,
        "summary": summary,
    }


def render_sector_relative_markdown(report: dict) -> str:
    if "error" in report:
        return f"# Sector-Relative Validation\n\n**Error:** {report['error']}\n"

    lines = [
        "# Sector-Relative Validation",
        "",
        f"**Transactions:** {report['transaction_count']} ({report['with_sector']} with sector data, {report['skipped_no_sector']} skipped)",
        "",
        "Does the stock outperform its sector? If accuracy < 55%, signals are beta, not alpha.",
        "",
        "| Window | Outperform | Underperform | Sector-Adj Accuracy | Mean Excess Return | Alpha? |",
        "|--------|-----------|-------------|--------------------|--------------------|--------|",
    ]
    for key, stats in report["summary"].items():
        acc = f"{stats['sector_adjusted_accuracy']:.1%}" if stats["sector_adjusted_accuracy"] is not None else "N/A"
        ret = f"{stats['mean_excess_return']:.4%}" if stats["mean_excess_return"] is not None else "N/A"
        alpha = "YES" if stats.get("is_alpha") else "NO" if stats.get("is_alpha") is False else "N/A"
        lines.append(f"| {key} | {stats['outperform']} | {stats['underperform']} | {acc} | {ret} | {alpha} |")

    return "\n".join(lines) + "\n"
