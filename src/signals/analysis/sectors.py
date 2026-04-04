"""Sector enrichment for signal context.

Fetches GICS sector/industry data via yfinance with local SQLite caching
to avoid repeated API calls.

Usage:
    from signals.analysis.sectors import get_sector_map, build_sector_summary
    sectors = get_sector_map(["AAPL", "NVDA", "JPM"])
    summary = build_sector_summary(db_path, sectors)
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


_CACHE_DB = Path(__file__).resolve().parent.parent.parent.parent / "data" / "sector_cache.db"

_CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS sector_cache (
    ticker TEXT PRIMARY KEY,
    sector TEXT,
    industry TEXT,
    fetched_at TEXT DEFAULT (datetime('now'))
);
"""


def _get_cache_conn() -> sqlite3.Connection:
    _CACHE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_CACHE_DB))
    conn.row_factory = sqlite3.Row
    conn.execute(_CACHE_SCHEMA)
    return conn


def get_sector_map(tickers: list[str]) -> dict[str, dict[str, str]]:
    """Get sector/industry for a list of tickers, using cache where available.

    Returns dict of ticker → {"sector": str, "industry": str}.
    """
    result: dict[str, dict[str, str]] = {}
    cache_conn = _get_cache_conn()

    # Check cache first
    uncached = []
    for ticker in tickers:
        row = cache_conn.execute(
            "SELECT sector, industry FROM sector_cache WHERE ticker = ?",
            (ticker.upper(),)
        ).fetchone()
        if row:
            result[ticker.upper()] = {"sector": row["sector"] or "Unknown", "industry": row["industry"] or "Unknown"}
        else:
            uncached.append(ticker.upper())

    # Fetch missing from yfinance
    if uncached and HAS_YFINANCE:
        for ticker in uncached:
            try:
                info = yf.Ticker(ticker).info
                sector = info.get("sector", "Unknown")
                industry = info.get("industry", "Unknown")
            except Exception:
                sector = "Unknown"
                industry = "Unknown"
            result[ticker] = {"sector": sector, "industry": industry}
            cache_conn.execute(
                "INSERT OR REPLACE INTO sector_cache (ticker, sector, industry) VALUES (?, ?, ?)",
                (ticker, sector, industry),
            )
        cache_conn.commit()

    cache_conn.close()
    return result


def build_sector_summary(
    db_path: str,
    sector_map: dict[str, dict[str, str]] | None = None,
) -> dict:
    """Aggregate signals by sector.

    Returns dict with sector-level buy/sell counts and signal summaries.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT subject_key, label, score, confidence, source
        FROM signal_results
        WHERE label IN ('bullish', 'bearish')
          AND confidence > 0
    """).fetchall()
    conn.close()

    if sector_map is None:
        tickers = list({r["subject_key"].replace("entity:", "").upper() for r in rows})
        sector_map = get_sector_map(tickers)

    by_sector: dict[str, dict] = defaultdict(lambda: {
        "bullish": 0, "bearish": 0, "tickers": defaultdict(list)
    })

    for row in rows:
        ticker = row["subject_key"].replace("entity:", "").upper()
        sector_info = sector_map.get(ticker, {"sector": "Unknown"})
        sector = sector_info["sector"]
        by_sector[sector][row["label"]] += 1
        by_sector[sector]["tickers"][ticker].append({
            "source": row["source"],
            "label": row["label"],
            "score": row["score"],
        })

    summary = {}
    for sector, data in sorted(by_sector.items()):
        summary[sector] = {
            "bullish_count": data["bullish"],
            "bearish_count": data["bearish"],
            "net_sentiment": data["bullish"] - data["bearish"],
            "unique_tickers": len(data["tickers"]),
            "top_tickers": sorted(
                data["tickers"].keys(),
                key=lambda t: max(abs(s["score"]) for s in data["tickers"][t]),
                reverse=True,
            )[:5],
        }

    return summary


def render_sector_summary_markdown(summary: dict) -> str:
    """Render sector summary as markdown."""
    if not summary:
        return "No sector data available.\n"

    lines = [
        "## Sector Summary",
        "",
        "| Sector | Bullish | Bearish | Net | Tickers |",
        "|--------|---------|---------|-----|---------|",
    ]
    for sector, data in sorted(summary.items(), key=lambda x: abs(x[1]["net_sentiment"]), reverse=True):
        top = ", ".join(data["top_tickers"][:3])
        lines.append(
            f"| {sector} | {data['bullish_count']} | {data['bearish_count']} | "
            f"{data['net_sentiment']:+d} | {top} |"
        )

    return "\n".join(lines) + "\n"
