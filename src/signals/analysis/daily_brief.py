"""High-signal daily brief generator.

Produces a focused report of the most actionable signals, filtered
for quality based on validation findings:
- Insider buys are predictive (69.6% at 5d)
- Congress buys are predictive (58-64%)
- Sells are noise for both sources
- Single-transaction signals are insufficient

Usage:
    from signals.analysis.daily_brief import build_daily_brief, render_daily_brief_markdown
    brief = build_daily_brief(db_path)
    print(render_daily_brief_markdown(brief))
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ClusterBuyAlert:
    ticker: str
    unique_buyers: int
    total_buys: int
    actors: list[str]
    total_value: float | None
    earliest_date: str
    latest_date: str
    source: str


@dataclass
class AnomalyAlert:
    ticker: str
    alert_type: str  # "first_buy_in_period" or "elevated_activity"
    current_buys: int
    historical_monthly_avg: float
    months_since_last_buy: int | None
    actors: list[str]


@dataclass
class StrongSignal:
    ticker: str
    source: str
    label: str
    score: float
    confidence: float
    included_count: int
    lookback_window: int


@dataclass
class CrossSourceSignal:
    ticker: str
    insider_label: str
    insider_score: float
    insider_confidence: float
    congress_label: str
    congress_score: float
    congress_confidence: float
    overlay_outcome: str
    strength_tier: str | None


def build_daily_brief(
    db_path: str,
    reference_date: datetime | None = None,
    min_confidence: float = 0.4,
    cluster_threshold: int = 2,
    lookback_days: int = 30,
    include_sectors: bool = False,
) -> dict:
    """Build a high-signal daily brief from the derived database.

    Args:
        db_path: Path to the derived SQLite database.
        reference_date: Date for the brief (default: today).
        min_confidence: Minimum confidence for signal inclusion.
        cluster_threshold: Minimum unique buyers for cluster alert.
        lookback_days: Days back to search for cluster buying.
        include_sectors: Fetch sector data and include sector summary.
    """
    if reference_date is None:
        reference_date = datetime.now()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # 1. Cluster buy alerts: multiple unique insiders buying the same stock
    cluster_alerts = _find_cluster_buys(
        conn, reference_date, cluster_threshold, lookback_days
    )

    # 2. Strong buy signals (bullish with decent confidence)
    strong_insider = _find_strong_signals(conn, "insider", min_confidence)
    strong_congress = _find_strong_signals(conn, "congress", min_confidence)

    # 3. Cross-source signals from combined results
    cross_source = _find_cross_source(conn)

    # 4. Anomaly detection: first-time buying or elevated activity
    anomalies = _find_anomalous_activity(conn, reference_date, lookback_days)

    # 5. Summary stats
    stats = _build_stats(conn)

    conn.close()

    return {
        "as_of_date": reference_date.strftime("%Y-%m-%d"),
        "cluster_buy_alerts": [_alert_to_dict(a) for a in cluster_alerts],
        "strong_insider_buys": [_signal_to_dict(s) for s in strong_insider],
        "strong_congress_buys": [_signal_to_dict(s) for s in strong_congress],
        "cross_source_signals": [_cross_to_dict(c) for c in cross_source],
        "anomaly_alerts": [_anomaly_to_dict(a) for a in anomalies],
        "stats": stats,
        "sector_summary": _build_sector_summary(
            db_path, strong_insider, strong_congress, cross_source, cluster_alerts
        ) if include_sectors else None,
    }


def _build_sector_summary(
    db_path: str,
    strong_insider: list,
    strong_congress: list,
    cross_source: list,
    cluster_alerts: list,
) -> dict | None:
    try:
        from signals.analysis.sectors import get_sector_map, build_sector_summary
        tickers = set()
        for s in strong_insider:
            tickers.add(s.ticker)
        for s in strong_congress:
            tickers.add(s.ticker)
        for c in cross_source:
            tickers.add(c.ticker)
        for a in cluster_alerts:
            tickers.add(a.ticker)
        if not tickers:
            return None
        sector_map = get_sector_map(list(tickers))
        return build_sector_summary(db_path, sector_map)
    except ImportError:
        return None


def _find_cluster_buys(
    conn: sqlite3.Connection,
    reference_date: datetime,
    cluster_threshold: int,
    lookback_days: int,
) -> list[ClusterBuyAlert]:
    """Find stocks with multiple unique insider buyers in the lookback window."""
    cutoff = reference_date.strftime("%Y-%m-%d")
    from datetime import timedelta

    start = (reference_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    rows = conn.execute(
        """
        SELECT ticker, actor_name, actor_type, execution_date, amount_estimate
        FROM normalized_transactions
        WHERE source = 'insider'
          AND include_in_signal = 1
          AND direction = 'BUY'
          AND ticker IS NOT NULL
          AND execution_date >= ?
          AND execution_date <= ?
        ORDER BY ticker, execution_date
        """,
        (start, cutoff),
    ).fetchall()

    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_ticker[row["ticker"]].append(dict(row))

    alerts = []
    for ticker, txns in sorted(by_ticker.items()):
        unique_actors = {t["actor_name"] for t in txns if t["actor_name"]}
        if len(unique_actors) >= cluster_threshold:
            values = [t["amount_estimate"] for t in txns if t["amount_estimate"]]
            dates = [t["execution_date"] for t in txns if t["execution_date"]]
            alerts.append(
                ClusterBuyAlert(
                    ticker=ticker.upper(),
                    unique_buyers=len(unique_actors),
                    total_buys=len(txns),
                    actors=sorted(unique_actors),
                    total_value=sum(values) if values else None,
                    earliest_date=min(dates) if dates else "",
                    latest_date=max(dates) if dates else "",
                    source="insider",
                )
            )

    alerts.sort(key=lambda a: a.unique_buyers, reverse=True)
    return alerts


def _find_strong_signals(
    conn: sqlite3.Connection, source: str, min_confidence: float
) -> list[StrongSignal]:
    """Find bullish signals with sufficient confidence."""
    rows = conn.execute(
        """
        SELECT subject_key, score, confidence, included_count, lookback_window
        FROM signal_results
        WHERE source = ?
          AND label = 'bullish'
          AND confidence >= ?
          AND included_count >= 2
        ORDER BY confidence DESC, score DESC
        """,
        (source, min_confidence),
    ).fetchall()

    # Deduplicate by ticker — keep the window with highest confidence
    best_by_ticker: dict[str, dict] = {}
    for row in rows:
        ticker = row["subject_key"].replace("entity:", "").upper()
        existing = best_by_ticker.get(ticker)
        if existing is None or row["confidence"] > existing["confidence"]:
            best_by_ticker[ticker] = dict(row)
    return [
        StrongSignal(
            ticker=row["subject_key"].replace("entity:", "").upper(),
            source=source,
            label="bullish",
            score=row["score"],
            confidence=row["confidence"],
            included_count=row["included_count"],
            lookback_window=row["lookback_window"],
        )
        for row in best_by_ticker.values()
    ]


def _find_cross_source(conn: sqlite3.Connection) -> list[CrossSourceSignal]:
    """Find combined overlay results."""
    rows = conn.execute(
        """
        SELECT subject_key, insider_score, congress_score,
               insider_confidence, congress_confidence,
               overlay_outcome, strength_tier, label
        FROM combined_results
        ORDER BY ABS(score) DESC
        """
    ).fetchall()

    return [
        CrossSourceSignal(
            ticker=row["subject_key"].replace("entity:", "").upper(),
            insider_label="bearish" if row["insider_score"] < 0 else "bullish",
            insider_score=row["insider_score"],
            insider_confidence=row["insider_confidence"],
            congress_label="bearish" if row["congress_score"] < 0 else "bullish",
            congress_score=row["congress_score"],
            congress_confidence=row["congress_confidence"],
            overlay_outcome=row["overlay_outcome"],
            strength_tier=row["strength_tier"],
        )
        for row in rows
    ]


def _find_anomalous_activity(
    conn: sqlite3.Connection,
    reference_date: datetime,
    recent_days: int = 30,
    history_months: int = 12,
) -> list[AnomalyAlert]:
    """Detect unusual insider buying — first-time buys or elevated activity."""
    cutoff = reference_date.strftime("%Y-%m-%d")
    from datetime import timedelta
    recent_start = (reference_date - timedelta(days=recent_days)).strftime("%Y-%m-%d")
    history_start = (reference_date - timedelta(days=history_months * 30)).strftime("%Y-%m-%d")

    # Recent buys
    recent_rows = conn.execute(
        """
        SELECT ticker, actor_name, execution_date
        FROM normalized_transactions
        WHERE source = 'insider' AND include_in_signal = 1 AND direction = 'BUY'
          AND ticker IS NOT NULL AND execution_date >= ? AND execution_date <= ?
        """,
        (recent_start, cutoff),
    ).fetchall()

    if not recent_rows:
        return []

    recent_by_ticker: dict[str, list[dict]] = defaultdict(list)
    for row in recent_rows:
        recent_by_ticker[row["ticker"]].append(dict(row))

    # Historical buys (before the recent window)
    history_rows = conn.execute(
        """
        SELECT ticker, execution_date
        FROM normalized_transactions
        WHERE source = 'insider' AND include_in_signal = 1 AND direction = 'BUY'
          AND ticker IS NOT NULL AND execution_date >= ? AND execution_date < ?
        """,
        (history_start, recent_start),
    ).fetchall()

    history_by_ticker: dict[str, list[str]] = defaultdict(list)
    for row in history_rows:
        history_by_ticker[row["ticker"]].append(row["execution_date"])

    alerts = []
    for ticker, recent_txns in sorted(recent_by_ticker.items()):
        history = history_by_ticker.get(ticker, [])
        historical_count = len(history)
        monthly_avg = historical_count / max(1, history_months)
        current_count = len(recent_txns)
        actors = sorted({t["actor_name"] for t in recent_txns if t["actor_name"]})

        if historical_count == 0:
            # First buy in the history period
            alerts.append(AnomalyAlert(
                ticker=ticker.upper(),
                alert_type="first_buy_in_period",
                current_buys=current_count,
                historical_monthly_avg=0.0,
                months_since_last_buy=None,
                actors=actors,
            ))
        elif monthly_avg > 0 and current_count > 2 * monthly_avg * (recent_days / 30):
            # Elevated activity: current > 2x expected
            last_date = max(history) if history else None
            months_since = None
            if last_date:
                try:
                    last_dt = datetime.strptime(last_date, "%Y-%m-%d")
                    months_since = max(0, int((reference_date - last_dt).days / 30))
                except (ValueError, TypeError):
                    pass
            alerts.append(AnomalyAlert(
                ticker=ticker.upper(),
                alert_type="elevated_activity",
                current_buys=current_count,
                historical_monthly_avg=round(monthly_avg, 2),
                months_since_last_buy=months_since,
                actors=actors,
            ))

    alerts.sort(key=lambda a: a.current_buys, reverse=True)
    return alerts


def _anomaly_to_dict(a: AnomalyAlert) -> dict:
    from dataclasses import asdict
    return asdict(a)


def _build_stats(conn: sqlite3.Connection) -> dict:
    """Summary statistics for the brief."""
    insider_count = conn.execute(
        "SELECT COUNT(*) FROM signal_results WHERE source='insider' AND label != 'insufficient'"
    ).fetchone()[0]
    congress_count = conn.execute(
        "SELECT COUNT(*) FROM signal_results WHERE source='congress' AND label != 'insufficient'"
    ).fetchone()[0]
    combined_count = conn.execute(
        "SELECT COUNT(*) FROM combined_results"
    ).fetchone()[0]
    return {
        "insider_active_signals": insider_count,
        "congress_active_signals": congress_count,
        "combined_signals": combined_count,
    }


def _alert_to_dict(a: ClusterBuyAlert) -> dict:
    from dataclasses import asdict
    return asdict(a)


def _signal_to_dict(s: StrongSignal) -> dict:
    from dataclasses import asdict
    return asdict(s)


def _cross_to_dict(c: CrossSourceSignal) -> dict:
    from dataclasses import asdict
    return asdict(c)


def render_daily_brief_markdown(brief: dict) -> str:
    """Render the daily brief as markdown."""
    lines = [
        f"# Market Intelligence Brief — {brief['as_of_date']}",
        "",
    ]

    stats = brief["stats"]
    lines.extend([
        f"Active signals: {stats['insider_active_signals']} insider, "
        f"{stats['congress_active_signals']} congress, "
        f"{stats['combined_signals']} combined",
        "",
    ])

    # Cluster buy alerts (highest priority)
    alerts = brief["cluster_buy_alerts"]
    if alerts:
        lines.extend([
            "## Cluster Insider Buying",
            "",
            "Multiple unique insiders buying the same stock (highest-quality signal).",
            "",
        ])
        for a in alerts:
            value_str = f", total ~${a['total_value']:,.0f}" if a["total_value"] else ""
            lines.append(
                f"- **{a['ticker']}**: {a['unique_buyers']} unique buyers, "
                f"{a['total_buys']} transactions ({a['earliest_date']} to {a['latest_date']}{value_str})"
            )
            if a["actors"]:
                lines.append(f"  - Buyers: {', '.join(a['actors'][:5])}")
        lines.append("")
    else:
        lines.extend(["## Cluster Insider Buying", "", "No cluster buying detected.", ""])

    # Cross-source signals
    cross = brief["cross_source_signals"]
    if cross:
        lines.extend([
            "## Cross-Source Signals",
            "",
            "Entities with both insider and congressional trading activity.",
            "",
        ])
        for c in cross:
            tier = f" [{c['strength_tier']}]" if c["strength_tier"] else ""
            lines.append(
                f"- **{c['ticker']}**: {c['overlay_outcome']}{tier} — "
                f"insider={c['insider_label']}({c['insider_score']:+.3f}), "
                f"congress={c['congress_label']}({c['congress_score']:+.3f})"
            )
        lines.append("")

    # Strong insider buys
    insider = brief["strong_insider_buys"]
    if insider:
        lines.extend(["## Strong Insider Buys", "", ""])
        for s in insider:
            lines.append(
                f"- **{s['ticker']}**: score={s['score']:.3f}, "
                f"confidence={s['confidence']:.2f}, "
                f"{s['included_count']} transactions ({s['lookback_window']}d window)"
            )
        lines.append("")

    # Strong congress buys
    congress = brief["strong_congress_buys"]
    if congress:
        lines.extend(["## Strong Congressional Buys", "", ""])
        for s in congress:
            lines.append(
                f"- **{s['ticker']}**: score={s['score']:.3f}, "
                f"confidence={s['confidence']:.2f}, "
                f"{s['included_count']} transactions ({s['lookback_window']}d window)"
            )
        lines.append("")

    # Anomaly alerts
    anomalies = brief.get("anomaly_alerts", [])
    if anomalies:
        lines.extend(["## Anomaly Alerts", "", "Unusual insider buying activity relative to historical baseline.", ""])
        for a in anomalies:
            if a["alert_type"] == "first_buy_in_period":
                lines.append(
                    f"- **{a['ticker']}**: First insider buy in 12+ months — "
                    f"{a['current_buys']} transaction(s) by {', '.join(a['actors'][:3])}"
                )
            else:
                lines.append(
                    f"- **{a['ticker']}**: Elevated activity — "
                    f"{a['current_buys']} buys vs {a['historical_monthly_avg']:.1f}/month historical avg"
                )
        lines.append("")

    # Sector summary
    sector_summary = brief.get("sector_summary")
    if sector_summary:
        lines.extend(["## Sector Summary", ""])
        lines.append("| Sector | Bullish | Bearish | Net | Top Tickers |")
        lines.append("|--------|---------|---------|-----|-------------|")
        for sector, data in sorted(sector_summary.items(), key=lambda x: abs(x[1]["net_sentiment"]), reverse=True):
            top = ", ".join(data["top_tickers"][:3])
            lines.append(f"| {sector} | {data['bullish_count']} | {data['bearish_count']} | {data['net_sentiment']:+d} | {top} |")
        lines.append("")

    if not alerts and not cross and not insider and not congress and not anomalies:
        lines.append("*No high-signal events detected.*")

    return "\n".join(lines) + "\n"
