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

import json
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
    rank_info: dict | None = None


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
    include_committees: bool = False,
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
    _enrich_with_rank(conn, strong_insider, reference_date)
    strong_congress = _find_strong_signals(conn, "congress", min_confidence)

    # 3. Cross-source signals from combined results
    cross_source = _find_cross_source(conn)

    # 4. Anomaly detection: first-time buying or elevated activity
    anomalies = _find_anomalous_activity(conn, reference_date, lookback_days)

    # 5. Insider Participation Index (market-level breadth)
    participation = _compute_participation_index(conn, reference_date)

    # 6. Earnings proximity alerts
    earnings_alerts = _find_earnings_proximity_alerts(conn, reference_date)

    # 7. Committee sector rotation
    committee_rotations = _find_committee_rotation_signals(conn, reference_date)

    # 8. Summary stats
    stats = _build_stats(conn)

    conn.close()

    return {
        "as_of_date": reference_date.strftime("%Y-%m-%d"),
        "cluster_buy_alerts": [_alert_to_dict(a) for a in cluster_alerts],
        "strong_insider_buys": [_signal_to_dict(s) for s in strong_insider],
        "strong_congress_buys": [_signal_to_dict(s) for s in strong_congress],
        "cross_source_signals": [_cross_to_dict(c) for c in cross_source],
        "anomaly_alerts": [_anomaly_to_dict(a) for a in anomalies],
        "participation_index": participation,
        "earnings_proximity_alerts": earnings_alerts,
        "committee_rotation_signals": committee_rotations,
        "stats": stats,
        "sector_summary": _build_sector_summary(
            db_path, strong_insider, strong_congress, cross_source, cluster_alerts
        ) if include_sectors else None,
        "committee_correlated_trades": _build_committee_correlation(db_path) if include_committees else None,
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


def _build_committee_correlation(db_path: str) -> list[dict] | None:
    """Find trades where member's committee sector matches the traded stock's sector."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT ticker, actor_name, provenance_payload, direction FROM normalized_transactions WHERE source = 'congress' AND include_in_signal = 1"
        ).fetchall()
        conn.close()

        correlated = []
        for row in rows:
            try:
                payload = json.loads(row["provenance_payload"]) if isinstance(row["provenance_payload"], str) else row["provenance_payload"]
            except (json.JSONDecodeError, TypeError):
                continue
            if payload.get("committee_sector_match"):
                correlated.append({
                    "ticker": row["ticker"],
                    "actor_name": row["actor_name"],
                    "direction": row["direction"],
                    "committees": payload.get("committees", []),
                    "committee_sectors": payload.get("committee_sectors", []),
                })
        return correlated if correlated else None
    except Exception:
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


def _enrich_with_rank(conn: sqlite3.Connection, signals: list[StrongSignal], reference_date: datetime) -> None:
    """Add rank_info to each insider signal from underlying transaction data."""
    from signals.insider.engine import rank_transaction

    for signal in signals:
        ticker = signal.ticker
        rows = conn.execute(
            """
            SELECT actor_type, owner_type, execution_date, provenance_payload
            FROM normalized_transactions
            WHERE source = 'insider' AND include_in_signal = 1 AND direction = 'BUY'
              AND ticker = ? ORDER BY execution_date DESC LIMIT 1
            """,
            (ticker,),
        ).fetchall()

        if not rows:
            continue

        row = rows[0]
        # Build a txn dict compatible with rank_transaction
        try:
            payload = json.loads(row["provenance_payload"]) if isinstance(row["provenance_payload"], str) else {}
        except (json.JSONDecodeError, TypeError):
            payload = {}

        txn = {
            "role_class": row["actor_type"],
            "is_likely_planned": 0,  # if it passed filtering, it's not a 10b5-1
            "ownership_nature": "D" if row["owner_type"] == "direct" else "I",
            "pct_holdings_changed": None,  # not easily available from normalized_transactions
            "transaction_date": row["execution_date"],
        }
        rank_info = rank_transaction(txn, reference_date)
        signal.rank_info = rank_info


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


def _compute_participation_index(
    conn: sqlite3.Connection,
    reference_date: datetime,
    window_days: int = 90,
    universe_size: int = 504,
) -> dict:
    """Compute Insider Participation Index — % of S&P 500 with insider buying.

    A market-level breadth indicator. High participation during price declines
    = bullish divergence. Low participation during rallies = bearish warning.
    """
    from datetime import timedelta
    cutoff = reference_date.strftime("%Y-%m-%d")
    start = (reference_date - timedelta(days=window_days)).strftime("%Y-%m-%d")

    # Current window
    count = conn.execute(
        """
        SELECT COUNT(DISTINCT ticker) FROM normalized_transactions
        WHERE source = 'insider' AND include_in_signal = 1 AND direction = 'BUY'
          AND ticker IS NOT NULL AND execution_date >= ? AND execution_date <= ?
        """,
        (start, cutoff),
    ).fetchone()[0]

    rate = count / universe_size if universe_size > 0 else 0.0

    # Historical average (all available data)
    hist_row = conn.execute(
        """
        SELECT COUNT(DISTINCT ticker || ':' || SUBSTR(execution_date, 1, 7)) as ticker_months,
               COUNT(DISTINCT SUBSTR(execution_date, 1, 7)) as months
        FROM normalized_transactions
        WHERE source = 'insider' AND include_in_signal = 1 AND direction = 'BUY'
          AND ticker IS NOT NULL
        """
    ).fetchone()

    ticker_months = hist_row[0] or 0
    months = hist_row[1] or 1
    historical_avg_monthly = ticker_months / months / universe_size if months > 0 else 0.0
    # Scale to window
    historical_avg = historical_avg_monthly * (window_days / 30)

    return {
        "rate": round(rate, 4),
        "count": count,
        "universe_size": universe_size,
        "window_days": window_days,
        "historical_avg": round(historical_avg, 4),
        "above_average": rate > historical_avg,
        "context": "bullish" if rate > historical_avg else "bearish",
    }


def _find_earnings_proximity_alerts(
    conn: sqlite3.Connection,
    reference_date: datetime,
    proximity_days: int = 30,
) -> list[dict]:
    """Flag insider buys near known earnings dates as high-conviction."""
    from datetime import timedelta

    try:
        import yfinance as yf
    except ImportError:
        return []

    cutoff = reference_date.strftime("%Y-%m-%d")
    start = (reference_date - timedelta(days=90)).strftime("%Y-%m-%d")

    rows = conn.execute(
        """
        SELECT DISTINCT ticker, actor_name, execution_date
        FROM normalized_transactions
        WHERE source = 'insider' AND include_in_signal = 1 AND direction = 'BUY'
          AND ticker IS NOT NULL AND execution_date >= ? AND execution_date <= ?
        """,
        (start, cutoff),
    ).fetchall()

    alerts = []
    seen_tickers = set()
    for row in rows:
        ticker = row["ticker"]
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)

        try:
            t = yf.Ticker(ticker)
            cal = t.calendar
            if cal is None or (hasattr(cal, 'empty') and cal.empty):
                continue
            # calendar may be a DataFrame or dict depending on yfinance version
            if hasattr(cal, 'iloc'):
                earnings_date = cal.iloc[0, 0] if cal.shape[1] > 0 else None
            elif isinstance(cal, dict):
                earnings_date = cal.get("Earnings Date", [None])[0] if "Earnings Date" in cal else None
            else:
                continue

            if earnings_date is None:
                continue

            # Convert to date for comparison
            if hasattr(earnings_date, 'date'):
                earnings_dt = earnings_date
            else:
                earnings_dt = datetime.strptime(str(earnings_date)[:10], "%Y-%m-%d")

            exec_dt = datetime.strptime(row["execution_date"], "%Y-%m-%d")
            days_to_earnings = (earnings_dt - exec_dt).days

            if 0 < days_to_earnings <= proximity_days:
                alerts.append({
                    "ticker": ticker.upper(),
                    "actor_name": row["actor_name"],
                    "execution_date": row["execution_date"],
                    "earnings_date": str(earnings_date)[:10],
                    "days_to_earnings": days_to_earnings,
                })
        except Exception:
            continue

    alerts.sort(key=lambda a: a["days_to_earnings"])
    return alerts


def _find_committee_rotation_signals(
    conn: sqlite3.Connection,
    reference_date: datetime,
    lookback_days: int = 180,
    recent_days: int = 30,
) -> list[dict]:
    """Detect when committee members collectively shift direction in their regulated sectors."""
    from signals.congress.committees import COMMITTEE_SECTOR_MAP
    from datetime import timedelta

    cutoff = reference_date.strftime("%Y-%m-%d")
    recent_start = (reference_date - timedelta(days=recent_days)).strftime("%Y-%m-%d")
    prior_start = (reference_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    rows = conn.execute(
        """
        SELECT ticker, direction, execution_date, provenance_payload, actor_name
        FROM normalized_transactions
        WHERE source = 'congress' AND include_in_signal = 1
          AND execution_date >= ? AND execution_date <= ?
          AND provenance_payload LIKE '%committee%'
        """,
        (prior_start, cutoff),
    ).fetchall()

    if not rows:
        return []

    # Parse provenance and group by committee
    committee_trades: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        try:
            payload = json.loads(row["provenance_payload"]) if isinstance(row["provenance_payload"], str) else {}
        except (json.JSONDecodeError, TypeError):
            continue

        committees = payload.get("committees", [])
        committee_sectors = payload.get("committee_sectors", [])
        if not committees or not committee_sectors:
            continue

        # Check if the traded stock's sector matches any committee sector
        if not payload.get("committee_sector_match"):
            continue

        for code in committees:
            committee_trades[code].append({
                "ticker": row["ticker"],
                "direction": row["direction"],
                "execution_date": row["execution_date"],
                "actor_name": row["actor_name"],
            })

    # Analyze rotation per committee
    rotations = []
    for code, trades in committee_trades.items():
        if len(code) > 4:  # skip subcommittees
            continue

        recent = [t for t in trades if t["execution_date"] >= recent_start]
        prior = [t for t in trades if t["execution_date"] < recent_start]

        if not recent or not prior:
            continue

        recent_buys = sum(1 for t in recent if t["direction"] == "BUY")
        recent_sells = sum(1 for t in recent if t["direction"] == "SELL")
        prior_buys = sum(1 for t in prior if t["direction"] == "BUY")
        prior_sells = sum(1 for t in prior if t["direction"] == "SELL")

        recent_total = recent_buys + recent_sells
        prior_total = prior_buys + prior_sells
        if recent_total == 0 or prior_total == 0:
            continue

        recent_direction = "BUY" if recent_buys > recent_sells else "SELL" if recent_sells > recent_buys else "NEUTRAL"
        prior_direction = "BUY" if prior_buys > prior_sells else "SELL" if prior_sells > prior_buys else "NEUTRAL"

        # Only flag if direction actually flipped
        if recent_direction != prior_direction and recent_direction != "NEUTRAL" and prior_direction != "NEUTRAL":
            sectors = COMMITTEE_SECTOR_MAP.get(code, [])
            members = sorted({t["actor_name"] for t in recent if t["actor_name"]})
            rotations.append({
                "committee_code": code.upper(),
                "sectors": sectors,
                "prior_direction": prior_direction,
                "recent_direction": recent_direction,
                "prior_buys": prior_buys,
                "prior_sells": prior_sells,
                "recent_buys": recent_buys,
                "recent_sells": recent_sells,
                "recent_members": members[:5],
            })

    return rotations


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
        lines.extend(["## Strong Insider Buys", ""])
        for s in insider:
            rank_info = s.get("rank_info")
            if rank_info:
                lines.append(f"**{s['ticker']}** — Rank: {rank_info['rank']}/{rank_info['max_rank']}")
                for f in rank_info.get("factors", []):
                    lines.append(f"  ✓ {f}")
                for f in rank_info.get("factors_missing", []):
                    lines.append(f"  ○ {f}")
                lines.append("")
            else:
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

    # Committee-correlated trades
    committee_trades = brief.get("committee_correlated_trades")
    if committee_trades:
        lines.extend(["## Committee-Correlated Trades", "", "Trades where member's committee jurisdiction matches the stock's sector.", ""])
        for ct in committee_trades[:10]:
            committees = ", ".join(ct.get("committees", [])[:3])
            lines.append(f"- **{ct['ticker']}** ({ct['direction']}) by {ct.get('actor_name', '?')} — committees: {committees}")
        lines.append("")

    # Insider Participation Index
    participation = brief.get("participation_index")
    if participation:
        rate_pct = participation["rate"] * 100
        avg_pct = participation["historical_avg"] * 100
        context = participation["context"].upper()
        lines.extend([
            "## Insider Participation Index",
            "",
            f"{participation['count']} of {participation['universe_size']} S&P 500 companies "
            f"({rate_pct:.1f}%) have insider buying in the last {participation['window_days']} days.",
            f"Historical average: {avg_pct:.1f}%. Current: **{context}** context.",
            "",
        ])

    # Pre-Earnings Insider Buys
    earnings = brief.get("earnings_proximity_alerts", [])
    if earnings:
        lines.extend(["## Pre-Earnings Insider Buys", "", "Insider purchases within 30 days of known earnings dates.", ""])
        for e in earnings:
            lines.append(
                f"- **{e['ticker']}**: {e['actor_name']} bought {e['days_to_earnings']}d before "
                f"earnings ({e['earnings_date']}) — high conviction"
            )
        lines.append("")

    # Committee Rotation Alerts
    rotations = brief.get("committee_rotation_signals", [])
    if rotations:
        lines.extend(["## Committee Rotation Alerts", "", "Committees where members shifted direction in their regulated sectors.", ""])
        for r in rotations:
            sectors = ", ".join(r["sectors"][:2]) if r["sectors"] else "?"
            members = ", ".join(r["recent_members"][:3])
            lines.append(
                f"- **{r['committee_code']}** ({sectors}): {r['prior_direction']} → {r['recent_direction']}"
            )
            lines.append(
                f"  Prior: {r['prior_buys']} buys, {r['prior_sells']} sells. "
                f"Recent: {r['recent_buys']} buys, {r['recent_sells']} sells. "
                f"Members: {members}"
            )
        lines.append("")

    if not alerts and not cross and not insider and not congress and not anomalies:
        lines.append("*No high-signal events detected.*")

    return "\n".join(lines) + "\n"
