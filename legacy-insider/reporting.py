"""
Reporting module.

Generates:
1. CLI text reports
2. HTML dashboard
3. Top bullish/bearish company lists
4. Most informative filings
5. Excluded companies/transactions report
"""

import html
import json
import logging
import os
import time
from datetime import datetime

import config
from db import get_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI text report
# ---------------------------------------------------------------------------
def generate_cli_report(db_path: str = None) -> str:
    """Generate a full text report for CLI output."""
    sections = []

    with get_connection(db_path) as conn:
        # Aggregate index
        indices = conn.execute(
            "SELECT * FROM aggregate_index ORDER BY window_days"
        ).fetchall()

        # Company scores
        scores = conn.execute(
            "SELECT * FROM company_scores ORDER BY window_days, score DESC"
        ).fetchall()

        # Companies
        companies = conn.execute(
            "SELECT * FROM companies ORDER BY fortune_rank"
        ).fetchall()

        # Transaction stats
        total_txns = conn.execute("SELECT COUNT(*) as c FROM transactions").fetchone()["c"]
        signal_txns = conn.execute(
            "SELECT COUNT(*) as c FROM transactions WHERE include_in_signal = 1"
        ).fetchone()["c"]
        excluded_txns = conn.execute(
            "SELECT COUNT(*) as c FROM transactions WHERE include_in_signal = 0"
        ).fetchone()["c"]

        # Exclusion reasons
        exclusion_reasons = conn.execute("""
            SELECT exclusion_reason, COUNT(*) as c
            FROM transactions
            WHERE include_in_signal = 0 AND exclusion_reason IS NOT NULL
            GROUP BY exclusion_reason
            ORDER BY c DESC
            LIMIT 15
        """).fetchall()

    # Header
    sections.append("=" * 72)
    sections.append("  INSIDER TRADING SIGNAL ENGINE — REPORT")
    sections.append(f"  Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    sections.append("=" * 72)

    # Data summary
    sections.append("\n## DATA SUMMARY")
    sections.append(f"  Companies in universe: {len(companies)}")
    sections.append(f"  Total transactions parsed: {total_txns}")
    sections.append(f"  Transactions in signal: {signal_txns}")
    sections.append(f"  Transactions excluded: {excluded_txns}")

    if exclusion_reasons:
        sections.append("\n  Top exclusion reasons:")
        for row in exclusion_reasons:
            sections.append(f"    {row['exclusion_reason']}: {row['c']}")

    # Aggregate index
    sections.append("\n" + "=" * 72)
    sections.append("  FORTUNE 500 EXECUTIVE RISK APPETITE INDEX")
    sections.append("=" * 72)
    sections.append(
        "  NOTE: This index reflects insider trading patterns as a proxy for"
    )
    sections.append(
        "  executive risk appetite. It is NOT a direct measure of economic"
    )
    sections.append(
        "  outlook or stock market direction. Insider buying is generally"
    )
    sections.append(
        "  more informative than insider selling."
    )

    for idx in indices:
        window = idx["window_days"]
        sections.append(f"\n  --- {window}-Day Window ---")
        sections.append(
            f"  Sector-Balanced Index:  {idx['sector_balanced_index']:+.4f}"
        )
        sections.append(
            f"  Raw Risk Appetite:     {idx['risk_appetite_index']:+.4f}"
        )
        sections.append(
            f"  CEO/CFO-Only Index:    {idx['ceo_cfo_only_index']:+.4f}"
        )
        sections.append(
            f"  Bullish Breadth:       {idx['bullish_breadth']:.1%}"
        )
        sections.append(
            f"  Bearish Breadth:       {idx['bearish_breadth']:.1%}"
        )
        sections.append(
            f"  Cyclical Sectors:      {idx['cyclical_score']:+.4f}"
        )
        sections.append(
            f"  Defensive Sectors:     {idx['defensive_score']:+.4f}"
        )
        sections.append(
            f"  Companies w/ Signal:   {idx['companies_with_signal']} / {idx['total_companies']}"
        )
        sections.append(
            f"  Insufficient Data:     {idx['insufficient_pct']:.1%}"
        )

        # Sector breakdown
        breakdown = json.loads(idx["sector_breakdown"]) if idx["sector_breakdown"] else {}
        if breakdown:
            sections.append("  Sector Breakdown:")
            for sector in sorted(breakdown.keys()):
                marker = "●" if sector in config.CYCLICAL_SECTORS else "○"
                sections.append(
                    f"    {marker} {sector:30s} {breakdown[sector]:+.4f}"
                )

    # Company-level scores by window
    for window in config.ANALYSIS_WINDOWS_DAYS:
        window_scores = [s for s in scores if s["window_days"] == window]
        if not window_scores:
            continue

        sections.append(f"\n{'=' * 72}")
        sections.append(f"  COMPANY SIGNALS — {window}-DAY WINDOW")
        sections.append("=" * 72)

        # Top bullish
        bullish = [s for s in window_scores if s["signal"] == "bullish"]
        if bullish:
            sections.append(f"\n  TOP BULLISH ({len(bullish)}):")
            for s in bullish[:10]:
                sections.append(
                    f"    {s['ticker']:8s} score={s['score']:+.4f} "
                    f"conf={s['confidence']:.2f}/{s['confidence_tier']} "
                    f"buys={s['buy_count']} sells={s['sell_count']}"
                )

        # Top bearish
        bearish = [s for s in window_scores if s["signal"] == "bearish"]
        bearish.sort(key=lambda x: x["score"])
        if bearish:
            sections.append(f"\n  TOP BEARISH ({len(bearish)}):")
            for s in bearish[:10]:
                sections.append(
                    f"    {s['ticker']:8s} score={s['score']:+.4f} "
                    f"conf={s['confidence']:.2f}/{s['confidence_tier']} "
                    f"buys={s['buy_count']} sells={s['sell_count']}"
                )

        # Neutral
        neutral = [s for s in window_scores if s["signal"] == "neutral"]
        if neutral:
            sections.append(f"\n  NEUTRAL ({len(neutral)}):")
            for s in neutral[:10]:
                sections.append(
                    f"    {s['ticker']:8s} score={s['score']:+.4f} "
                    f"conf={s['confidence']:.2f}/{s['confidence_tier']}"
                )

        # Insufficient
        insuf = [s for s in window_scores if s["signal"] == "insufficient"]
        if insuf:
            sections.append(
                f"\n  INSUFFICIENT EVIDENCE ({len(insuf)}): "
                + ", ".join(s["ticker"] for s in insuf)
            )

    # Explanations for top signals (90d window)
    sections.append(f"\n{'=' * 72}")
    sections.append("  SIGNAL EXPLANATIONS (90-Day Window)")
    sections.append("=" * 72)
    window_90 = [s for s in scores if s["window_days"] == 90 and s["signal"] != "insufficient"]
    for s in window_90[:15]:
        sections.append(f"\n  {s['ticker']}:")
        sections.append(f"    {s['explanation']}")

    # Footer
    sections.append(f"\n{'=' * 72}")
    sections.append("  METHODOLOGY NOTES")
    sections.append("=" * 72)
    sections.append("  - Source: SEC EDGAR Form 4 filings (executed transactions only)")
    sections.append("  - Includes: CEO, CFO, Chair, President, COO open-market trades")
    sections.append("  - Excludes: affiliates, funds, 10% holders, option exercises,")
    sections.append("    tax withholding, gifts, awards, derivative transactions")
    sections.append(f"  - Buy/sell asymmetry: buys weighted {config.DIRECTION_WEIGHT_BUY}x, "
                    f"sells weighted {abs(config.DIRECTION_WEIGHT_SELL)}x")
    sections.append(f"  - 10b5-1 planned trades discounted to {config.PLANNED_TRADE_DISCOUNT}x")
    sections.append(f"  - Per-insider saturation cap: {config.PER_INSIDER_SATURATION_CAP:.0%}")
    sections.append(f"  - Recency half-life: {config.RECENCY_HALF_LIFE_DAYS} days")
    sections.append("  - Sector-balanced index: equal-weight across GICS sectors")
    sections.append("  - 'Insufficient evidence' when confidence < "
                    f"{config.CONFIDENCE_INSUFFICIENT}")
    sections.append("  - See methodology.md for full documentation")

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# HTML dashboard
# ---------------------------------------------------------------------------
def generate_html_dashboard(db_path: str = None) -> str:
    """Generate an HTML dashboard report."""
    with get_connection(db_path) as conn:
        indices = conn.execute(
            "SELECT * FROM aggregate_index ORDER BY window_days"
        ).fetchall()
        scores = conn.execute(
            "SELECT * FROM company_scores ORDER BY window_days, score DESC"
        ).fetchall()
        companies = conn.execute(
            "SELECT * FROM companies ORDER BY fortune_rank"
        ).fetchall()

    generated = time.strftime("%Y-%m-%d %H:%M:%S")

    html_parts = [
        "<!DOCTYPE html>",
        "<html lang='en'>",
        "<head>",
        "<meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<title>Insider Trading Signal Engine</title>",
        "<style>",
        _dashboard_css(),
        "</style>",
        "</head>",
        "<body>",
        "<div class='container'>",
        f"<h1>Insider Trading Signal Engine</h1>",
        f"<p class='subtitle'>Generated: {generated}</p>",
        "<div class='disclaimer'>",
        "<strong>Disclaimer:</strong> This index reflects insider trading patterns "
        "as a proxy for executive risk appetite. It is NOT a direct measure of "
        "economic outlook or stock market direction. Insider buying is generally "
        "more informative than insider selling. See methodology for details.",
        "</div>",
    ]

    # Aggregate index cards
    for idx in indices:
        window = idx["window_days"]
        sbi = idx["sector_balanced_index"]
        signal_class = "bullish" if sbi > 0.15 else "bearish" if sbi < -0.15 else "neutral"

        html_parts.append(f"<h2>{window}-Day Executive Risk Appetite</h2>")
        html_parts.append("<div class='cards'>")

        html_parts.append(
            _card("Sector-Balanced Index", f"{sbi:+.4f}", signal_class)
        )
        html_parts.append(
            _card("CEO/CFO-Only", f"{idx['ceo_cfo_only_index']:+.4f}", "")
        )
        html_parts.append(
            _card("Bullish Breadth", f"{idx['bullish_breadth']:.1%}", "")
        )
        html_parts.append(
            _card("Bearish Breadth", f"{idx['bearish_breadth']:.1%}", "")
        )
        html_parts.append(
            _card("Cyclical", f"{idx['cyclical_score']:+.4f}", "")
        )
        html_parts.append(
            _card("Defensive", f"{idx['defensive_score']:+.4f}", "")
        )
        html_parts.append(
            _card(
                "Coverage",
                f"{idx['companies_with_signal']}/{idx['total_companies']}",
                "",
            )
        )
        html_parts.append("</div>")

        # Sector breakdown table
        breakdown = json.loads(idx["sector_breakdown"]) if idx["sector_breakdown"] else {}
        if breakdown:
            html_parts.append("<h3>Sector Breakdown</h3>")
            html_parts.append("<table><thead><tr>")
            html_parts.append("<th>Sector</th><th>Type</th><th>Score</th><th>Bar</th>")
            html_parts.append("</tr></thead><tbody>")
            for sector in sorted(breakdown.keys()):
                s = breakdown[sector]
                stype = "Cyclical" if sector in config.CYCLICAL_SECTORS else "Defensive"
                bar_width = min(abs(s) * 200, 100)
                bar_color = "#22c55e" if s > 0 else "#ef4444" if s < 0 else "#9ca3af"
                bar_dir = "right" if s >= 0 else "left"
                html_parts.append(
                    f"<tr><td>{sector}</td><td>{stype}</td>"
                    f"<td class='{'bullish' if s > 0 else 'bearish' if s < 0 else ''}'>"
                    f"{s:+.4f}</td>"
                    f"<td><div class='bar' style='width:{bar_width}%;background:{bar_color};"
                    f"float:{bar_dir}'></div></td></tr>"
                )
            html_parts.append("</tbody></table>")

    # Company scores table (90d default view)
    html_parts.append("<h2>Company Signals (90-Day)</h2>")
    window_90 = [s for s in scores if s["window_days"] == 90]
    if window_90:
        html_parts.append("<table><thead><tr>")
        html_parts.append(
            "<th>Ticker</th><th>Signal</th><th>Score</th>"
            "<th>Confidence</th><th>Buys</th><th>Sells</th>"
            "<th>Buyers</th><th>Sellers</th>"
        )
        html_parts.append("</tr></thead><tbody>")
        for s in window_90:
            sig = s["signal"]
            sig_class = sig if sig in ("bullish", "bearish", "neutral") else "insufficient"
            html_parts.append(
                f"<tr><td><strong>{_e(s['ticker'])}</strong></td>"
                f"<td class='{_e(sig_class)}'>{_e(sig.upper())}</td>"
                f"<td>{s['score']:+.4f}</td>"
                f"<td>{s['confidence']:.2f} ({_e(s['confidence_tier'])})</td>"
                f"<td>{s['buy_count']}</td><td>{s['sell_count']}</td>"
                f"<td>{s['unique_buyers']}</td><td>{s['unique_sellers']}</td></tr>"
            )
        html_parts.append("</tbody></table>")

    # Explanations
    html_parts.append("<h2>Signal Explanations</h2>")
    for s in window_90:
        if s["signal"] != "insufficient":
            sig_class = s["signal"]
            html_parts.append(
                f"<div class='explanation'>"
                f"<strong class='{_e(sig_class)}'>{_e(s['ticker'])}</strong>: "
                f"{_e(s['explanation'])}</div>"
            )

    html_parts.extend([
        "<div class='footer'>",
        "<p>Source: SEC EDGAR Form 4 filings. See methodology.md for full documentation.</p>",
        "</div>",
        "</div>",
        "</body>",
        "</html>",
    ])

    return "\n".join(html_parts)


def _e(text) -> str:
    """HTML-escape a string for safe interpolation."""
    return html.escape(str(text)) if text is not None else ""


def _card(title: str, value: str, css_class: str) -> str:
    return (
        f"<div class='card {_e(css_class)}'>"
        f"<div class='card-title'>{_e(title)}</div>"
        f"<div class='card-value'>{_e(value)}</div>"
        f"</div>"
    )


def _dashboard_css() -> str:
    return """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #f8fafc; color: #1e293b; margin: 0; padding: 20px; }
.container { max-width: 1100px; margin: 0 auto; }
h1 { color: #0f172a; margin-bottom: 4px; }
h2 { color: #334155; margin-top: 32px; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px; }
h3 { color: #475569; }
.subtitle { color: #64748b; margin-top: 0; }
.disclaimer { background: #fffbeb; border: 1px solid #fbbf24; border-radius: 6px;
              padding: 12px 16px; margin: 16px 0; font-size: 0.9em; color: #92400e; }
.cards { display: flex; flex-wrap: wrap; gap: 12px; }
.card { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px;
        padding: 16px; min-width: 130px; flex: 1; }
.card-title { font-size: 0.8em; color: #64748b; text-transform: uppercase; }
.card-value { font-size: 1.4em; font-weight: 700; margin-top: 4px; }
.card.bullish .card-value { color: #16a34a; }
.card.bearish .card-value { color: #dc2626; }
.card.neutral .card-value { color: #64748b; }
table { width: 100%; border-collapse: collapse; background: #fff;
        border-radius: 8px; overflow: hidden; margin: 12px 0; }
th { background: #f1f5f9; text-align: left; padding: 10px 12px; font-size: 0.85em;
     color: #475569; text-transform: uppercase; }
td { padding: 8px 12px; border-top: 1px solid #f1f5f9; font-size: 0.95em; }
tr:hover { background: #f8fafc; }
.bullish { color: #16a34a; font-weight: 600; }
.bearish { color: #dc2626; font-weight: 600; }
.neutral { color: #64748b; }
.insufficient { color: #9ca3af; font-style: italic; }
.bar { height: 14px; border-radius: 3px; min-width: 2px; }
.explanation { background: #fff; border: 1px solid #e2e8f0; border-radius: 6px;
               padding: 10px 14px; margin: 6px 0; font-size: 0.9em; }
.footer { margin-top: 40px; padding-top: 16px; border-top: 1px solid #e2e8f0;
          color: #94a3b8; font-size: 0.85em; }
"""


# ---------------------------------------------------------------------------
# Save reports
# ---------------------------------------------------------------------------
def save_reports(db_path: str = None, output_dir: str = None):
    """Generate and save all reports."""
    output_dir = output_dir or os.path.join(config.PROJECT_ROOT, "output")
    os.makedirs(output_dir, exist_ok=True)

    # CLI report
    cli_report = generate_cli_report(db_path)
    cli_path = os.path.join(output_dir, "report.txt")
    with open(cli_path, "w") as f:
        f.write(cli_report)
    logger.info(f"CLI report saved: {cli_path}")

    # HTML dashboard
    html = generate_html_dashboard(db_path)
    html_path = os.path.join(output_dir, "dashboard.html")
    with open(html_path, "w") as f:
        f.write(html)
    logger.info(f"HTML dashboard saved: {html_path}")

    return {
        "cli_report_path": cli_path,
        "html_dashboard_path": html_path,
        "cli_report": cli_report,
    }
