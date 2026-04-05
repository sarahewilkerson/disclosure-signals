"""FastAPI web dashboard for Disclosure Signals.

Serves the daily brief as a live web page, querying the derived DB
at request time for always-current data.

Usage:
    signals serve --port 8001 --db /tmp/disclosure-monitor-sp500-v2.db

Routes:
    GET /           — Daily brief rendered as HTML
    GET /api/brief  — Brief as JSON
    GET /api/signals — Signal results with optional filtering
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(
    title="Disclosure Signals",
    description="Market intelligence from insider + congressional trading disclosures",
    version="1.0",
)

def _get_db_path() -> str:
    """Get DB path from environment at request time (not module import time)."""
    return os.environ.get("SIGNALS_DB_PATH", "/tmp/disclosure-monitor-sp500-v2.db")


@app.get("/", response_class=HTMLResponse)
def index():
    """Render the latest daily brief as HTML."""
    from signals.analysis.daily_brief import build_daily_brief, render_daily_brief_markdown

    brief = build_daily_brief(_get_db_path())
    md = render_daily_brief_markdown(brief)
    html = _brief_to_html(md, brief["as_of_date"])
    return html


@app.get("/api/brief")
def api_brief():
    """Return the daily brief as JSON."""
    from signals.analysis.daily_brief import build_daily_brief

    brief = build_daily_brief(_get_db_path())
    return JSONResponse(content=brief, media_type="application/json")


@app.get("/api/signals")
def api_signals(
    source: str | None = Query(None, description="Filter by source: insider, congress"),
    label: str | None = Query(None, description="Filter by label: bullish, bearish, neutral"),
    min_confidence: float = Query(0.0, description="Minimum confidence threshold"),
    limit: int = Query(100, description="Maximum results"),
):
    """Query signal results with optional filters."""
    db_path = _get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = "SELECT * FROM signal_results WHERE confidence >= ?"
    params: list = [min_confidence]

    if source:
        query += " AND source = ?"
        params.append(source)
    if label:
        query += " AND label = ?"
        params.append(label)

    query += " ORDER BY ABS(score) DESC LIMIT ?"
    params.append(limit)

    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()

    return JSONResponse(content={"count": len(rows), "signals": rows})


def _brief_to_html(markdown: str, date: str) -> str:
    """Convert brief markdown to a clean, styled HTML page."""
    lines = markdown.splitlines()
    body_lines = []

    in_table = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("# "):
            body_lines.append(f"<h1>{stripped[2:]}</h1>")
        elif stripped.startswith("## "):
            body_lines.append(f"<h2>{stripped[3:]}</h2>")
        elif stripped.startswith("- **"):
            body_lines.append(f"<li>{_md_inline(stripped[2:])}</li>")
        elif stripped.startswith("- "):
            body_lines.append(f"<li>{_md_inline(stripped[2:])}</li>")
        elif stripped.startswith("  -"):
            body_lines.append(f"<li class='sub'>{_md_inline(stripped.strip()[2:])}</li>")
        elif stripped.startswith("|") and "---" not in stripped:
            if not in_table:
                body_lines.append("<table>")
                in_table = True
            cells = [c.strip() for c in stripped.split("|")[1:-1]]
            is_header = not any("<td>" in h for h in body_lines[-5:]) and body_lines[-1] == "<table>"
            tag = "th" if is_header else "td"
            row = "".join(f"<{tag}>{_md_inline(c)}</{tag}>" for c in cells)
            body_lines.append(f"<tr>{row}</tr>")
        elif stripped.startswith("|") and "---" in stripped:
            continue
        else:
            if in_table:
                body_lines.append("</table>")
                in_table = False
            if stripped:
                body_lines.append(f"<p>{_md_inline(stripped)}</p>")

    if in_table:
        body_lines.append("</table>")

    body = "\n".join(body_lines)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Disclosure Signals — {date}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Segoe UI', Roboto, sans-serif;
    max-width: 800px; margin: 0 auto; padding: 24px 20px;
    color: #1d1d1f; background: #fafafa; line-height: 1.6;
  }}
  h1 {{
    font-size: 24px; font-weight: 700;
    border-bottom: 3px solid #0066cc; padding-bottom: 10px; margin-bottom: 16px;
  }}
  h2 {{
    font-size: 17px; font-weight: 600; color: #0066cc;
    margin-top: 28px; margin-bottom: 10px;
    padding-bottom: 4px; border-bottom: 1px solid #e5e5e7;
  }}
  p {{ margin: 8px 0; font-size: 15px; }}
  ul {{ list-style: none; padding: 0; }}
  li {{
    margin: 6px 0; padding: 8px 12px; font-size: 14px;
    background: #fff; border: 1px solid #e5e5e7; border-radius: 8px;
  }}
  li.sub {{ margin-left: 20px; background: #f5f5f7; font-size: 13px; }}
  strong {{ color: #1d1d1f; }}
  table {{
    border-collapse: collapse; width: 100%; margin: 12px 0;
    font-size: 13px; background: #fff; border-radius: 8px; overflow: hidden;
  }}
  th {{ background: #f5f5f7; font-weight: 600; text-align: left; padding: 8px 12px; }}
  td {{ padding: 6px 12px; border-top: 1px solid #e5e5e7; }}
  .footer {{
    margin-top: 40px; padding-top: 16px; border-top: 1px solid #e5e5e7;
    font-size: 12px; color: #86868b; text-align: center;
  }}
  .api-links {{
    margin-top: 12px; font-size: 12px; color: #86868b;
  }}
  .api-links a {{ color: #0066cc; text-decoration: none; }}
</style>
</head>
<body>
{body}
<div class="footer">
  Generated by Disclosure Signals on {date}
  <div class="api-links">
    API: <a href="/api/brief">/api/brief</a> |
    <a href="/api/signals">/api/signals</a> |
    <a href="/api/signals?source=insider&label=bullish">/api/signals?source=insider&label=bullish</a>
  </div>
</div>
</body>
</html>"""


def _md_inline(text: str) -> str:
    """Convert inline markdown to HTML."""
    return re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
