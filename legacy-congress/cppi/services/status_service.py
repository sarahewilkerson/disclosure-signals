"""Status application service for CPPI."""

from __future__ import annotations

from dataclasses import dataclass

from cppi.db import get_connection


@dataclass
class StatusResult:
    members: int
    filings: int
    transactions: int
    positioning_scores: int
    latest_score_scope: str | None
    latest_score_window_days: int | None
    latest_score_breadth_pct: float | None
    latest_score_confidence_tier: str | None
    latest_score_computed_at: str | None


def get_status(db_path: str | None = None) -> StatusResult:
    """Fetch CPPI database status without formatting concerns."""
    with get_connection(db_path) as conn:
        members = int(conn.execute("SELECT COUNT(*) FROM members").fetchone()[0])
        filings = int(conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0])
        transactions = int(conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0])
        positioning_scores = int(conn.execute("SELECT COUNT(*) FROM positioning_scores").fetchone()[0])
        latest = conn.execute(
            """
            SELECT scope, window_days, computed_at, breadth_pct, confidence_tier
            FROM positioning_scores
            ORDER BY computed_at DESC
            LIMIT 1
            """
        ).fetchone()

    return StatusResult(
        members=members,
        filings=filings,
        transactions=transactions,
        positioning_scores=positioning_scores,
        latest_score_scope=latest["scope"] if latest else None,
        latest_score_window_days=latest["window_days"] if latest else None,
        latest_score_breadth_pct=latest["breadth_pct"] if latest else None,
        latest_score_confidence_tier=latest["confidence_tier"] if latest else None,
        latest_score_computed_at=latest["computed_at"] if latest else None,
    )
