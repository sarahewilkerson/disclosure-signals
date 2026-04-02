"""Status application service for the legacy insider engine."""

from __future__ import annotations

from dataclasses import dataclass

from db import get_connection


@dataclass
class StatusResult:
    companies: int
    filings: int
    transactions: int
    signal_transactions: int
    company_scores: int
    filing_date_oldest: str | None
    filing_date_latest: str | None
    parse_errors: int


def get_status(db_path: str | None = None) -> StatusResult:
    """Fetch the legacy insider database status without formatting concerns."""
    with get_connection(db_path) as conn:
        companies = conn.execute("SELECT COUNT(*) as c FROM companies").fetchone()["c"]
        filings = conn.execute("SELECT COUNT(*) as c FROM filings").fetchone()["c"]
        txns = conn.execute("SELECT COUNT(*) as c FROM transactions").fetchone()["c"]
        signal_txns = conn.execute(
            "SELECT COUNT(*) as c FROM transactions WHERE include_in_signal = 1"
        ).fetchone()["c"]
        scores = conn.execute("SELECT COUNT(*) as c FROM company_scores").fetchone()["c"]

        oldest = None
        latest = None
        if filings > 0:
            latest = conn.execute("SELECT MAX(filing_date) as d FROM filings").fetchone()["d"]
            oldest = conn.execute("SELECT MIN(filing_date) as d FROM filings").fetchone()["d"]

        errors = conn.execute(
            "SELECT COUNT(*) as c FROM filings WHERE parse_error IS NOT NULL"
        ).fetchone()["c"]

    return StatusResult(
        companies=companies,
        filings=filings,
        transactions=txns,
        signal_transactions=signal_txns,
        company_scores=scores,
        filing_date_oldest=oldest,
        filing_date_latest=latest,
        parse_errors=errors,
    )

