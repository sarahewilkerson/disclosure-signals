"""
Database layer for the Insider Trading Signal Engine.

Uses SQLite (stdlib). All schema creation, upsert, and query helpers live here.
Includes migration support for schema evolution.
"""

import glob
import logging
import sqlite3
import json
import os
from contextlib import contextmanager

from config import DB_PATH

logger = logging.getLogger(__name__)

# Directory containing SQL migration files
MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "migrations")

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    cik             TEXT PRIMARY KEY,
    ticker          TEXT,
    company_name    TEXT,
    fortune_rank    INTEGER,
    revenue         REAL,
    sector          TEXT,
    resolved_at     TEXT
);

CREATE TABLE IF NOT EXISTS filings (
    accession_number    TEXT PRIMARY KEY,
    cik_issuer          TEXT,
    cik_owner           TEXT,
    owner_name          TEXT,
    officer_title       TEXT,
    is_officer          INTEGER,  -- 0/1
    is_director         INTEGER,
    is_ten_pct_owner    INTEGER,
    is_other            INTEGER,
    is_amendment        INTEGER,
    amendment_type      TEXT,
    period_of_report    TEXT,
    aff10b5one          INTEGER,  -- 0/1, structured Rule 10b5-1 indicator (post-April 2023)
    additional_owners   TEXT,     -- JSON array of additional owner dicts (for multi-owner filings)
    filing_date         TEXT,
    xml_url             TEXT,
    raw_xml_path        TEXT,
    parsed_at           TEXT,
    parse_error         TEXT,
    FOREIGN KEY (cik_issuer) REFERENCES companies(cik)
);

CREATE TABLE IF NOT EXISTS transactions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    accession_number        TEXT,
    cik_issuer              TEXT,
    cik_owner               TEXT,
    owner_name              TEXT,
    officer_title           TEXT,
    security_title          TEXT,
    transaction_date        TEXT,
    transaction_code        TEXT,
    equity_swap             INTEGER,  -- 0/1
    shares                  REAL,
    price_per_share         REAL,
    total_value             REAL,
    shares_after            REAL,
    ownership_nature        TEXT,     -- 'D' or 'I'
    indirect_entity         TEXT,
    is_derivative           INTEGER,  -- 0/1
    underlying_security     TEXT,
    footnotes               TEXT,
    -- Classification fields (populated by classification step)
    role_class              TEXT,
    transaction_class       TEXT,
    is_likely_planned       INTEGER,  -- 0/1
    is_discretionary        INTEGER,  -- 0/1
    pct_holdings_changed    REAL,
    include_in_signal       INTEGER,  -- 0/1
    exclusion_reason        TEXT,
    FOREIGN KEY (accession_number) REFERENCES filings(accession_number),
    FOREIGN KEY (cik_issuer) REFERENCES companies(cik)
);

CREATE TABLE IF NOT EXISTS company_scores (
    cik                 TEXT,
    ticker              TEXT,
    window_days         INTEGER,
    computed_at         TEXT,
    signal              TEXT,
    score               REAL,
    confidence          REAL,
    confidence_tier     TEXT,
    buy_count           INTEGER,
    sell_count          INTEGER,
    unique_buyers       INTEGER,
    unique_sellers      INTEGER,
    net_buy_value       REAL,
    explanation         TEXT,
    filing_accessions   TEXT,  -- JSON list
    PRIMARY KEY (cik, window_days)
);

CREATE TABLE IF NOT EXISTS aggregate_index (
    window_days             INTEGER,
    computed_at             TEXT,
    risk_appetite_index     REAL,
    bullish_breadth         REAL,
    bearish_breadth         REAL,
    neutral_pct             REAL,
    insufficient_pct        REAL,
    ceo_cfo_only_index      REAL,
    sector_balanced_index   REAL,
    cyclical_score          REAL,
    defensive_score         REAL,
    sector_breakdown        TEXT,  -- JSON {sector: score}
    total_companies         INTEGER,
    companies_with_signal   INTEGER,
    PRIMARY KEY (window_days)
);

CREATE INDEX IF NOT EXISTS idx_transactions_issuer ON transactions(cik_issuer);
CREATE INDEX IF NOT EXISTS idx_transactions_owner ON transactions(cik_owner);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_signal ON transactions(include_in_signal);
CREATE INDEX IF NOT EXISTS idx_filings_issuer ON filings(cik_issuer);
CREATE INDEX IF NOT EXISTS idx_filings_owner ON filings(cik_owner);
"""


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
@contextmanager
def get_connection(db_path=None):
    """Context manager for SQLite connections."""
    path = db_path or DB_PATH
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path=None):
    """
    Initialize the database by running all pending migrations.

    This is the primary entry point for database setup. It creates the
    schema_version table if needed and applies any migrations that haven't
    been run yet.
    """
    migrate(db_path)


def migrate(db_path=None):
    """
    Run all pending database migrations.

    Migrations are SQL files in the migrations/ directory, named with a
    numeric prefix (e.g., 001_initial_schema.sql). Each migration is run
    exactly once, tracked via the schema_version table.
    """
    with get_connection(db_path) as conn:
        # Create schema_version table if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version     INTEGER PRIMARY KEY,
                filename    TEXT NOT NULL,
                applied_at  TEXT NOT NULL
            )
        """)

        # Get already-applied migrations
        applied = set()
        rows = conn.execute("SELECT version FROM schema_version").fetchall()
        for row in rows:
            applied.add(row["version"])

        # Find migration files
        if not os.path.isdir(MIGRATIONS_DIR):
            # Fall back to inline schema if no migrations directory
            logger.warning(f"Migrations directory not found: {MIGRATIONS_DIR}")
            logger.warning("Falling back to inline SCHEMA_SQL")
            conn.executescript(SCHEMA_SQL)
            return

        migration_files = sorted(glob.glob(os.path.join(MIGRATIONS_DIR, "*.sql")))

        for filepath in migration_files:
            filename = os.path.basename(filepath)
            # Extract version number from filename (e.g., "001_initial.sql" -> 1)
            try:
                version = int(filename.split("_")[0])
            except (ValueError, IndexError):
                logger.warning(f"Skipping migration with invalid name: {filename}")
                continue

            if version in applied:
                logger.debug(f"Migration {filename} already applied")
                continue

            # Read and execute migration
            logger.info(f"Applying migration: {filename}")
            with open(filepath, "r") as f:
                sql = f.read()

            try:
                conn.executescript(sql)
                conn.execute(
                    "INSERT INTO schema_version (version, filename, applied_at) VALUES (?, ?, datetime('now'))",
                    (version, filename)
                )
                logger.info(f"Migration {filename} applied successfully")
            except Exception as e:
                logger.error(f"Migration {filename} failed: {e}")
                raise

        # Also run inline schema for any tables that might be missing
        # This ensures backwards compatibility with existing code
        conn.executescript(SCHEMA_SQL)


def get_schema_version(db_path=None) -> int:
    """Return the highest applied migration version number."""
    with get_connection(db_path) as conn:
        try:
            row = conn.execute("SELECT MAX(version) as v FROM schema_version").fetchone()
            return row["v"] if row and row["v"] is not None else 0
        except sqlite3.OperationalError:
            # Table doesn't exist yet
            return 0


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------
def upsert_company(conn, company: dict):
    """Insert or update a company record."""
    conn.execute("""
        INSERT INTO companies (cik, ticker, company_name, fortune_rank, revenue, sector, resolved_at)
        VALUES (:cik, :ticker, :company_name, :fortune_rank, :revenue, :sector, :resolved_at)
        ON CONFLICT(cik) DO UPDATE SET
            ticker = excluded.ticker,
            company_name = excluded.company_name,
            fortune_rank = excluded.fortune_rank,
            revenue = excluded.revenue,
            sector = excluded.sector,
            resolved_at = excluded.resolved_at
    """, company)


def upsert_filing(conn, filing: dict):
    """Insert or update a filing record."""
    conn.execute("""
        INSERT INTO filings (
            accession_number, cik_issuer, cik_owner, owner_name, officer_title,
            is_officer, is_director, is_ten_pct_owner, is_other,
            is_amendment, amendment_type, period_of_report, aff10b5one,
            additional_owners, filing_date, xml_url, raw_xml_path, parsed_at, parse_error
        ) VALUES (
            :accession_number, :cik_issuer, :cik_owner, :owner_name, :officer_title,
            :is_officer, :is_director, :is_ten_pct_owner, :is_other,
            :is_amendment, :amendment_type, :period_of_report, :aff10b5one,
            :additional_owners, :filing_date, :xml_url, :raw_xml_path, :parsed_at, :parse_error
        )
        ON CONFLICT(accession_number) DO UPDATE SET
            parsed_at = excluded.parsed_at,
            parse_error = excluded.parse_error,
            raw_xml_path = excluded.raw_xml_path,
            additional_owners = excluded.additional_owners
    """, filing)


def insert_transaction(conn, txn: dict):
    """Insert a transaction record."""
    conn.execute("""
        INSERT INTO transactions (
            accession_number, cik_issuer, cik_owner, owner_name, officer_title,
            security_title, transaction_date, transaction_code, equity_swap,
            shares, price_per_share, total_value, shares_after,
            ownership_nature, indirect_entity, is_derivative,
            underlying_security, footnotes,
            role_class, transaction_class, is_likely_planned, is_discretionary,
            pct_holdings_changed, include_in_signal, exclusion_reason
        ) VALUES (
            :accession_number, :cik_issuer, :cik_owner, :owner_name, :officer_title,
            :security_title, :transaction_date, :transaction_code, :equity_swap,
            :shares, :price_per_share, :total_value, :shares_after,
            :ownership_nature, :indirect_entity, :is_derivative,
            :underlying_security, :footnotes,
            :role_class, :transaction_class, :is_likely_planned, :is_discretionary,
            :pct_holdings_changed, :include_in_signal, :exclusion_reason
        )
    """, txn)


def insert_transactions_batch(conn, txns: list):
    """
    Batch insert multiple transaction records.

    Uses executemany for better performance when inserting many transactions
    from a single filing or during bulk ingestion.
    """
    if not txns:
        return

    conn.executemany("""
        INSERT INTO transactions (
            accession_number, cik_issuer, cik_owner, owner_name, officer_title,
            security_title, transaction_date, transaction_code, equity_swap,
            shares, price_per_share, total_value, shares_after,
            ownership_nature, indirect_entity, is_derivative,
            underlying_security, footnotes,
            role_class, transaction_class, is_likely_planned, is_discretionary,
            pct_holdings_changed, include_in_signal, exclusion_reason
        ) VALUES (
            :accession_number, :cik_issuer, :cik_owner, :owner_name, :officer_title,
            :security_title, :transaction_date, :transaction_code, :equity_swap,
            :shares, :price_per_share, :total_value, :shares_after,
            :ownership_nature, :indirect_entity, :is_derivative,
            :underlying_security, :footnotes,
            :role_class, :transaction_class, :is_likely_planned, :is_discretionary,
            :pct_holdings_changed, :include_in_signal, :exclusion_reason
        )
    """, txns)


def clear_transactions_for_filing(conn, accession_number: str):
    """Remove all transactions for a filing (used when re-parsing or handling amendments)."""
    conn.execute("DELETE FROM transactions WHERE accession_number = ?", (accession_number,))


def clear_scores(conn):
    """Clear computed scores for re-computation."""
    conn.execute("DELETE FROM company_scores")
    conn.execute("DELETE FROM aggregate_index")


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def get_companies(conn):
    """Return all companies."""
    return conn.execute("SELECT * FROM companies ORDER BY fortune_rank").fetchall()


def get_company_by_cik(conn, cik: str):
    """Return a single company by CIK."""
    return conn.execute("SELECT * FROM companies WHERE cik = ?", (cik,)).fetchone()


def get_signal_transactions(conn, cik_issuer: str, since_date: str):
    """Return transactions included in signal for a company since a date."""
    return conn.execute("""
        SELECT * FROM transactions
        WHERE cik_issuer = ?
          AND transaction_date >= ?
          AND include_in_signal = 1
        ORDER BY transaction_date DESC
    """, (cik_issuer, since_date)).fetchall()


def get_filing_accession_numbers(conn, cik_issuer: str):
    """Return all known accession numbers for an issuer."""
    rows = conn.execute(
        "SELECT accession_number FROM filings WHERE cik_issuer = ?",
        (cik_issuer,)
    ).fetchall()
    return {row["accession_number"] for row in rows}


def get_amendment_candidates(conn, cik_issuer: str, cik_owner: str,
                             period_of_report: str, exclude_accession: str = None):
    """
    Find prior filings that an amendment might be replacing.

    Includes both original filings AND prior amendments (chained amendments).
    Excludes the filing itself to avoid self-matching.
    """
    query = """
        SELECT accession_number FROM filings
        WHERE cik_issuer = ?
          AND cik_owner = ?
          AND period_of_report = ?
    """
    params = [cik_issuer, cik_owner, period_of_report]
    if exclude_accession:
        query += "  AND accession_number != ?\n"
        params.append(exclude_accession)
    query += "ORDER BY filing_date ASC"
    return conn.execute(query, params).fetchall()


def filing_exists(conn, accession_number: str) -> bool:
    """Check if a filing has already been ingested."""
    row = conn.execute(
        "SELECT 1 FROM filings WHERE accession_number = ?",
        (accession_number,)
    ).fetchone()
    return row is not None


def get_excluded_transactions(conn, cik_issuer: str, since_date: str):
    """Return excluded transactions for transparency reporting."""
    return conn.execute("""
        SELECT * FROM transactions
        WHERE cik_issuer = ?
          AND transaction_date >= ?
          AND include_in_signal = 0
        ORDER BY transaction_date DESC
    """, (cik_issuer, since_date)).fetchall()


def get_companies_with_new_filings(conn, since_date: str) -> list:
    """
    Return companies that have new filings since the given date.

    Used for incremental scoring — only rescore companies with recent activity.
    """
    return conn.execute("""
        SELECT DISTINCT c.cik, c.ticker, c.company_name, c.fortune_rank,
               c.revenue, c.sector, c.resolved_at
        FROM companies c
        JOIN filings f ON f.cik_issuer = c.cik
        WHERE f.parsed_at >= ?
        ORDER BY c.fortune_rank
    """, (since_date,)).fetchall()


def upsert_company_score(conn, score: dict):
    """Insert or update a company score record."""
    conn.execute("""
        INSERT INTO company_scores (
            cik, ticker, window_days, computed_at, signal, score,
            confidence, confidence_tier, buy_count, sell_count,
            unique_buyers, unique_sellers, net_buy_value,
            explanation, filing_accessions
        ) VALUES (
            :cik, :ticker, :window_days, :computed_at, :signal, :score,
            :confidence, :confidence_tier, :buy_count, :sell_count,
            :unique_buyers, :unique_sellers, :net_buy_value,
            :explanation, :filing_accessions
        )
        ON CONFLICT(cik, window_days) DO UPDATE SET
            ticker = excluded.ticker,
            computed_at = excluded.computed_at,
            signal = excluded.signal,
            score = excluded.score,
            confidence = excluded.confidence,
            confidence_tier = excluded.confidence_tier,
            buy_count = excluded.buy_count,
            sell_count = excluded.sell_count,
            unique_buyers = excluded.unique_buyers,
            unique_sellers = excluded.unique_sellers,
            net_buy_value = excluded.net_buy_value,
            explanation = excluded.explanation,
            filing_accessions = excluded.filing_accessions
    """, score)


def get_last_score_timestamp(conn) -> str | None:
    """Return the most recent computed_at timestamp from company_scores."""
    row = conn.execute("""
        SELECT MAX(computed_at) as last_computed FROM company_scores
    """).fetchone()
    return row["last_computed"] if row else None
