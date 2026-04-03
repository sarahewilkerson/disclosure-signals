"""
Database layer for CPPI.

Uses SQLite (stdlib). Schema based on Section 7 of the design document.
Includes migration support for schema evolution.
"""

import glob
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime

from cppi.config import DB_PATH

logger = logging.getLogger(__name__)

# Directory containing SQL migration files
MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "migrations")

# ---------------------------------------------------------------------------
# Schema DDL (Section 7 of design)
# ---------------------------------------------------------------------------
SCHEMA_SQL = """
-- Members of Congress
CREATE TABLE IF NOT EXISTS members (
    bioguide_id     TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    chamber         TEXT NOT NULL,       -- 'house' | 'senate'
    state           TEXT NOT NULL,
    party           TEXT NOT NULL,
    in_office       INTEGER DEFAULT 1,
    committees      TEXT,                -- JSON array
    updated_at      TEXT
);

-- Raw filings (PDFs/HTML)
CREATE TABLE IF NOT EXISTS filings (
    filing_id       TEXT PRIMARY KEY,
    bioguide_id     TEXT,                -- May be NULL until resolved
    chamber         TEXT NOT NULL,       -- 'house' | 'senate'
    filer_name      TEXT NOT NULL,
    filing_type     TEXT NOT NULL,       -- 'PTR' | 'FD' | 'Amendment'
    disclosure_date TEXT NOT NULL,
    source_url      TEXT NOT NULL,
    source_format   TEXT NOT NULL,       -- 'pdf_electronic' | 'pdf_paper' | 'html' | 'gif'
    source_hash     TEXT,
    raw_path        TEXT,
    parsed_at       TEXT,
    parse_error     TEXT,
    FOREIGN KEY (bioguide_id) REFERENCES members(bioguide_id)
);

-- Individual transactions
CREATE TABLE IF NOT EXISTS transactions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id               TEXT NOT NULL,
    bioguide_id             TEXT,
    owner_type              TEXT NOT NULL,   -- 'self' | 'spouse' | 'dependent' | 'joint' | 'managed'
    asset_name_raw          TEXT NOT NULL,
    asset_type              TEXT,            -- 'stock' | 'option' | 'etf' | 'bond' | etc.
    resolved_ticker         TEXT,
    resolved_company        TEXT,
    resolution_method       TEXT,
    resolution_confidence   REAL,
    transaction_type        TEXT NOT NULL,   -- 'purchase' | 'sale' | 'sale_partial' | 'exchange'
    execution_date          TEXT NOT NULL,
    disclosure_date         TEXT NOT NULL,
    ingestion_date          TEXT NOT NULL,
    disclosure_lag_days     INTEGER,
    -- Amount fields are NULL if unparseable (excluded from scoring per design)
    amount_min              INTEGER,
    amount_max              INTEGER,
    amount_code             TEXT,
    amount_midpoint         REAL,
    -- Signal inclusion
    include_in_signal       INTEGER DEFAULT 1,
    exclusion_reason        TEXT,
    -- Provenance
    page_number             INTEGER,
    extraction_confidence   REAL,
    FOREIGN KEY (filing_id) REFERENCES filings(filing_id),
    FOREIGN KEY (bioguide_id) REFERENCES members(bioguide_id)
);

-- Scoring results
CREATE TABLE IF NOT EXISTS positioning_scores (
    scope               TEXT,            -- 'all' | 'house' | 'senate' | committee name
    window_days         INTEGER,
    computed_at         TEXT,
    -- Breadth metrics
    breadth_pct         REAL,            -- (buyers - sellers) / total as percentage
    unique_buyers       INTEGER,
    unique_sellers      INTEGER,
    -- Volume metrics
    net_positioning     REAL,
    buy_volume          REAL,
    sell_volume         REAL,
    -- Confidence
    confidence_score    REAL,
    confidence_tier     TEXT,            -- 'HIGH' | 'MODERATE' | 'LOW'
    confidence_factors  TEXT,            -- JSON breakdown
    -- Concentration
    top5_share          REAL,
    -- Metadata
    transaction_count   INTEGER,
    excluded_count      INTEGER,
    sector_positioning  TEXT,            -- JSON (future use)
    PRIMARY KEY (scope, window_days)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_transactions_filing ON transactions(filing_id);
CREATE INDEX IF NOT EXISTS idx_transactions_bioguide ON transactions(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_transactions_exec_date ON transactions(execution_date);
CREATE INDEX IF NOT EXISTS idx_transactions_signal ON transactions(include_in_signal);
CREATE INDEX IF NOT EXISTS idx_filings_bioguide ON filings(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_filings_chamber ON filings(chamber);
CREATE INDEX IF NOT EXISTS idx_filings_date ON filings(disclosure_date);
"""


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
@contextmanager
def get_connection(db_path=None):
    """Context manager for SQLite connections."""
    path = str(db_path or DB_PATH)
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
    Initialize the database by running schema creation and migrations.
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
        if os.path.isdir(MIGRATIONS_DIR):
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
                        "INSERT INTO schema_version (version, filename, applied_at) VALUES (?, ?, ?)",
                        (version, filename, datetime.now(UTC).isoformat())
                    )
                    logger.info(f"Migration {filename} applied successfully")
                except Exception as e:
                    logger.error(f"Migration {filename} failed: {e}")
                    raise

        # Also run inline schema for any tables that might be missing
        conn.executescript(SCHEMA_SQL)


def get_schema_version(db_path=None) -> int:
    """Return the highest applied migration version number."""
    with get_connection(db_path) as conn:
        try:
            row = conn.execute("SELECT MAX(version) as v FROM schema_version").fetchone()
            return row["v"] if row and row["v"] is not None else 0
        except sqlite3.OperationalError:
            return 0


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------
def upsert_member(conn, member: dict):
    """Insert or update a member record."""
    conn.execute("""
        INSERT INTO members (bioguide_id, name, chamber, state, party, in_office, committees, updated_at)
        VALUES (:bioguide_id, :name, :chamber, :state, :party, :in_office, :committees, :updated_at)
        ON CONFLICT(bioguide_id) DO UPDATE SET
            name = excluded.name,
            chamber = excluded.chamber,
            state = excluded.state,
            party = excluded.party,
            in_office = excluded.in_office,
            committees = excluded.committees,
            updated_at = excluded.updated_at
    """, member)


def upsert_filing(conn, filing: dict):
    """Insert or update a filing record."""
    conn.execute("""
        INSERT INTO filings (
            filing_id, bioguide_id, chamber, filer_name, filing_type,
            disclosure_date, source_url, source_format, source_hash,
            raw_path, parsed_at, parse_error
        ) VALUES (
            :filing_id, :bioguide_id, :chamber, :filer_name, :filing_type,
            :disclosure_date, :source_url, :source_format, :source_hash,
            :raw_path, :parsed_at, :parse_error
        )
        ON CONFLICT(filing_id) DO UPDATE SET
            bioguide_id = excluded.bioguide_id,
            filer_name = excluded.filer_name,
            raw_path = excluded.raw_path,
            parsed_at = excluded.parsed_at,
            parse_error = excluded.parse_error
    """, filing)


def insert_transaction(conn, txn: dict):
    """Insert a transaction record."""
    conn.execute("""
        INSERT INTO transactions (
            filing_id, bioguide_id, owner_type, asset_name_raw, asset_type,
            resolved_ticker, resolved_company, resolution_method, resolution_confidence,
            transaction_type, execution_date, disclosure_date, ingestion_date,
            disclosure_lag_days, amount_min, amount_max, amount_code, amount_midpoint,
            include_in_signal, exclusion_reason, page_number, extraction_confidence
        ) VALUES (
            :filing_id, :bioguide_id, :owner_type, :asset_name_raw, :asset_type,
            :resolved_ticker, :resolved_company, :resolution_method, :resolution_confidence,
            :transaction_type, :execution_date, :disclosure_date, :ingestion_date,
            :disclosure_lag_days, :amount_min, :amount_max, :amount_code, :amount_midpoint,
            :include_in_signal, :exclusion_reason, :page_number, :extraction_confidence
        )
    """, txn)


def insert_transactions_batch(conn, txns: list):
    """Batch insert multiple transaction records."""
    if not txns:
        return
    conn.executemany("""
        INSERT INTO transactions (
            filing_id, bioguide_id, owner_type, asset_name_raw, asset_type,
            resolved_ticker, resolved_company, resolution_method, resolution_confidence,
            transaction_type, execution_date, disclosure_date, ingestion_date,
            disclosure_lag_days, amount_min, amount_max, amount_code, amount_midpoint,
            include_in_signal, exclusion_reason, page_number, extraction_confidence
        ) VALUES (
            :filing_id, :bioguide_id, :owner_type, :asset_name_raw, :asset_type,
            :resolved_ticker, :resolved_company, :resolution_method, :resolution_confidence,
            :transaction_type, :execution_date, :disclosure_date, :ingestion_date,
            :disclosure_lag_days, :amount_min, :amount_max, :amount_code, :amount_midpoint,
            :include_in_signal, :exclusion_reason, :page_number, :extraction_confidence
        )
    """, txns)


def upsert_positioning_score(conn, score: dict):
    """Insert or update a positioning score record."""
    conn.execute("""
        INSERT INTO positioning_scores (
            scope, window_days, computed_at, breadth_pct,
            unique_buyers, unique_sellers, net_positioning,
            buy_volume, sell_volume, confidence_score, confidence_tier,
            confidence_factors, top5_share, transaction_count,
            excluded_count, sector_positioning
        ) VALUES (
            :scope, :window_days, :computed_at, :breadth_pct,
            :unique_buyers, :unique_sellers, :net_positioning,
            :buy_volume, :sell_volume, :confidence_score, :confidence_tier,
            :confidence_factors, :top5_share, :transaction_count,
            :excluded_count, :sector_positioning
        )
        ON CONFLICT(scope, window_days) DO UPDATE SET
            computed_at = excluded.computed_at,
            breadth_pct = excluded.breadth_pct,
            unique_buyers = excluded.unique_buyers,
            unique_sellers = excluded.unique_sellers,
            net_positioning = excluded.net_positioning,
            buy_volume = excluded.buy_volume,
            sell_volume = excluded.sell_volume,
            confidence_score = excluded.confidence_score,
            confidence_tier = excluded.confidence_tier,
            confidence_factors = excluded.confidence_factors,
            top5_share = excluded.top5_share,
            transaction_count = excluded.transaction_count,
            excluded_count = excluded.excluded_count,
            sector_positioning = excluded.sector_positioning
    """, score)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def get_members(conn, chamber: str = None):
    """Return all members, optionally filtered by chamber."""
    if chamber:
        return conn.execute(
            "SELECT * FROM members WHERE chamber = ? ORDER BY name",
            (chamber,)
        ).fetchall()
    return conn.execute("SELECT * FROM members ORDER BY name").fetchall()


def get_member_by_bioguide(conn, bioguide_id: str):
    """Return a single member by bioguide ID."""
    return conn.execute(
        "SELECT * FROM members WHERE bioguide_id = ?",
        (bioguide_id,)
    ).fetchone()


def get_signal_transactions(conn, since_date: str, chamber: str = None):
    """Return transactions included in signal since a date."""
    query = """
        SELECT t.*, f.chamber
        FROM transactions t
        JOIN filings f ON t.filing_id = f.filing_id
        WHERE t.execution_date >= ?
          AND t.include_in_signal = 1
    """
    params = [since_date]
    if chamber:
        query += " AND f.chamber = ?"
        params.append(chamber)
    query += " ORDER BY t.execution_date DESC"
    return conn.execute(query, params).fetchall()


def get_excluded_transactions(conn, since_date: str):
    """Return excluded transactions for transparency reporting."""
    return conn.execute("""
        SELECT * FROM transactions
        WHERE execution_date >= ?
          AND include_in_signal = 0
        ORDER BY execution_date DESC
    """, (since_date,)).fetchall()


def filing_exists(conn, filing_id: str) -> bool:
    """Check if a filing has already been ingested."""
    row = conn.execute(
        "SELECT 1 FROM filings WHERE filing_id = ?",
        (filing_id,)
    ).fetchone()
    return row is not None


def clear_scores(conn):
    """Clear computed scores for re-computation."""
    conn.execute("DELETE FROM positioning_scores")


def clear_transactions_for_filing(conn, filing_id: str):
    """Remove all transactions for a filing (used when re-parsing)."""
    conn.execute("DELETE FROM transactions WHERE filing_id = ?", (filing_id,))
