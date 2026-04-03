-- CPPI Initial Schema
-- Version: 001
-- Date: 2026-03-29

-- Members of Congress
CREATE TABLE IF NOT EXISTS members (
    bioguide_id     TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    chamber         TEXT NOT NULL,
    state           TEXT NOT NULL,
    party           TEXT NOT NULL,
    in_office       INTEGER DEFAULT 1,
    committees      TEXT,
    updated_at      TEXT
);

-- Raw filings
CREATE TABLE IF NOT EXISTS filings (
    filing_id       TEXT PRIMARY KEY,
    bioguide_id     TEXT,
    chamber         TEXT NOT NULL,
    filer_name      TEXT NOT NULL,
    filing_type     TEXT NOT NULL,
    disclosure_date TEXT NOT NULL,
    source_url      TEXT NOT NULL,
    source_format   TEXT NOT NULL,
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
    owner_type              TEXT NOT NULL,
    asset_name_raw          TEXT NOT NULL,
    asset_type              TEXT,
    resolved_ticker         TEXT,
    resolved_company        TEXT,
    resolution_method       TEXT,
    resolution_confidence   REAL,
    transaction_type        TEXT NOT NULL,
    execution_date          TEXT NOT NULL,
    disclosure_date         TEXT NOT NULL,
    ingestion_date          TEXT NOT NULL,
    disclosure_lag_days     INTEGER,
    amount_min              INTEGER,
    amount_max              INTEGER,
    amount_code             TEXT,
    amount_midpoint         REAL,
    include_in_signal       INTEGER DEFAULT 1,
    exclusion_reason        TEXT,
    page_number             INTEGER,
    extraction_confidence   REAL,
    FOREIGN KEY (filing_id) REFERENCES filings(filing_id),
    FOREIGN KEY (bioguide_id) REFERENCES members(bioguide_id)
);

-- Scoring results
CREATE TABLE IF NOT EXISTS positioning_scores (
    scope               TEXT,
    window_days         INTEGER,
    computed_at         TEXT,
    breadth_pct         REAL,
    unique_buyers       INTEGER,
    unique_sellers      INTEGER,
    net_positioning     REAL,
    buy_volume          REAL,
    sell_volume         REAL,
    confidence_score    REAL,
    confidence_tier     TEXT,
    confidence_factors  TEXT,
    top5_share          REAL,
    transaction_count   INTEGER,
    excluded_count      INTEGER,
    sector_positioning  TEXT,
    PRIMARY KEY (scope, window_days)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_transactions_filing ON transactions(filing_id);
CREATE INDEX IF NOT EXISTS idx_transactions_bioguide ON transactions(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_transactions_exec_date ON transactions(execution_date);
CREATE INDEX IF NOT EXISTS idx_transactions_signal ON transactions(include_in_signal);
CREATE INDEX IF NOT EXISTS idx_filings_bioguide ON filings(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_filings_chamber ON filings(chamber);
CREATE INDEX IF NOT EXISTS idx_filings_date ON filings(disclosure_date);
