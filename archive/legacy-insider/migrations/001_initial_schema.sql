-- Migration 001: Initial schema
-- Creates all core tables for the Insider Trading Signal Engine.

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

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_transactions_issuer ON transactions(cik_issuer);
CREATE INDEX IF NOT EXISTS idx_transactions_owner ON transactions(cik_owner);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_signal ON transactions(include_in_signal);
CREATE INDEX IF NOT EXISTS idx_filings_issuer ON filings(cik_issuer);
CREATE INDEX IF NOT EXISTS idx_filings_owner ON filings(cik_owner);
