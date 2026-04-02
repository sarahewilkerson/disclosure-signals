-- Phase 2 Schema Migration
-- Adds support for sector exposures, historical scores, and validation results

-- Add sector exposures to members table
ALTER TABLE members ADD COLUMN sector_exposures TEXT;  -- JSON

-- Historical scores table for backtesting
CREATE TABLE IF NOT EXISTS historical_scores (
    scope           TEXT,
    window_days     INTEGER,
    as_of_date      TEXT,
    breadth_pct     REAL,
    net_positioning REAL,
    confidence_score REAL,
    PRIMARY KEY (scope, window_days, as_of_date)
);

-- Validation results table for vendor cross-validation
CREATE TABLE IF NOT EXISTS validation_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,  -- 'quiver' | 'capitol_trades'
    validated_at    TEXT NOT NULL,
    match_rate      REAL,
    total_compared  INTEGER,
    discrepancies   TEXT  -- JSON
);

-- Index for historical scores queries
CREATE INDEX IF NOT EXISTS idx_historical_scores_date ON historical_scores(as_of_date);
