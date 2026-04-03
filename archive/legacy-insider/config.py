"""
Configuration constants for the Insider Trading Signal Engine.

All thresholds, weights, and tunable parameters live here.
Changes to scoring methodology should be made in this file.

Environment variables (all optional, with defaults):
    SEC_USER_AGENT  - Required format: "AppName/Version (email@domain.com)"
    SEC_BASE_URL    - SEC website base URL
    SEC_DATA_URL    - SEC data API base URL
    SEC_EFTS_URL    - SEC full-text search API URL
    DB_PATH         - Path to SQLite database file
"""

import os

# ---------------------------------------------------------------------------
# SEC EDGAR settings (configurable via environment variables)
# ---------------------------------------------------------------------------
# IMPORTANT: SEC requires a User-Agent with company name, app name, and email.
# Set the SEC_USER_AGENT environment variable before running.
SEC_USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "InsiderSignalEngine/1.0 (your-email@example.com)"
)
SEC_BASE_URL = os.environ.get("SEC_BASE_URL", "https://www.sec.gov")
SEC_DATA_URL = os.environ.get("SEC_DATA_URL", "https://data.sec.gov")
SEC_EFTS_URL = os.environ.get("SEC_EFTS_URL", "https://efts.sec.gov/LATEST")
SEC_RATE_LIMIT_DELAY = 0.12  # seconds between requests (≈8 req/sec, under 10 limit)
SEC_MAX_RETRIES = 3

# Company tickers JSON (maps ticker → CIK)
# Note: This file is at www.sec.gov, not data.sec.gov
SEC_COMPANY_TICKERS_URL = f"{SEC_BASE_URL}/files/company_tickers.json"

# Filing index by CIK
# Usage: SEC_SUBMISSIONS_URL.format(cik="0000320193")
SEC_SUBMISSIONS_URL = f"{SEC_DATA_URL}/submissions/CIK{{cik}}.json"

# Archives base for constructing filing XML URLs
SEC_ARCHIVES_URL = f"{SEC_BASE_URL}/Archives/edgar/data"


def validate_runtime_config():
    """Validate runtime SEC configuration when network access is required."""
    if os.environ.get("SKIP_CONFIG_VALIDATION", "").lower() in {"1", "true", "yes"}:
        return

    # Check User-Agent is properly configured
    if "example.com" in SEC_USER_AGENT.lower():
        raise ValueError(
            "SEC_USER_AGENT contains placeholder 'example.com'. "
            "Set the SEC_USER_AGENT environment variable with your actual email. "
            "Example: SEC_USER_AGENT='MyApp/1.0 (myemail@company.com)'"
        )

    # Validate User-Agent format (should contain email-like pattern)
    if "@" not in SEC_USER_AGENT or "(" not in SEC_USER_AGENT:
        raise ValueError(
            "SEC_USER_AGENT must be in format 'AppName/Version (email@domain.com)'. "
            f"Got: {SEC_USER_AGENT}"
        )

# ---------------------------------------------------------------------------
# Data / cache paths (configurable via environment variables)
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.environ.get("CACHE_DIR", os.path.join(PROJECT_ROOT, "cache"))
FILINGS_CACHE_DIR = os.path.join(CACHE_DIR, "filings")
DB_PATH = os.environ.get("DB_PATH", os.path.join(PROJECT_ROOT, "insider_signal.db"))
CIK_CACHE_PATH = os.path.join(CACHE_DIR, "company_tickers.json")

# ---------------------------------------------------------------------------
# Role classification
# ---------------------------------------------------------------------------
# Top leadership roles — matched via regex patterns (case-insensitive).
# Each pattern uses word boundaries or negative lookbehinds to avoid false
# positives (e.g., "Senior Vice President" must NOT match as "president").
TOP_LEADERSHIP_PATTERNS = [
    # (compiled_regex, role)  — order matters for priority
    (r"\bceo\b", "ceo"),
    (r"\bchief executive officer\b", "ceo"),
    (r"\bcfo\b", "cfo"),
    (r"\bchief financial officer\b", "cfo"),
    (r"\bprincipal financial officer\b", "cfo"),
    (r"\bchairman\b", "chair"),
    (r"\bchairwoman\b", "chair"),
    (r"\bchairperson\b", "chair"),
    (r"\bchair of the board\b", "chair"),
    (r"\bexecutive chair\b", "chair"),
    (r"\bexec\.?\s*chair\b", "chair"),
    # "president" must NOT be preceded by "vice " — excludes all VPs
    (r"(?<!\bvice )(?<!\bvice)\bpresident\b", "president"),
    (r"\bchief operating officer\b", "coo"),
    (r"\bcoo\b", "coo"),
    # C-suite expansion (added 2026-03-28)
    (r"\bcto\b", "cto"),
    (r"\bchief technology officer\b", "cto"),
    (r"\bclo\b", "clo"),
    (r"\bchief legal officer\b", "clo"),
    (r"\bgeneral counsel\b", "clo"),
    (r"\bcio\b", "cio"),
    (r"\bchief information officer\b", "cio"),
    (r"\bcmo\b", "cmo"),
    (r"\bchief marketing officer\b", "cmo"),
    (r"\bcao\b", "cao"),
    (r"\bchief accounting officer\b", "cao"),
    (r"\bprincipal accounting officer\b", "cao"),
]

# Role priority: if a title matches multiple, use the highest-priority one
ROLE_PRIORITY = ["ceo", "cfo", "chair", "president", "coo", "cto", "clo", "cio", "cmo", "cao"]

# Entity-name patterns that indicate the filer is not a natural person
ENTITY_EXCLUSION_PATTERNS = [
    r"\bllc\b", r"\bllp\b", r"\bl\.l\.c\b", r"\bl\.p\b", r"\blp\b",
    r"\btrust\b", r"\bfoundation\b", r"\bfund\b", r"\bcapital\b",
    r"\bpartners\b", r"\badvisors\b", r"\bholdings\b", r"\binc\b",
    r"\bcorp\b", r"\bltd\b", r"\bgroup\b", r"\bassociates\b",
    r"\binvestment\b", r"\bmanagement\b", r"\benterprise\b",
]

# Title patterns that indicate former/inactive officers
FORMER_OFFICER_PATTERNS = [
    r"\bformer\b", r"\bex-", r"\bfmr\b", r"\bretired\b", r"\bpast\b",
]

# ---------------------------------------------------------------------------
# Transaction classification
# ---------------------------------------------------------------------------
# Transaction codes and their classification
# See SEC ownership forms: https://www.sec.gov/files/forms-3-4-5.pdf
TRANSACTION_CODE_MAP = {
    "P": "open_market_buy",
    "S": "open_market_sell",
    "M": "option_exercise",
    "F": "tax_withhold",
    "A": "award_grant",
    "G": "gift",
    "J": "other_acquisition",
    "C": "conversion",
    "D": "disposition_to_issuer",
    "I": "discretionary_transaction",
    "U": "disposition_change_in_tenancy",
    "W": "acquisition_by_will",
    "Z": "deposit_or_withdrawal",
    "K": "equity_swap",
    "L": "small_acquisition",  # Section 16 small acquisition
}

# Transaction codes included in core signal
CORE_SIGNAL_CODES = {"P", "S"}

# ---------------------------------------------------------------------------
# 10b5-1 plan detection (footnote keyword matching)
# ---------------------------------------------------------------------------
PLANNED_TRADE_KEYWORDS = [
    "10b5-1", "10b-5-1", "rule 10b5", "rule 10b-5",
    "trading plan", "pre-arranged", "pre-established",
    "prearranged", "predetermined",
]

# ---------------------------------------------------------------------------
# Scoring weights
# ---------------------------------------------------------------------------
# Direction weights: buys are 2x more informative than sells
DIRECTION_WEIGHT_BUY = 1.0
DIRECTION_WEIGHT_SELL = -0.5  # negative = bearish direction

# Role seniority weights
ROLE_WEIGHT = {
    "ceo": 1.0,
    "cfo": 0.95,
    "chair": 0.9,
    "president": 0.85,
    "coo": 0.8,
    # C-suite expansion (added 2026-03-28)
    "cto": 0.75,
    "clo": 0.75,
    "cio": 0.70,
    "cmo": 0.70,
    "cao": 0.70,
    # officer_other: officers without top leadership title
    "officer_other": 0.5,
}

# 10b5-1 planned trade discount factor
PLANNED_TRADE_DISCOUNT = 0.25

# Ownership type weights
DIRECT_OWNERSHIP_WEIGHT = 1.0
INDIRECT_OWNERSHIP_WEIGHT = 0.6  # indirect holdings are noisier

# Transaction size signal based on % of insider's holdings changed
SIZE_SIGNAL_BRACKETS = [
    # (max_pct, weight) — evaluated in order, first match wins
    (0.01, 0.5),   # < 1% of holdings
    (0.05, 0.8),   # 1-5%
    (0.20, 1.0),   # 5-20%
    (1.00, 1.2),   # 20-100%
    (float("inf"), 1.2),  # > 100% (cap)
]
SIZE_SIGNAL_UNKNOWN = 0.6  # when holdings data is missing

# Recency decay: exponential with 45-day half-life
RECENCY_HALF_LIFE_DAYS = 45

# Per-insider saturation cap: no single insider can contribute more than
# this fraction of a company's total signal magnitude
PER_INSIDER_SATURATION_CAP = 0.30

# ---------------------------------------------------------------------------
# Signal classification thresholds
# ---------------------------------------------------------------------------
# Company-level score thresholds for signal labels
BULLISH_THRESHOLD = 0.15    # score > this → bullish
BEARISH_THRESHOLD = -0.15   # score < this → bearish

# Confidence thresholds (tiers)
CONFIDENCE_INSUFFICIENT = 0.25   # below this → "insufficient evidence"
CONFIDENCE_LOW = 0.50            # 0.25-0.50 → "low confidence"
CONFIDENCE_MODERATE = 0.75       # 0.50-0.75 → "moderate confidence"
CONFIDENCE_MAX = 0.90            # cap — never claim certainty

# ---------------------------------------------------------------------------
# Time windows for analysis
# ---------------------------------------------------------------------------
ANALYSIS_WINDOWS_DAYS = [30, 90, 180]

# ---------------------------------------------------------------------------
# Sector classification
# ---------------------------------------------------------------------------
GICS_SECTORS = [
    "Energy",
    "Materials",
    "Industrials",
    "Consumer Discretionary",
    "Consumer Staples",
    "Health Care",
    "Financials",
    "Information Technology",
    "Communication Services",
    "Utilities",
    "Real Estate",
]

CYCLICAL_SECTORS = {
    "Information Technology",
    "Consumer Discretionary",
    "Financials",
    "Industrials",
    "Materials",
    "Energy",
}

DEFENSIVE_SECTORS = {
    "Consumer Staples",
    "Health Care",
    "Utilities",
    "Real Estate",
    "Communication Services",
}

# ---------------------------------------------------------------------------
# Exercise-and-sell detection
# ---------------------------------------------------------------------------
# If an S transaction occurs within this many calendar days of an M transaction
# by the same insider for a similar share count, flag as exercise_and_sell
EXERCISE_AND_SELL_WINDOW_DAYS = 3
EXERCISE_AND_SELL_SHARE_TOLERANCE = 0.10  # 10% tolerance on share count match
