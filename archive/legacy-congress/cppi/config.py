"""
Configuration module for CPPI.

All tunable parameters are defined here to support sensitivity analysis
and methodology adjustments without code changes.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "cache"
DB_PATH = Path(os.getenv("CPPI_DB_PATH", str(DATA_DIR / "cppi.db")))

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
(CACHE_DIR / "pdfs" / "house").mkdir(parents=True, exist_ok=True)
(CACHE_DIR / "pdfs" / "senate").mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Anti-Dominance Controls (Section 8 of design)
# ---------------------------------------------------------------------------

# Per-member cap: No single member contributes more than this % of total signal
MEMBER_CAP_PCT = float(os.getenv("CPPI_MEMBER_CAP_PCT", "0.05"))

# Winsorization: Clip transaction amounts at this percentile
WINSORIZE_PERCENTILE = float(os.getenv("CPPI_WINSORIZE_PERCENTILE", "0.95"))

# Amount estimation method: 'geometric_mean', 'midpoint', 'lower_bound', 'log_uniform'
AMOUNT_METHOD = os.getenv("CPPI_AMOUNT_METHOD", "geometric_mean")

# Log scaling: Apply log(1 + value) to compress large transactions
USE_LOG_SCALING = os.getenv("CPPI_USE_LOG_SCALING", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Staleness Penalties (Section 4 of design)
# ---------------------------------------------------------------------------
# Penalty applied based on total_lag (execution_date to ingestion_date)
STALENESS_PENALTIES = {
    45: 1.0,   # <= 45 days: fresh
    60: 0.9,   # <= 60 days: slightly stale
    90: 0.7,   # <= 90 days: stale
    180: 0.4,  # <= 180 days: very stale
    # > 180 days: 0.2 (default)
}
STALENESS_DEFAULT = 0.2

# ---------------------------------------------------------------------------
# Owner Type Weights (Section 6.5 of design)
# ---------------------------------------------------------------------------
OWNER_WEIGHTS = {
    "self": 1.0,
    "spouse": 0.8,
    "joint": 0.9,
    "dependent": 0.5,
    "managed": 0.3,
}
OWNER_WEIGHT_DEFAULT = 0.3

# ---------------------------------------------------------------------------
# Signal Validity Thresholds (Section 10.5 of design)
# ---------------------------------------------------------------------------

# Minimum transactions required to generate a signal
MIN_TRANSACTIONS = int(os.getenv("CPPI_MIN_TRANSACTIONS", "50"))

# Minimum unique members required
MIN_MEMBERS = int(os.getenv("CPPI_MIN_MEMBERS", "10"))

# ---------------------------------------------------------------------------
# Amount Ranges (STOCK Act standard ranges)
# ---------------------------------------------------------------------------
# Maps range text to (min, max) tuple
AMOUNT_RANGES = {
    "$1,001 - $15,000": (1_001, 15_000),
    "$15,001 - $50,000": (15_001, 50_000),
    "$50,001 - $100,000": (50_001, 100_000),
    "$100,001 - $250,000": (100_001, 250_000),
    "$250,001 - $500,000": (250_001, 500_000),
    "$500,001 - $1,000,000": (500_001, 1_000_000),
    "$1,000,001 - $5,000,000": (1_000_001, 5_000_000),
    "$5,000,001 - $25,000,000": (5_000_001, 25_000_000),
    "$25,000,001 - $50,000,000": (25_000_001, 50_000_000),
    "Over $50,000,000": (50_000_001, 100_000_000),
}

# ---------------------------------------------------------------------------
# Data Source URLs
# ---------------------------------------------------------------------------
HOUSE_BASE_URL = "https://disclosures-clerk.house.gov"
HOUSE_PTR_URL = f"{HOUSE_BASE_URL}/public_disc/ptr-pdfs"

SENATE_BASE_URL = "https://efdsearch.senate.gov"
SENATE_SEARCH_URL = f"{SENATE_BASE_URL}/search/home/"
SENATE_PTR_URL = f"{SENATE_BASE_URL}/search/view/ptr"
SENATE_PAPER_URL = f"{SENATE_BASE_URL}/search/view/paper"
SENATE_MEDIA_URL = "https://efd-media-public.senate.gov"

# ---------------------------------------------------------------------------
# Rate Limiting
# ---------------------------------------------------------------------------
REQUEST_DELAY_SECONDS = float(os.getenv("CPPI_REQUEST_DELAY", "0.5"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CPPI_REQUEST_TIMEOUT", "30"))

# Aliases for cleaner imports
REQUEST_DELAY = REQUEST_DELAY_SECONDS
REQUEST_TIMEOUT = REQUEST_TIMEOUT_SECONDS

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("CPPI_LOG_LEVEL", "INFO")
