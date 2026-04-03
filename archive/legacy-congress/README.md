# Congressional Policy Positioning Index (CPPI)

Measures aggregate disclosed congressional portfolio positioning from public financial disclosure filings.

## Overview

CPPI ingests periodic transaction reports (PTRs) from House and Senate disclosure systems, parses transaction details, resolves assets to tickers, and computes positioning signals.

Key features:
- Dual signal: breadth (equal-weight by member) and volume (dollar-weighted)
- Staleness penalties for disclosure lag
- Anti-dominance controls (member caps, winsorization)
- Composite confidence scoring
- Exclusion policy for non-informative assets

## Installation

```bash
# Clone or copy the project
cd congressional_positioning

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .

# Initialize database
python -m cppi.cli init
```

## Quick Start

```bash
# 1. Check cached filings (from prior downloads)
python -m cppi.cli ingest --days 90

# 2. Parse filings to extract transactions
python -m cppi.cli parse

# 3. Compute positioning scores
python -m cppi.cli score --window 90

# 4. Generate report
python -m cppi.cli report --output output/report.txt --stdout

# Check status
python -m cppi.cli status
```

## CLI Commands

### `cppi init`
Initialize the database schema.

### `cppi ingest`
Ingest filings from House and Senate disclosure sites.

```bash
cppi ingest --days 90           # Last 90 days
cppi ingest --bulk --days 90    # Bulk download House PDFs from FD XML
cppi ingest --house-only        # House only
cppi ingest --senate-only       # Senate only
```

**Note:** The `--bulk` flag downloads the House Financial Disclosure (FD) XML files, which contain filer names linked to document IDs. The `cppi parse` command uses these XML files to populate House filer names. Without the FD XML cache, House filings will have "Unknown" as the filer name.

### `cppi parse`
Parse downloaded filings to extract transactions and resolve entities.

```bash
cppi parse           # Skip unchanged filings (idempotent)
cppi parse --force   # Re-parse all filings regardless of cache
```

**Idempotent parsing:** The parse command uses SHA256 hashes to track which filings have been processed. On subsequent runs, unchanged filings are automatically skipped. Use `--force` to re-parse everything.

### `cppi score`
Compute positioning scores for a time window.

```bash
cppi score --window 90          # 90-day window
cppi score --window 30          # 30-day window
```

### `cppi report`
Generate a positioning report.

```bash
cppi report --output report.txt --window 90
cppi report --format json --output report.json
cppi report --stdout            # Also print to terminal
```

### `cppi status`
Show database status and latest scores.

### `cppi enrich` (Phase 2)
Enrich data from external sources.

```bash
cppi enrich --members           # Fetch member data from Congress.gov
cppi enrich --members --committees  # Also fetch committee assignments
cppi enrich --no-cache          # Ignore cache, fetch fresh data
```

### `cppi analyze` (Phase 2)
Run analysis tools for signal quality assessment.

```bash
cppi analyze sensitivity        # Parameter sensitivity analysis
cppi analyze weights            # Breadth vs volume comparison
cppi analyze crossref           # Cross-reference with insider trading data
```

#### Cross-Reference Analysis (Phase 5+6)

Compare congressional trading signals with corporate insider (SEC Form 4) trading data:

```bash
# Basic cross-reference
cppi analyze crossref --window 90

# Specify insider database path
cppi analyze crossref --window 90 --insider-db /path/to/insider_signal.db

# Include JSON summary
cppi analyze crossref --window 90 --json
```

The cross-reference report shows:
- **Convergent signals**: Tickers where both Congress and insiders agree (both bullish or both bearish)
- **Divergent signals**: Tickers where Congress and insiders have opposing views
- **Agreement rate**: Percentage of overlapping tickers with matching signals

Requires the `insidertradingsignal` database. Set `INSIDER_SIGNAL_DB` environment variable or use `--insider-db` flag.

### `cppi validate` (Phase 2)
Validate CPPI data against external vendor sources.

```bash
cppi validate --source quiver   # Compare against Quiver Quantitative
```

Requires `QUIVER_API_KEY` environment variable for Quiver validation.

### `cppi diagnose` (Phase 2)
Run diagnostics on specific members or filings.

```bash
cppi diagnose member P000197    # Diagnose by bioguide ID
cppi diagnose member pelosi     # Diagnose by name pattern
```

### `cppi backtest` (Phase 2)
Run historical backtesting of CPPI signals.

```bash
cppi backtest --start 2024-01-01 --end 2024-06-30
cppi backtest --start 2024-01-01 --end 2024-06-30 --window 90 --forward-days 30
cppi backtest --start 2024-01-01 --end 2024-06-30 --benchmark SPY --store-scores
```

Requires `yfinance` package for historical price data.

**WARNING:** Backtest results are for research only. Past correlations do not imply predictive power.

## Configuration

Environment variables (or in `cppi/config.py`):

```bash
# Anti-dominance controls
CPPI_MEMBER_CAP_PCT=0.05        # Max 5% contribution per member
CPPI_WINSORIZE_PERCENTILE=0.95  # Clip at 95th percentile

# Amount estimation
CPPI_AMOUNT_METHOD=geometric_mean  # or: midpoint, lower_bound
CPPI_USE_LOG_SCALING=false

# Validity thresholds
CPPI_MIN_TRANSACTIONS=50
CPPI_MIN_MEMBERS=10

# Network
CPPI_REQUEST_DELAY=0.5          # Seconds between requests
CPPI_REQUEST_TIMEOUT=30
```

## Methodology

See [docs/methodology.md](docs/methodology.md) for full methodology documentation including:
- Three-timestamp model and staleness penalties
- Amount estimation from ranges
- Owner type weighting
- Inclusion/exclusion policy
- Anti-dominance controls
- Confidence scoring

## Output Format

Sample report output:

```
=============================================================================
CONGRESSIONAL DISCLOSED POSITIONING INDEX
Generated: 2026-03-28
Window: 90 days ending 2026-03-28
=============================================================================

POSITIONING SUMMARY
---------------------------------------------------------------------------
Breadth Signal:      NET BUYERS (62% buy-biased members)
Volume Tilt:         Estimated +$47M equivalent (range-based, lag-adjusted)
Confidence:          MODERATE (score: 0.58)

...
```

## Data Sources

- House: https://disclosures-clerk.house.gov/
- Senate: https://efdsearch.senate.gov/

## Senate Bulk Scraping (Phase 3)

The Senate EFD site requires JavaScript for search results. A Playwright-based scraper is provided for bulk download.

### Requirements

```bash
pip install playwright requests beautifulsoup4
playwright install chromium
```

**Note:** On headless Linux servers, Chromium requires additional system dependencies:
```bash
sudo apt-get install libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
  libcups2 libdrm2 libdbus-1-3 libxkbcommon0 libatspi2.0-0 \
  libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2
```

### Usage

```bash
# Scrape PTR UUIDs from Senate EFD
python scripts/scrape_senate.py scrape \
  --from-date 2024-01-01 \
  --to-date 2026-03-31 \
  --output senate_ptrs.json

# Download PTR HTML files
python scripts/scrape_senate.py download \
  --input senate_ptrs.json \
  --cache-dir ./cache

# Download GIF images for paper filings
python scripts/scrape_senate.py download-gifs \
  --cache-dir ./cache

# Then run normal pipeline
cppi parse
cppi score --window 1500
cppi report --window 1500 --stdout
```

**Note:** Keep `senate_ptrs.json` in the project root. The `cppi parse` command uses it to populate senator names in the database. Without it, Senate filings will have "Unknown" as the filer name.

## OCR for Paper Filings

Some House and Senate filings are scanned paper documents (not electronic). These require OCR to extract text.

### Requirements

```bash
# macOS
brew install tesseract poppler

# Linux
sudo apt-get install tesseract-ocr poppler-utils

# Python packages
pip install pytesseract pdf2image
```

### How It Works

1. **House paper filings** (IDs starting with `822`) are scanned PDFs
2. When `pdfplumber` returns no text, the parser automatically falls back to OCR
3. OCR output is validated before parsing to filter garbage
4. If Tesseract is not installed, paper filings are skipped with a warning

### Verification

```bash
# Run parse and check for zero-transaction warnings
cppi parse 2>&1 | grep -E "(warning|0 transactions)"

# Test OCR on a specific paper filing
python -c "
from cppi.parsing import parse_house_pdf
from pathlib import Path
filing = parse_house_pdf(Path('cache/pdfs/house/8220118.pdf'))
print(f'Transactions: {len(filing.transactions)}')
"
```

## Development

```bash
# Run tests
pytest tests/ -v

# Run linter
ruff check cppi/

# Run specific test file
pytest tests/test_scoring.py -v
```

## Disclaimer

This tool provides aggregate positioning data for research and analysis purposes. It is NOT:
- Financial advice
- An ethics or compliance monitor
- A prediction of future market performance

All data is derived from public disclosure filings. See methodology.md for limitations.

## License

MIT
