# Insider Trading Signal Engine

A Python-based system for analyzing SEC Form 4 filings to generate insider trading signals for Fortune 500 companies.

## Overview

The Insider Trading Signal Engine processes SEC EDGAR Form 4 filings (insider ownership reports) and computes actionable buy/sell signals based on C-suite executive trading activity. The system applies empirically-validated weighting factors for transaction type, insider role, trade size, and timing to produce company-level signals with confidence tiers.

**Key Features:**
- Automated Form 4 XML parsing with amendment deduplication
- Role-based transaction weighting (CEO/CFO trades weighted higher)
- 10b5-1 plan detection and discounting
- Configurable analysis windows (30, 90, 180 days)
- Company-level signal aggregation with confidence tiers
- Market-wide risk appetite index

See [methodology.md](methodology.md) for full technical documentation of the scoring algorithm.

## Installation

### Requirements
- Python 3.10+
- SQLite 3.35+ (included with Python)

### Setup

```bash
# Clone the repository
git clone https://github.com/sarahewilkerson/insidertradingsignal.git
cd insidertradingsignal

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

### Required: SEC User-Agent

The SEC requires a properly formatted User-Agent header for all requests. Set this environment variable before running:

```bash
export SEC_USER_AGENT="YourAppName/1.0 (your-email@company.com)"
```

**Important:** The application will fail to start if the User-Agent contains the placeholder `example.com`. You must provide a valid email address.

### Optional Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SEC_USER_AGENT` | (required) | Format: `AppName/Version (email@domain.com)` |
| `SEC_BASE_URL` | `https://www.sec.gov` | SEC website base URL |
| `SEC_DATA_URL` | `https://data.sec.gov` | SEC data API base URL |
| `SEC_EFTS_URL` | `https://efts.sec.gov/LATEST` | SEC full-text search API |
| `DB_PATH` | `./insider_signal.db` | SQLite database file location |
| `CACHE_DIR` | `./cache` | Directory for cached filings |

### For Testing

To skip configuration validation during tests:

```bash
export SKIP_CONFIG_VALIDATION=1
python -m pytest
```

## Usage

### Quick Start

```bash
# Initialize database and ingest sample companies
python cli.py run --csv sample_companies.csv --max-filings 10

# Check database status
python cli.py status

# Generate signal report
python cli.py report
```

### CLI Commands

#### `ingest` - Download and parse Form 4 filings

```bash
# Ingest filings for companies in CSV
python cli.py ingest --csv companies.csv

# Limit filings per company (for testing)
python cli.py ingest --csv companies.csv --max-filings 5
```

The CSV must have columns: `ticker`, `company_name`, `sector` (optional: `fortune_rank`, `revenue`)

#### `classify` - Apply transaction classification rules

```bash
python cli.py classify
```

Classifies transactions by:
- Role (CEO, CFO, chair, president, COO, director, 10% owner)
- Transaction type (open market buy/sell, option exercise, etc.)
- Planned vs discretionary (10b5-1 detection)

#### `score` - Compute company signals

```bash
# Score all companies
python cli.py score

# Score specific company by CIK
python cli.py score --cik 0000320193
```

#### `report` - Generate signal report

```bash
# Full report
python cli.py report

# Bullish signals only
python cli.py report --signal bullish

# Top N by confidence
python cli.py report --top 10
```

#### `run` - Full pipeline

```bash
# Run complete pipeline: ingest -> classify -> score -> report
python cli.py run --csv companies.csv
```

#### Async HTTP Mode

For faster ingestion with concurrent requests:

```bash
# Use async HTTP (faster for large universes)
python cli.py ingest --csv companies.csv --async

# Control concurrency (default: 5)
python cli.py ingest --csv companies.csv --async --concurrency 3
```

#### Historical Backfill

```bash
# Backfill historical data for a date range
python cli.py ingest --csv companies.csv --start-date 2023-01-01 --end-date 2023-12-31
```

#### `status` - Database statistics

```bash
python cli.py status
```

### Example CSV Format

```csv
ticker,company_name,sector,fortune_rank,revenue
AAPL,Apple Inc.,Information Technology,1,394328
MSFT,Microsoft Corporation,Information Technology,2,211915
AMZN,Amazon.com Inc.,Consumer Discretionary,3,513983
```

## REST API

The engine includes a FastAPI-based REST API for programmatic access.

### Starting the API Server

```bash
# Start the API server
uvicorn api:app --host 0.0.0.0 --port 8000

# Development mode with auto-reload
uvicorn api:app --reload
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check and database status |
| `/status` | GET | Database statistics and pipeline status |
| `/scores` | GET | List company scores with filtering |
| `/scores/{ticker}` | GET | Get score for specific company |
| `/aggregate` | GET | Market-wide sentiment index |
| `/sectors` | GET | List available sectors |
| `/companies` | GET | List companies in universe |

### Example API Calls

```bash
# Health check
curl http://localhost:8000/health

# Get all bullish signals
curl "http://localhost:8000/scores?signal=BULLISH&window_days=90"

# Get score for Apple
curl http://localhost:8000/scores/AAPL

# Get aggregate market index
curl "http://localhost:8000/aggregate?window_days=90"
```

### API Documentation

Interactive API documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Architecture

```
insidertradingsignal/
├── cli.py           # Command-line interface
├── api.py           # REST API (FastAPI)
├── config.py        # Configuration and thresholds
├── db.py            # Database schema and queries
├── parsing.py       # Form 4 XML parser
├── classification.py # Transaction classification
├── scoring.py       # Signal computation
├── reporting.py     # Report generation
├── ingestion.py     # SEC EDGAR API client (sync + async)
├── universe.py      # Company universe management
├── types.py         # TypedDict definitions
└── methodology.md   # Scoring algorithm documentation
```

### Data Flow

1. **Ingestion**: Download Form 4 XML files from SEC EDGAR
2. **Parsing**: Extract filing and transaction data from XML
3. **Classification**: Apply role and transaction type rules
4. **Scoring**: Compute weighted signals per company
5. **Reporting**: Generate actionable signal reports

### Database Schema

- `companies` - Company metadata (CIK, ticker, sector)
- `filings` - Form 4 filing records
- `transactions` - Individual transactions with classifications
- `company_scores` - Computed signals per analysis window
- `aggregate_index` - Market-wide indicators

## Signal Interpretation

| Signal | Score Range | Meaning |
|--------|-------------|---------|
| Bullish | > 0.15 | Net insider buying; potential positive outlook |
| Bearish | < -0.15 | Net insider selling; potential negative outlook |
| Neutral | -0.15 to 0.15 | Mixed or minimal activity |
| Insufficient | confidence < 0.25 | Not enough data for reliable signal |

### Confidence Tiers

| Tier | Range | Interpretation |
|------|-------|----------------|
| High | 0.75 - 0.90 | Strong signal, multiple insiders, diverse activity |
| Moderate | 0.50 - 0.75 | Good signal, some corroborating activity |
| Low | 0.25 - 0.50 | Weak signal, limited activity |
| Insufficient | < 0.25 | Not enough transactions for reliable signal |

## Running Tests

```bash
# Run all tests
SKIP_CONFIG_VALIDATION=1 python -m pytest

# Run with verbose output
SKIP_CONFIG_VALIDATION=1 python -m pytest -v

# Run specific test file
SKIP_CONFIG_VALIDATION=1 python -m pytest tests/test_parsing.py
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make changes and add tests
4. Run tests: `python -m pytest`
5. Submit a pull request

### Code Style

- Follow PEP 8 conventions
- Add type hints to function signatures
- Include docstrings for public functions
- Write unit tests for new functionality

## License

[Add license information]

## Acknowledgments

- SEC EDGAR for public filing data
- Academic research on insider trading signals (see methodology.md for citations)
