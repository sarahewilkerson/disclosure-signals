# CPPI Phase 5+6: Insider Trading Cross-Reference Integration

**Status:** APPROVED
**Date:** 2026-04-01
**Location:** `/tmp/congressional_positioning/`
**Branch:** `feat/2026-04-01-insider-crossref`

---

## Objective

Add cross-referencing capability between congressional trades (CPPI) and corporate insider trades (insidertradingsignal) to identify:
1. Tickers where both Congress and insiders show the same signal
2. Divergences where signals conflict
3. "Smart money convergence" when multiple data sources agree

---

## Current State

| Project | Location | Tickers | Transactions |
|---------|----------|---------|--------------|
| CPPI | `/tmp/congressional_positioning/` | ~500 | 115,716 |
| insidertradingsignal | `/tmp/insidertradingsignal/` | ~200 | ~50,000+ |

**Overlapping tickers found:** NVDA, META, LLY, CRM, GOOGL, and many others.

---

## Integration Approach

**Read-only cross-database queries** — Keep both projects separate, add cross-referencing module to CPPI that queries both databases.

### Why this approach:
- No data migration needed
- Both projects remain independently functional
- Easy to extend with additional data sources later
- Low blast radius

---

## Unit Decomposition

### Unit 1: Create Cross-Reference Module (S)

**Files to create:**
- `cppi/analysis/crossref.py` — Core cross-referencing logic

**Implementation:**
```python
def get_insider_signals(db_path: str, tickers: list[str], window_days: int) -> dict:
    """Query insidertradingsignal DB for signals on given tickers."""

def compute_crossref_signals(
    cppi_transactions: list,
    insider_signals: dict,
    window_days: int
) -> CrossRefReport:
    """Compare congressional vs insider signals."""

def format_crossref_report(report: CrossRefReport) -> str:
    """Format report for display."""
```

**Done when:** Module imports without errors, basic functions implemented.

### Unit 2: Add CLI Integration (S)

**Files to modify:**
- `cppi/cli.py` — Add `crossref` analysis type

**Implementation:**
1. Add to argparse: `analyze crossref --window 90 --insider-db PATH`
2. Call `crossref.py` functions
3. Output formatted report

**Done when:**
```bash
cppi analyze crossref --window 90 --insider-db /tmp/insidertradingsignal/insider_signal.db
# Outputs cross-reference report
```

### Unit 3: Cross-Reference Report Format (S)

**Report sections:**
1. **Convergent Signals** — Tickers where both sources agree (both bullish or both bearish)
2. **Divergent Signals** — Tickers where sources conflict
3. **Congress-Only** — Tickers traded by Congress but no insider data
4. **Summary Statistics** — Agreement rate, top tickers by conviction

**Example output:**
```
CONGRESSIONAL / INSIDER CROSS-REFERENCE
=======================================
Window: 90 days

CONVERGENT (BOTH BULLISH): 12 tickers
  NVDA: Congress +$2.1M net | Insiders +15 net buys
  META: Congress +$890K net | Insiders +8 net buys

CONVERGENT (BOTH BEARISH): 5 tickers
  ...

DIVERGENT: 8 tickers
  TSLA: Congress BUYING (+$1.2M) | Insiders SELLING (-23 net)
  ...

AGREEMENT RATE: 67% (on overlapping tickers)
```

### Unit 4: Tests (S)

**Files to create:**
- `tests/test_crossref.py`

**Test cases (with mocks - no real DB dependency):**
1. `test_get_insider_signals_returns_dict()` — Mock sqlite3, verify dict structure
2. `test_get_insider_signals_db_not_found()` — Return empty dict, log warning
3. `test_crossref_identifies_convergence()` — Both sources bullish on same ticker
4. `test_crossref_identifies_divergence()` — Congress buying, insiders selling
5. `test_crossref_no_overlap()` — Gracefully handles zero overlapping tickers
6. `test_crossref_empty_window()` — Handles case where window has no transactions
7. `test_cli_crossref_missing_db()` — Prints warning, exits gracefully
8. `test_cli_crossref_runs()` — Integration test with tmp DBs

**Mock strategy:** Use `unittest.mock.patch` to mock `sqlite3.connect()` for unit tests. Create temporary SQLite DBs for integration test only.

---

## Execution Order

```
Unit 1 (Module) ──► Unit 2 (CLI) ──► Unit 3 (Report) ──► Unit 4 (Tests)
```

Sequential — each unit depends on the previous.

---

## Files to Modify/Create

| File | Action | Description |
|------|--------|-------------|
| `cppi/analysis/crossref.py` | CREATE | Core cross-reference logic |
| `cppi/analysis/__init__.py` | MODIFY | Add crossref exports |
| `cppi/cli.py` | MODIFY | Add `analyze crossref` command |
| `tests/test_crossref.py` | CREATE | Test coverage |

---

## Configuration

New optional environment variable:
```
INSIDER_SIGNAL_DB=/tmp/insidertradingsignal/insider_signal.db
```

Default: Look in standard location if not specified.

---

## Edge Case Handling

| Scenario | Handling |
|----------|----------|
| Insider DB not found | Log warning, return empty report with message "Insider DB not available" |
| No overlapping tickers | Report shows "0 overlapping tickers" in summary, no convergent/divergent sections |
| Stale insider data (>90 days old) | Check max transaction_date, warn if older than window |
| Empty CPPI transactions | Exit early with "No CPPI transactions in window" |
| Ticker normalization (GOOG/GOOGL) | Normalize to primary ticker using TICKER_ALIASES map |

---

## Hard 30% (Uncertainty Areas)

1. **Ticker normalization** — Congress uses "GOOG" vs "GOOGL", need to handle both
   - Mitigation: Create TICKER_ALIASES dict mapping variants to canonical ticker
2. **Date alignment** — Insider data may have different date ranges
   - Mitigation: Check date range overlap, warn if <50% overlap
3. **Signal calculation consistency** — Ensure buy/sell classification matches across projects
   - Mitigation: Use same logic: buy=positive net, sell=negative net
4. **Performance** — Cross-database queries could be slow; may need caching
   - Mitigation: Limit to top 100 congressional tickers by volume

---

## Blast Radius

**Low risk:**
- New module, doesn't change existing functionality
- Read-only queries to both databases
- Optional feature (works without insider DB)

**Potential issues:**
- If insidertradingsignal schema changes, queries will break
- Mitigation: Version-check the schema on startup

---

## Verification Strategy

### Pre-flight:
```bash
pytest tests/ -q  # 296 tests pass
```

### Post-flight:
```bash
pytest tests/ -q  # 296+ tests pass (new tests added)
cppi analyze crossref --window 90 --insider-db /tmp/insidertradingsignal/insider_signal.db
# Should output report with convergent/divergent tickers
```

---

## Commit Sequence

1. `docs: add Phase 5+6 insider crossref plan`
2. `feat(analysis): add crossref module for insider comparison`
3. `feat(cli): add analyze crossref command`
4. `test: add crossref test coverage`

---

## Documentation to Update

| Document | Change |
|----------|--------|
| `README.md` | Add section on `cppi analyze crossref` command with usage example |
| `cppi/config.py` | Add `INSIDER_SIGNAL_DB` config option with default path |

---

## Out of Scope

- Merging databases into one
- Automated alerts/notifications
- Web UI for cross-reference
- Historical backtesting of convergent signals

These could be future enhancements.

---

## Execution Results

**Status:** COMPLETED
**Date:** 2026-04-01
**Branch:** `feat/2026-04-01-insider-crossref`

### Commits
1. `034c5b1` - docs: add Phase 5+6 insider crossref plan
2. `1abad75` - feat(analysis): add crossref module for insider comparison

### Implementation Notes
- Unit 1 (Module): Created `cppi/analysis/crossref.py` with all planned functions
- Unit 2 (CLI): Added `cppi analyze crossref` command with `--insider-db` and `--json` flags
- Unit 3 (Report): Implemented in crossref.py (format_crossref_report)
- Unit 4 (Tests): Created `tests/test_crossref.py` with 28 test cases

### Bug Fix During Execution
**Issue:** `amount_midpoint` column was NULL for all CPPI transactions, causing all signals to show as NEUTRAL.
**Fix:** Changed query to use `COALESCE(amount_midpoint, (amount_min + amount_max) / 2.0, 0)` for proper midpoint calculation.

### Deviations from Plan
1. **config.py update skipped** — Used `os.environ.get("INSIDER_SIGNAL_DB", DEFAULT_INSIDER_DB)` directly in crossref.py instead of adding to config.py. Functionally equivalent and simpler.

### Verification Results
- Pre-flight: 296 tests pass ✅
- Post-flight: 324 tests pass ✅ (28 new tests added)
- CLI functional test:
  ```
  cppi analyze crossref --window 90
  # Convergent (Both Bearish): 19 tickers
  # Divergent: 19 tickers
  # Agreement rate: 50% (39 overlapping tickers)
  ```

### Signs Added to CLAUDE.md
- 2026-04-01: When querying SQLite databases across projects, verify column values exist during planning context gathering.

---

## Sync Verification
- [x] Verification strategy executed: PASS (324 tests)
- [x] Branch pushed to remote: N/A (local project, no remote)
- [x] Branch merged to main: YES (commit 4dcfd73)
- [x] Main pushed to remote: N/A (local project)
- [x] Documentation updated and current: YES (README.md updated)
- [x] Production deploy: N/A (local project)
- [x] Local, remote, and main are consistent: YES (local only)
- Verified at: 2026-04-01T21:50:00Z
