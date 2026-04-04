# Plan: Polish + Test Debt Cleanup

**Date:** 2026-04-04
**Status:** Planned
**Scope:** CLI polish, test coverage, brief integration, stale doc cleanup

---

## Context

Execution reviews across Units 5-8 accumulated tracked items: missing CLI flags, missing tests for yfinance-dependent modules, partial sector integration, and a stale plan doc reference. None are blockers but collectively they represent debt that should be cleaned before further feature work.

---

## Changes

### 9a. CLI --baseline and --regime flags on validate

Add flags to existing `signals validate` command:
- `--baseline` runs `run_baseline_comparison()` appended to output
- `--regime` runs `run_regime_analysis()` appended to output

File: `src/signals/cli.py` (modify `cmd_validate` and `validate` subparser)

### 9b. Sector summary in daily brief

Integrate `build_sector_summary()` into `render_daily_brief_markdown()` so the sector table appears automatically when sector data is available, without requiring a separate call.

File: `src/signals/analysis/daily_brief.py`

### 9c. Tests for sectors.py and validation.py (mocked yfinance)

Add tests that mock yfinance to test:
- `get_sector_map()` cache logic
- `_find_anomalous_activity()` edge cases (no history, elevated activity)
- `run_baseline_comparison()` and `run_regime_analysis()` with mocked returns

File: `tests/test_engine_parity.py`

### 9d. Clean stale plan doc reference

Remove "MINIMUM_CONGRESS_TRADE_AMOUNT duplicated" note from first plan doc — it was resolved in Unit 5c.

File: `docs/plans/2026-04-04-unified-signal-quality.md`

---

## Completion Criteria
- `signals validate --baseline` and `--regime` work
- `signals brief` includes sector summary automatically
- 3+ new tests for mocked yfinance paths
- No stale tracked items in plan docs
