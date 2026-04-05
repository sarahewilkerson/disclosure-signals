# Plan: Committee Data + Backtesting + Regime A/B

**Date:** 2026-04-04
**Status:** Approved
**Scope:** R1 (regime A/B), A1-A3 (committees), C1-C2 (backtesting)

See ephemeral plan at `.claude/plans/peaceful-plotting-puzzle.md` for full details.
Plan committed before execution.

---

## Execution Results

**Executed:** 2026-04-04
**Branch:** `feat/committees-backtest-regime-ab`

### R1: Regime A/B Comparison
- Current market: **bear** (SPY -5.4% over 60 days), regime_weight_buy = 1.1
- A/B finding: Regime weighting changes score **magnitude**, not **direction**. Directional accuracy is identical (78.9% at 5d) because regime_weight is a uniform multiplier on all buys. The feature's value would only manifest in portfolio-level sizing or if weights were large enough to cross bullish/bearish thresholds. At +/-10%, it doesn't. Feature remains experimental/disabled-by-default.

### Feature A: Committee Membership Data
- `congress/committees.py` (370 lines): congress.gov API fetcher + GitHub YAML committee assignments
- 538 current members loaded, 531 with committee assignments
- Name resolution works: Pelosi → P000197, Warren → W000817, Tuberville → T000278
- COMMITTEE_SECTOR_MAP: 20 committee codes mapped to GICS sectors
- Warren → Financials + Industrials (Banking + Armed Services)
- Tuberville → Consumer Staples + Health Care + Industrials (Agriculture + Armed Services + HELP)
- 7 tests in test_committees.py

### Feature C: Historical Backtesting
- `analysis/backtest.py` (221 lines): BacktestConfig, run_backtest(), date generator
- `analysis/timeseries.py` (155 lines): signal stability, turnover, Jaccard similarity
- CLI: `signals backtest --start --end --interval --insider-xml-dir --house-pdf-dir --senate-html-dir`
- 4 tests in test_backtest.py

### Test Count
- 103/104 pass (1 pre-existing rg failure)
- 11 new tests (7 committees + 4 backtest)

## Sync Verification
- [x] Verification strategy executed: PASS
- [x] Branch pushed to remote: YES
- [x] Branch merged to main: YES
- [x] Main pushed to remote: YES
- [x] Documentation updated and current: YES
- [x] Production deploy: SKIPPED
- [x] Local, remote, and main are consistent: YES
- Verified at: 2026-04-04
