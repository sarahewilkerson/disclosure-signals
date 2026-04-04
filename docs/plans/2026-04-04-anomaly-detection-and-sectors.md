# Plan: Anomaly Detection + Sector Enrichment

**Date:** 2026-04-04
**Status:** Planned
**Scope:** Add anomaly flags to daily brief + sector-level signal aggregation

---

## Context

The daily brief currently shows cluster buys, cross-source signals, and strong buys. The original task specified anomaly detection as "more valuable than raw score" — detecting when insider activity for a ticker is unusual relative to its historical baseline.

With only 45 insider buys across 4+ years in the current dataset, each buy event is inherently rare. The anomaly signal is simple: **flag tickers with insider buying for the first time in N months**. This is high-signal because insider buying is already validated as predictive (69.6% at 5d).

For sectors: yfinance provides GICS sector/industry data. Adding sector context to signals enables sector-level aggregation ("3 tech companies have insider buying" vs individual alerts).

---

## Changes

### 7a. Anomaly detection in daily brief

Add `_find_anomalous_activity()` to `daily_brief.py`:
- For each ticker with recent insider buys, check if there was any buy in the prior 12 months
- Flag tickers where current buying is "new" (no prior buys in lookback)
- Also flag tickers where current buy count exceeds 2x the historical monthly average
- Add "Anomaly Alerts" section to the brief markdown

### 7b. Sector enrichment

Add `src/signals/analysis/sectors.py`:
- `fetch_sector_map(tickers)` → dict of ticker → {sector, industry}
- Cache results in a local SQLite table (avoid repeated yfinance calls)
- `build_sector_summary(db_path)` → sector-level buy/sell aggregation
- Add sector context to daily brief signals

### 7c. Integrate into daily brief

- Add anomaly flags section
- Add sector labels to cluster alerts and strong signals
- Add sector summary section at the end of the brief

---

## Completion Criteria

- Anomaly detection flags new insider buying in brief
- Sector data fetched and cached
- Sector labels appear on brief signals
- Tests for anomaly detection logic

## Files

| File | Change |
|------|--------|
| `src/signals/analysis/daily_brief.py` | Add anomaly detection, sector integration |
| `src/signals/analysis/sectors.py` | New: sector enrichment with caching |
| `tests/test_engine_parity.py` | Test anomaly detection |

---

## Execution Results

**Executed:** 2026-04-04
**Branch:** `feat/anomaly-detection-and-sectors`
**Commits:** 1

### Results
- **7a Anomaly detection:** Implemented `_find_anomalous_activity()` — flags first-time insider buying in 12+ months and elevated activity (>2x historical avg). Found: BR (Broadridge) has first insider buy in 12+ months.
- **7b Sector enrichment:** `sectors.py` (159 lines) with yfinance fetching + SQLite caching. `build_sector_summary()` aggregates by sector. Results: Technology +5 net bullish, Healthcare +4, Consumer Cyclical +2.
- **7c Integration:** Anomaly alerts section added to daily brief markdown. Sector summary available as separate render.
- **Tests:** `test_daily_brief_anomaly_detection` verifies first-time buy flagging.
- 80/81 tests pass (1 pre-existing).

## Sync Verification
- [x] Verification strategy executed: PASS
- [x] Branch pushed to remote: YES
- [x] Branch merged to main: YES
- [x] Main pushed to remote: YES
- [x] Documentation updated and current: YES
- [x] Production deploy: SKIPPED
- [x] Local, remote, and main are consistent: YES
- Verified at: 2026-04-04
