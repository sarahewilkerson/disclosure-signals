# Plan: Unified Signal Quality — Insider + Congressional Overlay

**Date:** 2026-04-04
**Status:** Approved
**Scope:** Units 0 + 1 (of 4 total units)

---

## Task Description

The disclosure-signals system has two source adapters (SEC Form 4 insider trading + House/Senate PTR congressional disclosures) feeding a shared core with combined overlay. The overlay currently produces 0 results because insider data was never scored into the derived DB. Signal quality has specific flaws producing noise.

**Goal:** (1) Get insider data into the DB so the overlay produces results, (2) fix 4 signal quality issues that produce the noisiest output.

---

## Approach and Methodology

### Diagnosis

The combined overlay produces 0 results because:
- The DB (`/tmp/disclosure-monitor-sp500-v2.db`) has 12,633 congress normalized transactions and 236 congress signals, but 0 insider data
- The pipeline was interrupted during SEC EDGAR ingestion (17/503 companies)
- 9,238 cached XML files exist at `/tmp/disclosure-monitor-sp500-insider/filings/` but were never scored
- The overlay requires both sources to have matching `subject_key` values — with no insider rows, every congress signal gets blocked as `MISSING_COUNTERPART`

Signal quality issues:
- Single-transaction entities produce score=1.0/-1.0 (no minimum count threshold)
- Managed account trades weighted 0.3 (should be 0.0 — member not making decisions)
- balance_factor gives 1.1x bonus for mixed buy/sell (backwards — unanimous direction is higher conviction)
- 10b5-1 planned trades discounted to 0.25 (too generous — academic evidence shows near-zero predictive power)

---

## Decomposition

| Unit | Description | Size | Depends On | Done When |
|------|------------|------|------------|-----------|
| **Unit 0** | Score insider XMLs into DB, run overlay, assess overlap | M | None | combined_results > 0, overlap documented |
| **Unit 1** | Signal quality quick wins (4 changes) | M | Unit 0 | All changes committed, tests pass, versions bumped |
| Unit 2 | Noise reduction (min value, lag penalty, smooth staleness) | M | Unit 1 | Deferred to separate plan |
| Unit 3 | Overlay hardening (pre-flight check, strength tier) | S | Unit 0 | Deferred to separate plan |

---

## Completion Criteria

### Unit 0
- `SELECT source, COUNT(*) FROM signal_results GROUP BY source` shows both `insider` and `congress`
- `SELECT COUNT(*) FROM combined_results` > 0 (or documented explanation if overlap is zero)
- Overlap entity count documented
- If overlap < 10: reassess overlay value before proceeding

### Unit 1
- All 4 changes committed on `feat/signal-quality-quick-wins` branch
- `pytest tests/ -q` passes (65 tests, 25 files)
- `SELECT COUNT(*) FROM signal_results WHERE included_count = 1 AND label != 'insufficient'` = 0
- Method versions bumped in `versioning.py`

---

## Execution Sequence

### Unit 0 (operational, no code changes)
1. Pre-flight: `.venv/bin/python -m pytest tests/ -q` — confirm green baseline
2. Back up DB: `cp /tmp/disclosure-monitor-sp500-v2.db /tmp/disclosure-monitor-sp500-v2.db.bak`
3. Score insider XMLs via `run_direct_xml_into_derived()` → `/tmp/disclosure-monitor-sp500-v2.db`
4. Run combined overlay via `build_from_derived()`
5. Query DB to verify overlap and document findings
6. **Decision gate:** If overlap < 10, stop and reassess

### Unit 1 (code changes on feature branch)
1. `git checkout -b feat/signal-quality-quick-wins`
2. Commit 1: Min transaction count threshold (1a) — both engines + 2 new tests
3. Commit 2: Managed account weight → 0.0 (1b) + 1 new test
4. Commit 3: Remove balance_factor bonus (1c) + updated parity fixtures + 1 new test
5. Commit 4: Reduce 10b5-1 discount to 0.05 (1d) + 1 new test
6. Commit 5: Bump method versions in `versioning.py`

---

## Identified Risks (Hard 30%)

### Unit 0
1. **XML failures mid-run:** Parse errors handled gracefully (`continue`), but `resolve_entity()` exceptions could abort. Mitigation: DB backup, inspect error, re-run.
2. **Duration:** 9,238 XMLs estimated 2-10 minutes. Monitor for >15min.
3. **Reference date mismatch:** Congress and insider scored with different dates. Acceptable for initial assessment.
4. **Near-zero overlap:** If insider resolves to `cik:` keys while congress uses `entity:ticker`, they won't match. Decision point for whether to proceed.

### Unit 1
1. **`label_from_score` signature change:** Adding `transaction_count` parameter requires updating all callers. Run `grep -r "label_from_score" src/` to find them all.
2. **Parity fixture drift:** Threshold and balance_factor changes invalidate expected test values. Must audit each fixture.
3. **Threshold interaction:** New `len(scored) < 2` check in `aggregate_company_signal` must return same structure as existing insufficient path.

---

## Blast Radius

### Unit 0
- Additive-only: inserts new rows into existing DB
- No repo code changes
- Reversible via DB backup restore

### Unit 1
| File | Change | Risk |
|------|--------|------|
| `src/signals/insider/engine.py` | Remove balance_factor, reduce 10b5-1 discount, add min txn check | Low — constant changes |
| `src/signals/congress/engine.py` | Add min txn count param to `label_from_score`, set managed=0.0 | Medium — signature change |
| `src/signals/core/versioning.py` | Bump 2 method versions | Low |
| `tests/test_engine_parity.py` | Update expected values, add 5 new tests | Low |

No downstream consumers outside this repo. Changes are isolated to scoring constants and one function signature.

---

## Verification Strategy

### Unit 0
1. SQL queries confirming both sources present and combined_results > 0
2. Overlay outcome distribution: `SELECT overlay_outcome, COUNT(*) FROM combined_results GROUP BY overlay_outcome`
3. Entity overlap count: `SELECT COUNT(DISTINCT subject_key) FROM combined_results`
4. Insider key format check: `SELECT COUNT(DISTINCT subject_key) FROM signal_results WHERE source='insider' AND subject_key LIKE 'entity:%'`

### Unit 1
1. Full test suite: `.venv/bin/python -m pytest tests/ -q`
2. New tests: 5 specific test functions (named in plan)
3. SQL verification: no single-transaction non-insufficient signals
4. Before/after combined_results count comparison

---

## Documentation to Update

### Unit 0
- None (operational only)

### Unit 1
- `docs/reason-codes.md` — verify codes match `enums.py`
- `src/signals/core/versioning.py` — bump versions
- Inline comments referencing old constant values

---

## Execution Results

**Executed:** 2026-04-04
**Branch:** `feat/signal-quality-quick-wins`
**Commits:** 7 (1 unplanned parser fix + 4 planned quality changes + 1 version bump + 1 plan doc)

### Unit 0 Results
- 9,238 Form 4 XMLs scored → 23,659 normalized transactions, 390 signal results
- **Unplanned fix discovered:** Parser was not extracting `issuerTradingSymbol` from XML. Fixed in `parser.py` (2 lines) and `direct_service.py` (1 line). This increased entity-keyed signals from 6 to 130 and entity overlap from 6 to 33.
- Combined overlay produced 4 results: GOOG (aligned bearish), AAPL/AMZN/BKNG (low confidence alignment or conflict)
- 199 congress-only entities blocked as SINGLE_SOURCE_ONLY, 12 as AMBIGUOUS
- Decision gate passed: 33 > 10 entity overlap threshold

### Unit 1 Results
- All 4 signal quality changes implemented and tested
- 5 new tests added to `test_engine_parity.py`
- 2 parity fixtures updated (`insider_engine_agg.json`, `expected_vertical_slice.json`)
- Method versions bumped to `2026-04-04.quality1`
- 71/72 tests pass (1 pre-existing `rg` subprocess failure)

### Issues Encountered
- Insider parser missing `issuerTradingSymbol` field — root cause of low overlay overlap. Fixed.
- Vertical slice `_run_congress_vertical_slice` (line 319) uses inline label logic, bypassing `label_from_score()`. Acceptable — vertical slice is a fixture test tool, not production scoring.
- Plan document committed after execution commits (process violation — should precede).

### Process Notes
- Plan Hard 30% #4 (near-zero overlap) correctly predicted the critical issue
- The parser fix was scope creep but was necessary and justified by the decision gate

## Sync Verification
- [x] Verification strategy executed: PASS (71/72 tests, 1 pre-existing)
- [x] Branch pushed to remote: YES
- [x] Branch merged to main: YES (fast-forward)
- [x] Main pushed to remote: YES
- [x] Documentation updated and current: YES
- [x] Production deploy: SKIPPED (no deploy command configured)
- [x] Local, remote, and main are consistent: YES
- Verified at: 2026-04-04

---

## Units 2-3 Execution Results

**Executed:** 2026-04-04
**Branch:** `feat/noise-reduction-and-overlay-hardening`
**Commits:** 5 (3 scoring changes + 1 overlay hardening + 1 version bump)

### Unit 2 Results
- **2a:** Minimum trade value thresholds implemented. Insider: $10K floor. Congress: $1,001-$15,000 bracket excluded. `BELOW_MINIMUM_VALUE` ReasonCode added.
- **2b:** Disclosure lag penalty function added. Default 0.7 when disclosure_date unknown. Integrated into `score_transaction` via optional parameter.
- **2c:** Staleness step function replaced with 60-day half-life exponential decay. Eliminates cliffs at day boundaries.

### Unit 3 Results
- **3a:** Pre-flight completeness warning added to `build_from_derived()`.
- **3b:** Deferred (overlap sparse, would further reduce combined results).
- **3c:** Deferred (low priority for this cycle).

### Tests Added (remediation)
- `test_disclosure_lag_penalty` — 7 assertions covering all lag brackets + None handling
- `test_minimum_trade_value_insider_exclusion` — verifies constant value
- `test_minimum_trade_value_congress_exclusion` — verifies both house and senate constants
- `test_staleness_continuous_decay` — verifies monotonic decrease with no cliffs over 365 days

### Process Notes
- Units 2-3 executed without separate plan documents (plan said "will get their own plans" but user authorized direct continuation)
- 3 test fixture failures discovered during 2a implementation (test data used lowest bracket, now excluded)
- `MINIMUM_CONGRESS_TRADE_AMOUNT` duplicated in 2 files — tracked for future deduplication

## Units 2-3 Sync Verification
- [x] Verification strategy executed: PASS (76/77 tests, 1 pre-existing)
- [x] Branch pushed to remote: YES
- [x] Branch merged to main: YES (fast-forward)
- [x] Main pushed to remote: YES
- [x] Documentation updated and current: YES
- [x] Production deploy: SKIPPED (no deploy command configured)
- [x] Local, remote, and main are consistent: YES
- Verified at: 2026-04-04
