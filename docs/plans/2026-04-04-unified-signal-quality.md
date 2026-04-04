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
