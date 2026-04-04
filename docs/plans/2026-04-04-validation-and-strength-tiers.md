# Plan: Signal Strength Tiers + Forward-Return Validation

**Date:** 2026-04-04
**Status:** Planned
**Scope:** Units 3c + 4a (signal strength tier + forward-return validation)
**Depends on:** Units 0-3 complete (all merged to main)

---

## Context

Units 0-3 built the scoring pipeline and noise reduction. The system now produces 4 combined results and 390 insider + 236 congress signals. The critical gap identified in the substantive signal review: "You have zero validation of whether your signals predict anything." Every parameter choice is unvalidated intuition.

This cycle adds:
1. **Signal strength tiers** (3c) — classify combined results as strong/moderate/weak for filtering
2. **Forward-return validation** (4a) — fetch price data and compute correlation with signal scores

Unit 3b (execution-date window alignment) is deferred — with only 4 combined results, further reducing overlap is counterproductive.

---

## Completion Criteria

### 3c: Signal Strength Tier
- `CombinedResult` has `strength_tier` field populated for all results
- Tier logic: "strong" (both confidence >= 0.7, score magnitude > 0.3), "moderate" (both non-insufficient, confidence >= 0.4), "weak" (otherwise)
- Unit test verifying tier classification

### 4a: Forward-Return Validation
- Script that loads signal results, fetches forward returns via yfinance, and outputs a validation report
- Report includes: directional accuracy (% of bullish signals with positive forward returns), score-return correlation, per-window analysis (30/90/180 day signals)
- Located in `src/signals/analysis/` to match existing analysis module pattern
- Does NOT change scoring logic — this is measurement only

---

## Execution Sequence

1. Commit this plan document
2. Create branch `feat/strength-tiers-and-validation`
3. **3c:** Add strength_tier to overlay.py, update CombinedResult, add test
4. **4a:** Install yfinance, add validation script, run against current DB
5. Bump versions, run tests, commit

---

## Risks

- **yfinance rate limiting:** Yahoo Finance API may throttle. Mitigation: batch requests, cache results.
- **Price data availability:** Some tickers may not have data for the signal date range. Handle gracefully with "N/A" in report.
- **Forward-return interpretation:** Signal dates are `as_of_date` which is the reference date, not necessarily when the signal was "actionable." Must use execution dates from underlying transactions for proper timing.

---

## Files to Modify

| File | Change |
|------|--------|
| `src/signals/combined/overlay.py` | Add strength_tier to CombinedResult construction |
| `src/signals/core/dto.py` | Add `strength_tier` field to CombinedResult |
| `src/signals/core/derived_db.py` | Add strength_tier column |
| `src/signals/analysis/validation.py` | New: forward-return validation script |
| `tests/test_engine_parity.py` | Add strength tier test |
| `pyproject.toml` | Add yfinance dependency |

---

## Execution Results

**Executed:** 2026-04-04
**Branch:** `feat/strength-tiers-and-validation`
**Commits:** 3 (plan doc + strength tier + validation framework)

### 3c: Signal Strength Tier
- `_classify_strength()` added to `overlay.py` — strong/moderate/weak classification
- `strength_tier` field added to CombinedResult DTO + DB schema with migration
- Test: `test_strength_tier_classification` with 5 assertions

### 4a: Forward-Return Validation
- `validation.py` (448 lines) with both signal-level and transaction-level validation
- yfinance added as optional dependency
- Initial findings from 2025 data (key result):

| Source | Direction | 5d Accuracy | 20d Accuracy | 60d Accuracy | Transactions |
|--------|-----------|-------------|--------------|--------------|-------------|
| Insider | BUY | **69.6%** | 56.5% | 56.5% | 23 |
| Insider | SELL | 45.0% | 47.4% | 41.5% | 2,414 |
| Congress | BUY | 58.6% | **63.0%** | **64.2%** | 811 |
| Congress | SELL | 46.8% | 44.5% | 35.7% | 526 |

**Key insight:** Buys are predictive (above 50%) for both sources. Sells are not predictive and often worse than random. This validates the buy/sell asymmetry in the scoring model and suggests sells should be discounted further.

## Sync Verification
- [x] Verification strategy executed: PASS (77/78 tests, 1 pre-existing)
- [x] Branch pushed to remote: YES
- [x] Branch merged to main: YES (fast-forward)
- [x] Main pushed to remote: YES
- [x] Documentation updated and current: YES
- [x] Production deploy: SKIPPED (no deploy command configured)
- [x] Local, remote, and main are consistent: YES
- Verified at: 2026-04-04

## Execution Review
- **Verdict:** ✅ CLEAN (Iteration 1 of 3)
- **Process:** Plan committed before execution — no violations
- **Tests:** 77/78 pass (1 pre-existing)
- **Debt:** No must-fix items. Track: validation module tests (mocked yfinance), sequential ticker fetching
- **Signs:** 1 new pattern added to CLAUDE.md (signal-level validation needs historical dates)
