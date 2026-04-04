# Plan: Validation-Driven Scoring Tuning + Operational Improvements

**Date:** 2026-04-04
**Status:** Planned
**Scope:** Act on validation findings + fix operational gaps

---

## Context

The forward-return validation (Unit 4a) produced the first empirical evidence for scoring parameters:

- **Insider buys: 69.6% accuracy at 5d** (n=23) — real signal
- **Insider sells: 41-47% accuracy** — worse than random, noise
- **Congress buys: 58-64% accuracy** improving with horizon — real signal
- **Congress sells: 35-47% accuracy** — noise
- **Key finding: buys are predictive, sells are not** for both sources

Current sell weight (`DIRECTION_WEIGHT_SELL = -0.5`) is too generous. Mean forward returns after sells are positive (stocks go UP after insider/congress selling), meaning the sell signal has *negative* predictive value.

Additionally, congress `score_transaction` callers never pass `disclosure_date`, so all congress trades receive the default 0.7 lag penalty even when the actual disclosure date is available.

---

## Changes

### 5a. Reduce insider sell weight (empirically driven)

Change `DIRECTION_WEIGHT_SELL` from `-0.5` to `-0.15` in `src/signals/insider/engine.py`.

**Rationale:** Validation shows sells have 41-47% directional accuracy (worse than coin flip). The sell weight should be low enough to prevent sells from dominating signals. At -0.15, a sell contributes 15% of the magnitude of a buy, reflecting its near-zero information content.

**Blast radius:** Changes insider signal scores. 45 bearish signals will shift toward neutral. Some may flip to insufficient. This is desired — the bearish signals are empirically wrong most of the time.

### 5b. Pass disclosure_date to congress scoring

Update both `direct_service.py` and `senate_direct.py` to pass `disclosure_date` to `score_transaction()`. Currently both have the disclosure date available in the transaction data but don't pass it.

- House: `txn.notification_date` is the disclosure date
- Senate: `txn.transaction_date` is execution, disclosure date comes from the filing metadata

**Blast radius:** Changes congress scores. Trades with known disclosure dates will get more accurate lag penalties instead of the default 0.7.

### 5c. Deduplicate MINIMUM_CONGRESS_TRADE_AMOUNT

Move the constant to `src/signals/congress/constants.py` and import from both `direct_service.py` and `senate_direct.py`.

### 5d. Bump method versions

Increment `INSIDER_SCORE_METHOD_VERSION` and `CONGRESS_SCORE_METHOD_VERSION`.

---

## Completion Criteria

- Sell weight = -0.15 with test verifying ratio
- Both congress services pass disclosure_date
- MINIMUM_CONGRESS_TRADE_AMOUNT in constants.py only
- Method versions bumped
- All tests pass

## Risks

- **5a:** Insider sell weight change affects all insider signals. Expected and desired — validation says sells are noise.
- **5b:** Senate disclosure_date extraction — need to verify the field exists and is correctly typed. House has `notification_date`; Senate may use filing date.

## Files to Modify

| File | Change |
|------|--------|
| `src/signals/insider/engine.py` | DIRECTION_WEIGHT_SELL → -0.15 |
| `src/signals/congress/direct_service.py` | Pass disclosure_date, import constant from constants.py |
| `src/signals/congress/senate_direct.py` | Pass disclosure_date, import constant from constants.py |
| `src/signals/congress/constants.py` | Add MINIMUM_CONGRESS_TRADE_AMOUNT |
| `src/signals/core/versioning.py` | Bump versions |
| `tests/test_engine_parity.py` | Update sell-related fixtures |
