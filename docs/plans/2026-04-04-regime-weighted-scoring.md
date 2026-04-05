# Plan: Market-Regime Weighted Scoring

**Date:** 2026-04-04
**Status:** Approved
**Scope:** Feature B only — regime computation + engine integration + A/B validation

---

## Task Description

Integrate market regime awareness into the scoring engines. Compute SPY trailing return at pipeline startup, derive a regime weight (conservative: max +/-10%), pass through to both insider and congress `score_transaction()` as an optional parameter (default 1.0 preserves backward compatibility).

**Important caveat:** This is an experimental hypothesis. Validation showed bear accuracy 75% vs bull 76.5% (n=8 vs n=17) — statistically indistinguishable. Implementation must include an A/B comparison. If regime weighting doesn't improve accuracy, it ships disabled-by-default.

---

## Completion Criteria

- `compute_regime()` returns RegimeContext with regime classification and weight
- Both `score_transaction()` functions accept `regime_weight` parameter
- Pipeline computes regime at startup and passes to branches
- A/B comparison documents whether regime weighting improves accuracy
- `pytest tests/test_regime.py` passes with mocked yfinance
- Full test suite passes (no regressions)
- Method versions bumped

---

## Execution Sequence

### B1: Regime computation module (S)
- New file: `src/signals/core/regime.py`
- `RegimeContext` dataclass + `compute_regime()` function
- Bear (SPY < -2%): weight=1.1, Neutral: 1.0, Bull (SPY > +8%): 0.95
- Fallback: weight=1.0, regime="unknown" when yfinance fails

### B2: Thread through scoring engines (S)
- `insider/engine.py`: add `regime_weight=1.0` to `score_transaction()`, multiply into `transaction_signal`
- `congress/engine.py`: add `regime_weight=1.0` to `score_transaction()`, multiply into `final_score`
- `insider/direct_service.py`, `congress/direct_service.py`, `congress/senate_direct.py`: accept and pass through

### B3: Pipeline integration + A/B comparison + tests (M)
- `core/pipeline.py`: compute regime before ThreadPoolExecutor, pass to branches
- `tests/test_regime.py`: mock yfinance for bull/bear/neutral/failure, verify flow-through
- A/B comparison: run `run_transaction_validation()` with and without regime, document findings
- `core/versioning.py`: bump INSIDER and CONGRESS score versions

---

## Risks (Hard 30%)

1. **Regime effect may not be real.** Mitigation: conservative weights, A/B comparison, disabled-by-default if no improvement.
2. **yfinance data gaps.** Mitigation: return "unknown" with weight=1.0.
3. **Forward-looking bias.** Mitigation: lookback ends at reference_date - 1 day. Test assertion.

---

## Blast Radius

- All new parameters default to 1.0 — no behavioral change without explicit opt-in
- No DB schema changes
- Method version bump separates regime-aware runs in DB

---

## Verification Strategy

1. `pytest tests/test_regime.py` — mocked yfinance for all regime states
2. `pytest tests/ -k "not test_legacy_references"` — full regression suite
3. A/B comparison output documenting regime effect (or lack thereof)
4. `signals validate --regime` shows regime context

---

## Documentation to Update

- `src/signals/core/versioning.py` — bump versions
- `regime.py` module docstring — weight formula, experimental status
- No new env vars, DB columns, or dependencies
