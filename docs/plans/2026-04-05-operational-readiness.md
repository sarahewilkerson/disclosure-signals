# Plan: Operational Readiness — Resolution Fix + Sell Elimination + Full Run

**Date:** 2026-04-05
**Status:** Approved
**Scope:** Phases 1-3 (code fixes + first full pipeline run with overlay)

## Task Description

Fix congress entity resolution (42% exclusion rate from incomplete canonical CSV), eliminate sell signals (validated as noise), and run the complete pipeline end-to-end for the first time.

## Root Causes

1. `resolve_entity()` in `core/resolution.py:87-113` requires canonical_entities.csv match. CSV has 49 entries; META, UNH, JPM, TSLA etc. fail at confidence=0.0 despite having valid tickers.
2. `DIRECTION_WEIGHT_SELL = -0.15` in `insider/engine.py:35` allows sells to generate bearish signals. Validation showed 41-47% accuracy (noise).
3. Combined overlay never ran against current DB (restored from backup during A/B testing).

## Completion Criteria

- Congress included transactions increase from ~48 to ~1,800+
- 0 bearish insider signals (sells contribute zero)
- Combined overlay produces >0 results
- `signals brief --sectors --committees` generates complete output
- All tests pass

## Execution Sequence

### Phase 1: Ticker-passthrough resolution fallback (S)
- Add fallback in `resolve_entity()`: if ticker provided and no canonical match, trust with confidence=0.95
- Add `test_resolve_entity_ticker_passthrough` test
- Bump RESOLUTION_METHOD_VERSION

### Phase 2: Set sell weight to 0.0 (S)
- Change `DIRECTION_WEIGHT_SELL` from -0.15 to 0.0
- Bump INSIDER_SCORE_METHOD_VERSION

### Phase 3: Full pipeline run + overlay + brief (M, operational)
- Re-score DB with all fixes active
- Run combined overlay
- Generate daily brief with all sections

## Risks (Hard 30%)

1. Ticker passthrough trusts caller-provided tickers. Mitigated by upstream asset resolution validation.
2. Entity_key format (`entity:{ticker}` vs `entity:{name}`) differs from canonical entries. Verified: overlay uses subject_key (ticker-based), not entity_key.
3. Committee enrichment adds ~15-30s latency on first run (API fetch, then cached).

## Blast Radius

- Phase 1: 5,351 congress transactions change from excluded to included
- Phase 2: 21 insider bearish signals become neutral/insufficient
- Phase 3: Operational only — no code changes

## Verification

- Phase 1: Unit test + `SELECT COUNT(*) FROM normalized_transactions WHERE source='congress' AND include_in_signal=1`
- Phase 2: `SELECT COUNT(*) FROM signal_results WHERE source='insider' AND label='bearish'` = 0
- Phase 3: `SELECT COUNT(*) FROM combined_results` > 0
- Full suite: `pytest tests/ -k "not test_legacy_references"`

## Documentation

- `src/signals/core/versioning.py` — bump RESOLUTION + INSIDER versions
- `docs/reason-codes.md` — document ticker_passthrough match type
