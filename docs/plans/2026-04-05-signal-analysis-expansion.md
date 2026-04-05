# Plan: Signal Analysis Expansion — 4 Features

**Date:** 2026-04-05
**Status:** Approved
**Scope:** F1 (Participation Index) + F2 (Cluster Conviction) + F3 (Earnings Proximity) + F4 (Committee Rotation)

## Task Description

Four independent signal analysis features expanding the system from stock-level signals to market-level intelligence. All additive — no existing behavior changes except F2 (conviction multiplier modifies scoring).

## Completion Criteria

- F1: `_compute_participation_index()` returns rate + context in brief
- F2: Multi-buyer cluster produces amplified score within [-1,1]
- F3: Pre-earnings insider buys flagged in brief
- F4: Committee directional shifts detected in brief
- All tests pass, `signals brief` shows new sections

## Risks

- F1: 22/504 tickers (4%) is noisy at monthly granularity — use 90-day window
- F2: tanh boundary math — clamp to ±0.999 before atanh
- F3: yfinance earnings data incomplete — skip gracefully per-ticker
- F4: JSON provenance_payload parsing — Python-side, not SQL

## Blast Radius

- F2 modifies `aggregate_company_signal()` — requires version bump
- F1/F3/F4 are additive (new functions + brief sections only)

## Files

- `src/signals/analysis/daily_brief.py` — F1, F3, F4
- `src/signals/insider/engine.py` — F2
- `src/signals/core/versioning.py` — F2
- `tests/test_engine_parity.py` — all features
