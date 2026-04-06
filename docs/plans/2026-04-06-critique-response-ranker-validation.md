# Plan: Critique Response — Additive Ranker + Sector Validation

**Date:** 2026-04-06
**Status:** Approved
**Scope:** Additive ranking system with explainability + sector-relative validation

## Task

Address external critique: replace multiplicative scoring complexity with transparent ranking, add sector-relative validation to determine if signals are alpha or beta.

## Completion Criteria

- `rank_transaction()` returns 0-9 rank with factor breakdown
- Daily brief shows rank + explainability cards for each signal
- `run_sector_relative_validation()` produces sector-adjusted accuracy
- SYSTEM_DOCUMENTATION.md updated with ranking description
- All tests pass

## Commits

1. Ranker + explainability cards (engine.py, daily_brief.py, tests)
2. Sector-relative validation (validation.py, cli.py, tests)

## Risks

- Ranker may lose information vs multiplicative scorer — keep both, compare
- Sector ETF data may be unavailable — graceful skip with count
