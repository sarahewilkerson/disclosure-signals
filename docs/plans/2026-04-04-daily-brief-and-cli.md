# Plan: High-Signal Daily Brief + CLI Validation Command

**Date:** 2026-04-04
**Status:** Planned
**Scope:** Daily brief report generator + CLI `validate` command

---

## Context

The scoring pipeline is tuned and validated. The missing product output is: "What would a high-signal daily brief look like?" The user explicitly asked this. The answer, informed by validation findings:

**A high-signal daily brief should contain:**
1. **Clustered insider buying alerts** — 2+ unique C-suite insiders buying the same stock within 30 days (the highest-quality signal, 69.6% accuracy)
2. **High-confidence congress purchases** — concentrated single-stock buys (58-64% accuracy)
3. **Cross-source alignment** — entities where both insider and congress signals agree (strongest conviction)
4. **Anomaly flags** — entities with unusual transaction volume relative to their historical baseline

**A daily brief should NOT contain:**
- Sell-driven signals (validated as noise)
- Single-transaction signals (filtered by min threshold)
- Low-confidence or insufficient signals
- Broad ETF or tiny-trade noise (already filtered)

---

## Changes

### 6a. Daily brief report generator

New module: `src/signals/analysis/daily_brief.py`
- `build_daily_brief(db_path, reference_date)` → structured brief
- `render_daily_brief_markdown(brief)` → human-readable output
- Sections: cluster buy alerts, strong congress buys, cross-source signals, anomaly flags
- Filters: only bullish signals with confidence >= 0.4, only buy-direction transactions

### 6b. CLI `validate` command

Add `signals validate` subcommand to CLI:
- `signals validate --db <path> --source insider --min-date 2025-01-01 --max-date 2025-12-31`
- Calls `run_transaction_validation()` and renders markdown output
- Optional `--forward-days` flag (default: 5,10,20,60)

### 6c. CLI `brief` command

Add `signals brief` subcommand to CLI:
- `signals brief --db <path>` → daily brief output
- `--format json|text` flag
- `--date` flag for reference date (default: today)

---

## Completion Criteria

- Daily brief module produces structured output from current DB
- `signals validate` CLI command works end-to-end
- `signals brief` CLI command works end-to-end
- Tests for daily brief logic

## Files to Modify/Create

| File | Change |
|------|--------|
| `src/signals/analysis/daily_brief.py` | New: daily brief generator |
| `src/signals/cli.py` | Add `validate` and `brief` subcommands |
| `tests/test_engine_parity.py` | Test for brief filtering logic |
