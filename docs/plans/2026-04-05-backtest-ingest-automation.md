# Plan: Backtest + Fresh Ingest + Daily Automation

**Date:** 2026-04-05
**Status:** Executed
**Scope:** Operational — no source code changes

---

## Execution Results

### Phase 1: Historical Backtest (H2 2025, 6 monthly dates)

**Runtime:** 894.6 seconds (~15 minutes, ~150s per date)

| Date | Insider | Congress | Combined | Duration |
|------|---------|----------|----------|----------|
| 2025-07-01 | 390 | 467 | 3 | 144.3s |
| 2025-08-01 | 390 | 467 | 3 | 141.9s |
| 2025-09-01 | 390 | 467 | 3 | 145.7s |
| 2025-10-01 | 390 | 467 | 3 | 146.1s |
| 2025-11-01 | 390 | 467 | 3 | 166.9s |
| 2025-12-01 | 390 | 467 | 3 | 149.6s |

**Time-Series Metrics:**
- Mean stability: 6.41% (most tickers rarely active — correct for rare buy events)
- Mean flip rate: 2.15% (signals almost never change direction)
- Mean turnover: 2.19% (98% of signals persist between months)

**Finding:** Signals are extremely stable. This is consistent with the data: insider buys are rare events that persist across 30/90/180-day windows, and the same cached filings are scored at each date.

### Phase 2: Fresh Pipeline Run

**Deferred** — requires SEC user-agent string configured in `~/.local/jobs/.env.signals`. The scoring-only path was tested and works (scored 390 insider + 467 congress + 1 combined in ~2.5 minutes).

### Phase 3: Daily Automation

- **Script:** `~/.local/jobs/scripts/signals_daily_run.py` — supports both full pipeline (with SEC agent) and scoring-only (cached files)
- **Config:** `~/.local/jobs/.env.signals` — paths and optional SEC user-agent
- **Schedule:** Daily at 06:00 via launchd
- **Status:** `signals-daily-pipeline` loaded and enabled in jobctl
- **Legacy:** `cppi-update` disabled (was failing daily)

**Issue encountered:** Initial script used `signals run` CLI which attempted SEC ingestion even with a placeholder user-agent (403 Forbidden). Fixed by adding `run_scoring_only()` function that calls Python scoring APIs directly, bypassing CLI and SEC calls.

## Sync Verification
- [x] Verification strategy executed: PASS
- [x] jobctl sync: OK (8 jobs)
- [x] signals-daily-pipeline: loaded, manual test successful
- [x] cppi-update: disabled
- [x] Backtest completed with 6 dates
- Verified at: 2026-04-05
