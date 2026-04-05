# Plan: Fresh Pipeline + Missing Tests + Web Dashboard

**Date:** 2026-04-05
**Status:** Approved
**Scope:** Phase 1 (fresh ingest), Phase 2 (F3/F4 tests), Phase 3 (FastAPI dashboard)

## Completion Criteria
- Phase 1: Email received with fresh brief from live SEC data
- Phase 2: 2 new tests passing for earnings proximity + committee rotation
- Phase 3: `signals serve --port 8001` serves brief at localhost, TestClient test passes

## Risks
- SEC rate limiting on first live ingest (~30-60 min)
- uvicorn dependency (new optional dep)
- Concurrent DB access during dashboard reads (WAL mode handles)

## Files
- `tests/test_engine_parity.py` — F3/F4 tests
- `src/signals/web/app.py` — FastAPI dashboard (new)
- `src/signals/cli.py` — serve command
- `pyproject.toml` — uvicorn dep
- `tests/test_web.py` — TestClient tests (new)

## Deferred
Russell 2000 expansion — separate plan (5.5-hour ingest, new data source)
