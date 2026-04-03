# Plan: Fix All Outstanding Issues in insidertradingsignal

**Status: ✅ APPROVED (Decomposition Plan)**
**Reviewed:** 2026-03-28
**Execution:** Each unit requires lightweight plan → execute → verify cycle

## Task Description

Address all issues identified in the code critique:
- 2 must-have improvements
- 4 should-have improvements
- 3 nice-to-have improvements
- 4 removals
- 7 tech debt items (2 high, 3 medium, 2 low)

**Target:** `/tmp/insidertradingsignal` (local clone)

---

## Decomposition

This task is too large for a single cycle. Decomposed into 10 units with dependencies mapped.

```
Unit 1 (Foundation) ─┬─► Unit 4 (Multi-owner)
                     ├─► Unit 5 (Batching)
Unit 2 (Dead code)   │
                     ├─► Unit 6 (Incremental scoring)
Unit 3 (Types)  ─────┤
                     ├─► Unit 7 (Migrations)
                     │
                     └─► Unit 8 (Scheduling) ─► Unit 9 (Backfill)

Unit 10 (README) — parallel with all

Units 11-13 (Nice-to-have) — after core complete
```

---

### Unit 1: Config Hardening (S) — Foundation

**Description:** Validate User-Agent on startup, make SEC URLs configurable, add env var support.

**Files to modify:**
- `config.py` — add validation, env var loading
- `cli.py` — fail fast on invalid config

**Done when:**
- [ ] `SEC_USER_AGENT` validated on import; raises `ValueError` if contains "example.com"
- [ ] SEC URLs configurable via env vars with current values as defaults
- [ ] Tests pass

---

### Unit 2: Dead Code Removal (S) — Foundation, parallel with Unit 1

**Description:** Remove unused code, relocate test data.

**Changes:**
- Delete `_parse_holding()` in `parsing.py:186-192`
- Delete `get_all_signal_transactions()` in `db.py:255-264`
- Move `sample_fortune500.csv` to `tests/fixtures/`
- Update any imports/references

**Done when:**
- [ ] No dead code remains
- [ ] `sample_fortune500.csv` in `tests/fixtures/`
- [ ] All tests pass

---

### Unit 3: Type Definitions (S) — Foundation, parallel with Units 1-2

**Description:** Add TypedDict definitions for DB row structures.

**Files to modify:**
- Create `types.py` with TypedDict for: `FilingRow`, `TransactionRow`, `CompanyRow`, `ScoreRow`
- Update `db.py` return type hints

**Done when:**
- [ ] `types.py` exists with 4+ TypedDict definitions
- [ ] `db.py` functions have typed return values
- [ ] No new mypy errors

---

### Unit 4: Multi-Owner Handling (M) — Depends on Unit 3

**Description:** Parse all reporting owners in a filing, not just the first.

**Files to modify:**
- `parsing.py` — iterate over all `reportingOwner` elements
- `db.py` — handle multiple owners per filing (may need schema thought)

**Approach decision:**
- Option A: One filing row per owner (simpler, denormalized)
- Option B: Separate `filing_owners` join table (normalized)

**Recommend Option A** — maintains current schema, just duplicates filing rows per owner.

**Done when:**
- [ ] Filings with multiple owners create multiple rows
- [ ] Test with `form4_*.xml` fixtures
- [ ] Existing tests pass

---

### Unit 5: Transaction Batching (M) — Depends on Unit 3

**Description:** Batch `INSERT` statements instead of one-at-a-time.

**Files to modify:**
- `db.py` — add `insert_transactions_batch(conn, txns: list)`
- `parsing.py` — use batch insert

**Done when:**
- [ ] `insert_transactions_batch()` exists
- [ ] `parse_and_store_filing()` uses batch insert
- [ ] Performance improvement measurable (optional: benchmark)

---

### Unit 6: Incremental Scoring (M) — Depends on Units 3, 5

**Description:** Only rescore companies with new filings since last run.

**Files to modify:**
- `db.py` — add `get_companies_with_new_filings(since_date)`
- `scoring.py` — `score_all_companies()` accepts optional company list
- Remove `clear_scores()` call; use UPSERT instead

**Done when:**
- [ ] Re-running `score` only processes changed companies
- [ ] `company_scores` uses UPSERT (ON CONFLICT UPDATE)
- [ ] Tests pass

---

### Unit 7: Database Migrations (M) — Depends on Unit 3

**Description:** Add migration support for schema changes.

**Approach:** Simple numbered migration files + version table.

**Files to create:**
- `migrations/` directory
- `migrations/001_initial.sql` (current schema)
- `migrations/002_add_version_table.sql`
- `db.py` — `migrate()` function that runs pending migrations

**Done when:**
- [ ] `migrations/` directory exists with initial migration
- [ ] `schema_version` table tracks applied migrations
- [ ] `init_db()` calls `migrate()` automatically

---

### Unit 8: Graceful Shutdown + Scheduling (M) — Depends on Unit 1

**Description:** Handle SIGINT/SIGTERM gracefully; add scheduling config.

**Files to modify:**
- `cli.py` — signal handlers for graceful shutdown
- Create `contrib/launchd/` with plist template
- Create `contrib/cron/` with example crontab

**Done when:**
- [ ] Ctrl+C during ingestion stops cleanly (no corrupt state)
- [ ] `contrib/launchd/com.insider-signal.plist` exists
- [ ] `contrib/cron/example-crontab` exists

---

### Unit 9: Historical Backfill (M) — Depends on Unit 8

**Description:** Add CLI option to backfill historical filings.

**Files to modify:**
- `cli.py` — add `--start-date` and `--end-date` to `ingest` command
- `ingestion.py` — pass date range to `search_form4_filings()`

**Done when:**
- [ ] `python cli.py ingest --csv x.csv --start-date 2020-01-01 --end-date 2023-12-31` works
- [ ] Historical filings ingested correctly

---

### Unit 10: README.md (M) — Parallel with all units

**Description:** Create comprehensive README with setup, usage, examples.

**Content:**
- Project description
- Installation/setup
- Configuration (env vars, User-Agent)
- Usage examples
- Architecture overview
- Contributing guide

**Done when:**
- [ ] `README.md` exists with all sections
- [ ] Includes example commands that work
- [ ] Links to `methodology.md`

---

### Unit 11: Async HTTP with httpx (M) — Nice-to-have, after Units 1-9

**Description:** Replace `requests` with `httpx` for async support.

**Files to modify:**
- `requirements.txt` — add httpx
- `ingestion.py` — convert `EdgarClient` to async

**Done when:**
- [ ] `httpx` in requirements
- [ ] Ingestion uses async HTTP
- [ ] Rate limiting still works

---

### Unit 12: REST API (L) — Nice-to-have, after Units 1-9

**Description:** Add FastAPI-based REST API for querying signals.

**Decompose further if approved:**
- 12a: Basic FastAPI setup + health endpoint (S)
- 12b: Company scores endpoint (S)
- 12c: Aggregate index endpoint (S)
- 12d: Swagger docs (S)

**Files to create:**
- `api.py` — FastAPI app
- `requirements.txt` — add fastapi, uvicorn

---

### Unit 13: Test Coverage (M) — Parallel, after Unit 2

**Description:** Add unit tests for `ingestion.py` and `universe.py`.

**Files to create:**
- `tests/test_ingestion.py`
- `tests/test_universe.py`

**Done when:**
- [ ] Tests exist for `EdgarClient`, `search_form4_filings`, `ingest_company`
- [ ] Tests exist for `load_universe_csv`, `resolve_cik`
- [ ] All tests pass with mocked HTTP

---

## Execution Order

| Phase | Units | Parallelizable |
|-------|-------|----------------|
| 1 | Units 1, 2, 3 | Yes (all 3 parallel) |
| 2 | Units 4, 5, 10, 13 | Yes (all 4 parallel) |
| 3 | Units 6, 7, 8 | Partial (6 depends on 5) |
| 4 | Unit 9 | Sequential |
| 5 | Units 11, 12 | Yes (both parallel) |

**Estimated total effort:** 12-16 hours across all units

---

## Verification Strategy

Per-unit:
- Run `python -m pytest tests/` after each unit
- Run `python cli.py status` to verify DB integrity
- For ingestion changes: test with sample CSV

End-to-end after all units:
```bash
python cli.py run --csv tests/fixtures/sample_fortune500.csv --max-filings 5
python cli.py status
python cli.py report
```

---

## Commit Discipline

- Each unit = one atomic commit (or PR)
- Commit message format: `fix(unit-N): [description]`
- All tests must pass before committing

---

## Risks (Hard 30%)

1. **Multi-owner handling (Unit 4):** May surface edge cases in scoring if same transaction appears under multiple owners. Add deduplication logic to prevent double-counting transactions.

2. **Incremental scoring (Unit 6):** Must handle deleted/amended filings correctly. Score could drift if amendments aren't tracked.

3. **Async HTTP (Unit 11):** Rate limiting logic must be rethought for concurrent requests. Could violate SEC 10 req/sec limit.

4. **REST API (Unit 12):** Security considerations (auth, rate limiting) not in original scope. May need to add.

---

## Skipped (Out of Scope)

- **Price data integration** — Requires external data source (Yahoo Finance, etc.). Architectural decision needed.
- **Alerting** — Requires notification service (email, Slack). Infrastructure decision needed.
- **Consistent date handling** — Low priority, would touch many files for minimal benefit.

These can be separate follow-up tasks if desired.

---

## Files Modified Summary

| File | Units |
|------|-------|
| `config.py` | 1 |
| `cli.py` | 1, 8, 9 |
| `db.py` | 2, 3, 5, 6, 7 |
| `parsing.py` | 2, 4, 5 |
| `scoring.py` | 6 |
| `ingestion.py` | 9, 11 |
| `types.py` (new) | 3 |
| `api.py` (new) | 12 |
| `README.md` (new) | 10 |
| `tests/test_ingestion.py` (new) | 13 |
| `tests/test_universe.py` (new) | 13 |
| `migrations/*.sql` (new) | 7 |
| `contrib/launchd/*` (new) | 8 |
| `contrib/cron/*` (new) | 8 |
| `tests/test_api.py` (new) | 12 |

---

## Execution Results

**Executed:** 2026-03-28
**Status:** ✅ COMPLETE

### All Units Completed:

| Unit | Status | Notes |
|------|--------|-------|
| 1 | ✅ | SEC_USER_AGENT validation with env var support |
| 2 | ✅ | Moved sample_fortune500.csv to tests/fixtures/ |
| 3 | ✅ | Created types.py with TypedDict definitions |
| 4 | ✅ | Multi-owner handling via additional_owners JSON field |
| 5 | ✅ | insert_transactions_batch() with executemany |
| 6 | ✅ | Incremental scoring with UPSERT support |
| 7 | ✅ | Database migrations with schema_version table |
| 8 | ✅ | Signal handlers + launchd/cron templates |
| 9 | ✅ | --start-date and --end-date CLI options |
| 10 | ✅ | Comprehensive README.md |
| 11 | ✅ | AsyncEdgarClient with httpx |
| 12 | ✅ | FastAPI REST API with tests |
| 13 | ✅ | test_ingestion.py, test_universe.py, test_api.py |

### Test Results:

- **209 tests pass** (0 failures, 0 warnings)

### Deviations from Plan:

1. **Unit 4**: Used JSON field approach instead of duplicating filing rows per owner (cleaner, maintains schema compatibility)
2. **Unit 7**: Version table created inline in migrate() instead of separate migration file (simpler)
3. **Unit 12**: Added more endpoints (/sectors, /companies) and test coverage (tests/test_api.py)

### Issues Encountered:

1. `sqlite3.ProgrammingError: missing :additional_owners binding` - Fixed by adding field to all filing dicts
2. `sqlite3.IntegrityError: FOREIGN KEY constraint failed` in tests - Fixed by adding company record in test setUp
3. FastAPI deprecation warning - Changed from on_event to lifespan context manager

### Commit:

```
6bd43e9 feat: implement all 13 units from fix-all-issues plan
277965b fix: correct SEC EDGAR URL construction for filing downloads
```

---

## Runtime Bug Fixes (Post-Execution)

After implementing all 13 units, live script execution revealed four SEC EDGAR API issues:

1. **SEC company_tickers.json URL** - Was 404'ing at `data.sec.gov/submissions/company_tickers.json`. Fixed to use `www.sec.gov/files/company_tickers.json`.

2. **EFTS search CIK format** - Code stripped leading zeros from CIK, but EFTS full-text search requires zero-padded 10-digit format. Searching "320193" returned 0 hits, but "0000320193" returned 46 filings.

3. **Filing archive URL construction** - SEC archives are organized by issuer CIK (company), not filer CIK (person). The `resolve_filing_xml_url()` function was parsing filer CIK from accession number and using it in URL path. Fixed to accept issuer_cik parameter.

4. **XML filename resolution** - Fallback filenames (`primary_doc.xml`, `doc4.xml`, `ownership.xml`) don't match actual Form 4 XML names which are unpredictable (e.g., `rrd384222.xml`, `wk-form4_1773786674.xml`). Fixed by parsing the directory listing HTML to find actual XML files.

After fixes, script successfully ingested filings:
- AAPL: 2 new filings
- MSFT: 3 new filings
- AMZN: 2 new filings

---

## Sync Verification

- [x] Verification strategy executed: PASS (209 tests)
- [x] Branch pushed to remote: PENDING
- [ ] Branch merged to main: PENDING
- [ ] Main pushed to remote: PENDING
- [x] Documentation updated and current: YES
- [ ] Production deploy: N/A (no deploy configured)
- [ ] Local, remote, and main are consistent: PENDING
- Verified at: 2026-03-28T13:15:00
