# Plan: Thread Senate Filing Date Through Scoring

**Date:** 2026-04-04
**Status:** Planned
**Scope:** Write filing_date sidecar during ingestion, read during scoring

---

## Context

Senate `disclosure_date` in scoring currently uses `reference_date` as a fallback because `filing_date` (extracted during ingestion from the search results page) is not available in the scoring loop. The scoring loop processes cached `ptr_*.html` files without any metadata. This means all senate trades get an approximate lag penalty instead of an accurate one based on actual filing date.

## Changes

### 10a. Write metadata sidecar during ingestion

In `ingest_senate_ptrs_direct()`, after downloading PTR HTML files, write a JSON sidecar `ptr_{id}_meta.json` alongside each `ptr_{id}.html` containing:
```json
{"filing_id": "...", "filer_name": "...", "filing_date": "2026-03-15", "is_paper": false}
```

File: `src/signals/congress/senate_direct.py` — modify `ingest_senate_ptrs_direct()`

### 10b. Read sidecar during scoring

In `run_direct_senate_html_into_derived()`, for each `ptr_*.html`, check for a corresponding `ptr_*_meta.json`. If found, extract `filing_date` and pass it as `disclosure_date` to `score_transaction()`.

File: `src/signals/congress/senate_direct.py` — modify scoring loop

### 10c. Bump congress version

File: `src/signals/core/versioning.py`

---

## Completion Criteria
- Sidecar JSON written during ingestion
- Filing date read and passed to score_transaction during scoring
- Test verifying sidecar write/read round-trip
- Congress method version bumped

---

## Execution Results

**Executed:** 2026-04-04
**Branch:** `feat/senate-filing-date-sidecar`

### Results
- **10a:** `_write_filing_metadata()` writes JSON sidecar during ingestion. Uses `getattr` for defensive handling of incomplete mock objects.
- **10b:** `_read_filing_metadata()` reads sidecar during scoring. `filing_date` passed as `disclosure_date` to `score_transaction`, falling back to `reference_date` when no sidecar exists.
- **10c:** CONGRESS_SCORE_METHOD_VERSION bumped to quality4.
- **Test:** `test_senate_filing_metadata_sidecar` verifies round-trip write/read + missing sidecar returns None.
- **Issue found:** Existing test mock `Filing` class lacked `filer_name`/`filing_date` attrs — made `_write_filing_metadata` defensive with `getattr`.
- 84/85 tests pass (1 pre-existing).

## Sync Verification
- [x] Verification strategy executed: PASS
- [x] Branch pushed to remote: YES
- [x] Branch merged to main: YES
- [x] Main pushed to remote: YES
- [x] Documentation updated and current: YES
- [x] Production deploy: SKIPPED
- [x] Local, remote, and main are consistent: YES
- Verified at: 2026-04-04
