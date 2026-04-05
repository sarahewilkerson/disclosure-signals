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
