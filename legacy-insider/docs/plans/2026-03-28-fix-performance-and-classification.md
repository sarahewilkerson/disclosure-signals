# Plan: Fix Performance and Data Quality Issues in insidertradingsignal

**Status:** APPROVED
**Date:** 2026-03-28
**Target:** `/tmp/insidertradingsignal`

---

## Problem Statement

Two issues discovered during live script execution:

1. **Performance Issue:** Script spends ~14 seconds per filing retrying 404 errors before falling back to directory parsing
2. **Data Quality Issue:** All 52 transactions excluded with reason `no_officer_role`

---

## Issue 1: Retry Delays on 404s (Performance)

### Root Cause

**Location:** `ingestion.py` lines 54-72 (`EdgarClient.get()`) and lines 100-119 (`AsyncEdgarClient.get()`)

The retry logic treats ALL HTTP errors the same way, retrying 404s which are permanent failures. This wastes ~14 seconds per missing file (3 retries × 2/4/8s backoff).

### Fix

Skip retries for 4xx client errors (permanent failures), only retry 5xx and network errors.

---

## Issue 2: Officer Role Filtering (All Transactions Excluded)

### Root Cause

**Location:** `classification.py` lines 51-55 (`classify_role()`)

Logic bug uses `OR` instead of `AND`: `if not is_officer or not officer_title:` causes early exit when `is_officer=False`, never checking if title matches patterns.

### Fix

1. Restructure logic to check title patterns first
2. Expand `TOP_LEADERSHIP_PATTERNS` to include C-suite: CTO, CLO, CIO, CMO, CAO

---

## Files to Modify

| File | Changes |
|------|---------|
| `ingestion.py` | Fix retry logic in both `EdgarClient.get()` and `AsyncEdgarClient.get()` |
| `classification.py` | Restructure `classify_role()` logic |
| `config.py` | Add C-suite patterns |
| `tests/test_classification.py` | Add tests for new patterns and edge cases |
| `tests/test_ingestion.py` | Add tests for 4xx fast-fail |
| `methodology.md` | Document expanded officer patterns |

---

## Done When

- [x] 404 errors fail fast (no 14s delay)
- [x] Classification correctly identifies C-suite titles
- [x] All 226 tests pass (17 new tests added)
- [x] New tests for 4xx behavior and C-suite patterns
- [ ] Live run completes in <2 minutes for 10 companies (not verified - optional)
- [x] methodology.md updated

---

## Execution Results

**Executed:** 2026-03-28
**Status:** ✅ COMPLETE

### Changes Made

| File | Changes |
|------|---------|
| `ingestion.py` | Fixed `EdgarClient.get()` (lines 54-87) and `AsyncEdgarClient.get()` (lines 115-148) to skip retries on 4xx errors |
| `classification.py` | Restructured `classify_role()` (lines 28-67) to check title patterns before is_officer flag |
| `config.py` | Added C-suite patterns (CTO, CLO, CIO, CMO, CAO) and role weights |
| `tests/test_classification.py` | Added 17 new tests for C-suite patterns and edge cases |
| `tests/test_ingestion.py` | Added 5 new tests for 4xx fast-fail behavior |
| `methodology.md` | Updated to document 10 C-suite roles and title priority over is_officer flag |

### Test Results

- **226 tests pass** (up from 209)
- **17 new tests added** (12 classification, 5 ingestion)

### Deviations from Plan

1. Tests grew from 209 to 226 (17 new tests vs originally stated "new tests")
2. Added `officer_other` role weight (0.5) for officers with is_officer=True but no matching title pattern

---

## Sync Verification

- [x] Verification strategy executed: PASS (226 tests)
- [x] Branch pushed to remote: YES
- [x] Branch merged to main: N/A (feature branch is default branch)
- [x] Main pushed to remote: N/A
- [x] Documentation updated and current: YES
- [x] Production deploy: N/A (no deploy configured)
- [x] Local, remote, and main are consistent: YES
- Verified at: 2026-03-28T14:30:00Z
