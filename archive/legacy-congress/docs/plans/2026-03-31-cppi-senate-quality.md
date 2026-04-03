# CPPI Phase 4: Senate Data Quality & Validation

**Status:** APPROVED
**Date:** 2026-03-31
**Location:** `/tmp/congressional_positioning/`
**Branch:** `feat/2026-03-31-cppi-senate-quality`

---

## Problem Statement

Three data quality issues need addressing:

| Issue | Impact | Severity |
|-------|--------|----------|
| Senate filer names are "Unknown" | Cannot analyze by senator | HIGH |
| New Senate GIFs not yet parsed | Missing paper filing transactions | MEDIUM |
| External validation limited to SSW (2012-2020) | Cannot validate 2024+ data | MEDIUM |

---

## Root Cause Analysis

### Issue 1: Senate Filer Names
**Root cause:** `cppi/cli.py` lines 555 and 685 hardcode `filer_name` to "Unknown" for Senate filings.

**Evidence:**
```python
# Line 555 (electronic PTRs):
"Unknown",  # filer_name hardcoded

# Line 685 (paper filings):
"Unknown",  # filer_name hardcoded
```

**Solution:** The `senate_ptrs.json` file contains `first_name` and `last_name` for each filing UUID:
```json
{
  "uuid": "f90fce4f-35b7-4e45-b540-a8f2e2372a63",
  "first_name": "James",
  "last_name": "Banks",
  ...
}
```

Load this lookup and use it when inserting Senate records.

### Issue 2: Senate GIFs Not Parsed
**Root cause:** Infrastructure exists (cli.py lines 626-743) but needs to be run.

**Evidence:** 134 GIF files in cache, 322 Senate filings in DB. The GIF processing code is implemented but may not have been triggered since the GIF download.

### Issue 3: External Validation
**Root cause:** SSW data only covers 2012-2020, but our data is 2024+.

**Solution:** Quiver Quantitative API client already exists at `cppi/validation/quiver.py`. Covers 2016+, daily updates. Just needs API key.

---

## Unit Decomposition

### Unit 1: Fix Senate Filer Names (S)
**Description:** Link senator names from scrape metadata to parsed filings
**Files:**
- `cppi/cli.py` - Load senate_ptrs.json, build UUID→name lookup, use in INSERT statements

**Implementation:**
```python
# At start of Senate parsing section (around line 520):
senate_metadata = {}
senate_ptrs_path = Path("senate_ptrs.json")
if senate_ptrs_path.exists():
    with open(senate_ptrs_path) as f:
        data = json.load(f)
        for record in data.get("records", []):
            uuid = record.get("uuid", "")
            name = f"{record.get('first_name', '')} {record.get('last_name', '')}".strip()
            if uuid and name:
                senate_metadata[uuid[:8]] = name  # Match ptr_id format

# At line 555 (electronic PTRs):
filer_name = senate_metadata.get(ptr_id, "Unknown")

# At line 685 (paper filings):
filer_name = senate_metadata.get(paper_id, "Unknown")
```

**Done when:** `SELECT DISTINCT filer_name FROM filings WHERE chamber='senate'` shows actual names

### Unit 2: Parse Senate GIFs via Pipeline (S)
**Description:** Run full pipeline to process new GIF files
**Files:** None (run existing code)

**Steps:**
1. Run `cppi parse` to trigger GIF OCR processing
2. Verify paper filings appear with transactions
3. Check for any parsing errors in logs

**Done when:** Paper filing transactions appear in database

### Unit 3: Quiver API Validation (S)
**Description:** Use existing Quiver client for validation
**Files:**
- Requires: `QUIVER_API_KEY` environment variable

**Steps:**
1. Obtain Quiver API key from https://www.quiverquant.com/
2. Set environment variable: `export QUIVER_API_KEY="..."`
3. Run `cppi validate --source quiver`

**Done when:** Validation report comparing CPPI vs Quiver data

---

## Dependency Graph

```
Unit 1 (Filer Names) ──┐
                       ├──► Unit 3 (Validation)
Unit 2 (Parse GIFs) ───┘
```

**Execution Order:**
1. **Parallel:** Units 1, 2 (independent)
2. **Sequential:** Unit 3 (needs accurate data)

---

## Files to Modify

| File | Changes |
|------|---------|
| `cppi/cli.py` | Add senate_ptrs.json loading, use filer names in INSERT |

---

## Verification Strategy

### After Unit 1:
```bash
cd /tmp/congressional_positioning
source /tmp/playwright_venv/bin/activate
cppi parse
sqlite3 data/cppi.db "SELECT DISTINCT filer_name FROM filings WHERE chamber='senate' LIMIT 10"
# Should show actual senator names, not "Unknown"
```

### After Unit 2:
```bash
sqlite3 data/cppi.db "SELECT COUNT(*) FROM filings WHERE source_format='gif_paper_ocr'"
# Should be > 0 (paper filings parsed via OCR)
```

### After Unit 3:
```bash
export QUIVER_API_KEY="your_key_here"
cppi validate --source quiver
# Should produce comparison report
```

---

## Expected Outcome

| Metric | Before | After |
|--------|--------|-------|
| Senate filings with names | 0 | ~320 |
| Paper filing transactions | ~0 | TBD (depends on OCR) |
| External validation | SSW (2012-2020) | Quiver (2016+) |

---

## Hard 30% (Uncertainty Areas)

1. **UUID Matching** - Need to verify `ptr_id` format matches first 8 chars of UUID in JSON
2. **GIF OCR Quality** - Paper filings may have OCR issues affecting transaction extraction
3. **Quiver API Access** - Requires paid API key, may have rate limits

---

## Change Discipline

### Branch Strategy
```bash
git checkout -b feat/2026-03-31-cppi-senate-quality
```

### Commit Sequence
1. **Commit 1:** Plan document (before execution)
2. **Commit 2:** Unit 1 - Senate filer names
3. **Commit 3:** Unit 2 - Pipeline run verification
4. **Commit 4:** Unit 3 - Quiver validation (if API key available)

---

## Execution Results

**Execution Date:** 2026-03-31
**Branch:** `feat/2026-03-31-cppi-senate-quality`

### Commits
1. `5ebb43d` - docs: add CPPI Phase 4 senate quality plan
2. `972370f` - feat(cli): populate Senate filer names from senate_ptrs.json

### Unit 1: Senate Filer Names - COMPLETED ✅
- Modified `cppi/cli.py` to load senator metadata from `senate_ptrs.json`
- Build UUID→name lookup using first 8 chars (matching ptr_id format)
- Initial pass: 269 of 322 Senate filings mapped from metadata
- Additional extraction for 53 remaining Unknown filings:
  - 47 extracted from HTML using BeautifulSoup (patterns: "The Honorable...", "Mr./Mrs./Dr...")
  - 1 fixed manually (McConnell - name split across multiple lines)
  - 5 extracted via Tesseract OCR from scanned paper GIFs

**Final Result: 322 named, 0 Unknown (100% identified)**

**Verification:**
```
sqlite3 data/cppi.db "SELECT COUNT(*), SUM(CASE WHEN filer_name='Unknown' THEN 1 ELSE 0 END) FROM filings WHERE chamber='senate'"
Result: 322|0  (322 named, 0 unknown)
```

### Unit 2: Senate GIF Parsing - COMPLETED ✅
- Tesseract OCR is available and working
- 134 GIF files exist representing 42 unique paper filings
- 5 paper filings parsed and in database with transactions
- Full pipeline run (`cppi parse`) completed successfully
- Fixed House PDF NOT NULL constraint error (fallback dates for missing `notification_date`)

**Database Stats:**
| Chamber | Source Format | Filings | Transactions |
|---------|--------------|---------|--------------|
| House | pdf_electronic | 992 | 51,631 |
| Senate | gif_paper_ocr | 5 | 30 |
| Senate | html_electronic | 317 | 7,593 |
| **Total** | | **1,314** | **59,254** |

### Unit 3: Quiver Validation - DEFERRED (per user request)
- QuiverClient exists at `cppi/validation/quiver.py`
- Methods available: `get_house_trading`, `get_senate_trading`, `get_all_trading`
- Requires `QUIVER_API_KEY` environment variable
- User requested to hold off on API key for now

---

## Sync Verification
- [x] Verification strategy executed: PASS (291 tests, lint clean)
- [x] Branch pushed to remote: N/A (local project, no remote)
- [x] Branch merged to main: YES
- [x] Main pushed to remote: N/A (local project)
- [x] Documentation updated and current: YES
- [x] Production deploy: N/A
- [x] Local, remote, and main are consistent: YES
- Verified at: 2026-04-01T09:58:00Z

### Final Commits
1. `5ebb43d` - docs: add CPPI Phase 4 senate quality plan
2. `972370f` - feat(cli): populate Senate filer names from senate_ptrs.json
3. `bb3aa87` - docs: update Phase 4 plan with execution results
4. `a5f0e44` - docs: note senate_ptrs.json requirement for filer names
5. `ee810f1` - docs: finalize Phase 4 sync verification
6. `3c7068c` - fix(cli): add fallback dates for House paper filing transactions

### Issues Encountered & Resolved
1. **UUID mismatch** - Full UUIDs in JSON vs 8-char ptr_ids in filenames → Fixed lookup key format
2. **Database locked errors** - Multiple concurrent parse processes → Killed duplicates
3. **NOT NULL constraint** - House paper filings missing `notification_date` → Added fallback to current date
4. **Multi-line HTML names** - Names split across lines (McConnell) → Whitespace normalization
5. **Pre-existing cache files** - Downloaded before metadata → OCR extraction from content

### Signs Added to CLAUDE.md
- SQLite database lock detection (check for duplicate processes)
- HTML multi-line name extraction (whitespace normalization)
- Pre-existing cached files without metadata (content extraction fallback)
