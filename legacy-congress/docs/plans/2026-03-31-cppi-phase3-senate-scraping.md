# CPPI Phase 3: Senate Bulk Scraping via Playwright

**Status:** COMPLETE (with Hetzner sudo blocker)
**Date:** 2026-03-31
**Location:** `/tmp/congressional_positioning/`

---

## Problem Statement

The Senate efdsearch.senate.gov site requires JavaScript for search results pagination. The current SenateConnector's `search_ptrs()` method returns 0 results because:
1. Session establishment works (CSRF + agreement POST)
2. But search results are rendered via JavaScript (DataTables)
3. The raw HTML response contains no result rows

**Root cause:** Site uses DataTables JS library for rendering results.

---

## Execution Results

### Unit 1: Reconnaissance - COMPLETE

**Selectors documented:**
- Agreement checkbox: `input#agree_statement`
- Submit button: `button[type=submit]`
- PTR checkbox: `input[name='report_type'][value='11']`
- Date inputs: `input#fromDate`, `input#toDate`
- Results table: `#filedReports` (DataTables)
- Pagination: `.paginate_button.next` for next page
- Info: `.dataTables_info` for total count

**URL patterns:**
- Electronic PTRs: `/search/view/ptr/{uuid}/`
- Paper filings: `/search/view/paper/{uuid}/`

**Total 2024-present PTRs:** 341 (303 electronic, 38 paper)

### Unit 2: Playwright Setup - BLOCKED ON HETZNER

**Status:** Playwright Python package installed in `/opt/fedresearch/cppi-venv/`
**Blocker:** Chromium requires system dependencies (`libnss3`, `libnspr4`, etc.)
**Issue:** `sudo apt-get install` requires password, no passwordless sudo configured

**Workaround:** Ran scraper locally on macOS with Playwright working.

### Unit 3: Scraper Script - COMPLETE

**Created:** `/tmp/congressional_positioning/scripts/scrape_senate.py`

**Features:**
- Playwright-based session establishment
- PTR-specific search with date filtering
- Full pagination support (DataTables)
- UUID extraction with metadata
- JSON output with resume support
- Subcommands: `scrape` and `download`

### Unit 4: Download Integration - COMPLETE

**Features:**
- Session-based HTML download using requests
- Cache structure matches existing SenateConnector
- Progress logging with checkpoints
- 1-second delay between requests (rate limiting)

**Downloaded:** 315 PTR HTML files to cache

### Unit 5: Pipeline Integration - COMPLETE

**Results after full pipeline run:**

| Metric | Before | After |
|--------|--------|-------|
| Included transactions | 9,682 | 20,061 |
| Active members | 609 | 784 |
| Breadth signal | -1% | -8% |
| Net volume | +$351K | -$177K |
| Confidence | 0.80 | 0.79 |

---

## Files Created

- `/tmp/congressional_positioning/scripts/scrape_senate.py` - Main scraper
- `/tmp/congressional_positioning/docs/plans/2026-03-31-cppi-phase3-senate-scraping.md` - This plan

---

## Remaining Work

1. **Hetzner Playwright deps:** Need sudo access to install:
   ```bash
   sudo apt-get install libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
     libcups2 libdrm2 libdbus-1-3 libxkbcommon0 libatspi2.0-0 \
     libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2
   ```

2. **CLI integration:** Add `--bulk` flag to `cppi ingest --senate-only` to call scraper

---

## Lessons Learned

- Senate site uses DataTables for rendering - requires JS execution
- Playwright on headless servers needs system dependencies (not just pip install)
- Session establishment works via requests but search results require browser
- Paper filings (38/341) need OCR - existing pipeline handles this

---

## Verification

```bash
# Run scraper
python scripts/scrape_senate.py scrape \
  --from-date 2024-01-01 \
  --to-date 2026-03-31 \
  --output senate_ptrs.json

# Download PTRs
python scripts/scrape_senate.py download \
  --input senate_ptrs.json \
  --cache-dir ./cache

# Parse and score
cppi parse
cppi score --window 1500
cppi report --window 1500 --stdout
```

All verification commands executed successfully on local machine.

---

## Sync Verification

- [x] Verification strategy executed: PASS (276 tests, lint clean)
- [x] Branch pushed to remote: N/A (local project, no remote)
- [x] Branch merged to main: N/A (working directly on main)
- [x] Main pushed to remote: N/A
- [x] Documentation updated and current: YES (README updated)
- [x] Production deploy: N/A (no deploy config)
- [x] Local, remote, and main are consistent: YES (local only)
- Verified at: 2026-03-31T18:35:00Z

## Execution Review Notes

**Process Violation:** Plan was written to `docs/plans/` during/after execution rather than before. Future work should commit plans BEFORE starting implementation.

**Commit:** `00612ee` - feat: add Senate PTR scraper (Phase 3)
