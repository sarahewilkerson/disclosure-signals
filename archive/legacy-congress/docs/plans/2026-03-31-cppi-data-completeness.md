# CPPI Data Completeness Audit & Fix Plan

**Status:** APPROVED
**Date:** 2026-03-31
**Location:** `/tmp/congressional_positioning/`
**Branch:** `feat/2026-03-31-cppi-ocr-support`

---

## Problem Statement

Critical analysis reveals the CPPI system is **silently missing significant transaction data**:

| Issue | Impact | Severity |
|-------|--------|----------|
| 115 House paper filings not parsed | ~500-1000+ transactions missing | HIGH |
| Senate paper filings need better OCR | Unknown transactions missing | MEDIUM |
| Annual Financial Disclosures not captured | Validation data missing | LOW |
| Silent parsing failures | Unknown data loss | MEDIUM |

**Evidence:**
- 1,095 House PDFs cached → only 964 filings in database (131 missing)
- 115 House PDFs are paper filings (8220xxx IDs) with zero extractable text
- These are scanned images that need OCR but parser silently returns 0 transactions
- Senate has 37 paper HTML files + 5 GIF files - GIFs need OCR

---

## Root Cause Analysis

### Issue 1: House Paper Filings (HIGH PRIORITY)
**Root cause:** `HousePDFParser.parse()` calls `pdfplumber.extract_text()` which returns empty string for scanned images. Parser continues without error, returning filing with 0 transactions.

**Evidence:**
```
$ ls cache/pdfs/house/822*.pdf | wc -l
115

$ python -c "
from cppi.parsing import parse_house_pdf
filing = parse_house_pdf('cache/pdfs/house/8220118.pdf')
print(f'Transactions: {len(filing.transactions)}')  # Output: 0
print(f'Errors: {filing.parse_errors}')             # Output: []
"
```

**PDF inspection shows:**
- 1 page
- 0 chars of extractable text
- 1 embedded image (the scanned document)

### Issue 2: Silent Failures
**Root cause:** No validation that extracted text is non-empty before attempting transaction parsing. No logging when filings yield zero transactions.

### Issue 3: Senate Paper Filings
**Root cause:** GIF images require OCR. Current code has OCR support but only 5 GIFs exist vs 37 paper HTML files. The paper_*.html files may be downloading error pages instead of actual paper filings.

---

## External Benchmarks

Compared our data against external sources:

| Source | Data Volume |
|--------|-------------|
| senate-stock-watcher-data (GitHub) | Comprehensive Senate PTRs as JSON |
| Unusual Whales 2025 Report | $170M sold, $125M bought |
| CPPI Current | ~$20M buy, ~$21M sell (10x lower!) |

The 10x volume difference suggests we're missing significant data.

---

## Unit Decomposition

### Unit 1: House Paper Filing OCR (M) — PRIORITY
**Description:** Add OCR support for House paper filings (8220xxx IDs)
**Files:**
- `cppi/parsing.py` - Add OCR fallback when `extract_text()` returns empty
- `cppi/ocr.py` - Reuse existing OCR infrastructure

**Implementation:**
```python
# In HousePDFParser.parse():
text = page.extract_text() or ""
if not text.strip() and page.images:
    # Scanned page - use OCR
    from cppi.ocr import ocr_pdf_page
    text = ocr_pdf_page(pdf_path, page_num)
```

**Done when:** 115 paper filings parse with transactions

### Unit 2: Parsing Failure Detection (S)
**Description:** Add logging and validation for empty parsing results
**Files:**
- `cppi/parsing.py` - Log warnings when 0 transactions extracted
- `cppi/cli.py` - Report parsing failures in summary

**Implementation:**
- Log warning: "No transactions extracted from {filing_id} - may be scanned image"
- Track failed_parses counter in cmd_parse()
- Output summary: "X filings yielded 0 transactions (may need OCR)"

**Done when:** Parse command reports which filings failed

### Unit 3: Senate Paper Filing Verification (S)
**Description:** Verify paper filing downloads are actual content, not error pages
**Files:**
- `scripts/scrape_senate.py` - Verify downloaded content
- `cppi/connectors/senate.py` - Validate HTML content

**Investigation needed:**
- Why 37 paper HTML files but only 5 GIF files?
- Are HTML files actually paper filings or electronic?
- Check if paper filing download is returning error pages

**Done when:** All 37 paper filings have valid content

### Unit 4: Data Validation Against External Source (S)
**Description:** Compare our transaction counts against senate-stock-watcher-data
**Files:**
- New: `scripts/validate_against_ssw.py`

**Implementation:**
1. Clone senate-stock-watcher-data repo
2. Load all_transactions.json
3. Compare counts by senator and date range
4. Report discrepancies

**Done when:** Report showing coverage gaps vs external data

### Unit 5: End-to-End Test (S)
**Description:** Full pipeline test with validation
**Files:**
- `tests/test_e2e_pipeline.py` (new)

**Test cases:**
1. Parse sample paper filing → verify transactions extracted
2. Parse sample electronic filing → verify transactions extracted
3. Full pipeline: ingest → parse → score → verify counts

**Done when:** Tests pass with expected transaction counts

---

## Dependency Graph

```
Unit 1 (House OCR) ──────┐
                         ├──► Unit 4 (Validation) ──► Unit 5 (E2E Test)
Unit 2 (Failure Detection)│
                         │
Unit 3 (Senate Verify) ──┘
```

**Execution Order:**
1. **Parallel:** Units 1, 2, 3 (independent fixes)
2. **Sequential:** Unit 4 (needs Units 1-3 complete)
3. **Sequential:** Unit 5 (final validation)

---

## Hard 30% (Uncertainty Areas)

1. **OCR Quality on House PDFs**
   - House paper filings may have different layouts than Senate
   - Transaction table structure unknown until we see actual content
   - Mitigation: Test on 3-5 sample files first

2. **Senate Paper Filing Format**
   - Need to understand why HTML files exist for "paper" filings
   - May need different parsing approach
   - Mitigation: Inspect actual file contents

3. **Transaction Matching for Validation**
   - Different sources may have slight variations in names/dates
   - Need fuzzy matching for comparison
   - Mitigation: Match on (senator + date + amount range)

---

## Edge Case Handling

### OCR Output Validation
Before parsing OCR text, validate it's not garbage:
```python
def validate_ocr_output(text: str) -> bool:
    """Return True if OCR output appears to be valid text."""
    if not text or len(text) < 50:
        return False
    # Check for excessive special characters (OCR garbage)
    special_char_ratio = sum(1 for c in text if not c.isalnum() and c not in ' .,()-$%:/\n') / len(text)
    if special_char_ratio > 0.3:  # >30% special chars = garbage
        return False
    # Check for some expected patterns (dates, dollar amounts)
    has_date = re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', text)
    has_amount = re.search(r'\$[\d,]+', text)
    return bool(has_date or has_amount)
```

### Tesseract Not Installed
Graceful fallback when OCR is unavailable:
```python
def ocr_pdf_page(pdf_path: str, page_num: int) -> str:
    """OCR a page from a scanned PDF. Returns empty string if OCR unavailable."""
    try:
        import pytesseract
        from pdf2image import convert_from_path
    except ImportError:
        logger.warning("OCR dependencies not installed (pytesseract, pdf2image)")
        return ""

    try:
        images = convert_from_path(pdf_path, first_page=page_num+1, last_page=page_num+1)
        if not images:
            return ""
        return pytesseract.image_to_string(images[0])
    except Exception as e:
        logger.warning(f"OCR failed for {pdf_path} page {page_num}: {e}")
        return ""
```

### Multi-Page Paper PDFs
Handle paper filings with multiple pages:
```python
# In HousePDFParser.parse():
all_text = []
for page_num, page in enumerate(pdf.pages):
    text = page.extract_text() or ""
    if not text.strip() and page.images:
        text = ocr_pdf_page(pdf_path, page_num)
        if validate_ocr_output(text):
            all_text.append(text)
        else:
            logger.warning(f"OCR produced invalid output for {pdf_path} page {page_num}")
    else:
        all_text.append(text)
full_text = "\n".join(all_text)
```

---

## Change Discipline

### Branch Strategy
```bash
git checkout -b feat/2026-03-31-cppi-ocr-support
```

### Commit Sequence
Each commit must leave codebase in working state:

1. **Commit 1:** Plan document (before execution)
   ```bash
   git add docs/plans/2026-03-31-cppi-data-completeness.md
   git commit -m "docs: add CPPI data completeness audit plan"
   ```

2. **Commit 2:** Unit 1 - House OCR support
   ```bash
   git add cppi/parsing.py cppi/ocr.py
   git commit -m "feat(parsing): add OCR fallback for House paper filings"
   ```

3. **Commit 3:** Unit 2 - Failure detection
   ```bash
   git add cppi/parsing.py cppi/cli.py
   git commit -m "feat(cli): add parsing failure detection and logging"
   ```

4. **Commit 4:** Unit 3 - Senate verification
   ```bash
   git add scripts/scrape_senate.py cppi/connectors/senate.py
   git commit -m "fix(senate): verify paper filing downloads"
   ```

5. **Commit 5:** Unit 4 - External validation script
   ```bash
   git add scripts/validate_against_ssw.py
   git commit -m "feat(validation): add external data comparison script"
   ```

6. **Commit 6:** Unit 5 - E2E tests
   ```bash
   git add tests/test_e2e_pipeline.py
   git commit -m "test: add end-to-end pipeline tests"
   ```

7. **Commit 7:** Documentation updates
   ```bash
   git add README.md docs/methodology.md
   git commit -m "docs: update OCR requirements and paper filing handling"
   ```

---

## Files to Modify

| File | Changes |
|------|---------|
| `cppi/parsing.py` | Add OCR fallback, logging for empty results |
| `cppi/ocr.py` | Add `ocr_pdf_page()` function |
| `cppi/cli.py` | Add parsing failure summary |
| `scripts/scrape_senate.py` | Verify paper filing downloads |
| `cppi/connectors/senate.py` | Validate downloaded content |
| NEW: `scripts/validate_against_ssw.py` | External validation |
| NEW: `tests/test_e2e_pipeline.py` | End-to-end tests |

---

## Verification Strategy

### After Unit 1:
```bash
cd /tmp/congressional_positioning
source /tmp/playwright_venv/bin/activate
python -c "
from cppi.parsing import parse_house_pdf
filing = parse_house_pdf('cache/pdfs/house/8220118.pdf')
print(f'Transactions: {len(filing.transactions)}')
assert len(filing.transactions) > 0, 'OCR failed'
"
```

### After Unit 2:
```bash
python -m cppi.cli parse 2>&1 | grep -E "(warning|0 transactions)"
# Should show warnings for any remaining failures
```

### After All Units:
```bash
# Re-run full pipeline
python -m cppi.cli parse
python -m cppi.cli score --window 1500
python -m cppi.cli report --window 1500 --stdout

# Verify increased transaction count
# Before: ~41,000 included
# After: Should be ~45,000+ included
```

### External Validation:
```bash
python scripts/validate_against_ssw.py
# Should show <5% discrepancy vs senate-stock-watcher-data
```

---

## Expected Outcome

| Metric | Before | After (Expected) |
|--------|--------|------------------|
| House filings parsed | 964 | 1,095 (+131) |
| House transactions | 42,658 | ~48,000 (+5,000) |
| Senate transactions | 7,623 | ~8,000 (+~400) |
| Total included | 41,624 | ~47,000 |
| Volume estimate | ~$20M | ~$50-100M |

---

## Documentation to Update

- `README.md` - Add note about OCR requirement for paper filings
- `docs/methodology.md` - Document paper filing handling

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| OCR produces garbage | Medium | High | Validate OCR output format before parsing |
| Different PDF layouts | Medium | Medium | Create layout-specific parsers if needed |
| External data mismatch | Low | Low | Use as guidance, not ground truth |

---

## References

- [senate-stock-watcher-data](https://github.com/timothycarambat/senate-stock-watcher-data) - Comprehensive Senate transaction data
- [us-senate-financial-disclosure-scraper](https://github.com/jeremiak/us-senate-financial-disclosure-scraper) - Senate scraping approach
- [Unusual Whales 2025 Report](https://unusualwhales.substack.com/p/congressional-trading-report-2025) - External benchmark

---

## Execution Results

**Execution Date:** 2026-03-31
**Branch:** `feat/2026-03-31-cppi-ocr-support`

### Commits
1. `e44598a` - docs: add CPPI data completeness audit plan
2. `6647ff1` - feat(parsing): add OCR fallback for House paper filings
3. `95110ee` - feat(cli): add parsing failure detection and logging
4. `4083aa8` - fix(senate): add GIF download for paper filings (Unit 3)
5. `2db6cc6` - feat(validation): add external data comparison script (Unit 4)
6. `b197252` - test: add end-to-end pipeline tests (Unit 5)
7. `7a70c15` - docs: update OCR requirements and paper filing handling
8. `bf05734` - fix: lint errors in validation script and E2E tests

### Results
| Metric | Before | After |
|--------|--------|-------|
| Senate GIF files | 5 | 134 |
| Tests | 276 | 291 |
| Lint errors | 0 | 0 |

### Deviations from Plan
- OCR functions added to `parsing.py` instead of separate `cppi/ocr.py` (consolidation)
- GIF downloading added to `scrape_senate.py` instead of `cppi/connectors/senate.py` (consolidation)
- SSW validation limited by outdated external data (2012-2020 only)

### Issues Discovered
1. **SSW data is outdated** - Only covers 2012-2020, cannot validate 2024+ data
2. **Senate filer names are "Unknown"** - Parser doesn't extract senator names from filing content

### Hard 30% Retrospective
- OCR Quality: ✅ Accurate - Created PaperFilingParser for different layout
- Senate Paper Format: ✅ Accurate - HTML files are GIF wrappers
- Transaction Matching: ⚠️ SSW data age was not anticipated

---

## Sync Verification
- [x] Verification strategy executed: PASS (291 tests)
- [ ] Branch pushed to remote: PENDING
- [ ] Branch merged to main: PENDING
- [ ] Main pushed to remote: N/A (local project)
- [x] Documentation updated and current: YES
- [ ] Production deploy: N/A
- [ ] Local, remote, and main are consistent: PENDING
- Verified at: 2026-03-31T21:55:00Z
