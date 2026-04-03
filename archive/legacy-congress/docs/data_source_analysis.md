# Congressional Disclosure Data Source Analysis

**Unit 0 Deliverable — Context Gathering Results**
**Date:** 2026-03-29
**Samples Collected:** 30 House PDFs, 25 Senate samples (20 electronic + 5 paper)

---

## Executive Summary

This document captures findings from exploring the House and Senate disclosure sites to understand data access patterns, file formats, and parsing requirements for the Congressional Policy Positioning Index (CPPI) system.

**Key Findings:**
1. Both chambers have **two distinct filing formats** (electronic vs paper/scanned)
2. Senate requires **session agreement** before accessing reports
3. House PDFs can be directly downloaded; Senate electronic filings are HTML, paper filings are GIFs
4. No CAPTCHAs or aggressive anti-bot measures detected (rate limiting appears lenient)
5. Senate has a **search API hypothesis partially confirmed** — electronic filings are structured HTML, not a REST API

---

## 1. House Disclosures (clerk.house.gov)

### 1.1 URL Patterns

**Base URL:** `https://disclosures-clerk.house.gov/`

**PTR PDF Pattern:**
```
https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{YEAR}/{FILING_ID}.pdf
```

**Filing ID Format:**
- Electronic filings: `2002xxxx` (8 digits, year prefix 2002-2026)
- Paper filings: `822xxxx` (7 digits, 822 prefix)

**Asset Type Codes Reference:**
```
https://fd.house.gov/reference/asset-type-codes.aspx
```

### 1.2 Access Constraints

| Constraint | Status | Notes |
|------------|--------|-------|
| robots.txt | 404 (none) | No restrictions specified |
| Rate limiting | Not detected | No throttling during testing |
| CAPTCHAs | None | Direct PDF access |
| Authentication | None | Public access |
| Session required | No | Stateless PDF downloads |
| Directory listing | 403 | Cannot enumerate files |

### 1.3 Format Variations

#### Electronic Format (IDs: 2002xxxx)
- **File size:** 60-100 KB
- **Structure:** Structured PDF with embedded text tables
- **Parseable:** Yes, via pdfplumber or pdftotext
- **Sample fields:**
  - Filing ID
  - Filer Name & Status
  - State/District
  - Transaction table (Owner, Asset, Transaction Type, Date, Amount)

**Sample transaction table structure:**
```
Owner | Asset | Transaction Type | Date | Amount | Cap Gains >$200
SP    | Stock Name [ST] | S | 03/15/2024 | $1,001 - $15,000 |
```

**Owner Codes:**
- `SP` — Spouse
- `DC` — Dependent Child
- `JT` — Joint
- (blank) — Self

**Asset Type Codes:**
- `ST` — Stock
- `OP` — Stock Option
- `GS` — Government Securities (Treasuries)
- `MF` — Mutual Fund
- `EF` — Exchange Traded Fund
- `OT` — Other
- `BD` — Corporate Bond
- `CS` — Cryptocurrency

#### Paper/Scanned Format (IDs: 822xxxx)
- **File size:** 200-300 KB
- **Structure:** Scanned grid form with checkbox columns
- **Parseable:** Requires OCR (no embedded text)
- **Amount ranges:** Checkbox columns A through K representing value ranges

**Amount range mapping (from form):**
| Column | Range |
|--------|-------|
| A | $1,001 - $15,000 |
| B | $15,001 - $50,000 |
| C | $50,001 - $100,000 |
| D | $100,001 - $250,000 |
| E | $250,001 - $500,000 |
| F | $500,001 - $1,000,000 |
| G | $1,000,001 - $5,000,000 |
| H | $5,000,001 - $25,000,000 |
| I | $25,000,001 - $50,000,000 |
| J | Over $50,000,000 |
| K | (Spouse value threshold) |

### 1.4 Sample Files Collected

```
30 total House PDFs:
- 28 electronic format (IDs 20024xxx-20026xxx)
- 2 paper format (IDs 8220162, 8220320)
```

---

## 2. Senate Disclosures (efdsearch.senate.gov)

### 2.1 URL Patterns

**Base URL:** `https://efdsearch.senate.gov/`

**Search Home:**
```
https://efdsearch.senate.gov/search/home/
```

**Electronic PTR Pattern:**
```
https://efdsearch.senate.gov/search/view/ptr/{UUID}/
```

**Paper Filing Pattern:**
```
https://efdsearch.senate.gov/search/view/paper/{UUID}/
```

**Media (Paper GIF) Pattern:**
```
https://efd-media-public.senate.gov/media/{YEAR}/{MONTH}/000/{PATH}/{FILEID}.gif
```

**UUID Format:** Standard UUID v4 (lowercase for PTR/annual, sometimes uppercase for paper)

### 2.2 Access Constraints

| Constraint | Status | Notes |
|------------|--------|-------|
| robots.txt | 404 (none) | No restrictions specified |
| Rate limiting | Not detected | 0.5s delays used out of caution |
| CAPTCHAs | None | Agreement-based access |
| **Session required** | **YES** | Must accept agreement first |
| Directory listing | N/A | UUID-based, not enumerable |

**Session Establishment Process:**
1. GET `/search/home/` — Obtain CSRF token and session cookies
2. POST `/search/home/` with:
   - `csrfmiddlewaretoken={token}`
   - `prohibition_agreement=1`
3. Redirects to `/search/` with valid session cookie
4. Session cookie: `sessionid` with `search_agreement` flag

**Required Headers for POST:**
```
Content-Type: application/x-www-form-urlencoded
Referer: https://efdsearch.senate.gov/search/home/
```

### 2.3 Search API Hypothesis

**Original hypothesis:** Senate has a structured search API.

**Finding:** Partially confirmed — no REST API, but:
- Electronic filings are rendered as structured HTML tables
- Data is easily parseable from HTML (not PDF)
- Third-party scrapers (senate-stock-watcher) have successfully automated extraction

**Implication:** Senate electronic filings may be easier to parse than House PDFs.

### 2.4 Format Variations

#### Electronic Format (UUID-based PTR)
- **Format:** HTML page with structured table
- **Parseable:** Yes, via HTML parsing (BeautifulSoup, etc.)
- **File size:** 12-50 KB (HTML)

**Table columns:**
| Column | Description |
|--------|-------------|
| # | Transaction number |
| Transaction Date | Date of trade |
| Owner | Self, Spouse, Joint, Dependent |
| Ticker | Stock symbol (with Yahoo Finance link) |
| Asset Name | Full company/asset name |
| Asset Type | Stock, Option, ETF, etc. |
| Type | Purchase, Sale (Partial), Sale (Full), Exchange |
| Amount | Dollar range (e.g., "$15,001 - $50,000") |
| Comment | Filer notes |

**Sample HTML structure:**
```html
<table class="table table-striped">
  <thead>
    <tr class="header">
      <th scope="col">Transaction Date</th>
      <th scope="col">Owner</th>
      <th scope="col">Ticker</th>
      ...
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>04/24/2017</td>
      <td>Self</td>
      <td><a href="https://finance.yahoo.com/quote/IBM">IBM</a></td>
      ...
    </tr>
  </tbody>
</table>
```

#### Paper/Scanned Format (GIF images)
- **Format:** GIF image (scanned form)
- **File size:** 100-200 KB
- **Parseable:** Requires OCR
- **Media URL:** Served from `efd-media-public.senate.gov`

### 2.5 Sample Files Collected

```
25 total Senate samples:
- 20 electronic format (HTML files)
- 5 paper format (GIF images)
```

---

## 3. Data Structure Comparison

| Field | House Electronic | House Paper | Senate Electronic | Senate Paper |
|-------|------------------|-------------|-------------------|--------------|
| Format | PDF (text) | PDF (image) | HTML | GIF |
| Parsing | pdfplumber | OCR required | BeautifulSoup | OCR required |
| Transaction Date | ✓ | ✓ (OCR) | ✓ | ✓ (OCR) |
| Owner Type | Codes (SP, DC, JT) | Codes | Full text | Full text |
| Ticker | Sometimes embedded | Manual | Usually linked | Manual |
| Asset Name | ✓ | ✓ (OCR) | ✓ | ✓ (OCR) |
| Amount Range | ✓ | Checkbox cols | ✓ | Form fields |
| Transaction Type | Codes (P, S, E) | Codes | Full text | Full text |

---

## 4. Amount Range Standards (STOCK Act)

Both chambers use the same STOCK Act amount ranges:

| Range Code | Min | Max | Geometric Mean |
|------------|-----|-----|----------------|
| $1,001 - $15,000 | 1,001 | 15,000 | $3,873 |
| $15,001 - $50,000 | 15,001 | 50,000 | $27,387 |
| $50,001 - $100,000 | 50,001 | 100,000 | $70,712 |
| $100,001 - $250,000 | 100,001 | 250,000 | $158,114 |
| $250,001 - $500,000 | 250,001 | 500,000 | $353,553 |
| $500,001 - $1,000,000 | 500,001 | 1,000,000 | $707,107 |
| $1,000,001 - $5,000,000 | 1,000,001 | 5,000,000 | $2,236,068 |
| $5,000,001 - $25,000,000 | 5,000,001 | 25,000,000 | $11,180,340 |
| $25,000,001 - $50,000,000 | 25,000,001 | 50,000,000 | $35,355,339 |
| Over $50,000,000 | 50,000,001 | 100,000,000 | $70,710,678 |

---

## 5. Transaction Type Codes

### House Codes
| Code | Meaning |
|------|---------|
| P | Purchase |
| S | Sale |
| E | Exchange |

### Senate Full Text
- Purchase
- Sale (Partial)
- Sale (Full)
- Exchange

---

## 6. Third-Party Data Sources

### Existing Scrapers (for reference, not dependency)

1. **senate-stock-watcher-data** (GitHub: timothycarambat)
   - Daily JSON files with parsed Senate transactions
   - Includes PTR links and transaction arrays
   - Data structure verified usable for validation

2. **us-senate-financial-disclosure-scraper** (GitHub: jeremiak)
   - Full scraper with HTML parsing and PDF annotation
   - Produces transactions.csv
   - Handles both electronic and paper formats

### Commercial APIs (not used, for awareness)
- Quiver Quantitative (quiverquant.com/congresstrading)
- Financial Modeling Prep Senate Trading API
- Finnhub Congressional Trading API

---

## 7. Parser Requirements Summary

### House Parser
1. **Electronic PDFs:**
   - Extract text via pdfplumber
   - Parse transaction table structure
   - Map owner codes (SP, DC, JT)
   - Extract embedded tickers when present

2. **Paper PDFs:**
   - OCR via Tesseract or similar
   - Detect checkbox columns for amount ranges
   - Higher error rate expected
   - May need manual validation set

### Senate Parser
1. **Electronic HTML:**
   - Parse HTML table structure
   - Extract ticker from Yahoo Finance links
   - Straightforward field mapping
   - Highest accuracy expected

2. **Paper GIFs:**
   - Convert GIF to processable format
   - OCR text extraction
   - Similar challenges to House paper

---

## 8. Recommendations for Unit 1+

### Connector Architecture
1. **House Connector:** Direct HTTP downloads, no session needed
2. **Senate Connector:** Session establishment required before each batch

### Parser Priority
1. Senate electronic (HTML) — easiest, start here for validation
2. House electronic (PDF) — moderate complexity
3. Paper formats (both) — defer or simplify (may mark as LOW CONFIDENCE)

### Validation Strategy
- Use senate-stock-watcher data for cross-validation
- Manual spot-check 10 filings per format
- Track parse error rates by format

### Risk Factors
1. **Paper filings may be too error-prone:** Consider excluding or heavily downweighting
2. **Senate session may expire:** Implement session refresh logic
3. **Format changes:** Monitor for disclosure form updates

---

## 9. Files in Validation Set

### House PDFs (`cache/pdfs/house/`)
```
Electronic: 20024300, 20024305, 20024309, 20024330, 20024375, 20024425,
            20024450, 20024542, 20024625, 20024750, 20024800, 20024825,
            20025000, 20025025, 20025200, 20025368, 20025475, 20025535,
            20025675, 20025700, 20025819, 20025875, 20026125, 20026150,
            20026250, 20026446, 20026537, 20026590

Paper: 8220162, 8220320
```

### Senate Samples (`cache/pdfs/senate/`)
```
Electronic HTML: ptr_0068462f, ptr_00f9f9ee, ptr_01b2a815, ptr_02c266b1,
                 ptr_03ac42db, ptr_04d279c4, ptr_05bf0349, ptr_064147b0,
                 ptr_07163d2f, ptr_0ab7eda5, ptr_0c3d0d20, ptr_0de0b87b,
                 ptr_0e08d912, ptr_0e3cbf76, ptr_0ec02578, ptr_0f3a5882,
                 ptr_7718a62c, ptr_a0010f4a, ptr_b7e581e7, ptr_cca86792

Paper GIF: paper_00181DE2, paper_005C1940, paper_02115eab,
           paper_068059AF, paper_836382b4
```

---

## 10. Unit 0 Completion Checklist

- [x] 25+ House PTR PDFs downloaded (30 collected)
- [x] 25+ Senate PTR samples downloaded (25 collected)
- [x] URL patterns documented
- [x] Search parameters documented
- [x] Rate limit behavior tested (lenient)
- [x] Access constraints documented (Senate session required)
- [x] Format variations catalogued (4 formats total)
- [x] Date format variations noted (MM/DD/YYYY standard)
- [x] Amount range formats documented
- [x] Owner type patterns documented
- [x] Findings written to this document

**Unit 0 Status: COMPLETE**

---

*Document created as part of CPPI Unit 0: Context Gathering*
