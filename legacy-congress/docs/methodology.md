# CPPI Methodology

## Congressional Disclosed Positioning Index

This document describes the methodology used by the Congressional Policy Positioning Index (CPPI) to measure aggregate disclosed congressional portfolio positioning.

---

## 1. Signal Objective

**Primary objective:** Measure aggregate disclosed congressional portfolio positioning -- the net direction and magnitude of disclosed trading activity across members and their households.

**What this measures:**
- Disclosed buying and selling patterns across Congress
- Breadth (how many members are net buyers vs sellers)
- Volume (estimated dollar amounts, lag-adjusted)

**What this is NOT:**
- A claim that Congress "knows" something about the economy
- A prediction system
- An ethics or compliance monitor
- An inference about motivations

All outputs use neutral language describing "positioning patterns" and "disclosure activity." The system measures disclosed behavior; it does not interpret intent.

---

## 2. Data Sources

### House of Representatives
- Source: clerk.house.gov Public Disclosure
- Format: PDF filings (electronic and paper)
- Filing type: Periodic Transaction Reports (PTR)
- Disclosure requirement: Within 45 days of transaction

### Senate
- Source: efdsearch.senate.gov
- Format: HTML (electronic) or scanned images (paper)
- Filing type: Periodic Transaction Reports (PTR)
- Disclosure requirement: Within 45 days of transaction

### Filing Formats

Congressional filings come in two formats:

| Format | House | Senate | Processing |
|--------|-------|--------|------------|
| Electronic | PDF (2002xxxx IDs) | HTML | Direct text extraction |
| Paper | PDF (822xxxx IDs) | GIF images | OCR required |

**Electronic filings** are submitted digitally and contain extractable text. These parse reliably with high accuracy.

**Paper filings** are scanned handwritten or typed documents submitted by mail. These require Optical Character Recognition (OCR) to extract text, which introduces potential errors.

### Paper Filing OCR

For paper filings, the system:

1. **Detects paper filings** by filing ID pattern (House: 822xxxx) or content (images with no text)
2. **Attempts OCR** using Tesseract if available
3. **Validates OCR output** to filter garbage (high special character ratios, missing expected patterns)
4. **Parses with adjusted logic** - paper filing formats differ from electronic

Paper filing parsing has ~80% success rate due to:
- Handwriting legibility variations
- Scanning quality issues
- Non-standard form completion

If Tesseract is not installed, paper filings are skipped with a warning. Install with:
```bash
# macOS
brew install tesseract poppler

# Linux
sudo apt-get install tesseract-ocr poppler-utils
```

---

## 3. Three-Timestamp Model

Every transaction carries three dates:

```
execution_date     disclosure_date     ingestion_date
     |                   |                   |
     |<-- trade_lag ---->|<-- data_lag ----->|
     |                   |                   |
     |<--------- total_lag ----------------->|
```

- **execution_date**: When the member (or household) executed the trade
- **disclosure_date**: When the filing became public
- **ingestion_date**: When we processed the filing

### Staleness Penalty

Older trades are less informative. We apply a decay penalty:

| Total Lag | Penalty Factor | Interpretation |
|-----------|----------------|----------------|
| <= 45 days | 1.0 | Fresh (within disclosure window) |
| <= 60 days | 0.9 | Slightly stale |
| <= 90 days | 0.7 | Moderately stale |
| <= 180 days | 0.4 | Very stale |
| > 180 days | 0.2 | Extremely stale |

---

## 4. Amount Estimation

Congressional disclosures report amounts in standardized ranges (STOCK Act):

| Range Code | Min | Max |
|------------|-----|-----|
| $1,001 - $15,000 | $1,001 | $15,000 |
| $15,001 - $50,000 | $15,001 | $50,000 |
| $50,001 - $100,000 | $50,001 | $100,000 |
| $100,001 - $250,000 | $100,001 | $250,000 |
| $250,001 - $500,000 | $250,001 | $500,000 |
| $500,001 - $1,000,000 | $500,001 | $1,000,000 |
| $1,000,001 - $5,000,000 | $1,000,001 | $5,000,000 |
| Over $50,000,000 | $50,000,001 | $100,000,000 |

### Estimation Method

We use the **geometric mean** of range bounds:

```
estimated_amount = sqrt(min * max)
```

Example: $100,001 - $250,000 -> estimated $158,114

This method:
- Weights large ranges appropriately
- Doesn't over-weight the upper bound
- Handles log-normal distributions common in financial data

Alternative methods available for sensitivity analysis:
- Lower bound (conservative)
- Midpoint (simple average)
- Log-uniform expected value (theoretically correct for uniform log-space)

---

## 5. Owner Type Weighting

Disclosures include transactions by household members. We weight by likely information content:

| Owner Type | Weight | Rationale |
|------------|--------|-----------|
| Self | 1.0 | Direct member trades |
| Joint | 0.9 | Shared decision |
| Spouse | 0.8 | Household alignment |
| Dependent | 0.5 | Less direct |
| Managed Account | 0.3 | Advisor-driven |

---

## 6. Inclusion/Exclusion Policy

### Included Assets
- Common stocks (weight 1.0)
- Single-stock ETFs (weight 1.0)
- Sector ETFs (weight 0.8, flagged)
- Options (weight 0.7)
- Preferred stocks (weight 0.6)

### Excluded Assets
- Broad index ETFs (SPY, QQQ, VTI, etc.) - non-informative
- Mutual funds - pooled, diversified
- Treasury securities - different asset class
- Municipal bonds - tax-driven
- Corporate bonds - different signal
- Private placements - cannot map to market
- Crypto - limited data
- Blind trusts - member has no control

### Transaction Types
- Included: Purchase (P), Sale (S), Sale Partial
- Excluded: Exchange, Gift, Inheritance, Dividend reinvestment

---

## 7. Anti-Dominance Controls

To prevent a few large traders from dominating the signal:

### Per-Member Cap
No single member contributes more than 5% of total signal volume. Excess is clipped (not zeroed).

### Winsorization
Transaction scores are clipped at the 95th percentile to limit outlier impact.

### Log Scaling (Optional)
Apply log(1 + value) to compress large transactions. Disabled by default.

---

## 8. Dual Signal Presentation

We report BOTH breadth and volume signals:

### Breadth Signal
- Equal-weight by member
- (Net buyers - Net sellers) / Total active members
- More robust to large-trade dominance
- Example: "58% net buyers"

### Volume Signal
- Dollar-weighted (lag-adjusted)
- Signed estimated volume after all adjustments
- Example: "Estimated +$47M equivalent"

---

## 9. Confidence Scoring

Signal quality varies. We compute a composite confidence score:

| Factor | Weight | Scoring |
|--------|--------|---------|
| Member coverage | 25% | 50+ members = 1.0 |
| Resolution quality | 20% | % of assets resolved to tickers |
| Timeliness | 20% | Mean staleness penalty |
| Concentration | 15% | 1.0 - top 5 member share |
| Transaction volume | 10% | 200+ transactions = 1.0 |
| Chamber balance | 10% | 50/50 House/Senate = 1.0 |

### Confidence Tiers
- **HIGH** (> 0.7): Strong data quality
- **MODERATE** (0.4 - 0.7): Acceptable with caveats
- **LOW** (< 0.4): Interpret with extreme caution

---

## 10. Minimum Thresholds

Signal is only generated when:
- 50+ included transactions in window
- 10+ unique members active

Below these thresholds, report shows "INSUFFICIENT DATA".

---

## 11. Honest Limitations

This methodology has known limitations:

1. **Disclosure lag**: Data is 6-12 weeks old by the time we see it
2. **Range uncertainty**: All amounts are estimates from ranges
3. **Resolution gaps**: Some assets cannot be mapped to tickers
4. **Spouse/managed trades**: May not reflect member intent
5. **Selection bias**: Active traders over-represented
6. **Not predictive**: This measures past behavior, not future outcomes

---

## 12. Configuration

All methodology parameters are configurable:

```python
# Anti-dominance controls
MEMBER_CAP_PCT = 0.05        # Max contribution per member
WINSORIZE_PERCENTILE = 0.95  # Outlier clipping

# Amount estimation
AMOUNT_METHOD = "geometric_mean"  # or: midpoint, lower_bound, log_uniform_ev
USE_LOG_SCALING = False      # Compress large trades

# Staleness penalties
STALENESS_PENALTIES = {
    45: 1.0,   # Fresh
    60: 0.9,   # Slightly stale
    90: 0.7,   # Moderately stale
    180: 0.4,  # Very stale
}

# Validity thresholds
MIN_TRANSACTIONS = 50
MIN_MEMBERS = 10
```

---

## 13. Disclaimer

This tool provides aggregate positioning data for research and analysis purposes. It is NOT:
- Financial advice
- An ethics or compliance monitor
- A measure of trading skill or impropriety
- A prediction of future market performance

All data is derived from public disclosure filings. The signal reflects disclosed activity patterns, not intent, knowledge, or prediction.

---

*Methodology version: 1.1*
*Last updated: 2026-03-31*
