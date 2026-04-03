# Insider Trading Signal Engine — Methodology

## Purpose

This system ingests SEC EDGAR Form 4 filings, filters out low-value and noisy transactions, scores high-value executive behavior, and produces:

1. A company-level bullish/bearish/neutral signal
2. An aggregate Fortune 500 Executive Risk Appetite Index
3. A confidence score and explanation for every conclusion
4. A report showing which leaders and companies are actually informative

**Important caveat:** Insider trades are a proxy for internal conviction and risk appetite, not direct proof of macroeconomic views. This system does not claim to predict stock prices or economic conditions.

---

## Data Source

- **Source of truth:** SEC EDGAR Form 4 filings (XML format)
- **Filing types:** Form 4 (standard) and Form 4/A (amendments)
- **Only executed transactions** are included. Form 144 (proposed sales) are excluded entirely.
- **API:** EDGAR submissions API (`data.sec.gov/submissions/CIK{cik}.json`) for filing metadata; direct XML download for filing content.
- **Rate limits:** ≤10 requests/second with proper User-Agent header per SEC policy.

---

## Universe

The system operates on a user-provided CSV of companies with fields:
- `company_name`, `ticker`, `rank`, `revenue`, `sector`, `cik`

CIKs are resolved via SEC's `company_tickers.json` if not provided. The system supports any set of companies, but is designed for the Fortune 500 universe.

Sectors should use the 11 GICS sector classifications:
Energy, Materials, Industrials, Consumer Discretionary, Consumer Staples, Health Care, Financials, Information Technology, Communication Services, Utilities, Real Estate.

---

## Role Inclusion / Exclusion

### Included (C-Suite and Top Leadership)

Only filings from these roles are included in the core signal:

| Role | Title Matching (case-insensitive regex patterns) |
|------|--------------------------------------------------|
| CEO | "ceo", "chief executive officer" |
| CFO | "cfo", "chief financial officer", "principal financial officer" |
| Chair | "chairman", "chairwoman", "chairperson", "executive chair" |
| President | "president" (excluding "vice president") |
| COO | "coo", "chief operating officer" |
| CTO | "cto", "chief technology officer" |
| CLO | "clo", "chief legal officer", "general counsel" |
| CIO | "cio", "chief information officer" |
| CMO | "cmo", "chief marketing officer" |
| CAO | "cao", "chief accounting officer", "principal accounting officer" |

**Note:** The system prioritizes the officer_title text over the XML `isOfficer` flag, since the flag is not always reliably set by filers. If the title matches a pattern above, the transaction is included regardless of the `isOfficer` flag.

### Excluded

| Category | Rule |
|----------|------|
| 10% holders | `isTenPercentOwner=true` AND `isOfficer=false` AND `isDirector=false` |
| Entities | Owner name matches patterns: LLC, LP, Trust, Foundation, Fund, Capital, Partners, Advisors, Holdings, Inc, Corp, Ltd, Group, Associates, Investment, Management, Enterprise |
| Former officers | Title contains: "former", "ex-", "fmr", "retired", "past" |
| Directors only | `isDirector=true` AND `isOfficer=false` AND no matching title |
| Non-C-suite officers | Officers whose title does not match any of the 10 C-suite roles above (e.g., SVP, EVP, VP of Sales) |

---

## Transaction Inclusion / Exclusion

### Included in Core Signal

| Code | Type | Treatment |
|------|------|-----------|
| P | Open-market purchase | **Bullish signal** — weighted at +1.0 |
| S | Open-market sale | **Bearish signal** — weighted at -0.5 |

### Excluded from Core Signal

| Code | Type | Reason |
|------|------|--------|
| M | Option exercise | Not discretionary; often compensation-related |
| F | Tax withholding | Automatic; not discretionary |
| A | Award/grant | Incoming compensation; not a trading decision |
| G | Gift | Not a market-priced trade |
| C | Conversion | Structural change; not a trading decision |
| D | Disposition to issuer | Non-market transaction |
| J, K, L, etc. | Various | Non-standard transactions |

### Additional Exclusions

- **Derivative transactions:** Options, warrants, RSUs are excluded. Only common stock transactions count.
- **Equity swaps:** `equitySwapInvolved=true` → excluded.
- **Indirect ownership through entities:** If ownership is indirect AND the entity name matches entity patterns (Trust, LLC, etc.) → excluded.
- **Exercise-and-sell combos:** If an S transaction occurs within 3 calendar days of an M transaction by the same insider with share counts within 10% tolerance, the S is flagged as `exercise_and_sell` and excluded.

---

## Scoring Methodology

### Per-Transaction Score

Each qualifying transaction is scored as:

```
transaction_signal = direction × role_weight × discretionary_weight × size_signal × ownership_weight × recency_weight
```

#### Direction Weight (Buy/Sell Asymmetry)

- **Buy (P):** +1.0
- **Sell (S):** -0.5

**Rationale:** Academic literature consistently finds insider purchases are more predictive of future returns than insider sales. Insiders sell for many non-informative reasons (liquidity, diversification, taxes). They buy primarily because they believe the stock is undervalued. The 2:1 ratio reflects this empirical asymmetry.

#### Role Weight

| Role | Weight |
|------|--------|
| CEO | 1.00 |
| CFO | 0.95 |
| Chair | 0.90 |
| President | 0.85 |
| COO | 0.80 |
| CTO | 0.75 |
| CLO | 0.75 |
| CIO | 0.70 |
| CMO | 0.70 |
| CAO | 0.70 |
| officer_other | 0.50 |

**Note:** The `officer_other` category captures officers with `isOfficer=true` whose titles don't match any C-suite pattern. These are included but weighted lower to maintain coverage while preserving signal quality.

#### Discretionary Weight (10b5-1 Plans)

- **Discretionary trade:** 1.0
- **Likely planned (10b5-1):** 0.25

10b5-1 detection is based on keyword matching in filing footnotes. Keywords: "10b5-1", "10b-5-1", "rule 10b5", "trading plan", "pre-arranged", "pre-established", "prearranged", "predetermined".

Planned trades are included but heavily discounted, not excluded, because plan initiation timing can still carry information.

#### Size Signal

Based on the percentage of the insider's holdings changed:

| % of Holdings | Weight |
|---------------|--------|
| < 1% | 0.5 (routine/small) |
| 1–5% | 0.8 |
| 5–20% | 1.0 |
| 20%+ | 1.2 (cap — prevents outlier domination) |
| Unknown | 0.6 (penalize missing data) |

#### Ownership Weight

- **Direct:** 1.0
- **Indirect:** 0.6

#### Recency Weight

Exponential decay with 45-day half-life:

```
recency = exp(-0.693 × days_ago / 45)
```

---

### Company-Level Aggregation

#### Per-Insider Saturation Cap

No single insider can contribute more than **30%** of a company's total signal magnitude. This prevents a single mega-trade from dominating the company score.

If insider A's absolute signal exceeds 30% of the total absolute signal from all insiders, it is capped.

#### Normalization

```
company_raw_score = sum(capped per-insider signals) / max(1, count_unique_insiders)
company_score = tanh(company_raw_score)  # bounded to [-1, +1]
```

#### Confidence Score

Confidence is computed from:
- Transaction count (log-scaled, 40% weight)
- Unique insider count (log-scaled, 60% weight — breadth matters more)
- Balance bonus: 1.1× if both buys and sells exist

Confidence is capped at 0.90 (never claim certainty).

#### Confidence Tiers

| Range | Tier |
|-------|------|
| < 0.25 | Insufficient |
| 0.25–0.50 | Low |
| 0.50–0.75 | Moderate |
| 0.75+ | High |

#### Signal Label

| Score | Confidence | Label |
|-------|------------|-------|
| > +0.15 | ≥ 0.25 | **Bullish** |
| < -0.15 | ≥ 0.25 | **Bearish** |
| ±0.15 | ≥ 0.25 | **Neutral** |
| Any | < 0.25 | **Insufficient evidence** |

The system defaults to "insufficient evidence" rather than guessing when data is sparse.

---

### Aggregate Index

#### Sector-Balanced Index (Headline)

1. Group companies by GICS sector.
2. Compute mean company score per sector (only companies with confidence ≥ 0.25).
3. Equal-weight across sectors.

This prevents tech/finance domination of the headline number.

#### Other Measures

- **Risk Appetite Index:** Simple mean of all company scores (not sector-balanced).
- **CEO/CFO-Only Index:** Same pipeline but restricted to CEO and CFO roles only.
- **Bullish Breadth:** Fraction of companies with sufficient evidence that show a bullish signal.
- **Bearish Breadth:** Fraction of companies with sufficient evidence that show a bearish signal.
- **Cyclical Score:** Mean sector score across cyclical sectors (Technology, Consumer Discretionary, Financials, Industrials, Materials, Energy).
- **Defensive Score:** Mean sector score across defensive sectors (Consumer Staples, Health Care, Utilities, Real Estate, Communication Services).

---

## Amendment Handling

When a Form 4/A (amendment) is filed:
1. Match it to the original filing by `(issuer_cik, owner_cik, period_of_report)`.
2. Delete all transactions from the original filing.
3. Insert the corrected transactions from the amendment.

This prevents double-counting.

---

## What This System Does NOT Do

- It does not predict stock prices or market direction.
- It does not classify the economy as bearish because of large founder liquidity events.
- It does not treat 10% holders, affiliates, or investment funds as executive sentiment.
- It does not use Form 144 (proposed sales).
- It does not use ML or black-box models.
- It does not overclaim. When evidence is insufficient, it says so.

---

## Traceability

Every company score includes:
- The list of SEC filing accession numbers that contributed to it.
- Counts of buys/sells and unique buyers/sellers.
- A plain-English explanation of the signal.
- The confidence tier.

Every excluded transaction includes a specific `exclusion_reason` field explaining why it was filtered out.
