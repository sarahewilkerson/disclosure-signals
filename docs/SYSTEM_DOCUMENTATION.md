# Disclosure Signals: System Documentation

## What This System Does

Disclosure Signals is a local-first market intelligence platform that analyzes two types of legally-mandated financial disclosures to produce actionable trading signals:

1. **SEC Form 4 (Insider Trading)** — Corporate officers (CEOs, CFOs, directors) must disclose stock purchases and sales within 2 business days. When a CEO buys their own company's stock with personal money, they're betting on its future with private knowledge.

2. **Congressional Periodic Transaction Reports (PTRs)** — Members of Congress must disclose stock trades within 45 days. When a senator on the Banking Committee buys bank stocks, they may have regulatory foresight the market lacks.

The system ingests these filings, normalizes them into a common schema, scores them with empirically-tuned weights, combines them into a cross-source overlay, and produces a daily intelligence brief delivered via email and a web dashboard.

---

## Theory and Research Basis

### Why insider buying works as a signal

Academic evidence consistently shows that insider purchases predict positive abnormal returns:

- **Lakonishok & Lee (2001):** Firms with insider buying outperform by 4-8% over 12 months. Clustered buying (3+ unique insiders) is the strongest predictor.
- **Seyhun (1998):** Insider purchases are more informative than sales because purchases are almost always discretionary, while sales have many non-informational motivations (taxes, diversification, liquidity).
- **Cohen, Malloy & Pomorski (2012):** Pre-arranged 10b5-1 plan trades have near-zero predictive power. Only "opportunistic" trades carry information.

### Why insider selling is noise

Our own validation confirmed the academic consensus:

| Signal | 5-Day Accuracy | 20-Day Accuracy | Sample Size |
|--------|---------------|-----------------|-------------|
| Insider BUY | **70-80%** | **68%** | 19-23 |
| Insider SELL | 41-47% | 47% | 2,414 |

Sells are worse than random. Stocks go *up* after insider selling (mean forward return is positive). This is because most insider sales are routine: tax withholding, diversification, estate planning, liquidity needs. We eliminated sells entirely from the scoring model (`DIRECTION_WEIGHT_SELL = 0.0`).

### Why congressional trading matters (with caveats)

Congressional trading signals are weaker than insider signals but still above random:

| Signal | 5-Day Accuracy | 60-Day Accuracy | Sample Size |
|--------|---------------|-----------------|-------------|
| Congress BUY | **58.6%** | **64.2%** | 811 |
| Congress SELL | 46.8% | 35.7% | 526 |

The post-STOCK Act evidence for systematic congressional outperformance is contested. The value is most likely at the committee level — a senator on Armed Services buying defense stocks may have budget foresight. This is why the system includes committee-sector correlation analysis.

### The trivial baseline finding

A critical validation result: the simple strategy "predict bullish for any ticker where an insider bought" achieves **79% accuracy at 5 days**. This means the raw buy event IS the signal. The scoring model's value lies in **filtering** (what to exclude) rather than **weighting** (how to combine). This insight shaped the architecture — aggressive noise exclusion (sells, 10b5-1 plans, tiny trades, managed accounts, non-signal assets) matters more than sophisticated weight optimization.

---

## Architecture

```
Data Sources                 Processing                     Output
─────────────              ────────────                   ──────

SEC EDGAR ──┐              ┌─ Normalize ─┐
(Form 4 XML) │             │  (45 fields) │
             ├─ Ingest ──> ├─ Resolve ────┤──> Signal ──> Combined ──> Daily Brief
House PTRs ──┤             │  (entity)    │    Results    Overlay      (email + web)
(PDF)        │             ├─ Score ──────┤
Senate PTRs ─┘             │  (per-source)│
(HTML)                     └──────────────┘
                                 │
                           Derived SQLite DB
                           (normalized_transactions,
                            signal_results,
                            combined_results)
```

### Core Data Flow

1. **Ingest:** Download filings from SEC EDGAR (Form 4 XML), House clerk (PTR PDFs), Senate (PTR HTML). Cache locally for idempotent re-processing.

2. **Parse:** Extract structured data from each source format. Form 4 XML parsed with `xml.etree`, House PDFs with `pdfplumber`, Senate HTML with `BeautifulSoup`.

3. **Normalize:** Convert all sources to a common 45-field `NormalizedTransaction` schema covering actor, entity, transaction details, amounts, quality scores, and full provenance.

4. **Resolve:** Map entities to canonical tickers via a multi-tier resolution chain:
   - Canonical CSV ticker match (confidence 0.99)
   - Canonical CIK match (confidence 0.97)
   - Ticker passthrough for non-canonical entities (confidence 0.95)
   - Name fuzzy matching (confidence 0.90)
   - Ambiguous/unresolved (confidence 0.40/0.0)

5. **Score:** Source-specific scoring engines produce `SignalResult` objects:
   - **Insider engine:** `direction × role_weight × discretionary × size_signal × ownership × recency × regime_weight`, with cluster conviction amplification for multi-buyer events
   - **Congress engine:** `base_value × direction × staleness × owner_weight × resolution_confidence × signal_weight × disclosure_lag × regime_weight`

6. **Overlay:** Match insider and congress signals by entity key. Classify as ALIGNED_BULLISH, ALIGNED_BEARISH, TRUE_CONFLICT, or LOW_CONFIDENCE_ALIGNMENT. Assign strength tier (strong/moderate/weak).

7. **Report:** Generate daily brief with 10 sections, render as markdown/HTML, deliver via email and web dashboard.

---

## Scoring Model Details

### Insider Scoring

Each insider transaction is scored as:

```
transaction_signal = direction × role_weight × discretionary_weight 
                   × size_signal × ownership_weight × recency_weight 
                   × regime_weight
```

| Factor | Values | Rationale |
|--------|--------|-----------|
| Direction | BUY=1.0, SELL=0.0 | Sells validated as noise (41-47% accuracy) |
| Role | CEO=1.0, CFO=0.95, Chair=0.9, President=0.85, COO=0.8, Other=0.5 | C-suite has more information |
| Discretionary | Planned(10b5-1)=0.05, Unplanned=1.0 | 10b5-1 plans have near-zero predictive power |
| Size | 1%=0.5, 5%=0.8, 20%=1.0, 100%=1.2 | Larger relative purchases signal more conviction |
| Ownership | Direct=1.0, Indirect=0.6 | Direct ownership is more intentional |
| Recency | exp(-0.693 × days/45) | 45-day half-life exponential decay |
| Regime | Bear=1.1, Neutral=1.0, Bull=0.95 | EXPERIMENTAL — small n |

**Aggregation:** Per-company signals are aggregated with saturation capping (30% max per insider) and tanh normalization. Cluster conviction: 25% score boost per additional unique buyer (in tanh space).

**Minimum threshold:** Requires 2+ qualifying transactions to emit a non-insufficient signal.

### Congress Scoring

```
final_score = base_value × direction × staleness × owner_weight 
            × resolution_confidence × signal_weight × lag_penalty 
            × regime_weight
```

| Factor | Values | Rationale |
|--------|--------|-----------|
| Base value | Geometric mean of amount range (e.g., √(15001 × 50000)) | Congressional filings report ranges, not exact amounts |
| Staleness | exp(-0.693 × days/60) | 60-day half-life continuous decay |
| Owner weight | Self=1.0, Joint=0.9, Spouse=0.8, Dependent=0.5, Managed=0.0 | Managed accounts excluded (member not making decision) |
| Disclosure lag | ≤30d=1.0, ≤60d=0.85, ≤120d=0.6, >120d=0.3 | Late disclosures have diminished information value |

**Minimum trade value:** Excludes the $1,001-$15,000 bracket (too noisy, 15× range).

---

## Noise Suppression

The system aggressively filters noise. Only ~2-5% of raw transactions produce signals:

| Filter | Transactions Excluded | Rationale |
|--------|----------------------|-----------|
| Sell direction (insider) | ~3,800 | 41-47% accuracy — worse than random |
| Non-signal assets (congress) | ~5,500 | Bonds, ETFs, mutual funds, crypto, treasuries |
| Below minimum value | ~5,800 | Insider <$10K, Congress lowest bracket |
| Low resolution confidence | ~1 | Ticker passthrough resolved 99.98% |
| 10b5-1 planned trades | Discounted 95% | Near-zero predictive power |
| Managed accounts | Weight=0.0 | Member not making the decision |
| Single-transaction entities | Marked insufficient | One trade is not a signal |

---

## Validation Results

### Forward-Return Analysis (2025 data)

| Source | Direction | 5d Accuracy | 20d Accuracy | 60d Accuracy | N |
|--------|-----------|-------------|--------------|--------------|---|
| Insider | BUY | **79%** | 68% | 68% | 19 |
| Insider | SELL | 45% | 47% | 42% | 2,414 |
| Congress | BUY | 59% | 63% | **64%** | 811 |
| Congress | SELL | 47% | 45% | 36% | 526 |

### Trivial Baseline

The "predict bullish if anyone bought" baseline achieves 79% at 5 days. The scoring model's value is in filtering, not weighting.

### Signal Stability (Backtest, H2 2025)

- **Mean flip rate:** 2.15% — signals almost never change direction between months
- **Mean turnover:** 2.19% — 98% of signals persist between consecutive months
- **Consistency:** Signal counts are identical across all 6 monthly dates (deterministic scoring)

---

## Daily Brief Sections

The daily brief produces up to 10 sections:

### 1. Cluster Insider Buying
Multiple unique C-suite insiders buying the same stock within 30 days. The highest-quality signal — academic evidence shows 2%+ abnormal returns within a month.

### 2. Cross-Source Signals
Entities where both insider and congressional trading activity overlaps. Classified by overlay outcome (aligned bullish/bearish, conflict) and strength tier.

### 3. Strong Insider Buys
Individual bullish insider signals with confidence ≥ 0.4 and ≥ 2 qualifying transactions. Deduplicated by ticker across lookback windows.

### 4. Strong Congressional Buys
Same criteria applied to congressional signals.

### 5. Anomaly Alerts
Tickers with unusual insider buying relative to historical baseline. Flags first-time buys in 12+ months or elevated activity exceeding 2× the historical monthly average.

### 6. Insider Participation Index
Market-level breadth indicator: what % of S&P 500 companies have insider buying in the last 90 days? Above historical average = bullish context, below = bearish. This turns the system from a stock picker into a market sentiment indicator.

### 7. Pre-Earnings Insider Buys
Insider purchases within 30 days of known earnings dates, flagged as high-conviction. If a CEO buys their stock 2 weeks before earnings, they're confident.

### 8. Committee Rotation Alerts
Detects when members of a congressional committee collectively shift from buying to selling (or vice versa) in the sectors their committee regulates. This is the "political information advantage" signal.

### 9. Sector Summary (optional)
Net bullish/bearish sentiment by GICS sector, aggregated from all signal sources. Uses yfinance for sector classification with local caching.

### 10. Committee-Correlated Trades (optional)
Trades where the member's committee jurisdiction matches the stock's GICS sector. Uses congress.gov API data with 90-day caching.

---

## Known Gaps

### Data Gaps

1. **Sparse insider buy sample.** Only 34 unique tickers with qualifying insider buys across the S&P 500 universe. Most companies have only sell activity (which we correctly exclude). This limits the system's stock-picking breadth.

2. **Congressional disclosure delay.** PTRs can be filed 45+ days after execution, and many are filed late. By the time we see a congressional trade, the market may have already moved. The disclosure lag penalty mitigates but cannot eliminate this.

3. **Amount range imprecision.** Congressional filings report ranges ($1,001-$15,000, $15,001-$50,000, etc.), not exact dollar amounts. The geometric mean estimator is the best available approximation but introduces estimation noise.

4. **S&P 500 universe only.** The system currently covers only S&P 500 companies. Mid-cap and small-cap companies may have more informational insider trading (less analyst coverage = larger information advantage).

### Analytical Gaps

5. **No options analysis.** Insider options exercises, congressional option purchases, and options flow data are not analyzed. Options can signal direction more strongly than stock trades but add complexity.

6. **No short interest cross-reference.** When insiders buy while short interest is high, the conviction signal is stronger. This data source is not integrated.

7. **No earnings surprise correlation.** The earnings proximity signal flags pre-earnings buys but doesn't track whether those earnings were actually positive surprises.

8. **No sector-relative analysis.** A defense stock rising 5% when the defense sector is up 8% is underperformance, not a successful prediction. Sector-adjusted returns would be more accurate than raw returns.

---

## Known Blind Spots

### Structural Blind Spots

1. **Survivorship bias.** The canonical entity CSV and S&P 500 universe exclude delisted, merged, or bankrupt companies. The most dramatic insider selling (before a bankruptcy) would be in companies that subsequently left the index.

2. **10b5-1 plan gaming.** Some insiders structure 10b5-1 plans strategically (setting them up when they have private information, then appearing "routine"). Our 95% discount treats all 10b5-1 trades as noise, but sophisticated insiders may exploit this.

3. **Family member trades.** Trades by "spouse" or "dependent" may actually be directed by the insider. Our owner weights (spouse=0.8, dependent=0.5) attempt to account for this but the true attribution is unknowable.

4. **Filing amendments.** Amended PTRs may correct or supersede earlier filings. The system doesn't deduplicate or reconcile amendments, potentially double-counting some trades.

### Market Structure Blind Spots

5. **Market regime sensitivity.** Insider buying in a bear market is more informative than in a bull market (academic evidence). Our regime weighting attempts to capture this but the effect is small at current conservative weight levels (+/- 10%).

6. **Sector concentration.** Insider buying clusters in certain sectors (biotech, small-cap tech). Without sector adjustment, the system may appear to predict well simply because it's overweight in high-momentum sectors.

7. **Macro correlation.** In strong bull markets, all insider buys look correct regardless of information content. The validation accuracy numbers (70-80%) may partly reflect bull market beta rather than true alpha.

---

## Operational Architecture

### Daily Pipeline

```
06:00 daily via jobctl (launchd)
  ├── Score cached insider XMLs (15,091 files)
  ├── Score cached House PDFs (526 files)
  ├── Score cached Senate HTML (157 files)
  ├── Build combined overlay
  ├── Generate daily brief (10 sections)
  ├── Save brief to /tmp/disclosure-monitor-sp500-v2-logs/
  └── Email via iCloud+ SMTP to configured recipients
Duration: ~4 minutes
```

### Data Refresh

SEC EDGAR ingestion (new filings) is a separate manual process:
```bash
signals run --csv /tmp/live_universe_sp500.csv \
  --sec-user-agent "DisclosureSignals/2.0 (email@domain.com)" \
  --insider-cache-dir /tmp/disclosure-monitor-sp500-insider/ \
  --congress-cache-dir /tmp/disclosure-monitor-sp500-congress/
```
Takes 4-8 hours for 503 companies due to SEC rate limiting (10 req/s). Idempotent — only downloads new filings not already cached.

### Web Dashboard

```bash
signals serve --port 8001 --db /tmp/disclosure-monitor-sp500-v2.db
```
- `GET /` — Daily brief as styled HTML
- `GET /api/brief` — Brief as JSON
- `GET /api/signals` — Signal results with filtering

---

## CLI Reference

| Command | Description |
|---------|-------------|
| `signals run` | Full pipeline (ingest + score + overlay) |
| `signals brief` | Generate daily brief (`--sectors`, `--committees`) |
| `signals validate` | Forward-return validation (`--baseline`, `--regime`) |
| `signals backtest` | Historical time-series backtest |
| `signals serve` | Web dashboard on localhost |
| `signals status` | Database status summary |
| `signals doctor` | Workspace health checks |
| `signals insider rewrite-run` | Insider-only ingest + scoring |
| `signals congress rewrite-run-house` | House PTR ingest + scoring |
| `signals congress rewrite-run-senate` | Senate PTR ingest + scoring |
| `signals combined build` | Build cross-source overlay |

---

## Configuration

### `~/.local/jobs/.env.signals`

```bash
SEC_USER_AGENT=DisclosureSignals/2.0 (email@domain.com)
REPO_ROOT=/Users/mw/disclosure-signals
UNIVERSE_CSV=/tmp/live_universe_sp500.csv
INSIDER_CACHE=/tmp/disclosure-monitor-sp500-insider/
CONGRESS_CACHE=/tmp/disclosure-monitor-sp500-congress/
DERIVED_DB=/tmp/disclosure-monitor-sp500-v2.db
ARTIFACTS_DIR=/tmp/disclosure-monitor-sp500-v2-logs/
SMTP_HOST=smtp.mail.me.com
SMTP_PORT=587
SMTP_USER=<icloud-email>
SMTP_PASSWORD=<app-password>
EMAIL_FROM=Disclosure Signals <email@icloud.com>
EMAIL_RECIPIENTS=recipient@gmail.com
```

### `data/.env` (Congress API)

```bash
CONGRESS_API_KEY=<congress.gov-api-key>
```

---

## Future Development

### High Priority

1. **Russell 2000 expansion.** The DB schema is generic — only a new universe CSV is needed. Initial ingest: ~5.5 hours. Would dramatically increase the insider buy sample from 34 tickers to potentially 200+.

2. **Ingest/scoring separation.** The daily job should always score cached files (fast, reliable). A separate weekly job should run the full SEC ingest (slow, rate-limited). Currently the daily job runs scoring-only; ingest is manual.

3. **Options flow integration.** Cross-reference insider stock purchases with unusual options activity (call volume spikes, put-call ratio shifts). Academic evidence shows options markets price insider information before stock markets.

### Medium Priority

4. **Sector-adjusted returns.** Validate signal accuracy using sector-relative returns rather than absolute returns. A tech stock up 5% when XLK is up 8% is a miss, not a hit.

5. **Earnings surprise tracking.** The earnings proximity signal flags pre-earnings buys but doesn't close the loop — did the earnings actually beat? Cross-reference with actual EPS surprises.

6. **Short interest cross-reference.** When insiders buy while short interest is elevated, the conviction signal is amplified. Short interest data is available from FINRA with a 2-week delay.

7. **Vesting schedule arbitrage.** Distinguish between forced RSU vesting sales (noise) and discretionary purchases made despite upcoming vesting events (high conviction).

### Experimental

8. **Insider Participation Index as a macro indicator.** Currently shows 1.4% of S&P 500 with insider buying. Track this over time to detect market-level regime changes. Divergences between insider breadth and price breadth may predict reversals.

9. **Committee sector rotation as a policy signal.** When Banking Committee members collectively shift from buying to selling financials, this may indicate upcoming regulatory tightening. This is the system's most unconventional signal.

10. **Machine learning for insider intent.** Train separate models per sector to distinguish informational buys from routine buys. Input features: role, trade size, historical trading pattern, time since last buy, sector momentum, options flow.

---

## Testing

- **113 passing tests** across 29 test files (+ 1 pre-existing environment-specific failure)
- Covers: parsing, scoring, resolution, overlay, pipeline, validation, daily brief, web dashboard, committees, backtest
- 1 pre-existing failure (`test_legacy_references_are_quarantined` — `rg` shell shim issue)
- Run: `.venv/bin/python -m pytest tests/ -k "not test_legacy_references"`

## Version History

| Version | Date | Changes |
|---------|------|---------|
| passthrough1 | 2026-04-05 | Ticker passthrough resolution (99.98% exclusion reduction) |
| nosell1 | 2026-04-05 | Eliminate sell signals (DIRECTION_WEIGHT_SELL → 0.0) |
| conviction1 | 2026-04-05 | Cluster conviction scoring (25% per additional buyer) |
| regime1 | 2026-04-04 | Market regime weighting (experimental, ±10%) |
| quality2/3/4 | 2026-04-04 | Noise reduction: min value, lag penalty, smooth staleness |
| quality1 | 2026-04-04 | Min transaction threshold, managed=0, balance_factor removed, 10b5-1=0.05 |
