# House OCR Decision

## Decision
Native House OCR and paper-filing support is worth the next tranche.

## Why
- The native direct House path currently covers only text-extractable PDFs.
- Recent live direct runs showed a material skip rate in the House branch.
  - Example direct run: `10` House PDFs processed, `6` skipped at score time.
- That means the current direct House pipeline is operationally useful but still misses a meaningful slice of real filings.

## Current state
- Native and direct:
  - House connector
  - House text-PDF parser
  - House scoring path
- Not yet native:
  - OCR-backed paper House filings
  - scanned/no-text House PDFs

## Recommendation
Proceed with a bounded House OCR tranche if the goal is broader direct-path coverage.

Do not treat it as a broad “document AI” rewrite. Keep it narrow:
1. add OCR only for no-text House PDFs and `822*` paper filings
2. preserve current text-PDF path unchanged
3. add fixture-backed parity tests for OCR outputs
4. keep explicit skip reasons and skip-rate diagnostics

## Exit criteria
- native House OCR path reduces skipped House filings materially on the same live sample windows
- direct-vs-legacy parity is acceptable on frozen OCR fixtures
- skip-reason diagnostics show a lower share of `paper_filing` and `no_extractable_text`
