# Reason Codes

## Exclusion Reason Codes

- `LOW_RESOLUTION_CONFIDENCE`
- `MISSING_TICKER`
- `AMBIGUOUS_ENTITY_MATCH`
- `DUPLICATE_FILING`
- `AMENDMENT_SUPERSEDED`
- `UNSUPPORTED_TRANSACTION_TYPE`
- `OCR_PARSE_LOW_CONFIDENCE`
- `ENTITY_ROLE_EXCLUDED`
- `NON_SIGNAL_ASSET`
- `BELOW_MINIMUM_VALUE`

## Combine Block Reason Codes

- `MISSING_COUNTERPART`
- `LOW_RESOLUTION_CONFIDENCE`
- `AMBIGUOUS_ENTITY_MATCH`
- `LOW_SOURCE_CONFIDENCE`

## Resolution Match Types

- `ticker` — Canonical CSV ticker match (confidence 0.99)
- `cik` — Canonical CSV CIK match (confidence 0.97)
- `ticker_passthrough` — Caller-provided ticker trusted when not in canonical CSV (confidence 0.95)
- `name` — Canonical CSV name alias match (confidence 0.90)
- `ambiguous_name` — Multiple canonical name matches (confidence 0.40)
- `none` — No match found (confidence 0.0)

## Warning/Error Codes

- `NETWORK_RETRY`
- `PARSE_FAILED`
- `SCHEMA_MISMATCH`
- `RUN_INCOMPLETE`

