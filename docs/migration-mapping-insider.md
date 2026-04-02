# Migration Mapping: Insider

## Legacy Source
- Database and module set under `legacy-insider`

## Vertical Slice Mapping

### Filing identity
- `filing.accession_number` -> `source_filing_id`
- `filing.cik_owner` -> `actor_id`
- `filing.owner_name` -> `actor_name`
- `filing.cik_issuer` -> `entity_key` via fixture mapping in the vertical slice

### Transaction fields
- `transaction.transaction_date` -> `execution_date`
- `filing.filing_date` -> `disclosure_date`
- `transaction.transaction_code` -> `transaction_type` + `direction`
- `transaction.total_value` -> `amount_estimate`
- `transaction.shares` -> `units_low` and `units_high`
- `transaction.price_per_share` -> `price_low` and `price_high`

### Transform notes
- Vertical slice uses a small deterministic issuer mapping for known fixture CIKs.
- Unknown ticker remains nullable and blocks combined analysis where applicable.

