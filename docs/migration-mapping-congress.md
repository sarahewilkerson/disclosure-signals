# Migration Mapping: Congress

## Legacy Source
- Database and module set under `legacy-congress`

## Vertical Slice Mapping

### Filing identity
- PTR HTML filename stem -> `source_filing_id`
- owner field -> `owner_type`
- ticker/asset name -> entity resolution inputs

### Transaction fields
- transaction date -> `execution_date`
- amount range -> `amount_low`, `amount_high`, `amount_estimate`
- owner -> `owner_type`
- transaction type -> `transaction_type` + `direction`
- ticker and asset name -> `ticker`, `issuer_name`, `entity_key`

### Transform notes
- Vertical slice uses `EntityResolver` from the legacy congress package.
- Missing insider counterpart is recorded as a combine block, not an error.

