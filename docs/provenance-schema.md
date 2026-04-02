# Provenance Schema

All normalized and result rows carry structured provenance.

## Normalized Provenance Payload

- `source_system`
- `raw_record_id`
- `raw_filing_id`
- `stage_timestamps`
- `parser_version`
- `resolver_evidence`
- `source_values`
- `method_versions`

## Result Provenance Refs

- `normalized_row_ids`
- `input_fingerprint`
- `stage_timestamps`
- `method_versions`
- `evidence_summary`

Explanation text is not provenance. Explanation is human-facing and may be regenerated. Provenance must remain machine-usable for audit and parity comparison.

