# DTO Spec

## Unknown and Null Policy

- `null` means the value is unavailable after a valid attempt to determine it.
- `"UNKNOWN"` means the field is expected conceptually but the source did not provide enough information.
- `"NOT_APPLICABLE"` means the field does not apply to this record type.
- Free-form missing semantics are not allowed.

## `NormalizedTransaction`

Required fields:
- `source`
- `source_record_id`
- `source_filing_id`
- `actor_id`
- `actor_name`
- `actor_type`
- `owner_type`
- `entity_key`
- `instrument_key`
- `ticker`
- `issuer_name`
- `instrument_type`
- `transaction_type`
- `direction`
- `execution_date`
- `disclosure_date`
- `amount_low`
- `amount_high`
- `amount_estimate`
- `currency`
- `units_low`
- `units_high`
- `price_low`
- `price_high`
- `quality_score`
- `parse_confidence`
- `resolution_confidence`
- `resolution_method_version`
- `include_in_signal`
- `exclusion_reason_code`
- `exclusion_reason_detail`
- `provenance_payload`
- `normalization_method_version`
- `run_id`

## `SignalResult`

Required fields:
- `source`
- `scope`
- `subject_key`
- `score`
- `label`
- `confidence`
- `as_of_date`
- `lookback_window`
- `input_count`
- `included_count`
- `excluded_count`
- `explanation`
- `method_version`
- `code_version`
- `run_id`
- `provenance_refs`

## `CombinedResult`

Additional required fields:
- `agreement_state`
- `insider_score`
- `congress_score`
- `insider_confidence`
- `congress_confidence`
- `entity_resolution_confidence`
- `combine_method_version`
- `do_not_combine_reason_code`
- `do_not_combine_reason_detail`

