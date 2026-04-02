# Interfaces

This document freezes the service boundaries used by the first implementation slice.

## Services

### `IngestionService`
- Input: source-specific location or fixture path.
- Output: raw artifact metadata with stable source identifiers.
- Side effects: may write raw cached artifacts.
- Idempotency key: `(source, source_identifier, content_hash)`.
- Failure modes:
  - retriable network failure
  - fatal storage failure
  - row-scoped malformed source response

### `ParsingService`
- Input: raw artifact metadata.
- Output: parsed source-specific artifact DTO.
- Side effects: none in the vertical slice.
- Idempotency key: `(source_record_id, parse_artifact_hash, parser_version)`.
- Failure modes:
  - row-scoped malformed artifact
  - fatal parser contract mismatch

### `NormalizationService`
- Input: parsed source artifact DTO.
- Output: list of `NormalizedTransaction`.
- Side effects: writes normalized rows to derived persistence.
- Idempotency key: `(parsed_record_id, normalization_method_version)`.

### `EntityResolutionService`
- Input: source-specific asset identity fields.
- Output: canonical `entity_key`, optional `instrument_key`, ticker, confidence, reason codes.
- Side effects: none; persisted by normalization caller.

### `ScoringService`
- Input: normalized transactions plus source-specific evidence.
- Output: `SignalResult`.
- Side effects: writes results to derived persistence.
- Idempotency key:
  - source: `(source, subject_key, as_of_date, lookback_window, method_version, input_fingerprint)`
  - combined: `(entity_key, as_of_date, combine_method_version, insider_result_ref, congress_result_ref)`

### `ReportingService`
- Input: persisted source results and combined overlay results.
- Output: stable text and JSON renderings.
- Side effects: writes report artifacts.

### `RunTracker`
- Input: run type, source, params, versions.
- Output: persisted run row with stable run id.
- Side effects: writes run state transitions and summary metrics.

## Repository Boundaries

- Raw repositories stay source-specific inside `legacy-insider` and `legacy-congress`.
- Derived repositories live under `signals.core`.
- `combined` reads only persisted derived outputs, never raw domain internals.

## Dependency Rules

- `signals.core` may not import `signals.insider`, `signals.congress`, or `signals.combined`.
- `signals.insider` and `signals.congress` may depend on `signals.core`, but not on each other.
- `signals.combined` may depend on persisted derived outputs and shared DTOs, but not legacy raw internals.

