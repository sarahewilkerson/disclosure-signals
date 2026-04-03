# Legacy Dependency Audit

## Summary
The default direct workflow no longer depends on `legacy-insider` or `legacy-congress` for:
- insider live ingest
- insider XML parse/score
- House direct ingest
- House direct parse/score for the current text/OCR path
- Senate direct ingest
- Senate direct parse/score
- combined overlay

The remaining legacy dependencies are now explicit and non-default.

## Remaining Dependencies in `src/signals`

### Explicit legacy compatibility surfaces
- `src/signals/cli.py`
  - `insider ingest`
  - `insider parse`
  - `insider classify`
  - `insider run-legacy`
  - `insider score`
  - `congress init`
  - `congress ingest`
  - `congress parse`
  - `congress score`
  - top-level `run --legacy`
- `src/signals/core/legacy_subprocess.py`
- `src/signals/core/legacy_loader.py`

### Legacy-backed service/repository paths
- `src/signals/insider/service.py`
- `src/signals/congress/service.py`

### Legacy bridge modules
- `src/signals/insider/legacy_bridge.py`
- `src/signals/congress/legacy_bridge.py`

### Legacy-backed slice/test helpers
- `src/signals/insider/slice.py`
- `src/signals/congress/slice.py`
- `tests/test_unified_legacy_workflows.py`
- `tests/test_engine_parity.py`
- `tests/test_flow_parity.py`

### Legacy fixtures still referenced by rewrite/parity tests
- `legacy-insider/tests/fixtures/form4_simple_buy.xml`

## Direct-Path Defaults Removed
The following direct CLI defaults no longer point into legacy cache locations:
- `insider rewrite-score --xml-dir`
- `congress rewrite-score-house --pdf-dir`

Both now default to `data/rewrite_cache/...`.

## Retirement Gates
Legacy folders should not be archived or deleted until all of the following are true:

1. No default operator command depends on legacy code or legacy DBs.
2. Legacy compatibility commands are either:
   - removed, or
   - moved behind an explicit compatibility plugin/module boundary.
3. Parity tests no longer import legacy runtime modules directly.
4. Required fixtures used by rewrite/parity tests are copied into non-legacy test fixture locations.
5. `rg "legacy-insider|legacy-congress|run_legacy_cli|legacy_bridge|legacy_loader|legacy_subprocess" src tests`
   returns only:
   - this audit document
   - intentional archive references
   - optional compatibility shims slated for removal.

## Recommended Removal Order
1. Copy remaining legacy-owned fixtures into `tests/fixtures`.
2. Replace parity tests that import legacy runtime modules with frozen expected-output fixtures.
3. Remove legacy-backed slice helpers.
4. Remove `src/signals/*/service.py` legacy import paths once no longer needed.
5. Remove legacy CLI compatibility commands.
6. Archive or delete `legacy-insider` and `legacy-congress`.

## Current Assessment
The system is operationally direct by default, but not yet purge-ready. The remaining legacy code is now mostly:
- compatibility surface
- parity/reference surface
- non-default fallback surface

That is a good place to be, but it is not yet equivalent to zero dependency.
