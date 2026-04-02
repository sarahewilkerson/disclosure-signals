# Versioning Policy

- `code_version` is the git SHA of the monorepo at run time.
- `normalization_method_version`, `resolution_method_version`, `score_method_version`, and `combine_method_version` are explicit code constants.

The relevant method version must change when:
- a scoring formula changes
- inclusion or exclusion logic changes
- normalization rules change
- entity matching rules change
- persisted explanation semantics change

