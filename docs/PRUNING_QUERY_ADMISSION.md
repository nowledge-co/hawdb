# Out-of-core pruning workspace admission

The private candidate builder now carries its existing query working account
through predicate grouping and per-field pruning. No account is created per
field or segment: the two-account query root remains shared with candidate
buffers and retained lexical/vector scores.

## Scope and semantics

One admitted, sorted array borrows predicate references for the duration of
candidate construction. It replaces rebuilding a field-name map and predicate
vectors for every segment. Parsed predicates and report fields remain owned by
their existing callers; borrowing does not claim to account for those owners.

For each field, the pruner first evaluates the existing search-summary checks
and updates the field's report counters. Every field is observed even if an
earlier field has already ruled the segment out. Storage refinement is needed
only while the segment might still match. It uses the existing `SegmentPruner`
and scalar conversion, with one referenced field's `FieldSummary` at a time.
The legacy all-field path and the new path share the same field-summary builder.

Unreferenced fields are not copied. Value dictionaries are constructed only
for equality/IN refinement, not for range, presence or missing checks. NOT IN
continues to use the existing search-summary semantics with no storage
translation. This does not add an index, filter operator or optimizer feature,
and it does not change report meanings or public interfaces.

## Live-phase envelope

All additions and multiplications are checked. Admission precedes the relevant
allocation or normalization boundary:

- The retained reference array reserves `predicate_count * size_of::<&Predicate>()`
  before allocation and in-place sorting. Its owner survives query-handle drop.
- String comparison reserves 12 times the largest simultaneous actual/expected
  byte lengths plus 1 KiB. The same conservative Unicode-normalization envelope
  is used by candidate filtering. Range/presence checks do not inspect or charge
  an unused string dictionary merely to size this phase.
- One-field storage materialization reserves 16 `(String, FieldSummary)` slots
  plus field bytes and 512 bytes for B-tree node/link/header slack. A one-entry
  B-tree can allocate a complete leaf, not just its one live entry.
- When needed, each dictionary value reserves the existing 1 KiB set-entry
  allowance plus 12 times its byte length, covering normalized Value/ScanScalar
  conversion and set construction slack.
- The largest translated predicate reserves its property bytes, normalized
  scalar bytes and simultaneous Value/PruningDecision vector slots, plus the
  predicate slot and 1 KiB fixed slack. IN evaluation's decision vector remains
  live alongside its values and the field summary.

Comparison scratch is released before storage materialization. The field
summary and translated predicate are dropped before releasing their workspace
lease. Peak scratch is therefore bounded by the largest field/phase rather than
the sum of all segment dictionaries. Repeated segments retain only the reference
array between evaluations. Errors and cancellation release the same leases.

These are conservative requested-capacity/container envelopes, not allocator or
process-RSS measurements. The summaries here never construct exact-row bitmaps
or membership filters; adding either requires extending the envelope before use.

## Verification

Normal regressions cover reference-array pre-admission/lifetime, independent
one-field overlap arithmetic, exact/one-short budgets with competing owners,
Unicode comparison denial, unrelated/unused dictionaries, duplicate fields,
later-field reporting after rejection, empty/unsatisfiable/NOT IN cases and
128 successive segment evaluations with a constant peak. Cancellation releases
the retained workspace. A candidate-builder integration test proves denial
before payload I/O, removal of only the query spill, unchanged publication bytes
and successful reader reopen.

The manual seed `0x206f111e` campaign checks 512 generated document/predicate
groups against the preceding all-field pruning and reporting path. It includes
Unicode/NUL, missing values, equality, IN/NOT IN, numeric/timestamp ranges,
presence, kind aliases, duplicate fields and competing result owners. Every
case retries exact/one-short peak budgets and releases tracked capacity while
retaining the two-account bound. This is legacy decision/report parity, not an
independent proof of every existing predicate's semantics.

```bash
cargo test -p skein-search --all-features pruning_memory -- --nocapture
cargo test -p skein-search --all-features pruning_budget_rejection -- --nocapture
cargo test -p skein-search --all-features pruning_admission_campaign \
  -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Six negative controls must fail assertions: omitted reference-array charge,
comparison charge or summary charge; copying unrelated fields; stopping report
observation early; and bypassing admission at the real candidate-builder call
site. Restore all controls before positive verification. The new fuzz target
is manual and ignored in ordinary runs; no default/dedicated CI fuzz is added.

## Remaining #206 boundaries

This admits pruning's transient workspace, not its parsed-input or report
ownership. Filter/ACL parsing, the field-report accumulator, report copies in
retriever/public results and matched-span scratch still need complete
owners. Public output and projection backend admission/retained-hit contracts
still require the pending API decision. Reader/mapping host ownership, persistent
delta, component limits, Jieba, representative-corpus reduction and native
qualification remain separate full-issue gates. v1 bytes, dependencies, I/O
backends and Bazel runtime/timeout settings are unchanged.

Private ranking/fusion scratch and the hydration candidate page now retain
their own shared-root charges (`RANKING_QUERY_ADMISSION.md`), independently of
the still-unowned public report and hydrated-output lifetimes.
Hydration raw/text/document containers now retain internal operation charges
(`HYDRATION_QUERY_ADMISSION.md`); this does not extend them past the public
plain-document return boundary.
