# Search Feature Test Contracts

Optional capabilities are admission boundaries, not a reason to suppress all
storage and analyzer tests. `skein-search` supports independent
`full-text-search`, `vector-search`, and `background-maintenance` selections.
Runtime requests are intersected with compiled availability: callers cannot
enable a capability that was not compiled.

## Coverage Ownership

- Positive text, vector, hybrid, and background/QoS integration tests declare
  their exact required features. Hybrid serving requires both text and vector.
- The existing tokenizer-named search tests remain positive end-to-end text
  tests. Separate direct analyzer checks cover identifiers, case folding, stems,
  CJK dictionary terms, normalized/application aliases, and stopwords without
  going through search admission.
- Feature-independent generation publication, complete hydration, corruption,
  immutable-generation pinning, retention cleanup, and scalar cosine tests
  remain executable in minimal builds. Optional RaBitQ assertions reflect
  whether its artifact can exist; canonical vector payloads still round-trip.
- `tests::feature_contract` exercises 96 resident/out-of-core admission cases
  per configuration, including text/vector/hybrid modes, runtime capability
  overrides, empty queries, and zero limits. Disabled modes return typed errors;
  they cannot bypass admission through empty results.
- Background delta tests exercise direct and scheduled admission. Denial must
  preserve the existing document and source epoch and leave no scheduler work
  reservation. Probe diagnostics distinguish unavailable artifacts from a
  feature that was not compiled.

No production behavior, public API, v1 format, or capability policy changes are
needed for this coverage. No test uses `ignore`, an early successful return, or
a forced capability override to make an unsupported positive path succeed.

## Local Feature Matrix

The baseline for issue [#313](https://github.com/nowledge-co/skein/issues/313) is
main `09fa6f616f180616fbd12a902ee2bf557a31eaa6`: minimal search tests reported
63 passed and 107 failed because optional features were assumed by tests.
The corrected matrix has these unit-test counts (documentation tests are
reported separately and are not counted as executed unit coverage):

| Features | Tests | Bazel target suffix |
| --- | ---: | --- |
| Default | 181 | `tests` |
| None | 74 | `minimal_tests` |
| Text only | 140 | `text_only_tests` |
| Vector only | 90 | `vector_only_tests` |
| Background only | 85 | `background_only_tests` |
| Text + vector | 170 | `text_vector_tests` |
| Text + background | 151 | `text_background_tests` |
| Vector + background | 101 | `vector_background_tests` |

For Cargo, select one feature combination per command so feature unification
does not silently turn a minimal test into an all-feature test:

```bash
cargo test -p skein-search
cargo test -p skein-search --no-default-features
cargo test -p skein-search --no-default-features --features full-text-search
cargo test -p skein-search --no-default-features --features vector-search
cargo test -p skein-search --no-default-features --features background-maintenance
cargo test -p skein-search --no-default-features --features full-text-search,vector-search
cargo test -p skein-search --no-default-features --features full-text-search,background-maintenance
cargo test -p skein-search --no-default-features --features vector-search,background-maintenance
```

Every Bazel variant compiles the crate's test source directly with its feature
selection, rather than depending on the default-feature `skein_search` library.
The targets have prefix `//crates/search:skein_search_`. They are ordinary search
tests, not new runtime libraries or production integration points. Development
dependencies used by fixtures are not evidence about release link exclusion.

```bash
bazel test //crates/search:all \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Keep the fuzz targets local-only. Adding search feature tests does not authorize
adding fuzz to default or dedicated CI jobs.

## CI Ownership

The existing Weekly Platform CI runs all eight Cargo configurations in its
four-platform job: Linux x86_64 and arm64, macOS arm64, and Windows x86_64. Each
configuration is a separate Cargo invocation, and any failure stops the step.
The workflow retains its schedule-only trigger and adds no fuzz invocation.

Prow's current explicit target lists do not include the search feature matrix.
A green presubmit result is not evidence that these configurations executed
remotely. Before merge, retain the local matrix evidence separately; the weekly
workflow validates the merged default-branch revision.
