# Fallible in-memory search wrapper contract

Status: implemented in PR #576 after the owner requested completing the prepared contract.
Issue: [#564](https://github.com/nowledge-co/skein/issues/564).

## Contract rationale

Search capability checks return `SkeinError::CapabilityUnavailable`. The former
infallible in-memory wrappers discarded these errors with `expect`, including
errors from retrieval graph enrichment and pipeline admission. The public chain
now propagates these errors to the caller.

The existing `try_*` methods enable pruned physical range reads. The in-memory
entrypoints below retain resident payload access: after a projection is reopened,
a selective query can still use resident documents even if persisted payload
bytes are subsequently damaged. Changing error carriers must not add I/O to
that path. Embedded-store serving routes already using `try_*` retain their
persisted-aware execution.

## Public return contract

This changes only the return carrier of the following 14 existing public methods from
`T` to the existing crate `Result<T>`. Parameters, method names, generics, options,
backend selection, payload access, ranking and report contents remain unchanged.
No new error variant, result field, setting, capability default, persistent format
or query-language contract is introduced.

| Owner | Method | Return type |
| --- | --- | --- |
| `SearchIndex` | `search` | `Result<Vec<SearchHit>>` |
| `SearchIndex` | `search_with_report` | `Result<SearchResultSet>` |
| `SearchIndex` | `search_with_options` | `Result<SearchResultSet>` |
| `SearchIndex` | `search_with_options_prefer_compressed_vector_projection` | `Result<SearchResultSet>` |
| `SearchIndex` | `search_with_options_compressed_vector_projection_mode` | `Result<SearchResultSet>` |
| `SearchIndex` | `search_with_options_adaptive_vector_projection` | `Result<SearchResultSet>` |
| `Database` | `retrieve_knowledge` | `Result<KnowledgeRetrievalOutput>` |
| `DatabaseReadTransaction` | `retrieve_knowledge` | `Result<KnowledgeRetrievalOutput>` |
| `NowledgeGraphAdapter<'a>` | `retrieve_knowledge` | `Result<KnowledgeRetrievalOutput>` |
| `NowledgeMemSearchProjection` | `search_candidates` | `Result<SearchResultSet>` |
| `NowledgeMemSearchProjection` | `search_candidates_with_report` | `Result<NowledgeMemSearchCandidateOutput>` |
| `NowledgeMemSearchProjection` | `search_candidate_readiness` | `Result<NowledgeMemSearchCandidateReadinessReport>` |
| `NowledgeMemSearchProjection` | `search_candidate_shadow_evidence<I, S>` | `Result<NowledgeMemSearchCandidateShadowEvidence>` |
| `NowledgeMemSearchProjection` | `search_candidate_shadow_evidence_json<I, S>` | `Result<serde_json::Value>` |

The owner wrappers return their existing fallible backend result directly. The
private `KnowledgeRetrievalGraphContext` propagates both search and graph/pipeline
errors. The facade and Mem projection wrappers use `?`/`map` through the whole
chain. Already-fallible embedded-store/handle methods propagate the result rather
than wrapping a new nested `Result` in `Ok`.

An unavailable requested capability returns `CapabilityUnavailable` before token
analysis, physical search reads or graph enrichment. Hybrid mode preserves the
existing full-text-then-vector validation order and requires both capabilities.
No disabled retriever is silently enabled, dropped or replaced by a successful
empty/partial result. Other backend or enrichment errors are propagated too.

Existing `try_*` methods keep their signatures and persisted-aware execution.
The additional `try_*` names are retained for compatibility with those existing
callers; a wider naming or read-policy redesign is outside this correction.

## Caller adjustment and alternatives

Callers expecting a successful result must now handle `Result`, normally with
`?`. Tests/fixtures whose compiled feature assumptions guarantee success can
unwrap explicitly at their assertion boundary. Function pointers and generic
adapters expecting the previous return type must be adjusted. The error carrier
change is source-breaking even though successful query behavior is preserved.

An additive alternative would add fallible in-memory twins and document or
deprecate the old methods. It would avoid immediate caller changes, but leave
reachable panics in the same public methods and increase an already duplicated
API family. Merely routing the old wrappers through current `try_*` methods also
changes payload access and does not remove an outer infallible error boundary.
Returning empty reports would hide failure, especially through the Vec-only
`search` wrapper, and is not an acceptable contract.

Given the development-only library state and the goal of fixing supported entry-
point panics, the direct `Result` transition is recommended. This approval does
not authorize removing or consolidating other public APIs.

## Regression coverage

The search owner tests exercise all six public entrypoints with disabled runtime
and compiled capabilities, including hybrid error ordering and empty/zero-limit
requests. They retain enabled-result parity with the pre-existing internal
in-memory execution controls and distinguish selective corrupt-payload behavior
from the existing persisted-aware `try_*` route.

`tests/fallible_search_contract.rs` imports the public embedded facade. Default
and minimal Bazel targets cover database/read-transaction/adapter retrieval,
direct candidate/readiness/shadow APIs, and embedded stores and handles. Search
capability rejection precedes pipeline admission; admitted search propagates
pipeline result-budget errors. Failed readiness and shadow operations return
errors, and embedded handle permits are released. Enabled profiles compare
complete retrieval outputs, candidate reports, readiness and shadow evidence.

Existing successful callers handle `Result`; examples propagate it, assertion
fixtures unwrap it, and fuzz validation converts errors to rejected evidence.
The private workload correction was delivered separately in #594.

Validation entrypoints:

```sh
cargo check --workspace --all-targets --locked
cargo test -p skein --test fallible_search_contract --locked
cargo test -p skein --no-default-features --test fallible_search_contract --locked
bazel test //:skein_fallible_search_contract_tests //:skein_fallible_search_contract_minimal_tests //crates/search:skein_search_tests //crates/search:skein_search_minimal_tests
bazel test //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
```

The broader minimal root suite has historical fixtures that assume optional
features are enabled. This correction does not disable those tests or claim
that every unrelated minimal-profile fixture now passes. Full qualification
results are bound to the final PR source and recorded in its review.
