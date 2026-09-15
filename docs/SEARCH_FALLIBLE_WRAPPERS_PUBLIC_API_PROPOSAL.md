# Fallible in-memory search wrapper contract

Status: proposed for owner review, not approved or implemented as public signatures.
Issue: [#564](https://github.com/nowledge-co/skein/issues/564).
Audited main: `7aa6bff3805ce435710075c82d3db7b45dcade02` (September 15, 2026).
Private worktree: `target/worktrees/564-fallible-search-contract`.

## Problem and verified call paths

Search capability checks already return `SkeinError::CapabilityUnavailable`.
Three public in-memory owner methods discard that error with `expect`, and their
convenience wrappers propagate the panic. Database/read-transaction retrieval
and the direct Mem projection candidate/readiness/shadow APIs inherit it. A
private retrieval wrapper also discards non-I/O errors from graph enrichment and
pipeline/result admission.

The main Mem embedded-store search/retrieval paths already use fallible APIs;
they should keep those routes. Two workload-evidence helpers create in-memory
indexes but call the legacy methods. Those helpers can use existing `try_*`
methods and their existing `ready=false`/`error_class` reports without changing
any public type.

Existing `try_*` APIs are not exact substitutes for the old methods. They enable
pruned physical range reads, while the old wrappers explicitly choose in-memory
payload access. On a reopened projection with a selective metadata predicate,
a damaged persisted payload can fail the former while the latter still uses its
in-memory documents. A repair must preserve that distinction, not introduce new
I/O or change enabled-query results merely to obtain an error carrier.

## Recommended contract

Change only the return carrier of the following 14 existing public methods from
`T` to the existing crate `Result<T>`. Parameters, method names, generics, options,
backend selection, payload access, ranking and report contents remain unchanged.
No new error variant, result field, setting, capability default, persistent format
or query-language contract is introduced.

| Owner | Method | Proposed return type |
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

## Private proof and implementation boundaries

The preparation changes no public signature. Private owner tests exercise the
already-existing fallible backend under the exact in-memory controls, compare
complete enabled results, reproduce the current panic on unavailable capabilities,
and distinguish persisted I/O using a selectively pruned damaged payload.
Private workload helper changes use existing fallible APIs and retain existing
error-report schemas. Both default and minimal-feature verification are required
before asking for the contract decision; results are recorded in the local audit.

The prepared code has nine passing targeted tests across default and minimal
profiles: three/default and one/minimal owner proofs, plus three/default and
two/minimal workload tests. The default proofs include complete enabled-result
equality and the selective damaged-payload read-policy distinction. The minimal
public workload fixture returns an unready report with the existing capability
error class instead of unwinding.

Default facade all-target compilation, strict all-target search Clippy, strict
facade library Clippy and formatting pass. Strict facade library-and-test Clippy
reports `items_after_test_module` in the unchanged `src/api/types/analytics.rs`;
the identical command on clean audited main reproduces the same diagnostic.
That baseline warning is retained rather than suppressed or folded into this fix.
These checks qualify the private preparation, not the unimplemented public API
transition or the complete minimal root suite.

After approval:

1. Propagate the existing `Result` through the 14 methods and their private callees.
2. Adjust every caller and example, including readiness/shadow and function-pointer
   contracts, without changing query text, ranking, capability enforcement or
   in-memory/persisted execution policy.
3. Replace the proposal's legacy-panic observations with regression assertions
   against actual public methods. Cover disabled full text, disabled vector,
   hybrid requirements, read transactions, Mem projection and embedded handles.
4. Preserve all enabled-result/report parity, the selective-corrupt-payload I/O
   distinction and graph/pipeline error propagation. Verify failure performs no
   graph enrichment or successful readiness publication.
5. Complete default/minimal owner tests, facade checks, the default Bazel suites
   and all three required local fuzz targets, then native CI and independent review.

The 52 historical wrapper failures are distinct from the other optional-feature
assumptions among the 166 recorded minimal root failures. This proposal does not
claim those unrelated fixtures are repaired or authorize hiding failures through
feature/test selection changes. Full minimal-suite limitations must remain
explicit in the final evidence.

## Decision gate

Approval is requested for exactly the 14 return-type changes and necessary
caller adjustments above. The owner-supplied AGENTS.md instructions reserve
public API changes for confirmation, and #564 explicitly reserves its return-type
or fallback contract for owner review. Earlier manifest, compact, binder, pin and
consumer-registry approvals cover different interfaces.
