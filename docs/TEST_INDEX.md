# Test ownership and known slow paths

Use this index to select an existing regression owner and understand what a
slow target is checking. Target definitions and CI job configuration remain
authoritative; this is a map of test families, not an exhaustive list of every
Rust test function. Update it when changing a family's scope, build profile,
execution lane, fixture size, isolation or known failure mode.

Checked candidate exhaustion ([#1713](https://github.com/nowledge-co/hawdb/issues/1713))
is owned by `candidate_evidence::consumption::tests` in the existing Search unit
feature matrix, including `//crates/search:hawdb_search_tests`. Text cases
require `full-text-search`; vector/hybrid cases additionally require
`vector-search`; typed refusal diagnostics run in every profile. Two-document
resident and persisted out-of-core fixtures distinguish completed empty queries
from bounded windows, offsets, restricted ranking, unavailable/unknown
retrievers and ANN candidate generation followed by raw reranking.
Six-document fixtures also cover lossy lexical term reduction through resident,
retained-generation and persisted readers, unchanged `MostSelective` controls,
exact-vector hybrid requests, and preservation of bounded results on refusal.
The persisted cases run with and without runtime admission, retaining query
memory metrics and typed working-memory refusal alongside the coverage checks.
The public facade assertions extend `//:hawdb_fallible_search_contract_tests` and
`//:hawdb_fallible_search_contract_minimal_tests` in their existing CI owners.
Snapshot and exact-vector owners retain their existing assertions while
explicitly choosing bounded coverage for top-k. Compile-fail documentation
rejects direct hit-field access through the sealed output.
The compile-fail examples also reject host assembly of a result with an
unrelated request. They have manual ownership: replay with
`cargo test --locked -p hawdb-search --doc SearchCandidateOutput` and
`cargo test --locked -p hawdb --doc SearchProjection::search_candidates`.
Replay the Search family with `--test_arg=candidate_evidence::consumption::`.
No lane, profile, timeout or performance gate changes. See the
[checked consumption contract](specs/CHECKED_CANDIDATE_EXHAUSTION.md).

Mem query-shaped compatibility (`nowledge-co/mem#9338`) is owned by
`api::tests::mem_query_contract` in `//:hawdb_unit_general_tests` (the existing
required native general lane). The finite fixtures cover C1-C5 execution,
NULL/type/arity errors, Entity multiplicity, Space isolation, atomic constant
multi-binding SET, alias ordering, missing bindings, operation/output-memory
refusal, transaction rollback, temporal range preservation (including equal-instant
UTC-offset tie-breaks) and WAL-backed
close/reopen. They retain read CASE/UNWIND, OPTIONAL MATCH aggregation and SQL
comparison controls. Replay `cargo test --locked -p hawdb --lib mem_9338 --
--test-threads=1` or the owning Bazel target with `--test_arg=mem_9338`.
The immutable 1,427-case migration corpus is unchanged; explicit stage overrides
now qualify `mem-0361` and `mem-0420` with supplied parameters, while preserving
historical counts and missing-parameter errors. Core text-scalar and storage
minimum-preservation cases use the existing component test owners.
No new execution lane, profile, fixture amplification or timeout is introduced.
See [the contract and explicit SQL scope](specs/MEM_QUERY_COMPATIBILITY_9338.md).

Snapshot-bound typed Search candidates (`nowledge-co/hawdb#1567`) extend
`embedded_store_app_read_snapshot_tests` in the existing
`//:hawdb_unit_general_tests` owner. The fixture places a Memory behind more
than `k` nearer Source vectors, including a Source with the same external ID,
and proves that projection `kind` filtering happens before vector Top-K while
returning qualified `kind`/`external_id` identities. Resident and out-of-core
projections must return the same hit and freshness epoch before same-snapshot
graph hydration. Separate guards cover cumulative candidate-plus-graph row and
payload refusal, caller cancellation, and graph-only snapshots without a Search
pin; the same task-context guard covers deadline refusal. Replay with
`cargo test --locked -p hawdb --lib typed_snapshot_candidates -- --test-threads=1`
or the owning Bazel target with
`--test_arg=typed_snapshot_candidates`. No new lane, profile, fixture
amplification or timeout is introduced.

Request-level exact vector candidates (`nowledge-co/hawdb#1692`) live in
`embedded_store::exact_candidate_tests` in the same
`//:hawdb_unit_general_tests` owner. A 480-row, 8-dimension resident and
out-of-core fixture interleaves Source rows with `kind=memory` rows. Default
out-of-core snapshot reads must report DiskANN `graph_distance` candidates and
resident reads the adaptive `compression_disabled` scalar scan (already exact);
exact snapshot and handle reads must equal an independent brute-force cosine
top-k (IDs and score bits), report `exact_vector_search`, and keep hybrid
vector scores raw. Projection-level entry points cover the same equality and
refuse a 4 KiB working budget, and out of core a page window above the
admitted score entries, with a typed `ResourceBudgetExceeded` error (variant,
resource and budget asserted, #1748); snapshot reads refuse a 4 KiB
`query_memory_bytes` with a typed `MemoryBytes` error; exact plus an effective
compressed mode is refused. Request admission and report guards are in
`candidate_evidence::boundaries` (`//crates/search:hawdb_search_tests`). Replay
with `cargo test --locked -p hawdb --lib exact_candidate_tests` or
`--test_arg=exact_candidate_tests`. No new lane, profile or timeout.

Embedded out-of-core candidate working memory (`nowledge-co/hawdb#1722`) lives
in `embedded_store::out_of_core_working_memory_tests` (same
`//:hawdb_unit_general_tests` owner), reusing the #1692 fixture. At the
smallest admission serving text, exact vector-only and windowed exact hybrid
snapshot reads (measured at 256 MiB), those reads and default vector-only
succeed within the charge while an unwindowed exact hybrid read is refused with
a typed `MemoryBytes` budget error; at 256 MiB every mode succeeds with a
nonzero `query_memory_peak_bytes` within admission, through both snapshot and
handle. `out_of_core::tests::admitted_working`
(`//crates/search:hawdb_search_tests` and the FTS+vector profiles) asserts with
the live-allocation allocator, not RSS, that the charged peak covers the live
working set (excluding the caller-owned result), that refused queries never
allocate beyond their admission, that exact hybrid is refused one byte below
its charged peak, and that at 20,000 eligible documents exact vector-only and
unwindowed exact hybrid charge between 1x and 2x their live working bytes.
`snapshot_exact_vector_capacity_is_served_and_refused_above` builds 384- and
1024-dimension out-of-core projections at an 8 MiB admission and requires
`EmbeddedReadSnapshot::exact_vector_search_capacity` to equal the duplicate-ID
cap (32,768), an exact vector-only read of exactly that many eligible
documents to succeed and one more to fail with a typed `MetadataEntries`
budget error;
`snapshot_exact_vector_capacity_is_served_and_refused_above_for_resident_reads`
picks the smallest resident admission whose snapshot capacity is exactly 1,500
eligible documents, serves that many untruncated and refuses 1,501 with a typed
`MemoryBytes` budget error (#1748).
`out_of_core::tests::out_of_core_exact_vector_window_and_hydration_caps_are_typed_budget_errors`
(`//crates/search:hawdb_search_tests`) covers the out-of-core page, rank-window,
uniqueness and hydrated-document caps of exact reads.
Caller-mistake refusals are not storage faults (#1754):
`out_of_core::tests::out_of_core_offset_and_limit_overflow_is_a_semantic_error`
asserts `Semantic` for an overflowing `offset + limit` on full and candidate
reads, `public_hydration_preserves_order_budgets_and_old_reader` asserts
`Semantic` for duplicate hydration ids, and
`hydration_byte_refusal_preserves_the_complete_operation_budget` asserts a
typed `MetadataEntries` refusal naming
`SearchOutOfCoreConfig.max_hydrated_documents` for over-limit hydration by id
and mutation-target loading (all `//crates/search:hawdb_search_tests`).
Replay with `cargo test --locked -p hawdb --lib out_of_core_working_memory`
and `cargo test --locked -p hawdb-search --lib admitted_working`. No new lane,
profile or timeout.

The same Mem query owner also covers PR 1504 review optimizations: invariant
regexp_replace validation on empty scans, cache rebinding across pattern and
replacement values (including an invalid-pattern failure), row-dependent patterns,
Unicode whitespace/zero-width Space isolation, adjacent SET row budgets and
aliased writes after a selector property changes. Core `string_function` tests
prove prepared row evaluation does not recompile or mutate source-only cache
owners and that output admission precedes materialization. Literal/parameter
patterns use the existing source-only regex cache lifecycle; dynamic patterns
retain their evaluator path. Replacement sizing admits every piece before a
single exact buffer allocation. Query memory/work caps and mutation atomicity
remain required. Existing fixtures and execution lanes are retained.

Cypher scalar-expression regex matching (`nowledge-co/hawdb#1550`) extends the
existing expression-function and regex-cache owners without adding a target or
lane. `hawdb-cypher` parser tests retain direct-property `=~` and cover generic
string functions, parameter left operands, and parenthesized property/literal
operands, including keyword-shaped parameter names and property keys;
`api::tests::expression_functions` in `//:hawdb_unit_general_tests` covers the
same dispatch paths plus literal and parameter patterns, NULL propagation across
NOT/AND/OR, repeated left/pattern rebinding through success and typed failure,
invalid regexes, deterministic instance fingerprints and exact cache variants,
direct-property compatibility, durable reopen and concurrent reads. The
plan-cache requested-allocation oracle
keeps both property and scalar-expression regex programs source-only while
retained and execution-owned after binding. Replay with `cargo test --locked -p
hawdb-cypher parses_regex_match`, `cargo test --locked -p hawdb --lib
scalar_expression_regex -- --test-threads=1`, and `cargo test --locked -p
hawdb-plan-cache --test template_retention -- --test-threads=1`. A reviewed
44-row parser transition fixture preserves the immutable historical rejection
oracles. Shiro adds generic-function and keyword-identifier positive parser
seeds, a typed plan-differential shape that alternates literal/parameter
patterns, and capability-model references for matching plus
invalid-pattern/non-string refusals. Replay with `cargo test
--locked --manifest-path tools/shiro/Cargo.toml --lib scalar_regex` and the
existing Bazel fuzz suites. Existing regex validation limits, query work/memory
admission and cancellation checkpoints are unchanged; no new target or lane is
introduced.

[Bazel exclusive test scheduling](https://bazel.build/reference/be/common-definitions#common.tags)
places this owner after build activity and non-exclusive tests.
`//crates/executor:hawdb_executor_unit_tests` retains its full ordinary unit-test
inventory and its original default size/deadline, using one `exclusive` target
rather than another executable or a reduced filter. The existing staged-mutation
endpoint oracle keeps all 3,000 Person nodes, duplicate/missing/wrong-type rows,
both index modes, four endpoint-pattern forms and exact reference comparisons.
PR 1504 check run 125124334072 timed out while this oracle was still running;
the visible tail also shows concurrent compilation. The complete remote BEP
was not obtained, so no precise remote per-case duration or speedup is claimed.
At source `1ea024e038`, the complete local owner ran 595 passing/16 existing
ignored cases in 33.124 seconds including runner overhead; an isolated exact
replay passed in 15.42 seconds. Local compile-cache hits are separate from these
execution measurements. The isolation preserves Cargo/Bazel feature selections
and the current fixture, assertions, timeout and compilation profile; external
machine contention remains possible. Replay the existing owner with
`bazel test //crates/executor:hawdb_executor_unit_tests`.

Compressed histogram ownership stays in existing lanes:

- `//crates/core:hawdb_core_tests`: `histogram::tests` owns MCV/residual mass,
  population versus sample NDV, NULL/missing, monotone extreme numeric ranges
  and exact string bucket endpoints.
- `//crates/storage:hawdb_storage_tests`: `statistics::distribution::tests`
  owns deterministic row sampling, byte/target refusal and optional codec
  validation. External refresh's independent legacy bag/set oracle additionally
  verifies duplicate-preserving row counts and sample mass when the budget admits
  a distribution, keeping its deliberately inconsistent relationship-population
  fixture ineligible. Existing cooperative statistics cases compare the new maps,
  including late unsupported-value exclusion and cancellation/retry.
- `//crates/optimizer-graph:hawdb_optimizer_graph_tests`: catalog distribution
  cases cover shared node/relationship skew estimates, NULL-aware complements,
  duplicate IN values, exact point-count precedence and COW statistics snapshots
  shared by query-local overrides.
- `//crates/plan-cache:hawdb_plan_cache_tests`: statistics marker rebinding and
  scalar-count/payload refusal. Existing physical-key retention measurement
  includes statistical bindings under its unchanged isolated owner.
- Existing root plan-cache tests check equal-frequency MCV point classes,
  distinct hot/cold/unobserved classes, generic range templates and the unchanged
  ACL scope-rebinding cache contract. The 8-KiB external recovery fixture compares every legacy field
  and separately checks optional distribution omission versus materialized rebuild.
- `//:hawdb_unit_general_tests`: `api::tests::compressed_histogram` owns hot/cold
  physical-cache variants and complete results, update/delete with old readers,
  external refresh, missing values and checkpoint reopen. Minimal/default
  selections use the existing general owners.

No CI lane or compilation profile changes. Local replay filters are
`histogram::`, `statistics::distribution::`, `catalog::distribution::`,
`statistics_bindings_` and `api::tests::compressed_histogram::` in their respective
Cargo library packages. See [the model and bounds](COMPRESSED_HISTOGRAMS.md).

The optimizer-graph joint-statistics owner also covers a residual filter above
HashJoin when either or both marginal key NDVs are missing. Single-key and
multi-key predicates retain the existing default selectivity; compatible
marginals still avoid double counting an already-estimated hash key. Replay:
`cargo test --locked -p hawdb-optimizer-graph --lib unknown_hash_key_ndv_preserves`.

Joint graph statistics extend the existing
`//crates/optimizer-graph:hawdb_optimizer_graph_tests` owner with bounded,
unordered catalog key sets, invalid/stale/sampled/incomplete statistics,
correlated/independent reference populations, conditional access coverage,
duplicate bindings, NULL-bearing grouping, alias scope, optional rebindings,
single-key hash intermediates and selected-trace provenance. Replay with
`cargo test --locked -p hawdb-optimizer-graph --lib`. Root
`api::tests::joint_ndv_statistics` covers exact rows in auto/memo/direct modes,
pinned readers, warm cache reuse, correlated/independent/skewed keys, NULLs,
duplicate join pairs, optional unmatched rows and deterministic tied sort windows.
It belongs to `//:hawdb_unit_general_tests`; the supported minimal replay is
`cargo test --locked -p hawdb --no-default-features --lib --
api::tests::joint_ndv_statistics:: api::tests::aggregates::
api::tests::statistics:: --test-threads=1`. The default-feature replay omits
`--no-default-features`. Executor `transform::tests::limit_admission` is owned by
`//crates/executor:hawdb_executor_unit_tests`; it checks selected-window payload
admission, skipped wide rows, exhausted offsets and one-byte-deficit refusal.
Replay the complete unchanged transform family with `cargo test --locked -p
hawdb-executor --lib transform::tests`. The root integration keeps a changed
Sort plus LIMIT choice within a 2-MiB query budget. Existing batch/payload and
query ceilings remain enforced. See [the estimation contract](QUERY_JOINT_STATISTICS.md).
The manual `joint_ndv_planning` benchmark is local-only; no test lane, feature
matrix, timeout or compilation profile changes.

The same optimizer owner checks MATCH seed/target correlation, complete versus
sparse/invalid joint populations, and MCV/exact-point precedence. Catalog
controls retain the 32-group cap and ambiguity/staleness rules, share the
immutable view across query overrides and retain one label key per label.
The ignored `joint_key_lookup_measurement` compares the former owned-key
lookup with the four-key borrowed stack path, checking equal results and
reporting retained label keys. Replay with `cargo test --locked --release
-p hawdb-optimizer-graph --lib joint_key_lookup_measurement -- --ignored
--nocapture --test-threads=1`. Metadata-only timings do not qualify query latency
or process RSS; collection and persistence lanes are unchanged.

`//:pypi_publish_script_test` belongs to the required root
`//:hawdb_unit_tests` aggregate. It executes the publisher workflow's Python
preflight and registry verification with temporary distributions and mocked
PyPI responses. It covers publisher-created attestation sidecars, frozen
pre-upload hashes, incomplete or conflicting uploads, delayed visibility and
network failures without credentials, uploads or real sleeps.
The same owner checks that packaging explicitly selects the installed Rust
toolchain and that its override, installer and wheel builder agree with
`rust-toolchain.toml`, preventing release preparation from requesting development
components again. Replay with
`bazel test //:pypi_publish_script_test`, or
`python scripts/pypi-publish.test.py` with `packaging` installed. Actual Trusted
Publishing and public-install smoke verification remain release-workflow gates.

The plan-cache retention family uses
`//crates/plan-cache:hawdb_plan_cache_tests` for weighted LFU total/per-entry
limits, zero budgets, overflow, replacement and spare table-capacity accounting.
`//crates/plan-cache:hawdb_template_retention_tests` is its independent native
requested-allocation oracle: logical and physical CASE templates at 20/100/512
candidates, nested values, source-only regex templates, temporal projections with a boxed duration parameter and byte-driven churn must retain no more requested
heap than their conservative charges. The observer is thread-local and runs as
one test; requalify its standard-library layout assumptions on toolchain
changes. Replay with `cargo test --locked -p hawdb-plan-cache --test
template_retention -- --test-threads=1`. Requested heap excludes allocator
rounding and caller-owned in-flight copies; these checks are not RSS or latency
benchmarks. The facade's `api::plan_cache::retention_allocation` oracle belongs
to `//:hawdb_unit_general_tests`: it observes complete private cache keys,
physical IR, traces, shared metadata, proof-schema table/property IDs and EXISTS
strings, both scoring-key forms and table capacity
at 20/100/512 candidates, owned row-bound/nested list and path expressions, UNWIND source payloads, boxed shortest-path
descriptors with spare string capacities (Any/All selection), temporal literals,
thousands of boxed durations, temporal-function arguments, map literals and boxed
property access, and byte-driven eviction. Its thread-local
observer is enabled only in that one unit test. Replay with `cargo test --locked
-p hawdb --lib retention_allocation -- --test-threads=1 --nocapture`.
The same existing root owner covers `simple_case_dispatch` physical cache hits,
changed parameters, folded guards, lazy/cached errors, result budgets,
cancellation, zero/tiny cache-budget refusals and accepted/refused admission traces;
`query_observability` checks regex source-template hits beside a hot plan.
`knowledge_label_regex_memory_connections` retains its original physical-hit,
complete bounded-query result, snapshot and budget/error assertions. Core regex
and independent allocation tests prove that execution compilation neither lives
in nor grows the source-only cache owner.
Its vector-procedure pipeline check preserves default-feature cache bypass
and minimal-feature capability refusal before any cache lookup or bypass.
Run those filters with default and `--no-default-features` configurations.
Existing target profiles and execution lanes are unchanged.

`cargo build --locked --release --bench physical_plan_cache` builds the manual
embedded CASE cache benchmark. It covers cold/reused/fresh-handle reads at
20/100/512 candidates, scope and body scaling, complete result parity and a
separate qualification-feature allocation/preparation observer. It adds no
benchmark CI lane. With no arguments it runs a small 20-candidate, 512-row,
three-repeat smoke using the same complete-result assertions. Explicit arguments
retain the measured experiment sizes. Procedure, source-bound measurements and the per-read-handle
reuse limitation are in the [physical cache report](benchmarks/physical_plan_cache_macos_2026_10_31/README.md).

`//:storage_tla_evidence_script_test` belongs to the required root
`//:hawdb_unit_tests` aggregate. It validates the actual checked-in TLA model
inventory through `scripts/check-storage-tla.sh --manifest-json` before running
the synthetic evidence/negative fixtures. Its runfiles include every root-level
model and config plus the manifest, so unsorted/duplicate entries, undeclared
models and missing pairs fail before merge. This inventory step neither downloads
tools nor runs TLC; the existing postsubmit TLA lane retains model checking,
mutant rejection and retained-evidence verification. The existing moderate script
timeout is unchanged. Replay with `bazel test //:storage_tla_evidence_script_test`
or `bash scripts/check-storage-tla.test.sh`.

`embedded_store::tests::catch_up_admission_tests` belongs to the existing root
`//:hawdb_unit_general_tests` owner. Its two guards each exercise full-residency
and incremental projection dispatch with one explicitly configured governor.
A retained Background physical I/O wave must not reject caller-selected
Foreground catch-up; existing Background APIs still refuse. A saturated
Foreground lane must refuse before hydration and succeed after release. The
fixtures retain durable projection and permit-refund assertions, keep automatic
maintenance enabled, and do not use sleeps or admission retries. Initial task
refund is observed before opening the automatic-maintenance owner; global
admission counters are not assumed stable while that worker can run. Local replay:
`cargo test --locked -p hawdb --lib catch_up_admission_tests -- --test-threads=1`.
See [caller-priority catch-up](https://github.com/nowledge-co/hawdb/issues/1306).

`embedded_store::tests::storage_pressure_tests` (`nowledge-co/hawdb#1725`)
belongs to the existing root `//:hawdb_unit_general_tests` owner. It opens a
durable handle with explicit checkpoints only and pins
`EmbeddedStoreHandle::storage_pressure_snapshot` to the `Database` snapshot
(ignoring only the wall-clock WAL age and filesystem free-space samples), both
with WAL debt and after a checkpoint. Under the root `test-support` feature,
like the other storage failpoint tests, the failpoint
`set_generation_reclamation_remove_failpoint` fails one obsolete checkpoint
deletion; the handle must report it as pending reclamation files/bytes and
`GenerationReclamationDebt` until the next checkpoint clears it. Default-feature
Bazel builds compile that case out. A reader held
across three checkpoints must report reader lag, obsolete bytes and retained
WAL generations without pending debt; after release and one more checkpoint,
reader lag and pending debt are zero and the main branch data directory holds
at most two `wal.*.hawdb` files.
Replay with `cargo test --locked -p hawdb --features test-support --lib
storage_pressure_tests -- --test-threads=1` or the owning Bazel target with
`--test_arg=storage_pressure_tests`. No new lane, profile, fixture
amplification or timeout is introduced.

`embedded_store::tests::schema_maintenance_tests` (`nowledge-co/hawdb#1739`)
belongs to the existing root `//:hawdb_unit_general_tests` owner. With
`background_maintenance = false`, it writes `Entity` nodes and `MENTIONS`
relationships schema-lessly (never `CREATE NODE TABLE`), some lacking the
property and some with an explicit NULL, and checks that no property descriptor
exists. It then declares `TYPE ANY DEFAULT 0.0` on both, observes `BACKFILL`
through `EmbeddedStoreHandle::property_schemas` and
`plan_schema_maintenance`, and drives `run_bounded_schema_maintenance(2)` over
several passes until no descriptor is in `BACKFILL`. A budget below the planned
validation estimate leaves `VALIDATING` unchanged, and the planned estimate
empties the plan. Absent keys are filled, explicit NULLs
and present values are kept, both descriptors reach `PUBLIC`, and the label
still accepts arbitrary undeclared properties. A reopen without a checkpoint
and a reopen after one preserve the fills and states. A second case pins
`CREATE PROPERTY` re-declaration: identical declarations return
`created: false`, while a changed default, type or nullability is a typed
`HawDBError::Schema { surface: Storage, .. }` refusal (Display text
`storage error: ...`) that leaves the descriptor unchanged and the handle
unpoisoned. Replay with `cargo test --locked -p hawdb --lib
schema_maintenance_tests`. No new lane, profile, fixture amplification or
timeout is introduced.

`embedded_store::tests::initial_import_checkpoint_tests` (`nowledge-co/hawdb#1707`)
belongs to the existing root `//:hawdb_unit_general_tests` owner. It pins the
split between resident-only `apply_initial_import_projection_delta` and the
full-rewrite `checkpoint_initial_import_projection`. Deferred and per-page
checkpointing yield identical documents, delta reports, and text and hybrid
hits (exact score bits), before and after reopen. A dropped store reopens at
exactly the last checkpointed generation and import epoch, including a crash
before any checkpoint and one in the last, largest gap of a doubling schedule.
Replay from the host's durable cursor (plus one already-durable page) matches an
uninterrupted run. A resume seeded from the persisted checkpointed input bytes
takes at most one more checkpoint than the uninterrupted run had left. A crash
between the resident snapshot replace and the generation manifest publish
reopens fail-closed, and a fresh re-import recovers. For 8 and 32 pages, a doubling schedule writes at most 3x
the final checkpoint in at most `log2(P) + 2` generations, while per-page
checkpointing writes at least `P / 4` final-size rewrites. The fixtures are
small temporary persistent projections (at most 640 documents, 8-dimensional
embeddings), take a few seconds, and add no lane, profile or timeout. Local
replay: `cargo test --locked -p hawdb --lib initial_import_1707 -- --test-threads=1`.
See [the contract](SEARCH_INITIAL_IMPORT_CHECKPOINT.md).

`embedded_store::tests::search_projection_rebuild_tests` (`nowledge-co/hawdb#1738`)
belongs to the existing root `//:hawdb_unit_general_tests` owner. It covers
`EmbeddedStoreHandle::rebuild_out_of_core_search_projection`. An old library
whose incremental projection cursor is below a two-entry change log refuses
catch-up; the rebuild into a new directory (with host-hydrated 8-dimensional
embeddings, a host-owned row, `HAS_LABEL` business labels in both directions
and a graph write committed during the scan) is stamped at the snapshot epoch,
verified, and leaves the old manifest byte-identical. Reopening on the new
directory catches up exactly the concurrent write, and the projection's
document digest and text, exact-vector and exact-hybrid hits (score bits)
equal an independent fresh build (resident full rebuild pushed through the
ordered writer). Cancellation mid-scan, a mid-scan stage image (not
openable), a target that appears before the no-replace install, a source that
drifts between the build and verification scans, an existing target and a
host-supplied epoch all leave no target and no stage (each precondition and verification refusal is asserted to be the typed `HawDBError::SearchProjectionRebuildRefused` with its `reason`, and cancellation stays `Execution`), and the old projection
reopens with its old contents; a leftover crash stage does not block a later
rebuild. The bounded-memory case rebuilds 2,500 documents of 8 KiB (20 MB) with a
24 MiB reservation and asserts, with the root unit binary's thread-local
live-allocation oracle (`api::test_allocation`), that live allocations through
the scan, external sort and build stay below the reservation and half of the
corpus. Search-crate owner
`out_of_core::generation_writer::unordered::replacement::tests::fresh_replacement_builds_an_absent_projection_and_refuses_an_existing_one`
covers the fresh-root unordered writer. The fixtures are small temporary
persistent graphs; no lane, profile or timeout is added. Local replay:
`cargo test --locked -p hawdb --lib search_projection_rebuild_tests -- --test-threads=4`
and `cargo test --locked -p hawdb-search --lib fresh_replacement`. See
[the contract](SEARCH_PROJECTION_REBUILD.md).

`embedded_store::tests::search_epoch_advance_tests` (`nowledge-co/hawdb#1756`)
belongs to the existing root `//:hawdb_unit_general_tests` owner. It covers
`EmbeddedStoreHandle::advance_search_projection_source_epoch` on the resident
initial-import path and the incremental out-of-core path. Each path builds a
300-document corpus, advances across import-bookkeeping commits on
non-projection labels and a `DEFAULT` declaration, and asserts that the
generation file set is unchanged, every non-selector file keeps its inode and
length, and only the selector's bytes were written. Then:

- one `Memory` write in the range (and, incrementally, one `Memory` deletion)
  is refused with the typed `SearchProjectionEpochAdvanceRefused`
  (`ProjectionChanged`, never `Storage`), and so is a target before or beyond
  the current epoch (`RangeUnavailable`), including a `Memory` edit inside the
  range that was deleted after it;
- a two-entry change log trimmed by bookkeeping is refused as `ChangeLogGap`;
- Mem's `content_migration_state` upserts are allowed and counted
  (`allowed_relational_change_count`) only when declared with
  `with_non_projection_relational_tables`; an undeclared table and the empty
  default refuse, and empty, duplicate or `hawdb_` declarations refuse open;
- relabel statements (`REMOVE n:Memory`, `SET n:Other`) fail without
  committing;
- a concurrent graph writer (retrying `TransactionRetryRequired`) never slips
  a commit past an advance: each advance proves its exact epoch or refuses
  with a typed reason, and the final projection agrees with a rebuild;
- reopen keeps the advanced epoch;
- after incremental catch-up, the epoch and `SearchDocumentSetDigest` match
  `rebuild_out_of_core_search_projection`.

Search-crate owner `out_of_core::epoch_advance::tests`, in the existing
`hawdb-search` unit lane, covers the selector-only publication for resident
and out-of-core closures by file bytes and inode, and chained advances.
Injected aborts before and after the selector rename
(`ResidentCheckpointStep::EpochAdvance*`) reopen at exactly one epoch with
identical documents and hits. It also covers stale-reader conflicts and
resident `UncheckpointedChanges`. The fixtures are small temporary projections;
no lane, profile or timeout is added. Local replay:
`cargo test --locked -p hawdb --features test-support --lib search_epoch_advance -- --test-threads=4`
and `cargo test --locked -p hawdb-search --lib epoch_advance`. See
[the contract](SEARCH_PROJECTION_EPOCH_ADVANCE.md).

Search-crate owner `out_of_core::manifest_version::tests` (`nowledge-co/hawdb#1763`),
in the same `hawdb-search` unit lane, covers the selector version gate. A newer
selector is refused as the typed `UnsupportedStorageFormat`, never `Storage`,
through the decoder, `SearchOutOfCoreReader::open` and `SearchIndex::open`.
Damaged and version-inconsistent selectors stay `Storage`. Unversioned
selectors read and re-encode byte-identically. An epoch-advanced selector
round-trips at version 2, and a checkpoint or delta after it returns to
version 1. A new-generation build, an incremental update and a resident
checkpoint over a newer selector are refused with the typed error and leave it
byte-identical. Each refusal asserts `format() == SEARCH_PROJECTION_MANIFEST_FORMAT`;
`hawdb-core` `storage_format` tests pin the exported format constants. `bindings/ffi/src/error_tests.rs` and `bindings/python/src/errors.rs`
pin the binding kind. No lane, profile or timeout is added. Local replay:
`cargo test --locked -p hawdb-search --lib manifest_version`. See
[the policy](SEARCH_PROJECTION_MANIFEST_VERSION.md).

The experimental [Windows directory-custody first packet](../scripts/windows-directory-custody-probe.ps1)
is owned by the narrowly path-filtered, same-repository
[Directory Custody Primitive Probe](../.github/workflows/directory-custody-probe.yml)
PR job on Windows2025. It is a falsification fixture for
[the captured-open investigation](https://github.com/nowledge-co/hawdb/issues/1291),
not a production SDK API or a default storage-qualification gate. Publishing or
synchronizing its PR executes native code, so exact-source review precedes that
publication. The packet retains one original GenericRead/shareWRITE|DELETE
directory owner; checks same/child-process sharing and unobstructed controls;
records separate original-object ReOpenFile and empty-name NtCreateFile
write-only open/identity/Flush outcomes; observes attribute-only equal identity;
and checks duplicate/last-handle lifetime. Its 18 cases permit at most 10 child
attempts, each 15s plus bounded termination/output collection, within a 10-minute
job. Setup/C#compile and native runtime costs are not measured yet; the runner
performs no Rust build. Artifacts keep source hashes, ABI/masks, OS/filesystem,
per-case progress, child logs and distinct negative/incomplete outcomes.
Local replay on Windows uses a fresh process and artifact directory:
`pwsh -NoProfile -File scripts/windows-directory-custody-probe.ps1 -OutputDirectory
<new-temp-artifact-directory> -ExpectedSourceRevision <exact-git-HEAD>`.
Exit 0 means a fixed primitive combination was observed with the packet controls;
exit 2 means both candidates were falsified or unqualified; exit 1 means harness or
cleanup failure. None qualifies cold-RO ACLs, independent cursors, relative child
IO, missing/foreign/reparse bindings, ordinary-client coexistence, role admission,
power-loss durability or full captured-open integration. These remain separate
required qualification; a candidate refusal is never relabeled SDK success.

The executor `spill::tests` family covers streaming binding encoding with a
full query-memory account, read/decode admission, exact disk quotas, nesting
preflight, short writes, I/O errors and scalar/nested/vector round trips.
`blocking::sort::tests` compares Sort/TopN tie order with and without spilling
and checks 16-KiB rows through bounded merges. The existing executor unit target
owns these regressions; replay with `cargo test --locked -p hawdb-executor --lib
spill::tests` and the `blocking::sort::tests` filter. The existing local-only
`//crates/executor:hawdb_sort_merge_account_fuzz_tests` campaign also exercises
adaptive buffers below an observed peak while preserving hard root refusal,
as well as the tie-order and wide-row controls. Test lanes, profiles and deadlines stay
unchanged; allocation microprobes do not establish end-to-end RSS or latency.

Native Decimal and shared numeric SQL execution (#1419) use the existing core,
SQL, SQL-syntax, plan-cache, relational, system-SQL, storage, executor and root
unit owners. Core tests check exact arithmetic, bounded parsing, midpoint-away
rounding, overflow, numeric identity/order and ordered-key encodings against
integer/rational comparisons. SQL tests check both frontends, prepared parameters,
CAST metadata and flat expression depth limits. Relational tests check exact
index boundaries, integer/Decimal joins, SUM/HAVING and atomic failed UPDATEs.
Root BIGINT mutation regressions cover NULL propagation, NOT NULL and numeric
type refusals, and unchanged rows after rejected assignments or overflow.
Vector scalar projection tests require an explicit DOUBLE fallback cast for
COALESCE and reject a mixed Decimal/DOUBLE call even on an empty table.
Native locking tests keep comparison bounds unrounded across both residency
modes. Serializable tests retain true incompatible-type and volatile-value
refusals while checking mixed BIGINT/DOUBLE results above 2^53, fail-closed
cross-domain key capture, and exact integral Decimal-to-BIGINT capture.
Storage and spill tests check raw scale, malformed/truncated payload rejection,
page projection and subsequent-record alignment. Cache keys distinguish equal
numeric values with different scales when cached results retain representation.
Root `api::tests::decimal` runs persistent SQL in both Materialized and OutOfCore
modes with demand-paged indexes, plus graph WAL/checkpoint reopen. Graph coverage
checks Decimal ranges with exact integer bounds, repeated-bound intersection,
IN, NULL, negation and ORDER BY with scalar/composite indexes in both residency
modes. Zone maps, source summaries and range-block envelopes must not prune
matching Decimal rows using incomplete mixed-numeric extrema. Mixed temporal
and Decimal records pin distinct value kinds and WAL/checkpoint/spill tags.
Explicit scalar/composite range physical plans check exact Decimal and integer
bounds plus incomparable Float bounds at each recovery stage, independently of
cost-based scan choices for properties excluded from histogram collection.
These tests also cover exact and quoted Decimal schema defaults together with
main's SQL type aliases/contextual strings, and concurrent secondary-index rekeys
locking both stored images after Decimal rescaling while allowing disjoint readers.
These tests do not independently qualify machine power-loss behavior. Replay with
`cargo test --locked -p hawdb-core -p hawdb-plan-cache -p hawdb-relational
-p hawdb-storage -p hawdb-executor --lib decimal`,
`cargo test --locked -p hawdb-sql -p hawdb-sql-syntax -p hawdb-system-sql --lib`,
and `cargo test --locked -p hawdb --all-features --lib native_decimal`.
Python parameter tests cover native `decimal.Decimal` and seeded arithmetic
against an independent `fractions.Fraction` oracle; C JSON tests cover the
exact `$decimal` marker. Existing binding lanes remain unchanged; retained
foreign numeric buffers do not gain a Decimal ABI type. Manual Shiro result
controls distinguish Decimal scale in ordered and bag comparisons, including
nested lists/maps, while retaining duplicate multiplicities and float bits.
Replay: `cargo test --locked --manifest-path tools/shiro/Cargo.toml --lib
decimal_report_preserves_exact_digits_and_scale` and the `comparison` filter.
These controls remain in the existing local-only Shiro test targets.

Cypher `LOAD CSV` and the type-conversion functions are owned by existing
targets: `hawdb-core` `conversion_function::tests` (Neo4j conversion rules and
the Kùzu `string(x)` text table),
`hawdb-cypher` `tests::load_csv` and the PEG/reference differential
`parser::frontend::tests::load_csv_matches_reference_parser`, `hawdb-plan-cypher`
`logical::load_csv_tests` (row-source binding and refusals), `hawdb-optimizer-graph`
`graph::tests::load_csv` (lowering/EXPLAIN), `hawdb-plan-cache`
`template::tests::load_csv_plans_keep_parameter_sources_unbound_and_reusable`,
`hawdb-executor` `load_csv::tests` (RFC 4180 reader, path policy and its
`Semantic`/`Execution` error classes, record/file limits and per-row mutation
resolution, using temporary directories), `hawdb-executor`
`mutation::staged::tests` (statement-level matched-relationship endpoint
resolution: result equivalence with per-row scans for duplicates, misses,
composite keys, missing labels and indexed/unindexed labels; one pass per
endpoint pattern; cancellation; ledger refusal of staged mutations and endpoint
maps; fan-out endpoints refused with the commit's own affected-row errors before
their ids are copied; charged id copies on `UNWIND` batches; the host's
configured execution memory on the store entrypoint), `hawdb-storage`
`store::tests::id_in_mutation` (id-led conjunction fast path against a
single-element `Or` scan oracle, including pending nodes, and a 5,000-source
fan-out whose recheck excludes the leading id list), and
root `api::tests::load_csv` (end-to-end LDBC-style loads, refusal without an
import root, path escapes, remote URLs, parameter rebinding, mutation/WAL/read
and query-memory budgets, explicit transactions including nodes staged earlier
in the transaction, zero per-row label scans under the write guard for
`LOAD CSV` and `UNWIND` relationship loads at 3,000 nodes and rows via the
hidden storage scan counter, `UNWIND` relationship CREATE and MERGE batches
resolving their endpoints on the autocommit and explicit-transaction paths,
fan-out loads beyond `max_affected_rows` refused
with no writes, CSV fields read through temporal constructors and `toString`
over temporal values, missing-file error classes and, on Unix, a FIFO source refused
without blocking). Replay with `cargo test --locked -p hawdb-executor
--lib load_csv`, `cargo test --locked -p hawdb-executor --lib mutation::staged`
and `cargo test --locked -p hawdb --lib api::tests::load_csv`. See
[LOAD CSV](CYPHER_LOAD_CSV.md).

The complete existing `//crates/executor:hawdb_executor_unit_tests` owner runs
with Bazel `exclusive` after competing build/test actions in the same invocation.
It keeps the same executable, features, default libtest concurrency and original
300-second ceiling. The 3,000-node / 3,005-row endpoint comparison retains both
index modes and all four endpoint patterns. No new CI job, owner, partition
script or compiler profile is needed; Cargo and native Windows selections stay
unchanged. Ref [#1462](https://github.com/nowledge-co/hawdb/issues/1462).

[PR #1502 crate CI](https://github.com/nowledge-co/hawdb/runs/125103553248), head
`1a78af07764cf960080cd827a8c4cdc406ffd9c0`, passed 105/106 targets but timed out
this existing executor owner at 300.6s; the endpoint comparison was the last
unfinished case. [The same head's root CI](https://github.com/nowledge-co/hawdb/runs/125103553427)
passed 74/75 targets, including both new checkpoint-contract carriers, while
general failed five automatic-checkpoint lifecycle cases. The complete-family
root selector from #1500 moves those cases into the existing isolated checkpoint
owner (see below). These scheduling changes preserve every fixture, assertion,
feature and deadline. Final-head CI must qualify the mitigation; local timing
and the timeout tail alone do not establish the cause or a remote speedup.

Cypher `sum()` and expression-argument aggregates are owned by existing
targets: `blocking::aggregate::tests` (Sum numeric/NULL/empty/overflow/
non-numeric/DISTINCT accumulator and partial-merge rules),
`blocking::hash_oracle` (`sum_and_expression_aggregates_match_oracle_with_and_without_spill`
and the overflow check through partial and buffered paths), the
`hawdb-cypher` parser tests, `logical::pipeline::tests` planner targets, and
root `api::tests::aggregates`. Replay with `cargo test --locked -p
hawdb-executor --lib aggregate` and `cargo test --locked -p hawdb --lib --
api::tests::aggregates`. No lane, profile or deadline changes.

Row-bound node-pattern properties and read-only `UNWIND` are owned by the
existing Cypher parser and planner targets, executor batch-dispatch coverage,
and root `api::tests::row_bound_pattern_properties` in
`//:hawdb_unit_general_tests`. The regressions cover scalar/property bindings,
optional predicate boundaries, duplicate matches and inputs, NULL/empty values,
indexed and generic execution, parameter cache rebinding, dropped/unknown scope,
and payload/result budget failures. Its `indexed_lookup` module compares indexed
single-node GraphMatch access with the unindexed path for scalar/map/native
property keys, constant and residual Space/active filters, nonunique keys,
duplicate inputs, missing/NULL/invalid keys, fallible conjuncts before and after
UNKNOWN equality on NULL stored keys, and LIMIT
stop. Scan reports require six matching node visits for four bounded input keys;
multiple available indexes retain one selected property across NULL input keys;
wide unrequested bodies remain excluded under a small source/working budget.
Canonical reopen and a snapshot pinned across update/checkpoint retain exact
results. Executor `batch::tests::graph_match::indexed_node` owns admission release
after normal completion, consumer Stop/error, mid-stream cancellation and source
budget refusal. It runs in the existing executor unit owner; no lane/profile or
timeout changes. The Mem Thread-reference regression keeps
its original Boolean/COALESCE predicate and complete MATCH → row-dependent
UNWIND → WITH/aggregate → correlated MATCH/OPTIONAL MATCH query. Per-row
expansion also covers WITH aliases/native metadata, nested and multiple sources,
NULL lists/items, duplicate multiplicities, early LIMIT stop and scalar alias
reuse. Executor `transform::tests::row_unwind` owns cancellation, stop and refusal
cleanup; plan-cache `row_unwind_parameters` owns logical/physical rebinding, and
the existing allocation oracle covers owned row-UNWIND expressions and refuses
a 1 MiB output before allocating a copy. Mutation source refusals remain owned
by the planner. Replay with `cargo test --locked -p hawdb
--lib row_bound_pattern_properties`, `cargo test --locked -p hawdb-cypher
row_bound_pattern_properties`, and `cargo test --locked -p hawdb-plan-cypher
row_bound_pattern_properties`. No CI lane or timeout is added.

Conjunctive row-sourced column lookups ([#1544](https://github.com/nowledge-co/hawdb/issues/1544))
are owned by the existing `logical::pipeline::tests` planner target and root
`api::tests::node_column_lookup_conjuncts` in `//:hawdb_unit_general_tests`.
The planner case requires one `node.property = column` conjunct to become the
`NodeColumnLookup` key with the remaining conjuncts as its node predicate, in
any conjunct position and through nested conjunctions. Optional MATCH, a
fallible residual (a function call), `column = property`, and another equality
or membership test on the node (which an index could serve) keep the per-row
scan. The root differential compares the lookup with that scan row for row and
in order, without and with an index on the key, for duplicate, NULL, missing
and mixed integer/float keys, a second label sharing the property, a residual
on a missing property, and LIMIT. Further cases check that another indexed
equality keeps the scan, that `LIMIT 1` reads one candidate (key-only and
conjunctive), and that an optional conjunctive MATCH keeps its NULL rows. The
existing `indexed_lookup` fallible-conjunct regression still requires the
scan's error. Replay with `cargo test --locked -p hawdb --lib
node_column_lookup_conjuncts` and `cargo test --locked -p hawdb-plan-cypher
--lib conjunctive_column_constraints`. No CI lane or timeout is added.

Anchored optional relationship-count sums are covered by root
`api::tests::optional_match::count_source` in the existing
`//:hawdb_unit_general_tests` owner. Indexed and unindexed fixtures retain
duplicate source keys, complete initial properties, parallel relationships,
self-loops, directed/untyped legs and missing/NULL inputs. Source diagnostics
require three indexed candidates versus five projected label candidates, with
two matching sources and the same count sum. Unrelated labels and 32 KiB
unrequested bodies remain outside a 16 KiB query-memory budget; a full-record
control must refuse. Canonical reopen and a snapshot pinned across an
update/checkpoint retain exact results. Executor
`batch::tests::count_source` covers released admission after completion,
consumer Stop/error, pre-read and mid-traversal cancellation, and source refusal.
The existing executor `projection_allocation` oracle distinguishes native/cold
unrequested payload success from requested-payload rejection before copy/decode;
both retain the original large-allocation, external-call, ledger and store-state
assertions.
Replay with `cargo test --locked -p hawdb --lib relationship_count_source`
and `cargo test --locked -p hawdb-executor --lib relationship_count_source`.
No CI lane, profile, timeout or feature scope changes.

General `shortestPath` / `allShortestPaths` coverage is owned by existing
targets: executor `traversal::shortest_path_bfs::tests` (directions,
deterministic single/all selection, parallel relationships, equal endpoints,
bounded and capped hops, excluded relationships, operator-budget refusal,
cancellation during discovery and enumeration), `hawdb-cypher` parser tests,
`logical::pipeline::tests` binder/normalization tests and root
`api::tests::shortest_paths` (IC13 OPTIONAL MATCH, WITH/ORDER BY/LIMIT,
exact-parameter plan caching, hop cap and blocking-budget failures). Replay with
`cargo test --locked -p hawdb-executor --lib shortest_path_bfs` and
`cargo test --locked -p hawdb --lib api::tests::shortest_paths`. No lane,
profile or deadline changes. See [contract](SHORTEST_PATH_EXECUTION.md).

Anchored `GraphMatchExec` (#1423) is owned by optimizer
`graph::lowering::match_order::tests` and root `api::tests::anchored_match`
in the existing general unit lane. The embedded family checks exact rows,
step order, full-scan counts against an equivalent bound-first query, and
NULL anchors with required matches and OPTIONAL padding. A projected input
scan may contribute zero to the full-node scan counter; the regression checks
that anchoring adds no per-row scan rather than assuming the input scan kind.
Replay with `cargo test --locked -p hawdb --lib api::tests::anchored_match`.
No lane, fixture, feature matrix or deadline changes.

Cost-based pattern starts at index-seekable nodes ([#1485](https://github.com/nowledge-co/hawdb/issues/1485))
are owned by optimizer `graph::pattern_start::tests` (work estimates, the 2x
margin, missing, unlabelled, `:A:B` and disjunction statistics keep the start),
`graph::chain_start::tests` (a first-MATCH `Expand` chain restarts at its
`IndexNodeSeek`, a disabled rule or optimistic catalog keeps it),
`graph::lowering::match_order::tests` (a `GraphMatchExec` path moves to a
seek, a bound node stays) and root `api::tests::cost_based_start` in the
existing general unit lane. The root differential runs six cases (chain,
variable-length chain, repeated relationship type, cycle through the seek,
OPTIONAL MATCH padding, bound endpoint) on six randomized 102-node graphs with
self-loops and parallel relationships and five parameter sets each, and
requires identical row multisets for the chosen plan, the
`CostBasedPatternStart`-disabled qualification plan and the pattern written
from the other end, plus the cached plan; it asserts the start moved where the
seek is clearly cheaper. Further cases check EXPLAIN and the trace name the
start, a seek inside a longer `GraphMatchExec` scans no nodes, and a statistics
change misses the cached plan and re-plans the start. Replay with
`cargo test --locked -p hawdb --lib api::tests::cost_based_start` and
`cargo test --locked -p hawdb-optimizer-graph pattern_start chain_start match_order`.
No lane, fixture, feature matrix or deadline changes.

Directed bounded-path estimates additionally cover heterogeneous intermediate
labels, incoming reversal and the undirected mixed-direction fallback in
`graph::pattern_start::tests`, including missing tuples in a complete horizon,
empty complete maps, legacy/truncated coverage and out-of-horizon fallbacks.
Known-zero completions retain positive planning rows and charge dense/sparse
failed prefixes and probes in both directions; relationship predicates cannot
discount the unfiltered prefix work. Rare positive two-hop and three-hop
completions also retain visits and probes for dead ends, shorter depths and
intermediate labels outside the requested endpoint pair, in both orientations. These regressions replace the former
zero-work expectation. `graph::costing::graph_match::tests` owns
frequency-aware seed/expansion costs, actual seeded-union branches with shared
input cost, imported input labels, bound endpoints, OPTIONAL cardinality floors
and bounded fallback for unbounded/type-disjunctive paths. These use the existing
optimizer unit lane and profiles. The manual embedded example
`cargo run --locked --release --example optimizer_path_statistics` compares
fully consumed, identical ordered results for ordinary and rule-disabled plans
on one pinned synthetic snapshot, with alternating order, two warmups, fifteen
measurements and raw cost/execution profiles. It is a developer experiment,
not a new CI performance gate or a general LDBC acceptance claim.

Planning prefix scratch is covered by `graph::pattern_start::tests` in the
same optimizer unit lane: orientation and hop-limit reuse, independent catalog
lifetimes, a context size at most 2 KiB, and direct-computation fallback after
16 keys. A correlated three-node walk checks joint seed/target cardinalities
while both expansions and repeated estimates share one prefix scan.
The ignored `prefix_statistics_scaling_measurement` is a manual metadata-only
experiment at 100, 10,000 and 100,000 path tuples with 64 repeated estimates.
Replay with `cargo test --locked --release -p hawdb-optimizer-graph --lib
prefix_statistics_scaling_measurement -- --ignored --nocapture --test-threads=1`.
It compares identical prefix arithmetic with and without per-estimate reuse;
its timings exclude query execution and do not establish application speedup.
No new CI lane, feature, profile override or timeout is introduced.

Bounded-path provenance follows the existing statistics lanes: storage
`statistics::tests` and `statistics::checkpoint::tests` cover completed versus
budget-exhausted collection and retained copies; checkpoint metadata tests
cover ordinary/cooperative byte parity, round-trip, duplicate rejection and
legacy unknown coverage. Root `api::tests::statistics` covers external-refresh
truncation, checkpoint/reopen and retaining runtime MATCH execution with a
positive planning floor and charged visits to 64 dead-end first-hop edges. No lane, profile, timeout or fixture scope is removed.

Early `GraphMatchExec` WHERE placement (#1424) is owned by optimizer
`graph::lowering::match_predicates::tests` and root `api::tests::early_match_predicates`.
The optimizer cases include dependencies nested in scalar conversions, temporal functions,
component access and map literals, and prove that moving conjuncts changes
intermediate work without changing the complete predicate's output cardinality.
The embedded combined regression anchors an OPTIONAL MATCH before placing its
conjuncts and final property projection. It checks exact rows, NULL padding,
the reordered step indices, and retention of a property used only by WHERE.
The executor evaluates placed conjuncts before undoing in-place candidate bindings.
Replay with `cargo test --locked -p hawdb-optimizer-graph match_predicates`.
Existing fixtures and assertions remain; CI lanes, feature profiles and deadlines
are unchanged.

`GraphMatchExec` hash joins at the meeting node and `node IN list` id-set
semi-joins ([#1485](https://github.com/nowledge-co/hawdb/issues/1485)) are
owned by optimizer `graph::chain_start::tests` (an IC11-shaped first MATCH
splits at the meeting node and joins a build started at the place seek),
executor `graph_match::hash_join` and `graph_match::membership`, and root
`api::tests::meeting_join` in the existing general unit lane. The root
differential runs eighteen cases on five randomized 565-node graphs with
self-loops and parallel relationships and four parameter sets each: the IC11
single-MATCH and LDBC forms, duplicate meeting nodes (multiplicity), OPTIONAL
padding with empty builds, NULL meeting nodes, a repeated relationship type
inside the build, residual conjuncts over input columns (guarded, a guard
written after the division it protects, and an unguarded division that must
fail in both plans), an imported node missing from the snapshot (fails in
both), IC5, and collected lists with NULLs, duplicates, NULL or empty lists,
plain values and a non-list column, plus a NULL membership followed by a
failing conjunct, a false one before it, and a membership over a non-list
before another (errors). Row multisets and errors of the chosen plan, the plan with
`GraphMatchHashJoin` and `GraphMatchIdSetSemiJoin` disabled and the cached
plan must match, and every hash-join case must run as one on some graph.
Further cases check EXPLAIN, the trace and distinct plan fingerprints, a build
that spills under a 64-KiB blocking budget (and refuses rather than truncates
at 4 KiB), cancellation and work limits, and, with `--features acl`, that the
join returns only visible nodes and applies the visibility conjuncts in its build. Replay with
`cargo test --locked -p hawdb --lib api::tests::meeting_join` and
`cargo test --locked -p hawdb --lib --features acl meeting_join::access_control`.
No lane, fixture, feature matrix or deadline changes.

Disjunctive seeded unions ([#1625](https://github.com/nowledge-co/hawdb/issues/1625)),
a first `GraphMatchExec` whose WHERE disjuncts each pin a different node by an
indexed equality (LDBC SNB IC12), are owned by optimizer
`graph::lowering::seeded_union::tests` (branch starts, the reversed
variable-length walk, the whole WHERE plus one guard per earlier seed, seed
choice per disjunct, the cost gate, a fallible WHERE, and no union for one
node, a missing index or equality, a NULL literal, OPTIONAL or correlated
MATCH, a seeking start, two paths or a disabled rule), executor
`batch::tests::graph_match::seeded_union` (branches replace the steps, the
limit spans branches, and a NULL seed value guards nothing), and root
`api::tests::disjunctive_seed_union` in the existing general unit lane. The
root differential runs eight cases on five randomized 96-node tag hierarchies
with NULL and repeated names, diamonds and parallel relationships, and seven
names including NULL: IC12 and a collected form, three disjuncts over two
nodes of one label, extra conjuncts inside and beside the disjunction, and the
non-applying one-node, OPTIONAL MATCH and fallible (errors in both) forms. Row
multisets and errors of the chosen plan, the plan with `DisjunctiveSeedUnion`
disabled and the cached plan must match; duplicate rows and rows satisfying
both disjuncts must occur. Further cases check EXPLAIN, the trace and distinct
plan fingerprints, and, with `--features acl`, that every branch binds only
visible tags and classes. Replay with
`cargo test --locked -p hawdb-optimizer-graph seeded_union`,
`cargo test --locked -p hawdb-executor seeded_union`,
`cargo test --locked -p hawdb --lib api::tests::disjunctive_seed_union` and
`cargo test --locked -p hawdb --lib --features acl disjunctive_seed_union::access_control`.
No lane, fixture, feature matrix or deadline changes.

`GraphMatchExec` record projection (#1427) is owned by optimizer
`graph::match_reads::tests` (which node properties later projections,
aggregates, filters, sort keys, nested conversion/temporal/component/map expressions
and later MATCH clauses read; whole-node
escapes, same-name columns and unknown operators keep full reads) and root
`api::tests::graph_match_reads` in `//:hawdb_unit_general_tests`. The root
family checks exact rows on a resident and a checkpointed out-of-core store
for anonymous and named relationships, relationship property patterns,
whole-node returns, `collect`, node equality, node lists, WHERE-only and
ORDER BY properties, variable-length trails, multi-label targets, bound
targets, undirected self-loops, OPTIONAL MATCH padding and cached-plan
rebinding. Persistent fixtures use process/sequence-qualified directories so
parallel cases do not depend on clock uniqueness. Its structural assertions use the always-compiled storage
`record_reads_for_test` point-read counters. Replay with `cargo test --locked
-p hawdb-optimizer-graph match_reads` and `cargo test --locked -p hawdb --lib
api::tests::graph_match_reads`. No lane, profile or deadline changes.

`GraphMatchExec` relationship conjunct pushdown and relationship projection
(#1483) extend those owners: optimizer `graph::lowering::match_predicates::tests`
(which conjuncts read only a one-hop step's relationship, including OR, NOT,
IS NULL, mixed, constant, bound, reused, anonymous and variable-length cases),
optimizer `graph::match_reads::tests` (relationship property demand, pattern
properties, whole-relationship escapes, imports and variable-length steps),
executor `batch::tests::graph_match` (edge-conjunct result equivalence, error
deferral and malformed placement), and root `api::tests::edge_predicates` in
`//:hawdb_unit_general_tests`. The root family checks exact rows on a resident
and a checkpointed out-of-core store for three-valued conjuncts, pattern
properties, whole relationships, OPTIONAL MATCH padding when every edge fails
and errors raised only for complete matches. Its structural assertion uses the
`record_reads_for_test` counters, including the new `projected_relationships`,
to show that rejected relationships read no target. Replay with `cargo test
--locked -p hawdb-optimizer-graph match_`, `cargo test --locked -p
hawdb-executor graph_match` and `cargo test --locked -p hawdb --lib
api::tests::edge_predicates`. No lane, profile or deadline changes.

Canonical point-read segment reuse (#1482) is owned by storage
`canonical::point_read_scope_tests` and root `api::tests::point_read_reuse`
(`//:hawdb_unit_general_tests`). Storage cases prove that reads inside a
`CanonicalPointReadScope` seek the descriptor tree once per segment in
ascending and descending id order (gaps and out-of-range ids included), equal
unscoped and full reads for every property selection and value shape (the
length-based skip of a directory-validated segment and per-read validation of a
small segment agree on rows and admitted bytes), never cross reader
generations, stay per thread, never retain detached or over-budget leases, and
release every cache pin when the scope closes. Corruption cases rebind the
segment digest to invalid UTF-8 or an unknown value tag in an unselected
property and still fail closed and poison the reader, as do digest and
descriptor corruption, including when a valid segment is retained. The root
case reads 300 messages of one creator out of core with a bounded number of
descriptor seeks and checks rows against the resident store after an update,
a checkpoint, a pinned older snapshot and a reopen. Structural assertions use
the always-compiled `point_descriptor_seeks_for_test` counter. Replay with
`cargo test --locked -p hawdb-storage point_read_scope` and `cargo test --locked
-p hawdb --lib api::tests::point_read_reuse`. No lane, profile or deadline
changes.

Canonical point-read work per record (#1615) is owned by storage
`canonical::point_directory::tests` and `canonical::point_read_scope_tests`
plus root `api::tests::point_read_reuse` (`//:hawdb_unit_general_tests`).
Storage cases prove that a point directory's stride adapts to the segment's
frame size while its bytes never exceed the larger of the former 32-record
stride directory and 1/24 of the segment, that reads of dense-id segments
examine exactly one directory entry and at most one stride plus one frame
(gapped ids still binary search), and that a directory entry with a wrong id,
a wrong offset or a wrong length fails closed. The single-walk projected
decode equals the two-walk reference in rows and admitted bytes on three
records of a directory-backed segment for a bounded table of named
selections: none, each key alone (one per encoded value type of the
fixture), exactly the eight-value located capacity, nine values (which falls
back to the two-walk path), every key, and absent keys; each admission sees
the reference bytes before decoding and a refusal decodes nothing. The root
case compares out-of-core projected rows with the resident
store before and after committed uncheckpointed updates, deletions
(tombstones) and creations, with spilled selected and unselected values, and
after a checkpoint and reopen. Structural assertions use the test-only
`point_frames_examined` and `point_directory_entries_examined` counters.
Replay with `cargo test --locked -p hawdb-storage point_directory`, `cargo
test --locked -p hawdb-storage point_read_scope` and `cargo test --locked -p
hawdb --lib api::tests::point_read_reuse`. No lane, profile or deadline
changes.

Shared graph-slot cleanup (#1615) is owned by executor
`binding::graph_slots::tests` in the existing executor unit lane. It proves
that discarding a slot never clones its shared record, leaves retained output
snapshots intact, and releases the final slot array; owned `remove` still
returns an independent record. MATCH, OPTIONAL and hash-join cleanup use this
discard path. The ignored `shared_slot_cleanup_measurement` compares 10,000
owned-removal/discard operations at 64 B, 4 KiB and 64 KiB payloads, asserting
the exact clone count and copied payload bytes. Replay with `cargo test
--locked --release -p hawdb-executor --lib shared_slot_cleanup_measurement --
--ignored --nocapture --test-threads=1`. This is a cleanup-kernel measurement,
not full-query latency or process-RSS evidence. No lane/profile/timeout changes.

The adaptive execution family extends those existing executor owners with
encoded non-expression key selection, complete-record validation, 64-KiB nested
payload forwarding, descriptor/memory-driven 8/4/2-way merges, and live host
pressure releasing sort capacity. The existing sort differential campaign
includes the wide-record, fan-in and pressure controls, plus sustained-pressure
TopN cutoff preservation (4,096 inputs, 48 retained, no spill for ascending or
descending arrival). Executor kernel tests bound soft-target polling to once
per 64 safe checks without delaying hard refusal or the progress floor. The admitted hash-join
unit family covers pressure arriving after build completion without duplicate
probe output. QoS `runtime::memory_target::tests` owns unchanged grants, child
propagation, mixed foreground/background tasks, recovery hysteresis and
unavailable process feedback halving work areas while admission stays closed.

The vector-projection unit owner also covers filtered execution feedback at
0.1%, 1% and 10% eligible fractions, contiguous/sparse IDs, exhaustive filtered
ground truth, productive traversal, insufficient work and mid-scan cancellation.
Its additional manual-only
`//crates/vector-projection:hawdb_diskann_feedback_fuzz_tests` target runs those
controls with the existing small-beam recall differential case. Do not add this
campaign to CI. Existing report-value differential tests cover the new adaptive
scan counter. The release-only `spill_resource_probe` example measures identical
small, wide, many-run and four-query workloads; `--governed` binds foreground
permits to one shared governor and process policy. It is not a CI performance gate.
See [the implementation and measurement contract](specs/ADAPTIVE_EXECUTION_MEMORY_SPEC.md).
No existing feature matrix, test deadline or compilation profile is removed.

The executor `scan::indexed_tests::bounded_index_projection` family covers
admitted scalar-index projection reads with work budgets, single/multiple keys,
NodeId-ordered duplicate merging, cross-property union deduplication, missing
index fallback, residual predicates, cancellation, work refusal, early limits,
payload/consumer refusal refunds, and pinned snapshots across update/delete.
Empty input and early Stop preserve a prior or callback-caught work refusal.
Its 256-row resident fixtures and complete-scan controls require selective reads
to finish within 32/64 work units rather than enumerate the graph. One unit is
charged before each scalar index probe and each posting-iterator advance,
including end-of-list; heap capacity is admitted and proportional to query keys,
not matching rows. The existing `//crates/executor:hawdb_executor_unit_tests`
owner discovers all cases through `:hawdb_executor_tests`; replay with
`cargo test --locked -p hawdb-executor --lib bounded_index_projection`.
The existing `hawdb_projection_allocation_tests` owner retains before-copy
payload-admission coverage. No test lane, timeout or feature scope changes.
Root `api::tests::query_execution::bounded_scalar_index_queries_rebind_in_and_union_parameters`
also checks ordinary parameterized Cypher equality/IN/OR, fused projection plans,
ordered output and parameter rebinding under a 64-unit task budget. Its existing
general root owner runs both default/minimal selections.

Conjunctive Thread identity unions are covered by root
`api::tests::thread_metadata_reads::union_residual` and optimizer
`graph::tests::plan_structure::union_residual`, plus private access-path proof
controls. Existing root general default/minimal and optimizer-graph CI owners
retain these cases. The fixtures compare indexed/unindexed COALESCE and typed
CASE/trim Space reads, parameter rebinding, native-node overlap deduplication,
logical-id multiplicity, scope before LIMIT, missing indexes, the 64-lookup cap,
complete residual predicates, cost competition, unsafe/UNKNOWN error fences
(including non-string regex operands with missing union keys in both conjunct
orders), proof limits, unrequested 64 KiB bodies, typed query-memory refusals and
canonical
pinned snapshots across update/checkpoint/reopen. Replay with
`cargo test --locked -p hawdb --lib union_residual` and
`cargo test --locked -p hawdb-optimizer-graph --lib residual_union`.
Candidate counters measure five unique matching identities in the synthetic
fixture; they do not establish physical I/O or route latency. No test lane,
feature matrix, profile, timeout or exclusive measurement gate changes.

The `api::tests::materialized_views` family covers synchronous SQL view DDL,
projection/group retractions, differential SELECT equality, admission refusals,
rollback, concurrent commits, cascade deletes and WAL/checkpoint recovery.
Its `extrema` submodule covers retractable BIGINT/TEXT MIN/MAX, NULL-complete
source index admission, affected-prefix row budgets, exact creation row costs,
visible/unique view indexes, and Mem Thread message summaries with indexed
parameterized reads in shadow/demand-paged modes. The Mem schema fixture is
pinned in the test and guide; it does not claim legacy timestamp parsing parity.
It belongs to the existing root general unit lane and Cargo library tests;
local replay: `cargo test --locked -p hawdb --lib api::tests::materialized_views`.
Fixtures are small and do not introduce a new fuzz or CI lane. Supported modes
and limits are documented in [the implementation guide](MATERIALIZED_VIEWS.md).
The existing storage unit lane also covers admission-aware view recovery, WAL
decode parity, indexed MIN/MAX schema copying, and reservation release through
`cargo test --locked -p hawdb-storage --lib controlled_materialized_view_recovery`.
Storage's `relational::materialized_view::tests` checks the 250,000-row default,
the unchanged 64 MiB payload ceiling and exact refusal boundaries through counter
charges, without allocating large backfills. The existing storage unit target
checks that explicit maintenance limits also set hydration row, compressed-byte,
decompressed-byte and working-memory bounds. Its decoder matrix checks that WAL
row admission covers default mutations and checkpoint decoding covers WAL row,
value, schema and record shapes; independent logical/encoded byte limits are
not required to be equal. The same target
also owns `wal_recovery_ignores_foreground_work_limits_and_preserves_view_maintenance`
(two source rows and one COUNT view), decoder/default row-bound compatibility,
and both row and replay-access encoder/decoder admission parity with two-entry
fixtures and injected one-entry bounds. Replay:
`cargo test --locked -p hawdb-storage --lib wal_`.
The existing recovery preparation/discard case additionally lowers foreground
payload admission to one byte; recovery still stages and retries without
changing live state until installation. No CI lane, feature or timeout changes.

The `api::tests::search_indexes` family ([#1654](https://github.com/nowledge-co/hawdb/issues/1654))
covers engine-owned SQL search indexes through SQL only: `CREATE`/`DROP SEARCH
INDEX` guards and catalog visibility, `text_search` argument and shape refusals,
ordinary `EXPLAIN` without execution and `EXPLAIN ANALYZE` search-work evidence,
BM25 results compared bit-for-bit with a brute-force oracle over `SELECT *
FROM events` (Latin, Chinese, Japanese and Korean text) after inserts, updates, primary-key
moves, NULLs, deletes, upserts and drop/re-create backfill, WHERE-before-top-k
filtering with outer ORDER BY/LIMIT/OFFSET, explicit transactions and rollback,
WAL replay, checkpoint reopen, a torn WAL tail (strict refusal; repair drops the
incomplete transaction's rows and postings together), Shadow/DemandPaged index
modes, OutOfCore residency, Authoritative refusal, branch isolation, four
concurrent writers, transaction-budget refusal of an oversized backfill, an
oversized document and an 800,000-ideograph pseudo-random CJK document (refused
during admitted analysis, with its row), and coexistence with a materialized view. Three cases own
the compact postings layout: tails sealing into 128-posting blocks for terms in
every, every other and every fourth document, older documents gaining, changing
and losing terms inside sealed blocks at block boundaries, and one transaction
splitting an overgrown block; 300 updates of one document keeping its ordinal
and leaving bounded rows; and delete-heavy churn compacting shrunken blocks,
re-insertion after compaction, and the relation emptying with its last
document; every layout check also verifies each block bounds row against its
block and the lengths of its documents. A focused compaction regression
empties the retained first block while its successor has 192 postings, then
shrinks that successor to 19 postings and verifies the merged length bound,
remaining rows and exact search results. The `scan` submodule owns the
`Bm25IndexScan` access path: exact top-k and stream-filtered top-k equal to
the oracle (ids and score bits) after documents in early, middle and late
blocks shorten (`search_index_scan_stays_exact_after_documents_shorten`); a
one-row top-k over 3,000 documents decoding a small fraction of
the postings with most blocks never read (asserted from `EXPLAIN ANALYZE`);
filters by bounded over-fetch (several rounds, the stream's own top-k,
bounded hydration, a filter nothing satisfies); LIMIT pushdown in rank order
only; the EXPLAIN operator tree and ANALYZE fields; and the concurrency
footprint (source writers do not conflict with writers of other tables or
rows, and a serializable `text_search` read conflicts with a concurrent
source change only). The generated differential campaign
`search_index_scan_generated_campaign` (four random Zipf-like corpora, random
queries, `k`, stream filters and churn; the corpus seed is in every failure
message) is `#[ignore]`d, local only and not run by CI:
`cargo test --locked -p hawdb --lib search_index_scan_generated_campaign -- --ignored`.
The family belongs to the
existing root general unit lane and Cargo library tests; the reopen/mode case
takes about 25 seconds locally and the admission case about 5 seconds. Replay:
`cargo test --locked -p hawdb --lib api::tests::search_indexes`. The storage
unit lane owns the `cjk_ngram` analyzer cases (including
`analyzer_streams_document_terms_and_matches_materialized_tokens` and
`analyzer_refuses_large_cjk_document_before_expanding_it`, which observes the
thread's allocations with `test_allocator` and requires refusal of a
1,000,000-ideograph value within a few times the 64 KiB admission limit), the layout codec cases (varints
at every width and refusal of truncated, overlong and trailing spellings;
posting blocks, block bounds, dictionaries, statistics and lengths rows
round-tripping and refusing unordered ordinals, zero frequencies, inconsistent
counts and bounds; every single-bit flip of a sealed block or bounds row
fails its CRC32C, and a bounds row must agree with its block's verified
header) and the schema codec round trip with unknown-analyzer and
unknown-layout refusal
(`cargo test --locked -p hawdb-storage --lib search_index`); `hawdb-sql`'s `tests::search_index` covers the DDL and table
function grammar (`cargo test --locked -p hawdb-sql --lib search_index`).
`hawdb-executor`'s `bm25::tests` own the shared scorer and Block-Max WAND
kernel: brute-force equality (hits, keys, score bits) on named cases (empty
input, block boundaries, loose length bounds), ties among identical
documents ordered by key, and a 100,000-document corpus on which a one-row
top-k decodes under 5% of the postings
(`cargo test --locked -p hawdb-executor --lib bm25`; well under a second).
The 400-case generated campaign `block_max_wand_generated_campaign` is
`#[ignore]`d, local only and not run by CI; each failure names its case
seed, replayed by `generated_case(seed)`:
`cargo test --locked -p hawdb-executor --lib bm25::tests::block_max_wand_generated_campaign -- --ignored`.
`hawdb-relational`'s `query::tests::text_search_admission` indexes 4,096
documents sharing one term (and 16,384 in a second corpus) and requires both
top-10s to run within one 512 KiB blocking budget (the full-ranking scan
needed 56 bytes per match, 917 KB for the larger corpus), skipping most
blocks unread, to be refused by the `Bm25IndexScan` account under a tiny
budget, and to honour cancellation. The budget charges the fixed lengths
directory, all decoded entry slots (including deleted slots), and key bytes;
it replaces the earlier 320 KiB estimate based on encoded lengths payloads.
A sparse-after-deletion regression retains only the last document of each
of 32 lengths rows, succeeds at 128 KiB, and requires refusal at 32 KiB before
half the control's decoded-array allocation bytes are allocated. Its multi-term case on a second corpus
(ten short documents repeating the term) compares exact rows and score bits
with a residual-filtered scan and a whole-match-set ranking across
sealed-block boundaries, visiting fewer postings and skipping blocks
(`cargo test --locked -p hawdb-relational --lib text_search`). The
Bun suite (`bindings/bun/tests/hawdb.test.ts`, Native Bindings matrix) runs the
same SQL through `querySQL` on a persistent database and a read-only reopen.
No new lane, profile, timeout, fixture or fuzz target. See
[the guide](SQL_SEARCH_INDEXES.md).

The manual [IMV work-limit diagnostic](benchmarks/imv_work_limits_2026_10_30/README.md)
measures process peak RSS at the row-work ceiling and near the unchanged payload
ceiling through the public SQL facade. It is an unpublished temporary Cargo host
over the retained Rust source, outside library archives and all CI/test suites.
Large fixtures belong only to this explicit resource measurement; ordinary
admission UTs remain counter-based or use two-row fixtures. The guide records
the exact default-feature checked profile, source/binary fingerprints, replay
commands and the limits of cumulative process high-water measurements.

The `api::query_request::tests` unit family covers compact request collection
through the ordinary read-transaction API. The 2,048-row collection regression
compares rows and payload accounting with validated consumer delivery, checks
owned-result completion/peak charges, rejects row and payload overflows, and
reuses the snapshot after refusal. Existing scoring, access-control, EXPLAIN,
and cancellation cases exercise the same compact collector. Mem bounded read
snapshots use this collector and retain cumulative statement budgets.

`api::tests::knowledge_entity_deletes` retains its six deletion scenarios through
ordinary parameterized requests: incident edges, scoped nonmatches, idless
identity, invalid syntax, read-only refusal and WAL replay. It no longer uses
single-entity deletion adapters or their DTOs. `graph_route_evidence` and
`workload_fixtures` retain private route statement qualification, while the
readiness crate tests its generic reducer with a small parameterized statement.
These remain in their existing root/readiness CI owners; the weekly native
runtime lane also selects the deletion and route-evidence families.

`api::tests::{community_assignments,community_cleanup}` retain their seven
scenarios through bounded ordinary Cypher: scoped and global NULL assignment,
invalid syntax before WAL, preservation of other labels, detach/incident-edge
deletion, empty no-op, one-batch durability and reopen. They use query rows and
commit epochs instead of removed business response DTOs. Their existing root
general unit owner remains unchanged.

`api::dependency_read::tests` checks both Cypher and SQL source mappings under
zero, undersized and sufficient result reservations, plus cancellation and
failed-proof refusal. Consumer catch-up admits result memory separately from
publication working memory. The registered Consumer integration cases in
`mvcc_staging_session` exercise graph/SQL delta and full-source verification,
rebuild, reopen and released admission, including the native Windows lane.
Standalone staging/baseline fixtures explicitly reserve results too; a zero
reservation remains a refusal. Request parity also checks explicit zero SQL
payload limits. CI owns these regressions; the weekly native runtime lane
explicitly selects the prepared-source reservation/cancellation case.

`api::concurrent::lock_dependency_tests` covers incremental pessimistic SQL
locking after disjoint commits, unchanged snapshot reads and read-your-own-writes,
overlapping/ABA changes, empty-range inserts, deterministic queued-wait commit
and rollback, schema barriers, first-statement refresh, and both residency modes
through pinned checkpoint and WAL/checkpoint reopen. It belongs to the
existing root concurrent CI owner. Storage's
`store::read_dependencies::locked_ranges::tests` checks interval boundaries
against the standard-library range oracle, payload/rekey/tombstone summaries,
oversized-key barriers, source/history refusal, metadata admission and a
65,537-stamp fixture with constant point-seek work and bounded broad validation.
The table-guard regression also checks same-value and absent-key primary write
intents under shared/exclusive table guards, disjoint point validation and the
existing broad-work cap. The concurrent API regression checks the corresponding
stale broad UPDATE without a wait in both residency modes. Its saved seed42/case147
is `tools/mvcc-fuzz/cases/stale-broad-after-same-value.mvcc`; use the existing
standalone tool's `replay` command. No new CI campaign or timeout is introduced.
The existing storage CI owner runs it. Focused replay uses
`cargo test --locked -p hawdb --lib lock_dependency_tests` and
`cargo test --locked -p hawdb-storage --all-features --lib locked_ranges`.
Required manual MVCC and local fuzz campaigns keep their existing owners and
profiles; these resource assertions do not measure transaction throughput.

`api::concurrent::composite_lock_tests` extends the same root concurrent owner
with composite order-prefix mutations, exclusive/inclusive suffix boundaries,
empty-range inserts, secondary/primary lock coordination, nullable secondary
entries, literal/parameter rekeys, and deterministic queued refresh/deadlock
schedules, plus later-key validation after implicit table escalation. Full-scan
waits retain validation of unchanged/absent write intents, and contradictory
single-key predicates remain lock free. Both
materialized and out-of-core fixtures reopen checkpointed data
before exercising locks. Candidate discovery uses the ordinary pinned SQL row
and index readers, caps output at 65 rows and 1 MiB, and bounds candidate work;
more than 64 discovered lock entries or 1 MiB of staged key values escalates to
a table guard. The suite checks large assignment parameters before key fanout and
that escalation cannot expose a partial lock set, including a `LIMIT 1` query.
Sparse 5,000-row residual scans exercise internal work-budget escalation for
locking reads, UPDATE and DELETE, including autocommit and both primary-key
shapes/residencies. Typed work/output refusals distinguish internal probe caps
from caller limits; smaller caller limits still fail without table escalation.
Hydration retains caller limits while the streaming probe bounds visited rows.
Relational aggregate unit tests, `tests/execution_request_contract.rs` and
Python `test_read_controls.py` pin the public work-refusal contract: Rust
`ResourceBudgetExceeded` / Python `ResourceBudgetError`, with query-scoped
work units and the originating budget option. Existing cancellation, output
and memory refusal controls remain distinct in their current owners.
DB-API pickle regressions separately retain generic execution-error attributes
and typed work-budget attributes with their respective PEP 249 classifications.
Focused replay: `cargo test --locked -p hawdb --lib composite_lock_tests`.
Constraint-key changes and nonliteral index rekeys retain conservative guards;
nullable index version validation retains its existing table dependency.

`api::sql_runtime::tests` passes prepared SQL mutations through the database and
transaction kernels with diagnostic-only text, detecting accidental parser
fallback. DDL, INSERT/RETURNING, UPDATE, DELETE and Strict Append preserve bound
parameters, output refusal, earlier transaction work and publication. Root
general and the native runtime lane own these cases.
The same owner reuses a cached UPSERT across replacement values, transaction
rollback and a second database with a different key type, proving that cached
syntax still binds the current schema and parameters.
The relational crate's
prepared-mutation cases rebind current schemas/parameters and append routing;
its existing manual statement compiler campaign reuses templates across inputs
and compares independent typed writes. No new fuzz CI owner is introduced.

`hawdb-sql::template_cache::tests` keeps branch-catalog inspection out of the
selected branch's data-query cache without reparsing its returned template.
The same SQL crate unit owner covers separately bounded parsed DML retention,
INSERT/UPSERT/UPDATE/DELETE reuse, SELECT isolation, disabled/oversized retention,
churn and concurrent immutable hits. The mutation-retention child tests charge
spare vector/string capacity and nested expressions. DML keeps current-state
binding/compilation; only parse templates are retained (256 entries, 4 MiB total,
256 KiB per entry, reduced or disabled by the existing entry configuration).
The existing root recovery
`branch_lifecycle` family preserves the ordinary database cache-isolation check.

The executor `result_delivery::tests::borrowed` family checks native columnar
payload pointer identity for immediate consumers, compact validated delivery
across source retirement and segment boundaries, logical quota refusal before
callbacks, cancellation/error/unwind release, schema rejection and nested-copy
admission independent of source spare capacity. The executor unit owner runs
these regressions in CI. Root `api::tests::query_execution` checks the public
borrowed streaming entrypoint's compact views and zero-prefix budget refusal.
The weekly native runtime lane also selects these borrowed-result cases on
Windows and macOS. These do not yet qualify a native scan-to-consumer production
path.

The `api::query_request::adapter_tests` family exercises shared Cypher and SQL
requests across database, pinned-read, explicit-write, concurrent optimistic and
pessimistic, and Embedded transaction paths. Cases include parameters, scoring,
ACL availability, zero/output limits, parent/request cancellation, prepared-policy
invalidation, SQL RETURNING and Cypher SET RETURN refusal before private staging
or publication. A changed ACL requires fresh preparation; a missing ACL capability
retains its typed error. Branch-switch cases retain the distinct branch error.
Cypher cases retain earlier successful statements through refused
updates in both locking modes, enforce exact/zero row and payload limits, and
distinguish aggregate result size from affected-node count and zero matches.
Persistent coverage reopens a database after refused SET RETURN statements and
checks that only successful transaction statements survived. The executor
mutation-preflight owner independently proves output refusal precedes any storage
write call.
The production qualification fixture counts separate preparation/execution
admissions. Its small mixed soak funds both phases and bounded stream buffers
with 3 MiB, retains a raw dataset twice that size (768 nodes, 8 KiB payloads),
and preserves the 32 KiB blocking-operator/64 KiB segment-cache limits, complete
query counts, checkpoint advancement and final zero-admission assertions.
`api::query_request::host::tests` also covers host-scorer delivery through the
concurrent and transaction entry points. These remain root-unit regressions;
the weekly platform runtime lane exercises the request families with ACL enabled
and disabled. Core cancellation tests cover linked cancellation/waker cleanup,
controller preservation and common-ancestor work accounting in that lane.

`api::query_request::budget_tests` exercises one mixed Cypher/SQL read budget
while concurrent writes advance, rejects cumulative row/payload overflow without
debiting failed owned results, and charges EXPLAIN's delivered report. These
cases share the request family's root-unit and weekly platform owners.
`embedded_tokio::request_tests` covers owned request attachments, ACL enabled
and disabled, host-scorer invocation/disposal, streaming ceilings, cancellation,
admitted SQL template reuse with rebound parameters, SQL/Cypher RETURNING rejection
before commit with unchanged epochs and refunded admission, and refusal while an
explicit Embedded transaction is open. The weekly
runtime lane runs the complete Tokio family with `tokio-runtime`, then these
request cases with `tokio-runtime,acl`. Its saturated-admission case also proves
SQL waits for capacity before parsing. Native CI owns execution; local strict
Clippy checks compile the same test sources.

The capability-based [API naming migration](PUBLIC_API_NAMING.md) moves the
embedded store families to `embedded_store::`. Candidate facade identity and
entrypoint checks remain in `embedded_store::candidate_facade_tests`, and the
search-owned builder, report and shadow cases retain their existing search
test targets and feature matrices. The renamed families keep their execution
lanes and assertions.

`embedded_store::concurrent_owner::tests` requires a second writer to commit while
the first transaction callback remains suspended, then checks both the retained
old snapshot and the newly published state and released admission memory. It
uses the existing App snapshot fixture and root general unit owner.
The weekly native runtime lane also runs this family and
`embedded_store::app_read_snapshot_tests`, preserving their mixed-statement,
publication-failure, unwind, admission and old-generation assertions after Mem
switches to the concurrent transaction kernel.

`api::concurrent::graph_preparation_tests` compares actual planner bypass counts
with pessimistic execution/replay counters. One statement prepares once even when
new locks require two executions; the next statement rebinds its parameters and
uses the staged schema. Refused output restores the statement without dropping
earlier writes or their locks. This is owned by the root general unit suite and
the macOS/Windows runtime lane. Existing request, lock and serializability
families continue to cover the shared execution kernel; preparation counts are
not latency or allocation measurements.

The same family also pauses a sealed graph transaction between compaction and
WAL encoding, proves a disjoint writer commits during that pause, and verifies
that an intervening conflicting writer is rejected before WAL and stays rejected
after reopen. `api::concurrent::publication::tests` covers borrowed read-only
Cypher views across publication generations, mutable/EXPLAIN fallback, limits,
cancellation, reports and private observation refresh. These additions retain
the existing root general/concurrent owners, checked profile and timeouts.

Storage's existing unit owner covers cached COW page weights through replacement,
mutable guards, splitting, retain/unwind and saturating payload sizes;
relationship record sharing preserves old snapshots and untouched payload
addresses. `version::page_history_tests` exercises page-owned history charges,
last-owner refunds, unmanaged-root adoption, epoch-cache invalidation and pruning
without spare history budget. Existing retention, checkpoint admission and WAL
codec/recovery families remain their complementary owners. Manual release-only
`cargo bench --locked --bench mvcc_hot_paths` compares identical fixed-work
harnesses on baseline and candidate for small/large COW pages, pinned/unpinned
relationships, concurrent Cypher reads and synchronized graph commits including
reopen validation. It is excluded from CI; measurements and limits are linked
from [the MVCC validation protocol](MVCC_COMMIT_VALIDATION_PROTOCOL.md).

`api::concurrent::autocommit::tests` covers ordinary Cypher writers sharing a
label but updating distinct unique keys, readers progressing while private
execution is suspended, intervening commits without replanning, excluded unique
candidates, schema invalidation, cancellation/output refusal, and durable create
IDs after reopen. The root concurrent suite and macOS/Windows runtime lane own these
cases, along with out-of-core unique lookup, EXPLAIN ANALYZE rejection, mutation
summary ceilings and expression/NULL predicate parity.
The expression fallback uses a supported `COALESCE` comparison. The concurrent
read-observation cases pause SQL and Cypher private execution before publication;
post-commit telemetry is not a prepublication synchronization point.
`embedded_store::read_execution::tests` additionally exercises the actual
admitted report adapter with two such writers, captured plans, one preparation
per statement and full admission refund. These are correctness/operation-count
checks, not release latency or allocation evidence.

`embedded_tokio::concurrency_tests` covers both ordinary languages with two
writers and advancing readers, preparation reuse across intervening commits,
direct-host mutation exclusion, database replacement refusal, cancellation and
complete admission refund. It uses the same concurrent kernel phase gates as the
facade regressions. Root Tokio and the existing macOS/Windows Tokio-entrypoint
lane own this family; these are correctness checks, not release benchmarks.
Planning regressions now test compatibility across data/checkpoint changes,
configuration refusal and durable execution/reopen rather than the retired
whole-publication equality fence. Existing explicit transaction fences remain.

`api::concurrent::autocommit::sql::tests` covers two ordinary SQL point writers
and advancing readers, old publication release before lock waiting, fresh-snapshot
arithmetic, changed constraint scopes, excluded predicate candidates versus
explicit writers, independent table inserts with UUID defaults, FK cascades,
RETURNING refusal, precommit cancellation and durable reopen. Prepared DDL/Strict
Append execution receives diagnostic-only text to detect parser fallback. The
root concurrent unit suite and an exact required macOS/Windows runtime filter own
these cases. Existing explicit-transaction stale-snapshot tests remain unchanged.
These assertions establish scope/ownership behavior, not release speedup.

`HawDBAutocommitScope` joins the existing `docs/tla:storage_models` positive
owner. Its three manual controls (`UnprotectedPredicate`, `EarlySnapshot`,
`IgnoreSchema`) distinguish predicate protection, snapshot ordering and admission
invalidation. They do not replace positive checking or native scope-mapping tests.

`embedded_store::read_execution::tests` covers ordinary owned/streaming and report
reads from published views: streaming and report-observation callbacks must allow
a writer and fresh reader to finish, and execution-admission telemetry must
release the preparation view in both ordinary and report entrypoints.
Data-only commits remain reusable; schema and database-instance changes reject
the old preparation before execution. Every case checks released admission
memory. Report cases also require one cache lookup per preparation and preserve
physical-plan capture with rebound parameters. External text/vector seed reports
admit the published projection's I/O depth without retaining its generation while
waiting; graph-only reports and plain EXPLAIN do not reserve projection I/O.
A projection I/O-depth change during admission rejects the old request and
releases its preparation, execution memory and I/O reservations.
The root general unit owner and weekly runtime lane execute this family.
Production resource-profile cases in the same family require completion while
the publication owner is held, live cache observations with the original durable
and manifest metadata, one preparation, host/statement ceilings and full refund
after refusal. Admission telemetry advances data, schema or the database instance;
profiling releases its preparation view and rejects stale evidence against the
execution epoch. `store::tests::detached_snapshots_preserve_live_cache_and_captured_manifest_observations`
additionally checks nested snapshots after their writable owner closes. These
observations are shared cache/process metrics, not per-query concurrency attribution.
The family also checks schema guidance and generated queries against the last
published view while a direct writer holds the owner. Generated reads use one
preparation lookup, retain parameter/row/payload checks, and reject a stale
generated context after an ordinary data commit during admission. Plain EXPLAIN
observation covers read, DML and DDL plans while a writer and fresh reader finish.
Generated parameter rejection uses an explicitly typed `Thread.id` property; an
undeclared `Any` property correctly accepts integers. Both admitted generated-read
entrypoints reject invalid parameters before preparation and leave the plan-cache
counters unchanged; snapshot epoch validation still occurs at execution.
EXPLAIN observation uses a
separate 64 KiB host result budget for retained plan/schema metadata; ordinary
App reads keep 16 KiB. Failed observations surface the original query error
after releasing the waiting writer, without panicking inside telemetry.
`api::query_runtime::tests` checks that plain EXPLAIN never executes write plans,
ANALYZE rejects DML/DDL, zero diagnostic budgets fail, and direct/forked read
snapshots retain captured query settings with statement-local overrides. These
tests use the existing root general and weekly runtime owners and features.
Small logical-payload cases preserve successful bounded reads and typed payload
refusals while separately testing a stricter caller result-memory reservation and
zero delivery on refusal. Governor counters include both preparation and execution
admissions, with complete release after each statement.

The App snapshot family also exercises out-of-core generation replacement through
the actual Mem maintenance entrypoint while a snapshot callback remains active.
It checks the old graph/projection pair, the newer pair, generation advancement,
release of the old reader after callback completion, and refunded admission.
The FD-pressure retry case holds descriptor reservations on another thread so
refresh cannot borrow them; it checks the old pinned reader and same-handle retry.
This covers `SearchOutOfCoreReader::refresh_shared` through its embedded host;
the resident fixture now applies three graph/projection updates while an old
snapshot remains active and checks fresh/old pairs plus final refund. Resident
report-observation tests cover ordinary and external text queries while a writer
and a fresh reader advance. Search resident-generation units cover shared funding,
capacity refusal/retry, last-owner release and frozen freshness in query reports.
The existing Search feature matrix and root runtime lanes own these cases.
The resident text-seed fixture requires the graph, SQL and projection writer to
finish before the retained snapshot callback returns, then verifies its old
results and epoch remain stable. Incremental out-of-core helper fixtures submit
documents in the generation writer's required strictly increasing ID order.
The text-seed fixture also exercises the report entrypoint and EXPLAIN ANALYZE
with the captured out-of-core external operator. Plain/analyzed explain rows
have an explicit 1 MiB diagnostic cap and debit their delivered row/payload from
the cumulative snapshot ledger; ordinary reads keep their 8 KiB snapshot cap.

The root general `embedded_store` family exercises out-of-core text, hydration,
graph context and `CALL vector_search` with default DiskANN and explicit RaBitQ
artifacts. It checks typed ANN/raw score sources and the raw-scan behavior of
disabled compression. Replacement/shadow evidence fixtures explicitly select
RaBitQ because their compression gate remains required; sampled default-backend
recall retains DiskANN. `production_search::tests` likewise builds explicit
RaBitQ artifacts for its required/preferred compression lifecycle probes.
The default/minimal staging session family counts both deleted native keys and
legacy document tombstones before reducing the complete pinned source interval.

## Execution lanes

| Lane / entry point | Problems covered | Execution policy |
| --- | --- | --- |
| Prow root: `//:hawdb_unit_tests`, `//:hawdb_system_sql_loom_tests`, `//:hawdb_linux_ci_smoke_tests`, CLI/shadow and package-parity targets | Embedded facade behavior, concurrent publication, recovery, public consumers, compatibility protocols and benchmark evidence | Required PR and mainline regression coverage. The PR lane excludes the explicit checkpoint targets below; the mainline lane retains the aggregate coverage. Fuzz is excluded. |
| Prow crates: `//:hawdb_presubmit_crate_tests` | Internal component contracts and exact Search feature profiles | Required checked-profile component coverage (the release-only observer target runs in the native lane). Each package owns `:presubmit_tests`; manual-only targets must not accidentally enter implicit suite discovery. |
| Prow checkpoint growing-table presubmit: `//:hawdb_unit_checkpoint_headroom_growing_table_tests` | Complete steady-insert growing-table headroom regression | Required PR context `ci/skein-bazel-test-checkpoint-growing-table`; exclusive local test execution over the shared checked root binary. |
| Prow checkpoint large-commit presubmit: `//:hawdb_unit_checkpoint_large_commit_tests` | Uninterrupted whole-district and oversized-commit checkpoint controls | Required PR context `ci/skein-bazel-test-checkpoint-large-commit`; exclusive local test execution over the shared checked root binary. |
| Prow checkpoint controls presubmit: `//:hawdb_unit_checkpoint_headroom_controls_tests`, `//:hawdb_unit_checkpoint_lifecycle_tests`, `//:hawdb_unit_checkpoint_large_commit_interrupted_tests` | Remaining headroom controls, lifecycle, cancellation, deadline and process-stop cases | Required PR context `ci/skein-bazel-test-checkpoint-controls`; exclusive local test execution over the shared checked root binary. |
| Prow dense analytics: `//:hawdb_unit_analytics_rss_tests` | Persistent graph identity admission, edge streaming and bounded incremental RSS in a fresh process | Separate exclusive target; dense fixture construction is outside the measured child's RSS baseline. |
| Prow storage crash recovery: `//:hawdb_storage_crash_recovery_tests` | Subprocess termination at persistence boundaries, complete transaction recovery, endpoint integrity and artifact generations | Dedicated job binds evidence to source/toolchain/features. Process termination does not alone establish power-loss safety. |
| Native bindings: `//bindings/ffi:hawdb_ffi_tests`, `//bindings/go:hawdb_go_tests`, `bindings/bun`, `//bindings/python/...` | C ABI retention, host ownership/errors, Go integration/race checks, Bun FFI ownership/lossless `i64` behavior, reentrant close and sparse-array refusal, and Python behavior | Binding-specific PR/mainline workflows; the Unix Native Bindings matrix runs Bun against the same Bazel-built C ABI as Go. Bun lifecycle regressions run an instrumented internal native bridge in isolated child processes (5s per child), covering parameter/option getters that close a database or library, deterministic pointer-address reuse, and healthy-call ownership; the real-engine suite remains in the parent process. Open options (#1656): C ABI `open_options_tests` and Bun `tests/open-options.test.ts` cover every `hawdb_open` key (durability observed through `SHOW CURRENT BRANCH`, row and payload budget refusal, the monotonic-append fast path with out-of-order/duplicate fallback and reopen), unknown-key and invalid-value rejection before the database path is created, and defaults restored on a reopen without options; replay with `bazel test //bindings/ffi:hawdb_ffi_tests --test_arg=open_options` and `bun test tests/open-options.test.ts` (needs `HAWDB_LIBRARY`). Backup, restore and scrub (#1658): C ABI `maintenance_tests` and Bun `tests/maintenance.test.ts` back up an open database, keep writing, restore into a fresh directory with an identical report and reopen it at the backed-up state; scrub the original, a read-only handle and the restored copy; flip a byte of the served canonical checkpoint in a restored copy and expect a CRC32C/SHA-256 scrub failure that poisons only that handle; and refuse a damaged backup, an existing destination, read-only/in-memory backups and NULL arguments without creating the destination. Bun lifecycle scenarios `backup-close`, `backup-reuse`, `restore-backup-library-close` and `restore-destination-library-close` pass boxed paths whose conversion closes the database, closes and reopens one at the same address, or unloads the library, and assert that nothing is dispatched; `maintenance-healthy` is the control. Replay with `bazel test //bindings/ffi:hawdb_ffi_tests --test_arg=maintenance_tests` and `bun test tests/maintenance.test.ts` (needs `HAWDB_LIBRARY`). Transactions (#1657): C ABI `transaction_tests` cover SQL and Cypher statements committing together across reopen, rollback, use after commit/rollback (`no_transaction`), nested begin and autocommit beside an open transaction (`transaction_open`), a failed statement followed by rollback, close-with-open-transaction rollback, and a read-only handle; Bun `tests/transaction.test.ts` covers commit-on-return, rollback-on-throw, misuse errors, and crash/reopen through `tests/fixtures/transaction-crash.ts`, a child that SIGKILLs itself inside the transaction body (before commit is called) or after commit returned (30s per child), after which the parent sees none or all of it respectively. A kill between durable publication and commit's return is not exercised; that lost-acknowledgment outcome is documented as unknown but never partial. Replay with `bazel test //bindings/ffi:hawdb_ffi_tests --test_arg=transaction_tests` and `bun test tests/transaction.test.ts` (needs `HAWDB_LIBRARY`). JSON tests cover top-level/nested sparse arrays and dense null/empty-list controls. Replay with `bun test tests/json.test.ts tests/lifecycle.test.ts` from `bindings/bun`; these targeted tests need no native library. Windows contracts and scheduled qualification are distinct receipts. See workflow definitions for current triggers. |
| Swift: root `Package.swift`, `bindings/swift/Tests` | Owned C ABI results; exact signed/unsigned 64-bit codecs and native tags; unmodified map/parameter names; SQL/Cypher, empty/NULL results, error recovery, persistence/reopen, read-only and budget refusal; transaction commit/rollback, nesting and escaped-handle refusal; concurrent query/close and deinit cleanup; downstream SwiftPM product/module-map consumption | Existing Native Bindings Linux/macOS matrix, using its existing Bazel-built shared library. Ubuntu uses Swift 6.0.3; macOS uses the runner's Swift 6+ toolchain. `swift test -j 2 -Xswiftc -warnings-as-errors` runs the complete XCTest inventory, followed sequentially by `swift run --package-path bindings/swift/Tests/Consumer -j 2 -Xswiftc -warnings-as-errors`, both with explicit native library search and runtime paths. See [Swift replay commands](../bindings/swift/README.md#verification). No crash/power-loss or Apple mobile-platform qualification is implied. First build includes Swift module compilation plus the shared existing engine build; no new Rust build profile or benchmark lane. |
| Swift iOS: `.github/workflows/swift-apple.yml`, `bindings/swift/build-apple.py`, `bindings/swift/test_build_apple.py` | Device arm64 plus simulator arm64/x86-64 archives; refusal of missing slices, stale headers, and unrepresented native link dependencies; generated SwiftPM binary consumption and all 21 Swift codec/query/lifecycle/transaction cases in an iPhone simulator | PR/mainline path-scoped macOS 15 lane. Builds default-feature static archives sequentially under the repository release profile, two Cargo jobs, then strict release-profile iOS C ABI Clippy for all three targets. Generic-device Swift compilation and the complete simulator XCTest suite follow sequentially, with parallel testing disabled. Uploads the source-bound package only on success, with independent logs/xcresult evidence. Portable packaging refusal controls run first. Physical-device runtime, App Store and power-loss acceptance are separate; see [Apple replay](../bindings/swift/README.md#ios-and-xcframework). First build includes three native release compilations, target-specific Clippy metadata and Swift/Xcode module builds; no speedup is claimed. |
| Darwin host primitives for Swift iOS | Storage `file_io::rename_new::tests` and QoS `process_memory::tests::peak_rss_uses_native_platform_units`: native exclusive directory publication, current-process Mach RSS, and byte-valued Darwin peak RSS without Unix KiB multiplication | Existing storage/QoS Rust lanes retain every test and profile. Apple cfg branches now include iOS, while the existing non-Apple fallback is unchanged. iOS C ABI cross-target strict Clippy compiles these library paths; simulator Swift persistence/concurrency tests exercise the linked engine. Replay `cargo test --locked -p hawdb-storage file_io::rename_new::tests` and `cargo test --locked -p hawdb-qos process_memory::tests`. |
| MoonBit Native: `//bindings/moonbit:hawdb_moonbit_tests` (Rust lifecycle + real-engine conformance) | Direct Rust adapter builds against `//:hawdb`; Rust ownership, codec (including exact Decimal text, malformed-tag refusal and literal marker maps), autocommit/transaction separation, SQL/Cypher, transactions and durability contracts | Native Bindings Bazel Linux/macOS matrix runs the toolchain-free `//bindings/moonbit/native:hawdb_moonbit_rust_tests` on every engine change. `.github/workflows/moonbit-native.yml` runs the `manual`-tagged executable conformance through Bazel on Linux/macOS and through Cargo on Windows, for SDK changes and mainline engine changes; the MoonBit compiler must be on `PATH`. |
| Python release: `.github/workflows/pypi.yaml` | Released tag/version/main-history agreement, reviewed shared dependency identities, byte-identical sdist lock/profile and tool exclusions, five native wheel builds from that sdist, installed default/Pydantic binding tests, typing/license/metadata payloads and PyPI file hashes | GitHub Release publication or explicit replay of an existing release tag. Linux x86-64/AArch64 use manylinux 2.28; macOS x86-64/AArch64 and Windows x86-64 build natively. Reuses `bindings/python/tests` on CPython 3.11; the final public install smoke is Linux x86-64 with bounded index/CDN retries. No fuzz campaigns or new presubmit test lane. Build cost includes a first release-profile compilation on each platform. See [publishing](../bindings/python/README.md#publishing). |
| Native C ABI release: `.github/workflows/native-release.yml`, `//:native_release_package_script_test`, `//:native_release_mode_script_test` | Release-mode mapping: the mode test evaluates the real workflow's job and step conditions with the real `scripts/native-release-mode.py` and proves a stable `v<x.y.z>` release (or its `workflow_dispatch` replay) uploads and publishes under npm `latest`, a `-dev.<n>`/`-nightly.<n>` prerelease under `dev`/`nightly` (`test_each_release_kind_uploads_and_publishes_under_its_dist_tag`), while a draft, a tag/version mismatch and a prerelease flag that disagrees with the version reach no build, upload or npm job (`test_refused_releases_ship_nothing`); `publish-assembled.sh`, run against stand-in `npm`/`jq`, refuses unknown dist-tags, a prerelease version under `latest` and channel mismatches, and never lets `npm` read its package list (a 2FA prompt reading it once failed local publishes with `EOTP`); release tag/version agreement across the root, `bindings/ffi` Cargo/Bazel and `bindings/bun` versions; `--config=prebuilt` C ABI archives for macOS arm64/x86-64, Linux glibc x86-64/AArch64 and Windows x86-64; per-archive manifest (revision, default feature profile, ABI rule, measured macOS deployment target or glibc floor, static-link flags, file hashes); macOS `@rpath` install name and ad-hoc signature; shared and static C links of `bindings/ffi/examples/embed.c` (Windows: import library); the Bun suite against each unpacked archive; `SHA256SUMS`; byte-identical release-asset retries; npm trusted publishing and a public zero-argument install smoke | Publication of a stable GitHub Release (`v<x.y.z>`) or a development prerelease (`v<x.y.z>-dev.<n>` or `-nightly.<n>`), or a `workflow_dispatch` replay of one. Pull requests changing the pipeline build and test every archive without upload or publish. Both script tests are in the required root `//:hawdb_unit_tests` aggregate; the packager test runs with stand-in libraries and mocked `otool`/`objdump`/`readelf` output. Replay with `bazel test //:native_release_package_script_test //:native_release_mode_script_test`, or `python3 scripts/package-ffi-release.test.py` and `python3 scripts/native-release-mode.test.py`. Build cost is a first release-profile compilation on each of the five runners. The npm job runs only when the `NPM_TRUSTED_PUBLISHING` repository variable is `enabled`. See [prebuilt libraries](PREBUILT_LIBRARIES.md). |
| Bun npm packages: `bindings/bun/tests/platform.test.ts`, `bindings/bun/tests/npm-package.test.ts` | Host to `@nowledge/hawdb-ffi-<platform>` selection; glibc/musl detection from the running executable's ELF program interpreter (synthetic images: a glibc process on a host that also has musl installed selects glibc, a musl process stays refused, static/non-ELF/32-bit/unknown interpreters give no libc; on Linux hosts also the real Bun process); unsupported-host, missing-package and other-release refusals (a resolved platform package whose `package.json` version differs from the main package); platform table agreement with the release matrix; packed main/platform tarball contents and zero-argument `loadHawDB()` from a fresh `node_modules` tree, and refusal of an older platform package installed in place of the pinned one | `platform.test.ts` needs no library: `bun test tests/platform.test.ts` from `bindings/bun`. `npm-package.test.ts` uses the harness `HAWDB_LIBRARY` and runs in the Unix Native Bindings Bun step and in every Native Release build (Windows installs the assembled directories instead of tarballs). |
| Python dependency lock: `bindings/python/test_check_lock.py` and `check_lock.py` | Reject shared version/source/checksum drift and local engine version mismatch; accept reviewed multiple-version selections and binding-only dependencies | Existing Python Bindings pip job, before its locked wheel build. Replay: `python -m unittest discover -s bindings/python -p test_check_lock.py` and `python bindings/python/check_lock.py` with Python 3.11+. These pure metadata controls do not join the Bazel native-binding test inventory or compile the engine. |
| Weekly platform and daily Windows abandonment qualification | Supported platform/feature combinations and the complete Windows abandonment/GC fault matrix | Scheduled or explicit workflows. They are not fuzz time budgets. |
| Cypher PEG frontend | The default statement, profiled, pipeline and projected-filter entries use the compiled peg grammar with a Cypher-specific lexer. Selected captures lower to owned ASTs; immutable pre-removal and independently compiled main fixtures cover complete AST/source spans and rejection classes, including the current Mem query contracts. Generated clause/wrapper, contextual-keyword, UTF-8 truncation and backtracking cases cover anonymous-name order, hints, procedure admission and independent work/capture/nesting limits. The core function catalog supplies list/string names. Compact capture tests cover branch span/count boundaries, alias tag overlap, borrowed child slices and arena fallback; property-only scalar lists lower directly without temporary value lists. Private 12-byte lexer tokens retain the existing byte/token limits; range/layout tests cover full admitted offsets, UTF-8, named parameters and EOF. Logical capture budgets are unchanged. Rank CASE lowering has 2,026 independently exported main statement/pipeline outcomes covering literal and parameter branches, mixed references, operators, parentheses, invalid values and nesting boundaries; complete AST hashes include source spans. Independent main diagnostics cover 693 CSV-message, string-decoding, argument-arity, computed-UNWIND location and overflowing-integer location repairs, checking complete error strings and source positions. A third independent fixture adds 815 value/scalar-default diagnostic repairs and four procedure-argument controls, preserving existing correct diagnostics and checking complete messages and positions. A fourth fixture adds1,590 predicate, punctuation and trailing-input repairs (1,548 distinct cases); a Unicode-message/context regression verifies single-buffer diagnostic formatting. SQL retains its separate memoized PEG engine and resource tests. Handwritten drivers, the duplicate Cypher DSL and backend selection feature are removed. | `cargo test --locked -p hawdb-cypher`; `bazel test //crates/cypher:hawdb_cypher_tests //crates/cypher:hawdb_cypher_backtracking_fuzz_tests`. SQL engine regressions remain in `//crates/sql-syntax:hawdb_sql_syntax_tests`; SQL/PGQ integration targets remain unchanged. No new CI or crate boundary. |
| Local Shiro and MVCC tools | Generated SQL/Cypher result relations, parser/storage campaigns, independent Decimal/Cypher/temporal/keyless models, optimistic history checking and composite/secondary/keyless range-lock models | Manual only. Existing `//tools/shiro:shiro_tests` and `//tools/shiro:shiro_cli_tests` own Shiro controls. MVCC range controls use `cargo test --locked --manifest-path tools/mvcc-fuzz/Cargo.toml ranges`; campaigns/replay/reduction are documented in [Shiro](../tools/shiro/README.md) and [MVCC](../tools/mvcc-fuzz/README.md#composite-secondary-and-keyless-range-models). Preserve legacy replay formats; new coverage uses explicit protocols. No CI, scheduling, profile or deadline change. |

## Root regression owners

Namespace durability anchors use the existing storage descriptor tests,
`branch_project_open` integration target and root power-loss family. Storage
cases cover bounded inclusive barriers, unchanged default ancestry, failed
barrier retry/accounting, pre-effect validation, canonical aliases, symlink
escape and live-owner configuration conflicts. Related sibling components
inherit the explicit host anchor while retaining their own finite descriptor
domains; nested components keep the containing domain's capacity. Detached
producer scratch keeps the initiating project's descriptor accounting. Scratch
inside its explicit anchor stops there inclusively; scratch outside retains its
complete ancestor walk. Both paths propagate failed barriers without leaking
permits. Storage directory-sync cases retain failed operation/path/native-source
diagnostics and typed descriptor-budget classification through contextual IO
errors. These use the existing storage unit/native owners without a new lane.
The Windows descriptor case uses a fresh thread with a restricted token, removing administrator-group
authority and privileges without changing machine ACLs. It opens and reopens a
private project and sibling component beneath the OS-owned temp directory. The
embedded facade case round-trips graph and SQL data, opens and reopens the
sibling search store, checks its live descriptor domain's anchor, and refuses missing incremental projections
without creating them. The anchored initial-installation recorder
reuses the complete before/after mkdir, lost-write and committed mixed-transaction
oracles of the default installation case. These cases add no CI job or profile;
the native Windows storage descriptor lane discovers the restricted-token case.
Local replay filters are `namespace_anchor` (storage) and
`namespace_durability_anchor` (facade integration and power-loss owners).
The recorder requires the root `test-support` feature; the ordinary default
unit executable does not contain it. Replay it with
`bazel test //:hawdb_unit_test_binary --//:windows_native_profile=facade_all --test_arg=namespace_durability_anchor`
or `cargo test --locked -p hawdb --all-features --lib namespace_durability_anchor`.
Its existing full-feature qualification owner is the branch power-loss
workflow's `remaining` partition; the native Windows storage shard separately
owns the restricted-token descriptor case.

| Target / family | What it detects | Notes |
| --- | --- | --- |
| `//:hawdb_manual_checkpoint_tests`, `//:hawdb_manual_checkpoint_minimal_tests` | Explicit ordinary/concurrent/embedded checkpoints preserve durable publication independently of background support/admission. Embedded controls retain one same-governor background or foreground I/O permit, distinguish allowed foreground work from legitimate refusal, and check reopen and refunds. | Compatibility labels for the unpublished `hawdb-test-contracts` crate; default merges these five cases with six WAL recovery cases, and minimal retains those five plus all six WAL cases already available under Cargo without default features. Both required root carriers retain the original 300s deadline and use exclusive/single-thread execution. Replay `cargo test --locked -p hawdb-test-contracts --test checkpoint_contracts`, adding `--no-default-features` for minimal. The existing root automatic-checkpoint `publication_io` family also pauses the actual owner with its acquired wave, observes manual suspension, and checks durable completion/refunds under [#1219](https://github.com/nowledge-co/hawdb/issues/1219). |
| `//:hawdb_unit_general_tests` | Embedded API, schema/query and general facade regressions | Shared unit executable; partition script assigns each discovered case an owner. |
| Automatic checkpoint `read_handoff` | Read callbacks, concurrent and SQL reads adopt ready generations; invalid identities fail closed; typed EXPLAIN ANALYZE progresses without the owner mutex | Five existing cases in `//:hawdb_unit_checkpoint_lifecycle_tests`. The seal-pause fixture waits for the owner's handoff-pause acknowledgement before adoption or guard release; observing `Phase::Handoff` alone races probe entry. The invalid-identity case prepares its foreign persistent database before entering the bounded owner pause, keeping unrelated setup I/O outside the 15-second assertion window. Existing query, admission refund, snapshot and reopen assertions and deadlines remain. Replay: `bazel test //:hawdb_unit_checkpoint_lifecycle_tests --test_arg=api::automatic_checkpoint::tests::read_handoff`. Ref [#1116](https://github.com/nowledge-co/hawdb/issues/1116); this fixture race is distinct from other checkpoint/branch-open stalls. |
| `//:hawdb_unit_concurrent_tests` | Transaction locks, read observation, commit admission and concurrent publication races | Known long target: [#972](https://github.com/nowledge-co/hawdb/issues/972). Preserve barriers and observations; a deadline is not a substitute for a deterministic schedule. |
| Covered pessimistic graph mutations (`covered_graph`) and concurrent metrics (`api::concurrent::metrics::tests`) | Retained staged results under existing locks, new-target/upgrade replay, stale-epoch refusal, lock timeout/retry, rollback, failed-statement atomicity, durable reopen, disabled-by-default instrumentation and explicit metrics inspection during writer ownership | Owned by `//:hawdb_unit_concurrent_tests`; local replay with `--test_arg=covered_graph` or `--test_arg=api::concurrent::metrics::tests`. Assertions use work counts and final rows rather than timing thresholds. |
| Typed pessimistic retry errors | Core transaction-error tests cover typed-only classification and preserve optimistic conflict behavior. Root concurrent transaction tests verify SQL/Cypher stale snapshots, repeated query and commit calls, typed deadlock victims, lock release, unchanged rejected graph mutations and usable transactions after statement-level lock timeout. Owned by existing core and `//:hawdb_unit_concurrent_tests` CI; replay with `cargo test --locked -p hawdb --lib pessimistic_` and `cargo test --locked -p hawdb --lib deadlock_`. |
| `//:hawdb_unit_analytics_core_general_tests` | Graph algorithms, projected artifacts, identity and generation behavior | Separate from isolated dense RSS ownership. |
| `//:hawdb_unit_analytics_rss_tests` | Dense persistent graph algorithms with bounded reader RSS and streamed adjacency | 128 nodes / 16,256 relationships. Isolated by #1030; history in [#1027](https://github.com/nowledge-co/hawdb/issues/1027). Uses the same repository test profile as every other root partition. |
| `//:hawdb_unit_recovery_tests`, `//:hawdb_unit_storage_crash_matrix_tests` | Transaction persistence, checkpoint/compaction and recovery boundary regressions | Dedicated crash-job evidence has additional identity and completeness checks. |
| `//:hawdb_unit_row_page_compaction_{materialized,out_of_core}_tests` | Physical row-page reclamation while pinned readers retain old generations; correct reopen/scrub in both residency modes | Full 16-table churn fixture and physical-byte assertions. Repository-wide checked test profile. Split by residency in #959; history in [#889](https://github.com/nowledge-co/hawdb/issues/889). |
| `//:hawdb_unit_row_page_compaction_admission_tests` | Budget refusal, retryability, failpoints and admission release around compaction | Ordinary compilation; separate from the two full convergence cases. |
| `//:hawdb_mvcc_joined_reads{,_minimal}_tests` | Joined-read dependencies, absent/filtered parents, off-page children, key bindings and sticky errors | Root presubmit; full and minimal feature contracts, each compiled once and executed as eight disjoint exact-case Bazel shards. The 16 cold/warm, point/page and parent/child-effect scenarios are individually named tests, distributed two per shard alongside seven other joined-read cases (23 cases total). Every fixture and assertion remains; each shard retains the 300-second deadline. Fixed scenario matrix rather than timed fuzz. The shard script rejects empty inventories, invalid coordinates, failed or zero-case selections; its regression checks cover both 8-case and uneven 23-case inventories. Issue #1158 tracks loaded-run timeout headroom. |
| `//:hawdb_mvcc_projection_publication{,_minimal}_tests` | Projection publication versus concurrent reads and transaction validation | Preserve publication barriers and complete observation evidence. |
| `//:hawdb_mvcc_staging_session{,_minimal}_tests` | Fixed-E graph/SQL mapping, complete-E phantom/delete/content guards, disjoint-write rejection, append/projection isolation, registered Consumer publication, cancellation and reopen | Root CI; one-item batches and hot/cold full-rebuild equality. Registered Consumer cases also run on native Windows. The competing-registration/candidate fixture uses one background slot and proves idle tokens retain no execution slots. The stationary Consumer family covers fresh publication admission, 260 resident refusals with unchanged allocation ledger/directories, native birth retirement, seal/intent FD pressure with injected pre-selection failure and seven publication/ACK/cache boundaries with live resolution and reopen. FD observers require `test-support`; other cases run in the default unit lane too. Final artifact construction is blocking. See [source-coherent catch-up](specs/SOURCE_COHERENT_SEARCH_CATCH_UP_SPEC.md) and [registered publication](REGISTERED_CONSUMER_CATCH_UP.md). |
| `//:hawdb_mvcc_full_source{,_minimal}_tests`, `//:hawdb_mvcc_baseline_comparison{,_minimal}_tests` | Exhaustive pinned source enumeration, duplicate IDs, full set/content comparison and full-rebuild reference | Root CI; small deterministic hot/cold fixtures. Complete-E verification adds a full-source pass and conservatively rejects every post-E write. |
| `//:hawdb_branch_project_open{,_minimal}_tests` and root branch lifecycle units | Existing-project open, recovery, leases and resource ownership | Includes V2 project selectors, exactly one retained metadata directory handle, strict foreground saturation/export retry, pending-child internal recovery completion at four descriptors, and cold branch publication and branch switching while strict foreground capacity is occupied. Verify durable identity, catalog results, reservation retirement, source isolation and reopen. Native FD scaling runs in an isolated subprocess and verifies return to baseline after closure. Persistent fixture cost is distinct from randomized exploration. |
| `//:hawdb_execution_request_contract_tests`, process-memory/search-consumer contracts | Request budgets, refusal behavior, projection generation publication and host resource policy | Preserve exact error and readiness assertions. |
| Registered stationary Consumer unit family (`cargo test -p hawdb --all-features --lib api::search_projection_consumer::`) and `//:hawdb_search_projection_consumer{,_minimal}_tests` | Native creation/catch-up/reopen, immutable old pins, completed receipts, revocation, disposable-cache recovery, transient IO retry and allocation capacity | Includes 260 actual catch-ups with an old pin, 256 unresolved rows with typed exhaustion, and transient retry through both direct open and pending-publication `open_or_create` startup recovery without reinitialization. The long capacity fixture suspends automatic checkpoint publication; checkpoint concurrency remains covered separately. Root checked-profile lane plus Unix native binding targets; positive `registered_stationary` and `_1043_` selections execute in the Windows native contract lane. [Protocol, limits and remaining gates](CONSUMER_STATIONARY_PUBLICATION.md). |
| `api::search_projection_consumer::tests::recovery::registered_stationary_*` | Same-handle preparation/lost-response retry, interrupted initializer, late generation/resident refusal, exact reservation reuse and cleanup custody | Registered in the root unit family and selected by the existing Windows `registered_stationary` lane. The FD/rename-observer cases require `--features test-support` (included by Windows `--all-features`); the remaining cases also run in the default root lane. Post-intent descriptor pressure must not cause target refusal; injected pre-selection failures reuse the same operation without rerunning the initializer; unknown cleanup evidence must survive. Rejected selectors and pre-selection source advancement settle creation, release capacity and private controls, and require explicit fresh-target rebuild. Lost metadata replies refresh expired/changed lease policies. Recovery capacity applies even to a registry that needs reinitialization. Terminal cleanup retries preserve foreign evidence and succeed after another operation compacts the completed row. Ref [#1106](https://github.com/nowledge-co/hawdb/issues/1106). |
| Native configured directory components (`native_directory_component`) | Captured-parent native name open/exclusive birth, exact space/Unicode/native-unit spelling, nofollow/reparse and collision refusal, original capture evidence, descriptor/model pre-effect refusal and retained Created after a real post-mkdir observer error | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner and explicit Windows storage failure-boundary selection. Tests reuse the real namespace backend and recorder; lost-response fault synchronously poisons only a local test observer after actual mkdir. Names retain Linux raw-byte/Windows UTF16-unit budgets. Apple raw input fits PATH_MAX scratch while native create/lookup decide eligibility; real long Unicode and canonical-equivalent lookup use held-file identity to retain one original recorder inode/path. Exact model keys need one FD; alias probe admission is lazy, outside the engine lock, and refunded on refusal. A real governor-backed caller wrapper funds the native-specific conservative trace bound until directory Drop, including longer registered spellings and pre-IO budget refusal. Artifact names stay ASCII-only. Before/after crash images and the actual parent barrier remain separate from process recovery. Replay `cargo test --locked -p hawdb-storage --lib native_directory_component`. Cross-compilation does not qualify Windows native name/reparse/rename behavior. This primitive grants no captured-open, SDK custody or activation permission. |
| Storage `consumer_allocation::bootstrap_tests`, `file_io::source_directory::control_candidate::tests`, `held_wal_rollback` | Torn bootstrap candidate isolation, lost ledger reply, pre-effect retry versus uncertain rename, foreign-name cleanup refusal and original-WAL header preservation | Existing storage unit owner plus explicit native Windows failure-boundary selection. Torn/reordered images use the finite power-loss model, separately from process reopen and native execution. |
| Storage WAL admission (`store::durable::wal::tests`) | Healthy mutation admission avoids generation-directory inventory; refusal retains exact pressure measurements and reasons | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner. Two pure cases cover lazy observation and 1,536 pressure combinations, including integrity, WAL/delta, free-space, cache and retained-reader states. Public pressure reports and durability/namespace checks retain their existing owners. |
| Storage source-sync and captured-WAL units | Non-strict source-barrier progress versus uncertain durability failure | Source barriers complete under strict foreground descriptor pressure without losing the live handle; captured WAL append prepays native validation handles before writing even when a competing owner fills remaining capacity. The latter also executes explicitly in the native Windows contract lane. |
| Storage WAL identity-cache and source-directory namespace units | Reuse pinned descriptor identity while preserving fresh namespace lookup, rotation, symlink retargeting and strict admission | Existing storage unit owner. Unix-only cache coverage complements captured-WAL replacement, uncertain append, rollback and power-loss tests; Windows keeps the existing native identity path. |
| Private checkpoint WAL resume and verbatim suffix fixtures | Partial append rollback through the original native object, completed-prefix retry, exact payload preservation and independent reopen | Existing storage unit and weekly Windows/macOS storage lanes. Windows rollback verifies that recovery handles can exceed the foreground FD ceiling while remaining accounted. Independent test copies exclude the source's active ownership-lock sidecar. |
| Search generation cleanup captured-source units | Relative enumeration/unlink preserve foreign replacement files; both sweeps reject foreign retention without losing retries | Existing search unit and vector platform feature lanes. Windows replaces the path after native directory capture and before opening child locks; Unix also replaces it after publication/source binding. Both require an actual replacement and preserve all deletion/retention assertions; native ancestor-rename refusal is separately covered by storage source-directory units. |
| Storage `file_descriptors::tests::capacity_policy` and root `api::tests::descriptor_capacity` | Soft capacity with pinned readers, strict foreground refusal, non-strict internal progress, joined-worker policy handoff and accounting retirement | Existing storage/root checked-profile owners; deterministic small fixtures. Refusal cases explicitly select strict mode. Both project constructors and the database facade preserve typed strict/soft configuration conflicts, the active policy and counts; a policy can change after all previous owners close. The existing compat protocol-server unit lane round-trips both policy-conflict directions and rejects non-boolean fields. Unix subprocesses lower only their own soft limit to verify the shared process OS allowance across three project domains, namespace/standalone accounting, active/retired prepaid-slot ownership, atomic final-slot admission by competing projects, failed-open retirement, typed pre-effect refusal, unchanged WAL bytes, same-handle retry and reopen; native exhaustion remains a separate Linux test. The default full-budget reservation test also uses an isolated Unix process so it cannot consume another test's process allowance. Namespace-probe fixtures verify independent-project and alias isolation, initializing standard-library OS error formatting before measuring exact native handle retirement on every iteration. Native Bindings `windows-native-contracts` runs the complete descriptor family and facade capacity tests, records source/toolchain and retains output; native Windows execution remains distinct from cross-compilation. Metadata/bootstrap, ownership-lock, sidecar publication, source synchronization, change capture and search/initializer cleanup regressions cover internal progress or native failure retirement at occupied strict capacity. Change capture retains its preimage byte limits and complete replay assertions. See [descriptor capacity](FILE_DESCRIPTOR_CAPACITY.md). |
| Default/minimal/graph-text consumer, host allocator and portable tests | Public feature boundaries, allocator ownership, in-memory portability and excluded native dependencies | Do not replace exact minimal-feature compilation with a default-feature run. |
| Scoped Consumer preparation (`cargo test --locked -p hawdb --all-features --test mvcc_staging_session registered_consumer_`) | Real writer commits between capture, reservation, construction and selection; repeated durable watermarks; graph/SQL/schema/source fences; reserved/prepared drop recovery and cold reopen | Root integration lane owns the tests. The split API is described in [Consumer progress and retention](CONSUMER_PROGRESS_AND_RETENTION.md); manual `consumer_publication` timing also records commits during native construction. |
| Read-only conflict history (`cargo test --locked -p hawdb-storage --lib snapshot_retention_tests`) | Long readers release unrelated stamps; stale read-to-write upgrades fail closed; writable savepoints preserve delete/reinsert conflicts; snapshot allocation accounting | Storage unit lane owns the tests. Read data and dependency/write validation pins have separate lifetimes. |
| Cross-crate FD refusal and owned cleanup (`spill::tests`, relational `descriptor_exhaustion`, vector `artifact::descriptor_tests`, Search descriptor/root-registry/stage-cleanup families, storage control-candidate and root derived-repair units) | Strict foreground rejection versus internal progress, native cleanup failure, lock custody and exact retry | Existing crate/root checked-profile CI owners and the complete Search feature matrix. Foreground refusal fixtures explicitly select strict mode, and repair metadata/database owners share that policy. Pressure loops stop at the configured target rather than running to OS exhaustion. Abandoned projection writers, private stages, stationary producers and publication locks must progress at occupied strict capacity while preserving pinned readers and peer exclusion. Captured publication leases use bounded directory duplicates, preserve foreign lock-path evidence on native failure, release failed registration, and retry under strict pressure; background-only/vector-only matrices must not use native process exhaustion as the fixture. Directory blockers or injected native unlink failures retain cleanup debt, bytes, host charges and retry dispositions; cancellation/memory refusal and root-alias/foreign-root registry isolation remain covered. Native Windows selects storage control-candidate, stationary Consumer and captured-publication lease families explicitly; the lease cases reuse the existing all-feature Search test binary and retain their own evidence file. No feature matrix, fixture, assertion budget or timeout is reduced. |
| Resident search checkpoint atomicity (#1711) | `api::tests::power_loss::resident_search_checkpoint` (Unix `test-support` power-loss family) cuts the fourth full-residency `EmbeddedStore` checkpoint for the runtime `apply_search_projection_delta_and_checkpoint` path, `apply_initial_import_projection_delta_and_checkpoint` and the deferred `checkpoint_initial_import_projection`. The cuts are the first write (including torn writes), the generation snapshot and layout renames, before and after the selector, the snapshot and lexical aliases, and the first cleanup unlink. Every image reopens to the previous or the new checkpoint with identical documents, epochs and Text, Vector and Hybrid hit score bits. Pre-selector cuts must keep the previous checkpoint. `resident_checkpoint_atomicity_tests` in the existing `hawdb-search` unit owner abort at each step, including a crash on the Nth repeated delta, an old-layout upgrade, an old-layout mixed snapshot that still fails closed, damaged-generation refusal (both as the typed `HawDBError::SearchProjectionMismatch`, #1735) and a reopen sweep that removes only resident checkpoint scratch. `resident_generation_snapshot_reference_rejects_installed_same_length_changes` checks the pre-selector snapshot verification. `pre_1711_crash_state_is_a_typed_search_projection_mismatch` opens the pre-#1711 crash state through `EmbeddedStoreHandle`, asserts the typed refusal with unchanged text, an intact unpoisoned graph-only reopen and a rebuild into the refused directory. Replay with `cargo test --locked -p hawdb --features test-support --lib api::tests::power_loss::resident_search_checkpoint -- --test-threads=1` and `cargo test --locked -p hawdb-search --lib resident_checkpoint_atomicity`. | No new lane, profile or timeout. Recorder images do not qualify physical devices or Windows namespace semantics. Registered Consumer checkpoints are outside this family. See [resident checkpoint](SEARCH_SEGMENT_POWER_LOSS_QUALIFICATION.md#resident-checkpoint). |
| Superseded branch-private WAL reclamation ([#1727](https://github.com/nowledge-co/hawdb/issues/1727)) | Root unit owner `embedded_store::tests::wal_generation_reclamation_tests`: graph-only, resident and incremental search projection with `background_maintenance` off and on, eight write+`checkpoint()` rounds; at most two `wal.*.hawdb` per WAL directory, no growth across checkpoints, zero `generation_reclamation_pending_*` debt, and every written node reads back after reopen. `api::automatic_checkpoint::tests::branch_wal_reclamation` drives six unattended automatic publications with adoption and off-gate retirement (no manual checkpoint) to the same per-directory bound and zero debt, then reopens all rows. Storage unit owner `store::immutable_root::tests::branch_wal_reclamation`: only the head-selected branch WAL survives repeated parent checkpoints while a forked child (and its own checkpoint) keeps reading from sealed objects; an injected removal failure is retryable debt, an injected failure of the head re-read or of the pre-removal directory synchronization removes nothing and is retryable debt, and the next checkpoint clears both; an open reader without a known pin set keeps every data-root generation while the superseded branch WAL is still reclaimed; failures before head publication keep the selected WAL and failures after it leave a leftover the next checkpoint reclaims; names above the selected generation and reader-pinned generations are kept. The existing `checkpoint_candidate_branch_tail_survives_reopen_and_later_seal` keeps an uncertain-publication name intact through allocation and catch-up and now expects it reclaimed once the newer head is durable. `concurrent_transaction_publishes_graph_relational_and_append_writes_in_one_wal_epoch` reads its superseded transaction WAL through a hard link taken before the schema checkpoint. Power-loss family `api::tests::power_loss::branch_wal_reclamation` cuts before/after the superseded-WAL unlink under both durability policies; every lost/reordered/isolated image recovers the full epoch and a later checkpoint reclaims the leftover with no debt. `unsynchronized_head_rename_survives_restart_sweep_and_power_loss` restarts on the visible state of a process crash after the head rename but before its directory barrier (`PowerLossModel::resume_after_process_crash` keeps that rename pending), lets the restarted runtime sweep the WAL the adopted head superseded, and requires every subsequent power-loss image to keep the new head and all data. Replay with `cargo test --locked -p hawdb --lib wal_generation_reclamation_tests`, `cargo test --locked -p hawdb-storage --lib branch_wal_reclamation` and `cargo test --locked -p hawdb --features test-support --lib api::tests::power_loss::branch_wal_reclamation -- --test-threads=1`. | Existing owners and lanes; no new profile or timeout. Recorder images do not qualify physical devices or Windows namespace semantics. |
| Search generation cleanup above the verification ceiling ([#1755](https://github.com/nowledge-co/hawdb/issues/1755)) | Existing `hawdb-search` unit owner `generation_cleanup::ceiling_tests` lowers `max_verification_bytes` to 64 KiB with every generation above 256 KiB: six resident checkpoints (content and epoch-only) each report a verified, non-retrying `projection_cleanup` and leave at most the current and previous generations; after forgetting this process's verification records a small-ceiling pass deletes nothing (`captured_closure_unverified`), successive passes converge and reclaim, a closure file changed after verification is re-hashed and blocks deletion, and an explicit pass with `max_verification_bytes = u64::MAX` stream-verifies a resident snapshot above the fixed decode bound (test hook) without decoding it (`verification_decoded_snapshot_bytes == 0`) while still reclaiming. Root unit owner `embedded_store::tests::search_cleanup_ceiling_tests` covers resident `apply_search_projection_delta_and_checkpoint` / `checkpoint_initial_import_projection` and incremental out-of-core publications through `EmbeddedStoreHandle`, including the explicit `retry_search_projection_cleanup` refusal and reclaim after a simulated restart. Replay with `cargo test --locked -p hawdb-search --lib ceiling_tests` and `cargo test --locked -p hawdb --lib search_cleanup_ceiling`. | Existing owners and lanes; no new profile or timeout. Small fixtures exercise the over-ceiling path; they do not measure 128 MiB generations. |
| Superseded immutable-object reclamation ([#1736](https://github.com/nowledge-co/hawdb/issues/1736)) | Root unit owner `embedded_store::tests::object_reclamation_tests`: ten write+`EmbeddedStoreHandle::checkpoint()` rounds (relational table plus graph nodes) with `background_maintenance` off and on; after every checkpoint `checkpoint-artifact/` holds only objects of the selected head, the head it superseded and the catalog creation baseline; zero `object_reclamation_*` debt, `object_store_*` reported, the admitted `EmbeddedStoreHandle::reclaim_branch_storage` finds nothing pending, and all rows and nodes read back before and after reopen and a further checkpoint. `reader_pins_its_checkpoint_closure_until_released` keeps a held reader's closure across three checkpoints and reclaims it after release. `api::tests::object_reclamation` creates a child from an older parent root and holds a read transaction on it while the parent checkpoints five times; the old root, child head and parent head closures stay complete, the child then reads, writes and checkpoints (its own sweep), and both branches reopen with exact data. `api::automatic_checkpoint::tests::object_reclamation` drives eight unattended automatic publications with adoption and off-gate retirement; at most three sealed roots remain and debt is zero. Storage unit owner `store::immutable_root::tests::object_reclamation`: with both durability policies, a child created from an older root, its head, the main base root and a pinned reader survive parent sweeps while the namespace equals exactly the live closures; injected listing, pre-removal barrier, unlink and post-removal directory-sync faults keep the checkpoint successful, remove nothing before the barrier, record retryable debt and are cleared by the next checkpoint; a checkpoint with nothing to publish retries debt and an automatic pass never waits for a held catalog metadata lock; explicit reclamation above its limits removes one bounded batch per pass and drains the rest; a corrupt unreachable object is retained and named in the pressure snapshot while sweeps continue and explicit reclamation still refuses; another admitted owner's registered reader pin on its own superseded root survives foreign sweeps until released; two owners checkpointing concurrently (publications racing sweeps) never lose a live closure; a damaged object reachable from a retained root is reported as `object_reclamation_live_object_damaged` (nothing removed) until repaired; garbage this process published is removed without re-hashing under a 1-byte budget, while objects without a publication record drain one re-hashed batch per pass. `branch_abandonment::tests::automatic_evidence_verifies_each_archive_once_and_protects_unverifiable_sources` serves a verified archive from the per-process cache after its bytes change (explicit evidence refuses) and, over budget, protects the quarantined source instead of failing the sweep. Power-loss family `api::tests::power_loss::object_reclamation` (see [branch power-loss qualification](BRANCH_POWER_LOSS_QUALIFICATION.md)). Replay with `cargo test --locked -p hawdb --lib object_reclamation`, `cargo test --locked -p hawdb-storage --lib object_reclamation` and `cargo test --locked -p hawdb --features test-support --lib api::tests::power_loss::object_reclamation -- --test-threads=1`. | Existing owners and lanes; no new profile or timeout. Recorder images do not qualify physical devices or Windows namespace semantics. |
| Consumer physical reclamation (`cargo test --locked -p hawdb --all-features --lib consumer_reclamation`) | Old snapshot and initial-lease protection, durable branch and pending-operation roots, bounded retries, unknown-evidence retention, FD/memory release and ledger retirement before unlink | Seven lifecycle cases belong to the root unit lane, including cleanup and retry at occupied strict foreground FD capacity; two recorder cases belong to the power-loss family. The recorder fixture and database opens explicitly share the strict 32-descriptor configuration. The Windows native contract lane selects all nine through its existing family filter. Recorder cases explore lost/torn/reordered uncovered writes at the first retirement unlink and verify sealed-producer inode sharing before the target-directory barrier, durable linked data after scratch retirement, scratch-removal synchronization and Consumer reopen. Replay the latter pair with `cargo test --locked -p hawdb --features test-support --lib api::tests::power_loss::consumer_reclamation`. Close/reopen fixtures do not qualify the complete platform power-loss matrix. See [cleanup ownership](SEARCH_CLEANUP_OWNERSHIP.md). |
| Publication-owner admission (`api::publication::tests`) | A waiting admitted reader precedes later writers, an admitted writer precedes later readers, reader callbacks remain concurrent, and nonblocking writers honor the gate, retry after release and reject poisoned admission | Existing root unit lane, ordinary profile. Channel-controlled cases inspect only the private acquisition gate; they do not claim platform-wide FIFO fairness. Validation/publication exclusion and error/retry tests retain their existing ownership. No additional CI job. |
| Registered covered-token reclamation (`registered_consumer_reclamation_preserves_continuously_invalidated_covered_base`) and manual `consumer_publication` benchmark | Actual post-E writer commits while preparation is paused, typed refusal and retained base during GC; outer publication-owner latency and watermark progress under 1/4/8 writers | Regression: default/minimal `mvcc_staging_session` and native Windows registered selection. Benchmark: manual only, repository bench profile, disabled/legacy/covered/scoped-blocking/detached controls and raw p95/p99 samples. The opt-in `--sustained true` detached workload continues publication until writers finish or its explicit attempt ceiling, with checkpoints every 16,384 foreground writes and retained checkpoint samples. Its finite-progress qualification is separate from latency controls and the original strict writer gate. Manual `python3 scripts/measure-consumer-publication.test.py` checks executable/profile matching and actual feature provenance; no default CI registration. The separate opt-in `--with-readers` driver cohort adds one pinned canonical reader per writer and phase-boundary Linux process RSS/high-water/CPU-tick/I/O observations. It checks every document identity/content and retains per-request owner wait and preparation-overlap intervals. Missing resources, zero reads or drain-only reads cannot qualify; process high-water marks do not prove phase heap ownership. Reader measurement remains manual and does not change the completed writer-only cohorts. Whole-Database post-E dependencies retain conservative refusals; Domains-scoped finite progress is qualified separately. The helper records intake/mapping call counts with a `2 * feed.items + 1` host bound for its Done-only/no-delta mapper, without changing feed/mapping resources. A 60,000-update manual regression exercises complete coverage and native publication past the old 100,000-call bound; no new CI job. Quiescent drains are not sustained progress. [Workload and qualification boundaries](CONSUMER_PUBLICATION_QUALIFICATION.md). |
| `//:hawdb_system_sql_loom_tests` | Bounded modeled system-table publication interleavings | Model exploration; not a wall-clock fuzz campaign. |
| Package parity, query-limit inventory and root-partition script tests | Missing package ownership, unbudgeted query routes, duplicate or omitted test partition selection | Keep these structural checks when moving targets. |

The `_1043_` deleted-prefix fixture updates a surviving node in the same
prefix transaction so its one-operation budget still truncates before the
deletion after live-upsert filtering. Both residency modes retain prefix
refusal, unchanged durable checkpoint and complete-retry/rebuild parity.

Brace notation describes several concrete labels; expand it before invoking
Bazel. A family description does not imply every leaf belongs to every lane.

### Automatic-checkpoint frontend deadline controls

The required `//:hawdb_unit_checkpoint_headroom_tests` compatibility label
aggregates two explicit targets over the same checked root binary:
`//:hawdb_unit_checkpoint_headroom_growing_table_tests` owns the complete
growing-table steady-insert regression and
`//:hawdb_unit_checkpoint_headroom_controls_tests` owns every other discovered
control. Both retain exclusive scheduling, one libtest thread, the 900-second
deadline, complete fixtures/assertions/retry bounds and ordinary features. The
runner refuses unexpected Bazel sharding, empty selections and cross-target
substring overlap; `//:root_unit_partition_script_test` owns complete/disjoint
inventory and protocol controls. Source-bound 31-case timings, both earlier
Linux timeouts, progress diagnostics and local replay are in
[checkpoint qualification](CHECKPOINT_HEADROOM_QUALIFICATION.md). This separates
execution budgets and allows required CI to place them in different Pods; it
does not claim less total work or a measured remote speedup. Issue
[#1512](https://github.com/nowledge-co/hawdb/issues/1512) retains independent
progress/stability qualification.

For pull requests, `ci/skein-bazel-test-checkpoint-growing-table` owns the
growing-table target and `ci/skein-bazel-test-checkpoint-controls` owns the
remaining headroom target. The main root presubmit excludes both leaves; the
compatibility aggregate remains available for local and mainline replay.

`//:hawdb_unit_checkpoint_headroom_tests` owns
`api::automatic_checkpoint::tests::headroom` in the isolated default-feature root CI lane
(`background-maintenance` required). It retains the checked profile, 320-row
write/reopen fixtures and
target timeout ceilings. Functional idle/retiring/prepared-prefix tests freeze
only the private test clock for frontend capacity/backoff policy, so compiler
contention cannot consume a deliberately paused owner's production deadline.
Synthetic Preparing/Discarding/Releasing capacity and preparation-backoff cases
advance to one nanosecond before and exactly at the original deadlines, with
repeated notifications proving that wakes do not renew a deadline. Production
uses `Instant::now()` and keeps the 10 ms backoff and 5 s capacity policy.
Replay with `cargo test --locked -p hawdb --lib
api::automatic_checkpoint::tests::headroom`; also check
`--no-default-features --features background-maintenance`. The ordinary minimal
feature matrix remains unchanged.
This change does not establish a remote speedup or fix every #1116 stall;
joined-read sharding is owned by PR #1217.

### Relational live-overlay checkpoint pressure

`api::automatic_checkpoint::tests::relational_live_overlay` joins the sustained
WAL/delta pressure cases in the isolated `//:hawdb_unit_checkpoint_headroom_tests`
lane (`background-maintenance` required) and uses default storage limits and a one-hour checkpoint age, so only live-overlay pressure can
schedule work ([#1438](https://github.com/nowledge-co/hawdb/issues/1438)).
Four hundred whole-table update rounds on one handle (512 rows of 1 KiB) exceed
the 64 MiB live envelope several times; base `bb30c40` rejects round 97 before
WAL append. Cases assert every commit, peak occupancy within the envelope, at
least two pressure-triggered generations, values before and after reopen, exact
debt/residency accounting folded by checkpoint, the shared 70%/80% boundaries
for all four row/index pairs, a reader pinned across two drains, and a
subprocess stop at `during_checkpoint_publication` and
`after_manifest_publication` of the pressure-triggered drain that recovers every
acknowledged round whole before continuing. An insert-only case grows one
TPC-C ORDER_LINE-shaped table (composite primary key plus a secondary index,
2,500 rows per commit) through five pressure-triggered drains on one handle and
asserts every commit, no failed checkpoint attempt (each drain's capture of the
recovery delta plus the live overlay is admitted), live row/index occupancy
below its envelope after each commit, the delta charged as debt, point and
secondary-index reads before and after reopen, a further commit on the reopened
handle and exact totals once folded (writers retry the bounded-wait
`BranchBusy`, which applies nothing, when an unoptimized drain is slow) ([#1464](https://github.com/nowledge-co/hawdb/issues/1464));
`c8d767cd` refuses the second drain's capture three times and rejects commit 48.
`relational::row_page::live` pins the live-overlay and checkpoint-capture
charge of that row shape. The `large_commit` cases fill the overlay to about
67% with small ORDER_LINE commits, below the trigger, then commit whole
districts ([#1479](https://github.com/nowledge-co/hawdb/issues/1479)): two
consecutive 40,000-line commits (about 59% of the envelope each) each drain first and
succeed, with no failed attempt, exact totals and reads after reopen (base
`bd26dc4d` rejects the first, commit 22, before WAL append). A 30,000-line
autocommit statement drains first too. A commit larger than the whole envelope
is rejected, in a transaction and in autocommit, with the limits and its size
named, no drain, an unchanged epoch and nothing written. When the owner can
drain but its attempt is deferred (a one-byte governor denies background
admission, as under memory pressure), a district that needs the drain gets the
retryable `BranchBusy` with an unchanged epoch, no completed checkpoint and
nothing written, then commits once the governor admits the drain
([#1637](https://github.com/nowledge-co/hawdb/issues/1637),
[#1618](https://github.com/nowledge-co/hawdb/issues/1618); main returns the
non-retryable live-view admission error). An owner made unable to drain while
it is held before the drain a waiting district requested (suspended or
stopped) keeps that live-view rejection, with nothing written, and the district
commits after a manual checkpoint. A failed owner (integrity failure) fails the
district closed with the integrity error, never with `BranchBusy`, re-arms
nothing and applies nothing; reopening recovers exactly the committed rows at
the same epoch. An owner whose operation retries are
exhausted while held is re-armed by the waiting district after the 1 s backoff
([#1618](https://github.com/nowledge-co/hawdb/issues/1618)): the district gets
at most `BranchBusy` with nothing applied, then commits without a manual
checkpoint (one new epoch, `operation_retry_rearms >= 1`). The
concurrent-handle and task cases hold the idle owner just before it starts the drain that a
waiting district requested (no sleeps or free-running threads): on a concurrent
handle a reader keeps its published view during the wait, and another writer
queues behind the district and commits after it. A district whose task is
cancelled (concurrent handle, admitted permit) or whose deadline passes (single
handle) during that wait fails once the drain is adopted, with no new epoch or
rows; the permit, the sequencer and the barrier are released and the same keys
commit afterwards (`6f7c2e1c` commits both after the wait). A subprocess stop at
`during_checkpoint_publication` and `after_manifest_publication` of the drain
that a district requests recovers exactly the acknowledged commits, and the
recovered handle admits the district. The `large_commit` cases run in their own
`//:hawdb_unit_checkpoint_large_commit_tests` partition, and the interrupted
ones (`large_commit::interrupted`: cancellation, deadline, process stop) in
`//:hawdb_unit_checkpoint_large_commit_interrupted_tests` (each one libtest
thread, Bazel `exclusive`, the same 900-second deadline). Each case drives full
checkpoints of a table near the default envelope; inside
`//:hawdb_unit_checkpoint_headroom_tests` they pushed that partition past its
deadline in CI, and one shared lane later did too, mostly from a free-running
reader and writer competing with the drain. On an Apple M4 in the Cargo test
profile the two lanes take about 98 s and 92 s (about 365 s together on
efficiency cores only). Concurrent cases stage a district as 5,000-line
statements: one 30,000-line statement in a concurrent transaction stages about
four times slower than six smaller ones. Replay:
`bazel test //:hawdb_unit_checkpoint_large_commit_tests
//:hawdb_unit_checkpoint_large_commit_interrupted_tests`.
For pull requests, `ci/skein-bazel-test-checkpoint-large-commit` owns the first
target and `ci/skein-bazel-test-checkpoint-controls` owns the interrupted target
alongside checkpoint lifecycle and the remaining headroom controls. The main
root presubmit excludes these explicit leaves.
Replay with `cargo test --locked
-p hawdb --lib api::automatic_checkpoint::tests::relational_live_overlay`.

Transient checkpoint faults on a long-lived handle
([#1618](https://github.com/nowledge-co/hawdb/issues/1618)) inject the fault
deterministically: a regular file at the next generation's private staging path
makes every automatic preparation fail with an operation error before any
selector or WAL mutation, and removing it clears the fault (no timing or disk
exhaustion is needed). `steady_inserts_outlast_a_transient_checkpoint_fault_and_keep_draining`
(in `//:hawdb_unit_checkpoint_headroom_controls_tests`) runs the growing-table
2,500-line commits with the fault in place until the stopped circuit has been
re-armed and failed again, clears it, and continues through two drains. Every
commit succeeds or gets the retryable `BranchBusy` (at most 200 retries), live
and, until the first drain, base-relative row debt stay below the envelope,
failures stay within three plus one per re-arm, and reads and totals hold after
reopen; base `1141d6a7` rejects commit 26 with exactly the #1618 live-view
admission error (67,500 entries/67,299,228 bytes). In
`//:hawdb_unit_checkpoint_large_commit_tests`,
`a_district_commit_retries_while_its_drain_fails_and_succeeds_once_the_fault_clears`
fills to about 67% and retries one 40,000-line district: each rejection applies
nothing (unchanged epoch), and once the fault clears the district drains first and
commits; without the re-arm, the retry after the third failure returns the
non-retryable live-view rejection.
On an Apple M4 (Cargo test profile, loaded host) each takes about 52 s, most of
it the bounded 1 s/2 s/4 s re-arm backoff. `api::automatic_checkpoint::tests::operation_retry`
(`//:hawdb_unit_checkpoint_lifecycle_tests`) pins the circuit itself without a
database: the recorded cause, one attempt per re-arm and the doubling backoff up
to its 30 s cap; its existing cases still prove that an idle handle never
retries a stopped circuit and that a manual checkpoint re-arms it. Replay with
`cargo test --locked -p hawdb --lib transient_checkpoint_fault`,
`cargo test --locked -p hawdb --lib drain_fails_and_succeeds` and
`cargo test --locked -p hawdb --lib operation_retry`. No lane, profile,
feature matrix or deadline changes.

## Component regression owners

The manual `concurrent_writers` benchmark accepts
`--pessimistic-statements 1|8|32` for repeated updates of each writer's disjoint
node under pessimistic transactions. It keeps the 1/4/8-writer matrix, checks
every final row and durable reopen, and uses wave barriers to prevent unrelated
epoch drift from requiring retries. Record staging and commit latency separately;
barrier overlap is not evidence of parallel engine execution. This optional
workload does not change default or CI benchmark selection.

| Component / `:presubmit_tests` family | Problems covered |
| --- | --- |
| Manual work-admission benchmark | `HAWDB_EXECUTOR_BENCH_MODE=work-admission bazel run //:hawdb_bench_executor_vectorization` compares complete one-hop property hydration at degrees 32/1,024 and an 8,192-row predicate scan, with a deadline and no work cap. Three warmups, 11 raw samples of eight iterations, checksum/row-count and final-zero ledger assertions. Explicit opt-in only; default `full` and CI coverage are unchanged. Compare identical harness/profile/source identities; local timings are not remote CI speedups. |
| Core, expression, SQL/SQL-syntax, Cypher and DDL | Typed values, expression semantics, parsing/binding and schema constraints. |
| Ordinary relational projection admission | Existing `//crates/relational:hawdb_relational_tests` / Prow crates owner. Borrowed previews admit repeated projection values, locator sort keys and DISTINCT ordering keys before copying. Ordinary, ordered, borrowed and FROM-less paths retain their existing result contracts. A test-only, thread-local System allocation probe compares cumulative large-payload allocation traffic with the same-source narrow control; it does not measure peak RSS. Guards cover canonical memory and persisted row snapshots, expression aliases, refusal/refund/retry, empty and LIMIT 0 input, duplicate/NULL values and exact result-payload boundaries. Replay: `cargo test --locked -p hawdb-relational --all-features --lib projection_admission`. No feature, deadline, profile or lane change. See [projection admission](RELATIONAL_PROJECTION_ADMISSION.md). |
| Cypher temporal types | Core `temporal::` tests cover ISO parsing/rendering, constructors, components, arithmetic, `duration.between`, truncation, comparable order versus `Value` order/hash and the shared binary payload (golden bytes, corruption). Storage tests pin WAL fields 11–15, canonical tags 11–15 and text `D/T/L/Z/P` encodings with golden bytes, unknown-tag/field fail-closed decoding and background-writer parity; executor spill tags 10–14 round-trip. Parser (`tests::temporal`) and planner (`logical::temporal_tests`) cover syntax, depth limits, constant folding to `PropertyCompare` and the pinned statement clock. Root `api::tests::temporal` covers the embedded API: constructors, components, CREATE/SET/MERGE, WAL replay, checkpoint reopen in materialized and out-of-core modes, range-index seeks, CASE-based ORDER BY keys containing temporal component arithmetic with changing parameters and warm cache hits, and the LDBC IC3/IC4, IC7 and IC10 shapes. FFI, Python and Go binding tests cover tagged JSON. Existing CI lanes apply. Replay: `cargo test --locked -p hawdb-core temporal`, `-p hawdb-storage temporal`, `-p hawdb --lib temporal`. See [contract](CYPHER_TEMPORAL.md). |
| Kùzu `string(x)` cast ([#1682](https://github.com/nowledge-co/hawdb/issues/1682)) | Mem thread version stamps compare Kùzu `string(x)` text across migration, so the cast must reproduce Kùzu 0.11.3 bytes, not `toString`. Core `conversion_function::tests::kuzu_string_matches_kuzu_0_11_text` is a differential table of values measured with Kùzu 0.11.3 (timestamps with trimmed microseconds and zones dropped to the UTC instant, BC/five-digit years, `%f` floats including `inf`/`nan`/`-0`, `True`/`False`, lists/structs with empty NULL members, every interval part and sign, UUIDs, blob escapes, float vectors); `kuzu_string_alias_null_and_unsupported_types` covers the `to_string` alias, NULL in/NULL out, unchanged `toString` and the local-time/decimal errors. The text is measured by an exact non-allocating length pass and charged before it is rendered: core `charged_conversion_is_refused_before_rendering` (refusal before rendering, exact charges, exact lengths for every table case) and `unaccounted_conversion_refuses_oversized_text` (64 MiB cap for plan-time parameter binding and batch mutation rows); executor `expression::value::borrowed::conversion_tests` refuses `string()` of 1,000 `1e308` floats, a 4,096-dimension vector and nested integer lists through a 64 KiB query-memory account with the account's own error kind and unchanged available bytes, charges successful results at least their length and releases them, and bounds the account-less path. Root `api::tests::kuzu_string_cast` runs the issue table through Cypher and the two halves of the Mem stamp `COALESCE(string(t.updated_at), '') + '|' + COALESCE(string(t.message_count), '')` (joined on the host) over whole-second, sub-second, zoned, NULL and missing `updated_at` (also after SET and WAL replay), and pins that epoch-nanosecond `timestamp(...)` integers render as integers; `thread_version_stamp_expression_matches_kuzu` runs the full expression and is ignored until string `+` concatenation is supported (separate issue). Existing CI lanes apply. Replay: `cargo test --locked -p hawdb-core --lib kuzu_string` and `cargo test --locked -p hawdb --lib kuzu_string`. See [conversion functions](CYPHER_LOAD_CSV.md#kùzu-stringx). |
| Cypher JSON scalar selection ([#1605](https://github.com/nowledge-co/hawdb/issues/1605)) | `//:in_memory_portable` owns strict `json_value` selection and its explicit `NULL ON NONSCALAR` array/object-to-NULL mode. Literal and parameter paths cover projection, predicates, `CASE`, `COALESCE`, `WITH` and bounded aggregates; malformed/non-string/oversized documents, integer overflow, query-memory refusal and cancellation remain errors. The same target is the minimal browser WASM contract. Replay: `cargo test --locked -p hawdb --test in_memory_portable null_on_nonscalar` and the strict-Clippy command in `docs/WASM.md`. See [contract](CYPHER_JSON_VALUE.md). |
| Selected-record hydration ([#1611](https://github.com/nowledge-co/hawdb/issues/1611)) | `//crates/search:hawdb_search_tests` (`out_of_core::hydration::tests::document_line_summary_matches_the_decoded_document`) and every existing hydration owner, whose debug builds assert each selected record's summarized bytes equal its decoded `search_document_bytes`. Document hydration decodes only the selected records of a segment; the summary of the others gives their id and exact byte count. |
| Configured default DiskANN navigation beam ([#1614](https://github.com/nowledge-co/hawdb/issues/1614)) | `//crates/search:hawdb_search_tests` (`out_of_core::tests::vector_segments::configured_default_navigation_beam_applies_unless_a_query_sets_one`). `SearchOutOfCoreConfig::default_navigation_beam` (default 1024, unchanged behavior) applies to queries without a per-query beam exactly as an explicit beam would, and a per-query beam overrides it. |
| Candidate filters decode only their fields ([#1613](https://github.com/nowledge-co/hawdb/issues/1613)) | `//crates/search:hawdb_search_tests` (`document_decoding_tests::metadata_key_decode_keeps_only_the_requested_keys`) and every existing filtered-search owner, which now builds candidate sets through the key-restricted decode. Building a metadata-filtered candidate set decodes only the predicates' metadata keys, and segment pruning summarizes only those fields. |
| Metadata value postings ([#1613](https://github.com/nowledge-co/hawdb/issues/1613)) | `//crates/search:hawdb_search_tests` (`out_of_core::postings::tests`). Layout V4 metadata artifacts hold per-segment value postings under one field-semantics registry: ordinary normalized scalars, canonical `kind` aliases and defaulted `space_id`; multi-valued and unsupported predicates remain residual. `postings_select_the_scanned_candidates_and_hits` and `query_admitted_postings_select_the_scanned_candidates` compare complete postings and postings-plus-residual prefilters with the forced metadata scan on the non-query and admitted query paths, across generation-writer and resident-checkpoint projections and a distinct-value cap that excludes a field. They cover candidate IDs and vector ordinals, hybrid hits, Eq/In/AND filters with mixed case and whitespace, missing/empty/default `space_id`, `kind` aliases, documents without embeddings, absent fields, and residual shapes (`labels`, `metadata.*`, NotIn, range, presence, unsatisfiable). `postingless_v2_layouts_search_through_the_metadata_scan` builds a real V2 projection without postings, compares it with V4, and checks that V2 with postings, unknown formats and misplaced postings ranges are refused. `payload_validation_refuses_inconsistent_postings` also verifies that legacy `HDPOST01` postings still serve ordinary fields but make V4 semantic fields fall back. `corrupt_postings_ranges_fail_closed` flips a postings byte. `postings_reads_fail_closed_under_a_tight_query_budget` sweeps query budgets, which either refuse with a budget error or return identical candidates with a refunded ledger. `candidate_evidence::boundaries::report_json_preserves_complete_field_set_and_provenance` pins all four postings counters in both candidate and readiness JSON. Encoder admission regressions use an 8 KiB account to verify byte-identical postings for ordinary scalars and `space_id` with large ASCII/Unicode whitespace padding, empty scalars and Unicode lowercase expansion, while unknown `kind` values retain exact spelling and raw-byte admission; both successful encoding and budget refusal refund the ledger. The remaining cases cover fields left unindexed by the distinct-value or 256-byte value cap, payload size limits, reader-budget fallback, structural corruption and overwritten/deleted/newer-layer documents. Test segments hold two documents, so the cap is set per thread in tests (production: 4,096). |
| Ordinary squared-L2 projection scoring | Existing Search vector feature-matrix owners run `vector_metric_tests::ordinary_squared_l2`, `vector_metric_tests::out_of_core_squared_l2` and `vector_metric_tests::squared_l2_refuses`: nonunit norms, 1536-dimensional 0.80 boundaries, zero vectors, zero-score neighbor order, resident exact/default, checkpoint/reopen and retained generations, streamed exact/DiskANN, document/scores-only output, inherited incremental policy, cancellation/memory refusal, incompatible metadata and conflicting-option checkpoint refusal before snapshot replacement. Root general unit owners run `embedded_vector_seed_preserves_ordinary_squared_l2_score` for resident/out-of-core embedded Cypher output. Existing profiles, feature selections, timeouts and CI lanes remain unchanged. Replay: `bazel test //crates/search:hawdb_search_tests --test_arg=squared_l2` and `bazel test //:hawdb_unit_general_tests --test_arg=embedded_vector_seed_preserves_ordinary_squared_l2_score`. See [scoring contract](SEARCH_VECTOR_SCORING.md); ref [#1541](https://github.com/nowledge-co/hawdb/issues/1541). |
| Shared SQL/Cypher vectors | Core/SQL targets cover dimensions, encoding and exact distance; storage covers value/schema codecs, fingerprints and cooperative cancellation; root `hawdb_unit_general_tests` covers exact filtered ranking versus candidate post-filtering, traversal, rollback and WAL/checkpoint/read-only reopen. Executor allocation tests and native FFI/Python/Go targets cover payload accounting and ownership. Shiro reports retain vector dimensions and coordinate bits. Existing CI lanes apply. Focused replay: `--test_arg=shared_vector --test_arg=vector_schema --test_arg=vector_encoding --test_arg=vector_distance`; include `checkpoint_units` for storage cancellation and `owned_record_bounds` for the allocation oracle. See [contract](specs/VECTOR_COLUMN_SCHEMA_SPEC.md). |
| SQL TIMESTAMP / TIMESTAMPTZ ([#1453](https://github.com/nowledge-co/hawdb/issues/1453)) | Core `sql_timestamp::` tests cover PostgreSQL/ISO parsing, rejected offsets and malformed text, half-up sub-microsecond input, `TIMESTAMP(p)` rounding away from 2000-01-01 (including pre-1970 values), chrono range edges and overflow, ISO text output and TEXT casts. SQL-syntax and SQL tests cover `WITH/WITHOUT TIME ZONE` in DDL, typed literals and casts. Storage tests pin row-page, relational WAL/checkpoint codec and schema precision encodings, out-of-range payloads as corruption, ordered-key order across value kinds and primary-key range successors at the range edges. Relational `pgq_create_tests` keep timestamp graph properties, timestamp literals and time-zone casts failing closed, and `query::projection_memory::tests` charges `CAST(text AS TEXT)` its operand payload under a tight batch budget. Root `api::tests::sql_timestamp` covers WAL replay, checkpoint reopen and checkpoint-plus-tail reopen in Materialized and OutOfCore modes with demand-paged indexes, AutoRepairTornTail truncation inside the last transaction, primary-key/point/secondary-index range seeks (checked through EXPLAIN), ORDER BY with NULLS LAST, GROUP BY, DISTINCT, MIN/MAX, joins, casts, LocalDateTime/DateTime/text parameters, `information_schema.columns`, rejected literals and mixed TIMESTAMP/TIMESTAMPTZ comparisons, and strict append tables (assignment conversion, unrounded partition lookups). Python `tests/test_temporal.py` binds naive and aware `datetime` parameters (including non-UTC zones stored as their UTC instant) through `execute_sql` with tagged-dict results, comparisons and an index seek; `tests/test_dbapi.py` checks that cursors return naive/aware-UTC `datetime` values matching `DATETIME` and keep unrepresentable values tagged. FFI `temporal_tests` round-trips SQL timestamps through `hawdb_query_sql`. Go and MoonBit add no timestamp-specific tests (their tagged temporal encodings are unchanged). Existing lanes, profiles and timeouts apply. Replay: `cargo test --locked -p hawdb-core --lib sql_timestamp`, `-p hawdb-storage --lib timestamp`, `-p hawdb-relational --lib timestamp`, `-p hawdb --lib sql_timestamp`. See [the SQL timestamp contract](specs/POSTGRES_SQL_PGQ_SPEC.md#sql-timestamps). |
| Schema-bound vector indexes | Core tests cover resolved descriptors and canonical metadata; Search tests cover binding persistence, backend identity and typed budget diagnostics. Root tests exercise complete SQL/node source capture, empty/NULL inputs, composite keys, source/schema read-domain fencing, full rebuild after updates/deletes/NULL transitions, retained old domains and reopen, signed distances, cancellation and admission retry. Existing Search feature-matrix and root general CI lanes own these cases. Focused replay: `--test_arg=vector_index`; include `--test_arg=projection_fallback_requires_a_typed_resource_error` and `--test_arg=search_projection_consumer::tests` when changing shared Consumer publication. Eligible-domain cases cover SQL/Cypher parity after an ineligible nearest prefix, typed read-domain identity, source-work refusal even with no surviving candidate, bounded canonical fallback, duplicate paths and upstream LIMIT, ordered range/LIMIT with rebound parameters, and shared-ledger eligible/output memory handoff. The existing ACL capability runner also selects the traversal visibility test, which exercises exact and filtered-ANN modes and refuses a missing test name; its fixture covers hidden intermediate nodes. Storage adjacency tests independently meter rejected edges before emission. Core scoped-work tests cover shared clones, parent limits and cancellation. Storage projection admission borrows work ownership separately from payload admission and preserves typed exhaustion without allocator probes. Core guards distinguish unbounded work no-ops from explicit cancellation checkpoints. Executor point-read guards use a shared ledger sized for exactly one payload plus fixed callback overhead, covering resident and canonical nodes/relationships, repeated reads, missing/label-rejected reads and complete refund. Storage point guards meter missing/tombstoned/rejected reads and preserve sticky work refusal; existing adjacency guards cover rejected edges. Language cases cover missing/duplicate/unknown parser options, rejected SQL shapes and Cypher YIELD bindings, SQL/Cypher foreign-governor refusal, SQL single/early/final callback cancellation, consumer-error precedence and fresh-task read-domain reuse, typed parameters, signed distances, SQL composite-key hydration, post-window filtering, Cypher canonical traversal, and exact canonical identities across old/current/reopened generations. A smaller-than-input ANN window asserts actual rerank counts. Filtered-ANN guards assert zero collection scoring, eligibility before candidate generation, smaller-than-domain bounded reranking, validated empty eligibility, same-scope native mapping work refusal/retry and preserved default post-filtering. Native mapping units reject missing, duplicate-visible and NULL-ordinal mappings, exercise typed memory refusal at initial reserve and per-key growth, retry after pressure is released and refund their admitted memory on every exit. Root empty-index mapping tests preserve errors and same-permit retry; captured candidate-spill and Stage cleanup guards independently cover native resource owners; empty-domain queries still enforce binding/mode/candidate caps and perform no spill or vector scan. Rebuild/update/delete/NULL and old-pin cases exercise all three modes. Allocation-owner tests cover canonical SQL point reads, wide-row/repeated-projection/empty-literal refusal before copying, LIKE/ILIKE layout admission with Unicode/escape controls, covered-catch-up refusal before mapping/allocation, foreign-governor refusal before skipped execution, retry, producer workspace refusal and cancellable I/O-wave admission, same-database branch mismatch and uncertain selection recovery with old pins. Minimal consumer and portable facade cases refuse schema-bound index construction before filesystem work when the native backend is absent. See [contract](specs/VECTOR_INDEX_BINDING_SPEC.md). |
| SQL result column order | `//crates/executor:hawdb_executor_unit_tests` covers named rows ordered by a declared schema, empty results and rejected column drift. Root `//:hawdb_unit_general_tests` (`relational_sql::tests`) pins each shape's EXPLAIN plan and runs it with and without matching rows: borrowed scans, key and index lookups, inner and LEFT joins, scalar functions, index-ordered, top-N (on projected and non-projected columns, also over inner and LEFT joins), DISTINCT and alias-ordered projection, wildcards over top-N, DISTINCT and LEFT joins, LIMIT 0, and columnar, row-at-a-time and grouped aggregates, including HAVING and joins. A cost-reordered wildcard join and strict-append SELECT also run with and without rows; FROM-less SELECT and COUNT DISTINCT always return one row. Repeated output names and the reserved `__hawdb_relational_sort_` prefix fail during preparation, including EXPLAIN. `crates/qualification` `evidence_digest::tests` keeps result digests independent of column order, matching name-keyed reference rows. The Python binding reports these schemas unchanged as `QueryResult.columns`; `tests/pg-regress` sorts column names and does not cover order. Existing CI lanes apply. Replay: `cargo test --locked -p hawdb-executor --lib named_rows_follow_the_declared_schema` and `cargo test --locked -p hawdb --lib -- relational_sql::tests::database_sql_ relational_sql::tests::unordered_and_wildcard_joins`. Catalog and SQL EXPLAIN coverage is listed below. Cypher RETURN and the vector-index SQL streaming callback, which receives name-keyed maps, remain outside this family. |
| Catalog and SQL EXPLAIN column order | Root `api::tests::result_column_order` covers explicit and aliased catalog projections across Database, snapshot and transaction reads, empty results, declared wildcard order, mixed wildcard/explicit projections, duplicate output rejection and unchanged row/payload refusal. `api::tests::relational_explain_output` pins relational SQL EXPLAIN/ANALYZE column order; its `strict_append` module covers all three entrypoints. Owned by existing `//:hawdb_unit_general_tests` CI; focused replay: `cargo test --locked -p hawdb --lib -- result_column_order relational_explain_output`. Python DB-API regression coverage runs in its existing CI family. Cypher coverage is listed below. |
| SQL CURRENT_TIMESTAMP, now() and LOCALTIMESTAMP ([#1453](https://github.com/nowledge-co/hawdb/issues/1453)) | SQL tests (`parser::timestamp_tests`) cover lowering to a per-execution clock constant, precision arguments, quoted column names, clock column defaults and rejected shapes. Storage tests cover the additive clock-default tag through the existing schema codec owners. Root `api::tests::sql_current_timestamp` covers one transaction-start instant across SELECT, INSERT defaults and later statements, a separate per-statement Cypher clock in the same transaction, increasing values across transactions, read transactions, one instant across all rows of an auto-commit statement, fresh values on plan-template reuse, precision rounding under a pinned host clock, information_schema defaults, recorded defaults after WAL replay and checkpoint reopen, `WHERE due > CURRENT_TIMESTAMP` index range seeks checked through EXPLAIN, and rejected shapes. Existing lanes apply. Replay: `cargo test --locked -p hawdb-sql --lib timestamp`, `-p hawdb --lib sql_current_timestamp`. |
| Cypher result column order | Root `api::tests::result_column_order` covers declared RETURN ordinals, empty matches and LIMIT 0, cache reuse with rebound parameters, DISTINCT, hidden sort keys, WITH, interleaved aggregate/group outputs, numeric projections, SET RETURN and structured EXPLAIN/ANALYZE across database, snapshot and transaction entrypoints. Repeated output names retain the existing collision suffixes. Planner coverage in `plan_cache::output_schema::tests` pins RETURN and LIMIT 0 schemas for vector/text seeds, graph seeds, vector-index search and legacy graph algorithms. Executor result-delivery tests cover named and borrowed value reordering across segments and changing source orders, schema drift, empty-schema memory admission and reservation release after refusal. Owned-result tests bound repeated permutation validation to linear name visits and verify cache admission/release. Existing root/executor CI lanes own these tests; Python `test_smoke.py` checks declaration order and empty schemas on both backends; `test_notebook.py` retains empty-result HTML headers. Focused replay: `cargo test --locked -p hawdb --lib result_column_order` and `cargo test --locked -p hawdb-executor --lib result_delivery`. |
| Relationship-seeded scans from unlabeled sources and relationship-property `IN` ([#1548](https://github.com/nowledge-co/hawdb/issues/1548)) | Root `//:hawdb_unit_general_tests` (`api::tests::relationship_property_seeds::unlabeled_in`) and `//crates/optimizer-graph:hawdb_optimizer_graph_tests` (`graph::tests::costing::relationship_seeded_scan_costs_in_lists_and_unlabeled_sources`). Unlabeled and anonymous endpoints with relationship equalities seed from every node (outgoing, incoming, undirected, LIMIT; the report has no label and counts every node). Mem's `skill_mirror` check `MATCH ()-[r:SYNTHESIZED_FROM]->() WHERE r._nmem_source_table_id = $table_id AND r._nmem_source_offset IN $offsets RETURN true LIMIT 1`, and its row form, equal the node-first expand (`WITH` binds the source) for single, several, empty, NULL-only, NULL-containing, duplicated, missing, other-table, other-type and 60-value lists, resident, out of core (persisted projection plus delta writes), inside an uncommitted transaction and under tight query/batch budgets; a non-list parameter fails the same way. EXPLAIN shows `in=` on the seeded scan with the `PropertyIn` filter kept, the report names the probed `IN` property, a cached plan rebinds the list, and the instance fingerprint follows the values while the template fingerprint does not. A labeled source with only an `IN` list seeds; an empty literal list folds to `EmptyExec`. Costing scales the one-value estimate by distinct non-NULL values. Replay: `cargo test --locked -p hawdb --lib relationship_property_seeds::unlabeled_in`. No new lane, profile or timeout. |
| Optimizer, optimizer-{graph,relational,predicate,vector}, plan-{core,cache,cypher}, explain | Rewrite legality, plan construction, cache/publication concurrency and observable plans. |
| Graph row-goal costing | Existing optimizer-graph unit and root general owners cover consumed work through filters, projections, lookup/probes and expansion fanout; startup, nested/zero/saturated windows, missing statistics, conservative blocking/retrieval boundaries, one postorder costing visit, small/large ordered-alternative choices, exact ASC/DESC native keyset implication (mismatched bounds/direction and extra residuals remain conservative), unbounded prefix-only ordered access with full work in both memo and direct lowering, joint-filter selectivity reaching row goals with selected provenance intact, skewed full-result parity, NULL/type rebinding, deterministic ties, explicit search directives, cancellation and row/payload budgets. Replay: `cargo test --locked -p hawdb-optimizer-graph --lib row_goals` and `cargo test --locked -p hawdb --lib row_goal_planning`; repeat facade coverage with `--no-default-features`. The existing `//:hawdb_linux_ci_optimizer_summary_smoke_test` retains exact costs, decisions, fingerprints and repeated-planning assertions, including the filter-above-aggregate consumed-work cost. Existing lanes, profiles, budgets and timeouts apply. Manual Cargo-only `row_goal_planning` benchmark records source-bound release query/preparation timing and admitted memory/work with full result checks; no fuzz/benchmark CI job is added. See [costing contract](QUERY_ROW_GOALS.md) and [Apple Silicon measurements](benchmarks/row_goal_costing_macos_2026_10_31/README.md). |
| Bounded graph property proofs and qualification | `//crates/cascades:hawdb_cascades_tests` and `//crates/optimizer-graph:hawdb_optimizer_graph_tests` own bounded controls, Applied-only assertions, nullable keys, equality/order, aliases, fanout, evaluator and budget fences. Executor `transform` tests own exact column/literal batch sizing, duplicate-alias suffix preservation, bounded alias inventories and computed/graph-column fallback; root proves singleton-group replacement under the unchanged 2-MiB budget. Root `//:hawdb_unit_general_tests` owns `api::tests::optimizer_proofs`: same-statement pinned execution, errors, strict output limits, parameter rebinding, lifecycle/EXISTS cache identity, actual distinct plans and unchanged raw costs across ranking seeds. Replay with `cargo test --locked -p hawdb --lib optimizer_proofs`. Existing local-only `//tools/shiro:shiro_tests` / `shiro_cli_tests` own opt-in variants, replay, coverage separation and negative oracles; independent Cargo replay filter `optimizer_qualification`, plus `--bin shiro` for CLI controls. The manual `cargo bench --locked --features qualification --bench property_proofs -- distinct 10000 warm 5` reports complete parity and admitted memory. Existing uniform profiles, complete suites, assertions and timeouts remain. See [contract and scope](specs/OPTIMIZER_PROOFS_SPEC.md). |
| Sparse composite prefix ranges | `//crates/optimizer-graph:hawdb_optimizer_graph_tests`, `//crates/storage:hawdb_storage_tests` and root `//:hawdb_unit_general_tests` cover [#1183](https://github.com/nowledge-co/hawdb/issues/1183): missing/NULL suffix values, one or multiple leading equalities, strict/inclusive bounds, checkpoint/delta/reopen and retained views. Storage differentials force owned/admitted access independently of planner selection, preserve Stop and admission refusal, and check native index work for complete keys. Unconstrained wider suffixes use a complete fallback; full-key ranges retain their existing path. The contiguous-prefix projection fixture uses a complete `(prefix, range)` key; candidate guards separately reject an unconstrained suffix. Existing profiles and lanes apply; fixtures stay small. Focused Cargo replay: `cargo test --locked -p hawdb-storage sparse_composite`, `cargo test --locked -p hawdb sparse_composite`, or optimizer-graph package filter `composite_range_candidates`. |
| Native keyless SQL tables ([#1454](https://github.com/nowledge-co/hawdb/issues/1454)) | Existing storage unit owner: `relational::tests::hidden_row_id` covers duplicate identities, stable update handles, snapshot retention, deletion/checkpoint allocation fences, WAL replay, failed durability publication, concurrent allocation and exhaustion. Existing root general owner: `relational_sql::postgres_compat` covers Materialized and OutOfCore/DemandPaged SQL, implicit INSERT, duplicate rows, secondary and unique indexes, UPSERT, rollback, WAL and checkpoint reopen, and ALTER TABLE. `api::tests::hidden_row_id` checks allocation-only checkpoint changes and read-only recovery; `api::concurrent::key_range_tests::hidden_row_id` checks pessimistic lock conflicts. No new CI lane. |
| SQL UPDATE arithmetic on DOUBLE PRECISION, BIGINT and DECIMAL ([#1446](https://github.com/nowledge-co/hawdb/issues/1446)) | Pins the UPDATE assignment contract of the shared numeric kernel (#1429) for balance-style updates. Root general owner `relational_sql::update_arithmetic` covers DOUBLE/BIGINT/NULL parameters and cast literals, BIGINT promotion to DOUBLE PRECISION and exact integral conversion back, multiple assignments reading the original row (including a dependent swap `SET balance = ytd, ytd = balance` and arithmetic on the swapped pair), key and non-key predicates, DECIMAL exactness, BIGINT-to-DECIMAL promotion and assignment rescale, DECIMAL/DOUBLE PRECISION mixing only through CAST (including DECIMAL parameters beside DOUBLE operands; bare numeric literals are coerced per #1516), and all-or-nothing rejection of overflow, non-finite operands, fractional BIGINT results, precision overflow, NULL into NOT NULL and non-numeric operands or targets; plus rollback, WAL replay and checkpoint reopen. `api::tests::concurrent_transactions::double_arithmetic_update_uses_the_same_lock_and_conflict_contract` checks pessimistic point locks and optimistic first-committer-wins. Replay: `cargo test --locked -p hawdb --lib update_arithmetic`. No new CI lane. |
| Unbounded variable-length relationship patterns ([#1489](https://github.com/nowledge-co/hawdb/issues/1489)) | Root `//:hawdb_unit_general_tests` (`api::tests::unbounded_var_length`) and the cypher/plan-cypher crate tests. `*`, `*0..` and `*n..` reach every hop of a chain in both directions and undirected, equal the bounded pattern that covers every relationship, and join a second pattern. On a cycle with a self-loop, trails terminate and equal the covering bounded pattern. A 256-relationship chain is enumerated fully; one more relationship makes the 257-hop trail fail with an error naming the 256-relationship limit instead of truncating. `EXPLAIN` prints `hops=2..`. The parser transition table `migration_main_1489_unbounded_var_length_peg_baseline_v1.jsonl` records the 2 reviewed statement/pipeline transitions, and corpus case `owner-0094` now parses and plans an unbounded expand. Replay: `cargo test --locked -p hawdb --lib unbounded_var_length`, `cargo test --locked -p hawdb-cypher`, `cargo test --locked -p hawdb-plan-cypher`. No new lane, profile or timeout. |
| Out-of-core relationship-seeded node scans ([#1548](https://github.com/nowledge-co/hawdb/issues/1548)) | Root `//:hawdb_unit_general_tests` (`api::tests::relationship_property_seeds::out_of_core`). An out-of-core store seeds `RelationshipSeededNodeScan` from the persisted relationship equality projection of its canonical base (smallest estimated posting first, capped by the label's node count), skipping tombstoned and delta-rewritten relationships and merging the delta through its relationship property index. Every case is compared with the node-first expand (`WITH` binds the source): the canonical base alone (inline patterns, Mem's parameterized form, a missing value), committed delta creates, deletes of canonical relationships and updates that move one into or out of a match or rewrite another property, the same writes inside an uncommitted transaction, a snapshot pinned before them, tight query/batch budgets, and Auto residency crossing its materialization threshold at an in-process checkpoint. The scan-pruning report names the probed property and shows the seeds, and relationship equality projection reads increase. An incomplete projection (an oversized key) and a property first written after the checkpoint (no projection) keep the label scan. Rewritten canonical relationships stay in the delta adjacency and relationship property index (unfiltered expands still see them; the property index consistency report is clean). Replay: `cargo test --locked -p hawdb --lib relationship_property_seeds::out_of_core`. No new lane, profile or timeout. |
| Pattern predicates, pattern comprehensions and relationship-value endpoints ([#1488](https://github.com/nowledge-co/hawdb/issues/1488)) | Root `//:hawdb_unit_general_tests` (`api::tests::pattern_expressions`, plus the updated `relationship_type_disjunction` and `scalar_math_reduce_endpoints` cases) and the cypher, plan-cypher and executor crate tests (`tests::pattern_expressions`, the `migration_main_1488_pattern_predicates` frozen transitions, `logical::pattern_expression_tests` including nested-program visibility, `logical::math_reduce_endpoint_tests`, `expression::value::borrowed::math_reduce_tests`). LDBC SNB IC7 (`not((liker)-[:KNOWS]-(person))` in RETURN), IC10 (a pattern predicate over a list comprehension element and the bound-target friend filter) and IC14 (a pattern comprehension inside `reduce` over `relationships(path)` with `startNode`/`endNode` of relationship values) run on a small social graph and are compared with `EXISTS { MATCH ... }`, OPTIONAL MATCH + `collect`, MATCH counts or hand-derived rows. Further cases cover correlation through row variables, `WITH` columns, comprehension elements and outer columns read in the WHERE; introduced and anonymous variables; relationship uniqueness, bounded variable-length and reversed-anchor patterns; nesting in `size`, `head`, `reduce`, list comprehensions and other pattern comprehensions; WITH, WITH ... WHERE, MATCH ... WHERE (including multi-hop, disjunction and named-target pattern predicates), ORDER BY with and without LIMIT, grouping and aggregate arguments; NULL OPTIONAL MATCH variables (empty list, false predicate); list comprehension WHERE with NULL predicates; cached-plan rebinding of parameters inside the pattern; a 1 MiB query memory budget refusing an oversized produced list; a scanning pattern with no matches hitting the list work limit (work is charged per visited candidate); ORDER BY preferring a projected alias; one-hop WHERE predicates over UNWIND node values carried by WITH; column CASE forms inside a pattern body; relationship values that must name a stored relationship (forged, empty and stale after DETACH DELETE maps fail); prepared source reads refusing pattern expressions; and, with the `acl` feature (`access_control` submodule, replay with `--features acl`), comprehensions and predicates that cannot see or probe hidden nodes and endpoint reads that return NULL for hidden path nodes; and clear rejection of bare patterns, path variables, unbounded and kind-conflicting patterns. The 87 reviewed frozen parser transitions are all previously accepted WHERE predicates with an anonymous source node (see `crates/cypher/fixtures/README.md`). Replay: `cargo test --locked -p hawdb --lib pattern_expressions`, `cargo test --locked -p hawdb-cypher`, `cargo test --locked -p hawdb-plan-cypher`, `cargo test --locked -p hawdb-executor`. No new lane, profile or timeout. |
| Seeded pattern comprehensions ([#1607](https://github.com/nowledge-co/hawdb/issues/1607)) | Root `//:hawdb_unit_general_tests` (`api::tests::pattern_expressions::seeded_pattern_comprehensions_equal_the_unseeded_pattern`, `api::tests::pattern_expressions::access_control::seeded_pattern_expressions_see_only_visible_nodes`) and `//crates/plan-cypher:hawdb_plan_cypher_tests` (`logical::pattern_expression_tests::where_equalities_seed_an_introduced_node`). A WHERE that pins an introduced node in every disjunct (`a = startNode(r)`, LDBC SNB IC14) seeds the match; results equal the unseeded pattern, including a self-loop hop and NULL or non-node seeds, and access control restricts the seeded program. |
| Relationship-value point reads ([#1644](https://github.com/nowledge-co/hawdb/issues/1644)) | Root `//:hawdb_unit_general_tests` (`api::tests::relationship_value_point_reads`). `startNode(r)`/`endNode(r)` of a relationship value, and a relationship value imported into a pattern, read the relationship by id: on a 3000-relationship source each endpoint call is one projected relationship point read, no full relationship read and no adjacency visit (thread-local `record_reads_for_test`/`adjacency_visits_for_test`), resident and from the checkpointed out-of-core canonical base. Also covers committed uncheckpointed overlay relationships (created, updated over a canonical record, deleted canonical tombstone), relationships created or deleted earlier in the same transaction and rollback, and swapped-endpoint values. Forged/wrong-endpoint/ACL cases stay in `api::tests::pattern_expressions`. Replay: `cargo test --locked -p hawdb --lib relationship_value_point_reads`. No new lane, profile or timeout. |
| Retained descriptor leaves for adjacency seeks ([#1615](https://github.com/nowledge-co/hawdb/issues/1615)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner. `graph_descriptor_tree::demand::tests::leaf_scope` builds a three-level tree of 25-entry leaves: inside a `DescriptorLeafScope`, consecutive endpoint seeks equal ordinary prefix scans (values, control, emitted/examined counts, depth) while seeks strictly inside a retained leaf visit one page with no cache lookup or I/O (test-visible hit counter); prefixes equal to a leaf's first or last key, spanning two leaves, or outside every leaf take the root traversal, empty ranges and stopping consumers inside a leaf are answered from it; clones share retained leaves, other codec limits, reopened roots and other threads do not; the retained leaf still counts against `max_pages`/`max_page_bytes` (admission, not poison); a poisoned reader fails closed; nested scopes keep and the outermost releases every pin. `store::graph_read::adjacency_descriptor_read_tests` compares every node's adjacency (both directions, each type and untyped) and `HAS_TYPE\|IS_SUBCLASS_OF*` reachability over a multi-leaf adjacency descriptor tree with an independent relationship scan for resident, cold and warm out-of-core stores, including uncheckpointed additions and tombstones, and requires scoped reads to equal unscoped reads exactly. Replay: `cargo test --locked -p hawdb-storage --lib -- leaf_scope adjacency_descriptor_read_tests`. No new lane, profile or timeout. |
| Per-row query memory ledger operations ([#1615](https://github.com/nowledge-co/hawdb/issues/1615)) | Existing `//crates/executor:hawdb_executor_unit_tests` / Prow crates owner. `memory_ledger::reference_tests::ledger_matches_ordered_map_reference_across_random_operations` drives 64 seeded sequences of reserve, grow, shrink, split, absorb, transfer, drop, `can_reserve` and `available_bytes` against an ordered-map model of the charging rules, and requires equal outcomes (typed refusal scope, class, requested, used and budget), lease bytes, account peaks and snapshots after every step, a zero ledger after all leases drop, and more than 100 admissions, account refusals, query refusals and execution errors each. `leases_keep_their_ledger_and_child_backing_after_every_account_handle_drops` keeps a child grant charged until its last lease drops. `batch::tests::graph_match::ledger_accounting` runs expansion, two-hop, list/map-building, grouping and DISTINCT queries over a fixture with strings, lists, maps and missing properties: each fits a query budget equal to its observed peak with identical rows, is refused with `ResourceBudgetExceeded` one byte below it, and leaves a zero ledger after success, refusal, mid-stream cancellation, consumer error and downstream stop. `binding::tests::row_mutation_previews_equal_full_walks_and_settle_the_ledger` checks replacement and projection previews against full walks across overwrites, list/map growth, nulls and drops. Replay: `cargo test --locked -p hawdb-executor --lib -- reference_tests ledger_accounting row_mutation_previews`. No new lane, profile or timeout. |
| Buffered relational checkpoint encoding ([#1520](https://github.com/nowledge-co/hawdb/issues/1520)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`relational::codec::checkpoint::tests::checkpoint_buffered_payload_*`). The legacy row checkpoint stages encoded fields in one admitted block of at most 64 KiB; each written block is one `write`, one hash update, one I/O wave and one QoS unit, and the overflow-reachability pass takes one unit per 4,096 values. A test-only counting writer (no timing) proves physical writes and work units grow with bytes / block size and value groups, not with fields (96,000 extra fields add 14 writes and 38 units), with output byte-identical to the ordinary encoder. Other cases cover write errors surfacing at the final flush and mid-stream (no write after the failure, header untouched), cancellation at every unit with no write after the stop and a complete retry, and the staging lease being charged while blocks are written, released before the overflow section/header, and falling back to the original write-through path without recording a failure when memory admission refuses it. The existing image, wide-row (32 KiB budget) and durable publication cases keep byte equality and exhaustive CPU/I/O cuts. Replay: `cargo test --locked -p hawdb-storage --lib relational::codec::checkpoint::tests`. No new lane, profile or timeout. |
| Linear relational row-page packing ([#1521](https://github.com/nowledge-co/hawdb/issues/1521)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`relational::row_page::mutation::tests::linear_packing_*`). The packer sizes each candidate page arithmetically instead of encoding the whole page after every row. A reference copy of the previous encode-per-row packer runs on the same input through `RelationalRowPageBootstrap`; page partitions, page ids, encoded bytes and error messages must be identical. Inputs cover narrow rows at and one past the 256-row cap, wide rows, NULL, boolean, double, bytea, empty text and overflow references, strictly increasing text keys whose length shrinks after long keys, a sweep of every third byte limit from the 140-byte header to 6,000 bytes with row caps 1, 2, 3, 7 and 256, and rows the codec rejects (over the row limit, larger than a page, wrong column count, key over the key limit). Replay with `cargo test --locked -p hawdb-storage --lib linear_packing`. No lane, fixture, feature matrix or deadline changes. |
| Row-page dirty budget and pending schema checkpoints ([#1593](https://github.com/nowledge-co/hawdb/issues/1593)) | Root `//:hawdb_unit_general_tests` (`api::tests::relational_row_dirty_budget`). A test-only publication override (`set_relational_row_publication_config_override_for_test`) forces a small dirty budget for one database directory, so each case uses 3,000 rows (about 0.7 MiB encoded; twelve 1 MiB slots under the former v1 format). A 2 MiB budget admits the schema rebuild and a one-transaction update of one row per page, both refused before #1593 because each page was charged its 1 MiB slot; both reopen. Under format v2 the charge is also the bytes written. A 64 KiB budget refuses the schema checkpoint: the DDL commits, the checkpoint stays pending with its reason, resident reads and writes keep working without retrying or poisoning, an explicit checkpoint fails without an integrity error, reopen still opens with the checkpoint pending, and removing the override lets recovery complete it (the on-disk state a poisoned handle leaves on main). Out-of-core handles reject DDL before the WAL, so the out-of-core case creates the pending state on a resident handle: an out-of-core reopen refuses the uncheckpointed schema WAL with a non-integrity recovery error, a resident reopen completes the checkpoint, and out-of-core reads then serve the new table. `api::automatic_checkpoint::tests::relational_schema_pending` keeps automatic checkpoints enabled (20 ms age) while a 64 KiB budget refuses the schema checkpoint and a writer commits every 50 ms for three seconds: background attempts stop at the existing three-failure circuit (measured: 3 attempts, about 0.18 s process CPU including the writes), and the pending reason appears in the automatic checkpoint report and in the relational row residency report until an explicit checkpoint clears both. `api::tests::system_schema_registry` (crash after WAL sync) and `store::relational_row_pages::tests::durable_open_replays_wal_into_a_generation_pinned_row_delta` now expect resident rows, not an integrity error, while the schema checkpoint is pending. The 131k/300k-row production sizes are measured with a release harness, not in CI. Replay with `cargo test --locked -p hawdb --lib relational_row_dirty_budget`. No lane, fixture, feature matrix or deadline changes. |
| Per-table row-page rewrite on schema change ([#1593](https://github.com/nowledge-co/hawdb/issues/1593)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner. `store::relational_row_pages::tests::schema_change_rewrites_only_the_changed_tables` checkpoints three 300-row tables, then: `CREATE TABLE` plus an insert into another table in the same commit writes exactly that table's leaf and an empty new table; an uncheckpointed insert followed by `ADD COLUMN` rewrites only the altered table (including the insert) with page ids above its previous allocator in the new physical generation; `CREATE INDEX` rewrites only its table; untouched tables keep identical `(logical id, physical generation)` descriptors throughout, and a reopen recovers every table, the new column and the index. `relational::row_page::publication::tests::table_replacement_keeps_the_allocator_and_drops_every_base_page` rejects replacements that reuse an id below the base allocator or list explicit deletions, publishes a valid one without base pages, and keeps a pinned root's pages. Replay with `cargo test --locked -p hawdb-storage --lib schema_change table_replacement`. No lane, fixture, feature matrix or deadline changes. |
| Row-page format v2: packed exact-length pages ([#1521](https://github.com/nowledge-co/hawdb/issues/1521), [#1460](https://github.com/nowledge-co/hawdb/issues/1460)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`relational::row_page::publication::tests::format_v2`). Pages are written back to back by exact length: descriptors hold the byte offset, the length and the page's own header digest, and the artifact length equals the packed pages. Read-side tampering is re-signed past the descriptor binding so the page checks fire: an offset into a page, an offset selecting another valid page, a length one byte short, a range past the generation allocation (rejected before page I/O), a descriptor digest flip and an on-disk page byte flip all fail as corrupt. The packer closes multi-row pages at the target and keeps a row wider than the target in a page of its own; a table replacement that rewinds the hidden row-ID allocator is rejected (#1601 review). A page with a consistently re-signed digest and descriptor but an out-of-order row directory is rejected by plain reads, by the cooperative checkpoint read and by the mutation planner. `v1_row_page_manifest_requires_recreation` refuses an `SKRPGM01` manifest with the typed `UnsupportedStorageFormat` error and a recreate message, and an unknown v2 version as unsupported. Existing publication, crash-trace, pinned-reader, scrub, compaction, demand, snapshot and facade tests run unchanged on v2 except where they asserted slot sizes: those now assert exact lengths, byte-based occupancy and exact-length demand charges; full physical scrub recomputes both live page counts and the exact descriptor-length byte sum, rejecting checksum-consistent occupancy drift. The store layout tests pin the former 256-row cap through the test-only publication override. Follow-up fallout fixes: SQL row-read budgets no longer divide intermediate rows by the row cap; pages are bounded by rows read plus one, and the byte limit uses the hard maximum for every admitted page because consecutive single-row pages may all exceed the writer target. `relational_row_read_byte_budget_uses_the_hard_page_maximum` pins that derivation, while `storage_recovery::out_of_core_sql_scan_admits_consecutive_pages_wider_than_the_writer_target` checkpoints and reopens twenty wide one-row SQL pages, scans them under a one-row output cap and rejects the former target-sized false refusal. `content_store_row_pages::tests::initial_content_store_tables_are_qualified_through_canonical_row_pages` also qualifies; the production Content Store and overflow-compaction qualification fixtures keep their row artifact above the segment cache with larger inline payloads (production) and a 64 KiB cache (overflow compaction), since exact-length pages are no longer slot-inflated; the monotonic-append absence proof reads the one page that straddles a partition end (`metadata_only_monotonic_append_is_default_off_and_reduces_hydration_when_enabled` now runs on v2 geometry, and its fallback reads that page per point); the `relational_index_access` bench oracle reads the fixture's published page count with a separate full scan, and the `relational_monotonic_append` bench smoke hits the fast path again. Replay with `cargo test --locked -p hawdb-storage --lib format_v2`, `cargo test --locked -p hawdb-storage --lib physical_scrub_rejects_authenticated_occupancy_drift_and_old_file_growth`, `cargo test --locked -p hawdb --lib relational_row_read_byte_budget_uses_the_hard_page_maximum` and `cargo test --locked -p hawdb --lib out_of_core_sql_scan_admits_consecutive_pages_wider_than_the_writer_target`. No lane, fixture, feature matrix or deadline changes. |
| Parallel DiskANN construction ([#1472](https://github.com/nowledge-co/hawdb/issues/1472)) | Existing vector-projection crate tests (`diskann::parallel_build_tests`). With several admitted workers, batched Vamana construction gives a link-for-link identical graph for 2, 3 and 8 threads, with and without LSG, and under a `max_build_threads` cap. One worker (no task, one admitted thread, an executor-thread limit of one, or a budget that fits only one worker) keeps the sequential construction: its link hash equals the pre-change sequential builder's on a fixed 600-node dataset, with and without LSG. It differs from the batched graph. Worker threads follow the admitted parallelism (one without a task), the cap, the executor-thread limit, and the per-thread traversal scratch that fits in `max_working_bytes`; a budget of exactly the sequential bound still builds and falls back to one sequential worker; several workers also need room for the batch buffers. Cancellation during parallel construction returns no graph. Batch sizes start at one insertion, double up to 1% of the nodes and cover every insertion. The existing recall test against exhaustive filtered ground truth still passes. Replay: `cargo test --locked -p hawdb-vector-projection diskann`. No new lane, profile or timeout. |
| Relationship type disjunctions in MATCH ([#1489](https://github.com/nowledge-co/hawdb/issues/1489)) | Root `//:hawdb_unit_general_tests` (`api::tests::relationship_type_disjunction`) and the cypher/plan-cypher crate tests (`parses_relationship_type_disjunctions`, `relationship_type_disjunctions_stay_generic_graph_matches`, `relationship_type_disjunctions_are_rejected_outside_match_reads`). On a graph with types `A`, `B` and `C`, one-hop `[:A|B]` and `[:A|:B]` equal the union of `[:A]` and `[:B]` outgoing, incoming, undirected and from every node; a bound relationship reports each match's own type; a later MATCH, an OPTIONAL MATCH and an optional `count(r)` keep the union. `[:A|B*1..3]`, `[:A|B*0..]` and `[:A|B*]` follow trails that mix types (which neither type alone reaches) and terminate on cycles. A name absent from the catalog matches nothing (`[:A|Missing]` equals `[:A]`; `[:Missing1|Missing2]` returns no rows, or only the zero-hop row with `*0..`). `EXPLAIN` prints `A|B`, fingerprints of different type sets differ, and alternating `[:A]`, `[:A|B]`, `[:A|C]` and parameterized variants through the plan cache return their own rows. CREATE, MERGE, SET and DELETE queries, shortest paths and relationship existence predicates with a disjunction fail with an error instead of using the first type. The Debug text of single-type patterns and plans is unchanged, so no parser corpus transition is recorded. Replay: `cargo test --locked -p hawdb --lib relationship_type_disjunction`, `cargo test --locked -p hawdb-cypher`, `cargo test --locked -p hawdb-plan-cypher`. No new lane, profile or timeout. |
| Relational overflow pruning per commit ([#1439](https://github.com/nowledge-co/hawdb/issues/1439)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`relational::tests::overflow_pruning`) and root `//:hawdb_unit_general_tests` (`relational_sql::tests::relational_bulk_load_with_overflow_values_keeps_constraints_and_payloads`). A thread-local `#[doc(hidden)]` counter of rows examined for overflow reachability bounds each insert commit by its own rows (40 commits of 50 always-overflowed rows), independently of table size; an update that keeps its value examines its two row images. Exactness covers content-shared segments, release and reuse within one transaction, values of rows an upsert did not store, whole-table column rewrites and checkpoint encode/decode, which rejects unreachable segments. Checked builds also compare every incremental result with the full reachability pass. The SQL case loads parent/child tables with shared overflow bodies across 20 commits, then checks primary-key, unique and foreign-key rejection, cascade delete, value replacement and checkpoint reopen. Replay: `cargo test --locked -p hawdb-storage --lib overflow_pruning` and `cargo test --locked -p hawdb --lib relational_bulk_load_with_overflow`. No new lane, profile or timeout. |
| Cypher ORDER BY NULL placement and aggregate-scoped keys ([#1407](https://github.com/nowledge-co/hawdb/issues/1407), [#1408](https://github.com/nowledge-co/hawdb/issues/1408)) | Root `//:hawdb_unit_general_tests` (`api::tests::cypher_order_by_nulls`), the executor crate owner (`blocking::sort::tests::sort_and_top_n_place_null_last_ascending_and_first_descending`), the plan-cypher owner (`logical::pipeline::tests::aggregating_projections_reject_order_keys_outside_their_groups`) and the expression crate (`tests::sort_direction_places_null_like_open_cypher`). NULL sorts last ascending and first descending for property, projected-column, expression (#1403) and aggregate-output keys, NULL in the first or second of several keys, ORDER BY … LIMIT/SKIP on the asserted `TopNExec` path, `WITH … ORDER BY … LIMIT`, and a range-index-served order (asserted sort elision; its range predicate rejects NULL) next to sorted queries over the same indexed property. A 16-KiB `blocking_operator_bytes` database asserts spilled `SortExec` runs for encoded property keys and re-evaluated expression keys and a spilled `TopNExec` heap against an independent oracle; the executor unit test covers sort/top-k, resident/spilled and column/expression keys in both directions. SQL keeps its PostgreSQL defaults and explicit `NULLS FIRST`/`NULLS LAST`, with and without LIMIT. After an aggregating RETURN or WITH, property, id, column and expression keys over unprojected values fail with a semantic error instead of becoming hidden grouping keys; projected names, aggregates and keys equal to a projected grouping expression (`ORDER BY n.lastName` for `n.lastName AS last`) still order the unsplit groups. Replay: `cargo test --locked -p hawdb --lib cypher_order_by_nulls`. No new lane, profile or timeout. |
| Relationship-seeded node scans for single-hop expands ([#1548](https://github.com/nowledge-co/hawdb/issues/1548)) | Root `//:hawdb_unit_general_tests` (`api::tests::relationship_property_seeds`). A mandatory single-hop expand over a plain source label scan with relationship property equalities plans `RelationshipSeededNodeScan` when fewer relationships are estimated to match than the label has nodes. Results are compared, row for row and in order, with the same expand after `WITH` binds the source (asserted to keep `SeqNodeScan`). The inline cases cover outgoing, incoming and undirected, one or two properties, a shared property, a missing value, decoy labels and types, and LIMIT. Mem's parameterized `WHERE r.table = $t AND r.offset = $o` runs over six parameter sets on one handle (cached-plan rebinding). Uncommitted creates, deletes and updates are visible inside the writing transaction, and a snapshot pinned before them keeps its view. Further cases cover an undirected self-loop, a label union (scan kept), and tight query/batch budgets (equal results, or both refuse on the per-row ceiling). The scan records a node scan-pruning report (60 label nodes, two seeds), and a bound source records none. Out-of-core seeding has its own row (`relationship_property_seeds::out_of_core`). Replay: `cargo test --locked -p hawdb --lib relationship_property_seeds`. No new lane, profile or timeout. |
| Relational scans over many uncheckpointed rows ([#1480](https://github.com/nowledge-co/hawdb/issues/1480)) | Root `//:hawdb_unit_general_tests` (`api::tests::relational_overlay_scans`). After a checkpoint, 20,000 rows are inserted in five uncheckpointed commits, above the former fixed 16,384-entry overlay limit. In materialized and out-of-core residency, `COUNT(*)`, `SUM`, `ORDER BY ... LIMIT` (both directions) and a filtered scan return exact results. Overlay replacements (5,000 updated rows) and deletions (1,000 rows) merge exactly, a transaction scans the same view, and the checkpointed result is equal. With `max_read_result_rows = 100` the full scan still stops at its row bound, and a point read succeeds. Replay: `cargo test --locked -p hawdb --lib relational_overlay_scans`. No new lane, profile or timeout. |
| Relational intermediate-row ceiling separate from result rows ([#1679](https://github.com/nowledge-co/hawdb/issues/1679)) | Root `//:hawdb_unit_general_tests` (`api::tests::relational_intermediate_row_limit`, `api::dependency_read::tests::prepared_read_statement_rows_lower_both_row_ceilings`), `//crates/relational:hawdb_relational_tests` (`query::tests::aggregate_memory`) and the FFI crate tests (`open_options_tests`). Regression guard: with only `max_read_result_rows = 100`, `SELECT COUNT(*), SUM(OCTET_LENGTH(content))` over 3,000 rows (2,000 checkpointed, 1,000 in the overlay) is still refused at the inherited intermediate bound, in materialized and out-of-core/demand-paged modes, also from a session with no result ceiling. With `max_read_intermediate_rows = 10_000` the aggregate returns exact values through the database, a session limited to one row and a transaction, while streaming and ordered `SELECT id` over the table are refused at 100 rows and `LIMIT 100` succeeds. An intermediate ceiling of 500 refuses the aggregate without a result ceiling; `Some(0)` removes only the intermediate ceiling. Unset limits derive identical `RelationalQueryLimits` to the former coupled values for every result/request combination; explicit values drive candidate, index, hydration, overlay and row-page budgets, and request rows lower only output. `PreparedRead` keeps the minimum of each ceiling and `statement_rows`. Constant memory: in the relational crate, the query-memory-ledger peak for the columnar and row-at-a-time `COUNT/SUM` aggregates is the same at 10,000 and 200,000 rows (asserted within 1 KiB), with exact results and a refusal one row below the table size. Replay: `cargo test --locked -p hawdb --lib relational_intermediate_row_limit`, `cargo test --locked -p hawdb-relational --lib aggregate_memory`. No new lane, profile or timeout. |
| Checkpoint publication work units ([#1527](https://github.com/nowledge-co/hawdb/issues/1527)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`immutable_object::work_tests`). With a checkpoint work context, `publish_file_with_work` takes exactly one work unit and one I/O wave per streamed 64 KiB block for the hashed staging copy (5 for a 3-block-plus payload; the synced staging inode is installed without a read-back, see the same-process publication row), and reuse of an existing object validates it with one unit per block. The progress counter advances by the same count, and at most one unit is active. Cancelling at every unit and at every I/O wave leaves no staged file and no installed object, keeps the source bytes, and a retry publishes. The closure digest takes one unit and wave per block, equals the uncontrolled digest, and cancels at each unit. Replay: `cargo test --locked -p hawdb-storage --lib immutable_object::work_tests`. No new lane, profile or timeout. |
| Batched checkpoint unit admission ([#1616](https://github.com/nowledge-co/hawdb/issues/1616)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`background::checkpoint::batch_tests`), `//crates/qos:hawdb_qos_tests` (`batch_tests`) and root `//:hawdb_unit_checkpoint_lifecycle_tests` (`api::automatic_checkpoint::tests::qos_units`: the real owner prepares under a one-operation budget, every started unit ends, and admissions are fewer than units). Checkpoint builder units share one one-operation `LocalQosBatchPermit` per at most `CHECKPOINT_UNITS_PER_ADMISSION` (64) units. Storage cases: 1,000 units give exactly 16 admissions and 16 clock reads, and unit, finished, abandoned and progress counts add up, including `classify` and dropped units. Cancelling after every unit 1..131 (inside and across batches) starts no further unit and leaves no charge; cancellation from another thread is seen by the next unit. A dropped unit releases its batch at once with a failed outcome. A competitor takes the only operation between units and the next unit is deferred, while a running unit keeps it. Nested units join the running batch. An expired deadline is refused at admission, and one that passes mid-batch is observed after the batch's remaining units. `next_input` still takes one I/O wave per item, and two contexts sharing a one-operation budget alternate by reclaiming each other's idle batch. QoS cases: an idle batch is excluded from `state()`/snapshots and reclaimed only when its charge would refuse an admission, with one completion event; a reclaimed batch is not refunded twice; replacing the sink reclaims idle batches; a thread racing `resume`/`idle` against `try_start` never exceeds a one-operation budget. Existing per-unit cancellation sweeps and probes (`CheckpointWorkProbe`, `CancelAt`, `CancelPlanning`, `DenyNextUnit`, `CancelCheckpointAtFile`, `StopPreparedSource`) count units through `QosTelemetrySink::record_qos_unit`; their cut points and exact counts are unchanged. Replay: `cargo test --locked -p hawdb-storage --lib background::checkpoint`, `cargo test --locked -p hawdb-qos --lib batch_tests`, `cargo test --locked -p hawdb --lib qos_units`. No new lane, profile or timeout. Spec: [pressure-driven checkpoint](specs/PRESSURE_DRIVEN_CHECKPOINT_SPEC.md#batched-local-admission-of-builder-units). |
| Checkpoint context bookkeeping per unit ([#1651](https://github.com/nowledge-co/hawdb/issues/1651)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`canonical::checkpoint_writer::state::rebind_tests`, `background::checkpoint::batch_tests`) and `//crates/core:hawdb_core_tests` (`cancellation::tests::reason_fast_path_and_ancestor_walk_select_the_same_first_cause`). A canonical writer encodes 300 nodes under one shared `CheckpointWorkContext` handle and again under a distinct handle per record (the former per-record clone). Started, completed and progress units, batch stats (units, finished, abandoned, admissions, refusals, clock reads) and records are identical. Cancelling after units 1, 2, 7, 63, 64, 65, 129 and 500 stops at the same unit in both modes, and no unit starts after the cut. The test-only `REBINDS` counter shows the shared handle replaces no builder context while a fresh handle per record replaces at least four per record. Batch cases: clones charge one batch and progress counter, and builders copy on write. `rebind` replaces only another context or a `classify` scope. Units without a scheduler count progress exactly and observe cancellation at every unit, and a unit with no progress counter holds nothing. The core case checks that a root token answers from its own state and that linked and child tokens keep their depth-first reason order. Replay: `cargo test --locked -p hawdb-storage --lib -- rebind_tests batch_tests`, `cargo test --locked -p hawdb-core --lib reason_fast_path`. No new lane, profile or timeout. Spec: [pressure-driven checkpoint](specs/PRESSURE_DRIVEN_CHECKPOINT_SPEC.md#per-unit-context-bookkeeping). |
| Same-process published-object re-hash skip ([#1521](https://github.com/nowledge-co/hawdb/issues/1521), [#1440](https://github.com/nowledge-co/hawdb/issues/1440)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`immutable_object::published_tests`, `file_descriptors::tests::published_object_mount_and_cold_handle_skip_rehash_until_forgotten`) and root `//:hawdb_unit_general_tests` (`api::tests::published_object_verification`). A streamed `publish_file` records its synced, hashed staging inode (native identity, length, mtime) after the namespace barrier; the branch-selector dependency check (`verify_stored_trusting_publication`), branch mount and cold immutable-handle validation then skip the identity re-hash and charge no validation bytes for that exact unmodified inode. Buffered publication, reuse of an existing object, reclamation, recovery `verify_and_sync`, and `verify_stored` as used by doctor WAL repair and derived-artifact health (`diagnostic_verification_ignores_publication_records`) never consult the record. A forgotten record (as in a new process), a replaced inode or a later mtime re-hash and reject a flipped byte. End to end, a same-process reopen returns identical graph and SQL results with skips recorded; after forgetting every record, nothing is skipped and scrub passes. Every non-empty skipped object of the fixture (checkpoint image, manifests, descriptor roots and pages, canonical segments, adjacency, row pages, property projection and spill artifacts; at least 10 required), damaged in place with its mtime preserved, is either refused by a format checksum on open/read or reported by `scrub_storage`, and reads that succeed return the clean results; a cold open refuses the same damage at branch admission. Replay: `cargo test --locked -p hawdb-storage --lib published` and `cargo test --locked -p hawdb --lib published_object_verification`. No new lane, profile, fixture amplification or timeout. |
| Mount verification reused by the next cold handle open ([#1440](https://github.com/nowledge-co/hawdb/issues/1440)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`file_descriptors::tests::mount_verification_is_reused_once_by_the_next_cold_open`, `file_descriptors::tests::a_write_racing_the_mount_hash_leaves_no_reusable_record`, `file_descriptors::tests::mount_verification_records_are_bounded_per_project`, `file_descriptors::tests::mount_verification_record_payload_is_bounded`) and root `//:hawdb_unit_general_tests` (`api::tests::published_object_verification::new_process_open_hashes_each_mounted_object_once`). An object found on disk (no publication record) is hashed in full by every mount; the next cold handle open of it charges no validation and hashes nothing (`FileDescriptorMetrics::identity_verified_bytes` unchanged, `mount_verification_reuses` + 1) and reads the exact bytes. The record is one-shot: after eviction a cold open hashes again and refuses an mtime-preserving flipped byte. A refused remount discards an earlier unconsumed record, so the following cold open still hashes and refuses. A later mtime or a replaced inode with equal bytes is hashed in full. The mount stamps the descriptor before its first payload read and records only a stamp unchanged after the hash, so a modification-time change that lands while the mount is hashing leaves no record and the next cold open hashes in full. Unconsumed records are capped per project: mounting one more object than the cap without opening any evicts exactly the oldest record (`mount_verification_evictions` = 1), whose cold open then hashes in full while the newest record is still reused, and remounting an object that still has a record replaces it without evicting another. End to end, a new-process open (every publication record forgotten) returns identical graph and SQL results, `identity_verified_bytes` equals the total size of the distinct mounted objects (the pre-fix behaviour hashed them twice), at least one mount verification is reused, and mtime-preserving damage to the largest mounted object is refused with `StorageIntegrity` at branch admission. `relational_index_view::tests::mounted_context_tests::mounted_base_validation_accounts_once_and_warm_reads_keep_complete_results` now pins that the first cold read after a mount charges only root and leaf, while a cold read after eviction still charges whole-object validation plus root and leaf exactly once; that fixture's damage helper advances the mtime explicitly so coarse timestamp clocks stay deterministic. Replay: `cargo test --locked -p hawdb-storage --lib mount_verification_is_reused`, `cargo test --locked -p hawdb-storage --lib a_write_racing_the_mount_hash`, `cargo test --locked -p hawdb-storage --lib mounted_context_tests` and `cargo test --locked -p hawdb --lib published_object_verification`. No new lane, profile, fixture amplification or timeout. |
| Relational checkpoint read-ahead on open ([#1440](https://github.com/nowledge-co/hawdb/issues/1440)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`relational::codec::read_ahead_tests`). The ordinary relational checkpoint decode and format-validation input reads and hashes the payload in 64 KiB blocks. Fields of mixed sizes, including fields straddling blocks and one larger than a block, reproduce a 3-block payload; the first one-byte field reads exactly one block and read-ahead never runs more than one block ahead. A request past the payload end fails before any read-ahead, with the unchanged message, logical position, underlying offset and an untouched destination. A flipped byte in the last block fails at `finish` with the unchanged checksum message, and `verify_remaining` hashes the unread tail. Replay: `cargo test --locked -p hawdb-storage --lib relational::codec::read_ahead_tests`. No new lane, profile or timeout. |
| Out-of-core relationship property writes ([#1584](https://github.com/nowledge-co/hawdb/issues/1584)) | Root `//:hawdb_unit_general_tests` (`api::tests::out_of_core_relationship_set`). After a checkpoint, an out-of-core store copies a canonical relationship into the delta when a property is written; the copy must enter the delta adjacency and relationship property index because canonical reads skip delta-held relationships. A `SET` on one of two canonical relationships of a node is followed by unlabeled and typed outgoing expands, an incoming expand, a typed relationship scan and an expand on an untouched property of the rewritten relationship, all returning both relationships, inside the uncommitted transaction and after commit, and the property index consistency report is clean. A materialized store runs the same flow as a control. Replay: `cargo test --locked -p hawdb --lib out_of_core_relationship_set`. No new lane, profile or timeout. |
| Relationship mutations with unlabeled endpoints ([#1552](https://github.com/nowledge-co/hawdb/issues/1552)) | Root `//:hawdb_unit_general_tests` (`api::tests::unlabeled_endpoint_mutations`). Relationship `DELETE`, `SET`, `DETACH DELETE` of relationship targets and retarget `MERGE` (to and from the matched target) with the source, the target or both left unlabeled change what the same pattern reads, in autocommit and in an explicit transaction. A label the catalog does not know, a node filter or a relationship property that matches nothing still writes nothing. Relationships created earlier in the same transaction are updated and deleted through unlabeled patterns. With `max_affected_rows = 2` over eight nodes, an unconstrained source does not collect every node, so a one-relationship `SET` and `DELETE` succeed. Replay: `cargo test --locked -p hawdb --lib unlabeled_endpoint_mutations`. No new lane, profile or timeout. |
| Rematching nodes bound by read-side UNWIND ([#1486](https://github.com/nowledge-co/hawdb/issues/1486)) | Root `//:hawdb_unit_general_tests` (`api::tests::unwind_node_match`) and existing `//crates/plan-cypher:hawdb_plan_cypher_tests` / Prow crates owner (`logical::pipeline::tests::unwound_variables_import_as_nodes_in_later_matches`). LDBC SNB IC6- and IC9-shaped queries that `collect` friends, `UNWIND` the list and `MATCH` from the unwound variable with relationship and label constraints return the same rows as the query without `collect`/`UNWIND`, also after `WITH DISTINCT`, across two `UNWIND` levels and with `collect(DISTINCT ...)`. A NULL element matches nothing (OPTIONAL MATCH yields its NULL row), an empty list yields no rows, and integer, string (parameter), map and relationship elements fail with `MATCH variable 'f' must be bound to a node`. The planner imports the unwound variable as a node column; `WITH 1 AS f` and unwound relationship variables keep the existing refusal. Replay: `cargo test --locked -p hawdb --lib unwind_node_match`. No new lane, profile or timeout. |
| Logical rewrites below read-side UNWIND ([#1606](https://github.com/nowledge-co/hawdb/issues/1606)) | Root `//:hawdb_unit_general_tests` (`api::tests::unwind_node_match::unwound_collect_keeps_the_start_index_seek`) and `//crates/optimizer-graph:hawdb_optimizer_graph_tests` (`graph::logical_rewrite::tests::rewrites_run_below_unwind_rows`). `WITH collect(..) AS xs UNWIND xs AS x` (LDBC SNB IC9) still pushes the start filter below the expansion, so the start person is an `IndexNodeSeek`, and the rows equal the same query without the list. |
| Consuming an unread UNWIND source column ([#1606](https://github.com/nowledge-co/hawdb/issues/1606)) | Root `//:hawdb_unit_general_tests` (`api::tests::unwind_node_match::unwind_consumes_a_list_column_only_when_nothing_reads_it`), `//crates/optimizer-graph:hawdb_optimizer_graph_tests` (`graph::match_reads::tests::unwind_consumes_only_an_unread_source_column`) and `//crates/executor:hawdb_executor_unit_tests` (`transform::tests::consuming_row_unwind_moves_the_source_column_out_of_the_rows`). After `WITH collect(..) AS xs UNWIND xs AS x`, a column that no later operator reads and no `DISTINCT` compares is taken out of each row (`consumes_source=true`) instead of being copied into every output row; reads of the column, imports and `DISTINCT` keep it, and the rows equal the query without the list. |
| Single-walk row sizes and moved rows between operators ([#1615](https://github.com/nowledge-co/hawdb/issues/1615)) | Existing `//crates/executor:hawdb_executor_unit_tests` (`binding::tests::single_walk_sizes_equal_separate_walks_across_value_kinds`, `batch::tests::byte_bounded_batch_tests::single_walk_emission_matches_separate_memory_and_payload_walks`, `transform::tests::projection_moves_unchanged_columns_and_copies_everything_else`, `batch::tests::graph_match::moved_projection_and_match_rows_preserve_results_and_settle_the_ledger`). One walk yields each row's ledger charge and observer payload bytes; across every value kind, nested lists and maps, empty containers, nodes, relationships and paths it equals `binding_memory_bytes`/`binding_payload_bytes`, and the operator-boundary emitter charges, splits and reports exactly as the separate walks did and settles its ledger. A projection moves an unchanged non-inline column out of its consumed input row (a column projected twice moves once and is copied once; nested and computed values are copied), MATCH moves each consumed input row and its NULL-extended OPTIONAL row; results equal the copying paths and the ledger returns to zero at one- and many-row batches. Replay: `cargo test --locked -p hawdb-executor -- single_walk moved_projection projection_moves_unchanged`. No new lane, profile or timeout. |
| Group keys and `WITH DISTINCT` by entity identity ([#1615](https://github.com/nowledge-co/hawdb/issues/1615)) | Existing `//crates/executor:hawdb_executor_unit_tests`, `//crates/plan-cypher:hawdb_plan_cypher_tests` and root `//:hawdb_unit_general_tests`. `batch::tests::graph_match::identity_group_keys` runs 26 queries on ten random graphs (equal properties on different nodes, missing properties, a property name sorting before `_id`, self-loops, parallel edges) in memory and with forced spills, and compares the identity grouping with the map-keyed grouping and map-column planning it replaced (`with_legacy_map_keys`, `hawdb_plan_cypher::with_legacy_graph_group_keys`): node, relationship and NULL (OPTIONAL MATCH) keys, entities mixed with scalars, `WITH DISTINCT` over entities and over groupings, RETURN maps, later MATCH, UNWIND, WHERE, ORDER BY/LIMIT and pattern comprehensions over the carried variable; rows must match as multisets, or in order when the query orders totally (a spilled unordered `LIMIT` compares its size). It also requires every slotted shape to plan `graph_keys` slots and forced spills to reach node, relationship, NULL, partial, buffered and DISTINCT groupings. `blocking::aggregate::identity_tests` checks that a slot output shares the input record, COUNT state does not grow with rows sharing one record, the observed peak is an exact admission boundary with a zero ledger, rows of one entity with divergent records (a write between rows) form one group with the first record where map keys split them, and the spill key codec round-trips records and refuses misplaced slots. `ledger_accounting` adds slotted grouping, slotted DISTINCT and map-output grouping to its peak/one-byte-below/zero-settle and cancellation queries. `api::tests::identity_group_keys` compares slotted plans with map-column plans through the embedded API on resident and out-of-core stores, checks that carried records hold every property later clauses read without a re-import, and that no write can feed a grouping in the same statement. The migration corpus records the seven plans that now carry slots. Replay: `cargo test --locked -p hawdb-executor --lib -- identity_group_keys identity_tests ledger_accounting`, `cargo test --locked -p hawdb --lib -- api::tests::identity_group_keys`. No new lane, profile or timeout. |
| Pattern-expression arguments and seeds by entity identity ([#1615](https://github.com/nowledge-co/hawdb/issues/1615)) | Existing `//crates/executor:hawdb_executor_unit_tests`, `//crates/plan-cypher:hawdb_plan_cypher_tests` and root `//:hawdb_unit_general_tests` (the ACL case also under `--features acl`). `batch::tests::graph_match::pattern_identity_imports` runs 18 queries on eight random graphs in memory and with forced spills and compares plans that pass import-only arguments and seeds by identity with plans that copy every value (`hawdb_plan_cypher::with_legacy_pattern_value_imports`): node and relationship arguments from graph slots, `collect()`ed lists, `WITH`/`UNWIND` map columns and OPTIONAL MATCH NULLs; seeds from slots, bound relationship endpoints, list elements, map columns, an enclosing pattern's match, and seeds that are relationships, NULL or scalars (#1607/#1610 shapes). Rows must match (as multisets unless totally ordered), both ledgers settle to zero and the identity peak never exceeds the copying peak; a row refused by the forced-spill payload limit must be refused alike. It also requires slot seeds to skip the `node_owned` existence read that value seeds keep, and wrong-kind, non-entity, missing-node, missing graph variable and missing argument column errors to be identical. `logical::pattern_expression_tests::import_only_columns_reach_the_match_by_identity` pins which columns plan as identities (row-column CASE reads keep their value). `api::tests::pattern_identity_imports` repeats the comparison through the embedded API with the optimizer on resident and out-of-core stores, compares import errors, pins that nodes written by CREATE/MERGE in the same statement cannot reach a pattern seed or import (`nodes_written_in_the_same_statement_behave_as_value_imports`: refused, with the same error as value imports), and (`acl`) checks that hidden nodes passed as list elements, map columns or seeds match nothing under a visibility scope, equal to copied values. Replay: `cargo test --locked -p hawdb-executor --lib -- pattern_identity_imports`, `cargo test --locked -p hawdb-plan-cypher --lib -- import_only_columns`, `cargo test --locked -p hawdb --lib --features acl -- pattern_identity_imports`. No new lane, profile or timeout. |
| Resident relationship property pruning probes ([#1546](https://github.com/nowledge-co/hawdb/issues/1546)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`store::graph_read::resident_pruning::tests`). A `#[cfg(test)]` thread-local count of visited relationship-property index entries bounds Mem's occurrence lookup (`table = 7 AND offset = 42` with a shared table posting) at two probed keys with a one-relationship cap. A shared posting above the cap is rejected after one key, a range reads only its property's 300 keys, and `IsNotNull` reads its three keys, beside look-alike property names and a second relationship type. Pruned results equal plain adjacency for AND, Eq, In with duplicate and missing values, NotEq and Range. The existing filter-family case keeps rows, reports and stop. Replay: `cargo test --locked -p hawdb-storage --lib resident_pruning`. No new lane, profile or timeout. |
| Per-query vector navigation beam and candidate window ([#1470](https://github.com/nowledge-co/hawdb/issues/1470)) | Existing `//crates/search` unit tests (`out_of_core::tests::vector_segments::graph_navigation_beam_and_candidate_window_are_per_query`, `resident_diskann::tests::resident_graph_navigation_beam_is_per_query`). `VectorSearchExecutionOptions::with_navigation_beam` and `with_candidate_window` set the DiskANN beam and candidate window per query. Passing the defaults explicitly (beam 1024, window 128) gives the same hits and graph payload bytes as not passing them. A narrow beam reads less of the graph and still returns the limit. A beam below the candidate count is raised to it, and the window sets the generated candidates but never goes below the limit or rank window. Out of core, a window covering the corpus matches brute force when the reader admits it, and a window above `max_vector_candidates` is refused with a message naming that option. Replay: `cargo test --locked -p hawdb-search --all-features navigation_beam`. No new lane, profile or timeout. |
| Numeric functions, `reduce()` and `startNode()`/`endNode()` ([#1487](https://github.com/nowledge-co/hawdb/issues/1487)) | Root `//:hawdb_unit_general_tests` (`api::tests::scalar_math_reduce_endpoints`) and the core, cypher, plan-cypher and executor crate tests (`numeric_function::tests`, `tests::math_reduce_endpoints`, `logical::math_reduce_endpoint_tests`, `expression::value::borrowed::math_reduce_tests`). `abs`, `ceil`, `floor`, `round`, `sign` and `sqrt` parse in RETURN, WHERE, ORDER BY and nested scalars; every function maps NULL to NULL, `abs` keeps INTEGER, `sign` returns INTEGER and the others return FLOAT, `round` rounds ties toward positive infinity (`round(-1.5) = -1.0`, Neo4j's one-argument form), and non-numeric arguments or a wrong argument count fail. Conversion functions accept arithmetic arguments (`toFloat(a - b)`), which the LDBC IC7 latency expression needs; single-operand arguments keep their AST. `reduce(acc = init, x IN list \| expr)` folds left to right over literals, properties, `collect()`, `nodes(p)` and `relationships(p)`, nests and appears inside comprehensions and WHERE; a NULL list is NULL, a non-list fails, the initial value reads the enclosing row, and names that shadow a visible variable or each other are rejected. Accumulators stay charged to the working-memory account (an oversized fold fails and releases its charge) and each element is a list work unit. `startNode(r)`/`endNode(r)` return the stored source/target node value for outgoing, incoming and undirected matches (including anonymous endpoints), are NULL after an unmatched OPTIONAL MATCH, and fail for a node or a projected relationship value. Planning resolves them to the two node variables of the one-hop step that binds `r`, which GraphMatchExec reads whole; in cyclic GraphMatchExec patterns where another variable (`a`) binds the same node but reads only `name`, the endpoint is still the complete node, outgoing, incoming and undirected. IC7- and IC14-shaped fragments run on a small graph. No frozen parser baseline or corpus case changes outcome, so no transition table is recorded. Replay: `cargo test --locked -p hawdb --lib scalar_math_reduce_endpoints`, `cargo test --locked -p hawdb-cypher`, `cargo test --locked -p hawdb-plan-cypher`, `cargo test --locked -p hawdb-executor math_reduce`. No new lane, profile or timeout. |
| Kùzu `label(x)` and openCypher `labels(n)` ([#1678](https://github.com/nowledge-co/hawdb/issues/1678)) | Root `//:hawdb_unit_general_tests` (`api::tests::entity_label_functions`, `api::tests::graph_match_reads::label_filters_and_returns_read_no_full_records`) and the cypher, plan-cypher and executor crate tests (`tests::entity_labels`, `parser::frontend::tests::frozen_main_1678_label_transitions_preserve_complete_ir`, `logical::entity_label_tests`, `expression::value::borrowed::label_tests`). `label(n)` returns a node's single label and `label(r)` the relationship type (equal to `type(r)` over every row of an undirected match and as an `IN $types` filter); `labels(n)` lists node labels. Resident and checkpointed out-of-core stores cover RETURN, WITH (column and carried node), WHERE `IN $param`, `IN` literal lists, empty/NULL/unmatched lists, comparisons, `NOT ... IN`, ORDER BY in RETURN and WITH, aggregation keys, nested scalars, NULL from OPTIONAL MATCH, a mix with `b.id IN $ids`, plan-cache parameter rebinding and the verbatim Mem identity-view statements (`graph_nodes`, outgoing/incoming `neighbor_rows`, `graph_edges` with and without a Space). Executor units cover the zero-label NULL, the multi-label error, `labels(r)` errors and memory charging; the read-demand case proves a label filter reads no full node or relationship. With `--features acl`, `entity_label_functions::access_control` checks that hidden nodes never contribute a label to filters, aggregates, neighbor reads or OPTIONAL MATCH. The frozen parser corpora change for 18 already-accepted `label(r)` cases (reviewed in `migration_main_1678_label_peg_baseline_v1.jsonl`, each the exact `RelationshipType` to `Label` substitution) and the `owner-0090` plan golden; no case changes between rejection and acceptance. Replay: `cargo test --locked -p hawdb --lib entity_label_functions`, `cargo test -p hawdb --lib --features acl entity_label_functions`, `cargo test --locked -p hawdb-cypher`, `cargo test --locked -p hawdb-plan-cypher`, `cargo test --locked -p hawdb-executor label_tests`. No new lane, profile or timeout. |
| Relational resident row bytes per commit ([#1519](https://github.com/nowledge-co/hawdb/issues/1519)) | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner (`relational::tests::resident_row_bytes`, `store::relational_row_pages::tests::relational_row_bytes`). Each resident row map keeps the exact sum of its row-entry size estimates beside its row count, so durable commit admission and storage-pressure checkpoint sizing are O(tables). A `#[cfg(test)]` thread-local count of sized row entries bounds a single-row durable UPDATE commit at 1,000 and 8,000 resident rows (at most 576 entries, against at least 16,000 for the previous per-commit walk), with a positive control that a real walk is counted. Exactness against the full fold covers insert, replacement, primary-key change, upsert, delete, page splits and emptied pages, allocation-metered inserts and page copies, sparse hydration, both column-rewrite paths, COW clones and staged states, a rejected transaction, checkpoint encode/decode, row detachment, and durable WAL replay plus checkpoint-and-WAL reopen. Checked builds also compare the maintained total with the full fold on every read and on metered checkpoint decode. Replay: `cargo test --locked -p hawdb-storage --lib row_bytes`. No new lane, profile or timeout. |
| SQL composite index intervals | Root `//:hawdb_unit_general_tests` (`api::tests::relational_range`), relational unit and storage unit / existing Prow crates lanes. Compare indexed SQL with an unindexed database across open/closed, one-sided, NULL/contradictory bounds, duplicates, aliases, parameters, residuals, uniform ASC/DESC, OFFSET/LIMIT and first-column ranges. Cover nullable unique suffixes, checkpoint/live/pinned/private/recovery views in Shadow/DemandPaged/Authoritative modes. A 5,000-row prefix must deliver a late LIMIT 3 under a four-row index budget with three observed index rows and early Stop. Tiny-page and scalar/encoded-bound differentials cover trailing-key ties and extrema. Replay: `cargo test --locked -p hawdb --lib api::tests::relational_range`, storage filters `encoded_partial_bounds` / `relational_index_shadow_range_seek`, and the relational index-runtime suite. See [contract](specs/POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md). No new grammar, persisted format, lane or profile. |
| SQL primary-key prefix intervals ([#1523](https://github.com/nowledge-co/hawdb/issues/1523)) | Root `//:hawdb_unit_general_tests` (`api::tests::relational_primary_key_range`), optimizer-relational unit (`relational::cost_tests`) and storage unit (`relational::primary_key_range`) / existing Prow crates lanes. Compare a `PRIMARY KEY (a, b, c)` table with the same randomized rows under a surrogate key (full-scan oracle) across one- and two-column equality prefixes, open/one-sided/two-sided/repeated/contradictory/NULL/extreme bounds, first-column bounds, residuals, aliases, ASC (sort-free) and DESC orders, OFFSET/LIMIT and COUNT, in memory, Materialized and OutOfCore/DemandPaged checkpoints, committed live overlay, reopen/recheckpoint and inside transactions with uncommitted inserts/updates/key moves/deletes plus rollback. The py-tpcc `getNewOrder` shape must plan `TableRangeScanExec` without a selection and visit one row (at most two inside a transaction), through `snapshot_rows` out of core; cached templates rebind NULL/empty intervals. Storage differentials cover middle-column successor bounds. Replay: `cargo test --locked -p hawdb --lib api::tests::relational_primary_key_range`, `cargo test --locked -p hawdb-storage --lib primary_key_range`, `cargo test --locked -p hawdb-optimizer-relational cost_tests`. See [contract](specs/POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md). No new grammar, persisted format, lane or profile. |
| Executor and query/query-policy | Operator execution, result delivery, cancellation and budget/refusal behavior. Small borrowed-projection units in the existing executor unit / Prow crates lane compare 1–8 simple columns with the general path, preserve duplicate-alias and computed/wide fallback, exact wide-row refusal and workspace refunds, and release preparation before a byte-boundary Stop without repeated evaluation. Replay: `cargo test --locked -p hawdb-executor --lib expression::projection::tests`. No new lane, feature, fixture deadline or profile. Text-seed result-owner unit cases cover prepaid window storage and the internal capacity-growth path, including simultaneous replacement admission, exact row limits, sticky refusal and parent-allowance retention. Allocation probes use the repository profile; compare measurements only within the same profile. |
| Complete lexical and grouped evidence | Existing Cypher/Plan-Cypher, executor, Search, readiness and root general lanes cover opt-in complete providers, exhaustion/skipped reports, candidate ceilings before canonical filtering, lazy capacity growth, scalar MAX and bounded per-group evidence. Root differentials cover mixed supported keys, signed zero, ID maxima, permutations, graph eligibility and retained reads across raw/resident/layered disk sources with replacement and deletion. Executor allocation probes cover ID admission, one owned copy of repeated wide scalar keys, retained callback ownership and cancellation/error precedence. Search regressions compare canonical field phrase-alias occurrences and full-corpus scores, including raw window/complete parity and actual allocation peaks. Sparse overlapping-layer regressions cover metadata predicates, external identities and exact live-corpus scores in window/complete modes. The `grouped_evidence` and `grouped_evidence_native` release benches manually qualify scalar and native lexical/graph paths; neither is a CI performance gate. See [measured results](benchmarks/GROUPED_RETRIEVAL_RESULTS.md). See [contract](specs/GROUPED_RETRIEVAL_SPEC.md). |
| Mixed numeric range projections | Existing storage unit (`//crates/storage:hawdb_storage_tests`) and root general (`//:hawdb_unit_general_tests`) CI owners. Scalar node, relationship and composite projection ranges preserve numeric predicates when Value ordering separates Int and Float. Reference envelopes cover clipped equality prefixes, strict/inclusive bounds, extreme values and NaN; homogeneous blocks must still prune. Persistent/API cases refresh bounded statistics and rebind after cold reopen to require native scalar/composite range plans, then cover delta type changes, pinned reads and reopen; the original reference and snapshot assertions remain intact. Replay filters: `mixed_numeric`, `composite_numeric_range_envelopes`. No lane, feature or profile change. |
| SQL numeric literals beside DOUBLE PRECISION ([#1516](https://github.com/nowledge-co/hawdb/issues/1516)) | A schema- and parameter-bound pass (`hawdb_expression::sql::NumericLiteralCoercion`, bound per execution by `hawdb_relational::literal_coercion` for SELECT/FROM-less SELECT and INSERT/UPDATE/DELETE) converts a constant built only from numeric literals (`12.5`, `-12.5`, parenthesized, `1e3`, folded `12.5 + 1.0`) to the nearest DOUBLE when an arithmetic, comparison, IN or COALESCE operand is DOUBLE PRECISION (column, DOUBLE parameter, CAST, SUM/MIN/MAX of DOUBLE), matching PostgreSQL's implicit numeric-to-float8 cast. Expression unit `sql::literal_coercion::tests` covers literal forms, nearest-double conversion, unchanged DECIMAL columns/parameters/casts and integer literals, and folding errors. Root general owner `relational_sql::numeric_literal_double` differentially compares each literal form with its `CAST(... AS DOUBLE PRECISION)` spelling in UPDATE SET (all five operators, both orders) and WHERE, DELETE WHERE, INSERT VALUES, SELECT list/WHERE/IN/COALESCE/aggregate/HAVING/JOIN with an indexed DOUBLE column and FROM-less SELECT; keeps DECIMAL columns, parameters and casts beside DOUBLE strict (#1429); and pins exact BIGINT assignment with fraction/out-of-range messages instead of the 38-digit limit. CASE, BETWEEN, GREATEST/LEAST, ABS and ON CONFLICT arithmetic are unsupported SQL and outside this family. Replay: `cargo test --locked -p hawdb-expression literal_coercion` and `cargo test --locked -p hawdb --lib -- numeric_literal_double update_arithmetic`. No new CI lane. |
| Storage descriptor bound traversal | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner. Forward and reverse inclusive-bound scans retain ordered delivery, tree-height/page/descriptor admission, early Stop and caller refusal. Reverse guards cover cross-leaf/group boundaries and binary upper-bound work on a verified warm page; skipped prefixes must not be visited. Replay: `cargo test --locked -p hawdb-storage --lib reverse_bound_scan` and `warm_page_views_reuse_integrity_proof`. No new profile or lane. |
| Composite ordered query and cache | Existing optimizer-graph / Prow crates and root general lanes cover exact two-direction keyset concatenation versus full canonical sorting, runtime capability, non-NULL typed unique suffix proofs, strict continuation, parameter numeric-shape changes, ordered prepared-plan invalidation versus unrelated point-plan reuse, ambiguous aliases, mixed directions, index availability across checkpoint, retained reads, moves/deletes/inserts, consumer error/retry and reopen. See [contract](specs/COMPOSITE_ORDERED_SEEK_SPEC.md). Plan-cache units verify used-binding type-proof ownership, unused-list omission and shape separation. Actual resident native plans and canonical checkpoint/delta/reopen plans are asserted separately; pre-checkpoint OutOfCore pages retain full semantic references without forcing a cost decision. Focused replay filters: `ordered_composite` and `parameter_type_proofs`. The opt-in `composite_ordered_seek` manual benchmark measures raw alternating samples, index work and admitted memory at shallow/deep continuations; it is not a CI performance gate or device-cold claim. Persistent preparation uses explicit Materialized mode before checkpoint/OutOfCore reopen. Fixture loading uses bounded transactions, followed by full uniqueness validation, index creation and an exact row-count preflight; complete setup cost is separate. See [source-bound read measurements](benchmarks/composite_ordered_seek_2026_10_23/README.md). No existing lane/profile changes. |
| Ordered composite projection and overlay | Existing storage unit / Prow crates owner. Immutable ordered pages compare with a complete two-direction reference, including global block ordinals, exclusive continuation, deep descriptor work, cold/full versus warm/logarithmic block decoding, verified-cache capacity refusal and admitted scratch refund after refusal/Stop. Store pages merge canonical base with moved/deleted/inserted delta keys across retained views, checkpoint and reopen; the low-level domain consists of complete non-NULL keys, with canonical-query coverage proved separately by planning. Explicit ordered checkpoints preserve cancellation without payload probes or work-unit charges. Replay filters: `composite_ordered_pages`, `ordered_composite`, `ordered_scratch_checkpoint`. No new CI lane or compilation profile. |
| Storage borrowed key intervals | Existing `//crates/storage:hawdb_storage_tests` / Prow crates ownership. Forward, reverse and alternating-end traversal matches a BTreeMap reference across immutable segment boundaries, empty/singleton/outside intervals and COW mutation with a retained snapshot. Replay: `cargo test --locked -p hawdb-storage --lib borrowed_key_interval`. No new feature/profile/lane or timed campaign. |
| Storage, integrity, cascades and artifact/bootstrap | WAL/checkpoint, encoded artifacts, indexes, corruption/refusal and startup ownership. Projection capture covers 65,537-node commits, oversized-preimage gaps, cold node/adjacency/relationship/spill capture under strict foreground descriptor pressure, nested resource/cancellation refusals versus physical corruption across canonical readers, and cancellation across grouped replay work. |
| Native projection keys and immutable reuse | Existing storage unit/native platform owners cover deleted native node candidates for built-in and custom labels, materialized/out-of-core residency, pinned feed end epochs, WAL failure/uncertain recovery, checkpoint and reopen (`cargo test --locked -p hawdb-storage --lib search_projection_capture`). The restart-safe change-log checkpoint case also retains the deleted native candidate together with its legacy document tombstone and verifies canonical absence after reopen (`cargo test --locked -p hawdb-storage --lib checkpoints_search_projection_changes_for_restart_safe_catch_up`). `captured_link` cases in `file_io::source_directory` cover native inode reuse after both labels move, occupied names, foreign project/source identities, component validation, Unix symlinks and platform descriptor refusal/retry. The recorder cases model lost unsynchronized links, before/after cuts, required target-directory barriers and retention after source unlink; they do not qualify complete Consumer selection. Replay with `cargo test --locked -p hawdb-storage --lib captured_link` and `cargo test --locked -p hawdb-storage --lib power_loss`. Windows replaces captured directory labels before opening descendant files; Unix retains replacement after file capture. A separate Windows link case attempts ancestor rename with an already-open file and verifies original-object linking and unlink custody after either success or native pre-effect refusal. All inode, foreign-name and recorder/barrier assertions remain. Windows strict Clippy also owns its native FileLinkInformation path. Registered Consumer installation uses these links only for sealed immutable producer output; arbitrary source installation retains independent copying. No lane, profile, source-format or durability-default change. |
| Search change log across schema commits ([#1745](https://github.com/nowledge-co/hawdb/issues/1745)) | Additive schema commits (label/type/table/property creation including `DEFAULT`, index and constraint creation, table/property states `WRITE_ONLY`/`BACKFILL`/`VALIDATING`/`PUBLIC`) must leave a continuous change log: after each one, incremental graph-delta catch-up must reach the graph epoch and equal a full `rebuild_search_projection`, and Cypher reads must be unchanged. Default backfill fills are captured as record changes; records created under a default receive it at write time. Retiring states (`DELETE_ONLY`, `GC`) keep the rebuild floor, and rebuild-then-catch-up converges. Continuity holds after reopen with and without a checkpoint in both residencies. The embedded incremental out-of-core path covers declaration, reopen, Mem's declare-then-import preparation flow and backfill. Storage closure cases pin the classification boundary for nested, direct-single and failed-append schema commits. | Existing root general unit owner (`src/api/tests/search_projection_schema_states.rs`, `src/embedded_store/tests/schema_change_log_gap_tests.rs`) and storage unit owner (`search_projection_capture::closure_tests`); no new target or lane. Replay: `cargo test --locked --lib search_projection_schema_states`, `cargo test --locked --lib schema_change_log_gap`, `cargo test --locked -p hawdb-storage --lib search_projection_capture`. Rules: [STORAGE.md](STORAGE.md). |
| Search | Unit behavior plus nine explicit feature profiles; public streamed-document and source-policy lifecycle fixtures have independent owners below. |
| Default resident DiskANN | Existing Search unit targets and the unchanged vector feature profiles own `resident_diskann` (`cargo test --locked -p hawdb-search --lib resident_diskann`). They compare default filtered graph candidates with explicit exhaustive scoring across replacement, deletion, NULL and reinsertion, preserve original graph identity, and cover zero/signed-cosine behavior, non-finite-source refusal, offset pagination, hybrid fusion versus exact scoring, validated delta atomicity, checkpoint/reopen, typed memory/work refusal, cancellation and retry, and retained runtime allocation ownership through cache destruction. Concurrent admission blocking must not retain the cache lock, and initialization wait obeys the query deadline. Checkpoint cases require HDANN002, actual native graph execution, unchanged retained-reader bytes/results, and explicit RaBitQ settings across reopen/replacement. Reopen must reuse a verified mapping, remain queryable below the construction envelope, and keep its original baseline across CRUD and clean checkpoint copying. Corruption cases retain quarantine evidence and reject stale-source fallback. Directory replacement, stale cleanup and memory/cancellation fixtures retain both explicit RaBitQ and default graph coverage. Native registered source/MVCC and persistent artifact qualification have separate owners below. No CI lane or profile change. |
| Default out-of-core DiskANN | Existing Search targets and feature-matrix lanes own `generation_writer::tests::diskann` (`cargo test --locked -p hawdb-search --all-features --lib generation_writer::tests::diskann`). Filtered default queries compare with explicit exhaustive scoring after insert/replacement/delete/NULL deltas, with and without LSG. Guards check inherited topology settings, unchanged base graph bytes/native inode, retained old-reader results, abandoned-candidate rollback, cancellation and typed graph memory refusal, empty and deletion-only layers, legacy zero-cosine semantics across mixed graph/compatible layers, and refusal to satisfy explicit compression with a raw graph. Readiness also covers an empty-vector first segment, complete graph/visibility identity, exact live vector counts after mixed text/vector retractions, deletion-only and NULL-only heads, empty vector domains, old pins, cold reopen, and reuse of validated retraction counts during refresh. Mixed graph/legacy-codec closures cannot qualify as a pure graph. Explicit RaBitQ parameters also survive default delta preparation. The ordinary reader-refresh identity case checks model/version/dimension drift against the persisted graph binding. These are standalone projection segment-reuse cases; schema-bound canonical source-feed publication and power-loss qualification remain separate. Existing RaBitQ fixtures select their backend explicitly. No feature matrix, profile or lane is removed. See [delivery contract](specs/FILTERED_DISKANN_SPEC.md). |
| Analytics and vector-projection | Streaming algorithms, projected artifact IO and vector representation behavior. `//crates/vector-projection:hawdb_vector_projection_tests` also owns the Filtered DiskANN kernel/artifact family (`cargo test --locked -p hawdb-vector-projection --all-features diskann`; omit `--all-features` for standalone file I/O): Vamana full-beam/exhaustive distance parity, small-beam filtered recall against exhaustive ground truth, navigation without rejected-vector scoring, optional LSG and duplicate radii, deterministic construction, typed work/memory refusal, work-bounded workspace on a virtual large graph, and cancellation. File cases cover parameter/link/distance round trips, byte-identical mapped successor copying and cancelled-copy source retention, bounded header inspection, source/epoch/generation drift, truncation/trailing bytes/checksums, semantically invalid records with a matching checksum, admission refusal/retry and release of all charges. Core `projection_distance` tests also cover raw queries against unaligned byte-backed vectors and cooperative failure. Small synthetic fixtures use the repository profile; this is kernel/artifact evidence, not default-engine or database CRUD/MVCC qualification. See [delivery contract](specs/FILTERED_DISKANN_SPEC.md). |
| Exact vector arithmetic and prepared queries | Existing Core, Search and vector-projection owners cover ordered-f64 scalar/AVX2/NEON bit parity, forced short/unaligned native loads, finite/zero/dimension checks, work/error precedence, query-norm reuse, native DiskANN providers and raw-score parity. Replay: `cargo test --locked -p hawdb-core vector::`; `cargo test --locked -p hawdb-search raw_vector_math_tests`; `cargo test --locked -p hawdb-vector-projection --all-features diskann`. Native tests differ from cross-target compilation. Prepared distance entry points retain direct scalar evaluation below 16 dimensions, including work/error precedence. Auto uses portable arithmetic below 16 dimensions and for AArch64 L2/norm; NEON/AVX2 implementations remain directly covered. The ignored Core `exact_distance_measurement` and Cargo-only `exact_vector_profile` example are manual global-release component/complete-Search measurements, excluded from CI/fuzz scheduling: `cargo test --locked --release -p hawdb-core exact_distance_measurement -- --ignored --nocapture --test-threads=1`; `cargo run --locked --release --example exact_vector_profile`. No per-target profile or timeout changes. See [native ARM experiment, including adverse file controls](benchmarks/exact_vector_simd_macos_2026_10_30/README.md). Ref [#1339](https://github.com/nowledge-co/hawdb/issues/1339). |
| RaBitQ metric and score policy | Existing vector-projection unit owner and Search default/vector feature-matrix owners cover both 1/4-bit cosine and norm-corrected squared L2, inverse-alignment self similarity, exact/subnormal/maximum norms, zero queries, allowlists, delta metric inheritance, file/reopen parity, checksum-valid norm corruption and obsolete-format refusal. Search covers ordinary Mem Float32 scores (including the 0.80 boundary and zero-score far-neighbor order), explicit/default backend selection, metric-aware cold-cache validation for wrong dimensions/non-finite/zero queries, empty filtered domains and zero-size pages without cold RaBitQ/DiskANN cache construction while reusing existing caches and backend diagnostics, cancellation before construction, during capacity admission and during row fetch, cancellation-aware cache locking, refusal before cache construction and on failed Required scans (never partial/empty success), mixed DiskANN/compatible-layer zero-cosine behavior, shared transient quantizer admission/poisoning without duplicate charge, pinned checkpoint/reopen, streamed deletion-only successors with retained readers and schema-bound cosine/Euclidean raw reranking. Existing conflict-before-checkpoint coverage now rejects schema-binding conflicts while accepting RaBitQ. Replay: `cargo test --locked -p hawdb-vector-projection --lib metric_tests`; `cargo test --locked -p hawdb-search --all-features --lib vector_metric_tests`. Core codec tests retain Euclidean RaBitQ descriptor round trips. The root general owner also extends `vector_index_incremental_retains_explicit_rabitq_and_euclidean_zero_semantics` with both RaBitQ Euclidean widths, preserving the original cosine RaBitQ and Euclidean DiskANN controls through SQL CRUD, canonical catch-up and cold Consumer reopen. The ignored `rabitq_metric_measurement` is a manual checked-profile 32/2,048-row, 1,536-dimensional nonunit corpus probe of both widths/metrics and 64/256-candidate windows, with raw rerank counts, quality and scalar/Auto result parity; replay with `cargo test --locked -p hawdb-search --all-features rabitq_metric_measurement -- --ignored --nocapture --test-threads=1`. It is excluded from ordinary CI execution; see [measured component results](benchmarks/rabitq_metrics_2026_11_03/README.md). See [contract](specs/RABITQ_VECTOR_PROJECTION_SPEC.md). No CI lane, profile or timeout changes. |
| RaBitQ SIMD scoring | Existing `//crates/vector-projection:hawdb_vector_projection_tests` and native vector-platform CI own bitwise scalar/AVX2/NEON parity, CPU selection, all code values, unaligned and short inputs, dimension tails, filtered memory/file scans, ties and worker merging. Replay: `cargo test --locked -p hawdb-vector-projection kernel::`. The two ignored `simd_` release probes are local-only throughput and deadline-response measurements, run serially. The existing `rabitq_query_profile` example adds a local `compare` mode for interleaved scalar/native-SIMD complete-query measurements over 100,000 vectors, including raw-vector reranking and hydration with exact final-result parity. These probes do not replace native AArch64 qualification. See [method and limitations](RABITQ_VECTOR_PROJECTION_BENCHMARK.md). No CI lane, profile or fuzz job is added. |
| QoS, cgroup, telemetry, resource-profile and runtime-tokio | Resource detection/admission, metrics and synchronous/async runtime boundaries. Existing `//crates/qos:hawdb_qos_tests` owns SharedHost SSD no-hint 12/3 defaults, explicit-hint clamps, unchanged non-SSD/mobile profiles and independent foreground/background I/O saturation/refund. Host-detector initialization refusals preserve unknown host readings, the fallback budget and independent cgroup pressure; native recovery reuses the successful detector. Focused replay: `cargo test --locked -p hawdb-qos resource::tests`. No benchmark or device-discovery qualification is implied. |
| Readiness, evidence, route-ownership, nowledge-contracts, compat and qualification | Typed readiness, complete evidence identity, compatibility and host integration contracts. |
| `crates/fuzz-contracts` | Production readiness contract values only. Its fast unit tests are not the Shiro generator/campaign. |
| Python DB-API (`bindings/python/tests/test_dbapi.py`) | The PEP 249 layer in `hawdb.dbapi`: module globals and the PEP 249 exception hierarchy, with every engine error and every built-in error from statement parameters raised as its PEP 249 class while keeping its own type, attributes and pickling, or raised unchanged if it cannot be rebuilt as that class; a duplicate primary key and a NOT NULL violation raised as `IntegrityError` and a wrong-type value as `DataError` in autocommit and in a transaction, each still the `StorageError` or `ExecutionError` of its path, selected by the binding's `kind` (`tests/test_transactions.py` pins that `kind` on both native SQL paths, the binding's Rust `errors::tests` and FFI `error_tests` its mapping, and root `embedded::tests::relational_refusals_carry_their_kind_on_both_sql_paths` the engine's `HawDBError::Constraint`/`Schema` surface); tuple rows in SELECT-list order, a `description` with type codes, also for a key lookup that matches nothing, and identity-hashed type objects; type codes from a column's first non-null value anywhere in the result, known before any fetch, including NULL-led columns longer than a fetch chunk and all-NULL columns (`None`); rows converted per fetch (#1726): the cursor keeps no tuple it returned and releases the `QueryResult` once exhausted, on the next statement and on close, and a `tracemalloc` regression on 20,000 rows bounds `execute` to under 256 KiB of Python allocations and the `fetchall` peak to 1.25 times the returned list (the binding side, `fetchmany_tuples`, fresh dicts per fetch and `_first_non_null`, is in `tests/test_smoke.py`); `rowcount` from the engine's affected-row count for INSERT, UPDATE (including no match) and DELETE in autocommit and in a transaction, with `ON CONFLICT DO NOTHING` skips uncounted and -1 for DDL and `executemany`, plus `QueryResult.affected_rows` on `execute_sql` in both modes (Root `embedded::tests::autocommit_sql_reports_committed_affected_rows` and `transaction_sql_reports_provisional_affected_rows` pin the engine side under `//:hawdb_unit_general_tests`); lazily begun connection transactions with commit, rollback, close and context-manager semantics; autocommit set at connect or between transactions, read-only and branch-command routing; `executemany` delegation and the aborted state after a failed batch; parameter containers matching the native rule; and refusal of closed objects and of other threads, including reused thread idents. Owned by `//bindings/python:hawdb_python_tests` and the Python Bindings pip job. Engine transaction semantics run on file and in-memory databases, durability, read-only and branch cases on a file database, and the rest in memory. The engine's column order per query shape belongs to the SQL result column order family. Catalog queries also pin tuple order and empty-result descriptions under both autocommit and transactions; SQL EXPLAIN and EXPLAIN ANALYZE pin their diagnostic column and tuple order. The `pandas.read_sql` smoke skips without pandas; the pip job installs a pinned pandas and selects it with `-k pandas`. Replay: `pytest tests/test_dbapi.py` in `bindings/python` with the wheel installed. |
| Embedded SQL admission, transaction statement recording and fail-closed parity (#1732, item 4 of #1234) | Root `embedded::tests::admitted_autocommit_sql_is_refused_when_saturated_and_reports_affected_rows` and `admitted_transaction_sql_is_refused_when_saturated_and_keeps_the_transaction`: with every CPU slot of the facade's governor held, `HawDBEmbedded::query_sql_request_admitted_with_result` and `transaction_query_sql_request_admitted_with_result` refuse INSERT, SELECT and unparsable text with a retryable `EmbeddedQueryError::Admission` (`cpu_saturated`) and run or stage nothing; a pre-cancelled task context is `Stopped`; the transaction stays open; after release both report committed or provisional `affected_rows` and the governor returns to idle. `admitted_sql_result_budget_follows_the_governor_and_names_the_read_ceiling`: with the governor's result budget aligned to a 24 MiB `max_read_result_payload_bytes` (as the Python binding aligns it to its configured ceiling, 64 MiB by default), an admitted ~20 MB SELECT succeeds in autocommit and in a transaction, and a ~40 MB one fails with `HawDBError::ReadBudgetExceeded` (`PayloadBytes`, limit = the ceiling, `budget_option()` = `max_read_result_payload_bytes`). `admitted_branch_catalog_sql_does_not_wait_for_a_busy_runtime`: an admitted `SHOW BRANCHES` on a reader whose runtime is deferred behind a writer is served from project metadata, as on the raw path. `transaction_and_admitted_sql_are_recorded_like_autocommit_sql`: explicit-transaction SQL (admitted and not) and admitted autocommit reads appear in `system.statement_summary` with their own text and statement kind, a refused statement as an error, unparsable text not at all, and successful ones in the slow-query log. `transaction_sql_fails_closed_on_a_poisoned_handle`: after a storage integrity poisoning, transaction reads, writes and admitted statements fail with the poisoned-handle error like autocommit. Binding Rust `database::tests` drive `Database.execute_sql`, `Transaction.execute_sql` and `Database.executemany_sql` with a saturated governor and expect `hawdb.exceptions.AdmissionError` with `code` and `retryable`, an open transaction (or a rolled-back batch) and working `affected_rows`; `errors::tests::admission_refusal_carries_its_code_and_retryability` pins the mapping, and `read_ceilings_are_typed_budget_errors_naming_their_option` maps read ceilings to `ResourceBudgetError` with `budget_option`. Python `tests/test_dbapi.py::test_a_large_select_keeps_the_default_read_ceiling` reads ~20 MB through the DB-API in autocommit and in a transaction at default options, and `test_a_result_over_the_read_ceiling_names_its_option` expects `OperationalError`/`ResourceBudgetError` naming `max_read_result_payload_bytes` in both modes. Python `tests/test_transactions.py::test_transaction_sql_is_recorded_in_the_statement_summary` checks the statement summary through the wheel; `tests/test_dbapi.py::test_an_admission_refusal_is_operational_and_keeps_its_code` the DB-API class. | Root tests under `//:hawdb_unit_general_tests`; replay with `cargo test --lib embedded::tests`. Binding tests in the Python Bindings pip job's `cargo test --release --locked --lib` step (`database::tests errors::tests`); Python tests in `//bindings/python:hawdb_python_tests` and the pip job. The wheel exposes no way to saturate the governor, so the native refusal is pinned in the binding's Rust tests rather than pytest. |
| LlamaIndex property graph store (`integrations/llama-index-graph-stores-hawdb/tests`) | `HawDBPropertyGraphStore` under LlamaIndex's `PropertyGraphStore` contract, on in-memory and file databases: replaced nodes, rewrites for changed properties alone, kept relations and placeholder endpoints, and indexed copies of `triplet_source_id` and `ref_doc_id` that follow a rewrite; chunk text, label, empty-label and vector embedding round trips, including a rewrite for a changed embedding alone; LlamaIndex node round trips and deletion by chunk or document; id and property filters, JSON comparison by type, `None` versus empty lists and refused keys, values and out-of-range integers; hostile labels, ids and values bound as parameters; entity names matched by name where they differ from ids; entity-only triplets, with every property member matching on the same end; breadth-first `get_rel_map` depth, limit and ignored labels, with the limit counting each relation once and endpoints found by the ids they are stored under; per-filter deletes; the statements of each read snapshot and write transaction, recorded through a proxy under patched limits: reads split into pages by id count and node size, through the indexed copy of `ref_doc_id`, `get_triplets` over a snapshot, and `get_rel_map` as one statement per hop followed by paged endpoint sizes and reads; writes split into transactions by bytes and by row count; digest reads before any write, no write for unchanged nodes, one statement for new nodes, one per changed node, and one each for relation endpoints and relations, because each write statement the store runs takes time that grows with the stored graph; deletes split into transactions by nodes and relations, each label transaction deleting its own sources' relations, as a failed second transaction shows; filters combined in one transaction; relationships of any type counted; a node whose relations go to other deleted nodes deleted after them; and a node or relation source over the limit refused before anything is deleted; 1,500 vector embeddings of 3,072 dimensions split across 16 MiB WAL records; 3,000 chunks of 25,000 characters written, read and deleted although they exceed the 64 MiB one statement in a read snapshot returns and, on a file database, a 16 MiB WAL record; node pages that count ids, labels and names, with nodes whose labels, or ids and names, exceed a 1 MiB result limit read back by id and in full; the widest nodes and relations the store accepts, which a statement matching a relationship holds in one 16 MiB intermediate row, read back in full by `get`, `get_rel_map`, `get_triplets`, the schema and a delete by relation label: nodes of 4 MiB of values with 64 KiB ids and labels, 1 MiB of properties also kept as an indexed copy and, in entities, an embedding, linked by relations with 64 KiB labels and 1 MiB of properties; writes refused before any transaction commits (node values over 4 MiB of UTF-8, counting 4 bytes per embedding coordinate, also in CJK, node ids and labels and relation labels and endpoint ids over 64 KiB of UTF-8, also in CJK, node and relation properties over 1 MiB of JSON, non-finite or out-of-range embeddings, unencodable strings, NaN or unserializable properties, other node classes); schema labels, counts of the store's own label and type only, refresh and `aget_schema_str`; constraint and index creation that a reopened or read-only database reuses; `WrongThreadError` from every public method on another thread and on LlamaIndex's async worker thread; and one offline `PropertyGraphIndex` insert, retrieve and delete with a fixed extractor, mock LLM and embedding model and a whitespace splitter. Owned by the Python Bindings pip job on Ubuntu only: after the pandas step it installs a pinned `llama-index-core`, installs the package without dependencies over the wheel built from the same commit, runs `pip check` and the tests (about 19 seconds locally, about 14 of them opening file databases; the 3,000-chunk case takes about 0.5 seconds per database and the embedding case about 0.3). Not part of Bazel or `//bindings/python/...`; needs no network or API keys. An autouse fixture collects garbage on the test thread after each test, so no `hawdb.Database` is freed on LlamaIndex's worker thread. Replay from the repository root with a hawdb 0.28.0 or later wheel installed: `pip install ./integrations/llama-index-graph-stores-hawdb pytest` and `pytest integrations/llama-index-graph-stores-hawdb/tests`. |

## Default WAL recovery

The existing storage unit owner includes `store::doctor::recovery_tests::recovery_995_*`:
bounded suffix evidence, real recorder images with lost/torn/reordered relaxed
writes, durable-prefix protection, policy changes, grouped synchronization and
per-persistence-step IO retry and native repair crash images. The existing facade recovery owner includes
`api::tests::storage_recovery::wal_tail` for typed readiness and subprocess
append/repair cuts. Both are explicitly selected by the Windows native storage
shard; no new fuzz CI lane is added. Local replay uses
`cargo test --locked -p hawdb-storage --lib recovery_995 -- --nocapture` and the
existing facade recovery target. The local WAL-tail oracle remains
`bazel run //crates/fuzz:hawdb_storage_fuzz -- --wal-tail --seed 174 --cases 64`.
See [recovery protocol](STORAGE.md#repair-evidence-and-retry) and
[qualification limits](BRANCH_POWER_LOSS_QUALIFICATION.md#default-wal-recovery-and-synchronization-fences-995).

The same manual `concurrent_writers` benchmark accepts
`--after-relaxed-interval` to seed one relaxed append, then measure synchronous
ungrouped/grouped commits in that generation. It reports the first relaxed
append separately and preserves all row, epoch and reopen checks. Compare this
opt-in diagnostic against an identical baseline harness; do not pool it with
the ordinary post-checkpoint/reopened cohorts or claim that it measures steady
relaxed throughput. It has no additional CI owner.

## WAL identity performance qualification

The manual `concurrent_writers` harness retains its post-checkpoint control and
adds an explicit `--reopen-before-measure` fixture for captured WAL ownership.
The manually dispatched writer job runs separate fixed 40-pair cohorts with
identical baseline/candidate harnesses, retained raw samples and unchanged
per-cohort thresholds. It is excluded from default/scheduled execution.
The concurrent matrix selects writer counts from one through half of the
effective physical-core/cgroup capacity, with explicit manual writer/reader
overrides. Fresh snapshot readers verify atomic row pairs and published epochs
while a retained initial snapshot stays stable. Timed request intervals must
prove mixed progress, and every final durable state is reopened and checked.
Read/write p50/p95/p99 and fixed-work rates remain separate from the historical
single-stream control. CPU topology fixtures run locally; the manual workflow
retains complete paired receipts. See [matrix and replay protocol](CONCURRENT_WRITER_QUALIFICATION.md).
Replacement, rotation, symlink/unlink, strict-capacity and captured append
recovery regressions remain in their existing storage/native CI owners.
See [protocol, coverage diagnosis and retained evidence](benchmarks/wal_identity_linux_2026_10_23/README.md).
The [source 823e8e9c publication follow-up](benchmarks/publication_qualification_2026_10_23/FINAL_823E.md)
retains the completed 18-cohort allocation, all nine read-latency regressions,
and observational epoch/interval strata. These diagnostics do not change the
manual execution lane, fixtures, exact checks or strict acceptance thresholds.

The Filtered DiskANN kernel/artifact cases also run in the existing
`vector-projection-platform` job in `.github/workflows/ci.yml`, through its
standalone and `storage-io` Cargo test steps on Linux x86_64/aarch64, macOS
aarch64 and Windows x86_64. The virtual-large-graph fixture allocates
only its bounded query workspace; it does not create billions of records.

Default vector recall probes execute the same resident DiskANN path as ordinary
queries. `resident_diskann` owns sampled exact/graph parity, verified mapped
qualification identity and refusal to qualify a changed overlay. The
`recall_validation` units bind the reported backend to the artifact algorithm;
RaBitQ recall cannot qualify a DiskANN graph. Existing compressed recall cases
use the explicit compressed probe/qualification entrypoints. The production
RaBitQ qualification family also selects its original codec and probes
explicitly, including ACL cases; the compression advisor still requires RaBitQ
evidence. The embedded facade checks default-graph sampled recall and its
separate preferred-compression control. The minimum
production dataset size and complete release binding remain required. Replay
with `cargo test --locked -p hawdb-search --all-features --lib recall` and the
resident family above. These cases stay in the existing Search feature profiles.
The standalone/storage-io artifact family also checks the complete CRC32C
fingerprint captured during admitted verification. The manual
`rabitq_query_profile` example explicitly selects its original RaBitQ codec;
its corpus, profile and query assertions are unchanged.

The manual `filtered_diskann_qualification` example is a thin public-library
fixture, outside automated test/CI execution. It builds a fresh clustered
synthetic graph with an explicit memory envelope and optional LSG, then runs
held-out synthetic queries with unfiltered, correlated and anti-correlated
10%/1% filters against exhaustive eligible-domain ground truth. It refuses
ineligible/duplicate hits, an unexpected/fallback backend, empty ground truth or
mean recall below 950,000 per million for any filter family. JSON lines retain
every sample's recall, per-family aggregate recall, build/open/query timing, graph
identity, build resource evidence, scored-vector and read-byte counters; those
admitted/counted bytes are not measured RSS or physical disk traffic. Build and
query processes are separate, so process-lifetime RSS can be measured externally.
Use the repository profile consistently across controls. Local replay:
`cargo run --locked --example filtered_diskann_qualification -- build /tmp/new-diskann-fixture 1000 16 64 plain`,
then `cargo run --locked --example filtered_diskann_qualification -- query /tmp/new-diskann-fixture 8`.
Use another fresh path and `lsg` for the LSG control. The fixture retains its
files, refuses existing build paths, and grants no canonical incremental/CRUD
or production-data qualification by itself.

The same manual example's `canonical` mode owns synthetic source ingestion,
default-parameter plain/LSG builds and mixed replacement/delete/NULL/insert
workloads at an explicit changed-key rate. Initial loading is sealed every 10,000
rows within the default live-row overlay bound, before retaining the source/index
baseline. It checks canonical exact recall, independent eligibility, retained old
MVCC results, stale-source refusal, graph
reuse, restore/reinsertion, no-op maintenance and cold reopen. Existing library
tests retain their CI owners. The opt-in Native Bindings
`canonical_vector_lifecycle` dispatch runs four fixed 100k-by-128 workloads
in four independent matrix jobs: plain/LSG at 1%/10% changed keys, sixteen query samples per family and
state, a 320 MiB canonical source/producer envelope, source-feed work bounded
from the row count and a 2 GiB host governor, with a 180-minute ceiling per job.
The source capture explicitly funds the complete atomic changed-key envelope:
4,096 entries for the 1% controls and 10,000 for the 10% controls, with the
unchanged 256 KiB capture byte limit. These typed workload settings leave
library defaults intact. The host wrapper retries typed retryable background-task
saturation or `BranchBusy` for at most 30 seconds, only while the completed
stationary source and selection remain unchanged, and records every retry.
Every control retains its complete lifecycle; failures do not cancel siblings.
The original 128 MiB envelope correctly refused
the conservative resident source charge plus point-query workspace before graph
construction; graph-only preflight is not canonical-build qualification.
It preserves partial JSON/timing evidence,
source/tree/toolchain/profile and binary hash separately per job, including
repeated compilation cost. Compilation and each full
lifecycle have separate process time/RSS observations. Default/scheduled CI,
existing test deadlines and repository compiler profiles are unchanged; no fuzz
tool is registered. Replay and resource-evidence limits are documented in
[the canonical lifecycle protocol](specs/FILTERED_DISKANN_SPEC.md#manual-canonical-lifecycle-qualification).

Canonical SQL point CRUD is additionally covered by Storage's
`relational_row_workspace::tests::primary_key_mutations` in the existing
`//crates/storage:hawdb_storage_tests` / Prow crates owner. Replay with
`cargo test --locked -p hawdb-storage --all-features --lib relational_row_workspace::tests -- --test-threads=1`.
A native 16-row fixture changes three keys under a four-entry budget, compares
captures with the materialized oracle, encodes/decodes WAL, replays the exact
working set and checks unchanged old views. Residual NULL/contradictory/missing
key predicates, rekey destination closure/collision, complete composite-key
ordering, scan-only refusal, and legacy full-scan WAL recovery with strict drift
rejection are separate controls. Existing differential smoke and manual campaign
ownership, profiles and CI lanes remain unchanged.
The same owner also checks retained-row Materialized CRUD under that four-entry
budget, compares residual UPDATE/DELETE captures with the legacy scan oracle,
and recovers both access shapes with transient rows and primary-key changes.
Surplus access entries must fail exact replay validation. Storage's existing
checkpoint-admission cases also exercise controlled point-key allocation.

Composite-key mutation coverage uses the same Storage owner, with no new lane
or profile. `relational_row_workspace::tests::composite_key_mutations` compares
prefix and interval UPDATE/DELETE with the legacy full-scan oracle, keeping a
fixed eight-entry budget while unrelated orders grow from 3 to 1,000. It checks
candidate decode/workspace counts, residuals and empty/open/closed intervals,
transient/rekeyed rows, exact legacy and new WAL replay, admission refusal, and
pinned old views. `relational::primary_key_range::tests` checks all supported
ordered scalar domains, including signed zero, NaN payloads, embedded NUL and
maximal keys, against predicate truth and encoded ordering. Root
`storage_recovery::composite_mutations` in the existing general unit lane checks
parameterized SQL, multi-statement rollback/commit, warehouse isolation, old
reads, WAL reopen and checkpoint reopen under both residency modes. Focused
replay: `cargo test --locked -p hawdb-storage --all-features --lib composite_mutation`,
`cargo test --locked -p hawdb-storage --all-features --lib primary_key_range::tests`,
and `cargo test --locked -p hawdb --lib composite_mutations`.

The existing root vector API family additionally owns explicit source-capture
capacity refusal/success, single-background-slot and held-catalog admission with
unchanged selection and retry, no-op behavior under saturation, and
automatic-checkpoint feed preservation. The admission and automatic-checkpoint
controls require `background-maintenance`;
the feed control waits for a real automatic checkpoint, then suspends the owner
while validating the captured feed and catch-up so another checkpoint cannot
reacquire the sole background slot between an idle observation and admission.
They remain in the existing root/Prow and native API lanes. Replay with
`cargo test --locked -p hawdb --all-features --lib api::vector_index:: -- --test-threads=1`.
The existing Consumer vector uncertainty test also requires stationary-receipt
refusal after selector publication, so a host cannot mistake an uncertain
outcome for a safe retry.
The same Consumer vector owner checks explicit reopen working-memory ceilings,
host-capacity refusal, admission refunds, unchanged selected generation and
successful retry through both default and explicitly funded opens. Replay with
`cargo test --locked -p hawdb --all-features --lib vector_index_reopen_honors_explicit_working_memory_and_host_capacity`.
The manual canonical qualification declares its existing 320 MiB construction
envelope for cold reopen through `open_search_projection_consumer_with_task`;
the public default, generation-size and resident compatibility limits stay fixed.

The out-of-core DiskANN family also checks candidate-window propagation on a
512-vector fixture: independent navigation coverage, default output window, explicit rank window, configured
ceiling, excess-window refusal and unchanged explicit exhaustive behavior.
The ceiling must not silently become the requested candidate count. Replay with
`cargo test --locked -p hawdb-search --all-features --lib default_graph_uses_the_requested_candidate_window`.
Resident `default_graph_navigation_is_funded_independently_of_its_output_window`
also requires navigation beyond the 128 output candidates, rejects the former
smaller workspace without changing the cached graph, and retries with sufficient
funding. Both remain in existing Search lanes with the repository profile.

The vector-projection DiskANN family also owns sorted-ID filtered entry points,
small-domain ID lookup without rejected-link scanning, two-hop rejection bounds
with poisoned rejected-vector reads, late bridge promotion in dense/sparse
visited state, unknown-ID work refusal, parent work accounting, and deterministic
unique entry selection at ordinal/address-space boundaries. Existing arbitrary
provider multi-hop cases remain covered. Replay with
`cargo test --locked -p hawdb-vector-projection --all-features diskann`.

The existing Search resident DiskANN owner additionally covers an actual
1,024 x 16,384 source above the 64 MiB cache cap: default vector/hybrid and ACL
queries retain filtered scalar results and a typed fallback reason, and full
checkpoint/reopen preserves deletes and NULL transitions. A second fixture
holds a dirty cache with more than 32 MiB of real ID storage while checkpoint
uses the streaming compatible codec. Small-source graph use and explicit
query-budget/cancellation refusals remain covered by the same family. Replay:
`cargo test --locked -p hawdb-search --all-features --lib resident_diskann::tests`.
The storage `relational_row_workspace::tests::primary_key_mutations` family
also compares a non-PK UNIQUE update against the full reference, rejects both
existing-row and same-batch collisions, and replays the bounded access closure.
The power-loss partition script's six native controls include a discovered
case whose name contains the exact GC case; the remaining partition uses
complete exact names, preserving its membership without substring exclusions.
The existing Windows native search shard also executes the resident family,
retaining a separate fresh BEP/log/XML for it. No test lane, compilation profile
or timeout changes.

Resident read generations additionally retain default graph state and isolated
CRUD retractions alongside shared immutable document pages. The existing Search
owner covers old/current exact-result parity after insert, replacement, NULL and
delete, with the graph created before or after capture. Generation capacity
includes default graph construction/cache bounds and any mapped graph payload;
explicit compressed lazy-projection admission remains covered independently.
Replay with `cargo test --locked -p hawdb-search --all-features --lib resident_snapshot::`.

Canonical vector construction additionally verifies publication admission for
default, smaller and larger explicit task memory, over-budget refusal, and
propagation into the private graph builder. A pure 100,000-by-128 preflight and
governed reservation crosses the former 64 MiB ceiling without allocating graph
vectors. This is budget propagation evidence, not a large-graph build benchmark.
The existing root/Search owners retain these tests; replay with
`cargo test --locked -p hawdb --all-features --lib vector_index_publication_funds`
and `cargo test --locked -p hawdb-search --all-features --lib canonical_vector_builder_uses`.

The root vector-index family also owns
`api::vector_index::build::tests::diskann` (Cargo replay:
`cargo test --locked -p hawdb --lib vector_index`). It covers the schema-bound
DiskANN default, optional-LSG SQL/node publication and reopen, both filtered
language paths, large finite L2/zero-vector inputs, and old MVCC pins across a
full-rebuild successor after update/delete/NULL/insert. Explicit RaBitQ controls
remain in `search_projection_consumer::tests::vector_index`. These cases use
the existing root unit/presubmit lane. A vector Consumer metadata-only case
checks bounded snapshot size, no encoded resident rows, logical counts,
descriptor/freshness access, refusal of resident-index access, native queries
and reopen. Search's `vector_index` family rejects duplicate/mixed/unbound
summaries and impossible counts. Ordinary Consumer coverage retains the full
resident-index contract.

`api::vector_index::build::tests::incremental` belongs to the same root unit lane
and `vector_index` replay. It covers SQL/custom-node C..E updates with optional
LSG, repeated keys, two-vector construction for four changed keys over a
32-vector base, old graph byte/inode reuse, exact/filtered-ANN parity across
layers, old MVCC domains, no-op generation stability, empty/delete-only/reinsert,
checkpoint/reopen, rollback and cancellation, feed/memory/key pre-encoding refusal
and retry, explicit RaBitQ and Euclidean-zero behavior, schema/backend drift, host
origin spoof refusal and terminal builder invalidation. Incremental construction
must complete with its single admitted I/O wave: nested writer phases borrow
the synchronous phase instead of acquiring another governor slot. Search's
`generation_writer::tests::io_waves` and Consumer producer family also verify
wave waiting, cancellation, idle release and cleanup. The sibling
`api::search_projection_consumer::tests::vector_index::incremental_recovery`
family replays fixed-field SQL/custom-node publication faults before reservation,
after preparation, before/after selector publication and its barrier, before
acknowledgement and before/after registry publication. Live resolution and cold
reopen must preserve registration/operation identity after later source writes,
return the selected complete C or E generation, refuse binding it to newer G,
then catch up to G without reviving deleted/NULL vectors. Live old canonical
domains and original graph bytes remain unchanged. Replay with
`cargo test --locked -p hawdb --all-features --lib vector_index_incremental_publication_faults`;
the existing root unit and Windows consumers owners retain this scope. Its
`vector_index_incremental_prepared_fd_pressure` case covers both full rebuild
and incremental maintenance for SQL/node fields before/after intent under
occupied strict descriptor capacity. Original custody survives registry refresh,
execution slots are released, and later source advancement uses a new operation.
The root vector-index build family also owns
`vector_index_canonical_completion_scopes_full_source_to_declared_field`:
SQL/node baseline builds and explicit rebuilds must complete under finite work
limits with unrelated SQL/graph data and the operational schema-migration
table present.
Initial construction and explicit rebuild share the descriptor-scoped source
cursor. Its name keeps it in the existing Windows consumers selection. No
incremental feed, feature lane or compiler profile changes.
The acknowledged-image
owner is `api::tests::power_loss::consumer_vector_incremental` with `test-support`;
it checks SQL and custom-node sources under both durability policies, original
source/index pairing, old pins and
materialized power-loss reopen at strict 32-descriptor capacity. Its `faults`
submodule captures the first installed native link, before/after the sealed
candidate directory barrier that retires producer scratch, actual private selector writes, selector renames, and the final project
root barrier. SQL/custom-node sources and both durability policies retain one
atomic source transaction, original old pins, complete C/E data and exact native
IDs after replay. Every lost/reordered/isolated/torn plan is evaluated; only
identical full namespaces, inode aliases and bytes share a native reopen. This
bounds repeated startup cost without dropping fault plans or assertions. A lost
relaxed transaction may leave only C while an unselected E preparation exists;
recovery settles that logical operation, preserves all candidate bytes and permits
fresh catch-up. Selected E still requires its durable canonical source and full
artifact verification. Source-bound logs report plan and distinct-reopen counts;
these are known slow local replay families, not fuzz timeout durations. Replay
with `cargo test --locked -p hawdb --all-features --lib vector_index_incremental_power_loss_`.
Its `faults::recovery` family continues the actual recorder from a verified
physical image, then interrupts recovery publication before intent, with an
unselected intent and with a selected intent. Seven cuts cover the private
recovery write, both file-sync boundaries, both rename boundaries and both root
barriers, for SQL/custom nodes and both durability modes (84 combinations), plus
28 relaxed-mode combinations where E is actually lost before intent or during
private selector preparation. Recovery must preserve the surviving C source.
Every lost, isolated, reordered and applicable torn plan is evaluated; identical
physical states share reopening. Acknowledged recovery and two repeated reopens
retain the original selection receipt, source/index epochs, candidate bytes and
inode aliases. Stale C still refuses a current-E canonical binding. Native
Windows artifact assertions check exact lengths and read only the expected
bytes, including empty locked lease files, without an EOF probe.
Windows weekly power-loss and required native-consumer matrices isolate these
recovery cases in seven required shards: births, unselected intents, selected
intents, and lost relaxed SQL/node sources split by Link versus SelectorWrite
origin. The first three retain 28 combinations each; the final four retain seven
each, for the same 112 combinations. Linux/macOS retain the complete family in
one process. Three additional Windows shards own the existing first-crash
artifact-closure, torn-selector and selector/root-barrier tests, preserving all
32 cut/source/durability combinations. The weekly consumer-publication and
ordinary native consumer selections exclude the complete vector fault module
because these ten exact selections own it. The Windows `remaining` partition
also excludes these cases through the same discovery inventory. A shared selector
validates actual Rust discovery and both workflow matrices; a runtime coverage check requires
every `(origin, cut, durability, source family, source-updated)` combination
exactly once. All fault plans, physical-image deduplication, two repeated reopens,
single-process test concurrency, compiler profiles and 120-minute job ceilings
remain unchanged. Native runner negative controls reject missing coverage even
when the Rust test itself reports success. The former combined Windows lane
reached its 120-minute ceiling after only 34 recovery combinations; isolated
first-build and execution costs require final-head CI evidence. Replay the
selector through `bazel test //:power_loss_partition_script_test` and the runner
controls with `bash scripts/run-windows-native-bazel.test.sh`. Issue #1343 tracks
the remaining runtime headroom: pre-split BEP recorded 1684.2 seconds of native
vector selection execution, 1591.2 seconds for lost SQL source recovery and
1076.5 seconds for lost node source recovery. New shard concurrency can repeat
cold compilation; final-head Actions evidence must separate that cost from test
execution before claiming a speedup.
The weekly Windows matrix also retains all eight branch-abandonment cases in
their own serial shard; they consumed about 41 minutes before recovery began
in the former combined lane. This preserves their assertions and the existing
separate native GC campaign. The remaining partition is checked against actual
discovery, with automatic checkpoints, recovery and abandonment excluded only
because required sibling shards own them.
The subsequent Windows run at `60f8b596` also reached the unchanged 120-minute
ceiling in the combined remaining selection and the automatic manifest case.
Its consumer publication cases consumed about 31 minutes and its single
deletion/GC case about 48 minutes before later cases could finish. Windows now
owns consumer publication, Search projection publication and that GC case in
separate required shards; the core remaining selection retains all other cases.
The manifest test is split by `SyncOnEveryWrite` and `SyncOnCheckpoint`, retaining
both observation boundaries and all three successive generations per policy.
This changes four automatic Rust tests to five, and the complete root power-loss
discovery from 48 to 49 cases, while preserving all 48 automatic
`(cut, policy, boundary, generation)` configurations and the same physical plans.
A runtime checker requires each automatic configuration exactly once and a
positive physical-image completion summary for every selected test. Missing or
duplicate records fail even after a successful Rust exit. Linux/macOS still run
the entire family; Windows partitions must remain disjoint, exhaustive and
nonempty against compiled discovery. Compiler profiles, assertions, fixtures,
single-process concurrency and the 120-minute job ceilings are unchanged.
Complete discovery, first-build logs, execution logs and coverage receipts remain
artifacts; a partition change alone is not a final-head speedup claim.
The storage `power_loss::tests::recovery_` family owns recorder continuation,
bounded baseline import, hard-link identity, second torn writes, native drift,
active-handle refusal and failed-attachment cleanup; the Storage unit/native
owners run it. `resume_from_image` accepts only an exact native materialization
before runtime open, and treats the surviving image as the new durable baseline.
The root general `search_projection_graph_delta` family preserves the legacy
batch API's live-node candidate contract before applying its operation ceiling.
Materialized/out-of-core checkpoint reopens retain raw deleted native keys for
fixed projectors while a one-operation legacy delete remains admissible. The
point visibility check tests immutable record membership without owning property
values; existing segment I/O still applies. Whole-commit refusal and legacy
tombstones remain unchanged.
The storage `observations_capture` family owns captured-root directory barriers
and descendant writes, including rejection of escaped/non-normal paths. The
existing root and Windows consumers owners retain all feature scopes, profiles
and timeout ceilings. Recovery-publication cuts and K-proportional total I/O remain
separate qualification items.

## Search feature and lifecycle matrix

The pinned dictionary loader's `cjk_dictionary::tests` family verifies the
complete upstream byte digest and 48 upstream token/offset fixtures with HMM
enabled and disabled. It remains in every existing Search unit profile; the
standalone `hawdb_search_analyzer_workspace_allocation_tests` target imports the
same loader and retains its complete scratch/TLS qualification. Bazel declares
the compressed resource as compile data, including direct-source unit profiles.
Replay with `cargo test --locked -p hawdb-search --lib cjk_dictionary` or
`bazel test //crates/search:hawdb_search_tests --test_arg=cjk_dictionary`.
The [unregistered component probe](benchmarks/jieba_dictionary_loading_2026_10_27/README.md)
compares the old and new loaders outside the production dependency graph. It
must not be added to CI, a workspace, or a published library archive.

See [SEARCH_FEATURE_TESTS.md](SEARCH_FEATURE_TESTS.md) for feature-specific
expectations. The concrete profile set is default, acl, minimal, text_only,
vector_only, background_only, text_vector, text_background and vector_background.

| Target pattern | What it detects | Cost and known issues |
| --- | --- | --- |
| `//crates/search:hawdb_search_tests` and `hawdb_search_<profile>_tests` | Search unit, persistence/recovery and feature capability behavior | Large lifecycle fixtures include 128-MiB sources. The three admitted-root heap-reclamation probes run in exact-case child processes because staging-registry nodes are shared across operations; all peak and final-zero assertions remain. The `io_waves` family runs through the isolated owners below, using these same executables. Repository test profile; the full matrix still runs. Existing deadlines remain unchanged. |
| Binary vector sidecars (`out_of_core::vector_segment::tests`, `out_of_core::vector_query::tests`, `out_of_core::tests::vector_segments`) | Layout V2 round trip of exact `f32` bits for every row subset, multi-byte IDs and empty segments; head/row CRC detection of every corrupt head byte and of corrupt selected rows (unselected corrupt rows are never read); count/dimension/length/bounds/order mismatches; refusal of superseded V1 text payloads and layouts with a rebuild message; encoder rejection of non-finite, ragged and unordered rows; query-account admission refunds, bounded run reads and cancellation; exact, filtered, DiskANN, RaBitQ and hybrid serving equal to brute-force cosine bits; Required rerank checksum/budget/cancellation guards; ID-only head mapping | Existing Search unit feature-matrix owners and repository checked profile. Graph/compressed/hybrid serving cases require `vector-search` (hybrid also `full-text-search`); format cases run in every profile. No CI scheduling or timeout changes. Replay: `cargo test --locked -p hawdb-search --lib vector_segment` and `cargo test --locked -p hawdb-search --lib vector_query`. Ref [#1469](https://github.com/nowledge-co/hawdb/issues/1469). The text-decimal selected-decode family (`out_of_core::vector_selection_tests::`) and the selected-vector arm of `out_of_core::vector_query::layout_probe::` were retired with the V1 text sidecar; their recorded results remain in [VECTOR_SELECTED_DECODE_BENCHMARK.md](VECTOR_SELECTED_DECODE_BENCHMARK.md) and [the layout investigation](benchmarks/vector_adjacency_layout_2026_10_29/README.md). |
| Out-of-core hit hydration levels (`out_of_core::hydration::tests::hit_hydration_levels`) | Default document hydration versus sidecar hydration for vector-only requests (identical hits, no document payload reads) and `SearchHitHydration::ScoresOnly` (identical IDs and ranking scores, no hydration reads) across exact, filtered and graph vector modes, plus text/hybrid requests that keep full hydration | Existing Search unit feature-matrix owners and repository checked profile; graph cases require `vector-search`, text/hybrid cases `full-text-search`. Replay: `cargo test --locked -p hawdb-search --lib hit_hydration_levels`. |
| Adjacency layout investigation (`out_of_core::vector_query::layout_probe::adjacency_layout_measurement`) | Manual persistent-component A/B measurement of IDs-only/property adjacency scans with disabled/bounded engine caches | One ignored probe requires the repository-wide release profile and stays out of CI/fuzz scheduling: `cargo test --locked --release -p hawdb-search --lib layout_probe:: -- --ignored --nocapture --test-threads=1`. [Methods, results and limits](benchmarks/vector_adjacency_layout_2026_10_29/README.md). Payload-visit counters are not physical I/O; an engine cache disabled run is not an OS-cold run. The selected-vector arm was retired with the V1 text sidecar (#1469). Ref [#1326](https://github.com/nowledge-co/hawdb/issues/1326). |
| `hawdb_search_io_wave_tests` and `hawdb_search_<profile>_io_wave_tests` | Complete standalone writer Task/Wave admission, idle release, cancellation and cleanup contracts | All nine unit feature profiles reuse their existing executable. Required crate presubmits run this family with one libtest thread and Bazel `exclusive`; no additional Rust executable or compilation profile. The runner rejects empty discovery and overlapping skips and preserves caller arguments and failures. The existing 20-second task deadlines and enclosing target timeouts are unchanged. Replay: `bazel test //crates/search:hawdb_search_text_background_io_wave_tests`; the runner itself is covered by `//crates/search:search_unit_family_script_test`. |
| Search BM25 decode and filter reuse (`lexical_projection::`, `text_seed`) | Reusable posting buffers, corrupt-frame rollback, stable internal predicates, arbitrary callback semantics, exact metadata routing and memory ownership | Existing Search unit feature-matrix owners and repository checked profile; public text-seed cases require `full-text-search`. Allocation probes serialize through the existing measurement gate and cover 8/128/4096 postings; filter cases cover false results, cancellation, complete/window counts and sparse overlapping layers. Local replay: `bazel test //crates/search:hawdb_search_tests --test_arg=lexical_projection:: --test_arg=text_seed --test_arg=--nocapture`. Existing ignored differential campaigns remain local-only. Ref [#1206](https://github.com/nowledge-co/hawdb/issues/1206). |
| Search BM25 cursor selection (`lexical_projection::tests::cursor_selection`) | Exact score bits, tie-breaking, exhaustive match counts and callback order across empty/unknown and 1/4/16/32-term queries; pruning eligibility before the rank window fills | Existing Search unit feature-matrix owners and repository checked profile; existing pruning regressions cover active WAND and overlays. Replay: `bazel test //crates/search:hawdb_search_tests --test_arg=lexical_projection::`. The ignored 4096-document warmed timing probe is local-only: `cargo test --locked -p hawdb-search --lib cursor_selection_query_measurement -- --ignored --nocapture --test-threads=1`; two warmups and 21 repetitions per query, including statistics reads, excluding fixture construction. Ref [#1208](https://github.com/nowledge-co/hawdb/issues/1208). |
| Resident BM25 query-term statistics (`resident_bm25::tests`) | Legacy full-vocabulary oracle versus query-only DF/TF: exact score bits, weighted fields, phrase/identifier aliases, CJK, empty/unknown terms, filtered corpus, tied rankings, offsets, complete hit counts and hybrid rank windows; vocabulary-heavy allocation regression | Existing nine Search unit feature-matrix owners and repository checked profile. Scalar oracle/allocation cases run in every profile, resident text integration requires `full-text-search`, and hybrid integration also requires `vector-search`. Replay with `--test_arg=resident_bm25:: --test_arg=resident_text_seed::`. The ignored `resident_bm25_measurement` probe compares legacy/candidate statistics and scalar scoring inside one test binary, excluding fixture setup, filtering, hydration and result shaping; two warmups and 21 alternating timed pairs per workload. It is a manual allocation/timing diagnostic, not an RSS or end-to-end latency gate: `cargo test --locked -p hawdb-search --lib resident_bm25_measurement -- --ignored --nocapture --test-threads=1`. Ref [#1289](https://github.com/nowledge-co/hawdb/issues/1289). |
| Search BM25 verified dictionary reuse (`lexical_projection::tests::query_context::dictionaries`) | One FST validation per loaded shared block, bitwise BM25 and bounded-window parity for 1/4/32 terms, zero-allocation cache hits, Vec reuse at eviction, cancellation and failed-dictionary retry | Existing Search unit feature-matrix owners and repository checked profile. The ignored `dictionary_reuse_query_measurement` is a local-only work-count, allocation and warmed timing probe: `cargo test --locked -p hawdb-search --lib dictionary_reuse_query_measurement -- --ignored --nocapture --test-threads=1`. It uses 128/4096 single-token documents, 32 warmups and 21 timed batches; includes statistics reads and excludes fixture construction. Current writer blocks contain at most 128 total postings, so frame-only decoding cannot yet reduce their decoded memory bound. Ref [#1278](https://github.com/nowledge-co/hawdb/issues/1278). |
| Search BM25 global windows and shared physical buffers | Global bounded segment merge versus exhaustive scores, exact text counts/offsets/ties, duplicate-live-layer refusal, inherited WAND floors before the local heap fills, one physical read for co-located terms, cancelled cache hits and failed-read retry, borrowed wide ranking IDs and admission refunds | Existing Search feature-matrix owners; window integration requires `full-text-search`, buffer/ranking/pruning guards run in minimal profiles too. Replay with `--test_arg=lexical_projection:: --test_arg=out_of_core::tests::bm25_windows --test_arg=out_of_core::query_ranking::`. The ignored `bm25_window_allocation_measurement` and `rank_id_allocation_measurement` probes are manual allocation measurements under the repository test profile, not latency/RSS benchmarks or CI gates: `cargo test --locked -p hawdb-search --all-features --lib -- bm25_window_allocation_measurement rank_id_allocation_measurement --ignored --nocapture --test-threads=1`. Fixtures are built outside the allocator observation; probes use the existing serial measurement gate. |
| `hawdb_search_source_policy_tests` and `hawdb_search_<profile>_source_policy_tests` | Inherited source/token admission policy through build, replacement, compaction, deletion and reopen; valid over-default-budget reader bodies | Million-token boundaries intentionally remain large. Five FTS profiles previously exceeded 900s: [#1034](https://github.com/nowledge-co/hawdb/issues/1034). Non-FTS decomposition: [#872](https://github.com/nowledge-co/hawdb/issues/872). |
| `hawdb_search_streamed_lifecycle_tests` and FTS profile variants | 32-MiB streamed bodies through projection update, replay, compaction/retraction and budget refusal | Five complete FTS feature profiles. Reuse the same feature-profile library; compilation is uniform across the repo. |
| Allocation-specific integration targets | Identifier, descriptor, segment, analyzer and related allocation accounting | Uses the same repository profile; preserve measurement assumptions and compare allocation evidence after profile changes. |

Weekly platform steps use Bash's fail-fast execution on every OS, including
Windows, so an earlier failing native command cannot be hidden by a later
successful test command. The explicit PowerShell artifact collector remains
an override. A green legacy Windows job with failed tests in its log is not
passing qualification evidence.

Resident generation cases in the existing Search unit/feature matrix cover
ordered COW ranges, writer progress with an old read view, body ownership after
deletion, retained key/directory capacity, and old/new payload reads across
checkpoint replacement. The text-enabled case requires physical range reads,
checks old/new freshness, and checks final descriptor refund. The strict FD
fixture now verifies that an already pinned read succeeds at capacity while a
new generation open refuses with the typed cause and retries after release.
Storage's existing range-reader family independently replaces and removes the
pathname before the first captured read, retains digest rejection and checks
final clone release. These use the existing native Search/Storage CI lanes;
no local duplicate runtime execution or adapter lock/performance acceptance is
implied. Local replay filters are `resident_documents::`, `resident_snapshot::`
and `scan::reader::tests::captured_file_` in their owning Cargo packages.

The power-loss publication fixtures select actual temporary manifest writes by
filename prefix, preserving after-write, before/after-rename and torn/reordered
fault schedules for both ordinary and partitioned mutations. Validation scratch
written by later cleanup must not replace the observed publication boundary.
The partitioned initial-publication fixture explicitly builds legacy RaBitQ
content because its required-compression queries must exercise that codec after
every recovered publication. Its five cuts and empty/replacement states remain
unchanged; ordinary mutation/compaction fixtures retain the default graph.
The storage model unit family checks that this filter preserves path scope,
skip/last selection and pre-sync torn bytes despite later writes and barriers.
The branch-GC fixture checks balanced descriptor ownership after runtime closure
and uses reservation pressure to evict the model-owned idle file cache before
asserting return to the original native-root charge. Internal recovery/cleanup
may exceed the strict foreground target. Search-ancestry refusal/retry reserves
the remaining capacity after that root charge: this standalone writer synchronizes
ancestors serially and refuses with zero free slots. Unix retries with one;
Windows additionally refuses with one, refunds every charge and retries with
two because the model audits the retained root's full native identity through
a second counted handle. The completed writer retains only its spool beside
the original root anchor; the total descriptor limit and crash/reopen checks
are unchanged. Governed Consumer staging uses its separate captured-directory inventory.
These remain existing storage/root and native branch-power-loss CI families.

### Standalone Search writer I/O waves

The isolated `hawdb_search_io_wave_tests` and profile variants own
`out_of_core::generation_writer::tests::io_waves`: post-create refusal cleanup with no retained stage debt under one-slot Task/Wave reservations, original-governor contention
before native creation, cancellation (including the exact post-mkdir,
pre-return boundary in `spool::stage::tests`), zero-slot refusal, idle token release,
Task/Wave depth-one partition builds, same-governor streamed source/output
handoff, and nonflushing Drop/deferred cleanup with
fresh admitted retry. The mutation phase cases also cover occupied-slot
refusal before term-file creation, target hydration and unordered run creation;
cancellation retains the selected generation and refunds memory after cleanup.
They verify nonflushing unordered run Drop, fresh cleanup retry, and complete
owned/streamed/unordered updates under both one-slot Task and Wave reservations.
Existing prepared/FD cleanup fixtures explicitly declare
I/O capacity while preserving memory/FD limits and assertions. Replay with
`cargo test --locked -p hawdb-search --lib generation_writer` and
`--no-default-features`; the vector Consumer positive control also runs in the
normal default-feature Search target; its one-slot Task/Wave success controls
assert zero pending stages, retained cleanup metadata and disk debt before
fixture removal. The ignored `manual_bulk_input_admission_profile` measures
256 owned/streamed documents (about 8 MiB) with raw/governed controls, counts
original-governor wave acquisitions, includes final spool flush and excludes
artifact build/publication. Replay with `cargo test --locked -p hawdb-search
--lib manual_bulk_input_admission_profile -- --ignored --nocapture
--test-threads=1`; it has no throughput threshold and is not an extra CI job.
All nine feature selections, fixture scopes and timeouts remain unchanged;
only Bazel execution ownership is isolated. Native Cargo discovery is unchanged.
See [the admission contract](SEARCH_WRITER_IO_ADMISSION.md).

## CI benchmark smoke owners

Native numeric result delivery is covered by the executor `numeric::` family
and the facade's query-execution family. The deterministic native oracle covers
row, lending, owned-typed and columnar-morsel inputs, scalar/NULL values, output
ceilings, stop and error. Ownership cases verify source/literal pointer identity,
duplicate aliases, schema refusal, cancellation and panic cleanup. The existing
parallel resource-refusal case exercises both binding and native sinks. The
facade case checks compact owned parity, immediate native versus validated
compact callbacks, fail-closed row/payload limits and SKIP/LIMIT. The native
Runtime Platform lane selects these cases; the ignored long campaign remains
local-only. Report-value tests include the native projection counter.

The local-only `mixed_point_growth` probe additionally separates plain EXPLAIN,
EXPLAIN ANALYZE, no diagnostic, and their original combined sequence. Full and
minimal parameter maps isolate unused binding variants, with database-cache
counters for both timed phases. See `bindings/benchmarks/README.md`; these
manual controls do not enter CI or qualify latency on a contended host.

Plan-cache component regressions also cover omission of irrelevant bindings,
exact pagination variants and missing required parameters. The existing root
`plan_cache` family verifies rotating point values and unused bindings across
bound EXPLAIN/ANALYZE, including cache hit/miss counts and returned values.
Both remain in their ordinary existing crate/root CI owners; they are not
additional benchmark or fuzz jobs.

`//:hawdb_linux_ci_smoke_tests` includes correctness/protocol checks and
benchmark evidence. The following benchmark groups remain `exclusive`, so
Bazel executes them sequentially after ordinary tests in the invocation.
Do not remove isolation merely to reduce wall time: relative latency and RSS
measurements can become unreliable under competing fixtures.

| Target / group | What it checks | Current execution |
| --- | --- | --- |
| `optimizer_summary`, `optimizer_group_1`, `optimizer_group_3` smoke targets | Optimizer/report evidence plus join, aggregate, lookup, vectorization and storage/search benchmark protocols | Existing complete workloads through the repository test profile. |
| `hawdb_linux_ci_benchmark_<name>_smoke_test` | index_restart_cost, integrity_checksum, relational_index_access, relational_index_page_cache, relational_monotonic_append and relational_oltp_mix | Separate targets keep the original finite deadlines and exclusive isolation. |
| `hawdb_linux_ci_wal_group_commit_smoke_test` | Concurrent/single-writer commit behavior, group-sync counts, paired latency policy evidence and complete reopen/recovery | Four paired workloads, 11 rounds each. Fixed work, not a requested run duration. |
| Fixture, storage recovery, background maintenance, previous-wrapper and migration-gate smokes | Protocol shape, recovery, compatibility evidence and fail-closed readiness | Existing semantic gates are preserved. |

There are no CI-specific optimized benchmark wrappers or target-local
optimization flags. Benchmarks used by test smokes follow the same checked test
profile as other targets. To explicitly verify release behavior, use
`bazel test --config=release ...` or `bazel run --config=release ...`. The legacy
`hawdb_bench_relational_index_access_release` label is a compatibility alias;
it no longer changes compilation mode by itself.

## Repository-wide compilation profiles

Cargo loads `.cargo/config.toml` from the repository hierarchy, covering the root
workspace, independent tools and bindings. Run Cargo from
within the repository (including when using `--manifest-path`); callers outside
this directory hierarchy must explicitly supply the config. Published library
consumers still own their host application's profiles. External-consumer
measurements explicitly copy this config into their temporary host and record
its digest, so moving the host outside the repo does not silently lose the
selected release policy.

| Setting | Cargo dev | Cargo test / default Bazel | Cargo release/bench; explicit Bazel release verification |
| --- | --- | --- | --- |
| Optimization | 1 | 1 | 3 |
| Debug information | 0 | 0 | 0 |
| Codegen units | 256 | 16 | 1 |
| Cross-crate LTO | Disabled | Disabled | Thin |
| Debug assertions | Enabled | Enabled | Disabled |
| Integer overflow checks | Enabled | Enabled | Disabled |
| Symbol stripping | None | None | None |

Cargo incremental compilation remains enabled for dev/test and disabled for
release/bench. Bazel uses its action cache rather than Cargo incremental state.
Host build tools and platform linker/ABI requirements follow their toolchains;
they are not special optimization profiles for individual regression targets.
Bazel is primarily a test runner; release is an explicit regression-verification
profile, not a production artifact publishing path.

GitHub Actions jobs using `setup-bazel` enable Bazelisk downloads, repository
archives, disk action outputs and extracted external-repository caches. Disk
cache keys are scoped by workflow/job; the action also separates OS/architecture
and keeps disk/repository cache directories distinct. These caches are restored
and saved through GitHub Actions cache storage, without a Bazel remote cache.
Cached results remain cache reuse rather than new execution evidence; compare
restore/save overhead and cold/warm build costs before claiming a CI speedup.

The ignored executor regression
`observer::query::tests::query_observer_registration_without_debug_assertions`
is explicitly selected by `//crates/executor:hawdb_query_observer_release_tests`
under `--config=release` in the required Linux native-bindings job. Its target
is manual so the ordinary checked crate suite cannot run it in the wrong mode.
Its assertion remains intact; moving its execution does not waive coverage.
Tide gates on the `Bazel native bindings (ubuntu-24.04)` context: the current
[context policy](https://github.com/nowledge-co/test-infra/blob/5a7b3995d384ed4b3f3ba576fe6d06cd700f27e6/config/prow/cluster/config.yaml#L108)
marks only Windows native contracts optional for HawDB and does not skip unknown
contexts. Preserve this merge-gate ownership when changing workflow or Tide policy.

The policy follows the distinction illustrated by [TiKV's Cargo profiles](https://github.com/tikv/tikv/blob/master/Cargo.toml):
cheap development/test code generation with assertions, versus explicit optimized
release code generation. TiKV's dev/test optimization is 0; HawDB chooses 1 to
reduce measured unoptimized-loop overhead without the compile cost of level 2.
No per-package optimization exceptions are copied from TiKV.

## Recent slow-run evidence and interpretation

Complete PR #1145 BEP on 2026-10-21 recorded:

| Lane | Bazel elapsed | Notable executed targets |
| --- | --- | --- |
| [Root](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1145/pull-skein-bazel-test-root/2112766656664047616) | 1565.6s | Compaction 445.2/444.2s; WAL benchmark 376.1s; optimizer group 1 229.1s. Exclusive benchmark execution adds about 864s sequentially. |
| [Crates](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1145/pull-skein-bazel-test-crates/2112766656974426112) | About 1218s | FTS source-policy 637-655s; streamed lifecycle 433-455s; heavy unit profiles 420-590s. |
| [Dense analytics](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1145/pull-skein-bazel-test-analytics-dense/2112766656806653952) | 798.0s | Isolated RSS target 551.6s; TestRunner consumed about 551 CPU seconds, indicating actual computation rather than a timed sleep. |

The initial opt-level=2 experiment on macOS arm64 changed complete materialized
compaction from 71.947s to 16.756s and default source-policy from 89.424s to
11.275s. Its first optimized invocation cost about 171s versus 111s for the
baseline due to new compile actions. That target-specific implementation was
removed after adopting repository-wide profiles; those numbers are exploratory
evidence, not results of the final level-1 profile. Obtain final-head BEP to
judge execution savings together with cold-build cost.

The final uniform level-1 profile completed the same materialized compaction
case in 21.991s and default source-policy fixture in 15.920s. Both targets
executed, with local compilation concurrency limited to four after observing
host memory pressure. The first level-1 invocation cost about 126s including
new compilation versus about 111s for the original baseline; this remains
local evidence, not a remote CI speedup claim.

Bazel `long`/`eternal` timeouts are maximum allowed execution times. They do not
make tests run for that duration. Cache-hit BEP can retain the original result's
recorded test duration: check cached-result counts before interpreting it as
new execution. Compare exact source, runner, features, fixture size, execution
versus cache hits, cold build cost and required target inventory.

## Local fuzz and reproductions

- Shiro: `make shiro-fuzz` runs one worker continuously until interrupted or an
  oracle fails; `SHIRO_SECONDS=30` adds a local time budget. Explicit `--cases`
  and `--case-index` support finite verification/replay. See
  [tools/shiro](../tools/shiro/README.md).
- SQL maintenance and ranges: every Shiro case compares backfilled projection,
  grouped/global COUNT/SUM/BIGINT-and-TEXT extrema and indexed view reads with an
  unindexed reference after random INSERT/UPSERT/UPDATE/DELETE, commit, rollback,
  overflow rejection and clear/repopulation. It verifies pinned old snapshots
  and ordered composite ranges with inclusive/exclusive/equal/reversed/NULL
  bounds and residual filters. Local `//tools/shiro:shiro_fast_tests` owns the
  generator, positive campaign and corrupted-view negative controls; no CI
  ownership is added. `sql_maintenance_coverage` separates actual initial range
  plans from fallback plans. Replay with `--seed 7 --cases 32 --case-index 0`;
  the v4 campaign protocol rejects old resume reports. Cases rotate memory,
  file-backed Shadow and file-backed DemandPaged indexes. Disk cases compare
  full pre-close results/epochs after initial WAL replay, checkpoint reopen and
  checkpoint-plus-WAL recovery, then continue mutations; retained snapshots
  also cross checkpoint publication. Recovery reports prove the selected paths.
  The common-data-loss negative control prevents a jointly lost source and
  reference from passing. Original failing databases are retained; reduction
  scratch is cleaned. Replay indexes 1/2 for the disk modes. Source rows remain
  resident as required by materialized views; this is file recovery evidence,
  not out-of-core view support or crash/power-loss qualification.
- MVCC: `make mvcc-fuzz` defaults to 30 seconds of local transaction histories.
  `MVCC_FUZZ_ARGS="--mode pessimistic"` instead runs two pessimistic
  transactions on worker threads and also checks lock waits, deadlock victims,
  snapshot refresh and serialization aborts for SQL and Cypher writes. Its
  twelve families include incremental disjoint SQL locks; an independent
  logical-key version map distinguishes overlapping changes from unrelated
  epochs while checking original snapshot reads and committed results.
  Generate editable cases, replay failures and shrink them before introducing a
  focused deterministic regression. See [tools/mvcc-fuzz](../tools/mvcc-fuzz/README.md).
- Local deterministic fuzz verification:
  `bazel test //tools/shiro:shiro_tests //tools/shiro:shiro_cli_tests //:hawdb_linux_ci_fuzz_smoke_test`.
  Historical `//crates/fuzz:*` labels are compatibility wrappers only.
- A fuzz discovery belongs in CI as a small confirmed regression, not by adding
  the continuous tool or arbitrary duration-based exploration to CI.

Python capsule destructor regressions (`cargo test --manifest-path bindings/python/Cargo.toml --lib destructor_tests`)
run locally and in the Python Bindings Cargo release-profile lane after wheel
construction. They cover a pending `PanicException` and a panicking release callback. No Rust unwind
may cross the C destructor; descriptors are still released and ordinary pending
Python exceptions restored.

Python `open` / `Database` / DB-API `connect` path checks (`bindings/python/tests/test_file.py` and
`test_memory.py`) run in `//bindings/python:hawdb_python_tests` and the Python
Bindings `pytest` step on Linux and Windows. The Windows wheel lane runs the
installed default/Pydantic suites and strict Clippy for the independent binding
workspace before release; optional Arrow/pandas consumer lanes remain on Linux.
Path cases cover missing parents, read-only refusal, regular files and
non-directory parents through absolute and relative paths, `":memory:"` and empty paths (including an
existing project as the current directory), with rejected opens leaving files
and directories intact. POSIX permission cases preserve the underlying error
and skip only when the user can bypass directory permissions. Native handles
retain absolute paths across `chdir`, preserve symlink spelling on POSIX, and
reopen persisted data. The parent check remains non-atomic with engine open.
Windows parent validation checks metadata because a regular-file ancestor can
surface as NotFound for the child. Preserve the requested absolute path in that
refusal and leave the existing file untouched. The release profile and feature
set match the existing Linux wheel lane.

The same Python path family covers explicit `namespace_durability_anchor`
forwarding through `open`, `Database` and DB-API `connect`: graph/SQL commit,
writable/read-only reopen, string/PathLike boundaries, preservation of read
ceilings, invalid-anchor refusal before project creation, conflicting live
policy and refusal for in-memory opens. It uses the existing Bazel/Python
Bindings lanes and profiles; replay `pytest tests/test_file.py -k namespace_anchor`
and `pytest tests/test_dbapi.py -k namespace_anchor` from `bindings/python`
with the corresponding installed wheel. The Python workflow retains the tested
wheel by source SHA for manual host verification. CI's administrator process is
separate from a native ordinary-user process receipt; boundary existence alone
does not qualify physical power-loss persistence. Ref [#1255](https://github.com/nowledge-co/hawdb/issues/1255).

The `registered_consumer_embedded` cases in
`tests/support/embedded_consumer_coverage.rs` belong to `mvcc_staging_session`
and its existing `registered_consumer` native-platform selection. Both default
and minimal Bazel targets declare this helper in their `srcs`. They cover
#1171 one-operation private progress, cancellation/restart, complete pinned
rebuild comparison, prepared-drop/conflict recovery, original embedded owner
lifetime, and legacy full-residency/incremental interval compatibility.

The covered Consumer unit family also checks the exact retention boundary:
`registered_stationary_covered_retention_checks_complete_epoch_not_old_receipt`
accepts completed E at the resume floor after C expires, rejects E below the
floor, and preserves dependency rejection for a relevant post-E write. This is
part of the existing Search Consumer unit target; no new CI job is required.

## PostgreSQL 18.0 compatibility (manual)

| Family | Purpose and owner | Lane and known limits |
| --- | --- | --- |
| PostgreSQL regressions on HawDB alone | [`tests/pg-regress/tests/upstream_sql.rs`](../tests/pg-regress/tests/upstream_sql.rs), `make pg-regress-hawdb`; 19 unchanged upstream queries with exact source/output ranges and checked checksums | Manual Cargo integration test, `--no-default-features --test upstream_sql`; no PostgreSQL server or libpq. Covers column-left int8 comparisons, ungrouped HAVING and ordering by a non-projected input column. Original output supplies integer/ASCII-word expected rows; typed error mapping and fixture-only adaptations are explicit. Unselected syntax and ambiguous psql cells are not coverage. |
| PostgreSQL upstream SQL comparison | Same independent project, `make pg-regress`; Rust `libpq` reference client; typed rows, NULL, duplicate multiplicity, ordering and DML counts against a real 18.0 server | Optional manual path; pinned selected Boolean/int8 and adapted SELECT cases; unsupported source statements are not coverage. Stock expected files are provenance for this live-comparison CLI. |
| PostgreSQL isolation schedules | Same runner and embedded transaction worker; selected `simple-write-skew.spec` step permutations with explicit Serializable options and first-statement snapshot capture | Manual only; synchronous nonblocking steps. SSI victim/timing divergence is reported as a strict comparison mismatch, not proof of a HawDB isolation defect. Lock-wait scheduling, numeric fixtures and full PG equivalence remain outside this slice. |
| Harness negative controls | `cargo test --locked --manifest-path tests/pg-regress/Cargo.toml --all-targets --all-features` | Local comparator/provenance controls; no production CI registration. Independent Rust fmt/Clippy and actual PG 18.0 execution remain additional evidence. |

The pure Rust comparison CLI calls both the embedded library and Rust `libpq`
in-process. The additional embedded fixture has a manual Bazel binary target
`//tests/pg-regress:hawdb_pg_regress_worker`; no new crate is added to the library
workspace or package archive. The report records the source SHA/checksums,
working-tree state, first differing SQL, both observations and incomplete runs.

The stationary strict-capacity test also exercises detached reservation, native
construction and publication with the foreground descriptor target occupied;
no idle token may retain a descriptor reservation between those stages.

The same unit family covers unregister and competing reservation at Reserved,
Pending and Recorded native-birth states, busy unregister followed by successful
publication after registry-cache reload, and a scoped mapper that follows a
relationship to an out-of-scope label whose post-E mutation must invalidate
publication. Storage units check immutable reservation predecessors, missing
registration policy, and direct history pruning without staging compaction.

### Prepared query admission reuse

The runtime retained-memory tests cover fixed-reservation transfer, exact capacity,
shared process accounting, non-retryable self-wait refusal, and changed capacity.
`hawdb-runtime-tokio` tests cover single-CPU preparation/execution, queued future
drop and cancellation, and failed/panicking preparation. API runtime tests check
schema, branch and configuration revalidation and verify that a prepared object
does not retain a reader pin. Embedded template-cache tests require one lookup per
request, including disabled caches and non-deterministic bypasses. The
`HawDBPreparedAdmission` model and four negative controls cover the ownership
transition independently of query execution. Two manual reachability controls
also witness hard-refusal recovery after releasing preparation and a final
refusal when execution alone still exceeds capacity. The positive model remains
in the existing TLA suite; native capacity-band tests own Tokio waiter renewal.

## PR #1217 root CI scheduling

- `//:hawdb_unit_checkpoint_headroom_tests` retains the original headroom,
  relational live-overlay and two sustained WAL/delta pressure selections.
  `//:hawdb_unit_checkpoint_lifecycle_tests` owns the remaining
  `api::automatic_checkpoint::tests::` cases, including manual takeover,
  unattended/read handoff, planning/resource recovery, coalesced capture and
  shutdown. New lifecycle families inherit this second owner. Together the two
  disjoint partitions isolate the complete automatic-checkpoint namespace from
  general, without putting all 97 current cases under one deadline.
- Both checkpoint targets are included through the existing required root unit
  suites. They reuse the ordinary root executable and global checked profile,
  each with one libtest thread, Bazel `exclusive` and the unchanged 900-second
  deadline per invocation. The headroom aggregate now owns the two explicit
  targets documented above; the lifecycle target remains unsharded. Internal test-created concurrency remains intact. Automatic-checkpoint
  headroom CREATE failures retain the original success/refusal contract and phase,
  work-progress, wake/resource and libtest-concurrency diagnostics for
  [#1116](https://github.com/nowledge-co/hawdb/issues/1116); no caller retries or
  deadline changes. Isolation prevents phase/clock assertions from competing
  with unrelated unit cases and other actions in the same Bazel invocation;
  external node contention remains possible.
- The production five-second no-progress refusal, deliberate stalled-owner
  deadline tests, 320 acknowledged rows, checkpoint-count and reopen assertions
  are unchanged. No Busy retry, ignored assertion or production timing change
  is introduced. Native Cargo ownership is unchanged.
- The previous source-bound root run for #1217 (Prow
  `2113655888001634304`, head `7546e2d09`, base `8e9eab185`) completed all 68
  targets: 65 passed, two joined-read targets timed out at 300.154/300.316s and
  the general partition failed on `automatic checkpoint capacity`. The joined
  cases create synchronous durable fixtures and overlapped other tests and
  compilation. This is scheduling evidence, not proof of a WAL regression.
- Root presubmit also owns `//:mvcc_joined_read_shard_script_test` and
  `//:root_unit_partition_script_test`. They check complete/disjoint inventory
  ownership, nonempty execution, shard status signaling and native error
  propagation. Local replay uses the existing public joined-read labels or
  `bazel test //:hawdb_unit_checkpoint_headroom_tests //:hawdb_unit_checkpoint_lifecycle_tests`; no CI-only flags or
  special compiler profile are required. Final-head CI must establish runtime
  acceptance; target isolation alone is not a measured speedup.

The complete-family ownership closes a scheduling gap observed in
[PR #1492's root run](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1492/pull-skein-bazel-test-root/2117077993091239936),
head `0dc0983755b3e4941b554a8735291f10259b0bc5`, base
`f3e5ecd01514b2703bf2a00fffbaeb3d97ba9399`. General ran 2,161 cases and reported
17 automatic-owner failures: the unchanged 15-second waits expired in Preparing,
with continuing completed-unit progress and no operation failure in the retained
diagnostics. The partial selector had left these lifecycle cases in general.
The partition regression includes manual, handoff, planning-recovery and
coalesced-capture names, plus future nested and top-level lifecycle names. It
checks the separate owners, complete/disjoint inventory, empty-owner rejection
and status/argument propagation. No runtime
deadline, case assertion, compilation profile or retry changes. The diagnostics
do not establish corruption or every source of slow progress; external contention
remains possible. Complete current-head root execution, including both isolated
lanes, must qualify the mitigation under
https://github.com/nowledge-co/hawdb/issues/1499.

[PR #1502 review](https://github.com/nowledge-co/hawdb/pull/1502#issuecomment-6572313001)
showed why the complete namespace needs two owners: the original single-thread
97-case target passed in 832.8 seconds under its 900-second ceiling, while the
identical selector in #1500 timed out at 900.1 seconds. Restoring the original
headroom membership and giving the remaining lifecycle cases their own target
preserves isolation without increasing deadlines, relaxing assertions or adding
another root binary. Both target runtimes must be checked in current-head root
CI; the split alone does not establish runtime headroom.

PR #1277 scheduling evidence is bound to head `66920d194a` and base `84d5ed46e`.
The complete Prow BEP records one uncached failed Search text/background unit
attempt (197.851 seconds, 1,031 passed and one failed) and one uncached failed
root general attempt (191.404 seconds, 1,939 passed and two failed). Search
failed at its internal 20-second task deadline during preparation; the two
root sustained cases hit the five-second no-progress checkpoint capacity
refusal. These are internal refusals, not Bazel timeout ceilings. The existing
isolated checkpoint partition passed in the same root job (23.086 seconds).
The original logs and BEP remain available in the
[crate job](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1277/pull-skein-bazel-test-crates/2114716414920101888)
and [root job](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1277/pull-skein-bazel-test-root/2114716414613917696).
Contention is a scheduling hypothesis; final-head remote execution must still
establish whether isolation resolves these failures. No production deadline,
caller retry, fixture assertion or target timeout is changed.

## Bf-Tree MVCC adapter (manual experiment)

| Family | Purpose and owner | Lane and known limits |
| --- | --- | --- |
| Adapter correctness and recovery | [`experiments/bftree-mvcc`](../experiments/bftree-mvcc/README.md); `cargo test --locked --manifest-path experiments/bftree-mvcc/Cargo.toml --all-targets --all-features` | Manual independent workspace, no CI. Snapshot/atomic-pair visibility, conflict/rollback, binary-key model, page splits/history/ranges, deep-version filtered chunks and tombstones, exact inclusive snapshot seeks, native-returned-record/row budgets, bounds/ownership, exact retry and recovered receipt-backed sparse updates/deletion/reinsertion, uncertain/unwind and subprocess crash replay. Comparison-kernel differential tests cover binary ordering, 0–160-byte lengths, pointer offsets, every mismatch position, prefixes and random pairs; head/separator tests cover fixed previews and valid binary fences. Shuffled variable-length binary keys and deep histories exercise actual native inner levels, retained generations and late gap writes. The default dependency remains unpatched. Both ignored children are exercised by parent crash tests. Process exit alone does not qualify power-loss safety. |
| Bounded batch reads | Same independent workspace, `src/index/batch.rs` and adapter regressions; `bench-advanced <new-journal> <layout> 512 24 prefix64 keep` under the global release profile | Manual only. Input order/duplicates, missing/binary/prefix/max-length keys, old/current snapshots, dense cursor positioning and sparse/deep-history fallback, carried encoded targets, complete budget refusal and snapshot/collector races. Reports separate dense/sparse/missing/duplicate scalar/batch request tails and observable wrapper work; cursor starts are not exact native root-descent counts. Ownership regression retains an incidental descriptor copy across final-handle close/reopen, while active snapshots and the next owner remain exclusive. |
| Snapshot-safe collection/checkpoints | Same workspace, `src/adapter/maintenance.rs` and `maintenance/tests.rs`; `maintenance-demo <new-journal>` and `bench-advanced ... collect` | Manual only. Read/write leases, fixed-horizon bounded passes, released-lease cursor catch-up, retained anchors/tombstones, old-writer conflicts, generation publication, exact old receipts read by captured WAL ranges, full-prefix verification and suffix-only index replay. Injected errors and 15 subprocess exit/layout boundaries cover durable/partial deletion and checkpoint publication. Checksummed invalid metadata, lost unacknowledged tails, torn/zeroed writes and reordered full blocks fail closed without repairing evidence. Device/filesystem power-loss behavior remains unqualified. Receipt evidence consumes the unchanged bounded journal; collection does not reclaim disk capacity. |
| Layout comparison | Same workspace, `bench <new-directory> 16 2` and `bench <new-directory> 512 24` under the global release profile; [paired scan driver](../experiments/bftree-mvcc/measurements/scan-20261024/measure.py) also covers `2048 48` | Manual only. Rotated fresh-process versioned/hybrid/COW comparisons with identical synchronized journal; current/old read and write percentiles, prepare/sync versus index-installation phases, reopen, journal and RSS. Paired scan cohorts retain all controls, journal hashes and host/compiler observations. Memory working indexes; not a production engine or out-of-memory benchmark. |
| Bf-Tree comparison probe | [Manual prepare/build/measure drivers](../experiments/bftree-mvcc/simd/README.md), `bench-shaped` and [results](../experiments/bftree-mvcc/measurements/simd-20261024/README.md) | Manual only, outside CI and the production dependency graph. Checksum-verified temporary upstream/word/NEON dependency copies, strict adapter Clippy and dev/release MVCC tests, shape/layout smoke cases, rotated three-way comparisons of short/common-prefix/spread keys with COW controls and journal-byte equality. NEON measurements require an eligible ARM64 host. Cross-target kernel metadata checks do not qualify native Windows/Linux behavior. |
| Internal-key structural probes | [Manual structural drivers](../experiments/bftree-mvcc/structural/README.md), `prepare.py`, `build.py`, `measure.py` | Manual only, outside CI/production dependencies. Checksum/commit/lock/profile-bound temporary upstream/head/compact/combined copies, full adapter tests and strict Clippy in checked/release profiles. Separate diagnostic copies count actual pivot bytes, preview equality/fallbacks and retained generations through owned mutation-path summaries; instrumented timings never qualify performance. Five matched repetitions rotate layouts, key shapes and workloads with COW controls, exact normalized transaction equality, full raw reports and host/compiler observations. Read-only old-format audit is evidence normalization, not a database migration. |

The experiment is binary-only, unpublished and excluded from library archives.
See its README for independent formatting/Clippy and demo commands. Its journal
and Bf-Tree dependency do not change canonical storage or production test scope.


## Windows native contract ownership

The native workflow runs seventeen independent Bazel shards and a separate strict
Cargo Clippy job. The original `Windows native contracts` check requires every
result to pass. See [Windows native CI](WINDOWS_NATIVE_CI.md) for the complete
selection/profile/count matrix, cache policy, cold-build costs and local replay.
The daily/manual abandonment lane remains separate and requires eight passing
cases with the selected 1/2/4/8-thread setting. No fuzz campaign is added.

## Query identity encoding

The existing `//crates/query:hawdb_query_tests` / Prow crates lane owns published
`q1`/`t1` vectors, length-prefixed UTF-8/NUL framing, zero/max-u16 version headers,
lowercase hex, and ASCII-only language canonicalization across borrowed canonical
names, mixed ASCII case, Unicode whitespace and preserved non-ASCII case.
Replay: `cargo test --locked -p hawdb-query --lib`. These extend the existing
normalization family without changing its feature matrix, profile or lane.
The separate manual [encoding allocation experiment](benchmarks/query_identity_encoding_2026_10_27/README.md)
records source-bound constructor/retained-byte and admitted-host controls;
its probe is unpublished and is not registered in CI.

## Request-owned Cypher identity

The root general unit lane owns `api::tests::query_observability::query_identity`.
Its ordinary and concurrent cases cover read/write parameter rebinding, cache
hits and disabled caches, enabled/disabled summaries, recovery from failed
observation to successful completion, exact current-text hashes across equivalent query
spellings, telemetry, and summary/slow-log behavior after execution and planning
refusals. Counter deltas after the two equivalent-spelling requests prove the
ordinary and concurrent read hit, disabled-cache no-hit and SET bypass paths.
The same lane owns `api::query_runtime::tests::prepared_query_identity_rejects_mismatched_transfer_and_records_current_text`:
text mismatch leaves the prepared identity available, failed observation hashes
the current text through fallback, and a matching transfer consumes it once.
Replay: `cargo test --locked -p hawdb --lib query_identity`.
No profile, timeout, feature selection or execution lane changes. SQL, EXPLAIN
and deferred retained-cursor observation keep their existing owners. The manual
[allocation probes](benchmarks/query_identity_2026_10_27/README.md) record ordinary
and concurrent facade requests separately from admitted-host controls; no new
CI benchmark target is registered.

## Borrowed statement-summary completion

The existing `//crates/system-sql:hawdb_system_sql_tests` / Prow crates owner
covers borrowed versus owned completion, disabled capacity, first-sample
retention across text/language/kind changes, UTF-8 truncation, failure recovery,
FIFO eviction and saturating counters. Replay:
`cargo test --locked -p hawdb-system-sql --lib statement_summary_tests`.
The root general unit identity family above covers ordinary and concurrent
facade integration, including telemetry and slow logs when summaries are disabled.
No test profile, feature selection, timeout or execution lane changes.
The Cargo-only `statement_summary_allocations` example is a manual host-owned
allocation control; it compares current owned/borrowed inputs, separately from
historical whole-query producers, and is not registered in CI. Build with
`cargo build --locked --example statement_summary_allocations` and run
`target/debug/examples/statement_summary_allocations 1000`.
The [allocation record](benchmarks/statement_summary_2026_10_27/README.md)
retains complete facade/component samples and admitted-host negative controls.

## Strict append preparation

- Existing storage unit owner: `//crates/storage:hawdb_storage_tests`, discovered
  by the Prow crate presubmit suite on PRs. The weekly Storage Platform Cargo
  job additionally runs these library cases on macOS and Windows.
  `append_table::tests::preparation` covers mixed DDL/caller/generated batches,
  sparse metadata overlays, deep-history old/new partitions, checkpoint-state
  restoration and exact recovered sequence gaps, late rejection atomicity and
  retry without consumed sequence values. The tests are deterministic and use
  the global checked profile. Existing append admission/exhaustion and
  durability/concurrency cases remain additional coverage.
- Manual measurements: `cargo bench --locked -p hawdb-storage --bench
  strict_append_prepare` uses the global release profile. Both key modes,
  one/64 rows, zero/1024 unrelated tables and zero/1024 live batches; 257 measured
  preparations per release case with exact output/source validation. No CI or
  scheduled registration. See [workload, results and limits](STRICT_APPEND_PREPARE_BENCHMARK.md).
  These are preparation timings without WAL synchronization, not committed
  throughput or durable p99 measurements.

### Resident read leases

`HawDBResidentReadLease` covers atomic graph/projection capture after execution
admission, shared generation charges, refusal, release-before-refund and continued
publication while readers retain old generations. The storage-model CI suite owns
the positive model. Its manual-only `TornPair`, `UnchargedRead`, `PinnedWait` and
`EarlyRefund` controls must each violate the intended invariant. Native snapshot
and report tests above establish the host correspondence; the finite model does
not establish allocator capacity bounds or throughput.

The existing Mem `app_read_snapshot_tests` and `read_execution` runtime lanes
also cover detached candidate search, hydration and retrieval: resident and
out-of-core readers keep the old graph/projection pair while writer publication
and fresh public helpers advance; admission callbacks retire old publications,
accept data updates and reject changed I/O requirements. Retrieval checks zero
admitted working/result ceilings and a successful bounded control. Report
observation checks both `match_return` and `text_graph_search` event kinds and
preserves worker errors separately from a missing callback or timeout.

### Windows power-loss partitions

The weekly Branch Power Loss lane gives head, root, WAL-sync and manifest cuts
one Windows job per durability policy. Consumer
publication, Search projection publication, deletion/GC, abandonment and vector
recovery have required sibling selections, plus a disjoint core remainder.
Linux/macOS retain the full 52-case suite.
`scripts/branch-power-loss-shard.py` derives selected tests from
Cargo's actual discovery and rejects missing/new automatic families, duplicates,
empty partitions or an incomplete union. Each artifact records complete discovery,
all partition membership and the selected test names. Recorder and configuration
checks remain in every Windows job; all fault schedules, assertions, features,
checked-profile settings, `--test-threads=1` isolation and 120-minute job ceilings
are unchanged. This trades repeated build/setup cost for completing independent
fault groups before the job ceiling. Source `83eab403` run 39959116101 stopped
inside the automatic manifest case at 120 minutes; that is incomplete coverage,
not a 120-minute fuzz campaign. Source `60f8b596` again reached that ceiling after
starting 10 of 12 manifest configurations. The policy split retains all twelve;
runtime coverage also checks all 48 configurations across the four cut families
and positive completion summaries. Final-source runtime durations remain required.

At `ad4eaab5`, the separate Windows root and candidate-WAL-sync cases also
exceeded the unchanged 120-minute ceiling. Complete source-bound job logs show
roughly 15--18 minutes of compilation/setup before those cases; progress reached
8/12 and 11/12 configuration starts respectively, without a Rust completion
summary. These starts do not establish completed coverage. Root and WAL-sync now
follow the manifest policy split, retaining both boundaries and all three
successive generations in each policy fixture. The shared qualification body,
physical plans, assertions, compiler profile and job ceilings are unchanged.
Automatic Rust discovery grows from five to seven tests, and full power-loss
discovery from 49 to 51; the runtime validator still requires exactly the same
48 distinct configurations and positive completion summaries for every selected
automatic test. Missing/duplicate configuration and completion controls remain.

At `a9f9e0ef`, all six policy-specific manifest/root/WAL-sync selections completed,
but the remaining mixed-policy head case exceeded the same 120-minute ceiling
in run 40249469385. Native check annotations confirm the job timeout; its partial
log contains 8/12 configuration starts and no Rust or automatic completion
summary. Starts are not completed coverage. Head now uses the same independent
policy fixtures, preserving both boundaries, all three successive generations,
the shared qualification body and every physical fault plan/assertion. Discovery
is eight automatic tests and 52 full power-loss tests, with exactly the same 48
configuration identities. Profiles, serial execution and job ceilings are
unchanged. Source-bound runtime completion remains required for both head jobs.

`//:power_loss_partition_script_test` and the platform job's preflight check
exercise actual selector output under simulated Windows default CRLF translation,
then interpret LF-delimited arguments as Bash does. Control files explicitly use
LF so filters and `--exact`/`--skip` reach libtest unchanged. The test also rejects
duplicate, missing, unexpected and empty discovery. It locates the selector through
Bazel runfiles when the test launcher is renamed, preserving direct Python usage.
Candidate `92681f67` exposed
this transport bug before the selected qualification cases could start; those
failed jobs are not recovery-test results.
Manual dispatch with `scope=power-loss` runs the same complete power-loss
platform matrix in a separate concurrency group, preserving an ongoing full run.
`scope=runtime` selects both runtime platforms in another independent group for
query/adapter changes. Partial runs do not qualify their skipped lanes.
Scheduled and default `scope=all` manual runs continue to include every lane.

### Parameterized PageRank persistence

`api::tests::pagerank` exercises score updates and clearing through bounded
parameterized Cypher in explicit transactions. The root general unit owner
retains first-duplicate input normalization, missing IDs, preservation of
projected entities without external IDs, null clearing, exact commit epochs,
one WAL batch and reopen values. Its input-normalization fixture validates the
whole input before opening a transaction, including invalid scores and labels;
it performs no graph reads or mutations. The old PageRank CRUD methods, helpers
and response/request DTOs are removed from test builds as well as production.
These runtime regressions remain CI-owned; local strict Clippy compiles them.


## Mem adapter concurrency qualification

The manual `mem_query_concurrency` Cargo/Bazel benchmark exercises the ordinary
Mem handle with multiple report writers and point, report or streaming readers.
It retains per-call intervals, acknowledgement observations, errors, actual
runtime CPU/cgroup limits and residency, and verifies successful writes after
reopen under default synchronous durability. Its optional sustained-reader mode
keeps readers active until every fixed-work writer finishes, with the original
reader iteration count as a minimum and a caller-enforced process timeout.
Fixed-work and sustained workloads are reported separately. The `qualification` feature adds
actual publication-owner wait/hold histograms, process-wide Rust allocation
requests and shared-runtime Cypher parse/preparation counters. The parser count
includes admission and unprepared execution; the planner count includes cache
lookup/binding and optimization on misses, including refused attempts. SQL and
standalone parser users are outside that observation boundary. Instrumented
timings are not ordinary release latency. See
[`MEM_QUERY_CONCURRENCY.md`](MEM_QUERY_CONCURRENCY.md) for identical baseline
porting, workload manifests, boundaries and missing acceptance evidence.

The existing root general `api::publication::` family owns histogram boundary,
completed-guard, failed-acquisition and unwind accounting tests. These are also
compiled without qualification instrumentation in the minimal library build;
only unit-test owners enable their counters automatically. No manual benchmark
or additional fuzz job is added to CI.


The native `process_memory_policy_contract` shared-reservation fixture refreshes
its synthetic RSS sample after both persistent handles finish opening, before
starting admission assertions. Slow Windows initialization must not accidentally
turn the reservation-accounting test into sample-expiry coverage; the dedicated
missing/unsupported/stale case retains its fail-closed assertions and zero-age
sample. No production freshness limit changes.


The root general `augmentation_jobs_lifecycle` and `augmentation_jobs_interrupt`
modules now execute bounded parameterized Cypher directly. Status eligibility is
in MATCH/WHERE, mutation is in SET or MERGE/ON CREATE, and multiple transitions
share an ordinary explicit transaction. Caller-only input normalization retains
duplicate input, empty-reason and finite-progress refusal coverage. Existing
missing/status-mismatch, no-op epoch/WAL and one-batch durable-reopen assertions
remain. The legacy CRUD adapters, storage scans and eight response/input DTOs
are removed. These fixtures remain in their existing CI owner; local smoke does
not replace those assertions.

The same root general owner retains `graph_meta`, `schema_migrations`,
`community_lifecycle`, `community_memberships` and `community_reads` through
ordinary bounded queries. Coverage includes MERGE creation versus existing
rows, sequential duplicate requests, read-your-own-writes, guarded updates,
missing endpoints, stable projected ordering, separate count/page statements,
plan-cache reuse, pinned reads, one-WAL-batch publication and exact reopen.
An additional existing-only schema-migration case requires unchanged epoch and
WAL bytes. Host-only ID/property/finite-value normalization retains refusal
coverage. Twelve database mutation adapters, the Mem entity-delete forwarding
method, two test read wrappers, their graph scans/response shaping and 48 DTOs
disappear; request helpers only attach parameters
and row/payload limits. The remaining business fixtures still need migration.


`knowledge_entity_batch_deletes` and the root Source/Skill/Thread deletion cases
use bounded parameterized IN predicates and DETACH DELETE directly. They retain
scoped nonmatches, duplicate input, missing IDs, projected-idless protection,
read-only refusal, incident-edge removal, unchanged unrelated nodes and one-batch
recovery. They no longer use generic entity-delete response DTOs or business
read helpers for their postconditions. Native CI retains the existing owners.

### Matched UNWIND relationship MERGE

The existing plan-cypher relationship-batch family also checks retained heap
ownership of the boxed command, nested mutation values and projection spare
capacity. The allocation-free scalar row marker adds no reachable heap. These
cases protect integration with the shared retained-plan admission contract.
Scalar row references use the same AST binding as read-only node-pattern
properties; both planner families and the root read/mutation regressions
protect their independent scope and lowering paths. Bare scalar row mutation
values remain limited to relationship MERGE; node CREATE/MERGE retain their
existing row-property-only binding and refuse unsupported scalar row values.

The existing root general owner (`//:hawdb_unit_general_tests`) runs
`api::tests::merge_relationships::matched_*`: exact projected member rows,
duplicate input multiplicity, missing/NULL/empty matches, ON CREATE retention,
all database/concurrent transaction result-budget refusals, full-batch input and
storage refusal, NULL relationship-property refusal, large endpoint fan-out
with pending property-subset matches, and embedded host count-guard rollback
followed by WAL reopen. The executor preflight family (`//crates/executor:hawdb_executor_unit_tests`)
counts one projected scan/index lookup per endpoint pattern, preserves
unlabelled all-node matching even with an empty named catalog label, and covers early
fan-out refusal before ID rewriting, empty pairs, host memory budgets and
cancellation. Storage's existing `id_in_mutation` family in
`//crates/storage:hawdb_storage_tests` retains the independent scan oracle and
checks ID-led property/label rechecks over 4,096 candidates and pending nodes.
The parser and plan-cypher crate owners retain the syntax and scope/refusal
checks. No carrier, feature matrix, timeout or CI lane is added. Local replay:
`cargo test --locked -p hawdb --lib api::tests::merge_relationships::matched_`
and `cargo test --locked -p hawdb-plan-cypher --lib relationship_merge_batch`.
See [the supported pipeline contract](CYPHER_DEFAULT_PIPELINE.md#bounded-relationship-mutation-input)
and [#1397](https://github.com/nowledge-co/hawdb/issues/1397).

### Property mutation query fixtures

The property mutation cases in `src/api/tests.rs` and
`src/api/tests/knowledge_property_batch_mutations.rs` now use ordinary bounded
Cypher (five rows / 4 KiB per statement), fixed property names and explicit
transactions for batch writes. Input normalization only checks that a requested
assignment exists. Filtering and id matching stay in Cypher. Coverage preserves
invalid syntax, missing/filtered targets, idless protection, no-op epochs,
read-only refusal, exact changed/unchanged values and one-batch WAL/reopen.
The existing `//:hawdb_unit_general_tests` partition in the root Prow lane
owns execution; local lint compiles the cases but does not replace runtime
results.

Memory dedup-review fixtures use the same bounded request boundary, parameterized
IN matching and one explicit transaction/WAL batch. The root API lane retains
duplicate/missing/idless, empty-id and integer/string timestamp replay coverage.

Memory metadata fixtures retain omitted-timestamp versus explicit-timestamp
behavior, first-input duplicate precedence, missing/idless targets and durable
one-batch replay through fixed bounded SET statements in the same root API lane.

Memory content fixtures assert all 13 fields after a bounded parameterized SET,
first-input duplicate precedence, missing/idless protection and one-batch reopen.
Host input validation retains empty-id/unit/extraction and finite numeric checks.
The general root partition owns these migrated cases.

Memory access fixtures preserve repeated-touch increments, optional click/dwell
fields, negative-dwell refusal and one-batch replay using arithmetic SET
statements. Missing targets return no rows; host normalization does no graph
aggregation. The general root partition owns these migrated cases.

The memory decay refresh fixtures retain first-input deduplication, missing and
idless target protection, optional-confidence preservation, finite-number input
validation, exact values and one-batch reopen through bounded Cypher. Their
execution remains in the existing root API partition.

Lifecycle fixture corrections retain the original root cases: MERGE statements
are followed by bounded identity/value reads inside the same explicit
transaction; ordinary CREATE/DELETE statements use before/after state and
committed delete counts rather than expecting immediate projected rows.
Community summary presence and ordering use existing CASE predicates. The
fixtures retain identity, idempotence, no-op epoch/WAL and reopen assertions.

Normalized-space move fixtures use fixed Thread/Memory statements with
CASE-normalized source predicates, target exclusions and identity-property
matching. They preserve duplicate/missing/filtered targets, unchanged storage
identities, optional timestamps, retry no-op epochs, invalid property syntax and
one-batch WAL/reopen in the existing root API partition.
## License hook and subprocess lease handshake

- `scripts/check-license-headers.test.py` is a pinned-HawkEye fixture
  check for full/SPDX forms, Python/Shell shebangs, excluded/unsupported files,
  Go line headers/build constraints, MoonBit sources/interfaces/manifests,
  generated output/dependency exclusions, historical benchmark evidence,
  mixed valid/invalid batches, wrong owners/licenses and byte preservation.
  The staged-file hook uses the same HawkEye rules for both header templates.
  `.github/workflows/license-headers.yml` runs these fixtures and the full-tree
  wrapper audit on pull requests to `main` and pushes to `main`; the same
  fixtures can also run locally with pinned HawkEye on PATH.
- `//crates/storage:hawdb_storage_tests` owns the cross-process directory lease
  case `ownership::tests::rejects_lease_held_by_another_process`. The child keeps
  the lease until a parent pipe notification, avoiding its former ten-second
  self-expiry under CPU contention. Parent exit closes the pipe; the ordinary
  finite test deadline remains the outer bound. Native Cargo/Windows ownership
  lanes retain the same exclusion assertion.

## Native Go shared-library runfiles

`//bindings/go:hawdb_go_tests` resolves the single runtime file exposed by
`//bindings/ffi:hawdb_ffi_runtime`. The projection reuses the existing Rust
cdylib output and preserves its runtime runfiles; it creates no compilation or
copy action. The original `//bindings/ffi:hawdb_ffi` still provides the complete
linker outputs, including the Windows `.dll.lib` import library. Passing that
multi-file target to `rlocationpath` fails Windows analysis before Go executes.
Existing Go TestMain resolves the path through the runfiles API (including
Windows manifests) and actual bindings tests exercise dynamic loading on all
three native CI platforms. Windows uses the bindings feature profile and fresh
Go test results; the Unix native binding jobs retain their existing profile.

The native feature inventory audit also requires the independent telemetry
Bazel spec and lock to match the workspace Cargo.lock package version, source
and checksum. Update them together when upgrading the optional dependency.

### Memory lifecycle and evolution queries

`api::tests::memory_lifecycle` retains the ten lifecycle/latest/EVOLVES cases
previously in the root API module, owned by the same root general Prow partition.
Bounded parameterized SET/CREATE statements and explicit transactions preserve
first-input duplicate handling, scoped nonmatches, missing/idless endpoints,
exact property values, commit epochs and one-batch WAL/reopen. Input-only
validation covers empty identifiers/state/relation and non-finite confidence
before any database effect. The three database mutation adapters, twelve DTOs
and final two Mem business forwarding methods are removed. Transaction and
maintenance admission remain independent typed capabilities.

### Skill lifecycle and source queries

`api::tests::skill_lifecycle` retains eleven runtime cases from the root API
module in the general Prow partition. Fixed bounded SET statements cover usage,
optional success rate, metadata and lifecycle field groups. MATCH/MERGE/ON CREATE
keeps source relationships idempotent, including stable identities/timestamps
and unchanged WAL/epoch on retry or missing endpoints. Explicit transactions
retain the single WAL batch and exact durable-reopen checks. Input normalization
checks identifiers, lifecycle fields, nonnegative use count and finite bounded
success rates. Four business adapters and fourteen DTOs are removed; their
obsolete compile-only Skill DTO re-export identity assertion is removed too.

### Thread lifecycle and cleanup queries

`api::tests::thread_lifecycle` moves fifteen existing runtime cases from the root
API module into the same general Prow owner. Fixed bounded queries preserve
optional metadata timestamps, first-input duplicate handling, message counts
with newer-timestamp protection, repeated CONTAINS edges versus distinct deleted
messages, compaction endpoint/idless protection and exact/cascade identity
selection. Predicates and graph traversal remain in Cypher. Explicit transactions
and bounded reads verify affected counts, no-op epochs, unchanged survivors,
single WAL batches and exact reopen. Five business methods, fifteen DTOs, their
graph-inspection helpers and four old test read wrappers are removed.

### Label lifecycle and transfer queries

`api::tests::label_lifecycle` retains twelve root general cases through ordinary
bounded parameterized requests. Fixed SET/DELETE statements cover omitted
fields, duplicate input, missing/idless targets, exact/all label deletion and
surviving endpoints. Transfer candidates are selected, filtered and deduplicated
by Cypher; the host only binds each selected identity into an exact MERGE within
the same explicit transaction. Coverage preserves duplicate source edges,
space refusal, protected idless memories, existing relationship identity and
properties, retry no-op epoch/WAL, single-batch recovery and exact reopen. Four
business methods, twelve DTOs and their graph-scan helpers are removed. The
entity-label read fixture migration is described below.

### Entity label read queries

`api::tests::entity_labels` retains four existing root general cases using one
public read transaction and bounded parameterized requests. Separate entity
lookup and label pages distinguish a missing entity from a found entity with
no labels; Cypher owns ordering, projection and LIMIT. The same pinned reader
preserves three cache hits on repeat, future-property projections and stable
epochs. Malformed label/property syntax and empty normalized identities refuse
without WAL changes. Two business read wrappers, their handwritten result
shaping and eight DTOs are removed, completing removal of the test-only
lifecycle DTO module. Runtime execution remains in the root general CI owner.

### Source lifecycle and metadata queries

`api::tests::source_lifecycle` retains six root general runtime cases through
fixed bounded requests. Cypher predicates enforce current lifecycle state and
external identity; optional chunk assignments use a separate statement. Host
normalization validates identifiers, states and nonnegative chunk counts and
retains first-input duplicates without graph inspection. Assertions preserve
filtered/missing statement rows, empty commit output for SET RETURN, idless
protection, omitted fields, timestamps, one WAL batch and exact durable reopen. Two business methods and eight mutation DTOs
are removed. Source count, parsed/revision and label adapters remain separate
migration work.

### Source parsed metadata queries

`api::tests::source_parsed_metadata` retains three root general cases with bounded
parameterized SET statements. Base parsing fields and optional detail/path
assignments remain separate statements in one explicit transaction. The cases
preserve full versus omitted fields, empty file paths, missing/idless targets,
first-input duplicates, exact timestamps/content, validation before WAL and
one-batch durable reopen. One business adapter, four DTOs and the assignment
factory disappear; no executor feature or runtime test ownership changes.
Invalid-input coverage compares against the captured initial epoch, which may
include persistent bootstrap work.

### Source revision edge queries

`api::tests::source_revisions` retains three root general cases using bounded
parameterized endpoint reads and CREATE statements. Missing and idless source
identities create no relationship; ordinary explicit-transaction CREATE returns
no provisional rows, and commit/readback verifies the exact edge count and
properties. Input rejection preserves WAL/epoch, while two valid creates retain
one WAL batch and exact endpoints, timestamps and default properties on reopen.
One business adapter, four DTOs and its relationship-request factory are removed.

### Parsed Source creation queries

`api::tests::source_parsed_create` retains three root general cases through fixed
bounded MERGE/ON CREATE statements. Existing identities and repeated batch input
preserve original nodes/properties; three source formats retain exact input and
default fields. Pure input validation rejects missing fields, negative size and
nonpositive version before WAL changes. Explicit transactions preserve one WAL
batch, creation flags and complete durable-reopen contents. One business method,
four DTOs and the typed entity-create factory are removed.

### Source label relationship queries

`api::tests::source_labels` retains six root general cases through bounded
parameterized MERGE/ON CREATE and DELETE. Exact endpoint predicates protect
missing/idless sources and labels; repeated/existing associations retain
relationship identity and original properties. Staged pair reads, commit counts
and exact readback cover selection and survivors. Invalid inputs leave WAL and
epoch unchanged; both write families preserve one batch and durable reopen,
including retained endpoint nodes. Two business methods and eight DTOs are
removed with their typed relationship factories and validators.

### Source memory-count queries

`api::tests::source_memory_count` retains three root general cases with bounded
point calculations followed by fixed SET in the same explicit transaction.
Cypher computes increments and floor decrements; the host only binds computed
values. Repeated inputs observe earlier staged counts, NULL starts at zero and
missing identities produce no candidate. A nonnumeric stored count refuses its
query while prior staged changes remain committable. Invalid input preserves
WAL/epoch; two source updates retain one batch and exact reopen. One business
method, four DTOs and the graph-side aggregation helpers are removed.
Arithmetic stays in WITH/RETURN stages outside CASE branches, with a numeric
operand check before clamping. Increment clamps the base to `i64::MAX - delta`
before addition; decrement clamps the base to zero before adding its negative
delta and floors the result, preserving MAX/MIN boundary coverage.

### Source-reference cleanup queries

`api::tests::source_reference` retains its five root general cases. The three
mutation cases now use bounded parameterized CREATE/DELETE and candidate reads,
removing the cleanup adapter, three DTOs and three generic create-fixture calls.
Selection stays in Cypher; committed counts and exact survivor identities cover
reference isolation and retained nodes. Empty/whitespace references refuse
before WAL changes; setup-close/reopen-cleanup preserves one delete batch and
exact durable recovery. Existing pinned read/cache and delete-guard cases stay
in the same module and CI owner.

### Crystal source association queries

`api::tests::crystal_source` retains two root general cases through fixed bounded
MERGE and same-transaction reads. The cases preserve numeric finite input checks,
missing endpoints, exact relationship identity/properties, create versus retry
outcomes, unchanged epoch/WAL on retry and one-batch durable reopen. Two DTOs
and the business mutation adapter are removed without changing the CI owner.

### Entity creation and update queries

`api::tests::entity_writes` retains thirteen root general scenarios using bounded
fixed MERGE, SET and exact projections. The tests distinguish ON CREATE from ON
MATCH, preserve existing identity and properties, deduplicate batch inputs, reject
invalid syntax/identity before WAL, refuse read-only mutations and retain one
transaction batch with exact reopen values. The idless case exercises the
external-id predicate on an update; internal projected identifiers cannot select
that mutation. Four adapters, ten DTOs and dynamic statement factories disappear.
The obsolete compile-only identity assertion for the removed entity-create DTO
is removed separately from the thirteen runtime scenarios.

### Relationship deletion queries

`api::tests::{knowledge_relationship_deletes,knowledge_relationship_batch_deletes}`
retain six autocommit and seven explicit-transaction scenarios through fixed
bounded DELETE and projection queries. Endpoint/property/scope predicates stay
in Cypher; missing endpoints, preserved nodes, partial scope matches, read-only
refusal, invalid syntax before WAL and one-batch reopen remain covered. The
idless fixture retains a real edge to prove an internal projected identifier
cannot delete it. Four mutation adapters, seven DTOs and the dynamic deletion
statement builder are removed. The obsolete compile-only identity assertion for
the deleted DTO disappears separately; runtime root general ownership is unchanged.

### Relationship property update queries

`api::tests::knowledge_relationship_updates` retains nine root general scenarios
through bounded SET and projection queries. Relationship properties and source
scopes remain Cypher predicates; exact assignments, missing/idless endpoints,
filtered no-ops, invalid/empty SET, read-only refusal and one-batch durable reopen
remain covered. The idless case includes a real relationship whose weight must
remain unchanged. Four adapters, seven DTOs and their dynamic SET builder are
removed; non-returning SET results are verified through commit and readback.

### Relationship creation and merge queries

`api::tests::{knowledge_relationship_create_upserts,knowledge_relationship_batch_creates}`
retain ten and six root general cases through bounded CREATE/MERGE and fixed
readback. The cases preserve exact endpoint/property values, missing/idless
protection, source/target scope predicates, repeated MERGE identity/properties,
read-only refusal and one-batch durable reopen. Four fixture calls in two existing
storage-recovery/statistics cases also use bounded CREATE; their out-of-core,
checkpoint, cache/spill and exact-statistics assertions retain their CI owners.
The last eight business mutation methods and fourteen DTOs are removed, along
with the test-only analytics DTO module and handwritten mutation graph inspection.
Read adapter migration and final runtime qualification remain separate work.

### Entity and property query fixtures

The four entity-read cases and six former root batch/property cases now share
`src/api/tests/knowledge_entity_reads.rs`. Scope predicates and property
projection execute in fixed parameterized statements. Separate candidate and
visible phases share one snapshot and explicit cumulative output budgets;
caller-slot shaping does not filter or traverse the graph. Cache cases inspect
the snapshot cache (one template per statement), including separate exact and
scoped templates. Missing identities, filtered identities, null properties,
request order and exclusion of unselected payloads remain covered.

The entity-detail case moves from the removed legacy lookup helper into that
module, retaining exact distinct-neighbor and relationship counts. Its obsolete
malformed legacy-DTO decoder unit is removed with the decoder. The navigation
cases retain missing-identity and idless-node coverage through explicit external
or internal identity queries; the implicit facade fallback is removed. Two
existing out-of-core cases retain checkpointed entity/property reads and digest
corruption refusal. These cases remain in the existing root general CI lane.
Six read extension methods, eleven DTOs and the test-only mutation DTO module
are removed; fourteen other read wrappers still require migration.

### Crystal query fixtures

Nine existing root cases move to `src/api/tests/crystal_reads.rs`. Fixed queries
own key ranking, cursor predicates, limits, community grouping, distinct source
counts, visibility ordering and default-value projections. Detail/page counts
use separate statements on one snapshot with a cumulative row/payload budget.
Invalid key/cursor combinations and empty/null community scopes remain caller
validation. Cache assertions account for separate count/page templates and the
snapshot cache. All field, path-multiplicity, pagination and read-only epoch
checks remain in the root general CI lane. The three legacy wrappers, twelve
DTOs and host-side graph-result sorting/truncation helpers are removed.

### Community query fixtures

The existing two visibility and four memory cases retain their files and root
general CI owner. Visibility uses a bounded optional match and separate entity
count, preserving entities without memories, label isolation, default latest
flags, deterministic ordering and two-template cache reuse. Memory reads use
fixed mentioned/ranked/direct statements with query-owned scope/unit/crystal
predicates, distinct entity aggregation, ordered entity IDs and bounded output.
The main three-phase case shares one snapshot and cumulative budget. Mixed
sources remain separate named phases rather than a host-side graph merge.
All existing field, missing/default, rank, invalid input and cache assertions
remain query fixtures. Shared scope validation only normalizes caller input.
Two read wrappers, ten DTOs and their handwritten join/filter/sort helpers are
removed; navigation wrappers remain for the next migration.

### Induced-edge query fixtures

The two existing induced-edge cases move from `knowledge_navigation.rs` to
`induced_edge_reads.rs` under the same root general CI owner. Parameterized
identity, edge-count and ordered-page statements share an immutable snapshot
and explicit cumulative row/payload budget. Queries own endpoint membership,
strength/confidence/default selection, sorting and LIMIT. Caller missing-slot
shaping only consumes selected identity rows. Exact endpoints/types/strengths,
outside-node exclusion, total count independent of page size, repeat-cache hits
and invalid empty input remain covered. One wrapper, three DTOs and the legacy
host-side graph filtering/sorting helpers are removed.

### Relationship read query fixtures

Three existing navigation cases move to `relationship_reads.rs`. Separate seed,
scope and bounded relationship-page queries retain caller order, missing and
filtered seed distinctions, edge properties, unknown relationship-type empty
results and snapshot cache reuse. The scoped phases share one snapshot and
cumulative row/payload budget. The unknown-type fixture now asserts an existing
seed plus an empty edge query rather than the deleted facade's synthetic zero
seed counters. An existing out-of-core case additionally checks both exact edge
types and targets before checkpoint. The same root general CI lane owns these
cases. Two wrappers, four DTOs and legacy empty-group synthesis are removed;
shared traversal internals remain until neighbor/path/subgraph migration.

### Neighbor and dense navigation query fixtures

Five existing navigation cases move to `neighbor_reads.rs`, retaining explicit
internal identity for idless nodes, one/two-hop edge identities and properties,
incoming/outgoing matching, unknown types, normalized scope, missing seeds,
LIMIT 0 and deterministic limited pages with cache reuse. Fixed hop statements
replace the old host traversal; the main traversal phases share a cumulative
snapshot budget. An empty unknown-type result remains valid after the delivered
row quota is consumed. Fixture creation now uses ordinary queries.

The dense fixture still creates exactly the dense-adjacency threshold and
checks the physical group's layout, node, degree and untyped group enumeration.
Typed/untyped ordinary profiled reads return every exact relationship and target
under explicit output limits; distinct-neighbor count plus the seed retain the
subgraph cardinality check. The deleted facade's reason DTO/string fields are
not claimed as ordinary query-report fields. Existing out-of-core checkpoint
and reopen neighbor reads now use fixed statements, including exact target and
weight checks. These cases remain in the root general CI lane. Two wrappers,
three DTOs, their traversal helpers and one obsolete compile-only DTO re-export
identity assertion are removed; path/subgraph fixtures remain to migrate.

### Crystal key-ranking parser regression

`crystal_key_ranking_preserves_exact_prefix_and_contains_order` exercises both
Crystal projections with a Unicode key and conflicting importance/lexical
orders. It keeps exact matches before prefixes before contains matches, excludes
non-Crystals, and checks the cumulative six-row snapshot budget. Prefix ranking
uses `left(id, $key_chars) = $key` because `STARTS WITH` is a WHERE predicate,
not a supported CASE condition. The existing two failing Crystal CI fixtures
retain their result and cache assertions. The root general CI lane owns these
ordinary-query runtime checks.

### Final path and subgraph query fixtures

All six path/subgraph runtime cases in `knowledge_navigation.rs` now execute
fixed parameterized statements. Endpoint identity, one/two-hop paths, both
edge types, wrong direction, unknown types, source/target scopes, LIMIT 0,
zero allowed hops, missing endpoints and snapshot cache reuse remain covered.
Node pages use bounded zero-to-two-hop expansion; edge pages restrict both
endpoints to the query-selected node IDs. Host code only passes those selected
IDs to the next statement. The main path/subgraph phases share cumulative
snapshot budgets and assert exact identities, relationship types/properties
and commit epochs. All fixture creation uses ordinary queries.

These are bounded query fixtures, not a replacement generic BFS adapter.
Retired facade diagnostics and synthetic unknown-type node counts are removed;
an explicit seed lookup still distinguishes an existing seed from an empty
relationship result. Four wrappers, their request/output/diagnostic models,
legacy traversal/decoding/filter helpers, the test-support trait and both
re-export modules are removed. The obsolete contracts and facade fallback-enum encoding
units are removed with their enum; all six graph runtime cases remain in the root
general CI lane. Production retrieval retains its outgoing/incoming dense
adjacency reports using the existing storage directions directly.

The final navigation fixtures use scalar CASE comparisons for parameterized hop
bounds. Bare WHERE parameter predicates support equality/null checks, not ordered
comparisons. Bidirectional one/two-hop checks use fixed one-hop edges per pattern.
These syntax repairs retain
all exact-result, missing-endpoint, zero-hop and exhausted-budget assertions.

### Query-first concurrency review regressions

PR #1194 review repairs extend the existing root, search and Tokio test owners;
no execution lane, profile, timeout or discovery filter changes. Root autocommit
coverage pins a data writer before execution, publishes a new graph label, then
checks catalog/data preservation through checkpoint and reopen; UUID predicate
locks use the storage column type, and constraint/configuration changes reject
stale preparation. Embedded and Tokio cases cover execution that fits capacity
only after retained preparation is released. Search cases cover admitted lazy
resident RaBitQ reuse and refusal before allocation. Mem incremental maintenance
retains its reader/configuration after a descriptor refusal and retries on the
same handle. Query profiles count delivered SKIP/LIMIT rows once. Migrated route
fixtures execute repeated MERGE and missing/NULL assignments; parameter-only
normalization tests are named explicitly and are not database coverage.

These regressions run in the existing required Cargo/Bazel root/crate CI suites.
Local-only verification remains the Shiro regression/smoke targets and the short
MVCC campaign documented above.

The query-first review regressions also cover Mem report/stream admission when
execution fits but retained preparation does not, and route qualification
statements that exceed explicit row or payload ceilings. These live in
`src/embedded_store/read_execution.rs` and `src/graph_route_evidence_tests.rs` under
the existing root library test targets.

### Pinned full-scan input cost

The optimizer relational unit target (`//crates/optimizer-relational:hawdb_optimizer_relational_tests`;
`cargo test --locked -p hawdb-optimizer-relational --lib`) owns candidate-cap
independence, snapshot-size monotonicity, selective covering-index selection,
and unchanged point/default-context controls. These are checked-profile unit
cases in the existing crate lane; no fixture, feature matrix, timeout or
execution lane changes. See [the cost model](OPTIMIZER_COST_MODEL.md).

The proposed [cost qualification protocol](specs/OPTIMIZER_COST_QUALIFICATION_SPEC.md)
and [development plan](OPTIMIZER_COST_QUALIFICATION_PLAN.md) describe future
candidate-plan latency, cardinality diagnosis and coefficient-validation
coverage. The manual workload prerequisite now has concrete owners:

- `//tools/query-workloads:legacy_tests`: 2,160 frozen Cypher/SQL records from
  `37c0c50ec` (campaign v6, including scalar regex), protecting original seed/index and oracle-input mappings.
- `//tools/query-workloads:query_workloads_tests`: bounded, typed recursive
  SQL generation, deterministic replay and supported syntax combinations;
  independently enumerated directed CE populations, NULL counts and join
  multiplicities (64/128 rows, 77 cases), plus the 20-case compressed graph
  statistics inventory and complete-read query skeleton controls.
- `//tools/query-lab:query_lab_tests`: template/recursive execution on identical
  fixtures, complete typed replay with NULL/multiplicity, parse/output-refusal
  negative controls, setup failure denominators and frozen same-snapshot cost
  sampling (three warmups/fifteen measurements). These small fixtures use the
  ordinary checked profile; release baselines are explicit manual commands.
  CE controls additionally cover directed fixture truth and replay, zero/partial
  observation accounting, missing/failed case denominators, duplicate/foreign
  evidence refusal, A/A pairing and injected estimate/result/access changes.
  They validate diagnostic classification, not a global Q-error performance gate.
  The graph adapter also checks frozen fixture counts, same-execution
  cost/operator evidence, pinned replay and refusal of early-stop skeletons.
  The phase-1 cost proof executes ordinary, TopN and Sort-plus-Limit plans on
  one pinned Cypher snapshot, compares complete ordered results and retains raw
  costs, immutable policy identity, physical properties, cardinality estimates
  statistics decisions, complete fingerprints and bounded work usage. Fresh
  equal per-candidate deadline/work scopes remain constrained by one cumulative
  campaign context; the root regression reverses candidate order at the exact
  individual work allowance and proves all three complete while the campaign is
  charged for their sum. Replay rejects stale fingerprints and duplicate,
  missing or over-limit inventories. The phase-1 report protocol explicitly
  excludes source/build attestation, timing and regret, which remain unavailable.
  Root `api::tests::optimizer_proofs` also owns automatic batch-clock sharing,
  clock-function/parameter-map parity, caller-pin preservation, reversed replay
  and stale-clock refusal under the existing CI root lane. Shiro's existing
  optimizer qualification tests cover unpinned candidate clock sharing,
  deterministic clock replay, named-plan
  controls and error-versus-agreement classification. Its generated
  `temporal_case` family now checks seeded-clock candidate results against an
  independent NULL/duplicate/pagination model, including per-row timezone
  reads and empty input; editable capability replay retains the seed and clock
  derivation. Replay: `cargo test --locked --manifest-path tools/shiro/Cargo.toml
  --lib temporal_candidate_models` and `--lib clock_reading_variants`.
  No existing case mapping, feature, profile, timeout or lane is removed.

All three are included in local `//tools/shiro:shiro_tests`; no ordinary CI,
schedule, profile override or performance gate is added. Cargo replay uses
`cargo test --locked --manifest-path tools/query-workloads/Cargo.toml` and
`cargo test --locked --manifest-path tools/query-lab/Cargo.toml --all-targets`.
See [query-lab](../tools/query-lab/README.md) for frozen manifests, exact CLI
replay, cooperative-budget limits and the unqualified ordinary-plan baseline.
Existing Shiro oracle suites and CLI controls remain intact. Candidate-plan
qualification across all four families and coefficient promotion remain pending.

`cost_measurement::tests` additionally owns the separate paired bounded-order
diagnostic: bounded manifests, explicit unsupported-family inventory, complete
typed result/snapshot/identity parity across the 3-warmup/15-sample schedule,
NULL/duplicates, offset/wide output and empty results, equal-plan deduplication,
reliable inversion controls and refusal of missing/failed/zero-duration regret.
The existing embedded qualification entry point exposes preparation and
execution/materialization durations without changing ordinary query execution.
Replay with `cargo test --locked --manifest-path tools/query-lab/Cargo.toml
cost_measurement -- --test-threads=1`. Actual performance uses the repository
release profile and Cargo's compiled source receipt through `cost-measure`;
Bazel/checked runs remain correctness controls. See the query-lab README for
the bounded cohorts and unimplemented adapters. No CI lane or timeout changes.


## Embedded architecture ownership (#1222)

- `//:hawdb_storage_language_boundary_test` checks both normal Bazel dependency
  paths from storage to Cypher syntax and plans; Cargo metadata checks the same
  normal closure. It belongs to the existing root unit suite.
- Existing storage units own `canonical::node_label_cursor` and
  `store::projection_full_source::tests`: complete label synopses, absent labels,
  sparse IDs, overlapping labels, pinned checkpoint/delta/tombstone membership,
  cancellation, corruption and sticky budget/IO/memory refusals. Existing
  checkpoint accumulator cut/retry tests also cover resumable label inventory.
  Label Bloom cardinality cases compare one and 4,096 propertyless nodes with
  one or three distinct labels in ordinary and admitted flushes, including
  persisted descriptor round trips. Repeated labels retain the original memory
  allocation. A distinct-label insertion case cancels at every work-unit cut
  across two bounded shift blocks, then resumes without duplicate keys or leaks.
  Descriptor admission is checked before both matching and Bloom-negative
  decisions with cold and warm serving caches. Resident batches retain one
  pinned COW iterator per step and check cancellation between every delivered row.
- Existing default/minimal `hawdb_mvcc_staging_session` targets own the four
  `registered_consumer_defin` cases: canonical version/scope identity, graph/SQL
  catch-up and complete rebuild equality, target-version refusal/reopen, original
  owner retention, cancellation and conflict. No fixture or deadline is reduced.
- Ordinary query regression tests migrated on main in PR #1194 stay in their
  existing root partitions. Splitting API/embedded-owner implementations into
  private responsibility modules preserves their public facade identity and
  ordinary query execution; no captured-response replacement corpus is added.
- The manual storage `projection_label_source` and root `consumer_publication`
  benches report scan work/time and foreground latency/successful watermarks.
  The optional capture-before-writers schedule uses bounded setup handshakes
  outside measured transaction/preparation/construction intervals; it adds no
  rate limiter and changes no progress threshold. See the [source-bound
  controls](benchmarks/architecture_2026_10_24/README.md). No fuzz lane is added.


PR #1240 descriptor-admission follow-up retains the existing storage owner.
The label cursor reuses admitted checkpoint descriptor traversal; three new Rust
cases cover cold/warm serving caches and Bloom-positive/negative decisions under
tiny and exhausted concrete memory. They also check memory-before-IO refusal,
no denied descriptor progress, no shared-reader poison, prompt pruned-lease
release and selected descriptor/payload ownership after task closure. Original
production reproductions fail the new admission oracles; corrected cursor tests
and affected checkpoint/source tests pass. No target, fixture budget, timeout or
CI/fuzz lane is weakened.

## Owned SQL/PGQ token PEG

The existing `//crates/sql-syntax:hawdb_sql_syntax_tests` target owns the token
PEG engine regressions: ordered choice, successful and failed packrat reuse,
lookahead diagnostics, long invalid token text, invalid grammar rejection,
charged token-adjacency checks with capture rollback, memo reuse and Unicode
source-token boundaries,
linear malformed recursion,
fatal work/storage/depth budgets, independent language-depth admission through
memo reuse, capture rollback, and exact entry/edge accounting across direct,
small, dense and sparse memo storage. A sparse oracle checks adaptive storage
through forward visits, backward completion, holes, replacement and density
changes; dense tables retain at most four logical slots per occupied entry
and convert to sparse storage at most once. Lexical atoms finish without a
continuation frame while retaining the same logical work/depth checks. The existing
`postgres_pgq_tests` and `postgres_select_pgq_tests` targets own syntax ASTs,
pure PEG expression/graph rules, separate aggregate/SELECT DISTINCT admission,
independent exact 128-level expression and graph nesting (including their combination on the ordinary test thread), aliases,
joins, byte spans and composed GRAPH_TABLE rejection. All use the ordinary
checked profile and existing SQL syntax CI lanes.

`//crates/sql:hawdb_sql_tests` retains the frozen production/owned frontend
corpus and PGQ binder/lowering tests. It must preserve the recorded acceptance
and rejection codes, including the explicitly reviewed SELECT-family promotion
changes. All supported SQL families now use PEG, including DDL/DML,
materialized views and branches; production and test builds have no upstream
SQL parser dependency. Frozen Debug-AST SHA-256 fixtures preserve all 1,009 SQL
inventory outcomes and both Cypher public entries for 1,427 queries (2,854
records), exported from main `0f611b6dec` and independently rechecked against
`9c355e9a46`. Accepted hashes include source locations. The recorded 11
SELECT-family Semantic-to-Parse changes remain part of the frozen outcome
contract. The 102-case literal/contributing-span baseline and a new 489-case
generated SELECT/DDL/DML baseline were exported independently from main
`9c355e9a46`; the latter includes the generated `:=` rejection, all three comment
prefixes, empty SELECT/VALUES/storage forms, qualified aliases, mutation
predicates and unsupported statements. Never refresh these from the candidate. A main-only target directory, never
used by the candidate, independently recompiled `9c355e9a46` and re-exported all
1,608 SQL and 2,854 Cypher records with zero differences.

The additional 522-case `frontend_main_1474_peg_baseline_v1.jsonl` is exported
from an independent `f3e5ecd01514b2703bf2a00fffbaeb3d97ba9399` build. It covers
PostgreSQL integer/float/varying-character aliases and modifier refusal classes,
implicit INSERT columns, and parenthesized scalar/aggregate arguments. Six
explicit source-linked replacements promote INTEGER and implicit INSERT
columns from the unchanged 489-case historical fixture; each replacement
asserts the previous outcome and complete new AST hash. The new fixture and
three parser behavior tests run in the existing SQL test target; its fixture
glob includes the data without another target or CI lane.

The direct VALUES-literal PEG production keeps the active composite-lock
workload's 5,001 five-column rows inside the existing PEG step/node/memo caps.
It supports numeric, negative numeric, string, parameter, boolean and null
literals, with a delimiter lookahead before selecting that production. Other
expressions still match the full expression grammar and retain semantic
refusals. The parser regression uses all six literal families; the unchanged
`composite_lock_tests::sparse_discovery_work_budget_escalates_without_refusing_valid_statements`
remains the storage-level owner for the real workload. Baseline benchmarks
and oracle exports must use a separate Cargo target directory from the
candidate, including all transitive local crates; a shared target can reuse
another worktree's artifacts. Earlier shared-target timings are invalid.

The clause diagnostic campaign preserves 59 CREATE/15 INSERT condition cases,
1,816 priority pairs and 9,216 SQL sources through parse and prepare. Priority
conditions now exercise owned rejection metadata used by the CST lowerer;
foreign AST-field injection is retired along with that parser. Source tests
cover unsupported aliases, mutation OUTPUT, multi-table INSERT and ordering.
Oversized INSERT/DDL regressions preserve acceptance of large literals/comments;
oversized SELECT/EXPLAIN still fail closed at the existing input-byte cap.
The frozen 1,102-keyword admission inventory and explicit token/work tests
preserve conservative execution admission with the shared lexer. Existing root
SQL/materialized-view tests own query results, plans, parameters, caching,
joins, aggregates, vector execution and mutations. Existing branch SQL tests
own PEG lifecycle selectors, revision/request-key admission and page bounds.
Local Shiro parser campaigns and
`//crates/sql-syntax:hawdb_sql_alias_boundary_fuzz_tests` remain manual/local.
The ignored `parser::owned::tests::parser_timing_diagnostic` is a manual
production SELECT parser comparison, run with `cargo test --locked --release
-p hawdb-sql parser::owned::tests::parser_timing_diagnostic -- --exact --ignored
--nocapture`. It checks the eight independently exported main Debug-IR fingerprints first
and emits nine PEG samples per unchanged shape (discard the first). Compare
with a separate baseline revision using identical iteration counts: 2,000 short
queries and 100 wide queries. It covers short queries, vector calls, rejection,
and 500-column ASCII/UTF-8 projections; it is not a CI timing gate.
`cargo bench --locked -p hawdb-sql-syntax --bench frontend` is a manual parser-only
release benchmark; `-- --shapes` adds wide projections, many joins and rejected
inputs. It emits nine samples after grammar warmup and is not a CI timing gate.
See [SQL PEG parser](SQL_PEG_PARSER.md) for the matcher/transformer boundary,
budget accounting and explicit production-routing limitations.

### Unsupported storage format classification (#1260)

Existing storage unit ownership covers relational/append codec version refusal,
unknown materialized-view expression tags, and checksum precedence for slice,
streaming-file, and cooperatively controlled checkpoint reads. Core tests preserve
typed causes through nested I/O wrappers. The existing `branch_project_open`
integration owner covers public branch admission, Mem startup, doctor planning,
automatic/interrupted repair refusal, canonical checkpoint schemas, full-checkpoint
versions/features, and byte-for-byte file preservation. Python error tests keep compatibility refusal separate from integrity
errors. No CI lane or fuzz target is added.

Focused replays: `cargo test --locked -p hawdb-storage compatibility_tests`,
`cargo test --locked -p hawdb-storage append_wal_versions`, and
`cargo test --locked -p hawdb --test branch_project_open storage_format`.


### Full Cypher PEG continuation

`//crates/cypher:hawdb_cypher_tests` retains the existing pipeline and public
parser tests after deletion of the handwritten parser files. The independent
`migration_expressions_predicates_peg_baseline_v1.jsonl` adds 8,946 statement
and pipeline outcomes from main `9c355e9a46`, compiled in its main-only Cargo
target directory. Complete Debug-AST SHA-256 hashes include source spans;
rejections retain Parse/Semantic classes. These cases replace the old live
handwritten oracle for generated clauses, wrappers, Unicode truncations,
expressions and combined depth boundaries. Nine independently captured legacy
algorithm diagnostics remain exact-string assertions. The earlier 2,854 corpus
and 420 value/pattern/procedure records remain unchanged.

The shared engine has no native matcher API or AST value arena. Its
35 unit tests retain fatal budget propagation through alternatives, optional,
repeat and lookahead using real bounded PEG rules, and retain memoized subtree
identity/edges after capture rollback. Empty rules preserve zero-length source
spans. The adaptive-cache cases also retain pending/failed/matched state,
end positions, node identity and logical entry counts across forward/backward
visits, replacement and density changes with compact nonzero memo records.
These same sparse-oracle assertions cover one historical position in the local
rule slot, allocation on the third distinct position, the allocated history's
two inline records, vector growth, dense holes and permanent sparse conversion; all
logical entry counts, pending states, endpoints and node identities are retained.
The same assertions retain the original entry accounting when the direct slots
use the compiled grammar's existing 256-rule cap in a local array. Earlier native
qualification compares all 12,669 corpus records in cold/warmed worker threads
against the preceding heap-backed table: release passes at 128/256/512 KiB and
development passes at 256/512 KiB, with matching smaller-stack failures. This
is separate from WASM compiler checks and from the standard owner test profile.
Continuation-local results carry only match/mismatch; fatal resource errors
return directly from the matcher. The existing fatal-propagation cases retain
all original assertions across this representation change.
The compiled instruction matcher also fixes exact public work/depth/node/memo
boundaries for nested rule aliases, multi-token positive lookahead and cached
capture reuse. The admitted tree retains both named aliases and their spans.
SQL and Cypher share the same nongeneric CST grammar API.
Keyword-choice regression coverage fixes exact attempt charges, depth admission,
memo entry and capture counts, and diagnostic isolation across successful and
failed lookahead reuse. Named-rule call fusion keeps the alias and cached-capture
budget assertions unchanged. Manual comparison against the preceding PEG engine
retains 53,104 complete budget/CST/error records and 18,744 complete rendered
Cypher outcomes. An optimization-level-1 worker linked with ThinLTO against the
release library passes the latter corpus in cold/warmed native threads at
256/512 KiB; both versions abort at 128 KiB. This does not qualify WASM runtime
stack requirements or waive parser throughput and allocation gates.

Latest-main integration at `2ae7e88614` (#1411) adds bare row-bound values in
node patterns and classifies read-only UNWIND as a query pipeline. The PEG
value rule and selected-tree lowering retain both changes without restoring the
handwritten parser. All 12,220 earlier Cypher records were independently
re-exported from that main revision and matched the integrated candidate. The
older fixtures stay immutable: `migration_main_1411_peg_baseline_v1.jsonl`
records the eight intentional main transitions with their previous fingerprints,
plus 132 independently exported row-bound/CASE/context outcomes. Existing
#1411 parser and embedded query tests remain intact.

Main `9adbb166bf` (#1377) adds LOAD CSV, conversion functions and list-row
indexes. Its six parser tests remain unchanged. The new frozen
`migration_main_1377_load_csv_peg_baseline_v1.jsonl` contains 1,626 statement/
pipeline outcomes independently exported from that exact main revision, including
complete accepted AST/span hashes, nesting boundaries and rejection classes.
All 18,744 earlier outcomes are also independently re-exported and compared;
the combined 20,370 outcomes match the PEG integration. Existing fixtures and
their fingerprints remain unchanged. Four nonsemantic envelope/flag rules are
inlined to fit the added syntax within the original 256-rule cap. The ordinary
value, property-value and scalar-conversion grammars retain distinct admission
and recursion checks. The same Cypher Cargo/Bazel owner targets execute this
fixture; root `api::tests::load_csv` covers the embedded importer through PEG.
The ordinary-property diagnostic regression fixes the original error text for
invalid and missing values through both entry points. All 18,744 preceding
rendered outcomes remain exact after the explicit main change that adds LOAD
to the top-level dispatch keyword list. Conversion/index captures are admitted
only for those shapes, preserving ordinary value captures and memo accounting.
All 173 warmed allocation/refusal/wide/late-error/SQL probes retain non-increasing
calls, total requested bytes and peak live bytes versus PEG `04eb8ea1f7`.

The independent `migration_keyword_context_peg_baseline_v1.jsonl` adds 2,380
main `2ae7e88614` outcomes for keyword-named properties, all specialized
CASE families, including coalesced-difference floors, combined depth limits, row-binding refusals and scalar/SET/value
context differences. Dedicated PEG alternatives preserve specialized CASE on
`true`, `false`, `null` and `CASE` properties; they retain constant-value and
same-property admission. Scalar value-list lowering rejects reserved CASE row
bindings, and SET's ordinary-value branch preserves the CASE/COALESCE prefix
refusal. The compiled-rule cap stays 256; no resource budget is raised.

CST folding constructs owned identifier pairs and joined names directly, and
single predicates/comparisons return without allocating temporary predicate
vectors. Existing independent Debug-AST/error fixtures cover the resulting
ASTs, including comparison chains and nested boolean scopes. The shared VM and
memo storage remain unchanged by this allocation cleanup; no parser budget or
fixture is weakened. Parser allocation and throughput qualification remain open, separately from
the isolated analytics RSS regression.

The #1399 main continuation's matched-UNWIND assertion is retained in the
existing `parser/pipeline_tests.rs` owner after removing the legacy
`parser/pipeline.rs` implementation. Its scalar binding property must still
produce `ValueExpressionKind::Binding` through the PEG path; the main embedded
relationship-MERGE tests remain intact. This integration adds no parser fallback
or language-budget exception.

### Graph row-width and blocking-cost estimates

Existing optimizer-graph ownership covers `graph::costing::width::tests`,
`graph::costing::work::tests` and `graph::costing::tests_width`: pinned typed
schema, bounded distinct-domain width sampling, required-only decoding,
projection/graph ownership and alias scope, key-versus-body comparisons,
context-sensitive choices in memo/direct lowering, selected trace refresh,
metadata bounds and saturating row/cost arithmetic. Input estimates within the
OFFSET+LIMIT window use an equal ordering-work proxy and prefer TopN on ties in
both lowerings; greater-than-window controls retain comparison-aware choices.
Projection-fused pricing is pinned around the 64-MiB unit threshold and 2-MiB
facade threshold, where costing an unfused graph binding would invent spill.
Combined row-goal/width controls keep complete blocking comparison/spill work,
scale streaming decode work, and preserve the caller's memory hint for bounded
ordered-versus-Sort/TopN competitors in both memo and direct lowering.
Root `order_by_expressions` keeps the five-row exact-page control, including
its cheaper property Sort+Limit choice, and retains strict property/computed-key
TopN assertions with 64 additional low-ranked rows. Small and bulk cases share
complete expected-page assertions.
Asymmetric hash-join
inputs pin right-build retention and spill; one-to-three-hop traversal controls
keep opaque graph contributions visible in `proxy_fields`. Joint grouping and
width costing share the incoming alias scope: correlated key counts change the
spill estimate while an INT alias shadowing TEXT keeps its scalar width. The existing
`//:hawdb_linux_ci_optimizer_summary_smoke_test` pins the corresponding
comparison/decode costs for all seven affected graph workloads, retaining exact
row/cost, decision, fingerprint and repeated-planning assertions.
Core schema snapshot tests
cover table-qualified descriptor iteration, deletion and old snapshot identity.
The joint-NDV small-budget control retains all 400 source rows and the original
40-row window (TopN), and adds a 19-row window to keep Sort+Limit transfer
admission covered under the same 2-MiB query budget.
Root `api::tests::width_costing` checks complete NULL/tied output, pinned reads,
warm templates, small budgets and checkpoint/reopen in all residency modes.
A correlated-filter regression estimates at most 19 rows but consumes all 256
matches, returns the exact offset/tied page on cold and warm calls, and verifies
TopN stays within its 256-KiB blocking allowance without spill or leaked admission.
`query_observability::order_by_limit_uses_top_n_and_preserves_stable_order`
retains a selective TopN fixture under comparison-aware costing. Executor
`transform::tests::limit_admission` verifies selected-window transfers, skipped
payload, exhausted offsets and exact/one-byte-deficit admission; the same narrow
kernel regression is independently carried by the joint-NDV change. Replay:

```console
cargo test --locked -p hawdb-core --lib schema::snapshot_tests
cargo test --locked -p hawdb-optimizer-graph --lib
cargo test --locked -p hawdb-executor --lib transform
cargo test --locked -p hawdb --lib -- width_costing order_by_limit_uses_top_n_and_preserves_stable_order
```

The opt-in `width_cost_planning` release benchmark is manual, with bounded CLI
inputs, exact complete output checks, explicit row/payload budgets, cache and
spill/admission counters. No default target, profile, resource limit or CI lane
is changed. Read [the cost contract](QUERY_WIDTH_COSTS.md) before interpreting
logical width/CPU/IO units as measurements.

The source-bound [macOS ARM experiment](benchmarks/width_costs_macos_2026_10_31/README.md)
contains 19 cells, 114 processes and 684 fully checked calls, including narrow,
TEXT/STRING/vector, arrival order/native ordering, small/full K, three blocking
budgets, width-proxy misses and persistent residency controls. It shows gains
when TopN removes measured spill and records neutral/adverse outcomes explicitly;
it is not a CI performance gate or a bound on actual heap allocation.

### Temporal syntax on the owned Cypher PEG (#1382)

The existing Cypher Bazel owner retains all eight temporal parser tests and the
planner retains its temporal cases. The frontend removes no earlier fixture:
`migration_main_1382_temporal_peg_baseline_v1.jsonl` adds 386 fingerprinted main
transitions, 5,354 new outcomes and nine exact rendered diagnostics exported from
independently compiled main `ceb8db250c`. Complete Debug-AST hashes include spans.
The 17,888-record paired campaign covers both statement and pipeline entries,
constructors/namespaced calls, scalar/value maps, arithmetic precedence, unary
operators, generalized postfixes, contextual names, CASE/list combinations and
mixed nesting boundaries. Earlier main transition fingerprints stay checked.
The statement/pipeline behavior changes originate in main; prior oracles remain
immutable. Selected PEG captures lower directly into the corresponding static
value or per-row scalar AST, including generic CASE forms when specialization's
constant/reference checks fail. The handwritten temporal parser stays deleted.

Five unweighted CST container rules are inlined to accommodate the temporal and
scalar grammar within the unchanged 256-rule cap. Existing weighted rules and
their limits remain; the new value-call/map guards and scalar folding retain
bounded recursion. Map keys remain unique, scalar-map entries retain source order,
value maps remain ordered by key, and named-call/property adjacency follows main.
Native embedded temporal tests cover construction, parameters, arithmetic,
components, cached clocks, persistence, index seeks and out-of-core reopen.

### Boolean predicates and row-dependent UNWIND on PEG (#1428)

The Cypher owner retains main's boolean-composition and bounded UNWIND parser
regressions through the PEG driver. The immutable
`migration_main_1428_boolean_unwind_peg_baseline_v1.jsonl` adds 856 statement and
pipeline outcomes exported from independently built main `bb30c40fe2`. Together
with the temporal cohort, all 18,744 complete AST/span/rejection outcomes match.
Earlier fixtures remain unchanged. Bare boolean/null scalar predicates lower to
an equality against synthetic true; numeric/string/function operands still
require a predicate operator. Row-bound UNWIND sources remain syntax, with scope,
list types, multiplicity and payload admission checked by binding/execution.
No handwritten parser, extra named rule or increased grammar/resource cap is
introduced. Embedded row-bound, multi-WITH and shortest-path tests from main
remain under their existing root test owners.


## Unpublished public checkpoint contracts (#1501)

`crates/test-contracts` owns external manual-checkpoint and WAL-recovery
contracts through the same facade APIs and recovery oracle as before. One
engine-linked default `rust_test` contains all 11 original cases; its separate
minimal carrier preserves those five manual-checkpoint cases and the six WAL
cases available under the former Cargo minimal configuration. The shared
fixture owns a fresh directory for each case and removes only that directory.
The lightweight `fixture_tests` case proves independent data and cleanup even
when two live fixtures use the same label.

The required `//:hawdb_unit_tests` root suite owns both contract carriers.
`//:hawdb_linux_ci_smoke_tests` and the legacy WAL label refer to the same default
carrier, so aggregate expansion compiles/runs it once per invocation. The three
old public Bazel labels remain single-member `test_suite` wrappers. The actual
contract targets are marked `manual` to avoid duplicate implicit discovery, and
run with `exclusive`, one libtest thread, the ordinary checked profile and their original 300s ceilings.
The crate presubmit owns only the fixture unit test. No CI job is added; the
private automatic-checkpoint lifecycle/pause-hook tests remain in the root
unit binary. Their complete namespace uses the existing isolated checkpoint
owner, with unchanged deadline contracts.

`publish = false`, a Bazel `testonly` fixture library and explicit facade archive
exclusion keep the helper outside production artifacts. `publish-crates.sh`
refuses a publishable `hawdb-test-contracts` package or any normal/build edge to
it; the existing script regression covers both refusals and allowed dev edges.
Verify the actual publication order and facade archive as well as Cargo's
production dependency closure. This is test ownership/build consolidation;
measure cold/incremental compilation and actual execution separately before
claiming a CI speedup.

### Shared identity and plan-cache normalization (#1375)

The existing `hawdb-query` owner checks that the combined constructor matches
both standalone APIs, including mixed language casing and whitespace, repeated
and distinct parameter names, literals, quotes, invalid text and Unicode. The
existing digest/hash framing vectors and capacity checks remain unchanged.
`cargo test --locked -p hawdb --lib api::plan_cache::tests` retains cache admission,
parameter binding byte limits, capability/cancellation checks, statistics
isolation and WAL replay coverage. `in_memory_portable` and minimal WASM strict
Clippy cover the embedded facade. No new crate or CI job is introduced.

### Manual search and durable-write component profiles (#1669)

`examples/component_profiles.rs` is a manual release-only driver over existing
public search and concurrent-database APIs. Search rotates candidate selection,
selected-ID hydration and combined calls on a pinned synthetic generation,
asserting all score/rank fields, IDs and hydrated content. Writes verify complete
rows before checkpoint and after reopen under default strict durability while
recording preparation/commit and existing overlapping coordinator metrics.
The example's inline checks and bounded argument refusals are its local owner.
Two manual example regressions prove that repeated ranking calls receive
independent work/cancellation state and renewed absolute deadlines, covering
both preflight and measured-call helper ownership. Replay with
`cargo test --locked --example component_profiles -- --test-threads=1`.
Ordinary CI compiles the example but does not run performance campaigns. Replay and
measurement limits are in [component profiles](COMPONENT_PROFILES.md). No new
test lane, feature, target-local profile, timeout or production dependency.

### Property DEFAULT values (#1683)

`api::tests::property_defaults` in the existing `cargo test --locked -p hawdb
--lib` owner covers the Kùzu semantics table end to end for node and
relationship tables (absent → default; literal NULL, NULL parameter,
`MERGE ... ON CREATE SET p = NULL` and a later `SET p = NULL` → NULL), with the
stored explicit NULL checked in the node map and re-read after WAL replay and
after a checkpoint. It also covers explicit transactions, `UNWIND ... CREATE`,
`GraphStore::import_graph_snapshot_rows` rows with and without the key,
literal round trips (`[]`, `'{}'`, negative integers, nested lists) through
reopen and checkpoint, `TYPE ANY DEFAULT` with later writes of other types,
DDL-time rejection (type/NOT NULL mismatch, NULL, parameters, temporal
constructors, `current_timestamp()` on a `STRING` column),
`current_timestamp()` as one millisecond-truncated `INT` epoch-nanosecond
value per transaction that compares and sorts with an explicit
`current_timestamp()` written in the same transaction,
backfill of a non-empty table through bounded maintenance with records created
during `BACKFILL`, a reopen between bounded batches, a torn final backfill
batch that reopens resumable, one backfill instant for a `current_timestamp()`
default across bounded runs, `Database::property_schemas` across reopen, and
`tests/fixtures/pre_1683_property_descriptors` (written by main `d5ebd79b8`,
descriptors in a checkpoint and the WAL tail) reopening with no defaults and
accepting a defaulted declaration. The binary WAL codec sample set adds
defaulted `CreateProperty` ops (op code 29). No new lane, feature or fixture
format; replay with `cargo test --locked -p hawdb --lib property_defaults`.

## Logical dump and persistent format version (#1655)

Specification: [LOGICAL_DUMP.md](LOGICAL_DUMP.md).

- `tests/logical_dump.rs` (Bazel `//:hawdb_logical_dump_tests`, in the
  required `//:hawdb_unit_tests` suite; Cargo
  `cargo test --locked -p hawdb --test logical_dump`) is the public embedded
  API owner. It covers:
  - round-trip equality on a persistent database with every relational scalar
    type, Unicode/CJK/emoji text, the full `i64` range, NaN/±Inf/-0.0,
    binary with NUL and newline bytes, decimals, vectors, timestamps, column
    defaults, composite keys, a keyless table with duplicate rows, quoted
    identifiers, a self-referencing foreign key whose children precede their
    parents in key order, and a typed graph with indexes and constraints.
    Equality is checked by query comparison and by `dump(import(dump(db)))`
    being byte-identical to `dump(db)`. Pages of 7 rows and files of 10
    records exercise keyset paging and file rotation.
  - snapshot consistency: a pinned read transaction exports while another
    thread commits inserts, an update, a delete and graph writes.
  - fail-closed export: strict append tables, materialized views (search
    indexes excepted), a VECTOR
    default, an open transaction and an existing destination. Nothing is
    published and no staging directory is left behind.
  - import atomicity: a flipped byte in the last relationship file (after
    every table and node was written to the unpublished target), a truncated
    table file, a missing data file, a missing manifest, a damaged schema, a
    path-escaping manifest entry and an existing destination. Each publishes
    nothing and leaves no staging directory. A stale staging directory from
    an interrupted earlier import is neither adopted nor removed.
  - `search_index_definitions_are_validated_and_never_dropped`: a
    `search_indexes` entry on a non-text or absent column, or reusing a table
    or search index name, fails `verify_logical_dump` and import with
    `StorageIntegrity`; a well-formed entry naming an
    analyzer this build does not implement verifies and is refused on import
    with `Semantic`. Nothing is published.
  - `search_index_round_trip_rebuilds_the_same_ranking`: a table with two
    search indexes (one quoted name), CJK, full-width and NULL text, updates
    and deletes is exported in 7-row pages; `schema.json` lists both
    definitions and no index relation; the import, created index-first and
    loaded in 7-row pages, has the same `pg_indexes` definitions and returns
    the same `(id, score)` rows from `text_search` for every query, keeps
    maintaining the index on a later insert, and re-exports byte-identically.
  - graph property DEFAULT metadata (#1683) in format 1:
    `graph_property_defaults_round_trip_and_apply_after_import` round-trips
    literal (`INT`, `LIST`, `FLOAT`) and `current_timestamp()` defaults on
    node and relationship properties, checks the `schema.json` spellings, that
    restored records keep their stored values (an explicit NULL is not
    replaced), that `dump(import(dump(db)))` stays byte-identical, and that a
    node and a relationship created after import without the property get
    the default (a fresh `current_timestamp()` instant).
    `backfilling_property_defaults_restore_or_refuse_explicitly` restores a
    literal default in `BACKFILL` with records lacking it, finishes that
    backfill by maintenance in the imported database, refuses with
    `Semantic` to export a `current_timestamp()` default still in `BACKFILL`
    (nothing published), and exports it once maintenance has made it public.
    `graph_property_defaults_are_validated_and_fail_closed` makes
    `verify_logical_dump` and import refuse, with `StorageIntegrity` and
    nothing published, a null default, a literal the type rejects,
    `current_timestamp()` on a `STRING` property or in `backfill`, an unknown
    default kind and an unknown value tag.
  - a newer `dump_format_version` is a typed `UnsupportedStorageFormat`
    refusal before any write (from `verify_logical_dump` too), and unknown
    manifest fields are ignored.
  - `verify_and_confirm_reconcile_a_published_dump`: `verify_logical_dump`
    reports the export's manifest SHA-256, versions and counts, every damage
    case above fails verification without writing, and
    `confirm_logical_dump_publication` is idempotent on an export and an
    import, changes no byte, and refuses a missing path or a file.
  - the checked-in golden format 1 dump (`tests/logical_dump/golden_v1/`)
    imports with exact values. These bytes must never change. Regenerate a
    new golden version only with the `#[ignore]`d `write_golden_format_1_dump`
    and `HAWDB_GOLDEN_OUT=<dir>`.
  - a legacy store whose storage manifest declares `hawdb-storage-v0` is
    refused through the embedded open with
    `UnsupportedStorageFormat(UnsupportedVersion { "storage manifest", 0, 1 })`,
    changes nothing, and opens again once the version is restored.

  DDL dominates the cost (about 1.5 s per statement in the checked profile),
  so the damage cases share a one-table fixture. The round-trip case is the
  slowest, at about 35 s locally on an Apple M-series machine.
- `src/logical_dump/value.rs` unit tests (root unit binary,
  `logical_dump::value::tests`) cover base64 for every padding length and
  value round trips, including a map whose key is a tag name.
- `src/logical_dump/files/tests.rs` (root unit binary, general partition;
  `cargo test --locked -p hawdb --lib logical_dump::files`) owns publication
  through thread-local test hooks:
  `a_destination_created_immediately_before_publication_is_never_replaced`
  creates an empty directory, a populated directory, a file and (Unix) a
  symlink at the destination right before the no-replace move; each is left
  unchanged, nothing is published and no stage remains.
  `a_parent_sync_failure_after_the_move_is_an_unknown_outcome_that_reconciles`
  injects the post-move parent-directory sync failure, expects
  `PublishOutcomeUnknown` naming the destination with the complete result kept
  in place, and reconciles it with an injected-then-clean
  `confirm_publication`. The native no-replace syscalls themselves, including
  a destination created after the user-space existence check, are owned by
  `crates/storage` `file_io::rename_new::tests`
  (`a_target_created_after_the_check_is_not_replaced`).
- `crates/storage` `durable_manifest` tests and the two
  `rejects_unsupported_*_storage_version` store tests pin the typed storage
  manifest/checkpoint version refusal. `graph_recovery` keeps the Lightning
  empty-target import over the shared bounded `import_graph_rows_batch`.
- `bindings/ffi/src/dump_tests.rs` (`//bindings/ffi:hawdb_ffi_tests`;
  `cargo test --locked --manifest-path bindings/ffi/Cargo.toml dump`) covers
  the C ABI JSON reports and error kinds (`storage`, `semantic`,
  `invalid_argument`, `unsupported_storage_format`, `storage_integrity`), and
  `verify_and_confirm_reconcile_a_publication` covers `hawdb_verify_dump`
  (report equal to the export's, damage as `storage_integrity`) and the
  idempotent `hawdb_confirm_dump_publication`.
  `bindings/bun/tests/dump.test.ts` covers the Bun adapter, including handle
  revalidation after path conversion closes the database, `verifyDump` and
  `confirmDumpPublication`.
- `scripts/check-persistent-format.test.py` and
  `scripts/check-persistent-format.py` run in the `Persistent Format`
  workflow on every pull request. On a published release, the same workflow
  appends `Persistent format changed: yes|no (...)` to the release notes.
