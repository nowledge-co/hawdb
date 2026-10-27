# Test ownership and known slow paths

Use this index to select an existing regression owner and understand what a
slow target is checking. Target definitions and CI job configuration remain
authoritative; this is a map of test families, not an exhaustive list of every
Rust test function. Update it when changing a family's scope, build profile,
execution lane, fixture size, isolation or known failure mode.

The executor `spill::tests` family covers streaming binding encoding with a
full query-memory account, read/decode admission, exact disk quotas, nesting
preflight, short writes, I/O errors and scalar/nested/vector round trips.
`blocking::sort::tests` compares Sort/TopN tie order with and without spilling
and checks 16-KiB rows through bounded merges. The existing executor unit target
owns these regressions; replay with `cargo test --locked -p hawdb-executor --lib
spill::tests` and the `blocking::sort::tests` filter. The existing local-only
`//crates/executor:hawdb_sort_merge_account_fuzz_tests` campaign also exercises
the tie-order and wide-row controls. Test lanes, profiles and deadlines stay
unchanged; allocation microprobes do not establish end-to-end RSS or latency.

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

`api::sql_runtime::tests` passes prepared SQL mutations through the database and
transaction kernels with diagnostic-only text, detecting accidental parser
fallback. DDL, INSERT/RETURNING, UPDATE, DELETE and Strict Append preserve bound
parameters, output refusal, earlier transaction work and publication. Root
general and the native runtime lane own these cases. The relational crate's
prepared-mutation cases rebind current schemas/parameters and append routing;
its existing manual statement compiler campaign reuses templates across inputs
and compares independent typed writes. No new fuzz CI owner is introduced.

`hawdb-sql::template_cache::tests` keeps branch-catalog inspection out of the
selected branch's data-query cache without reparsing its returned template.
The SQL crate unit owner covers eviction/hit behavior; the existing root recovery
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

`nowledge_mem::concurrent_owner::tests` requires a second writer to commit while
the first transaction callback remains suspended, then checks both the retained
old snapshot and the newly published state and released admission memory. It
uses the existing App snapshot fixture and root general unit owner.
The weekly native runtime lane also runs this family and
`nowledge_mem::app_read_snapshot_tests`, preserving their mixed-statement,
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
`nowledge_mem::read_execution::tests` additionally exercises the actual
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

`nowledge_mem::read_execution::tests` covers ordinary owned/streaming and report
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

The root general `nowledge_mem` family exercises out-of-core text, hydration,
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
| Prow root: `//:hawdb_unit_tests`, `//:hawdb_system_sql_loom_tests`, `//:hawdb_linux_ci_smoke_tests`, CLI/shadow and package-parity targets | Embedded facade behavior, concurrent publication, recovery, public consumers, compatibility protocols and benchmark evidence | Required PR and mainline regression coverage. Fuzz is excluded. |
| Prow crates: `//:hawdb_presubmit_crate_tests` | Internal component contracts and exact Search feature profiles | Required checked-profile component coverage (the release-only observer target runs in the native lane). Each package owns `:presubmit_tests`; manual-only targets must not accidentally enter implicit suite discovery. |
| Prow dense analytics: `//:hawdb_unit_analytics_rss_tests` | Persistent graph identity admission, edge streaming and bounded incremental RSS in a fresh process | Separate exclusive target; dense fixture construction is outside the measured child's RSS baseline. |
| Prow storage crash recovery: `//:hawdb_storage_crash_recovery_tests` | Subprocess termination at persistence boundaries, complete transaction recovery, endpoint integrity and artifact generations | Dedicated job binds evidence to source/toolchain/features. Process termination does not alone establish power-loss safety. |
| Native bindings: `//bindings/ffi:hawdb_ffi_tests`, `//bindings/go:hawdb_go_tests`, `//bindings/python/...` | C ABI retention, host ownership/errors, Go integration/race checks and Python behavior | Binding-specific PR/mainline workflows; Windows contracts and scheduled qualification are distinct receipts. See workflow definitions for current triggers. |
| Weekly platform and daily Windows abandonment qualification | Supported platform/feature combinations and the complete Windows abandonment/GC fault matrix | Scheduled or explicit workflows. They are not fuzz time budgets. |
| Local Shiro and MVCC tools | Generated SQL/Cypher result relations, parser/storage campaigns and optimistic transaction-history checking | Manual only. Never include the tools in normal or dedicated CI, scheduled workflows or production dependencies. |

## Root regression owners

Namespace durability anchors use the existing storage descriptor tests,
`branch_project_open` integration target and root power-loss family. Storage
cases cover bounded inclusive barriers, unchanged default ancestry, failed
barrier retry/accounting, pre-effect validation, canonical aliases, symlink
escape and live-owner configuration conflicts. Detached producer scratch keeps
the initiating project's descriptor accounting while retaining its complete
ancestor walk, including failed-barrier propagation. The Windows descriptor case
uses a fresh thread with a restricted token, removing administrator-group
authority and privileges without changing machine ACLs. It opens and reopens a
private project beneath the OS-owned temp directory. The embedded facade case
round-trips graph and SQL writes. The anchored initial-installation recorder
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
| `//:hawdb_manual_checkpoint_tests`, `//:hawdb_manual_checkpoint_minimal_tests` | Explicit ordinary/concurrent/embedded checkpoints preserve durable publication independently of background support/admission. Embedded controls retain one same-governor background or foreground I/O permit, distinguish allowed foreground work from legitimate refusal, and check reopen and refunds. | Existing default/minimal public-consumer carriers and profiles; replay `cargo test --locked --test manual_checkpoint`, adding `--no-default-features` for minimal. The existing root automatic-checkpoint `publication_io` family also pauses the actual owner with its acquired wave, observes manual suspension, and checks durable completion/refunds under [#1219](https://github.com/nowledge-co/hawdb/issues/1219). |
| `//:hawdb_unit_general_tests` | Embedded API, schema/query and general facade regressions | Shared unit executable; partition script assigns each discovered case an owner. |
| Automatic checkpoint `read_handoff` | Read callbacks, concurrent and SQL reads adopt ready generations; invalid identities fail closed; typed EXPLAIN ANALYZE progresses without the owner mutex | Five existing cases in `//:hawdb_unit_general_tests`. The seal-pause fixture waits for the owner's handoff-pause acknowledgement before adoption or guard release; observing `Phase::Handoff` alone races probe entry. The invalid-identity case prepares its foreign persistent database before entering the bounded owner pause, keeping unrelated setup I/O outside the 15-second assertion window. Existing query, admission refund, snapshot and reopen assertions and deadlines remain. Replay: `bazel test //:hawdb_unit_general_tests --test_arg=api::automatic_checkpoint::tests::read_handoff`. Ref [#1116](https://github.com/nowledge-co/hawdb/issues/1116); this fixture race is distinct from other checkpoint/branch-open stalls. |
| `//:hawdb_unit_concurrent_tests` | Transaction locks, read observation, commit admission and concurrent publication races | Known long target: [#972](https://github.com/nowledge-co/hawdb/issues/972). Preserve barriers and observations; a deadline is not a substitute for a deterministic schedule. |
| Covered pessimistic graph mutations (`covered_graph`) and concurrent metrics (`api::concurrent::metrics::tests`) | Retained staged results under existing locks, new-target/upgrade replay, stale-epoch refusal, lock timeout/retry, rollback, failed-statement atomicity, durable reopen, disabled-by-default instrumentation and explicit metrics inspection during writer ownership | Owned by `//:hawdb_unit_concurrent_tests`; local replay with `--test_arg=covered_graph` or `--test_arg=api::concurrent::metrics::tests`. Assertions use work counts and final rows rather than timing thresholds. |
| `//:hawdb_unit_analytics_core_general_tests` | Graph algorithms, projected artifacts, identity and generation behavior | Separate from isolated dense RSS ownership. |
| `//:hawdb_unit_analytics_rss_tests` | Dense persistent graph algorithms with bounded reader RSS and streamed adjacency | 128 nodes / 16,256 relationships. Isolated by #1030; history in [#1027](https://github.com/nowledge-co/hawdb/issues/1027). Uses the same repository test profile as every other root partition. |
| `//:hawdb_unit_recovery_tests`, `//:hawdb_unit_storage_crash_matrix_tests` | Transaction persistence, checkpoint/compaction and recovery boundary regressions | Dedicated crash-job evidence has additional identity and completeness checks. |
| `//:hawdb_unit_row_page_compaction_{materialized,out_of_core}_tests` | Physical row-page reclamation while pinned readers retain old generations; correct reopen/scrub in both residency modes | Full 16-table churn fixture and physical-byte assertions. Repository-wide checked test profile. Split by residency in #959; history in [#889](https://github.com/nowledge-co/hawdb/issues/889). |
| `//:hawdb_unit_row_page_compaction_admission_tests` | Budget refusal, retryability, failpoints and admission release around compaction | Ordinary compilation; separate from the two full convergence cases. |
| `//:hawdb_mvcc_joined_reads{,_minimal}_tests` | Joined-read dependencies, absent/filtered parents, off-page children, key bindings and sticky errors | Root presubmit; full and minimal feature contracts, each compiled once and executed as eight disjoint exact-case Bazel shards. Every cold/page/effect fixture and assertion remains; each shard retains the 300-second deadline. Fixed scenario matrix rather than timed fuzz. The shard script rejects empty inventories, invalid coordinates, failed or zero-case selections. |
| `//:hawdb_mvcc_projection_publication{,_minimal}_tests` | Projection publication versus concurrent reads and transaction validation | Preserve publication barriers and complete observation evidence. |
| `//:hawdb_mvcc_staging_session{,_minimal}_tests` | Fixed-E graph/SQL mapping, complete-E phantom/delete/content guards, disjoint-write rejection, append/projection isolation, registered Consumer publication, cancellation and reopen | Root CI; one-item batches and hot/cold full-rebuild equality. Registered Consumer cases also run on native Windows. The competing-registration/candidate fixture uses one background slot and proves idle tokens retain no execution slots. The stationary Consumer family covers fresh publication admission, 260 resident refusals with unchanged allocation ledger/directories, native birth retirement, seal/intent FD pressure with injected pre-selection failure and seven publication/ACK/cache boundaries with live resolution and reopen. FD observers require `test-support`; other cases run in the default unit lane too. Final artifact construction is blocking. See [source-coherent catch-up](specs/SOURCE_COHERENT_SEARCH_CATCH_UP_SPEC.md) and [registered publication](REGISTERED_CONSUMER_CATCH_UP.md). |
| `//:hawdb_mvcc_full_source{,_minimal}_tests`, `//:hawdb_mvcc_baseline_comparison{,_minimal}_tests` | Exhaustive pinned source enumeration, duplicate IDs, full set/content comparison and full-rebuild reference | Root CI; small deterministic hot/cold fixtures. Complete-E verification adds a full-source pass and conservatively rejects every post-E write. |
| `//:hawdb_branch_project_open{,_minimal}_tests` and root branch lifecycle units | Existing-project open, recovery, leases and resource ownership | Includes V2 project selectors, exactly one retained metadata directory handle, strict foreground saturation/export retry, pending-child internal recovery completion at four descriptors, and cold branch publication and branch switching while strict foreground capacity is occupied. Verify durable identity, catalog results, reservation retirement, source isolation and reopen. Native FD scaling runs in an isolated subprocess and verifies return to baseline after closure. Persistent fixture cost is distinct from randomized exploration. |
| `//:hawdb_execution_request_contract_tests`, process-memory/search-consumer contracts | Request budgets, refusal behavior, projection generation publication and host resource policy | Preserve exact error and readiness assertions. |
| Registered stationary Consumer unit family (`cargo test -p hawdb --all-features --lib api::search_projection_consumer::`) and `//:hawdb_search_projection_consumer{,_minimal}_tests` | Native creation/catch-up/reopen, immutable old pins, completed receipts, revocation, disposable-cache recovery, transient IO retry and allocation capacity | Includes 260 actual catch-ups with an old pin, 256 unresolved rows with typed exhaustion, and transient retry through both direct open and pending-publication `open_or_create` startup recovery without reinitialization. The long capacity fixture suspends automatic checkpoint publication; checkpoint concurrency remains covered separately. Root checked-profile lane plus Unix native binding targets; positive `registered_stationary` and `_1043_` selections execute in the Windows native contract lane. [Protocol, limits and remaining gates](CONSUMER_STATIONARY_PUBLICATION.md). |
| `api::search_projection_consumer::tests::recovery::registered_stationary_*` | Same-handle preparation/lost-response retry, interrupted initializer, late generation/resident refusal, exact reservation reuse and cleanup custody | Registered in the root unit family and selected by the existing Windows `registered_stationary` lane. The FD/rename-observer cases require `--features test-support` (included by Windows `--all-features`); the remaining cases also run in the default root lane. Post-intent descriptor pressure must not cause target refusal; injected pre-selection failures reuse the same operation without rerunning the initializer; unknown cleanup evidence must survive. Rejected selectors and pre-selection source advancement settle creation, release capacity and private controls, and require explicit fresh-target rebuild. Lost metadata replies refresh expired/changed lease policies. Recovery capacity applies even to a registry that needs reinitialization. Terminal cleanup retries preserve foreign evidence and succeed after another operation compacts the completed row. Ref [#1106](https://github.com/nowledge-co/hawdb/issues/1106). |
| Storage `consumer_allocation::bootstrap_tests`, `file_io::source_directory::control_candidate::tests`, `held_wal_rollback` | Torn bootstrap candidate isolation, lost ledger reply, pre-effect retry versus uncertain rename, foreign-name cleanup refusal and original-WAL header preservation | Existing storage unit owner plus explicit native Windows failure-boundary selection. Torn/reordered images use the finite power-loss model, separately from process reopen and native execution. |
| Storage `file_io::source_directory::directory_relabel::tests` | Held-parent/original-child exclusive directory rename, live anchor, no path re-resolution, wrong identity/context/symlink/case-alias/target-race refusal, lost native reply versus definite move plus observer failure, one-helper FD admission, fresh finish/sticky binding fault, recoverable FD/sync refusal and retained recorder child inode/crash images | Existing `//crates/storage:hawdb_storage_tests` carrier and the existing native Windows `consumer-failure-boundaries` selection. Real Windows success with child/anchor held is required; cross-compilation or move-or-refusal is not success. The caller retains namespace exclusion and actual candidate memory funding; the primitive creates neither. Scoped bounded model `//docs/tla:HawDBCapturedDirectoryRelabel_check` joins the existing storage manifest; manual `//docs/tla:captured_directory_relabel_controls` preserves thirteen producer controls/five separate positive witnesses. [Primitive/refinement](CAPTURED_DIRECTORY_RELABEL.md). |
| `//:storage_tla_evidence_script_test` canonical inventory guard | Actual checked-in sorted/unique manifest and all canonical model/config pairs, including undeclared top-level extras, are checked through the production `--manifest-json` path without Java or work creation | Existing evidence-script carrier owns both the canonical runfiles guard and synthetic invalid-order/missing/extra/duplicate fixtures. `//docs/tla:canonical_storage_model_sources` supplies the explicit manifest plus package-local `.tla`/`.cfg` glob, preserving completeness checks. |
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
| Shared SQL/Cypher vectors | Core/SQL targets cover dimensions, encoding and exact distance; storage covers value/schema codecs, fingerprints and cooperative cancellation; root `hawdb_unit_general_tests` covers exact filtered ranking versus candidate post-filtering, traversal, rollback and WAL/checkpoint/read-only reopen. Executor allocation tests and native FFI/Python/Go targets cover payload accounting and ownership. Shiro reports retain vector dimensions and coordinate bits. Existing CI lanes apply. Focused replay: `--test_arg=shared_vector --test_arg=vector_schema --test_arg=vector_encoding --test_arg=vector_distance`; include `checkpoint_units` for storage cancellation and `owned_record_bounds` for the allocation oracle. See [contract](specs/VECTOR_COLUMN_SCHEMA_SPEC.md). |
| Schema-bound vector indexes | Core tests cover resolved descriptors and canonical metadata; Search tests cover binding persistence, backend identity and typed budget diagnostics. Root tests exercise complete SQL/node source capture, empty/NULL inputs, composite keys, source/schema read-domain fencing, full rebuild after updates/deletes/NULL transitions, retained old domains and reopen, signed distances, cancellation and admission retry. Existing Search feature-matrix and root general CI lanes own these cases. Focused replay: `--test_arg=vector_index`; include `--test_arg=projection_fallback_requires_a_typed_resource_error` and `--test_arg=search_projection_consumer::tests` when changing shared Consumer publication. Eligible-domain cases cover SQL/Cypher parity after an ineligible nearest prefix, typed read-domain identity, source-work refusal even with no surviving candidate, bounded canonical fallback, duplicate paths and upstream LIMIT, ordered range/LIMIT with rebound parameters, and shared-ledger eligible/output memory handoff. The existing ACL capability runner also selects the traversal visibility test, which exercises exact and filtered-ANN modes and refuses a missing test name; its fixture covers hidden intermediate nodes. Storage adjacency tests independently meter rejected edges before emission. Core scoped-work tests cover shared clones, parent limits and cancellation. Storage projection admission borrows work ownership separately from payload admission and preserves typed exhaustion without allocator probes. Core guards distinguish unbounded work no-ops from explicit cancellation checkpoints. Executor point-read guards use a shared ledger sized for exactly one payload plus fixed callback overhead, covering resident and canonical nodes/relationships, repeated reads, missing/label-rejected reads and complete refund. Storage point guards meter missing/tombstoned/rejected reads and preserve sticky work refusal; existing adjacency guards cover rejected edges. Language cases cover missing/duplicate/unknown parser options, rejected SQL shapes and Cypher YIELD bindings, SQL/Cypher foreign-governor refusal, SQL single/early/final callback cancellation, consumer-error precedence and fresh-task read-domain reuse, typed parameters, signed distances, SQL composite-key hydration, post-window filtering, Cypher canonical traversal, and exact canonical identities across old/current/reopened generations. A smaller-than-input ANN window asserts actual rerank counts. Filtered-ANN guards assert zero collection scoring, eligibility before candidate generation, smaller-than-domain bounded reranking, validated empty eligibility, same-scope native mapping work refusal/retry and preserved default post-filtering. Native mapping units reject missing, duplicate-visible and NULL-ordinal mappings, exercise typed memory refusal at initial reserve and per-key growth, retry after pressure is released and refund their admitted memory on every exit. Root empty-index mapping tests preserve errors and same-permit retry; captured candidate-spill and Stage cleanup guards independently cover native resource owners; empty-domain queries still enforce binding/mode/candidate caps and perform no spill or vector scan. Rebuild/update/delete/NULL and old-pin cases exercise all three modes. Allocation-owner tests cover canonical SQL point reads, wide-row/repeated-projection/empty-literal refusal before copying, LIKE/ILIKE layout admission with Unicode/escape controls, covered-catch-up refusal before mapping/allocation, foreign-governor refusal before skipped execution, retry, producer workspace refusal and cancellable I/O-wave admission, same-database branch mismatch and uncertain selection recovery with old pins. Minimal consumer and portable facade cases refuse schema-bound index construction before filesystem work when the native backend is absent. See [contract](specs/VECTOR_INDEX_BINDING_SPEC.md). |
| SQL result column order | `//crates/executor:hawdb_executor_unit_tests` covers named rows ordered by a declared schema, empty results and rejected column drift. Root `//:hawdb_unit_general_tests` (`relational_sql::tests`) pins each shape's EXPLAIN plan and runs it with and without matching rows: borrowed scans, key and index lookups, inner and LEFT joins, scalar functions, index-ordered, top-N (on projected and non-projected columns, also over inner and LEFT joins), DISTINCT and alias-ordered projection, wildcards over top-N, DISTINCT and LEFT joins, LIMIT 0, and columnar, row-at-a-time and grouped aggregates, including HAVING and joins. A cost-reordered wildcard join and strict-append SELECT also run with and without rows; FROM-less SELECT and COUNT DISTINCT always return one row. Repeated output names and the reserved `__hawdb_relational_sort_` prefix fail during preparation, including EXPLAIN. `crates/qualification` `evidence_digest::tests` keeps result digests independent of column order, matching name-keyed reference rows. The Python binding reports these schemas unchanged as `QueryResult.columns`; `tests/pg-regress` sorts column names and does not cover order. Existing CI lanes apply. Replay: `cargo test --locked -p hawdb-executor --lib named_rows_follow_the_declared_schema` and `cargo test --locked -p hawdb --lib -- relational_sql::tests::database_sql_ relational_sql::tests::unordered_and_wildcard_joins`. Catalog and SQL EXPLAIN coverage is listed below. Cypher RETURN and the vector-index SQL streaming callback, which receives name-keyed maps, remain outside this family. |
| Catalog and SQL EXPLAIN column order | Root `api::tests::result_column_order` covers explicit and aliased catalog projections across Database, snapshot and transaction reads, empty results, declared wildcard order, mixed wildcard/explicit projections, duplicate output rejection and unchanged row/payload refusal. `api::tests::relational_explain_output` pins relational SQL EXPLAIN/ANALYZE column order; its `strict_append` module covers all three entrypoints. Owned by existing `//:hawdb_unit_general_tests` CI; focused replay: `cargo test --locked -p hawdb --lib -- result_column_order relational_explain_output`. Python DB-API regression coverage runs in its existing CI family. Cypher RETURN and Cypher diagnostic schemas remain tracked by [#1249](https://github.com/nowledge-co/hawdb/issues/1249). |
| Optimizer, optimizer-{graph,relational,predicate,vector}, plan-{core,cache,cypher}, explain | Rewrite legality, plan construction, cache/publication concurrency and observable plans. |
| Sparse composite prefix ranges | `//crates/optimizer-graph:hawdb_optimizer_graph_tests`, `//crates/storage:hawdb_storage_tests` and root `//:hawdb_unit_general_tests` cover [#1183](https://github.com/nowledge-co/hawdb/issues/1183): missing/NULL suffix values, one or multiple leading equalities, strict/inclusive bounds, checkpoint/delta/reopen and retained views. Storage differentials force owned/admitted access independently of planner selection, preserve Stop and admission refusal, and check native index work for complete keys. Unconstrained wider suffixes use a complete fallback; full-key ranges retain their existing path. The contiguous-prefix projection fixture uses a complete `(prefix, range)` key; candidate guards separately reject an unconstrained suffix. Existing profiles and lanes apply; fixtures stay small. Focused Cargo replay: `cargo test --locked -p hawdb-storage sparse_composite`, `cargo test --locked -p hawdb sparse_composite`, or optimizer-graph package filter `composite_range_candidates`. |
| SQL composite index intervals | Root `//:hawdb_unit_general_tests` (`api::tests::relational_range`), relational unit and storage unit / existing Prow crates lanes. Compare indexed SQL with an unindexed database across open/closed, one-sided, NULL/contradictory bounds, duplicates, aliases, parameters, residuals, uniform ASC/DESC, OFFSET/LIMIT and first-column ranges. Cover nullable unique suffixes, checkpoint/live/pinned/private/recovery views in Shadow/DemandPaged/Authoritative modes. A 5,000-row prefix must deliver a late LIMIT 3 under a four-row index budget with three observed index rows and early Stop. Tiny-page and scalar/encoded-bound differentials cover trailing-key ties and extrema. Replay: `cargo test --locked -p hawdb --lib api::tests::relational_range`, storage filters `encoded_partial_bounds` / `relational_index_shadow_range_seek`, and the relational index-runtime suite. See [contract](specs/POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md). No new grammar, persisted format, lane or profile. |
| Executor and query/query-policy | Operator execution, result delivery, cancellation and budget/refusal behavior. Small borrowed-projection units in the existing executor unit / Prow crates lane compare 1–8 simple columns with the general path, preserve duplicate-alias and computed/wide fallback, exact wide-row refusal and workspace refunds, and release preparation before a byte-boundary Stop without repeated evaluation. Replay: `cargo test --locked -p hawdb-executor --lib expression::projection::tests`. No new lane, feature, fixture deadline or profile. Text-seed result-owner unit cases cover prepaid window storage and the internal capacity-growth path, including simultaneous replacement admission, exact row limits, sticky refusal and parent-allowance retention. Allocation probes use the repository profile; compare measurements only within the same profile. |
| Complete lexical and grouped evidence | Existing Cypher/Plan-Cypher, executor, Search, readiness and root general lanes cover opt-in complete providers, exhaustion/skipped reports, candidate ceilings before canonical filtering, lazy capacity growth, scalar MAX and bounded per-group evidence. Root differentials cover mixed supported keys, signed zero, ID maxima, permutations, graph eligibility and retained reads across raw/resident/layered disk sources with replacement and deletion. Executor allocation probes cover ID admission, one owned copy of repeated wide scalar keys, retained callback ownership and cancellation/error precedence. Search regressions compare canonical field phrase-alias occurrences and full-corpus scores, including raw window/complete parity and actual allocation peaks. Sparse overlapping-layer regressions cover metadata predicates, external identities and exact live-corpus scores in window/complete modes. The `grouped_evidence` and `grouped_evidence_native` release benches manually qualify scalar and native lexical/graph paths; neither is a CI performance gate. See [measured results](benchmarks/GROUPED_RETRIEVAL_RESULTS.md). See [contract](specs/GROUPED_RETRIEVAL_SPEC.md). |
| Mixed numeric range projections | Existing storage unit (`//crates/storage:hawdb_storage_tests`) and root general (`//:hawdb_unit_general_tests`) CI owners. Scalar node, relationship and composite projection ranges preserve numeric predicates when Value ordering separates Int and Float. Reference envelopes cover clipped equality prefixes, strict/inclusive bounds, extreme values and NaN; homogeneous blocks must still prune. Persistent/API cases refresh bounded statistics and rebind after cold reopen to require native scalar/composite range plans, then cover delta type changes, pinned reads and reopen; the original reference and snapshot assertions remain intact. Replay filters: `mixed_numeric`, `composite_numeric_range_envelopes`. No lane, feature or profile change. |
| Storage descriptor bound traversal | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner. Forward and reverse inclusive-bound scans retain ordered delivery, tree-height/page/descriptor admission, early Stop and caller refusal. Reverse guards cover cross-leaf/group boundaries and binary upper-bound work on a verified warm page; skipped prefixes must not be visited. Replay: `cargo test --locked -p hawdb-storage --lib reverse_bound_scan` and `warm_page_views_reuse_integrity_proof`. No new profile or lane. |
| Composite ordered query and cache | Existing optimizer-graph / Prow crates and root general lanes cover exact two-direction keyset concatenation versus full canonical sorting, runtime capability, non-NULL typed unique suffix proofs, strict continuation, parameter numeric-shape changes, ordered prepared-plan invalidation versus unrelated point-plan reuse, ambiguous aliases, mixed directions, index availability across checkpoint, retained reads, moves/deletes/inserts, consumer error/retry and reopen. See [contract](specs/COMPOSITE_ORDERED_SEEK_SPEC.md). Plan-cache units verify used-binding type-proof ownership, unused-list omission and shape separation. Actual resident native plans and canonical checkpoint/delta/reopen plans are asserted separately; pre-checkpoint OutOfCore pages retain full semantic references without forcing a cost decision. Focused replay filters: `ordered_composite` and `parameter_type_proofs`. The opt-in `composite_ordered_seek` manual benchmark measures raw alternating samples, index work and admitted memory at shallow/deep continuations; it is not a CI performance gate or device-cold claim. Persistent preparation uses explicit Materialized mode before checkpoint/OutOfCore reopen. Fixture loading uses bounded transactions, followed by full uniqueness validation, index creation and an exact row-count preflight; complete setup cost is separate. See [source-bound read measurements](benchmarks/composite_ordered_seek_2026_10_23/README.md). No existing lane/profile changes. |
| Ordered composite projection and overlay | Existing storage unit / Prow crates owner. Immutable ordered pages compare with a complete two-direction reference, including global block ordinals, exclusive continuation, deep descriptor work, cold/full versus warm/logarithmic block decoding, verified-cache capacity refusal and admitted scratch refund after refusal/Stop. Store pages merge canonical base with moved/deleted/inserted delta keys across retained views, checkpoint and reopen; the low-level domain consists of complete non-NULL keys, with canonical-query coverage proved separately by planning. Explicit ordered checkpoints preserve cancellation without payload probes or work-unit charges. Replay filters: `composite_ordered_pages`, `ordered_composite`, `ordered_scratch_checkpoint`. No new CI lane or compilation profile. |
| Storage borrowed key intervals | Existing `//crates/storage:hawdb_storage_tests` / Prow crates ownership. Forward, reverse and alternating-end traversal matches a BTreeMap reference across immutable segment boundaries, empty/singleton/outside intervals and COW mutation with a retained snapshot. Replay: `cargo test --locked -p hawdb-storage --lib borrowed_key_interval`. No new feature/profile/lane or timed campaign. |
| Storage, integrity, cascades and artifact/bootstrap | WAL/checkpoint, encoded artifacts, indexes, corruption/refusal and startup ownership. Projection capture covers 65,537-node commits, oversized-preimage gaps, cold node/adjacency/relationship/spill capture under strict foreground descriptor pressure, nested resource/cancellation refusals versus physical corruption across canonical readers, and cancellation across grouped replay work. |
| Native projection keys and immutable reuse | Existing storage unit/native platform owners cover deleted native node candidates for built-in and custom labels, materialized/out-of-core residency, pinned feed end epochs, WAL failure/uncertain recovery, checkpoint and reopen (`cargo test --locked -p hawdb-storage --lib search_projection_capture`). The restart-safe change-log checkpoint case also retains the deleted native candidate together with its legacy document tombstone and verifies canonical absence after reopen (`cargo test --locked -p hawdb-storage --lib checkpoints_search_projection_changes_for_restart_safe_catch_up`). `captured_link` cases in `file_io::source_directory` cover native inode reuse after both labels move, occupied names, foreign project/source identities, component validation, Unix symlinks and platform descriptor refusal/retry. The recorder cases model lost unsynchronized links, before/after cuts, required target-directory barriers and retention after source unlink; they do not qualify complete Consumer selection. Replay with `cargo test --locked -p hawdb-storage --lib captured_link` and `cargo test --locked -p hawdb-storage --lib power_loss`. Windows replaces captured directory labels before opening descendant files; Unix retains replacement after file capture. A separate Windows link case attempts ancestor rename with an already-open file and verifies original-object linking and unlink custody after either success or native pre-effect refusal. All inode, foreign-name and recorder/barrier assertions remain. Windows strict Clippy also owns its native FileLinkInformation path. Registered Consumer installation uses these links only for sealed immutable producer output; arbitrary source installation retains independent copying. No lane, profile, source-format or durability-default change. |
| Search | Unit behavior plus nine explicit feature profiles; public streamed-document and source-policy lifecycle fixtures have independent owners below. |
| Default resident DiskANN | Existing Search unit targets and the unchanged vector feature profiles own `resident_diskann` (`cargo test --locked -p hawdb-search --lib resident_diskann`). They compare default filtered graph candidates with explicit exhaustive scoring across replacement, deletion, NULL and reinsertion, preserve original graph identity, and cover zero/signed-cosine behavior, non-finite-source refusal, offset pagination, hybrid fusion versus exact scoring, validated delta atomicity, checkpoint/reopen, typed memory/work refusal, cancellation and retry, and retained runtime allocation ownership through cache destruction. Concurrent admission blocking must not retain the cache lock, and initialization wait obeys the query deadline. Checkpoint cases require HDANN002, actual native graph execution, unchanged retained-reader bytes/results, and explicit RaBitQ settings across reopen/replacement. Reopen must reuse a verified mapping, remain queryable below the construction envelope, and keep its original baseline across CRUD and clean checkpoint copying. Corruption cases retain quarantine evidence and reject stale-source fallback. Directory replacement, stale cleanup and memory/cancellation fixtures retain both explicit RaBitQ and default graph coverage. Native registered source/MVCC and persistent artifact qualification have separate owners below. No CI lane or profile change. |
| Default out-of-core DiskANN | Existing Search targets and feature-matrix lanes own `generation_writer::tests::diskann` (`cargo test --locked -p hawdb-search --all-features --lib generation_writer::tests::diskann`). Filtered default queries compare with explicit exhaustive scoring after insert/replacement/delete/NULL deltas, with and without LSG. Guards check inherited topology settings, unchanged base graph bytes/native inode, retained old-reader results, abandoned-candidate rollback, cancellation and typed graph memory refusal, empty and deletion-only layers, legacy zero-cosine semantics across mixed graph/compatible layers, and refusal to satisfy explicit compression with a raw graph. Readiness also covers an empty-vector first segment, complete graph/visibility identity, exact live vector counts after mixed text/vector retractions, deletion-only and NULL-only heads, empty vector domains, old pins, cold reopen, and reuse of validated retraction counts during refresh. Mixed graph/legacy-codec closures cannot qualify as a pure graph. Explicit RaBitQ parameters also survive default delta preparation. The ordinary reader-refresh identity case checks model/version/dimension drift against the persisted graph binding. These are standalone projection segment-reuse cases; schema-bound canonical source-feed publication and power-loss qualification remain separate. Existing RaBitQ fixtures select their backend explicitly. No feature matrix, profile or lane is removed. See [delivery contract](specs/FILTERED_DISKANN_SPEC.md). |
| Analytics and vector-projection | Streaming algorithms, projected artifact IO and vector representation behavior. `//crates/vector-projection:hawdb_vector_projection_tests` also owns the Filtered DiskANN kernel/artifact family (`cargo test --locked -p hawdb-vector-projection --all-features diskann`; omit `--all-features` for standalone file I/O): Vamana full-beam/exhaustive distance parity, small-beam filtered recall against exhaustive ground truth, navigation without rejected-vector scoring, optional LSG and duplicate radii, deterministic construction, typed work/memory refusal, work-bounded workspace on a virtual large graph, and cancellation. File cases cover parameter/link/distance round trips, byte-identical mapped successor copying and cancelled-copy source retention, bounded header inspection, source/epoch/generation drift, truncation/trailing bytes/checksums, semantically invalid records with a matching checksum, admission refusal/retry and release of all charges. Core `projection_distance` tests also cover raw queries against unaligned byte-backed vectors and cooperative failure. Small synthetic fixtures use the repository profile; this is kernel/artifact evidence, not default-engine or database CRUD/MVCC qualification. See [delivery contract](specs/FILTERED_DISKANN_SPEC.md). |
| RaBitQ SIMD scoring | Existing `//crates/vector-projection:hawdb_vector_projection_tests` and native vector-platform CI own bitwise scalar/AVX2/NEON parity, CPU selection, all code values, unaligned and short inputs, dimension tails, filtered memory/file scans, ties and worker merging. Replay: `cargo test --locked -p hawdb-vector-projection kernel::`. The two ignored `simd_` release probes are local-only throughput and deadline-response measurements, run serially. The existing `rabitq_query_profile` example adds a local `compare` mode for interleaved scalar/native-SIMD complete-query measurements over 100,000 vectors, including raw-vector reranking and hydration with exact final-result parity. These probes do not replace native AArch64 qualification. See [method and limitations](RABITQ_VECTOR_PROJECTION_BENCHMARK.md). No CI lane, profile or fuzz job is added. |
| QoS, cgroup, telemetry, resource-profile and runtime-tokio | Resource detection/admission, metrics and synchronous/async runtime boundaries. |
| Readiness, evidence, route-ownership, nowledge-contracts, compat and qualification | Typed readiness, complete evidence identity, compatibility and host integration contracts. |
| `crates/fuzz-contracts` | Production readiness contract values only. Its fast unit tests are not the Shiro generator/campaign. |
| Python DB-API (`bindings/python/tests/test_dbapi.py`) | The PEP 249 layer in `hawdb.dbapi`: module globals and the PEP 249 exception hierarchy, with every engine error and every built-in error from statement parameters raised as its PEP 249 class while keeping its own type, attributes and pickling, or raised unchanged if it cannot be rebuilt as that class; tuple rows in SELECT-list order, a `description` with type codes, also for a key lookup that matches nothing, and identity-hashed type objects; lazily begun connection transactions with commit, rollback, close and context-manager semantics; autocommit set at connect or between transactions, read-only and branch-command routing; `executemany` delegation and the aborted state after a failed batch; parameter containers matching the native rule; and refusal of closed objects and of other threads, including reused thread idents. Owned by `//bindings/python:hawdb_python_tests` and the Python Bindings pip job. Engine transaction semantics run on file and in-memory databases, durability, read-only and branch cases on a file database, and the rest in memory. The engine's column order per query shape belongs to the SQL result column order family. Catalog queries also pin tuple order and empty-result descriptions under both autocommit and transactions; SQL EXPLAIN and EXPLAIN ANALYZE pin their diagnostic column and tuple order. The `pandas.read_sql` smoke skips without pandas; the pip job installs a pinned pandas and selects it with `-k pandas`. Replay: `pytest tests/test_dbapi.py` in `bindings/python` with the wheel installed. |

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
recovery cases in five required shards: births, unselected intents, selected
intents, lost relaxed SQL sources and lost relaxed node sources. The first
three retain 28 combinations each; the final pair retain 14 each, for the same
112 combinations. Linux/macOS retain the complete family in one process.
The Windows `remaining` and ordinary consumer selections exclude this module
because the five exact selections own it. A shared selector validates actual
Rust discovery and both workflow matrices; a runtime coverage check requires
every `(origin, cut, durability, source family, source-updated)` combination
exactly once. All fault plans, physical-image deduplication, two repeated reopens,
single-process test concurrency, compiler profiles and 120-minute job ceilings
remain unchanged. Native runner negative controls reject missing coverage even
when the Rust test itself reports success. The former combined Windows lane
reached its 120-minute ceiling after only 34 recovery combinations; isolated
first-build and execution costs require final-head CI evidence. Replay the
selector through `bazel test //:power_loss_partition_script_test` and the runner
controls with `bash scripts/run-windows-native-bazel.test.sh`.
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
| `hawdb_search_io_wave_tests` and `hawdb_search_<profile>_io_wave_tests` | Complete standalone writer Task/Wave admission, idle release, cancellation and cleanup contracts | All nine unit feature profiles reuse their existing executable. Required crate presubmits run this family with one libtest thread and Bazel `exclusive`; no additional Rust executable or compilation profile. The runner rejects empty discovery and overlapping skips and preserves caller arguments and failures. The existing 20-second task deadlines and enclosing target timeouts are unchanged. Replay: `bazel test //crates/search:hawdb_search_text_background_io_wave_tests`; the runner itself is covered by `//crates/search:search_unit_family_script_test`. |
| Search BM25 decode and filter reuse (`lexical_projection::`, `text_seed`) | Reusable posting buffers, corrupt-frame rollback, stable internal predicates, arbitrary callback semantics, exact metadata routing and memory ownership | Existing Search unit feature-matrix owners and repository checked profile; public text-seed cases require `full-text-search`. Allocation probes serialize through the existing measurement gate and cover 8/128/4096 postings; filter cases cover false results, cancellation, complete/window counts and sparse overlapping layers. Local replay: `bazel test //crates/search:hawdb_search_tests --test_arg=lexical_projection:: --test_arg=text_seed --test_arg=--nocapture`. Existing ignored differential campaigns remain local-only. Ref [#1206](https://github.com/nowledge-co/hawdb/issues/1206). |
| Search BM25 cursor selection (`lexical_projection::tests::cursor_selection`) | Exact score bits, tie-breaking, exhaustive match counts and callback order across empty/unknown and 1/4/16/32-term queries; pruning eligibility before the rank window fills | Existing Search unit feature-matrix owners and repository checked profile; existing pruning regressions cover active WAND and overlays. Replay: `bazel test //crates/search:hawdb_search_tests --test_arg=lexical_projection::`. The ignored 4096-document warmed timing probe is local-only: `cargo test --locked -p hawdb-search --lib cursor_selection_query_measurement -- --ignored --nocapture --test-threads=1`; two warmups and 21 repetitions per query, including statistics reads, excluding fixture construction. Ref [#1208](https://github.com/nowledge-co/hawdb/issues/1208). |
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
fresh admitted retry. Existing prepared/FD cleanup fixtures explicitly declare
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
Bindings `pytest` step. They cover missing parents, read-only refusal, regular
files and non-directory ancestors, `":memory:"` and empty paths (including an
existing project as the current directory), with rejected opens leaving files
and directories intact. POSIX permission cases preserve the underlying error
and skip only when the user can bypass directory permissions. Native handles
retain absolute paths across `chdir`, preserve symlink spelling on POSIX, and
reopen persisted data. The parent check remains non-atomic with engine open.
No lane, feature or profile change.

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

- `//:hawdb_unit_checkpoint_headroom_tests` owns the complete
  `api::automatic_checkpoint::tests::headroom::` namespace and the two sustained
  `delta_pressure_sustains_generations_and_preserves_all_values_after_reopen`
  and `soft_wal_pressure_sustains_multiple_generations_without_host_checkpoints`
  cases, including capacity,
  notification, foreground and preparation-backoff submodules. It is included
  through the existing required root unit suites and excluded from general by
  the same inventory partitioner. It reuses the ordinary root executable and
  global checked profile, runs with one libtest thread and Bazel `exclusive`,
  and keeps the existing 900-second partition deadline. Internal test-created
  concurrency remains intact. Automatic-checkpoint headroom CREATE failures retain the original success/refusal contract and add phase, actual work-progress, wake/resource and libtest-concurrency diagnostics for [#1116](https://github.com/nowledge-co/hawdb/issues/1116); no caller retries or deadline changes. This prevents the phase/clock assertions from
  competing with roughly 1,800 unrelated unit cases and other test actions
  in the same Bazel invocation; external node contention remains possible.
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
  `bazel test //:hawdb_unit_checkpoint_headroom_tests`; no CI-only flags or
  special compiler profile are required. Final-head CI must establish runtime
  acceptance; target isolation alone is not a measured speedup.

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

The native workflow runs twelve independent Bazel shards and a separate strict
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

- `scripts/check-license-headers.test.py` is a local pinned-HawkEye fixture
  check for full/SPDX forms, Python/Shell shebangs, excluded/unsupported files,
  mixed valid/invalid batches, wrong owners/licenses and byte preservation.
  The staged-file hook uses the same HawkEye rules for both header templates.
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
comparisons. Bidirectional one/two-hop checks use fixed one-hop edges per pattern;
the frontend intentionally rejects non-outgoing variable-length patterns even
for missing endpoints or unknown relationship types. These syntax repairs retain
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
`src/nowledge_mem/read_execution.rs` and `src/graph_route_evidence_tests.rs` under
the existing root library test targets.

### Pinned full-scan input cost

The optimizer relational unit target (`//crates/optimizer-relational:hawdb_optimizer_relational_tests`;
`cargo test --locked -p hawdb-optimizer-relational --lib`) owns candidate-cap
independence, snapshot-size monotonicity, selective covering-index selection,
and unchanged point/default-context controls. These are checked-profile unit
cases in the existing crate lane; no fixture, feature matrix, timeout or
execution lane changes. See [the cost model](OPTIMIZER_COST_MODEL.md).


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
linear malformed recursion,
fatal work/storage/depth budgets, capture rollback with memo reuse, and exact
entry/edge accounting across direct and sparse memo storage. The existing
`postgres_pgq_tests` and `postgres_select_pgq_tests` targets own syntax ASTs,
graph nesting, aliases,
joins, byte spans and composed GRAPH_TABLE rejection. All use the ordinary
checked profile and existing SQL syntax CI lanes.

`//crates/sql:hawdb_sql_tests` retains the frozen production/owned frontend
corpus and PGQ binder/lowering tests. It must preserve the recorded acceptance
and rejection codes; production relational SQL still uses `sqlparser`.
Local Shiro parser campaigns and
`//crates/sql-syntax:hawdb_sql_alias_boundary_fuzz_tests` remain manual/local.
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
