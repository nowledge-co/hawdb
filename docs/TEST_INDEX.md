# Test ownership and known slow paths

Use this index to select an existing regression owner and understand what a
slow target is checking. Target definitions and CI job configuration remain
authoritative; this is a map of test families, not an exhaustive list of every
Rust test function. Update it when changing a family's scope, build profile,
execution lane, fixture size, isolation or known failure mode.

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

| Target / family | What it detects | Notes |
| --- | --- | --- |
| `//:hawdb_manual_checkpoint_tests`, `//:hawdb_manual_checkpoint_minimal_tests` | Explicit ordinary/concurrent/embedded checkpoints preserve durable publication independently of background support/admission. Embedded controls retain one same-governor background or foreground I/O permit, distinguish allowed foreground work from legitimate refusal, and check reopen and refunds. | Existing default/minimal public-consumer carriers and profiles; replay `cargo test --locked --test manual_checkpoint`, adding `--no-default-features` for minimal. The existing root automatic-checkpoint `publication_io` family also pauses the actual owner with its acquired wave, observes manual suspension, and checks durable completion/refunds under [#1219](https://github.com/nowledge-co/hawdb/issues/1219). |
| `//:hawdb_unit_general_tests` | Embedded API, schema/query and general facade regressions | Shared unit executable; partition script assigns each discovered case an owner. |
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
| Storage WAL admission (`store::durable::wal::tests`) | Healthy mutation admission avoids generation-directory inventory; refusal retains exact pressure measurements and reasons | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner. Two pure cases cover lazy observation and 1,536 pressure combinations, including integrity, WAL/delta, free-space, cache and retained-reader states. Public pressure reports and durability/namespace checks retain their existing owners. |
| Storage source-sync and captured-WAL units | Non-strict source-barrier progress versus uncertain durability failure | Source barriers complete under strict foreground descriptor pressure without losing the live handle; captured WAL append prepays native validation handles before writing even when a competing owner fills remaining capacity. The latter also executes explicitly in the native Windows contract lane. |
| Storage WAL identity-cache and source-directory namespace units | Reuse pinned descriptor identity while preserving fresh namespace lookup, rotation, symlink retargeting and strict admission | Existing storage unit owner. Unix-only cache coverage complements captured-WAL replacement, uncertain append, rollback and power-loss tests; Windows keeps the existing native identity path. |
| Storage `file_descriptors::tests::capacity_policy` and root `api::tests::descriptor_capacity` | Soft capacity with pinned readers, strict foreground refusal, non-strict internal progress, joined-worker policy handoff and accounting retirement | Existing storage/root checked-profile owners; deterministic small fixtures. Refusal cases explicitly select strict mode. Both project constructors and the database facade preserve typed strict/soft configuration conflicts, the active policy and counts; a policy can change after all previous owners close. The existing compat protocol-server unit lane round-trips both policy-conflict directions and rejects non-boolean fields. Unix subprocesses lower only their own soft limit to verify the shared process OS allowance across three project domains, namespace/standalone accounting, active/retired prepaid-slot ownership, atomic final-slot admission by competing projects, failed-open retirement, typed pre-effect refusal, unchanged WAL bytes, same-handle retry and reopen; native exhaustion remains a separate Linux test. The default full-budget reservation test also uses an isolated Unix process so it cannot consume another test's process allowance. Namespace-probe fixtures verify independent-project and alias isolation, initializing standard-library OS error formatting before measuring exact native handle retirement on every iteration. Native Bindings `windows-native-contracts` runs the complete descriptor family and facade capacity tests, records source/toolchain and retains output; native Windows execution remains distinct from cross-compilation. Metadata/bootstrap, ownership-lock, sidecar publication, source synchronization, change capture and search/initializer cleanup regressions cover internal progress or native failure retirement at occupied strict capacity. Change capture retains its preimage byte limits and complete replay assertions. See [descriptor capacity](FILE_DESCRIPTOR_CAPACITY.md). |
| Default/minimal/graph-text consumer, host allocator and portable tests | Public feature boundaries, allocator ownership, in-memory portability and excluded native dependencies | Do not replace exact minimal-feature compilation with a default-feature run. |
| Scoped Consumer preparation (`cargo test --locked -p hawdb --all-features --test mvcc_staging_session registered_consumer_`) | Real writer commits between capture, reservation, construction and selection; repeated durable watermarks; graph/SQL/schema/source fences; reserved/prepared drop recovery and cold reopen | Root integration lane owns the tests. The split API is described in [Consumer progress and retention](CONSUMER_PROGRESS_AND_RETENTION.md); manual `consumer_publication` timing also records commits during native construction. |
| Read-only conflict history (`cargo test --locked -p hawdb-storage --lib snapshot_retention_tests`) | Long readers release unrelated stamps; stale read-to-write upgrades fail closed; writable savepoints preserve delete/reinsert conflicts; snapshot allocation accounting | Storage unit lane owns the tests. Read data and dependency/write validation pins have separate lifetimes. |
| Cross-crate FD refusal and owned cleanup (`spill::tests`, relational `descriptor_exhaustion`, vector `artifact::descriptor_tests`, Search descriptor/root-registry/stage-cleanup families, storage control-candidate and root derived-repair units) | Strict foreground rejection versus internal progress, native cleanup failure, lock custody and exact retry | Existing crate/root checked-profile CI owners and the complete Search feature matrix. Foreground refusal fixtures explicitly select strict mode, and repair metadata/database owners share that policy. Pressure loops stop at the configured target rather than running to OS exhaustion. Abandoned projection writers, private stages, stationary producers and publication locks must progress at occupied strict capacity while preserving pinned readers and peer exclusion. Captured publication leases use bounded directory duplicates, preserve foreign lock-path evidence on native failure, release failed registration, and retry under strict pressure; background-only/vector-only matrices must not use native process exhaustion as the fixture. Directory blockers or injected native unlink failures retain cleanup debt, bytes, host charges and retry dispositions; cancellation/memory refusal and root-alias/foreign-root registry isolation remain covered. Native Windows selects storage control-candidate, stationary Consumer and captured-publication lease families explicitly; the lease cases reuse the existing all-feature Search test binary and retain their own evidence file. No feature matrix, fixture, assertion budget or timeout is reduced. |
| Consumer physical reclamation (`cargo test --locked -p hawdb --all-features --lib consumer_reclamation`) | Old snapshot and initial-lease protection, durable branch and pending-operation roots, bounded retries, unknown-evidence retention, FD/memory release and ledger retirement before unlink | Seven lifecycle cases belong to the root unit lane, including cleanup and retry at occupied strict foreground FD capacity; one recorder case belongs to the power-loss family. The recorder fixture and database opens explicitly share the strict 32-descriptor configuration. The Windows native contract lane selects all eight explicitly. The recorder explores lost/torn/reordered uncovered writes at the first unlink; close/reopen fixtures cover partial retirement but do not qualify the complete platform power-loss matrix. See [cleanup ownership](SEARCH_CLEANUP_OWNERSHIP.md). |
| Publication-owner admission (`api::publication::tests`) | A waiting admitted reader precedes later writers, an admitted writer precedes later readers, and reader callbacks remain concurrent | Existing root unit lane, ordinary profile. Channel-controlled cases inspect only the private acquisition gate; they do not claim platform-wide FIFO fairness. Validation/publication exclusion and error/retry tests retain their existing ownership. No additional CI job. |
| Registered covered-token reclamation (`registered_consumer_reclamation_preserves_continuously_invalidated_covered_base`) and manual `consumer_publication` benchmark | Actual post-E writer commits while preparation is paused, typed refusal and retained base during GC; outer publication-owner latency and watermark progress under 1/4/8 writers | Regression: default/minimal `mvcc_staging_session` and native Windows registered selection. Benchmark: manual only, repository bench profile, disabled/legacy/covered/scoped-blocking/detached controls and raw p95/p99 samples. The opt-in `--sustained true` detached workload continues publication until writers finish or its explicit attempt ceiling, with checkpoints every 16,384 foreground writes and retained checkpoint samples. Its finite-progress qualification is separate from latency controls and the original strict writer gate. Manual `python3 scripts/measure-consumer-publication.test.py` checks executable/profile matching and actual feature provenance; no default CI registration. The separate opt-in `--with-readers` driver cohort adds one pinned canonical reader per writer and phase-boundary Linux process RSS/high-water/CPU-tick/I/O observations. It checks every document identity/content and retains per-request owner wait and preparation-overlap intervals. Missing resources, zero reads or drain-only reads cannot qualify; process high-water marks do not prove phase heap ownership. Reader measurement remains manual and does not change the completed writer-only cohorts. Whole-Database post-E dependencies retain conservative refusals; Domains-scoped finite progress is qualified separately. The helper records intake/mapping call counts with a `2 * feed.items + 1` host bound for its Done-only/no-delta mapper, without changing feed/mapping resources. A 60,000-update manual regression exercises complete coverage and native publication past the old 100,000-call bound; no new CI job. Quiescent drains are not sustained progress. [Workload and qualification boundaries](CONSUMER_PUBLICATION_QUALIFICATION.md). |
| `//:hawdb_system_sql_loom_tests` | Bounded modeled system-table publication interleavings | Model exploration; not a wall-clock fuzz campaign. |
| Package parity, query-limit inventory and root-partition script tests | Missing package ownership, unbudgeted query routes, duplicate or omitted test partition selection | Keep these structural checks when moving targets. |

Brace notation describes several concrete labels; expand it before invoking
Bazel. A family description does not imply every leaf belongs to every lane.

### Automatic-checkpoint frontend deadline controls

`//:hawdb_unit_general_tests` owns
`api::automatic_checkpoint::tests::headroom` in the default-feature root CI lane
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
| SQL result column order | `//crates/executor:hawdb_executor_unit_tests` covers named rows ordered by a declared schema, empty results and rejected column drift. Root `//:hawdb_unit_general_tests` (`relational_sql::tests`) pins each shape's EXPLAIN plan and runs it with and without matching rows: borrowed scans, key and index lookups, inner and LEFT joins, scalar functions, index-ordered, top-N (on projected and non-projected columns, also over inner and LEFT joins), DISTINCT and alias-ordered projection, wildcards over top-N, DISTINCT and LEFT joins, LIMIT 0, and columnar, row-at-a-time and grouped aggregates, including HAVING and joins. A cost-reordered wildcard join and strict-append SELECT also run with and without rows; FROM-less SELECT and COUNT DISTINCT always return one row. Repeated output names and the reserved `__hawdb_relational_sort_` prefix fail during preparation, including EXPLAIN. `crates/qualification` `evidence_digest::tests` keeps result digests independent of column order, matching name-keyed reference rows. The Python binding reports these schemas unchanged as `QueryResult.columns`; `tests/pg-regress` sorts column names and does not cover order. Existing CI lanes apply. Replay: `cargo test --locked -p hawdb-executor --lib named_rows_follow_the_declared_schema` and `cargo test --locked -p hawdb --lib -- relational_sql::tests::database_sql_ relational_sql::tests::unordered_and_wildcard_joins`. EXPLAIN, system catalogs, Cypher RETURN and the vector-index SQL streaming callback, which receives name-keyed maps, are outside this family. |
| Optimizer, optimizer-{graph,relational,predicate,vector}, plan-{core,cache,cypher}, explain | Rewrite legality, plan construction, cache/publication concurrency and observable plans. |
| Sparse composite prefix ranges | `//crates/optimizer-graph:hawdb_optimizer_graph_tests`, `//crates/storage:hawdb_storage_tests` and root `//:hawdb_unit_general_tests` cover [#1183](https://github.com/nowledge-co/hawdb/issues/1183): missing/NULL suffix values, one or multiple leading equalities, strict/inclusive bounds, checkpoint/delta/reopen and retained views. Storage differentials force owned/admitted access independently of planner selection, preserve Stop and admission refusal, and check native index work for complete keys. Unconstrained wider suffixes use a complete fallback; full-key ranges retain their existing path. The contiguous-prefix projection fixture uses a complete `(prefix, range)` key; candidate guards separately reject an unconstrained suffix. Existing profiles and lanes apply; fixtures stay small. Focused Cargo replay: `cargo test --locked -p hawdb-storage sparse_composite`, `cargo test --locked -p hawdb sparse_composite`, or optimizer-graph package filter `composite_range_candidates`. |
| Executor and query/query-policy | Operator execution, result delivery, cancellation and budget/refusal behavior. Small borrowed-projection units in the existing executor unit / Prow crates lane compare 1–8 simple columns with the general path, preserve duplicate-alias and computed/wide fallback, exact wide-row refusal and workspace refunds, and release preparation before a byte-boundary Stop without repeated evaluation. Replay: `cargo test --locked -p hawdb-executor --lib expression::projection::tests`. No new lane, feature, fixture deadline or profile. Text-seed result-owner unit cases cover prepaid window storage and the internal capacity-growth path, including simultaneous replacement admission, exact row limits, sticky refusal and parent-allowance retention. Allocation probes use the repository profile; compare measurements only within the same profile. |
| Single-column sort-key preparation | Existing executor unit / Prow crates owner covers 64-bit inline integer keys, mixed Value ordering, stable ties, exact tuple/payload admission and refunds, missing/NULL columns, cancellation, and the general multi-key fallback. Minimal browser WASM keeps owned keys. Local replay: `cargo test --locked -p hawdb-executor --lib inline_integer_sort` and filter `sort_run_single_column`. The manual `concurrent_writers --sort-key-control` release mode compares integer, float, 256-byte string, NULL and mixed keys at 256/4096 rows against a bounded stable canonical reference. One labelled warmup and eight retained queries per case use one pinned snapshot; full typed output and physical Sort/Column shape must match. The [control protocol](benchmarks/SORT_KEY_PREPARATION_PROTOCOL.md) retains 40 process pairs and a warmup pair. These read-only controls do not replace the complete concurrent publication performance matrix. No scheduled/default lane is added. |
| Complete lexical and grouped evidence | Existing Cypher/Plan-Cypher, executor, Search, readiness and root general lanes cover opt-in complete providers, exhaustion/skipped reports, candidate ceilings before canonical filtering, lazy capacity growth, scalar MAX and bounded per-group evidence. Root differentials cover mixed supported keys, signed zero, ID maxima, permutations, graph eligibility and retained reads across raw/resident/layered disk sources with replacement and deletion. Executor allocation probes cover ID admission, one owned copy of repeated wide scalar keys, retained callback ownership and cancellation/error precedence. Search regressions compare canonical field phrase-alias occurrences and full-corpus scores, including raw window/complete parity and actual allocation peaks. Sparse overlapping-layer regressions cover metadata predicates, external identities and exact live-corpus scores in window/complete modes. The `grouped_evidence` and `grouped_evidence_native` release benches manually qualify scalar and native lexical/graph paths; neither is a CI performance gate. See [measured results](benchmarks/GROUPED_RETRIEVAL_RESULTS.md). See [contract](specs/GROUPED_RETRIEVAL_SPEC.md). |
| Mixed numeric range projections | Existing storage unit (`//crates/storage:hawdb_storage_tests`) and root general (`//:hawdb_unit_general_tests`) CI owners. Scalar node, relationship and composite projection ranges preserve numeric predicates when Value ordering separates Int and Float. Reference envelopes cover clipped equality prefixes, strict/inclusive bounds, extreme values and NaN; homogeneous blocks must still prune. Persistent/API cases refresh bounded statistics and rebind after cold reopen to require native scalar/composite range plans, then cover delta type changes, pinned reads and reopen; the original reference and snapshot assertions remain intact. Replay filters: `mixed_numeric`, `composite_numeric_range_envelopes`. No lane, feature or profile change. |
| Storage descriptor bound traversal | Existing `//crates/storage:hawdb_storage_tests` / Prow crates owner. Forward and reverse inclusive-bound scans retain ordered delivery, tree-height/page/descriptor admission, early Stop and caller refusal. Reverse guards cover cross-leaf/group boundaries and binary upper-bound work on a verified warm page; skipped prefixes must not be visited. Replay: `cargo test --locked -p hawdb-storage --lib reverse_bound_scan` and `warm_page_views_reuse_integrity_proof`. No new profile or lane. |
| Composite ordered query and cache | Existing optimizer-graph / Prow crates and root general lanes cover exact two-direction keyset concatenation versus full canonical sorting, runtime capability, non-NULL typed unique suffix proofs, strict continuation, parameter numeric-shape changes, ordered prepared-plan invalidation versus unrelated point-plan reuse, ambiguous aliases, mixed directions, index availability across checkpoint, retained reads, moves/deletes/inserts, consumer error/retry and reopen. See [contract](specs/COMPOSITE_ORDERED_SEEK_SPEC.md). Plan-cache units verify used-binding type-proof ownership, unused-list omission and shape separation. Actual resident native plans and canonical checkpoint/delta/reopen plans are asserted separately; pre-checkpoint OutOfCore pages retain full semantic references without forcing a cost decision. Focused replay filters: `ordered_composite` and `parameter_type_proofs`. The opt-in `composite_ordered_seek` manual benchmark measures raw alternating samples, index work and admitted memory at shallow/deep continuations; it is not a CI performance gate or device-cold claim. Persistent preparation uses explicit Materialized mode before checkpoint/OutOfCore reopen. Fixture loading uses bounded transactions, followed by full uniqueness validation, index creation and an exact row-count preflight; complete setup cost is separate. See [source-bound read measurements](benchmarks/composite_ordered_seek_2026_10_23/README.md). No existing lane/profile changes. |
| Ordered composite projection and overlay | Existing storage unit / Prow crates owner. Immutable ordered pages compare with a complete two-direction reference, including global block ordinals, exclusive continuation, deep descriptor work, cold/full versus warm/logarithmic block decoding, verified-cache capacity refusal and admitted scratch refund after refusal/Stop. Store pages merge canonical base with moved/deleted/inserted delta keys across retained views, checkpoint and reopen; the low-level domain consists of complete non-NULL keys, with canonical-query coverage proved separately by planning. Explicit ordered checkpoints preserve cancellation without payload probes or work-unit charges. Replay filters: `composite_ordered_pages`, `ordered_composite`, `ordered_scratch_checkpoint`. No new CI lane or compilation profile. |
| Storage borrowed key intervals | Existing `//crates/storage:hawdb_storage_tests` / Prow crates ownership. Forward, reverse and alternating-end traversal matches a BTreeMap reference across immutable segment boundaries, empty/singleton/outside intervals and COW mutation with a retained snapshot. Replay: `cargo test --locked -p hawdb-storage --lib borrowed_key_interval`. No new feature/profile/lane or timed campaign. |
| Storage, integrity, cascades and artifact/bootstrap | WAL/checkpoint, encoded artifacts, indexes, corruption/refusal and startup ownership. Projection capture covers 65,537-node commits, oversized-preimage gaps, cold node/adjacency/relationship/spill capture under strict foreground descriptor pressure, nested resource/cancellation refusals versus physical corruption across canonical readers, and cancellation across grouped replay work. |
| Search | Unit behavior plus nine explicit feature profiles; public streamed-document and source-policy lifecycle fixtures have independent owners below. |
| Analytics and vector-projection | Streaming algorithms, projected artifact IO and vector representation behavior. |
| QoS, cgroup, telemetry, resource-profile and runtime-tokio | Resource detection/admission, metrics and synchronous/async runtime boundaries. |
| Readiness, evidence, route-ownership, nowledge-contracts, compat and qualification | Typed readiness, complete evidence identity, compatibility and host integration contracts. |
| `crates/fuzz-contracts` | Production readiness contract values only. Its fast unit tests are not the Shiro generator/campaign. |
| Python DB-API (`bindings/python/tests/test_dbapi.py`) | The PEP 249 layer in `hawdb.dbapi`: module globals and the PEP 249 exception hierarchy, with every engine error and every built-in error from statement parameters raised as its PEP 249 class while keeping its own type, attributes and pickling; tuple rows, `description` type codes and identity-hashed type objects; lazily begun connection transactions with commit, rollback, close and context-manager semantics; autocommit set at connect or between transactions, read-only and branch-command routing; `executemany` delegation and the aborted state after a failed batch; parameter containers matching the native rule; and refusal of closed objects and of other threads, including reused thread idents. Owned by `//bindings/python:hawdb_python_tests` and the Python Bindings pip job. Engine transaction semantics run on file and in-memory databases, durability, read-only and branch cases on a file database, and the rest in memory. Two non-strict `xfail(raises=AssertionError)` cases pin the engine's name-sorted columns and missing zero-row schema for key lookups ([#1225](https://github.com/nowledge-co/hawdb/issues/1225)). The Python workflow does not run on engine changes, so an engine fix shows up there as XPASS; remove the markers and the documented caveats then. The `pandas.read_sql` smoke skips without pandas; the pip job installs a pinned pandas and selects it with `-k pandas`. Replay: `pytest tests/test_dbapi.py` in `bindings/python` with the wheel installed. |

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

## Search feature and lifecycle matrix

See [SEARCH_FEATURE_TESTS.md](SEARCH_FEATURE_TESTS.md) for feature-specific
expectations. The concrete profile set is default, acl, minimal, text_only,
vector_only, background_only, text_vector, text_background and vector_background.

| Target pattern | What it detects | Cost and known issues |
| --- | --- | --- |
| `//crates/search:hawdb_search_tests` and `hawdb_search_<profile>_tests` | Search unit, persistence/recovery and feature capability behavior | Large lifecycle fixtures include 128-MiB sources. The three admitted-root heap-reclamation probes run in exact-case child processes because staging-registry nodes are shared across operations; all peak and final-zero assertions remain. Repository test profile; the full matrix still runs. Existing deadlines remain unchanged. |
| Search BM25 decode and filter reuse (`lexical_projection::`, `text_seed`) | Reusable posting buffers, corrupt-frame rollback, stable internal predicates, arbitrary callback semantics, exact metadata routing and memory ownership | Existing Search unit feature-matrix owners and repository checked profile; public text-seed cases require `full-text-search`. Allocation probes serialize through the existing measurement gate and cover 8/128/4096 postings; filter cases cover false results, cancellation, complete/window counts and sparse overlapping layers. Local replay: `bazel test //crates/search:hawdb_search_tests --test_arg=lexical_projection:: --test_arg=text_seed --test_arg=--nocapture`. Existing ignored differential campaigns remain local-only. Ref [#1206](https://github.com/nowledge-co/hawdb/issues/1206). |
| Search BM25 cursor selection (`lexical_projection::tests::cursor_selection`) | Exact score bits, tie-breaking, exhaustive match counts and callback order across empty/unknown and 1/4/16/32-term queries; pruning eligibility before the rank window fills | Existing Search unit feature-matrix owners and repository checked profile; existing pruning regressions cover active WAND and overlays. Replay: `bazel test //crates/search:hawdb_search_tests --test_arg=lexical_projection::`. The ignored 4096-document warmed timing probe is local-only: `cargo test --locked -p hawdb-search --lib cursor_selection_query_measurement -- --ignored --nocapture --test-threads=1`; two warmups and 21 repetitions per query, including statistics reads, excluding fixture construction. Ref [#1208](https://github.com/nowledge-co/hawdb/issues/1208). |
| `hawdb_search_source_policy_tests` and `hawdb_search_<profile>_source_policy_tests` | Inherited source/token admission policy through build, replacement, compaction, deletion and reopen; valid over-default-budget reader bodies | Million-token boundaries intentionally remain large. Five FTS profiles previously exceeded 900s: [#1034](https://github.com/nowledge-co/hawdb/issues/1034). Non-FTS decomposition: [#872](https://github.com/nowledge-co/hawdb/issues/872). |
| `hawdb_search_streamed_lifecycle_tests` and FTS profile variants | 32-MiB streamed bodies through projection update, replay, compaction/retraction and budget refusal | Five complete FTS feature profiles. Reuse the same feature-profile library; compilation is uniform across the repo. |
| Allocation-specific integration targets | Identifier, descriptor, segment, analyzer and related allocation accounting | Uses the same repository profile; preserve measurement assumptions and compare allocation evidence after profile changes. |

### Standalone Search writer I/O waves

The existing Search crate unit target owns
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
No CI lane/profile/timeout/fixture scope changes. See [the admission contract](SEARCH_WRITER_IO_ADMISSION.md).

## CI benchmark smoke owners

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
workspace, independent tools, bindings and the parser experiment. Run Cargo from
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

Python `open` path checks (`bindings/python/tests/test_file.py` and
`test_memory.py`) run in `//bindings/python:hawdb_python_tests` and the Python
Bindings `pytest` step. A bare relative name opens or creates the project in
the current directory, `":memory:"` is refused, and a missing parent is refused
before any directory is created. No lane, feature or profile change.

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
| PostgreSQL upstream SQL comparison | [`tests/pg-regress`](../tests/pg-regress/README.md), `make pg-regress`; Rust `libpq` reference client; typed rows, NULL, duplicate multiplicity, ordering and DML counts against a real 18.0 server | Manual only; pinned selected Boolean/int8 cases; unsupported source statements are not coverage. Stock expected files are provenance, not substituted for live reference execution. |
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

## PR #1217 root CI scheduling

- `//:hawdb_unit_checkpoint_headroom_tests` owns the complete
  `api::automatic_checkpoint::tests::headroom::` namespace, including capacity,
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

## Windows native contract ownership

The native workflow runs seven independent Bazel shards and a separate strict
Cargo Clippy job. The original `Windows native contracts` check requires every
result to pass. See [Windows native CI](WINDOWS_NATIVE_CI.md) for the complete
selection/profile/count matrix, cache policy, cold-build costs and local replay.
The daily/manual abandonment lane remains separate and requires eight passing
cases with the selected 1/2/4/8-thread setting. No fuzz campaign is added.

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

### Pinned full-scan input cost

The optimizer relational unit target (`//crates/optimizer-relational:hawdb_optimizer_relational_tests`;
`cargo test --locked -p hawdb-optimizer-relational --lib`) owns candidate-cap
independence, snapshot-size monotonicity, selective covering-index selection,
and unchanged point/default-context controls. These are checked-profile unit
cases in the existing crate lane; no fixture, feature matrix, timeout or
execution lane changes. See [the cost model](OPTIMIZER_COST_MODEL.md).
