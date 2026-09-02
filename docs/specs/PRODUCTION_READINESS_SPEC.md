# Skein Production Readiness Specification

## Scope

This specification defines the evidence required before a Skein build may
serve production traffic or replace an existing Nowledge graph or search read
owner. It complements the runtime correctness contract in
`EMBEDDED_RUNTIME_SPEC.md`; passing unit tests or implementing an API does not
by itself satisfy this specification.

Skein is an embedded Rust library. Production traffic, readiness collection,
and cutover decisions MUST use typed Rust APIs. Command-line programs MAY
render or transport the same reports for development and CI, but they MUST NOT
be required by the production serving path.

## Readiness States

Readiness is divided into three non-interchangeable states:

1. **development ready**: the capability is implemented and its deterministic
   unit, integration, and model tests pass;
2. **traffic ready**: the exact production build and configuration pass
   recovery, resource, enabled-feature, and platform qualification on a
   representative replica;
3. **cutover ready**: every active route is traffic ready, offline shadow
   evidence demonstrates semantic equivalence, and rollback controls remain
   available.

A higher state MUST imply every lower state. Missing, stale, malformed, or
unbound evidence MUST produce `ready=false`; a caller-provided `ready=true`
field is never authoritative without recomputation from raw evidence.

Graph traffic readiness and search traffic readiness are independent. A host
MAY activate one while the other remains on the previous owner. Full cutover
readiness requires both.

## Evidence Identity

Every production qualification bundle MUST bind at least:

- Skein source revision and Rust toolchain;
- target OS, architecture, and enabled Cargo features;
- durable format and schema versions;
- database configuration digest and deployment profile;
- canonical graph commit epoch and dataset fingerprint;
- search projection generation, analyzer identity, embedding identity, and
  source graph epoch when search is qualified;
- evidence generation time and the policy that evaluates the evidence.

Evidence from another source revision, target, feature set, dataset, schema,
or projection generation MUST NOT qualify the current runtime. Regenerating a
canonical or search projection generation invalidates generation-bound
evidence.

Authorization is feature-bound. The initial Mem release MAY omit the `acl`
feature and MUST record that exact feature set in its qualification identity.
When `acl` is enabled, authorization policy freshness, pre-materialization
enforcement, filtered search parity, and cache isolation become mandatory
traffic-readiness evidence. Evidence from a build without `acl` MUST NOT
qualify a build that enables it.

`ProductionQualificationIdentity` is the typed release identity and
`ProductionEvidenceBinding` adds the generation time. The identity comparison
is exact, including canonical graph epoch and canonicalized Cargo feature set.
Storage production qualification MUST use
`Database::storage_resource_profile_for_production`; the development
`storage_resource_profile` entrypoint may measure resources but MUST serialize
as production-unready because it has no release binding.

The preferred executable contract is
`run_production_graph_storage_qualification`. It opens the representative copy
through `NowledgeMemEmbeddedStoreHandle`, applies an explicit out-of-core
`DatabaseConfig`, acquires a runtime-governor permit for every measurement,
and emits `skein-production-graph-storage-qualification-v1`. The report stores
only query and parameter digests, retains every raw cold and warm resource run,
and derives a reproducible aggregate from that complete series. Each run
records streaming status, output and intermediate row/payload counts, steady
and peak RSS, RSS growth, total and platform-supported split page faults, and
segment-cache residency and counter deltas. The report also
records one process-memory profile spanning open, measurements, cancellation,
and final digest verification. It retains the final bound raw storage profile
for its limits and storage identity, and fails readiness on admission
rejection, permit leakage, overcommit, stale identity, or any resource-profile
blocker. When a persistent graph index class is under qualification, the typed
requirement additionally binds a reference result digest and row count plus
per-run page and byte budgets. Every run must observe that exact class through
the store-owned read counters; the bounded streaming digest consumer does not
retain or serialize result rows. At least two measurement runs are required.
The required index class itself MUST report a cache miss on the first run and a
cache hit on a later run; process-wide cache counters cannot satisfy this
obligation. The relevant property-projection or adjacency artifact MUST exceed
the configured segment-cache capacity. A pre-cancelled bounded read MUST
terminate within its explicit latency budget, leave zero pinned cache bytes,
avoid poisoning the handle, and be followed by a successful digest read. A CLI
may transport this report, but it MUST NOT replace the typed function in the
production host.

The complete persistent-index release artifact MUST be produced by
`run_production_graph_index_qualification_matrix`. The matrix accepts exactly
one case for each of the eight persistent graph index classes. Every case MUST
use the same replica, out-of-core open configuration, runtime-governor
configuration, production identity, dataset fingerprint, and canonical graph
generation. Duplicate, missing, or requirement-free cases fail before any
measurement starts. Cases execute independently and retain class-scoped
blockers; the matrix is ready only when every case is ready. This aggregate
contract does not make one class's evidence authoritative for another class.
Each case serializes both its observed maximum block/byte reads and the
declared per-run block/byte limits so that a later release process can
re-evaluate every run without trusting the case's reported readiness.

`skein-graph-index-qualification` is the thin developer and evidence collector
for this typed matrix. It accepts one existing database path separately from a
bounded `skein-production-graph-index-plan-v1` JSON document. The wrapper MUST
derive `ShadowReadOnly + OutOfCore` open options and a read-only
`DatabaseConfig`; the plan cannot weaken those selectors. It MUST NOT import,
create, copy, repair, checkpoint, or mutate the representative database.

The plan MUST bind one shared runtime configuration, database cache budget,
process-resource envelope, release identity, and measurement count to all
cases. It MUST list the eight classes exactly once in stable class order and
retain explicit Cypher, typed parameters, query budgets, offline digest and row
count, per-run block and byte limits, and cancellation-latency limit for every
case. Unknown fields, missing or reordered classes, zero-sized budgets,
out-of-domain values, invalid digests, or an oversized plan MUST fail before
the database is opened. The local database path, Cypher, parameters, and result
rows MUST NOT enter retained evidence.

The `desktop_bound_8_gib` plan profile leaves runtime memory derivation dynamic
and limits accepted peak RSS to 2 GiB. It does not independently prove that the
detected host or cgroup limit is 8 GiB; release evidence MUST pair it with the
fixed memory-policy qualification. The `capability_512_mib` profile installs an
explicit 512 MiB governor ceiling and rejects a larger cache or RSS envelope.
These profiles remain distinct. A configured workload profile MUST provide a
non-zero explicit memory ceiling.

The operational invocation and parser-tested plan are documented in
[`PRODUCTION_GRAPH_INDEX_QUALIFICATION.md`](../PRODUCTION_GRAPH_INDEX_QUALIFICATION.md).

`skein-graph-storage-qualification` is the corresponding thin collector for
the independent general graph-storage artifact. It accepts one existing
database path separately from a bounded
`skein-production-graph-storage-plan-v1` document and MUST construct the same
`ShadowReadOnly + OutOfCore` read-only boundary. The plan contains exactly one
representative parameterized Cypher statement, at least two measurement runs,
and explicit database, execution, result, intermediate, process-memory, and
page-fault budgets. It MUST NOT install a persistent-index requirement; the
all-class matrix remains the only persistent graph-index release evidence.

The general collector and all-class collector MUST share the same resource
profile semantics. `desktop_bound_8_gib` leaves the governor dynamic, caps
automatic capacity at 2 GiB, and permits the effective budget to fall below
the nominal 1--2 GiB range under pressure. `capability_512_mib` installs an
explicit 512 MiB ceiling only for the separately declared low-memory
capability. Neither collector may reinterpret 512 MiB as a universal release
threshold. The operational invocation and parser-tested plan are documented
in
[`PRODUCTION_GRAPH_STORAGE_QUALIFICATION.md`](../PRODUCTION_GRAPH_STORAGE_QUALIFICATION.md).

Default reports MUST redact local paths, query text, parameters, row payloads,
embeddings, credentials, and raw parser or I/O payload fragments. Debug-only
diagnostics MAY expose local detail through an explicit host decision, but
debug reports MUST NOT be accepted as production cutover evidence.

`evaluate_production_release_qualification_bundle` is the final typed
cross-process evidence gate. It consumes the identity-bound Content Store
memory-policy matrix, a representative production-profile Content Store read,
a separate explicitly constrained 512 MiB Content Store read, the Content
Store mutation-replica matrix, separate 512 MiB capability and dynamic 8 GiB
desktop overflow-compaction runs, graph-storage, all-class graph index matrix,
out-of-core search, per-target vector, per-worker morsel, active-route blocking,
storage crash-recovery, and exact-revision release-control artifacts.
The evaluator
does not trust their top-level `ready` fields: it revalidates protocols,
release bindings, every graph cold/warm resource run, the recomputed graph
resource summary, lifecycle process-memory capabilities, raw resource limits,
lifecycle coverage, target and worker matrices, scalar parity, runtime-permit
cleanup, spill cleanup, crash-point coverage, required CI conclusions,
artifact digests, and the caller-declared regression policy. A missing,
truncated, reordered, incorrectly phased, over-budget, or summary-inconsistent
graph run fails closed. Its output retains only source-artifact SHA-256 digests
and assessments, so it can be retained as release evidence without copying queries,
paths, embeddings, or row payloads.

The Content Store artifacts are independently re-evaluated from retained raw
contracts and measurements. The memory-policy artifact MUST contain both the
dynamic desktop and explicit 512 MiB policy reports from the same detected
resource snapshot, and the evaluator MUST recompute their capacities, dynamic
budgets, nominal-range flag, and exact release binding. A nominal desktop
budget below 1 GiB under pressure remains valid; a desktop capacity or budget
above 2 GiB does not. Policy evidence cannot substitute for either workload
run. The production read MUST NOT use the 512 MiB capability profile, and the
separate capability read MUST use it. Both reads bind every frozen SQL
statement to its corpus-derived digest and row/payload limits, require one
open plus exact cold/warm runs per case, recompute row/index generation and
larger-than-cache invariants, and checks physical I/O, hydration, process,
page-fault, cache-pin, and runtime-governor accounting. The mutation artifact
requires exactly the 1, 4, 8, and 10 writer cases, validates every frozen
`INSERT` and `UPDATE` run, recomputes latency percentiles and regression,
checks commit-epoch and WAL group accounting, and proves the distinct
WAL-replay and manifest-only reopen boundaries plus result-digest parity. A
child report's empty blocker list cannot override contradictory raw fields.

The two overflow-compaction artifacts are not interchangeable. The capability
artifact MUST carry an explicit 512 MiB governor ceiling. The desktop artifact
MUST observe an 8 GiB host or cgroup limit, retain dynamic memory derivation,
and keep both capacity and budget at or below 2 GiB; pressure may reduce the
budget below 1 GiB. For both artifacts, the evaluator independently validates
the frozen SQL contracts and before/published/reopened digests, metadata-only
generation transitions, scan/sort/spill/rewrite bounds, zero closure hydration,
RSS and requested page-fault limits, artifact write amplification, physical
extent deletion, scrub evidence, and exact admission/completion accounting.

The graph-index matrix is a required release artifact distinct from the
general graph-storage run. The evaluator requires all eight classes in stable
order, validates every embedded graph resource report against the exact release
identity, recomputes parity, cold/warm cache lifecycle, operation and I/O
aggregates, per-run block/byte budgets, larger-than-cache residency, and
cancellation cleanup, and rejects missing or duplicate classes. A top-level
matrix `ready` value or `qualified_class_count` cannot hide an invalid case.

`skein-qualification-bundle` is a thin CI and release transport over that
typed evaluator. It accepts bounded JSON inputs from independently generated
process and platform artifacts and exits unsuccessfully when the recomputed
bundle is not ready. It is not a production serving control plane and cannot
generate representative evidence by itself. The Content Store inputs are
mandatory through `--content-store-memory-profiles-json`,
`--content-store-read-json`, `--content-store-512-mib-read-json`, and
`--content-store-mutation-matrix-json`; omitting any one fails before
evaluation.

## Cross-Platform Qualification

Linux, macOS, and Windows are production targets. Required CI checks for each
supported target MUST be green for the exact revision being released.

Resource reports MUST expose a typed metric capability set. A platform MUST
NOT relabel an aggregate counter as a semantically different split counter. In
particular, a Windows total page-fault count MUST NOT be reported as Unix minor
or major faults. A qualification policy MUST require all metrics declared
available for its target and MUST fail closed when a metric required by that
policy is unavailable.

Process resource sampling SHOULD provide:

- steady and peak resident memory;
- total page faults on every target that exposes them;
- minor and major page faults only on targets that expose that distinction;
- intermediate and output row counts;
- intermediate and output payload bytes;
- segment-cache residency, misses, evictions, and admission rejections.

`skein-storage-resource-profile-v2` exposes `resource_ready` separately from
production `ready`. Production readiness additionally requires an exact
evidence/expected-identity match. `metric_capabilities.total_page_faults`
applies on Unix and Windows; `metric_capabilities.split_page_faults` is false
on Windows, where the split fields MUST remain absent.

The Windows storage-platform CI job MUST retain a
`storage-resource-windows-latest-<revision>` artifact containing the bound v2
report, the exact canonical row/overflow backup-reopen-reclaim test output, and
a runner manifest with independent exit statuses for both probes. The manifest
MUST bind the source revision and Windows runner identity; either non-zero
probe status MUST fail the job. The report MUST be production-ready for the
platform fixture, identify `target_os` as `windows`, expose resident-memory and
total-page-fault capability, and leave Unix split page-fault fields absent.

Linux runtime sizing MUST derive effective CPU and memory from the smallest
known host and cgroup limits. Cgroup v2 `cpu.max`, effective/inherited cpuset,
`memory.max`, `memory.high`, `memory.current`, and memory headroom detection are
supported. The unified path MUST be resolved from process membership and the
cgroup2 mount root. A detected controller with an unreadable or invalid limit
fails closed; it MUST NOT silently use host-wide limits inside a container.
Cgroup v1 resource controllers are unsupported and fail closed to one CPU and
zero memory admission; a v1-only or resource-controller hybrid host cannot
qualify for production readiness.

## Resource Qualification

Production resource evidence MUST be collected from a representative copy of
the intended workload. Synthetic ignored tests are useful development gates
but do not qualify a production deployment.

`qualify_content_store_memory_profile` is the typed policy gate for the two
fixed Content Store memory profiles. `DesktopBound8Gib` requires an observed
effective host or cgroup limit of exactly 8 GiB. The default 25% policy must
derive a 2 GiB capacity and a dynamic headroom-bound budget. With 4--8 GiB
available, the report records the nominal 1--2 GiB range. Lower headroom may
legitimately lower the budget below 1 GiB and MUST NOT invalidate the policy;
the budget is not a fixed reservation.
`Capability512Mib` sets an explicit 512 MiB Skein capacity ceiling while still
honoring a smaller host or cgroup policy ceiling and dynamic available
headroom. The policy report MUST be paired with a constrained workload run
whose peak RSS stays within its declared 512 MiB envelope. This is a supported
capability profile, not the default profile or a universal release cutoff.

`run_production_content_store_memory_qualification` is the identity-bound
fixed-profile matrix. It MUST evaluate `DesktopBound8Gib` and
`Capability512Mib` from the same detected `RuntimeResourceSnapshot` and I/O
budget, bind the result to the exact current target and release identity, and
retain both raw policy reports. Matrix readiness MUST be recomputed from their
blockers rather than accepted from a caller-provided flag. The desktop report
MUST retain dynamic headroom derivation and MUST NOT reject a correct budget
only because it is below 1 GiB. The capability report MUST install the explicit
512 MiB ceiling without pretending that the OS limit is 512 MiB.

`skein-content-store-memory-qualification` is a thin evidence collector for
that typed matrix. It accepts one existing storage path separately from a
bounded `skein-production-content-store-memory-plan-v1` document. The path is
used only for storage-device classification and MUST NOT enter retained
evidence. The collector MUST NOT open, create, copy, repair, checkpoint, or
mutate a database. Its operational contract is documented in
[`PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION.md`](../PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION.md).

The canonical artifact MUST exceed the configured segment-cache budget. Search
qualification MUST also exercise a document corpus larger than the admitted
search memory budget. The report MUST record steady RSS, peak RSS, page faults,
intermediate rows, payload bytes, cache residency, spill bytes, spill runs, and
operator-specific peak tracked memory.

Relational production evidence MUST read canonical row and index residency from
the exact pinned serving view. It MUST bind row and index base generations and
visible epochs, record row, overflow, index, recovery-delta, and live-overlay
bytes independently, and reject a non-serving view. Directory size and the
presence of candidate files are not proof that a current relational generation
is selectable.

The typed entry point is
`run_production_content_store_storage_qualification`. It MUST open the imported
Skein copy read-only with authoritative indexes, reopen once per frozen SQL
case, retain distinct cold and warm runs, compare every output with an offline
digest, obtain one runtime-governor permit per measured read, and redact paths,
parameters, and rows. The report MUST retain the governor's derived capacity,
dynamic budget, and admission/completion deltas. Every open MUST retain the
engine-measured durable-manifest, checkpoint/root, WAL-replay, post-replay, and
total-open intervals. It MUST also retain the fresh instance's raw segment
payload-cache capacity, residency, pin, hit, miss, eviction, admission-
rejection, and digest-mismatch counters before the first user query. Startup
may perform bounded mandatory system-schema validation through canonical rows,
so the plan MUST declare non-zero request and resident-byte ceilings within the
cache capacity. Requests and residency MUST remain within those ceilings;
pins, evictions, admission rejections, and digest mismatches MUST remain zero.
Manifest and WAL work is accounted by the open intervals. The four sequential
phase intervals use one monotonic clock,
their saturated sum MUST NOT exceed the total, and the engine total MUST NOT
exceed the caller's enclosing open measurement. Release evaluation MUST
recompute the timing relations and bounded payload-cache predicate from the
raw fields rather than trusting reported booleans. A case is not ready when its
selected generation changes across opens, row and index epochs diverge, either
canonical artifact does not exceed the cache, an access wave is rejected by
the cache, a pin leaks, or an explicit row, payload, I/O, RSS, or page-fault
budget is exceeded. Per-run page-fault limits MUST NOT be applied to the
cumulative lifecycle profile. Synthetic runner tests validate this protocol
but cannot produce representative-replica evidence.

`skein-content-store-read-qualification` is a thin developer and evidence
collector wrapper over the typed runner. It accepts an existing database path
separately from one bounded `skein-production-content-store-read-plan-v1` JSON
document. The plan fixes the evidence identity, frozen statement names and
typed parameters, result oracle digests, read/I/O budgets, process limits, and
one declared resource profile. Unknown fields, an unknown protocol, zero-sized
database budgets, or values outside Skein's parameter domain MUST be rejected
before the database is opened. The wrapper MUST derive `read_only + OutOfCore +
Authoritative`; callers cannot weaken those selectors in JSON. The
`capability_512_mib` profile installs an explicit 512 MiB runtime ceiling, while
`desktop_bound_8_gib` retains the dynamic desktop governor with its 2 GiB
capacity ceiling. A configured-workload ceiling MUST be non-zero and no larger
than its declared available memory.

The wrapper MUST NOT create, copy, migrate, or mutate the representative
database, and the database path MUST NOT enter retained evidence. Replica
creation remains caller-owned. A ready report exits successfully, a complete
but blocked report exits unsuccessfully, and invalid input returns a distinct
error status. This executable contract makes a production run reproducible; it
does not turn a synthetic fixture or an unretained local run into release
evidence.

The operational invocation and bounded plan shape are documented in
[`PRODUCTION_CONTENT_STORE_QUALIFICATION.md`](../PRODUCTION_CONTENT_STORE_QUALIFICATION.md).

`skein-content-store-mutation-qualification` is the corresponding thin
developer and evidence collector for the writable matrix. It accepts the
read-only source and the four 1/4/8/10-writer replica paths separately from one
bounded `skein-production-content-store-mutation-plan-v1` JSON document. The
wrapper MUST NOT copy, create, or migrate a replica. The caller MUST provide
four distinct existing disposable database directories, each separate from
the source and initialized from the exact expected generation. The typed
runner canonicalizes and validates those paths before opening any replica for
mutation.

The mutation plan MUST list cases in exact 1, 4, 8, and 10 writer order, contain
one explicit worker definition per writer, and retain explicit frozen statement
names, parameters, conflict domains, verification digests, latency limits, and
the accepted latency-reference identity. It MUST also contain the complete WAL
group-commit activation evidence. The wrapper MUST construct group commit only
through the evidence-validating Skein constructors; a JSON boolean or policy
name alone cannot activate it. Unknown fields, incomplete writer matrices,
invalid numeric bounds, and WAL evidence rejected by the engine MUST fail
before mutation begins.

The wrapper derives writable `OutOfCore + Authoritative` database
configuration. It MUST retain the fixed resource-profile identity and process
RSS/page-fault limits but MUST NOT claim that the mutation runner owns a
runtime-governor permit. `desktop_bound_8_gib` declares 8 GiB of available
memory and caps accepted peak RSS at 2 GiB; `capability_512_mib` is the separate
explicit low-memory capability run. A configured workload MUST declare a
non-zero available-memory envelope. Paths, parameters, and rows MUST NOT enter
retained evidence.

The operational invocation and parser-tested plan are documented in
[`PRODUCTION_CONTENT_STORE_QUALIFICATION.md`](../PRODUCTION_CONTENT_STORE_QUALIFICATION.md).

Every query result path MUST have explicit row and payload limits. Every
blocking operator MUST do one of the following before exceeding its admitted
memory:

- spill through a byte- and run-bounded external algorithm;
- reject the operation with a stable resource error; or
- prove through route-bound admission evidence that the active production
  shape cannot exceed the limit.

`SortExec`, `TopNExec`, and grouped `AggregateExec` use byte- and run-bounded
ordered spill. Mergeable grouped aggregates spill partial states. Other grouped
aggregates spill only the group key and one normalized operand per aggregate;
they MUST NOT retain or serialize unrelated variables or properties from the
upstream binding. Aggregate output construction MUST retain one root owner while
an in-memory group, partial group, or decoded spill row becomes a `Binding`.
The handoff atomically moves the retained charge from blocking state to the
pipeline batch, admitting any representation-size increase against both the
batch account and query root without requiring both representations to be
charged after ownership has moved. Input rows and merge entries MUST remain
charged until their values have entered the accumulator, while completed output
batches remain charged until synchronous emission. `DistinctExec` uses ordered
spill runs followed by bounded merge deduplication. `NodeCartesianProductExec`
partitions an oversized build side into bounded spill runs and replays those
runs for each streamed probe row.
Before a node scan feeds an aggregate, physical-plan finalization MUST restrict
canonical decoding to predicate, group-key, aggregate-operand, and access-
validation properties whenever those expressions depend on one node variable.
Unreferenced large properties MUST remain unhydrated. The internal scan may
retain node identity for `COUNT(node)` and `COUNT(DISTINCT node)`, but it MUST
decline the optimization if a whole-node value could escape through grouping,
collection, projection, or result construction.
Sequential node scans and property, bounded property-union, composite equality,
composite prefix-range, single-property range, and text index scans MUST
admit each retained row to a query-rooted pipeline batch before insertion. The
batch MUST flush at the first row-count or resident-byte boundary and MUST keep
the stable `batch_payload_bytes` rejection for a single oversized row. A
property-union scan MUST additionally admit every retained deduplication key to
blocking state before the candidate becomes visible. Graph
existence predicates and optional degree calculation MUST consume adjacency
through the visitor API, stop as soon as their result is known, and pass the
query blocking account into any storage-side ordered merge; they MUST NOT
materialize a degree-sized relationship vector.
Replay retains the encoded spill lease until the right binding is admitted to
the blocking account. Before cloning either input, each output row reserves the
sum of both input resident estimates in a query-rooted pipeline-batch account;
the conservative reservation is reduced to the actual merged row size and is
transferred synchronously when the batch is emitted.
`GraphAlgorithm` admits the direction-specific projection together with a
conservative algorithm scratch and result estimate before PageRank or Louvain
allocates that state. It fails with a stable resource error rather than spilling,
checks cancellation within node and edge loops, and reports its combined
projection, scratch, and materialized-result peak as blocking-operator memory.
`ShortestPathExec` charges frontier and retained paths to one query-rooted
blocking account, streams adjacency without a degree-sized vector, and carries
result ownership through an accounted batch handoff. `ThreadRepairStatsExec`
uses the same handoff and counts typed adjacency visits without collecting each
thread's relationship and target records.
All of these paths MUST report tracked peak memory, input rows, spill bytes,
spill runs, and spilled rows, and MUST remove query-scoped runs on success,
error, cancellation, and consumer stop.

Active-route evidence for high-cardinality distinct and Cartesian build sides
uses `run_production_blocking_qualification`. The bound report requires one
unambiguous `DistinctExec` or `NodeCartesianProductExec` memory report per route,
checks the route-declared minimum input cardinality, and records either
`external_spill_observed` or `in_memory_within_admission`. Both are acceptable
only while tracked memory and cumulative spill limits hold. Final readiness
also requires zero live or pending spill-pool capacity, zero cleanup failures,
and governor admission/completion for every route.

Spill admission has two levels. Each blocking operator retains its cumulative
byte and run limits, while all queries whose `ExecutionMemoryConfig` resolves
to the same spill directory share one process-wide live-byte and live-run
pool. A record MUST reserve both levels before it enters the writer buffer.
The in-memory encoded length MUST be derived from the codec layout rather than
from a decoded-row estimate. During merge, the encoded payload remains charged
until the decoded row is admitted to the same query root; sort, aggregate, and
distinct merge fan-in MUST NOT use an unrooted operator tracker.
The pool MUST account for unflushed reservations when preserving
`min_spill_free_bytes`, release live capacity only after its run is removed,
and fail closed when filesystem capacity cannot be inspected. If multiple
configurations use one directory, the process retains the strictest limits it
has observed for that directory. `ExecutionMemoryConfig::spill_pool_snapshot`
exposes active and peak bytes and runs, pending writer bytes, orphan cleanup,
and deletion failures for readiness and monitoring.

Spill filenames are owned by a versioned Skein namespace. On first use of a
spill directory in a process, Skein MUST remove only namespace-matching files
from earlier process identities whose age reaches
`spill_orphan_grace_period`; unrelated files and current-process runs MUST
remain untouched. The default grace period is 24 hours. A failed live-run
deletion remains charged to the shared pool and observable rather than making
unreclaimed disk capacity available to new queries.

Production query entrypoints MUST pass through the runtime governor or an
equivalent host-owned admission boundary. Raw database access MAY remain a
low-level library capability, but its use MUST be reported as non-production
safe unless the host supplies equivalent global admission.

`skein-embedded-query-path-readiness-v1` reports this boundary without
claiming traffic readiness. `SkeinEmbedded::query_admitted`, its parameterized
and task-context variants, and `SkeinTokioEmbedded::query` are
`admission_safe=true`. `SkeinEmbedded::database`, `database_mut`, and
`into_database` remain controlled-host and test surfaces; their report is
`admission_safe=false` with `host_equivalent_governor_not_proven`. An
admission-safe path is only one input to production qualification and MUST NOT
replace revision-, dataset-, and workload-bound evidence.

The Nowledge production facade follows the same boundary. Its parameterized
query and bounded streaming-read entrypoints on `NowledgeMemGraph`,
`NowledgeMemEmbeddedStore`, and `NowledgeMemEmbeddedStoreHandle` MUST acquire a
`RuntimeGovernor` permit before execution. Streaming reads MUST admit the
minimum of the governor result budget, database result limit, and route payload
limit, and MUST retain the permit until the consumer returns. Task-context
variants MUST propagate cancellation and record it in the governor snapshot.
Diagnostic preflight and multi-statement typed transaction helpers are not
traffic-admission evidence until the host binds equivalent admission. The facade's
`database`, `database_mut`, and `into_database` accessors remain explicitly
controlled-host and test surfaces; calling them is not production admission
evidence. Hosts MAY inject one shared governor into the Nowledge facade so all
store handles participate in the same process-level CPU, memory, result, and
I/O limits.

The real Mem runtime MUST publish
`skein-nowledge-mem-serving-path-readiness-v1` from its long-lived
`NowledgeMemEmbeddedStoreHandle`. The report MUST bind a non-empty host runtime
identity and prove shared-governor admission for foreground parameterized
Cypher, bounded streaming reads, typed mutation, typed analytics, and typed
maintenance. An unbound handle is admission-safe but is not production-path
evidence. A raw `Database` report MUST remain fail-closed even when a controlled
host identity is present.

Parallel morsel qualification uses `run_production_morsel_profile` and
`evaluate_production_morsel_matrix`. Each profile MUST run through the
read-only `NowledgeMemEmbeddedStoreHandle`, contain at least 100 measured
samples after at least three warmups, observe the requested 4, 8, or 16 active
workers, execute the typed columnar morsel fragment, and prove that completed
outputs and ordinal reorder entries remain within the admitted worker window.
The artifact MUST also show that buffered typed output remains within its
per-worker byte reservations, the query root ledger returns to zero, the morsel
pipeline does not spill, and in-flight cancellation completes within its bound.
It additionally requires zero admission rejection, permit leakage, and
overcommit. The three profiles MUST come from independent processes and bind the
same release, dataset, graph epoch, query digest, and parameter digest.
`skein-production-morsel-matrix-v1` accepts the matrix only when throughput
improves at every step and caller-declared P99, peak-RSS, and cancellation
regression budgets hold. A materialized read-only profile is required because
out-of-core source scans deliberately remain serial until ordered parallel
range reads have their own correctness contract.

An asynchronous facade that returns a materialized result remains subject to
the result budget. A streaming asynchronous API MUST propagate consumer
backpressure and cancellation without retaining the complete result.
`SkeinTokioEmbedded::query_stream` and its parameterized/options variants use
the admitted query request, add the bounded channel residency to admitted
memory, and deliver execution-memory-sized batches through a finite channel.
The producer retains its runtime permit until the terminal report, observes a
child cancellation token linked to the caller deadline/token, and is cancelled
when the consumer is dropped. Mutation statements are rejected by this API and
continue through the serialized materialized mutation path.

## Durability Qualification

Release qualification MUST exercise real process termination in addition to
in-process failpoints. The crash matrix MUST cover at least:

- before WAL append;
- after append but before synchronization;
- after WAL synchronization but before snapshot publication;
- during checkpoint artifact publication;
- after manifest publication but before obsolete-generation reclamation.

Each crash point MUST reopen the database in a fresh process and verify that a
mutation batch is either entirely absent or entirely recovered. The harness
MUST verify commit epoch, replay LSN, endpoint integrity, projection watermark,
and the absence of partially published artifacts. Ordinary startup MUST reject
a torn WAL tail without changing it. Explicit doctor repair MUST preserve
structured prepared and applied repair records, retain a verified copy of the
original WAL, and report discarded bytes before normal serving can resume. The
repair MUST use a separate typed API: a read-only generation-bound dry run,
followed by explicit acknowledgement and state revalidation. A pending repair
record MUST block ordinary open, and an interrupted repair may be finalized
only when the manifest, retained WAL, and quarantine identities still match.

Generation-bound canonical adjacency and persistent property projection files
are rebuildable derived storage. Corruption in either MUST fail closed during
open or block access, while the explicit full-file health check MUST surface a
typed repair-required state before serving readiness succeeds. Repair MUST be
an explicit two-phase `DatabaseDoctor` operation: a read-only plan MUST validate
the canonical source and admit source rows, logical bytes, WAL replay,
temporary bytes, memory, generated entries, and spill runs; apply MUST
revalidate the source identity, retain checksummed quarantine copies, persist a
prepared audit record, and publish a new full checkpoint generation with the
manifest last. A pending record MUST block ordinary open. Interrupted
completion MUST verify the new generation and all quarantine identities before
publishing an applied record. Canonical segments, checkpoint state, property
spill, manifest, and WAL remain non-rebuildable source state and MUST fail
closed.

Durable-before-publish, pinned-reader, optimistic first-committer-wins,
shared/exclusive point and range lock compatibility, pessimistic lock release,
deadlock-victim cleanup, and multi-owner wait-for graph acyclicity invariants
MUST be model checked for the released storage protocol. Model-check
configuration and results MUST be part of the release CI artifact set, not only
documented as a local command.
The revision-bound `tla-model-check-<revision>` artifact MUST contain successful
TLC logs and exact `.tla` and `.cfg` inputs for every model declared by the
authoritative `docs/tla/storage_models.bzl` manifest, together with the Java
version and the pinned TLA+ Tools version and SHA-256 digest. Bazel and the
retained-evidence collector MUST consume that same manifest. Collection MUST
fail on duplicate declarations, missing model pairs, or undeclared `.tla`/`.cfg`
files. A downstream CI job MUST download and verify the complete artifact
before the model-check gate succeeds.

The typed crash artifact protocol is
`skein-storage-crash-recovery-evidence-v1`. Every required crash point MUST
appear for the admitted repetition count and every case MUST prove termination,
whole-batch recovery, epoch/LSN agreement, endpoint integrity, projection
watermark integrity, and active artifact-generation integrity. CI MUST retain
this report per target and source revision.

## Search Qualification

The mutable full-residency search index is a maintenance and compatibility
owner. A larger-than-memory production caller MUST use the generation-bound
out-of-core facade and its production constructor.

A complete larger-than-memory rebuild MUST enter through
`SearchOutOfCoreGenerationWriter` or an equivalent typed library path with the
same contract. Input IDs MUST be strictly ordered, every input and temporary
resource limit MUST fail before active-manifest publication, and finalization
MUST retain at most one admitted document segment plus the explicitly bounded
descriptor and lexical spill state. All immutable files MUST be durable before
the active out-of-core manifest switches. A failed build MUST leave the prior
generation readable and remove its private stage. This kernel property enables
the production-copy qualification but does not replace the representative Mem
evidence required below.

An incremental larger-than-memory update MUST use
`SearchOutOfCoreGenerationWriter::prepare_delta`. It MUST merge a bounded,
validated delta with one pinned immutable generation in document-ID order,
retain no corpus-wide document map, and publish through the same immutable
generation protocol as a full rebuild. Lifecycle evidence MUST report zero
resident corpus documents and the peak decoded source-segment bytes. The old
reader MUST remain usable while the new generation is finalized. After taking
the publish lease, finalization MUST compare the active manifest generation
with the pinned delta base. A changed base is a stale update and MUST abort
without replacing the newer generation; callers may rebuild the delta against
the new active generation.

When `vector-search` is enabled, the generation writer MUST build the
file-backed RaBitQ candidate artifact from the same ordered spool under the
same publish lease. Its generation, source graph epoch, embedding identity,
vector count, source digest, payload checksum, file length, and outer checksum
MUST be covered by the active out-of-core manifest. Filter sidecars MUST carry
stable vector ordinals. A compressed out-of-core query MUST push the ordinal
allowlist into the quantized scan, retain at most the admitted candidate window,
read raw vectors only for returned candidates, and use raw cosine scores for
final ranking. `Required` MUST fail closed when the bound artifact is absent or
invalid. `Preferred` MAY use the exact scalar segment path with an observable
fallback reason only when no usable RaBitQ projection is attached to the
reader. An I/O, corruption, cancellation, or resource error from an attached
artifact MUST fail closed instead of being hidden by scalar fallback. The
default exact scalar path remains available for differential qualification.

Production qualification MUST exercise `Required` and `Preferred` against the
source generation named by the report, not only against a later disposable
lifecycle generation. Both modes MUST select the file-backed RaBitQ
backend, push a metadata-derived ordinal allowlist, read projection payload
bytes, and finish with raw-vector scores without fallback. The generation-update
lifecycle repeats the same probes after publication and corrupts both a bound
RaBitQ artifact and the active manifest; either corruption MUST be rejected
during open.

The embedded serving entrypoint is
`NowledgeMemOpenOptions::with_qualified_out_of_core_search_projection`. It MUST
receive the raw lexical qualification report, the exact current release
identity, and explicit out-of-core budgets. Open MUST reject a graph whose
canonical commit epoch differs from the bound release identity and MUST expose
`qualified_out_of_core` plus a successful production-qualification binding in
the sanitized open report. This mode opens no full-residency `SearchIndex`.
Candidate search, bounded late hydration, Knowledge Retrieval graph-context
expansion, and Cypher vector seed reads MUST use the same admitted handle and
the pinned out-of-core generation. Out-of-core byte metrics MUST remain visible
on the typed candidate, hydration, and retrieval outputs. Runtime admission MUST
charge the configured decoded-segment, candidate-block, RaBitQ scan,
hydration, and matched-span buffers in addition to blocking score state and the
result budget; a bound larger than the shared governor can admit MUST reject
before search I/O.

Immutable lexical, out-of-core, and RaBitQ generation cleanup MUST retain
the active and immediately previous generations. `SearchIndex` MUST run a
cleanup cycle with bounded deletion attempts and a bounded pending queue after
open and checkpoint and expose the latest `SearchProjectionCleanupReport`.
Windows sharing violations and other delete failures MUST be retained without
failing an already durable checkpoint. A later cycle MUST revalidate generation
eligibility before retrying, and production search evidence MUST fail closed
while cleanup or published-generation discovery remains pending.

Search traffic readiness MUST recompute qualification from raw evidence and
require:

- exact TopK and score parity for text, vector, and hybrid modes;
- lifecycle, space, metadata, and source-filter parity before ranking, plus
  ACL parity when the release feature set enables `acl`;
- incremental update, delete, checkpoint, reopen, stale-generation, and
  corruption cases;
- bounded candidate spill, score state, sidecar reads, and late hydration;
- representative P50, P95, and P99 latency, steady and peak RSS, page faults,
  posting bytes, payload bytes, checkpoint time, and update amplification.

Request-time comparison with the previous engine MUST NOT run on the production
serving path. Shadow and differential evidence belong to an offline or
dedicated preflight path.

`skein-search-lexical-production-qualification` version 2 binds the report to
the release identity and to projection generation, source graph epoch,
document digest, analyzer digest, and embedding model/version/dimension. It
records target-appropriate process-memory capabilities, text/vector/hybrid
TopK score parity, sidecar and hydration bytes, and P50/P95/P99 latency. The
out-of-core production constructor MUST recompute these blockers against the
opened projection identity and an explicit current
`ProductionQualificationIdentity`. A report whose internally recorded release
identity is self-consistent but differs from the current revision, feature set,
configuration, dataset fingerprint, or canonical graph epoch MUST fail.

`run_production_search_out_of_core_qualification` is the typed evidence
collector. It MUST run in a fresh qualification process. It measures the
out-of-core candidate before opening the full-residency canonical oracle so the
oracle cannot inflate candidate RSS or lifetime peak RSS. Query text, vector,
hybrid, metadata, and optional ACL cases are represented by named typed cases;
the report stores request and result digests instead of query text, embeddings,
or document identifiers. Exact TopK and score parity is derived from those
digests, not from caller-provided booleans.

The collector takes separate serving and reference projection directories.
The serving directory owns the generation-bound out-of-core artifacts; the
reference directory is an offline full-residency oracle and MUST NOT be opened
by production traffic. Before comparison, the collector requires equal source
epoch, document count and digest, analyzer digest, and embedding identity. It
also records lifecycle RSS and page faults before opening the full-residency
oracle so oracle residency cannot hide generation-update memory growth.

Lifecycle probes MUST use at least three explicit, disposable writable copies
of the same projection generation. The runner mutates those copies to measure
incremental update and checkpoint latency, verifies reopen and pinned stale-
generation reads, and overlaps old-generation reads with checkpoint work. A
separate explicit corruption copy is intentionally damaged and MUST fail to
open. The runner never copies, removes, or corrupts the configured source
projection. Synthetic fixtures can test this protocol but remain blocked by
the representative document-count, larger-than-memory, identity, and resource
thresholds.

Search and vector release artifacts MUST carry the same
`SearchProjectionQualificationIdentity`. The release evaluator MUST reject a
vector target when its search generation, document digest, analyzer digest,
source graph epoch, or embedding identity differs from the qualified search
artifact. The file-backed vector projection generation MUST equal that common
search generation.

`run_production_vector_qualification` is the typed RaBitQ production
collector. Its `skein-production-vector-qualification-v1` report wraps the
bounded `skein-vector-recall-production-qualification-v1` probes and binds
them to the current release identity and the opened file projection's
generation, source graph epoch, raw-vector source digest, payload identity,
format, algorithm, bit width, dimension, transform seed, embedding
model/version, and document count. In-memory generation-zero projections and
corpora below 100,000 vector documents MUST NOT qualify production.

Its algorithm-oracle path and released search-generation path MAY be separate
directories, but their source epoch, document count and digest, analyzer
digest, and embedding identity MUST match. The report carries the released
search identity and the vector identity and resource evidence read from that
released generation; the final bundle additionally requires the vector
projection generation to equal that search generation. The offline projection
identity MUST match the serving algorithm, format, bit width, dimension,
transform seed, source epoch, document count, and embedding identity.

Before opening the full-residency oracle, the collector MUST run the released
out-of-core RaBitQ artifact with `Required`, record its latency, RSS, page
faults, backend, kernel, payload I/O, admitted workers, and raw-reranked result
digest, and propagate a cancelled `RuntimeTaskContext` into that scan. The
serving final digest MUST equal the corresponding offline RaBitQ oracle
digest for every case. Candidate recall remains the offline oracle's bounded
responsibility, bound to the same document identity and RaBitQ algorithm
identity; the production reader does not retain candidate-ID evidence solely
for qualification. This keeps full-residency recall and differential machinery
out of the serving path without allowing an unrelated or unexecuted artifact to
certify it.

The collector MUST include separate quantized candidate-window and final raw-
reranked TopK recall, scalar-versus-dispatched candidate parity, P50/P95/P99
latency, process RSS and page-fault capability evidence, projection and raw-
vector bytes, build memory and amplification, block pruning, admitted workers,
and selected kernels. Candidate identifiers MAY be retained transiently by a
validation-only execution control, but serialized production evidence MUST
contain only digests, aggregate counts, and per-million values.

Lifecycle probes MUST use explicit disposable copies and cover incremental
projection invalidation with safe scalar serving, checkpoint publication,
reopen, pinned stale-generation isolation, current-artifact corruption,
cancellation propagation, and foreground reads overlapping checkpoint work.
The report MUST also contain native RaBitQ scalar-reference evidence for every
case: dispatched/scalar candidate parity, final raw-rerank parity, and serving
parity. A qualification configured to require this evidence MUST fail closed
when it is absent or inconsistent. This is a dispatch-regression guard, not an
independent truth oracle; candidate recall remains measured against canonical
raw-vector truth. The production feature graph, checkpoint, and serving backend
MUST not depend on a separate vector-quantization runtime.

`skein-production-vector-qualification-matrix-v1` combines independently
generated reports and requires matching release and projection identities for
Linux x86_64, Linux AArch64, macOS AArch64, and Windows x86_64. Every target
MUST also pass its portable scalar reference comparison. Unit and synthetic
fixtures validate this protocol but cannot satisfy its representative-data or
target-matrix requirements.

## Route And Cutover Qualification

Every active graph and search route MUST declare one selected read owner. Route
readiness MUST be tied to the shared route catalog, query family, bounded query
evidence, and the current production build identity.

Active App routes that combine graph and relational reads MUST additionally
retain the typed bounded-snapshot report for the same observation. The evidence
consumer MUST recheck the declared row and payload budget arithmetic and the
required successful Cypher and SQL statement counts. A search-bearing route
MUST additionally require a non-zero external vector-seed execution count;
`search_projection_present` alone is insufficient. Such a route MUST NOT be
added to the graph-only route catalog merely to reuse its readiness result.

Cutover MUST remain blocked when any required area is missing or not ready,
including:

- route ownership or query-family coverage;
- storage recovery and resource qualification;
- authorization policy freshness and pre-materialization enforcement when the
  release feature set enables `acl`;
- search projection generation, freshness, and parity;
- background QoS and foreground admission;
- redaction and production library-path verification;
- initial import, dual-write convergence, or rollback controls while they are
  required by the migration phase.

Production health MUST report liveness for the selected owner and MUST NOT open
or probe the previous engine solely to make the selected owner appear healthy.

## Release Controls

Branch protection is a delivery-governance control, not a kernel traffic
readiness prerequisite. It MAY be deferred while Skein is in rapid iteration.
An unprotected development branch MUST NOT weaken the evidence required for a
production release or allow branch state alone to imply production readiness.

A production release MUST identify one exact revision and require its green
checks. Required release checks MUST include formatting, strict lint, workspace
tests, supported-platform runtime and storage tests, concurrency models, and
build system parity.

`skein-production-release-control-evidence-v1` is the typed exact-revision CI
input to the final bundle. Every required check MUST record the same full source
revision as the release identity, a successful conclusion, and the SHA-256 of
its retained evidence artifact. The final bundle MUST also consume and
revalidate `skein-storage-crash-recovery-evidence-v1`; a green job name or a
top-level `ready` value alone cannot satisfy either gate.

Before a general-availability phase, the project SHOULD protect its release
branch, require pull requests and required checks, and prohibit force pushes and
branch deletion. An equivalent audited release branch or immutable release-tag
workflow MAY satisfy this governance requirement without protecting the
rapid-iteration branch earlier.

The release process SHOULD also include dependency advisory and license policy
checks, storage crash/soak campaigns, and artifact retention for production
resource evidence. These checks MUST become required before their corresponding
risk is accepted for production.

Optimizer and persistent-format fuzz targets remain available through Bazel for
routine local verification. They MUST NOT be added to default or dedicated CI
jobs. The canonical local command is documented in `AGENTS.md`.

The scheduled quality workflow MUST run the advisory and license policy, the
RaBitQ scalar-reference corpus under an address sanitizer, and a mixed
foreground/background runtime soak. The runtime soak MUST
use an out-of-core fixture whose raw bytes exceed the admitted runtime memory
and whose canonical artifact exceeds the segment cache. It MUST exercise the
admitted Tokio facade, high-cardinality distinct and Cartesian paths, observe
bounded external spill, complete a concurrent mutation and checkpoint, and
retain latency, RSS, page-fault, cache, spill, checkpoint, and governor
counters in a revision-bound typed report.

Scheduled synthetic soak evidence MUST identify its controlled fixture setup
path and carry `production_eligible=false`. It verifies regression behavior but
MUST NOT satisfy the representative Mem replica, production-shaped search, or
representative embedding qualification gates.

No release report may claim production readiness while a required check is
red, skipped without an approved qualification artifact, or evaluated for a
different revision.

## Production Boundary

The supported embedded production boundary is one active root handle in one
application process, with concurrent snapshot readers and serialized durable
writes. Multi-process writers, distributed replication, cloud-primary
execution, broad openCypher compatibility, and algorithms outside active
Nowledge routes are not implied by production readiness.

Expanding this boundary requires a new specification and evidence plan before
implementation. It is not a TODO implied by completing the current embedded
cutover.
