# Skein TODO

This file contains only actionable, incomplete work. Completed behavior belongs
in the contracts indexed by [`docs/specs/README.md`](docs/specs/README.md), in
supporting design documents, and in Git history. Do not use checked tasks as a
second specification or completion archive.

The scope remains product-driven: Skein is a single-process embedded Rust
database for Nowledge graph and search workloads. Multi-process writers,
distributed replication, cloud-primary execution, broad openCypher coverage,
and algorithms outside active routes are not implied backlog items.

## P0: Production Release Blockers

- [ ] Qualify storage on a representative production replica.
  - Run the typed larger-than-cache resource profile against a representative
    Mem replica with canonical bytes exceeding the configured cache.
  - Record revision, target, feature set, configuration digest, dataset
    fingerprint, steady and peak RSS, page faults, intermediate rows, payload
    bytes, cache residency, spill, and admission counters.
  - The typed read-only Content Store runner now binds frozen SQL cases to the
    production identity, reopens a fresh cold cache per case, retains cold and
    warm runs, and rejects relational row/index generation drift, undersized
    artifacts, cache rejection, leaked pins, invalid engine-measured manifest,
    checkpoint/root, WAL-replay, post-replay, or total-open timing partitions,
    unbounded segment payload-cache activity before the first user query, or
    explicit resource-budget violations. The release validator recomputes the
    bounded-cache predicate from raw counters. Run it against the imported
    representative Skein copy; its synthetic contract test is not production
    evidence. The `skein-content-store-read-qualification` binary now derives
    the fixed read-only authoritative typed configuration from a bounded plan
    while keeping the local path out of evidence; retain this item until the
    resulting production-copy artifact is checked in.
  - The typed writable Content Store runner now requires four distinct
    disposable replicas separate from the read-only source, exercises frozen
    insert/update transactions at exactly 1, 4, 8, and 10 writers, retains
    commit p95, WAL/group-commit, RSS, page-fault, cache, row/index delta,
    replay-open, checkpoint, and manifest-only-open evidence, and rejects more
    than 5% regression against same-shape accepted-revision references. Run the
    matrix on disposable copies of the representative import; synthetic matrix
    coverage is contract evidence only. The
    `skein-content-store-mutation-qualification` binary now binds four
    caller-owned replica paths to a strict, parser-tested plan without copying
    the source or retaining paths and parameters; retain this item until the
    resulting representative matrix artifact is checked in.
  - The typed graph runner now retains every ordered cold/warm resource run,
    its recomputed aggregate, and lifecycle process memory. The final bundle
    rejects missing, truncated, reordered, over-budget, or inconsistent run
    evidence instead of trusting only the last warm profile.
  - Bind the artifact to the release readiness bundle and reject stale or
    mismatched evidence.
  - Acceptance: traffic readiness is derived from the production-copy report,
    not from an ignored synthetic test.

- [ ] Qualify the generation-bound out-of-core search path on production-shaped
  data.
  - Run exact text, vector, and hybrid parity for metadata, lifecycle,
    incremental, checkpoint, reopen, and corruption cases. Include ACL parity
    only when the release feature set enables `acl`.
  - Exercise at least 100,000 documents and a corpus larger than the admitted
    search memory budget.
  - Record P50, P95, and P99 latency, RSS, page faults, posting and sidecar
    bytes, hydration bytes, update latency, and checkpoint amplification.
  - Exercise the bounded generation-delta merge and require zero resident
    corpus documents while old-generation reads overlap publication.
  - Exercise the default 1-bit RaBitQ out-of-core serving path in both
    `Preferred` and `Required` modes, with metadata allowlist pushdown and
    bounded raw-vector late reranking.
  - Run those probes on the exact source generation recorded by the artifact,
    not only on a disposable post-update generation, and record lifecycle RSS
    and page faults before opening the full-residency oracle.
  - Require the selected projection generation, analyzer identity, embedding
    identity, source graph epoch, and release revision to match the evidence.
  - Acceptance: production routes open the out-of-core facade through its
    production constructor and the final cutover report recomputes readiness
    from the bound raw evidence.

- [ ] Qualify the default 1-bit RaBitQ candidate projection on representative
  Mem embeddings.
  - Run `run_production_vector_qualification` on representative embeddings,
    compare default 1-bit candidate recall against canonical raw-vector TopK,
    and record `ProductionVectorRaBitQReferenceEvidence` from
    `crates/qualification/src/production_vector/oracle.rs` as a differential
    dispatch and serving consistency check, not as recall truth.
  - Cover unfiltered, metadata-filtered, incremental, checkpoint, reopen,
    stale-generation, corruption, cancellation, and mixed-load cases. Include
    ACL-filtered cases only when the release feature set enables `acl`.
  - Record candidate recall, final raw-reranked recall, P50/P95/P99 latency,
    steady and peak RSS, page faults, projection bytes, build amplification,
    skipped blocks, admitted workers, and kernel selection.
  - Require evidence for Windows x86_64, Linux x86_64, Linux AArch64, macOS
    AArch64, and the scalar reference before production admission.
  - Bind every target to the exact search generation, document digest, analyzer
    digest, embedding identity, and source graph epoch accepted by the search
    qualification artifact.
  - Keep the full-residency recall oracle offline and require its document,
    analyzer, embedding, and epoch identity to match the released out-of-core
    search generation.
  - Measure the released out-of-core 1-bit RaBitQ artifact before opening the
    oracle; require serving/oracle final-result parity, payload I/O, raw
    reranking, and cancellation propagation from the serving path. Keep
    candidate recall in the identity-bound offline oracle so serving does not
    retain candidate IDs only for qualification.

## P1: Runtime And Availability Hardening

- [ ] Qualify default parallel morsel execution on production-shaped workloads.
  - Require higher throughput without a p99, peak RSS, cancellation-latency, or
    foreground-admission regression across 4, 8, and 16 workers.

- [ ] Close the remaining blocking-operator availability gaps for active
  workloads.
  - Capture production-route evidence for high-cardinality `DISTINCT` and
    Cartesian build sides through `run_production_blocking_qualification`.
  - Require the existing ordered distinct spill and partitioned Cartesian build
    spill to remain within byte, run, cleanup, and admission limits.
  - Accept an in-memory result only when route-bound evidence proves it remains
    within admission; do not weaken the blocking-operator memory limit.

## P1: Persistent Row And Index Storage

The immutable relational index codec, generation-fenced publisher, demand
reader, bounded page-cache integration, WAL recovery delta, differential
qualification, opt-in PostgreSQL SQL execution path, and checkpoint-bound
artifact identity with backup/restore/scrub coverage are implemented. The
remaining work below makes those derived foundations canonical without allowing
a stale index or a database-sized resident set to become a correctness
dependency. The storage-owned relational snapshot reader now binds one exact
checkpoint, recovery delta, and immutable live view; it performs bounded
live-over-recovery-over-checkpoint point and ordered range reads without a
database-sized base-row collection. Ordinary live admission failures reject
before WAL append, while schema-changing WAL is followed by a mandatory
manifest-last canonical row checkpoint that writable recovery retries after a
crash; read-only recovery remains fail closed.

- [ ] Complete cross-platform row/overflow-root lifecycle evidence.
  - [x] Inject durable-replace failure at the exact row-page and overflow
    generation-manifest destinations and prove that neither incomplete
    candidate becomes selectable.
  - [x] Run the canonical large-`TEXT` backup/restore, pinned-reader retention,
    physical-closure reclaim, and reopen regression through the existing
    macOS/Windows storage-platform test target.
  - [ ] Retain a green Windows CI artifact for this revision; local non-Windows
    execution and CI wiring do not substitute for Windows sharing semantics.
    The workflow now retains the bound resource report, exact row/overflow
    lifecycle output, and independent statuses in one revision-named artifact;
    keep this open until the remote Windows run supplies that artifact.

- [ ] Qualify canonical row pages for the first Mem relational tables.
  - The SQL runtime now selects one exact checkpoint/recovery/live snapshot
    reader, rejects an unavailable reader, and uses canonical memory only before
    the first checkpoint or inside a transaction-private workspace. Keep this
    engine contract while qualifying the product cutover.
  - The selected row-page generation is now self-describing: each checksummed
    table root carries the complete digest-bound schema, non-zero column count,
    and exact descriptor-derived row count. New tables without schema bytes and
    incremental schema drift fail before artifact creation. A read-only
    `OutOfCore` plus `Authoritative` open now validates the current row and index
    views and then releases its transitional materialized checkpoint rows while
    retaining schemas and exact logical counts. When no WAL record follows the
    checkpoint, it now builds that sparse state directly from canonical root
    metadata and avoids the full row decode. Published row deltas now carry the
    exact final per-table counts required to reconstruct that state after WAL.
    Row and index recovery manifests also bind the exact WAL generation, LSN
    interval, and ordered-record digest, so an epoch-only derived artifact
    cannot be reused. An `OutOfCore` plus `Authoritative` reopen now avoids a
    database-sized mutation workspace for both read-only and writable handles:
    it validates the whole WAL source, keeps only schema/count metadata, adopts
    row counts from the fenced row manifest, and fails closed on DDL, snapshot
    WAL, missing artifacts, or source drift. Writable recovery sparsely hydrates
    the exact replay access set into bounded row and index deltas, and later
    schema-stable transactions use bounded private row and index overlays.
  - Metadata-only checkpoints now resolve dirty keys from the final live
    overlay, publish only dirty row pages, conservatively retain the pinned
    overflow base, rebuild required indexes by batch-scanning the new row root
    through the spillable index builder, and omit the legacy full-row artifact.
    Exact full-scan overflow compaction is now a separately admitted typed
    maintenance operation with zero-hydration closure scanning, bounded
    external sorting, fresh physical rewrite, manifest-last selection, pinned
    reader retention, and TLA+ coverage. A typed production collector now runs
    on a caller-owned disposable replica, binds the exact initial identity,
    verifies frozen SQL digests before compaction, after publication, and after
    reopen, advances one Cypher marker plus a later checkpoint, scrubs the
    selected closure, and records reclaimable descriptors, physical deletion,
    RSS, page faults, elapsed time, governor admission, and write amplification.
    Its thin bounded CLI now emits the declared scan/spill/rewrite policy, and
    the release bundle independently revalidates raw evidence from distinct
    explicit 512 MiB capability and dynamic 8 GiB shared-host runs. The shared-host
    run keeps automatic capacity at or below 2 GiB while permitting pressure
    to reduce its budget below the nominal 1--2 GiB range.
    Retain this item until representative production-copy reports under the
    two profiles have been collected, retained, and accepted by that policy.
  - The typed runner now qualifies `content_documents`, `thread_messages`,
    `content_chunks`, and `content_anchors` using the frozen PostgreSQL statement
    corpus and graph-plus-relational commits that publish one shared epoch.
  - The four-table runner now proves authoritative checkpoint/reopen,
    exact per-statement row/payload admission, cold/warm result identity, WAL
    recovery delta, live row overlay, cache accounting, and zero leaked page
    pins. Source chunks retain stable source/chunk ordering, and anchors retain
    occurrence identity even when two messages share one legacy `message_id`.
    The `upsert_source_chunks` caller now has complete mixed-transaction
    evidence for shorter and empty whole-document replacement, duplicate-order
    statement rollback, graph/document count agreement, live visibility, and
    checkpoint/reopen identity.
    The `patch_source_chunks_space` caller now has complete mixed-transaction
    evidence for graph/document workspace agreement, read-your-own-writes,
    exact chunk-count and payload preservation, missing-owner no-op behavior,
    live visibility, and checkpoint/reopen identity.
    The `patch_thread_space_ownership` caller now has a guarded mixed-source
    batch qualification: graph Thread, relational document, and messages agree
    in one epoch, matching previews move, a stale preview remains unchanged,
    and non-ownership payloads survive live reads and checkpoint/reopen.
    The `patch_moved_space_ownership` caller now composes selected Threads and
    Sources in one guarded mixed transaction, reports exact document/message
    changes, proves stale selection remains unchanged, and retains payload and
    ordered-read identity through checkpoint/reopen. The `delete_thread_tail`
    caller now proves bounded ordered candidate identity, negative-start
    clamping, empty-tail no-op behavior, rollback atomicity, exact graph and
    relational summary agreement, retained payload identity, live tombstones,
    and checkpoint/reopen identity. The `delete_thread_messages` caller now
    proves bounded empty-owned/message-only document discovery, missing/repeated
    preflight no-ops without an epoch advance, rollback and workspace
    read-your-own-writes, graph Thread/identity/Message deletion and relational
    anchor/message/empty-document deletion in one durable epoch, unrelated
    payload identity, live tombstones, and checkpoint/reopen identity. The
    frozen corpus now has no `partial` callers.
  - Require checkpoint/reopen, WAL replay, corruption, cancellation, and
    locking evidence before enabling the path by default. Prove that a declared
    bounded workload runs within the supported 512 MiB low-memory profile, but
    evaluate cold/warm latency, RSS, page faults, and write amplification on the
    production replica's actual configured resource profile; 512 MiB is not a
    universal activation cutoff.
  - The authoritative transaction core now pins one persistent base and merges
    bounded private row and index overlays across SQL statements. A rejected
    statement leaves both overlays unchanged, while successful statements expose
    read-your-own-writes without rematerializing checkpoint rows. The typed
    runner executes a Content Store-shaped message UPSERT, page read, rejected
    foreign key statement, payload aggregate, and document-summary update as one
    group, and proves transaction-workspace routing plus atomic publication. The
    same runner now proves a cancelled Content Store point read is
    non-poisoning and pin-clean, and that `FOR UPDATE` makes a same-key UPSERT
    time out and abort without changing the row or commit epoch. A disposable
    backup/restore probe now bit-flips the current row-page artifact, requires
    scrub to poison the damaged handle, rejects later SQL, and proves the source
    database remains unchanged. The resource probe now records its declared and
    detected memory profile, all relevant query/cache budgets, warm-read
    percentiles, steady/peak RSS, page faults, WAL bytes, new immutable
    generation bytes, and a clearly labelled durable-write lower bound. A
    unified storage residency snapshot now reports the current relational row,
    overflow, index, recovery-delta, and live-overlay layers without scanning
    candidate files, so the future production-copy runner can prove that the
    selected relational artifacts exceed cache and remain epoch-aligned. A
    regular in-process run does not certify an OS-enforced 512 MiB limit: retain
    the isolated constrained-profile run and production-copy measurements as
    separate evidence gates.
  - Qualify `SharedHost` separately on an 8 GiB host: automatic Skein
    capacity must remain at or below 2 GiB, while the effective budget tracks
    sensed headroom and is expected to move through the 1--2 GiB range rather
    than becoming a fixed reservation. Keep the explicit 512 MiB run as a
    supported low-memory capability profile, not as the default or a universal
    release threshold. The typed memory-policy evaluator now rejects a mismatched
    detected limit, derives the exact 25% capacity/headroom budget, records the
    nominal 1--2 GiB range without turning 1 GiB into a hard floor, and proves
    that an explicit 512 MiB Skein ceiling is effective unless a smaller
    host/cgroup ceiling takes precedence. The identity-bound production matrix
    and `skein-content-store-memory-qualification` collector now evaluate both
    fixed policies from one detected snapshot without opening or mutating the
    database. The final release bundle now independently requires that matrix,
    one production-profile read artifact, and one explicit 512 MiB capability
    read artifact for the same release identity; it recomputes the policy
    budgets and rejects profile substitution. Retain this item until real
    representative artifacts are checked in; synthetic fixtures and policy
    reports alone do not prove peak RSS.
- [ ] Differentially qualify and production-activate persistent graph indexes.
  - Normal open admits aggregate encoded canonical, spill, and
    property-projection manifest bytes before allocation, verifies each selected
    manifest from one bounded byte image, and exposes the configured limit plus
    selected bytes. Canonical adjacency no longer keeps its descriptors in that
    graph-size-dependent image: descriptor page v1 provides bounded
    leaf/interior pages, strict key/value/range admission, and bound
    CRC32C/SHA-256 verification. The checkpoint streams descriptors through
    bounded level runs, publishes its page artifact before a checksummed root,
    and activates the exact root and adjacency artifact through the outer
    manifest. Normal open reads only the compact root; endpoint scans use the
    bound demand reader and shared cache, while deep scrub verifies the complete
    uncached closure. Canonical adjacency v1 rejects cross-generation page
    references until retained-closure reclamation is formalized. Property
    projection checkpoints now also stream all six node/relationship index
    classes into bounded descriptor pages, publish the root after the data
    artifact, and bind that root through the compact selected manifest. Normal
    open retains no block vector; estimates and execution demand-scan ordered
    descriptor prefixes, while deep scrub, backup, and derived repair validate
    the full data/page/root closure. Property spill checkpoints now stream a
    same-generation descriptor tree, publish data before pages/root,
    bind the root through the selected property manifest, and include the
    closure in normal-open root verification, scrub, backup, discard, and
    reclamation. The production spill reader now opens a compact manifest,
    lower-bound seeks one descriptor under independent limits, reads one block,
    exposes descriptor/data cache and I/O accounting, and uses exhaustive
    uncached deep scrub with sticky physical-failure poison. Canonical segment
    checkpoints now publish an order-preserving descriptor tree after the data
    artifact and bind its exact root through a compact selected manifest.
    Normal open verifies only the bounded root. Point lookup lower-bound seeks
    one descriptor, scans advance through fixed-size bounded descriptor
    batches, and reports separate descriptor/data cache and I/O. Deep scrub and
    backup hash the complete uncached tree and canonical artifact, decode every
    segment, prove aggregate count/range closure, and sticky-poison physical
    failure without poisoning admission. Collect representative
    production-copy evidence before claiming that all graph/index startup
    metadata residency is independent of entry count.
  - The independent general graph-storage release artifact now has a bounded
    `skein-graph-storage-qualification` collector over the existing typed
    runner. It binds one caller-owned read-only database to a strict plan,
    derives `ShadowReadOnly + OutOfCore`, keeps the shared-host governor dynamic,
    and treats explicit 512 MiB execution as a separate capability profile.
    Retain this item until a representative business-shaped graph artifact and
    the all-class index matrix are checked in for the same release identity.
  - Equality, range, full-text, ordered composite-equality, relationship
    equality/range, forward adjacency, and reverse adjacency projections are
    generation-bound and demand-paged. Relationship probes choose the property
    projection only when its estimated work does not exceed the bound endpoint
    adjacency path.
  - The Skein Lightning physical-id to stable-identity sidecar is separately
    publish-last and demand-paged because it must become durable before an
    initial-import graph WAL batch. It is not a query index and is not bound to
    a checkpoint that does not yet contain that import.
  - Activate each query index class independently after differential, recovery,
    cache-budget, and production-shaped evidence; derived BM25, vector,
    statistics, analytics, and optional columnar projections remain outside
    canonical recovery.
  - The core differential oracle now compares all eight selected persistent
    readers with canonical fallbacks on one pinned snapshot. Store-owned atomic
    counters survive read-snapshot cloning and expose per-class operations plus
    property/adjacency block, byte, decode, candidate, and layout totals through
    storage resource profiles. The typed production runner can bind one required
    class to an offline reference digest/row count and per-run block/byte
    budgets without retaining result rows. It now requires distinct cold and
    warm runs, proves the relevant artifact exceeds the cache, and records a
    bounded non-poisoning cancellation followed by a successful read.
    `SkeinGraphIndexQualification.tla` proves the ordered cache lifecycle,
    independent same-generation evidence gating, complete matrix publication,
    and fail-closed selected corruption. The typed production matrix rejects
    missing, duplicate, mixed-replica, mixed-runtime, or mixed-generation cases
    before measurement and reports readiness only when all eight independent
    class cases pass. The final release bundle now requires this matrix and
    revalidates every raw case, including its declared per-run I/O budgets,
    rather than trusting top-level readiness. Retain this item until
    representative production-replica cases for every class are checked in.
    The `skein-graph-index-qualification` binary now binds one caller-owned
    read-only path to a strict, parser-tested all-class plan and derives fixed
    `ShadowReadOnly + OutOfCore` options without retaining queries, parameters,
    rows, or paths; synthetic fixtures prove the contract, not production
    readiness.

## P1: PostgreSQL-Dialect Relational Content Store

This work reopens a deliberately deferred boundary: selected durable row and
large-value responsibilities currently owned by Mem's SQLite `content.db` may
move into Skein. PostgreSQL defines the SQL syntax and semantics; it is not a
runtime dependency and this backlog does not replace PostgreSQL in Nowledge
Cloud. The initial scope is `content_documents`, `thread_messages`,
`content_chunks`, `content_anchors`, and their migration state. External
artifact/blob files remain sidecars until a separate workload and recovery
qualification justifies moving them.

- [x] Qualify the partial callers in the frozen SQLite-to-Skein statement
  corpus.
  - The versioned corpus, schema-v4 canonical projection, explicit source-only
    control-table exclusions, parameter/result contracts, source inventory,
    and revision digest are specified by
    `docs/specs/POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`.
  - Every graph-plus-relational write caller is now `covered` by independent
    runtime evidence rather than parser feature counts. This includes source
    replacement, ownership moves, Thread message UPSERT and reconciliation,
    tail deletion, and whole-thread deletion.
  - Acceptance: every active Mem caller is `covered`, and cutover fails closed
    when the corpus protocol, revision, or digest differs from the qualified
    Skein artifact.

- [ ] Materialize the scoped Content Store schema and behavior in Skein.
  - Define PostgreSQL-dialect migrations for `content_documents`,
    `thread_messages`, `content_chunks`, `content_anchors`, and the durable
    migration ledger without editing an already-applied migration.
  - Preserve occurrence identity, stable ordering, content hashes,
    distillation exclusions, metadata text, source-chunk ownership, and legacy
    anchor matching semantics.
  - Replace SQLite repository helpers with small named SQL statements for exact
    lookup, bounded page, aggregate/count, tail guard, candidate page, and
    bounded hydration phases.
  - Route graph identity/relationship changes and their content rows through
    the canonical mixed commit already owned by `GraphStore`; do not retain a
    host-side dual-write boundary after cutover.
  - Acceptance: thread append/reconcile/tail delete, whole-thread delete,
    source-chunk replacement, ownership move, anchor creation, and projection
    rebuild all have exact behavior fixtures.

- [ ] Add an idempotent, resumable SQLite-to-Skein migration coordinator in
  Mem.
  - Keep `rusqlite` and SQLite snapshot handling in the Mem adapter; Skein must
    not acquire SQLite as a production dependency.
  - Acquire an explicit legacy write fence or run a durable dual-write
    obligation protocol before copying. Record database identity, schema
    checksums, source snapshot identity, import id, and source high watermark.
  - Copy by stable keyset pages with bounded payload bytes, persist the cursor
    after each committed Skein batch, and make replay idempotent by primary key
    and content hash.
  - Verify per-table counts, ordered identities, payload hashes, aggregate
    totals, anchor reachability, and representative query results before
    generating cutover evidence.
  - Preserve the SQLite database unchanged through qualification and rollback;
    destructive cleanup is a later, separately authorized step.
  - Acceptance: restart at every page and cutover boundary converges without
    missing or duplicate rows, and writes accepted during migration are either
    replayed or prevent cutover.

- [ ] Close the production evidence and decommissioning gates.
  - Bind the Mem thread-detail shadow to the typed bounded-snapshot report and
    require at least one successful Cypher statement plus the summary and page
    SQL statements from the same commit epoch. Recompute the row and payload
    budget arithmetic in the integration evidence; search-bearing observations
    must also require a non-zero external vector-seed execution count. Do not
    accept a graph-only route report as proof of the mixed read.
  - The final release bundle now requires the identity-bound memory-policy
    matrix, distinct production and explicit 512 MiB read-only Content Store
    artifacts, and the isolated 1/4/8/10-writer mutation-replica matrix. It
    independently revalidates memory derivation, profile separation, frozen
    statement contracts, raw cold/warm I/O and resource runs, runtime admission,
    writer sequences,
    latency percentiles and regression, WAL/group accounting, replay deltas,
    manifest-only reopen, checkpoint folding, and verification parity instead
    of trusting child `ready` fields. Representative retained artifacts are
    still required; synthetic fixtures establish only the evaluator contract.
  - Add a differential oracle that runs the frozen statement corpus against one
    SQLite snapshot and one Skein snapshot, comparing values, nulls, ordering,
    errors, and transaction outcomes rather than only row counts.
  - Exercise empty stores, duplicate legacy message ids, duplicate order
    indexes, non-ASCII and large content, 50,000-message threads, large source
    corpora, protected tail deletion, cross-space anchors, interrupted writes,
    torn WAL tails, checkpoint/reopen, backup/restore, and corruption.
  - Benchmark cold and warm P50/P95/P99 latency, throughput, database and WAL
    bytes, write amplification, compression ratio, steady/peak RSS, page
    faults, spill, and decompressed bytes on Windows, Linux, and macOS.
  - Run shadow reads and durable dual writes on a production copy, bind parity
    and recovery artifacts to the exact Skein revision and statement-corpus
    revision, then fail closed on stale or incomplete evidence.
  - Remove the SQLite runtime, backup/export branch, and `nmem-content`
    repository only after portable export/import, doctor, projection rebuild,
    rollback, and route ownership all select Skein with no fallback.

## P1: PostgreSQL SQL/PGQ Compatibility

- [ ] Add the Skein-owned PostgreSQL syntax frontend specified by
  `docs/specs/POSTGRES_SQL_PGQ_SPEC.md`.
  - [x] Add dependency-free token, byte-span, structured-error, and syntax-AST
    ownership under `crates/`.
  - [x] Structurally parse PostgreSQL SQL/PGQ `CREATE PROPERTY GRAPH` and
    standalone `GRAPH_TABLE`, including labels, properties, graph paths,
    predicates, edge directions, and quantifiers.
  - [x] Embed `GRAPH_TABLE` into a bounded owned PostgreSQL `SELECT` syntax
    envelope with explicit statement-family routing, outer projection/filter/
    grouping/order/limit/locking spans, and PostgreSQL-derived positive,
    negative, and raw-parse-versus-bind-stage cases.
  - [x] Replace opaque expression spans with a bounded Pratt AST, add
    inner/left/cross joins, and add read-only catalog-driven binding for graph
    slots, labels, properties, correlated columns, typed output schemas, and
    PostgreSQL raw-parse-versus-bind failures.
  - [ ] Add the remaining workload-qualified `SELECT` grammar and complete the
    parser/binder differential corpus before routing production SQL execution
    to the owned parser.
  - [x] Keep existing relational SQL on upstream `sqlparser` until each statement
    family has equivalent positive and negative owned-parser coverage; never
    retry a second parser after a failure.
  - [ ] Lower SQL/PGQ and Cypher into the same typed graph logical IR, optimizer,
    executor, snapshot, admission, cancellation, and explain pipeline.
    - [x] Lower the qualified single-path, single-label, single-hop
      `GRAPH_TABLE` subset into the shared `NodeScan`, `Expand`, `Filter`, and
      `Project` operators with explicit parameter binding and source-spanned
      fail-closed errors for unrepresented semantics.
    - [ ] Integrate the surrounding PostgreSQL relational plan, shared snapshot,
      admission, cancellation, explain, and equivalent-plan qualification
      before enabling owned-parser production routing.
  - [ ] Add property-graph catalog durability, information-schema views, bounded
    fuzzing, PostgreSQL differential tests, and recovery qualification.
    - [ ] Define transactional Property Graph descriptors over stable source
      table/property identifiers, keys, endpoint mappings, labels, and property
      exposure. Do not reuse analytics `ProjectedGraphDefinition`, which is a
      rebuildable materialization selector and lacks semantic catalog identity.
    - [ ] Publish descriptor creation through the source-schema transaction,
      WAL, checkpoint, backup/restore, and system-schema upgrade boundary, then
      add incompatible-source-DDL rejection and checkpoint/reopen coverage.
  - Acceptance: active SQL/PGQ statements produce PostgreSQL-compatible rows
    and error classes without a separate executor or public `query_gql` API.

## P2: Deferred Delivery Governance

- [ ] Adopt `main` branch protection when Skein enters a release-candidate or
  general-availability phase.
  - Keep direct pushes available during the current rapid-iteration phase.
  - Production release evidence must still bind to an exact revision with green
    required CI; an unprotected branch is not evidence that a red revision is
    production ready.
  - Before general availability, require pull requests, supported Cargo and
    Bazel checks, and prohibit force pushes and branch deletion.

## P2: Deferred Correctness Oracles

- [ ] Add NoREC only after a supported Cypher or SQL subset can express the
  general row-wise boolean-count relation without a fuzz-only executor.

## P2: Deferred Replica Repair

- [ ] Add replica-assisted storage repair after Skein has a replication layer.
  - Require an exact database identity, manifest lineage, generation, LSN range,
    and content-digest match before accepting repair bytes from a follower.
  - Stage and verify replacement WAL or segment data before atomic publication;
    never extend tolerant open into an implicit replica-repair path.
  - Record the source replica, repaired range, and old/new digests in a durable
    repair audit record.
  - Fall back to verified backup restore when no follower covers the missing
    commit. Any lossy salvage must target a new database directory and require
    explicit authorization while preserving the original database unchanged.
