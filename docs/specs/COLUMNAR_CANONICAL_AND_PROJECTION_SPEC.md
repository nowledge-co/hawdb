# Columnar Canonical Storage and Durable Projection Framework Specification

This contract redefines Skein's durable canonical representation as columnar
storage (Kuzu-style node groups) and generalizes the search-specific
projection machinery into a durable, incrementally maintained projection
framework (LanceDB-style base + delta artifacts). It supersedes the following
clauses of existing contracts, which must be amended in the same change that
lands each affected capability:

- `VECTORIZED_MORSEL_EXECUTION_SPEC.md`: the prohibition on a second durable
  columnar representation is repealed. The columnar representation defined
  here becomes the **only** canonical durable representation; executor
  columnar batches become zero-copy views over it.
- `../STORAGE.md`: the "no columnar property segments" non-goal is repealed.
- `POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`: the relational store's private
  checkpoint (`SKRLCKP1`), private WAL (`SKRLWAL1`), and private overflow
  (`SKOVFL01`) formats are replaced by the unified formats defined here.

Normative `MUST`, `MUST NOT`, `SHOULD`, and `MAY` clauses take precedence
over descriptive implementation notes, per `docs/specs/README.md`.

## 1. Goals and non-goals

Positioning: Skein is an **embedded, TP-first HTAP engine for operational
knowledge workloads**. The request mix it replaces (Nowledge Mem over
Kuzu + SQLite + LanceDB) is TP-shaped — point CRUD, short read-modify-write
graph transactions, paginated reads, bounded hybrid search — while resource
peaks are dominated by background AP jobs (graph algorithms, reindexing,
projection rebuilds). The contract therefore optimizes foreground
transactional latency first, admits analytical capability through the same
columnar representation second, and treats the unified snapshot across
relational, graph, and search-served reads as the property that
distinguishes one engine from three glued ones.

Goals:

1. Columnar canonical storage for graph and relational data with per-column
   compression, zone maps, and chunk-granular I/O.
2. Declared-only property indexes that are durable across restart.
3. A generic projection framework in which every rebuildable artifact
   (vector, lexical BM25, range, equality, adjacency, statistics) is durable,
   incrementally maintained, and provably equivalent to a full rebuild.
4. SQLite-capability parity for the relational content store under the same
   engine (point CRUD, secondary indexes, transactions, large values), so
   Nowledge Mem retains BM25 full-text search (including the jieba CJK
   analyzer) and its content-store surface.
5. Transactions with snapshot isolation across graph and relational data in
   a single commit.
6. Bounded memory: every unbounded structure is either bounded with
   backpressure or spillable; RSS has a hard budget and pressure degrades
   service instead of failing it.
7. Performance strictly better than the current row-oriented implementation
   on the normative gates in §10.

Non-goals: distributed replication (see `SKEIN_CRDT_REPLICATION_SPEC.md`),
changes to the Cypher/SQL language surface, and changes to the embedded
ownership model in `EMBEDDED_RUNTIME_SPEC.md`.

## 2. Terms

- **Node group**: fixed-capacity run of records of one table (label or
  relational table), ordered by record id, stored column-wise.
- **Column chunk**: the encoded values of one declared property for one node
  group, plus validity bitmap and zone map.
- **Deletion vector (DV)**: per-node-group bitmap marking rows deleted or
  superseded after the group was written. Generation-scoped copy-on-write.
- **Delta group**: small columnar group produced by flushing the memtable;
  the LSM L0 of this design.
- **Memtable**: the in-memory row-form delta (`CowSegmentedMap`) between
  checkpoints, including tombstones.
- **Projection**: a durable artifact derived as a pure function of canonical
  state at an epoch, plus a cursor recording that epoch.
- **Commit epoch**: monotonically increasing transaction commit counter; one
  WAL batch per epoch.
- **Generation**: monotonically increasing checkpoint artifact identity
  (`ManifestGeneration`).

## 3. Columnar canonical storage

### 3.1 Layout

1. Canonical node and relationship data MUST be stored as node groups per
   table. A graph label is a table; a relational table is a table. Node
   groups MUST be non-overlapping in record-id range and internally
   id-ordered, preserving the current segment ordering invariants.
2. Each declared property (a property with a `PropertyDescriptor` in the
   catalog) MUST be stored as a column chunk per node group. Column chunks
   MUST carry: physical encoding id, validity bitmap, zone map, content
   digest, and byte extent (offset/length) addressable independently of
   sibling chunks.
3. Undeclared properties MUST be preserved in a **residual column** per node
   group: a row-major encoded blob column keyed by `PropertyId` (interned
   key), retaining full schema-flexible semantics. Reads of undeclared
   properties MUST NOT require decoding declared columns and vice versa.
4. Property keys MUST NOT be stored inline as strings in canonical records.
   Keys MUST be interned (`PropertyId` or a per-artifact dictionary) in all
   canonical encodings, including the residual column.
5. Multi-label nodes: each node belongs to exactly one **primary table**
   (its primary label). Secondary labels MUST be stored as a label-set
   column (bitmap or dictionary-coded set). A scan by label L MUST consult
   both the primary table of L and the label-set columns of other tables
   that are known (via catalog statistics) to contain L as a secondary
   label.
6. Values larger than an inline threshold MUST be stored in a blob store
   addressed by spill reference; the column chunk stores
   `(length, inline bytes | blob ref)`. Scans that do not project the blob
   column MUST NOT read blob bytes. This unifies the existing
   `property-spill` and relational overflow (`SKOVFL01`) mechanisms.
7. Adjacency MUST be stored as per-node-group CSR (forward and backward):
   offsets column plus neighbor/rel-id columns, rebuildable from canonical
   data, maintained as a projection under §6.
8. Relational tables whose primary key is monotonically assigned (e.g.
   append-ordered thread messages) MUST assign record ids in primary-key
   order so node groups are primary-key-clustered: an ordered paginated
   read is then one group-contiguous range read. Ordered scans MUST
   terminate early once a `LIMIT` is satisfied instead of draining the
   group.

### 3.2 Physical encodings

1. A physical type lattice MUST exist beneath the logical `Value` types.
   Minimum encodings: plain, dictionary, run-length, bit-packed integers,
   and a string table encoding; chunk bodies MAY additionally be
   block-compressed (zstd). Encoding choice is per chunk and recorded in the
   chunk directory.
2. Zone maps MUST reuse the `FieldSummary` model
   (`crates/storage/src/scan/summary.rs`): numeric/datetime min-max,
   dictionary stats, membership filters. The scan planner MUST prune at
   chunk granularity through the existing `SegmentPruner` /
   `ScanSegmentManifest` contract, and pruning MUST be sound: a pruned chunk
   MUST NOT contain a qualifying row (model:
   `SkeinPropertyIndexPruning.tla`).
3. Point reads MUST decode only the chunks of requested columns. Full-row
   reconstruction is a projection over per-column reads, not a decode of
   the whole group.
4. `COUNT(*)`, per-table, and per-label cardinalities MUST be answerable
   from group metadata (record counts combined with deletion-vector
   cardinality and memtable deltas) without reading column chunks.

### 3.3 Updates: deletion vectors and delta groups

1. Committed mutations MUST NOT rewrite published node groups. A checkpoint
   MUST publish: (a) new delta groups holding flushed memtable rows, (b) new
   generation-scoped deletion vectors marking superseded base rows, and (c)
   references to untouched groups from the previous generation without
   copying their bytes.
2. Deletion vectors are part of the generation that publishes them. A reader
   pinned to generation G MUST observe exactly G's deletion vectors. Between
   checkpoints, deletes exist only as memtable tombstones.
3. The read path for any table is the ordered merge:
   `base groups ⊗ DV  ∪  delta groups  ∪  memtable`, filtered by tombstones,
   exactly one visibility rule shared by graph and relational access
   (model: `SkeinCompactionVisibility.tla`).
4. Background compaction MUST merge delta groups and high-deletion groups
   into standard groups. Compaction MUST be an identity transform on visible
   state at every epoch (§5.4) and MUST respect generation pinning and the
   reclamation rules of `SkeinGenerationReclamation.tla`.
5. Checkpoint write amplification MUST be proportional to the volume of
   change since the previous checkpoint (delta rows + DV bitmap bytes +
   manifest), not to the size of touched groups or of the database.

### 3.4 WAL unification

1. The engine MUST use a single binary WAL for graph and relational
   mutations, with explicit length-framed records, per-record checksums, and
   the torn-tail semantics currently modeled in
   `SkeinStorageDurability.tla` / `SkeinWalDoctor.tla`. Those models MUST be
   updated from newline framing to length framing in the same change.
2. Group commit semantics (`WalSyncGroupState`, bounded follower wait) are
   unchanged. The relational store MUST NOT retain a private WAL.

## 4. Declared indexes

1. Only declared indexes exist (`CREATE INDEX ...`); the write path MUST NOT
   index undeclared (label, property) pairs. `IndexKind::{Equality, Range,
   FullText}` and composite indexes keep their current declaration surface.
2. All declared indexes MUST be durable as projections (§6). Opening a store
   MUST NOT require rebuilding any declared index whose projection is
   current; residual catch-up is bounded by the crash window (§6.5).
3. Equality results MUST NOT be served from a Range projection. An equality
   projection MUST cover every encodable equality value; on encountering an
   unencodable or over-limit key it MUST mark itself
   `Incomplete{reason}` and reads MUST fall back to canonical scan for the
   affected definition (this generalizes the existing
   `PersistentPropertyProjectionManifest` completeness flag).
4. Zone maps are not indexes: they prune scans of any property, declared or
   not, but MUST NOT be treated as complete secondary indexes.

## 5. Transactions

1. Commit protocol is unchanged: private write set → single WAL batch per
   commit epoch → group-commit fsync → memtable apply. Columnarization
   happens only at checkpoint and MUST NOT add work to the commit path.
2. Snapshot isolation: a reader snapshot is
   `(pinned generation, commit epoch, memtable COW snapshot)`. Optimistic
   and pessimistic modes, the lock table, and deadlock victim selection are
   unchanged (`SkeinTransactionConcurrency.tla`).
3. A single transaction MAY mutate graph and relational data; the combined
   mutation set MUST commit atomically in one WAL batch (cross-model
   transaction).
4. **Compaction identity invariant**: for every commit epoch e and every
   interleaving of flush/compaction with concurrent transactions, the
   visible state at e is unchanged by flush and compaction
   (`SkeinCompactionVisibility.tla`).
5. Transaction write sets beyond a configured byte threshold MUST spill to
   private spool files; a transaction MUST be able to read its own spilled
   writes. Large transactions MUST NOT expand the memory budget (§8) and
   MUST NOT block unrelated transactions.
6. **Read-your-own-writes**: every read inside a transaction — canonical
   scan, index seek, or projection-served access — MUST observe the
   transaction's own uncommitted writes overlaid on its snapshot. An access
   path that cannot overlay the private write set for a given predicate
   MUST fall back to one that can rather than serve a stale result.

## 6. Durable projection framework

### 6.1 Ownership and identity

1. Every rebuildable derived artifact MUST be a projection under this
   framework: vector (TurboQuant), lexical BM25, equality, range, full-text,
   composite, adjacency, and statistics projections.
2. Projection state MUST NOT be written to the WAL and MUST NOT be embedded
   in canonical storage. Projections MAY read the WAL. A projection is
   always reconstructible as a pure function of canonical state.
3. Identity: `ProjectionIdentity { kind, generation, source_commit_epoch,
   config_digest }`. `config_digest` MUST cover every input that changes
   output bytes (analyzer version and lexicon digest for lexical, embedding
   model/version for vector, encoding parameters otherwise). A mismatch
   MUST invalidate the projection and schedule a full rebuild.

### 6.2 Durable layout

Each projection owns a directory:

```
projections/<kind>/
├── manifest.skein                  # sole publication point, atomic replace
├── base.<gen>.skein                # coverage [0, base_epoch]
└── delta.<gen>.<n>.skein           # coverage (e_i, e_{i+1}], append-ordered
```

1. The persisted manifest MUST record: identity, bound canonical
   generation, base artifact (file, digest, coverage), ordered delta
   artifacts (file, digest, coverage), `cursor_commit_epoch`
   (= max coverage), and completeness.
2. Publication MUST follow the existing durable protocol: temp file →
   fsync → rename → parent directory sync, single-file atomic manifest
   replace, publish lease against concurrent publishers, and
   active + previous generation GC.
3. Artifact segment format MUST follow the checksummed-footer pattern of
   `skein-vector-projection` (`SKTQ4F02`): column-wise segment bodies, JSON
   manifest footer, length + CRC + magic.

### 6.3 Incremental maintenance

1. The canonical store MUST expose a **projection changefeed**: per commit
   epoch, the upserted and deleted record ids, generalized from
   `SearchProjectionGraphChange`. The changefeed is memory-resident and
   bounded; it is NOT durable state.
2. The only durable incremental progress state of a projection is
   `cursor_commit_epoch` inside its manifest. There MUST NOT be a durable
   changefeed log.
3. Incremental build loop: read changefeed `(cursor, target]` → stream a
   delta artifact within a bounded memory budget → fsync → publish a new
   manifest with the appended delta and `cursor = target`. Cursor advance
   and artifact publication MUST be the same atomic manifest replace.
4. Merge: when delta count or bytes exceed thresholds, a background merge
   MUST rewrite base + deltas into a new base with identical coverage.
   Merge MUST be an identity transform on served results.
5. Query-time freshness: a projection serves
   `base ∪ deltas ∪ catch-up over (cursor, current]` where catch-up is
   evaluated from the live changefeed/memtable. Served results MUST equal a
   full rebuild at the query epoch (`SkeinProjectionDurability.tla`).
6. **Unified snapshot**: all reads issued by one query or transaction —
   relational, graph, and projection-served (vector, lexical, index) —
   MUST evaluate against the same snapshot `(generation, commit epoch)`.
   In particular, hydrating search candidates against canonical records
   MUST read the epoch the candidates were evaluated at, never a later
   one. This is the property the replaced three-engine deployment
   (authoritative graph + relational sidecar + rebuildable search
   projection) cannot provide, and it MUST hold for every combination of
   access paths the planner may choose.

### 6.4 Restart and crash semantics

1. On open, each projection manifest is validated (digests, identity,
   generation binding), then:
   - `config_digest` mismatch → full rebuild in background; the projection
     reports `Incomplete` and reads use the fallback path meanwhile.
   - cursor within the replayable window (`cursor ≥ replay floor`) → mount
     base + deltas and catch up from WAL replay; ready without rebuild.
   - cursor below the floor → generation-diff catch-up if the pinned base
     generation is still available, else full rebuild.
2. Restart cost for a current projection MUST be proportional to the crash
   window's change volume, not to data size.
3. Crash at any point MUST leave the previous manifest intact (atomic
   replace); incremental builds are idempotent from the persisted cursor.
4. Projection readiness MUST NOT block store open; readiness is reported
   per projection through the existing readiness surface.

### 6.5 Reclamation coupling

1. `StorageReclamationWatermark` gains `oldest_projection_cursor`. WAL and
   generation reclamation MUST NOT pass the oldest projection cursor,
   **except** when a projection lags beyond a configured staleness bound:
   then reclamation MAY proceed, and the projection MUST be marked
   `Incomplete{stale}` and scheduled for full rebuild. A dead projection
   MUST NOT pin the WAL indefinitely.

### 6.6 Lexical BM25 projection

1. BM25 lexical search (used by Nowledge Mem) MUST be carried by a
   `LexicalBm25` projection kind: per-segment posting lists (existing
   `SKEINLEXICAL0001` block format), per-segment collection statistics
   (document count, total token count, per-term document frequency).
2. Scoring MUST aggregate collection statistics across base + delta
   segments at query time; deleted documents MUST be excluded from both
   candidates and statistics denominators visible to ranking, either
   exactly or within a bound restored by the next merge. The chosen bound
   MUST be stated and validated by the existing recall-validation gate.
   Lexical and vector projections MUST bound their delta-segment count so
   query-time statistics and candidate merging stay proportional to a
   configured segment budget, not to write history.
3. Hybrid search execution (ANN + lexical + fusion) MUST run its branches
   under one execution-budget admission with explicit candidate limits and
   timeouts; predicate filters MUST push down into projection scans as id
   bitmaps (the existing `allowed_ids` contract); and candidate hydration
   against canonical records MUST read the unified snapshot of §6.3.6.
4. The analyzer contract is unchanged: jieba `cut_for_search` CJK analysis,
   `analyzer_digest` (analyzer version + lexicon) is part of
   `config_digest`; changing the analyzer or lexicon triggers a full
   rebuild, never a silent mix of tokenizations.

## 7. Relational store unification (SQLite-capability parity)

1. A relational table is a table under §3 with a fully declared schema, no
   residual column, and no label-set column.
2. Primary-key point reads MUST resolve through the durable equality
   projection to `(group, row offset)` and read only projected columns'
   chunks.
3. Secondary indexes, uniqueness constraints, and their maintenance ride
   the projection framework; constraint checks stay on the commit path.
4. The SQL surface of `POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md` is
   unchanged. Its storage clauses are replaced by §3–§6 of this contract.
5. SQL and Cypher MUST execute on the same executor operators over the same
   columnar reads; there MUST NOT be a second scan implementation.

## 8. Memory discipline

1. One `MemoryBudget` tree governs all engine memory. Default total aligns
   with the embedded envelope (512 MiB class). Components:
   - rigid small allocations: memtable bound, write-set bound, maintenance
     budget (compaction/projection builds);
   - elastic: chunk cache (CLOCK eviction at chunk granularity) sized as
     total − rigid − execution watermark;
   - execution: morsel admission budget with operator spill.
2. Every component MUST be either bounded-with-backpressure or spillable.
   Named obligations:
   - memtable: hard bound; full → early flush to delta groups (flush cost
     is proportional to delta size under §3.3, so the bound SHOULD be
     small, 32–64 MiB class);
   - transaction write sets: spill beyond threshold (§5.5);
   - projection builds and compaction: bounded external sort with fixed
     budgets (existing pattern);
   - **recovery**: WAL replay MUST flush intermediate delta groups when the
     memtable bound is reached; open MUST NOT require memory proportional
     to the crash window.
3. Pressure ladder, in order: shrink chunk cache → force memtable flush →
   write admission throttling (`StorageDebtController`). Each step MUST be
   observable. Memory pressure MUST degrade latency, never correctness or
   durability.
4. The engine MUST NOT rely on OS swap for correctness; cold structures are
   explicitly spilled. Operation under OS memory pressure (cgroup signal)
   MUST tighten the budget proactively.

## 9. Quality of service

1. All work admitted to the engine carries `(WorkPriority, WorkClass)`
   (existing `skein-qos` vocabulary: Foreground/Background × Query,
   Mutation, Projection, Import, Analytics, Shadow).
2. Background work — compaction, projection build/merge/rebuild, and
   analytical jobs (graph-algorithm projections such as PageRank, Louvain,
   and community refresh; reindexing; backfill; reconciliation) — MUST run
   under the maintenance budget and `RuntimeGovernor` admission; it MUST
   yield to foreground work, MUST be pausable and preemptible at morsel or
   group boundaries, and MUST be throttleable to a floor that still
   guarantees eventual convergence (delta groups and projection lag both
   bounded).
3. Background scans MUST NOT displace the foreground working set: chunk
   cache admission is tagged with the requester's `WorkPriority`, and
   background reads either bypass promotion or are confined to a bounded
   cache partition. A full-graph analytics scan running behind foreground
   traffic MUST leave foreground point-read hit rates intact.
4. Foreground tail-latency protection: admission MUST bound concurrent
   foreground work by the execution budget; deferred work is queued or
   rejected with an explicit `RuntimeAdmissionCode`, never silently
   degraded.
5. The QoS surface MUST expose, per component: budget, occupancy, pressure
   step, projection lag (epochs behind), and delta-group debt, so that the
   degradation ladder of §8.3 is externally observable.
6. Starvation bounds: a continuously loaded system MUST still advance
   checkpoints and projection cursors (no unbounded write-amplification
   debt); conversely maintenance MUST NOT push foreground p99 beyond the
   gates in §10.

## 10. Performance gates (normative)

Every phase that lands MUST include benchmark evidence against the current
`main` baseline on the same hardware. Regressions outside the stated bounds
block the phase, per `PRODUCTION_READINESS_SPEC.md` discipline.

| Gate | Benchmark | Requirement vs current |
| --- | --- | --- |
| Scan/aggregate over declared properties | `storage_segment_read`, `executor_vectorization` (end-to-end) | ≥ 2× throughput on selective column scans; no regression on full-row scans |
| Point lookup | `canonical_point_lookup` | p50/p99 no worse than row storage; cold-read I/O bytes reduced (columns touched only) |
| Index restart | `index_restart_cost` | open-time delta for N declared indexes ≈ 0 (bounded by crash-window catch-up, not data size) |
| Search restart | `search_generation`, `search_checkpoint` | projection mount + catch-up replaces full rebuild; restart cost ∝ crash window |
| Incremental projection | new `projection_incremental` bench | delta build cost ∝ change volume; query results identical to full rebuild (recall-validation gate) |
| OLTP mix (SQLite parity) | new `relational_oltp_mix` bench (YCSB-style point read/write/scan mix) | point read/write p99 not worse than current relational row storage; scan/aggregate strictly better |
| TP under background AP | `relational_oltp_mix` re-run with a concurrent background graph-analytics job (full-graph scan class) | foreground point read/write p99 within 2× of the quiet baseline; foreground cache hit rate intact (§9.3); background job still converges |
| Write amplification | new measurement in checkpoint report | checkpoint bytes ∝ change volume; steady-state space amplification ≤ current |
| Group commit | `wal_group_commit` | unchanged |
| Memory envelope | 512 MiB-class out-of-core run | RSS within budget; pressure ladder steps observable; no OOM |

The two new benchmarks MUST be added before the phases they gate (phase 1
for `relational_oltp_mix` may run against the current engine to record the
baseline).

## 11. Formal models

Existing models that continue to hold: `SkeinStorageDurability` (reframed
for binary WAL), `SkeinWalGroupCommit`, `SkeinWalDoctor`,
`SkeinConcurrentSnapshots`, `SkeinTransactionConcurrency`,
`SkeinGenerationReclamation`, `SkeinPropertyIndexPruning`,
`SkeinSystemSchemaUpgrade`.

New models required by this contract (wired into
`scripts/check-storage-tla.sh`):

1. `SkeinProjectionDurability.tla` — durable cursor + delta publication +
   WAL truncation floor + crash/recovery. Invariants: publication only
   after durable artifact; a `Ready` projection can always catch up
   (cursor ≥ replay floor); a projection below the floor is never served as
   `Ready`; served state ≡ full rebuild at the current epoch.
2. `SkeinCompactionVisibility.tla` — layered visibility
   (base ⊗ DV ∪ delta groups ∪ memtable) under concurrent flush,
   compaction, readers, and crash. Invariants: layered read ≡ logical
   state; published generations are immutable under pinning; flush and
   compaction are identity transforms; pinned generations are retained.

The four contract invariants these models carry: (1) projection ≡ pure
function of canonical + cursor; (2) compaction identity; (3) crash+recover
visible state ≡ full rebuild; (4) pressure degrades, never corrupts.

## 12. Phasing and acceptance

Each phase is independently releasable and revertible; each MUST land with
its spec amendments, tests, and the §10 evidence relevant to it.

| Phase | Content | Acceptance |
| --- | --- | --- |
| 0 | Property-key interning in canonical encodings | space amplification reduced; full regression suite green |
| 1 | Columnar node groups + zone maps + DV/delta groups; reader merge path; scans through `scan/` pruning | scan gates; point-lookup gate; write-amplification gate |
| 2 | Durable equality/composite index projections | index-restart gate |
| 3 | Generic projection framework; vector + lexical BM25 migrate; changefeed generalization; reclamation coupling | search-restart, incremental-projection gates; kill -9 restart serves without rebuild, results equal full rebuild |
| 4 | Relational unification (single WAL, shared groups, OLTP path) | OLTP-mix gate; cross-model transaction tests |
| 5 | Executor columnar direct reads (zero-copy chunk views) | `executor_vectorization` row/columnar checksum gate; scan gate re-run |

`src/store.rs` decomposition (recovery / checkpoint / residency modules) is
a prerequisite refactor inside phase 1 and MUST NOT change behavior on its
own.
