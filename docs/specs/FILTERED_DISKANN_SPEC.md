# Filtered DiskANN as the default vector engine

## Required end state

The embedded `hawdb` facade must use a persistent filter-aware DiskANN graph
for default approximate vector retrieval. Explicit exact queries stay exact.
The implementation must support insert, read, replacement, deletion and NULL
transitions, optional Local Scaling Graph (LSG) construction, and existing
canonical MVCC snapshots. A standalone in-memory graph does not satisfy this
contract.

## Architecture decision

Implement Vamana construction and multi-hop filtered search in the existing
`hawdb-vector-projection` crate. Reference the local Microsoft DiskANN sources
(`diskann/src/graph/internal/prune.rs`,
`diskann/src/graph/search/multihop_filter_search.rs` and RFC 01128).
Do not introduce a separate database, runtime, WAL or ANN publication selector.
The upstream `diskann-bftree` provider owns independent vector, neighbor and
quantization stores; its striped writes and CPR snapshots do not establish
HawDB transaction atomicity or canonical MVCC visibility.

The alternative is a custom upstream DiskANN provider over HawDB immutable
roots. That can reuse more upstream algorithm code but requires an async/runtime
bridge and dependency/portable-target qualification. The native kernel keeps
the synchronous embedded resource and publication contracts in one place.

### Graph and filter semantics

- Each graph provider pins one immutable set of ordinals, IDs, vectors and links.
  A provider must not look up a newer root during traversal.
- Vamana uses two construction passes, robust pruning and bounded reciprocal
  links. One directed connectivity edge per vertex survives pruning, including
  duplicate vectors and separated clusters.
- An eligible domain is exact and bound to the source, target and generation by
  the existing frontend. Rejected vertices may supply navigation links but their
  vector payloads cannot be read or consume the ANN result window.
- Candidate distance, work, bridge, and visit counts are observable. Work or
  memory exhaustion fails instead of returning an unmarked partial result.
  The query work bound also includes eligible-entry selection and ID lookup.
- Query visited-state uses dense bytes for small graphs and a fixed-capacity
  sparse table when the graph exceeds the visit budget. Its memory is bounded
  by admitted work rather than total disk graph size.
- Raw distances use the shared core implementation. Equal distances use stable
  IDs. Approximate traversal does not claim global exact recall.

### LSG

Use local radii estimated from a deterministic sample. For Euclidean vectors,
the radius is the mean distance to the closest sampled neighbors; for cosine,
use Euclidean distances after normalization. Construction uses
`squared_distance / (radius_left * radius_right)^exponent`, including candidate
navigation and pruning. Queries use the original metric. A dataset-relative
radius floor handles duplicates without NaN or infinity.

The formula and construction-only behavior follow the authors' implementation:
[LSG](https://github.com/19-hanhan/LSG), `IndexHNSW.cpp` and
`benchs/bench_ls_hnsw.py`. Sampling excludes a point itself; sampled-neighbor
count, sample size and exponent must be persisted and validated.

## Ordered delivery and exit gates

1. **Kernel and provider boundary.** Implement deterministic Vamana, filtered
   navigation and optional LSG with explicit memory/work/cancellation bounds.
   Verify exhaustive equivalence when the beam covers the graph, rejected-vector
   non-access, sparse/disconnected filters, duplicates, signed cosine distances,
   invalid input and typed resource refusal. This stage grants no serving or
   default-engine readiness.
2. **Persistent artifacts and registered serving.** Persist vector/link records,
   configuration, source identity and integrity metadata in immutable graph
   artifacts. Reuse admitted native file custody, reader retention and the
   existing Consumer selector. Check truncation, corrupt links, parameter/source
   drift, retry and cancellation without partial publication. Bind the explicit
   SQL/Cypher `filtered_ann` path before changing any defaults.
3. **CRUD and MVCC.** Capture committed inserts/updates/deletes/NULL transitions
   through existing source change feeds. Publish complete successor generations;
   retain old pinned graphs and canonical snapshots. Reuse unchanged segment
   graphs and atomically publish changed graphs with their visibility/tombstone
   state. CRUD must not require a full-source rebuild for each mutation.
   Verify old/current reads,
   rollback, branch/source mismatches, checkpoint/reopen and interrupted
   publication. An acknowledged canonical mutation must never expose a partial
   graph update; an index behind the requested epoch must fail closed.
4. **Default selection and qualification.** Select DiskANN in default index build
   and ordinary approximate search paths, keeping explicit exact/RaBitQ choices.
   Assert actual backend reports and canonical predicate semantics in both
   languages. Compare recall against exhaustive ground truth on correlated and
   anti-correlated selective filters, multiple graph sizes and update/delete
   workloads; report build/query costs and resource envelopes separately.

Update `docs/TEST_INDEX.md` with the owning targets at each stage. Preserve all
Search feature profiles and native/WASM capability refusal contracts. Run the
repository formatting, strict Clippy and required local regression/fuzz commands
on the final integrated tree. Kernel tests alone cannot close this contract.

## Current evidence boundary

The kernel/provider and immutable artifact primitives are implemented. The
artifact stores fixed-width ID/radius/degree/dimension/vector/link records,
construction settings, a 32-byte source digest, a 32-byte embedding binding
digest, source epoch and generation,
with a versioned header/footer and CRC. The caller retains native file custody,
must sync before publication, and must independently validate source authority.
Opening reserves verification scratch and retained metadata before mapping,
validates the complete checksum, then checks IDs, vector dimensions/finiteness,
cosine norms, radii, degrees, sorted unique links and canonical padding.
Model/version changes must match the graph's persisted binding digest; updating
only an outer manifest cannot rebind an existing graph to another embedding.

Synthetic kernel tests include a beam smaller than the source with a 95% recall
floor against exhaustive ground truth, with and without LSG and filtering.
This is regression evidence on a small fixed fixture, not representative recall,
latency, RSS or scalability qualification. Validated sorted-ID providers seed
eligible entry points from deterministic disjoint allowlist buckets. Domains no
larger than the beam use bounded ID lookup and score all present eligible IDs
without link traversal. Larger domains expand one rejected hop from
each eligible frontier vertex, considering all its rejected neighbors within
the visit/work budget; rejected second-hop vertices can be promoted if
later reached directly from an eligible vertex. No rejected vector is scored.
The complete ordinal-ID domain retains ordinary Vamana traversal. Providers
without a sorted-ID contract retain recursive bridging with work refusal.
Selective and anti-correlated scale fixtures still qualify the recall/cost
tradeoff independently of these kernel invariants.

Registered Consumer construction now reserves the complete private graph source
and construction envelope before copying vectors, writes and synchronizes the
graph, and seals it through the existing artifact-reference/publication path.
Canonical vector publication admits at least its existing 64 MiB request and
raises that separately funded request for a larger explicit task envelope.
The private graph builder receives the effective admitted task ceiling, including
any smaller caller limit. Initial construction, rebuild and incremental catch-up
share this policy; host admission and positive/representable memory checks remain
mandatory. Preflight/admission regressions cover a 100,000-by-128 graph envelope
without allocating that graph and do not establish its actual build cost.
The existing candidate-artifact filename and legacy `rabitq_*` manifest slots
are shared storage plumbing: a full graph source digest selects the graph format,
the descriptor names the real algorithm, and opening checks graph parameters,
metric, dimensions and source identity. No additional selector is introduced.
Reader mappings retain their original native custody and shared admission.

The Consumer installer now verifies sealed producer bytes against their SHA
references and creates exclusive native links through the held source/target
directories. It retains the original birth/scratch cleanup owners and
synchronizes the target directory before sealing; arbitrary external source
installation still copies independently. Scratch retirement synchronizes its
original parent before releasing custody and retries that same barrier after a
refusal. Primitive recorder tests cover lost links and directory barriers. A
registered Consumer recorder case checks shared producer/target inode identity,
the target barrier, durable scratch retirement, retained data and reopen; these
cases do not establish the complete incremental Consumer power-loss matrix.
This avoids a second data copy during installation; full byte verification and
initial graph construction costs remain separate from incremental maintenance.

Native change feeds retain deleted node IDs even for labels outside the built-in
Search document mapping. A fixed vector projector can therefore hydrate the
pinned end epoch and retract its own native ID when the node is absent. WAL
failure, live feed pinning, checkpoint and both residency modes have focused
storage cases. The feed alone still does not authorize source publication.

Initial field-index construction and explicit rebuild enumerate only the
descriptor's declared SQL table or node label through the scoped canonical
source cursor. Unrelated relational tables, including the operational
`hawdb_schema_migrations` table, are outside that field's input domain. Node
fields use the existing canonical label cursor; resident graph overlay/header
inspection retains its existing work accounting. Source completion and
publication still bind to the same global pinned epoch. Checkpointed regression
fixtures include both source families, unrelated data and finite work limits.

Schema-bound builds now default to DiskANN. The public backend definition uses
canonical integer-scaled Vamana/LSG settings. SQL and Cypher `filtered_ann` use
the real graph before exact reranking; reports identify the executed backend.
The root vector-index family includes optional-LSG SQL/node construction and
reopen, filtered language queries, old/current source fencing and full-rebuild
successors after update/delete/NULL/insert. Legacy explicit RaBitQ cases remain.

Registered vector Consumers seal a bounded metadata-only compatibility snapshot,
not a second resident copy of their vectors. Its logical document/vector counts
and reversible document-set digest are taken from the admitted artifact writer.
The selected manifest seals both its object reference and the metadata-only
marker, and reopen cross-checks the summary against that same immutable closure.
Ordinary search Consumers retain their full compatibility snapshots. Consumer
`document_count`, `vector_index_descriptor` and `projection_freshness` return
validated metadata for either representation; `search_index` refuses a vector
Consumer with no resident rows. Vector queries require the native pinned read
domain. Parser controls reject duplicate summaries, impossible counts, resident
rows mixed with summaries and missing Consumer/vector bindings. This removes the
full resident reload from vector publication, but does not make construction or
artifact verification proportional to changed records.

Unbound streaming projections now default to DiskANN as well. The ordinary
out-of-core query path selects their graph and independently reranks raw vectors.
`search_with_options_exact` and `VectorSearchExecutionOptions::with_exact_search`
select exhaustive scoring. An explicit compressed-projection request still
requires RaBitQ; a raw graph cannot satisfy that requirement. The generation
builder's `vector_backend` optionally selects canonical DiskANN/LSG or RaBitQ
settings; `None` retains the legacy RaBitQ option fields.
Legacy unbound cosine projections may contain zero vectors. Such content keeps
its compatible RaBitQ encoding and the default query uses exact raw scoring
when graph layers are incompatible. A zero ordinary query likewise produces no
cosine scores through the exact path. Schema-bound cosine definitions retain
their existing zero-vector refusal; Euclidean graphs accept zero vectors.

Ordinary resident `SearchIndex` queries now select a bounded immutable DiskANN
cache. Its ordinal map is private to the original graph. Mutations mark existing
ordinals as changed without editing links or copying graph vectors; queries
exclude those ordinals and merge eligible replacements/new IDs through a bounded
exact candidate overlay. NULLs, deletes and reinsertion cannot revive an old
vector. Source replacement or embedding-manifest changes reset the cache.
The eligible ordinal list is built from the canonical metadata-filter result;
rejected vertices remain navigation-only. Final scores retain the facade's
positive-only cosine relevance and stable-ID ties.

`SearchIndex::search_with_options_exact` preserves exhaustive resident scoring.
`try_search_with_options_context` carries memory/cancellation limits and honors
`with_exact_search`; explicit adaptive/compressed entry points keep their prior
RaBitQ selection policy. Default ANN pagination admits at least `offset + limit` candidates and keeps
a 128-candidate minimum window for hybrid fusion. Resident and mapped serving
independently retain at least 1,024 navigation candidates, or the requested
output window when larger, bounded by graph size. Navigation does not increase
the raw reranking window. Its full workspace is preflighted against the existing
query memory/work limits; insufficient admission fails rather than shrinking
the beam or returning partial results. Cache construction has a 64 MiB ceiling and query admission includes
its conservative retained envelope, ordinal selection and candidate workspace.
Before choosing the default resident graph, the facade preflights that cache
and its query workspace. If it cannot fit the ordinary cache envelope, default
resident search uses the existing scalar path and reports
`diskann_cache_budget_exceeded` with `scalar_vector_scan` as the actual backend.
Exact and explicit compressed requests retain their own backend contracts.
Smaller caller-selected query budgets remain binding, and work refusal,
cancellation, invalid data and corruption are not converted into cache fallback.
Fallback cannot qualify the default graph's recall or readiness.
Runtime allocation leases retain cache ownership and cover candidate execution.
Cache readers capture an immutable Arc and release the cache lock before
execution admission and graph traversal. Exclusive mutations update retraction
bits only after those search borrows end. Captured resident read generations
share immutable graph/ID storage; a successor copies retraction state before
changing it, preserving the old generation's vectors and exclusions. Captured
capacity includes the lazy graph construction/cache envelope and mapped graph
payload, alongside the independently available compressed projection.
Waiting for initial construction checks the caller's cancellation/deadline.
Focused concurrency cases block execution
admission while a second query completes, and verify a waiting deadline.

Resident checkpoints now write native DiskANN graphs into the existing candidate
slot and seal their full source digest in the same manifest. The disk reader
executes that graph after checkpoint; old readers retain their original graph
across later replacement/deletion/NULL checkpoints. Graph and streaming builds
share one source-identity recipe. The writer creates its candidate through the
held publication directory and carries the originating task through snapshot,
graph construction and sidecar publication. A compatible complete cache can
supply the immutable graph without another vector copy. Dirty caches rebuild at
this explicit checkpoint boundary; ordinary CRUD queries retain their overlay.
For an unbound index, a complete graph that exceeds the cache, including a
retained dirty predecessor, selects the existing streaming compatible codec
before creating the candidate file. This preserves checkpoint/reopen of the
complete canonical source. It does not enlarge the cache or admit a partial
graph. A schema-bound DiskANN descriptor still requires its declared graph and
its explicit construction/task admission; it cannot silently change codec.

The bound depends on both vector count and dimension, IDs, degree, beam and LSG,
rather than a fixed row-count limit. On a 64-bit host, construction-only
preflights with default degree 32 / beam 128 / no LSG and 16-byte IDs require
approximately 39.0 MiB for 50,000 x 128, 48.3 MiB for 15,000 x 768, and 49.2 MiB
for 8,000 x 1,536, including a conservative 1 KiB wrapper allowance. Query
workspace and canonical documents are additional; these are admission
calculations, not executed scale qualifications or process RSS limits. The
1,024 x 16,384 regression exceeds the actual 64 MiB construction cap and
checks scalar fallback, filtered vector/hybrid results, checkpoint, mutation
and reopen. Registered canonical builds separately enforce their source-mapping
and host/task envelopes; above those explicit limits they return a typed
resource refusal and retain the previous completed generation. The 100k x 128
lifecycle evidence uses its explicit 320 MiB build/reopen and 2 GiB host limits,
not the library defaults. No supported million-vector default-memory claim is
made.

Calling `set_rabitq_projection_build_options` explicitly selects a RaBitQ
checkpoint. Its bit width, segment rows and seed are sealed in the snapshot and
restored before reopening/replacement. Legacy unbound zero cosine content keeps
its compatible codec. Optional restore distinguishes non-graph, stale graph and
valid graph files; stale graph evidence is never sent to the RaBitQ corruption
classifier. Resource/I/O refusals remain distinct from physical corruption.

Direct resident `SearchIndex` serving restores a validated graph mapping through
the original opened file, including registered roots whose path label was replaced.
It retains a bounded vector-ordinal map and retraction bytes rather than copying
the graph vectors. Queries can run below the graph construction envelope; CRUD
keeps the same mapped baseline and uses the existing exact changed-record overlay.
A clean checkpoint copies verified graph records with the successor identity and
checksum without reconstruction. Corrupt optional graphs retain quarantined
bytes; fallback accepts only a graph matching the current source identity. A stale
graph is retained as evidence and never serves the newer snapshot. Registered
publication still fails closed on an inconsistent selected artifact closure.

The existing standalone delta/compaction protocol retains unchanged immutable
content and graph artifacts and builds graphs only for changed content. Deltas
inherit the pinned base's graph settings. Regression cases exercise filtered
insert/replacement/deletion/NULL transitions, LSG inheritance, empty and
deletion-only layers, unchanged graph bytes and native inode, old-reader query
results, typed construction-memory refusal, cancellation, legacy zero cosine
semantics and abandoned-candidate rollback. This establishes that path's segment
reuse; it does not establish canonical source-feed publication or power-loss
qualification for schema-bound Consumers.

Schema-bound vector Consumers expose `Database::catch_up_vector_index`. It
validates the completed Consumer receipt and canonical field-projector baseline,
captures one pinned C..E feed, deduplicates native keys and hydrates them from E.
Missing or NULL records retract the projector's own native identity. The
pre-admitted projector kind must agree with the private builder's terminal
completion marker; a generic host callback cannot promote itself by setting a
marker. The sealed bounded snapshot retains that origin epoch across reopen.
Any mutation after completion invalidates the private builder. Generic host
catch-up and complete-E coverage retain their existing contracts.

The successor links the completed base's verified immutable artifacts into its
original private producer, constructs only changed vectors and publishes their
target-specific retractions through the existing native Consumer selector.
Counts and reversible logical digests include the retained layers. Delete-only
updates retain content without writing a graph. A current no-op retains the
publication generation. Feed gaps, schema drift, algorithm changes, cancellation
and resource refusals cannot advance a partial source cursor. A rebuild remains
explicit. The same algorithm options must accompany maintenance; they cannot
change the persisted definition.

Remaining long-running-host work is tracked by #1245. Graph-node change feeds
currently carry native IDs without label domains, so unrelated-label changes
still consume incremental candidate/hydration admission. Catch-up layers and
mutation runs are not automatically folded; retained query and manifest costs
can grow with the number of catch-ups, and an unfiltered retracted layer still
materializes its eligible-ID allowlist. Hosts must not treat the current
incremental path as a bounded steady-state Mem default. Label-aware feeds and a
source-aligned layer/fold policy, including old-pin and recovery qualification,
remain required before that use. Dense construction scratch clearing,
unconditional reciprocal pruning, deadline-check granularity, sparse visited
hash distribution and descriptor tuning ceilings also remain performance
follow-ups. The review's optional kernel changes are not represented as fixes
or covered by prior scale measurements. This PR does not authorize Mem
activation.

Canonical SQL point mutations derive their hydration working set from complete
primary-key equalities, including conjunctions with residual predicates. Every
primary-key column must be bound, in schema order; residual predicates still
execute normally. OR, NOT, NULL equality, non-key filters and incomplete composite
keys retain the existing scan path and resource refusal. Destination-key and
constraint closure remains mandatory for primary-key changes. This bounds the
working set of ordinary point CRUD by its distinct keys without raising the
100,000-entry WAL replay-access budget. Retained-row Materialized UPDATE/DELETE
also use the complete key to select candidates and evaluate the full predicate.
The WAL encoding and exact replay-access comparison retain their existing
contracts. Resident authoritative recovery distinguishes previously recorded
full scans by coverage of the original table keys, including keys removed
earlier in the transaction; it accepts a point-access record only when execution
reproduces that record's complete access set. Sparse recovery keeps its existing
authenticated workspace semantics. This working-set bound does not imply
that all transaction execution or generation maintenance is O(K).

Root regressions cover SQL and custom node-table CRUD, repeated-key reduction,
optional LSG, exact/filtered-ANN language parity across old/new layers, graph
byte/inode reuse, a two-vector graph for four changed keys over a 32-vector base,
delete-only/empty/reinsert, old MVCC domains, checkpoint/reopen, rollback,
cancellation and typed feed, memory or key refusal followed by retry. They also refuse unproven host baselines,
schema/algorithm drift and late mutations of completed builders. A recorder case
reopens materialized acknowledged SQL and custom-node images after losing every
uncovered write under both durability policies at strict 32-descriptor capacity.

Fixed-projector publication faults cover SQL/custom-node sources at eight logical
boundaries with live resolution and cold reopen after later source writes.
Descriptor pressure cases retain the original prepared operation before/after
intent, cover full rebuild and incremental maintenance, and verify release of
execution slots. The shared rebuild coordinator uses the same internal descriptor
operation scope as the other Consumer maintenance entry points.

Physical recorder cases capture native target-link installation, sealed candidate
directory/scratch-retirement barriers, private selector writes, selector renames and the final root barrier.
All lost/reordered/isolated/torn plans are evaluated; byte-, alias- and
namespace-identical images share a native reopen. Source mutations are one atomic
transaction, so relaxed mode can lose the unbarriered E transaction completely.
A prepared selector is sealed before the host source-prefix barrier. Preparation
alone therefore cannot require E to survive or authorize graph reads at E.

Cold recovery holds the exclusive logical-target publication lease. An update
with no intent could never have selected a replacement: recovery closes that
obsolete grant while retaining its native directory and bytes. An unchanged C
selector similarly settles an E intent as not selected, even if the relaxed E
source transaction was lost. This releases logical operation capacity without
reconstructing a producer from a pathname or deleting unpublished contents.
The selected-E branch still requires the captured canonical source epoch and
complete marker/metadata/artifact verification before granting read custody.
Completed selector barriers cannot fall back to C. Recovery-publication tests
continue recording from exact native materializations of a first crash. They
cover abandoned births, unselected intents and selected intents, with subsequent
write, file-sync, rename and root-directory cuts under both durability modes and
both source families. Each surviving image becomes a stable baseline for the
next crash; importing it first verifies bytes, names and hard-link aliases and
does not imply an acknowledgement of the interrupted original operation.
Relaxed-mode cases also lose the complete unbarriered E transaction before
intent and during private-selector preparation. They require the source to
actually reopen at C before interrupting recovery publication again; surviving
E candidate bytes cannot authorize E data or require its lost transaction.
Repeated recovery must retain the original selection receipt and immutable
candidate bytes/inodes, and preserve source/index epoch matching or stale refusal.

Native Windows verification partitions recovery into seven exact Rust cases:
births, unselected intents and selected intents retain 28 combinations each;
lost relaxed SQL and node sources each split into Link and SelectorWrite origins,
with seven combinations per case. The full 112-combination matrix, every physical
fault plan and repeated native reopen remain intact. Three additional exact
shards own first-crash artifact closure, torn selectors and selector/root barriers,
retaining all 32 cut/source/durability combinations. Weekly power-loss and required
consumer jobs share this selection contract and validate emitted recovery
combination identities. Their remaining/ordinary selections exclude the vector
fault module because these ten required shards own it. Linux/macOS
still run the complete power-loss family together. Per-process concurrency,
repository compiler profiles and 120-minute job ceilings remain unchanged.
The weekly Windows matrix keeps all eight branch-abandonment cases together
in a separate serial shard. Discovery must prove that its sibling selections
and the remaining partition form the complete, disjoint family.
Windows also owns consumer publication, Search projection publication and the
single deletion/GC case in separate required shards. Automatic head, manifest,
root and candidate-WAL-sync qualification each have one exact case per durability
policy; both boundaries and three successive generations remain in each fixture.
This retains all 48 automatic configurations across the four cuts, with eight
Rust cases. Runtime coverage requires every
configuration and positive completion summaries exactly once; compiled discovery
must prove that no sibling selection overlaps or omits a case. Every physical fault plan and
assertion is retained under the same profile, serial execution and job ceilings.

Whole-generation byte verification and admitted reopen still read unchanged
artifacts. These cases prove changed-content construction and native reuse, not
end-to-end K-proportional maintenance or a scale improvement. Update/delete-rate,
resource and large canonical-build evidence belongs to the separate canonical
lifecycle protocol below. Completing this delivery also requires the complete
native/portable matrix and source-bound runtime recovery evidence; these bounded
recovery cases alone do not close the requested default-engine/CRUD/MVCC contract.

Default sampled recall now compares exhaustive results with the actual default
DiskANN query strategy, including its minimum navigation window. Explicit
compressed probe/qualification entrypoints retain RaBitQ coverage. Qualification
binds the observed backend to the artifact algorithm, so a successful compression
probe cannot stand in for graph evidence. The compression advisor retains its
RaBitQ-only validation contract; default graph reports have a separate validator.
A clean mapped resident graph reports
its verified generation, canonical epoch, source fingerprint and complete-artifact
length/CRC32C. CRC32C is captured during the existing admitted verification pass;
it adds no file read and changes no durable bytes. Mutated overlays and source
epoch drift cannot reuse that qualification identity. Resident snapshots do not
persist construction resource measurements, so that resource-evidence getter
returns unavailable instead of creating evidence from a different codec.

The unchanged production gate still requires at least 100,000 documents and the
complete current-release evidence binding. Small mapped-graph tests explicitly
remain unqualified with `dataset_too_small`; backend/identity binding tests alone
are not scale evidence. The manual RaBitQ query profiling fixture selects its
codec explicitly after the default-engine change.

Ordinary out-of-core graph queries derive their candidate window from the page,
explicit rank window and default navigation window. The configured candidate
ceiling remains a ceiling; it must not force every request to rerank that many
vectors. Requests exceeding the ceiling fail. Explicit exhaustive and compressed
paths retain their own selection policies. The manual clustered-corpus fixture
records per-sample and per-filter-family recall against held-out synthetic
queries and exhaustive filtered ground truth; it is not a production corpus or
canonical incremental maintenance qualification.

Out-of-core graph qualification binds the current logical generation/epoch and
the complete selected manifest, including every graph/content reference and
visibility run. Its checksum fingerprints this closure rather than the first
graph's record payload. Count only visible vectors: existing target validation
records a runtime-only vector retraction count, reusing it only when the run and
all target references match the previous verified reader. Querying qualification
adds no artifact read or document hydration; admitted collection sizes include
the cached scalar state. Deletion-only, NULL-only and empty-vector heads retain
accurate identities. Mixed graph/legacy-codec closures remain unqualified.
Resource evidence sums complete graph and visibility artifact bytes and stored
raw vectors across all layers; sequential construction envelopes use the maximum
per-graph bound. Stored rows include retired versions, while identity row counts
represent current visible vectors. This changes no durable encoding.

### Manual canonical lifecycle qualification

`filtered_diskann_qualification canonical` exercises the same typed `Database`
source, build, catch-up and language-query APIs as an embedded host. It is a
manual fixture wrapper in the existing example. For example:

```sh
cargo run --locked --all-features --example filtered_diskann_qualification -- canonical /tmp/hawdb-canonical-plain 100000 128 320 plain 10 16
cargo run --locked --all-features --example filtered_diskann_qualification -- canonical /tmp/hawdb-canonical-lsg 100000 128 320 lsg 100 16
```

The root must not already exist. Arguments after the root specify vector count,
dimension, build MiB, construction mode, changed keys per thousand and held-out
query samples. The changed-key count rounds down to complete groups of four,
with at least one group. Each group replaces one vector, deletes one row, sets
one vector to NULL and inserts one new row; the next phase restores the original
source. A coprime stride spreads mutations across cluster labels.
Initial loading commits 1,000 rows per transaction and checkpoints every 10,000
rows and at the end, before capturing C. This keeps the default bounded live-row
overlay intact; each checkpoint's time is recorded separately. Both CRUD phases
remain complete transactions after the retained baseline is captured.

The build option funds canonical source staging and its resident producer,
separately from publication and retained-reader admission. Source staging charges
a conservative 2 KiB per document in addition to its ID and vector payload; the
point-query workspace also needs funding. Thus the 100k-by-128 controls request
320 MiB rather than 128 MiB. The graph kernel's smaller construction preflight
does not qualify the full canonical source envelope. The fixture's host governor
declares 2 GiB for coexisting staging, publication, query and old/current readers.
The fixture also funds ordered source-key seeks, emitted rows and the terminal
probe from its row count. At 100k rows the scoped table cursor needs 2,000,021
work units, so the fixture explicitly admits 2,000,022 instead of the default
1,000,000 and records that feed-work budget in its configuration output.
These are explicit workload settings; library defaults, source accounting,
resource-refusal cases and the query thresholds remain unchanged.

The atomic source transaction also needs an explicit primary-key capture
envelope. The library default is 4,096 entries and 256 KiB per transaction.
The 1% control changes 1,000 keys and retains that default; the 10% control
changes 10,000 keys and explicitly declares a 10,000-entry host envelope.
Both keep the 256 KiB byte limit and record the configured limits. An
underfunded transaction still produces `RequiresRebuild`; the Consumer must
retain its old source cursor and selection. The fixture does not split the
atomic mutation or change library defaults to avoid this boundary.

Vector maintenance preserves publication admission as a typed
`VectorIndexBuildError::Admission`. A background task can temporarily occupy
the host's sole background slot or the source catalog's metadata lock. The
fixture retries only a retryable `BackgroundTaskSaturated` or a typed
`BranchBusy` refusal, for at most 30 seconds with 25 ms backoff. Each refusal
must leave the Consumer's source epoch and completed stationary generation
unchanged and emits an `admission_retry` record. A pending preparation or
uncertain selection cannot supply that stationary receipt. Other failures
propagate immediately. Admission and original native custody retain their
ordering; this retry policy does not apply to uncertain publication. No-op maintenance
requires no publication slot, even while that slot is occupied.

Cold reopen declares the same 320 MiB working envelope as construction through
`open_search_projection_consumer_with_task`. The host governor admits that
envelope before opening the native generation, and verification, reader state
and cancellation use the admitted task. A smaller task ceiling remains binding;
an oversized request cannot bypass host capacity. The original open method
retains its 64 MiB default. Generation-size and compatibility-resident limits
are unchanged. Resource refusal releases admission and permits retrying the
same selected generation without rebuilding or advancing its source cursor.

At baseline, after both publications and after cold reopen, each of the five
filter families compares ANN results with exhaustive canonical SQL distances,
checks independent eligible counts, IDs and raw distances, and requires at least
95% mean recall per family. Both publications also repeat the original ordered
ANN results through the retained C read domain, refuse a current source bound to
the stale index and verify unchanged baseline graph hashes and successor reuse
(including native inode identity on Unix). No-op catch-up and persisted backend
settings are checked separately.

JSON output separates source ingestion/checkpoint, graph construction, mutation
publication, query and cold-open time. It records source/generation/schema
identity, changed-key mapping, logical query reads, retained index memory,
governor admissions and native FD accounting. Logical read counters are not
physical device I/O; phase snapshots are not peak RSS. Measure process resources
externally when needed, keeping compilation cost separate from execution. These
clustered synthetic workloads do not establish production-corpus qualification.

The first completed 100k canonical baseline failed the unchanged 95% recall
floor: a 128-wide navigation beam reached only 86.875% in the weakest plain
filter family. A standalone fixture with thirteen smaller graphs did not
reproduce that single-graph limit. A direct single-graph diagnostic reproduced
all five canonical recall totals exactly. With the same 128 output candidates,
navigation widths 256, 512 and 1,024 reached minimum family means of 90.625%,
95.625% and 96.25%, respectively. Serving now uses the independent 1,024-wide
navigation workspace. On this diagnostic it scores 125,639 unfiltered vectors
across sixteen queries, versus 41,111 at width 128; this is a quality/cost
tradeoff, not a latency improvement. These plain in-memory diagnostic results
do not qualify LSG or the complete canonical CRUD/MVCC lifecycle; the unchanged
four-control 100k CI protocol must be rerun on the repaired source.

The subsequent four-control run completed all baseline queries with the repaired
navigation: minimum family mean recall was 96.25% for plain and 98.75% for LSG.
Every control then refused its first mixed CRUD transaction because primary-key
UPDATE/DELETE hydration still scanned the whole 100k table and exhausted the
unchanged WAL replay-access entry budget. Both live hydration and retained-row
Materialized execution now derive complete primary-key point reads as described
above. A 16-row canonical-page
regression changes three keys under a four-entry budget and checks source-view,
WAL replay and constraint parity. This bounded regression does not qualify the
complete 100k lifecycle; all four controls still require a final-source rerun.
The subsequent retained-row run exposed the underfunded default source-capture
envelope in the 10% controls and transient background-slot contention after the
1% publication and retained-reader checks. Those partial outcomes remain
failure evidence; the explicit envelope and bounded typed host retry above
must complete the same four full lifecycles before qualification.

The Native Bindings workflow exposes an explicit `canonical_vector_lifecycle`
dispatch. It runs the four 100k-by-128 plain/LSG and 1%/10% changed-key controls
in four independent matrix jobs, with sixteen samples per filter family
and state. All four must pass qualification; no failure cancels another control.
Each job retains the 180-minute
ceiling and the complete lifecycle; this ceiling is not a claimed workload
duration. Each job records source/tree, toolchain, compiler profile and binary
hash, including its own first-build cost; compilation and each lifecycle
have separate process time and peak RSS measurements. Complete or partial JSON,
stderr and exit outcomes survive failure. This benchmark dispatch includes no
fuzz tool; recovery sharding is owned by the separate native qualification lanes.

## Recorded canonical lifecycle evidence

Revision `ad4eaab50d7fa5868ca7d689a1d589a70bd9d1f8`, tree
`c8163a24afea7fe90452f56e388b9f552c016005`, completed all four fixed 100k-by-128
controls in [run 40238969246](https://github.com/nowledge-co/hawdb/actions/runs/40238969246).
Each control covers five filter families with sixteen samples, six qualifications
of eighty cases, and eight hundred exact/ANN query records. The two retained-old
qualifications preserve ordered baseline results at epoch 104 / generation 2.
Current domains reach epoch 106 / generation 8, with identical ordered results
after restore and cold reopen. Publication maps exactly 1,000 or 10,000 changed
keys and reuses the original graph artifacts. The final rerank window remains
128 and the family recall floor remains 95%.

| Mode / changed-key rate | Minimum family recall | First compilation | Complete lifecycle | Peak runtime RSS, reported kbytes |
| --- | --- | --- | --- | --- |
| Plain / 1% | 96.25% | 168.60s | 2278.42s | 1,446,040 |
| Plain / 10% | 96.25% | 164.55s | 2798.85s | 1,380,540 |
| LSG / 1% | 98.75% | 203.78s | 3045.86s | 1,429,492 |
| LSG / 10% | 98.75% | 192.12s | 3754.00s | 1,432,508 |

All four artifacts bind their source/tree, toolchain, repository compiler profile
and binary hash. Complete raw query records permit independently recomputing
recall and checking backend/epoch/generation, old-pin results, publication counts
and resource receipts. These measurements use the checked repository profile.
Cold reopen closes handles without dropping OS caches; logical counters do not
measure physical device I/O. They establish neither total O(K) maintenance nor
production-corpus, release-performance or hardware power-loss qualification.
Later CI results must retain their own revision and actual checkout identity;
this recorded run must not be relabeled as execution on a later commit.
