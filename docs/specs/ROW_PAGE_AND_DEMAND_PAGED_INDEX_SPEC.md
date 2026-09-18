# Row-Page Canonical Storage and Demand-Paged Index Specification

## Scope

This specification defines HawDB's target durable layout for a PC-oriented
embedded database. Canonical graph and relational state is row-oriented,
page-bounded, and indexed. Primary, unique, secondary, graph-property, and
adjacency indexes are persistent immutable pages that are read on demand
through a byte-bounded cache.

This contract replaces the former goal of making column groups the canonical
representation. Existing column-group, deletion-vector, and columnar-shadow
artifacts remain valid derived-projection experiments. They MUST NOT become a
recovery dependency or a second canonical writer without a new specification
and workload evidence.

HawDB has not shipped a durable storage format. This specification therefore
defines one destructive v1: readers MUST reject bytes that do not satisfy the
current v1 contract and MUST NOT add legacy magic, version fallbacks, migration
branches, or compatibility facades.

Normative `MUST`, `MUST NOT`, `SHOULD`, and `MAY` clauses take precedence over
descriptive implementation notes, per [`README.md`](README.md).

## Goals and non-goals

HawDB is an embedded, TP-first knowledge database. Point reads, short
transactions, predictable resident memory, cross-platform recovery, and graph
locality have priority over scan throughput.

Goals:

1. Canonical immutable row pages ordered by stable primary key or entity id.
2. Persistent indexes whose leaf and posting cardinality does not determine
   startup residency.
3. Snapshot/COW MVCC with durable-before-visible root publication.
4. PostgreSQL-style row, unique-key, and range locking plus graph entity and
   adjacency locking.
5. Storage-neutral vectorized execution over row pages.
6. One byte ledger covering caches, pins, dirty state, WAL replay, execution,
   results, spill, and projections.
7. Derived columnar, BM25, vector, statistics, and analytics projections that
   are removable without changing canonical recovery.

Non-goals:

- multi-process writers;
- distributed consensus or Cloud-primary execution;
- arbitrary historical time travel;
- serializable snapshot isolation;
- reliance on OS swap, `mmap`, or Linux-specific asynchronous I/O for
  correctness;
- making every projection transactionally canonical.

## Current and target boundaries

The current implementation already provides immutable COW row collections,
reader generation pins, strict WAL recovery, generation-scoped segment range
reads, and a byte-bounded digest-verified cache. Equality, range, full-text,
and ordered composite-equality graph-property indexes have a checkpoint-
generation projection whose payload blocks remain cold until a query needs
them; post-checkpoint WAL changes stay in the COW overlay and are merged at
read time. Relationship equality and range predicates over a bound one-hop
expansion use the same demand-paged artifact when its estimated global posting
work does not exceed the endpoint adjacency work. Stable-id graph index state
uses a separately published, fixed-page, demand-read sidecar for the ambiguous
physical-id mapping needed by HawDB Lightning. Relational constraints may opt
into the generation-bound authoritative reader, but materialized relational
postings remain a temporary checkpoint builder and differential oracle until
the next migration stage removes their ordinary-open residency.

The migration defined here is incremental:

1. write and validate immutable page artifacts without serving from them;
2. publish generation-fenced row and index roots in shadow mode;
3. differentially compare disk-backed lookup with the current oracle;
4. activate relational and graph index reads independently;
5. activate canonical row-page reads only after separate evidence.

No phase may silently serve a mixture of row data and stale index roots.

The equality graph-property projection is the first activated slice of step 4.
Its manifest is generation/epoch fenced and size bounded, opening its reader
does not populate the segment cache, and a lookup reads only key-overlapping
blocks. It is a rebuildable query index, not the uniqueness oracle. Missing or
incomplete projection coverage uses the canonical bounded scan; corruption in
a selected block fails closed.

### Required-property graph reads

Physical-plan finalization MAY fuse a scalar `Project` with a node access path
only when every projected expression and residual predicate can be evaluated
from one node variable. The fused operator MUST retain the selected access
class: label scan, equality/multi-seek, ordered composite equality, range, or
full-text. An ordered composite index MAY also serve a range seek only when the
query binds a non-empty contiguous equality prefix and constrains the
immediately following index property with at least one range bound. A missing
leading property or a gap between the equality prefix and range property MUST
decline the composite path. Trailing index properties are not required data.
Exact disjunctions over two or more indexed properties MAY retain a bounded
equality-union access path. The optimizer MUST decline that path when any
branch is not indexed or when the deduplicated lookup-value count exceeds 64.
Its required-property set is the union of projected properties,
residual-predicate properties, and access-validation properties. It MUST NOT
decode an unrelated property merely because that property is present on the
canonical row.

The same required-property rule applies when a node access feeds a grouped or
global aggregate. The decoded set is the union of group-key properties,
aggregate-operand properties, residual-predicate properties, and access-
validation properties. `COUNT(*)` and `COUNT(node)` require node identity but
no user property. A plan MUST decline this representation when an aggregate or
grouping expression requires the complete node value. The binding-preserving
scan remains an internal physical operator; it MUST NOT expose a partial node
map as a query result.

A persistent index probe first obtains candidate node identities, then asks the
canonical row reader for only that required-property set. Candidate validation
MUST include all access properties before a row becomes visible. COW/WAL overlay
rows retain authority over matching base identities and are projected to the
same field set before entering an executor batch. If a persistent projection is
not selected, the canonical fallback MAY decode a full row, but it MUST preserve
the same result semantics. Corruption after index selection fails closed; it
MUST NOT retry through a less selective path.

The final physical plan, fingerprint, `EXPLAIN`, and scan-pruning report MUST
identify the retained access class. The optimizer trace MUST expose a distinct
plan-finalization stage so qualification can distinguish logical rewriting,
access-path selection, and required-data enforcement.

An unfiltered, non-distinct global `COUNT` over every node, one label, every
relationship, or one relationship type MAY use the exact basic Count Store.
The relationship rewrite is valid only for a required one-hop pattern with no
endpoint-label or relationship-property constraint. `COUNT(*)` and
`COUNT(relationship)` are equivalent in that shape because a required expand
always binds the relationship. The fast path MUST decline optional, variable-
length, distinct, filtered, endpoint-constrained, or property-constrained
patterns. Its result, fingerprint, `EXPLAIN`, and pruning report MUST identify
the exact-count access rather than an adjacency expansion or aggregate scan.

`IndexNodeUnionSeek` reads each equality branch through its declared index,
deduplicates candidate node identities before hydration or output, and retains
the complete `OR` predicate as residual semantic authority. Its deduplication
set is charged to the query blocking-state budget. A budget failure aborts the
query; it MUST NOT fall back to an untracked scan or emit a partial result.
`HawDBPropertyIndexPruning.tla` models the all-branches-declared admission rule
and proves that set-union deduplication preserves full-scan results.

Composite equality indexes reuse that artifact and serving contract. The
ordered property-name list is encoded into an unambiguous internal projection
identity, and the ordered property values form one bounded list key. A lookup
selects the projection only for the complete declared order, reads only tuple-
overlapping blocks, validates every candidate against its canonical row, skips
base rows shadowed by the COW/WAL overlay, and finally streams matching overlay
rows. A missing or incomplete composite definition falls back to the canonical
path; corruption after selection fails closed and poisons the handle.

Relationship-property equality and range indexes reuse the generation-bound
property artifact with relationship-specific kind tags, so a numerically equal
label id and relationship-type id cannot share a definition. Checkpoint build
streams canonical relationships after nodes under the same spill, generated-
entry, key-size, and resident-byte limits. The complete node and relationship
definition directory is admitted before artifact creation; defaults reject
more than 65,536 definitions or 8 MiB of definition residency, and the build
report records both actual values. Opening the artifact keeps payload blocks
cold. A one-hop expansion selects a supported relationship probe only
when the estimated key-overlapping property entries are no greater than the
estimated endpoint/type adjacency entries; otherwise it retains the adjacency
path. A selected probe reads only key-overlapping blocks, re-reads each
candidate relationship from canonical storage, verifies type, property, and
endpoint predicates, skips base relationships shadowed by the COW/WAL overlay,
and then streams matching overlay relationships. Unsupported or incomplete
definitions fall back to adjacency. Corruption after selection fails closed
and poisons the database handle. `EXPLAIN ANALYZE` records the selected
relationship pruning strategy and candidate counts.

### Persistent graph index qualification

Persistent graph index selection is qualified independently for node equality,
node range, node full-text, ordered node composite equality, relationship
equality, relationship range, forward adjacency, and reverse adjacency. A
class MUST NOT be declared production-qualified from logical pruning output or
generic cache misses alone.

Every selected persistent reader records monotonic process-local counters on
the owning store. Read transactions share the same counter set as their pinned
store snapshot. The counters identify the exact index class and accumulate
property-projection or adjacency blocks considered, blocks pruned, blocks read,
bytes read, decoded entries or records, returned candidates, and sparse/dense
adjacency layouts. Canonical fallbacks MUST NOT increment a persistent-class
counter. These counters are observability evidence only; they MUST NOT affect
query semantics, admission, or index selection.

`StorageResourceProfileReport` snapshots the counters before and after one
streamed query and publishes only the saturating delta. A production index case
uses a dedicated qualification handle with no concurrent query traffic and
binds one required class to the exact production identity and canonical graph
epoch, a redacted query and parameter digest, a precomputed reference result
digest and row count, and explicit per-run block and byte budgets. Every
measurement run MUST observe the required class, at least one admitted page
read, and no digest mismatch. The first run is cold and the required class MUST
add at least one cache miss; that class MUST add a cache hit on a subsequent
run. Generic process-wide cache deltas cannot satisfy either obligation. The
residency snapshot reports property-projection and adjacency artifact bytes
separately, so the runner rejects a case unless the artifact for the required
class is larger than the configured cache. A cancellation probe records latency
and cache pins, requires a non-poisoned handle, and performs the digest read
only after cancellation. The runner computes that digest through a bounded
streaming consumer and never retains or serializes row payloads.

The reference digest is created offline by the canonical fallback oracle over
the same dataset/query identity. Runtime differential tests additionally clone
one pinned snapshot, disable the rebuildable property and adjacency readers on
the oracle copy, and require exact result parity for all eight classes. This
test-only oracle MUST NOT become a production query switch.

Production qualification of one class does not activate another. Each class
requires evidence bound to the same current generation for:

1. exact differential parity against the canonical fallback;
2. checkpoint/reopen and complete WAL-overlay recovery;
3. cold/warm cache, page, byte, pin, cancellation, and corruption behavior;
4. the production-shaped resource and result-digest run.

The typed production matrix requires all eight classes exactly once against
the same replica and production identity. It sorts the resulting reports by
class for deterministic evidence, prefixes every blocker with its class, and
publishes readiness only when all eight independent reports are ready. A
partial matrix, duplicate class, changed open/runtime configuration, or mixed
canonical generation is invalid rather than a weaker readiness state.
Every case retains its declared per-run block and byte limits alongside the
observed maxima and raw cold/warm runs. The final release evaluator treats the
matrix as a required artifact and independently checks class completeness and
order, exact release identity, result parity, operation and I/O aggregates,
per-run limits, larger-than-cache residency, and cancellation cleanup. It does
not accept the matrix's top-level readiness as proof.

Missing evidence, stale generation evidence, or an index/row generation
mismatch leaves that class unqualified while other classes may remain
qualified. Once a selected page reports corruption, the read fails closed and
the handle is poisoned; it MUST NOT retry through the canonical fallback.
`HawDBGraphIndexQualification.tla` models this independent evidence gate.

The immutable index-page codec is the first format-only slice of step 1. It
defines generation-tagged root, interior, leaf, and posting pages. Every page
has an independent CRC32C and SHA-256 payload digest; roots also bind the index
identity, source commit epoch, schema digest, child page, and tree height.
Field-tagged payloads skip unknown fields, while duplicate or missing required
root fields, unordered keys/postings, oversized fields/pages, truncation, and
checksum mismatches fail closed. The codec is not selected by the durable
manifest and does not change query or recovery behavior yet.

The relational index shadow publisher is the first implementation of step 2.
It builds one exact required root set from the current relational oracle. Each
table contributes a primary root, one root per table-level unique constraint,
one root per declared unique or secondary index, and one foreign-key-support
root per referencing constraint. Every root descriptor records its semantic
role and the table schema digest. The manifest separately binds the catalog
schema digest and the ordered logical root-set digest, including table, index,
role, and table schema identity. Reserved synthetic names are engine-owned so a
declared index cannot impersonate a constraint root. Trees use
generation-specific fixed-size slots, so a `PageId` determines its offset
without a cardinality-sized in-memory directory. Default slots are 64 KiB,
with 16 KiB admission limits for encoded keys and logical row locators, to
bound shared-host random I/O and avoid one-megabyte amplification for sparse pages.
The total required-root count is admitted before a candidate file is created,
so an oversized catalog cannot first build an unselectable artifact.
Every checkpoint attempt writes a generation-specific page artifact and then a
generation-specific root manifest after every slot is synced. Only after both
candidate files are durable may the canonical checkpoint with the same
generation and source commit epoch be published. Candidate construction is
best effort in `Shadow` and `DemandPaged` modes: admission, I/O, or encoding
failure is reported and removes the selected read view, but MUST NOT fail or
replace the canonical checkpoint. A crash before canonical publication may
leave a future candidate orphaned; normal writable open ignores and reclaims it
through generation cleanup while recovering the selected checkpoint plus WAL.

Successful construction also returns one typed
`RelationalIndexGenerationArtifacts` identity. It binds generation, source
commit epoch, catalog schema digest, exact root-set digest, and the length,
CRC32C, and SHA-256 of both the page artifact and generation manifest. Page-file
integrity is accumulated while fixed slots are emitted, so producing this
identity does not reread an index-sized file or add index-cardinality-sized
metadata. Manifest integrity is computed from the already bounded encoded
manifest. A successful candidate is reverse-bound by the publish-last durable
manifest for the same checkpoint generation. Candidate failure writes an
all-absent binding; partially present identity or artifact metadata is invalid.

These artifacts remain non-authoritative in `Shadow` and `DemandPaged` modes.
The canonical checkpoint does not depend on their optional reverse reference
in those modes. Open selects only the bound generation and reads its bounded
generation manifest into one byte image. That same image is verified against
the outer length, CRC32C, and SHA-256 binding and then decoded to verify its
internal generation/source-epoch, catalog schema, exact root set, and page
artifact length. Open MUST NOT verify one read and decode a second read of the
same manifest. It does not scan the page artifact to recompute its full digest
during normal open. An unbound candidate, including a legacy latest-pointer
manifest whose epoch happens to match the checkpoint, is never selected. A
missing, extra, renamed,
re-roled, schema-drifted, or manifest-corrupt root rejects the candidate. A
corrupt selected candidate does not prevent canonical open in `Shadow` mode.
In `DemandPaged` mode, an integrity failure for the explicitly selected
candidate fails the indexed statement closed instead of silently using
materialized postings; missing or admission-unavailable candidates may still
take the observable materialized fallback while that oracle exists.

`Authoritative` mode makes the complete binding mandatory for open and makes
the generation-pinned base plus recovery/live view a constraint dependency.
The view identity MUST match the bound base generation, source commit epoch,
root-set digest, and current visible commit epoch. Missing, stale, corrupt,
poisoned, or unavailable state rejects open or the next operation. A
snapshot MUST pin both the immutable read view and the complete generation
artifacts; it MUST NOT need a mutable durable handle to revalidate that
identity. A live durable handle additionally cross-checks the pinned artifacts
against the currently published canonical manifest binding. A
checkpoint in this mode MUST prepare a complete candidate before canonical
manifest publication; candidate failure aborts the checkpoint rather than
publishing an unusable authoritative generation.

Backup includes exactly the bound generation page artifact and generation
manifest, rejects unbound or extra relational-index files, and verifies their
full length, CRC32C, and SHA-256 before restore publication. Deep scrub also
recomputes both full artifact digests and validates their decoded identity.
Normal generation reclamation recognizes both file names, retains the previous
generation, and does not unlink older files while a reader epoch is pinned.

Relational primary keys are encoded as bounded, order-preserving logical row
locators, including composite keys. Opening a valid candidate verifies only its
manifest fence and artifact length; page header and payload integrity are
checked on first access. The sequential slot writer uses constant
page-accounting metadata; it does not retain a page-id set or offset directory
proportional to index size.

The relational index demand reader implements step 3.
Exact-key traversal reads one root-to-leaf path; leading composite-key prefix
traversal skips subtrees whose upper bound precedes the encoded prefix and
stops after the contiguous prefix range. Oversized postings are streamed from
their page chain and decoded back into logical composite primary keys. Every
lookup enforces page, byte, row, and tree-height limits and reports the pages,
bytes, leaf entries, matched keys, and rows it consumed. A callback may stop a
large posting early; values observed before an eventual error are provisional
and must be discarded. Admission or a missing root does not poison the reader,
while structural, checksum, generation, and row-locator corruption does.
Activation remains independent from publication and is off by default.

The relational recovery-delta path implements step 4 without activating SQL.
Relational apply emits the final insert/delete change for each affected
`(index identity, encoded index key, encoded primary key)` tuple while it is
already visiting the transaction's bounded changed-key set. It does not scan
the base index or infer changes independently from WAL syntax. Recovery
coalesces those tuples in an entry- and byte-bounded ordered overlay. A full
overlay is streamed directly to a checksummed immutable delta page without a
second cardinality-sized encoding buffer. Each replay attempt uses a unique
delta generation, so candidate pages never overwrite files referenced by the
previous recovery manifest. The base generation, base commit epoch, delta
generation, ordered page epoch ranges, recovered commit epoch, lengths,
CRC32C, and SHA-256 digests are published in one manifest only after strict
WAL replay completes. The `SKRIDXR1` version 1 manifest uses a fixed 148-byte
header. It also binds a 56-byte recovery-source identity containing the WAL
generation, inclusive start LSN, exclusive end LSN, and a
domain-separated SHA-256 over the ordered record sequence. Each record
contributes its LSN, exact logical payload length, and payload SHA-256. The LSN
range length MUST equal the recovered-epoch distance from the immutable base.
Opening a recovery reader requires the exact source identity recomputed while
validating that WAL prefix; matching only the recovered epoch is insufficient.
Crashes before that replacement leave the prior
manifest intact and new pages orphaned.

Point and leading-prefix differential reads merge the cold base with delta
pages in epoch order and suppress duplicate row locators under the same read
page, byte, and row budgets. This reader remains evidence-only. A
schema-changing relational WAL record or relational snapshot invalidates the
candidate because its schema digest/root set no longer matches the checkpoint;
normal open continues from canonical checkpoint plus WAL and records
`RecoveryUnavailable` rather than performing an unbounded startup backfill.

After a base or recovered reader is pinned, normal commits maintain one
immutable in-process relational index read view. Its identity binds the base
generation, optional recovery-delta generation, base and visible commit epochs,
and root-set digest. Relational DML appends transaction-apply change evidence
as immutable `Arc`-shared live batches; publishing a newer view clones only the
batch-pointer directory and retains the prior view for already pinned
snapshots. The total live overlay is admitted by both raw change count and
encoded bytes. Graph-only commits advance the view's visible epoch without
adding a batch. DDL, relational snapshot replacement, poisoned backing pages,
epoch discontinuity, or exhausted live admission removes the current view and
records an explicit unavailable status; canonical WAL publication still
succeeds, but no reader may continue from stale postings. This read view may be
selected only by the bounded SQL activation described below.

`GraphStore::qualify_relational_index_read_view` is the bounded typed evidence
path for that activation boundary. It samples a configured maximum number of
tables and rows, generates exact probes for every required primary,
table-unique, declared-unique, secondary, and foreign-key-support index, and
generates every leading prefix for sampled composite indexes. Each probe merges
the pinned base, recovery-delta, and live batches under the production page,
byte, row, and tree-height read limits, then compares
the ordered logical row locators with the current materialized relational
oracle. Reports expose immutable view identity, physical read evidence, live
work, row counts, and SHA-256 result digests without exposing sampled key
values. Missing views, corrupt pages, and admission failures return errors;
semantic differences, incomplete index coverage, or exhausted qualification
budgets return `ready = false`. This API does not introduce a business-specific
lookup surface.

`GraphStore::qualify_relational_constraint_read_view` narrows that evidence to
the exact lookups required by primary-key identity, unique enforcement, UPSERT
conflict detection, foreign-key target existence, and foreign-key referrer
discovery. It generates present and deterministic absent-key probes, includes a
null-containing probe for every nullable unique target, and deduplicates one
physical lookup that satisfies several semantic uses. Primary, table-unique,
declared-unique, and foreign-key-support roots are checked through one pinned
base-plus-recovery-plus-live view against the materialized postings at the same
visible commit epoch. Oracle row cloning is bounded by the production row limit;
the report exposes key and result digests rather than key values. Exhausted
table or probe coverage returns `ready = false`; the row sample limit bounds
representative probe discovery. Physical read admission, missing roots, or
corruption returns an error and provisional rows are discarded. A ready report
is evidence for sampled constraint semantics only. It does not itself change
routing. Selecting `Authoritative` is the separate explicit activation step;
materialized postings remain available only as a transitional differential
oracle for non-authoritative modes.

Relational index generation builds derive every non-primary entry directly
from the canonical rows rather than reading the materialized posting maps. A
root that fits its configured sort-memory budget remains in memory. A larger
root uses bounded external-sort runs with explicit aggregate spill-byte,
per-root run-count, and merge-fan-in limits. The final merge groups one ordered
index key at a time and streams row identifiers into bounded posting pages;
neither one high-cardinality posting nor the complete index is retained during
page encoding. A merge derives its exact output length from admitted source
runs and reserves that cumulative spill budget before creating the destination
run. Sort admission reserves I/O buffers and charges two-times
headroom for growable entry and merge-heap allocations. Every temporary entry
has a CRC32C so a corrupted spill cannot be re-encoded as a self-consistent
published index. Temporary runs are not durable state and are removed after
success or failure; a later publisher removes stale runs under the exclusive
publication lock after a process crash. A stale run that cannot be removed
fails the build instead of silently accumulating disk use. Build reports expose
cumulative run and spill bytes plus peak sort-memory bytes. This changes
checkpoint construction only: ordinary open still materializes postings until
the separate authoritative-residency stage removes that dependency.

PostgreSQL SQL activation is controlled by
`DatabaseConfig::relational_index_mode`. `Materialized` is the default rollback
mode, `Shadow` publishes and qualifies persistent generations without serving
them, and `DemandPaged` selects them for eligible reads with an observable
fallback. `Authoritative` selects the same bounded reader but prohibits
materialized fallback. Primary-key, unique, leading secondary-prefix, and index
nested-loop probes consume logical row locators from one generation-pinned
view and hydrate rows from the same relational snapshot. One statement-wide
ledger bounds logical pages, logical bytes, result locators, and tree height
across every probe, including repeated inner-side join probes. A missing view,
missing optional query index, or admission rejection before provisional output
uses the observable canonical materialized fallback. Corruption, durability or
generation mismatch, view-identity drift within a statement, and a locator
whose canonical row is missing fail closed. A writable authoritative
transaction pins the committed row and index views at begin and appends every
successful statement's row and index changes to private immutable overlays.
Queries and constraint checks merge those pinned bases with all prior statement
batches, so read-your-own-writes never consults the pre-transaction view alone
and never reconstructs database-sized posting maps. The private overlay has
cumulative entry and encoded-byte limits, and all index reads share a transaction-wide
page/row/byte ledger. If staging, constraint validation, or overlay admission
fails, the statement leaves the row workspace, accumulated WAL writes, and
private index overlay unchanged. The observable runtime path is
`transaction_workspace`, not a fallback to stale or materialized state. Commit
revalidates the complete write group against the then-current authoritative
view before WAL append.

In `Authoritative` mode one transaction-scoped ledger bounds the aggregate
logical pages, bytes, and row locators consumed by all primary, unique, UPSERT,
foreign-key-target, and foreign-key-referrer probes. The pinned view validates
constraints before WAL append. Constraint failure, exhausted admission, a
missing required root, or failure to stage the next live view leaves the WAL
LSN, canonical rows, and visible row/index epochs unchanged. After WAL append,
row state and the already-staged index view publish the same new epoch.
Schema-changing relational transactions are rejected until a new complete
generation can be prepared outside authoritative mutation service.

`EXPLAIN ANALYZE` reports the runtime path, fallback reason, base and delta
generations, commit epochs, root-set digest, logical and physical page bytes,
cache outcomes, recovery-delta work, live-overlay work, and index rows. It
distinguishes `demand_paged`, `authoritative`, canonical fallback, and mixed
execution. Plain `EXPLAIN` remains history-independent. Authoritative
activation changes constraint and fallback semantics and omits materialized
postings. An `OutOfCore` plus `Authoritative` open constructs metadata-only
relation state only after the canonical row view and authoritative index view
are validated at the current epoch. Cold open constructs only schemas and
exact row counts from the self-describing canonical row root and never decodes
or constructs the transitional checkpoint rows. With a non-empty WAL, a
read-only handle validates every frame and record without applying
schema-stable relational DML to a row oracle, then reuses only exact
pre-published row and index recovery artifacts. A writable handle instead
replays every authenticated access set through bounded sparse row and index
builders. Exact final counts come from the row recovery manifest. Both modes
must reach the same recovered epoch before serving. Schema-changing or snapshot
WAL, a missing required artifact, or any identity drift rejects open.

The storage crate and `GraphStore` provide the writable sparse-recovery path
used by that production selector. For every authenticated WAL
access, `GraphStore::hydrate_sparse_relational_recovery_access` probes
`RelationalRowDeltaBuilder::lookup_staged` first. The builder resolves the
bounded dirty map before immutable recovery runs newest-first, using the same
checksummed decoder as a published reader. A miss is demand-read from the
generation-pinned checkpoint root. This preserves read-after-earlier-WAL
semantics without attaching either source to the relational state. Delta-run
and checkpoint logical bytes share one cumulative per-record read budget;
checkpoint point reads receive only the remaining byte allowance.
`RelationalState::stage_sparse_transaction_for_authoritative_recovery_with_replay_access`
then accepts one explicit hydrated result for every authenticated access-set
entry, including explicit absence. It rejects missing, reordered, duplicate,
unknown, unhydrated, oversized, or byte-over-budget input before replay. The
temporary materialized workspace contains only those entries, reuses the
existing logical transaction implementation as the differential oracle,
compares the recomputed access set, advances exact detached row counts, retains
only newly created content-addressed overflow segments, and is then discarded.
Older unreachable recovery overflow segments may remain until an explicit
full-scan overflow compaction because proving them unreachable would require
scanning rows outside the bounded workspace.

Live schema-stable DML uses the same metadata-only merge boundary through
`RelationalState::stage_sparse_transaction_with_authoritative_replay_access`.
Its hydrated workspace must be strictly ordered, duplicate-free, fully
hydrated, and bounded by both entry count and conservative resident bytes. It
may be a strict superset of the resulting replay-access set: unchanged rows
returned by the pinned authoritative unique or foreign-key indexes are valid
constraint-support rows. The existing authoritative transaction engine runs
against that workspace, so unique and foreign-key semantics are not
reimplemented by `GraphStore`. Every actual replay-access key must still have
an explicit present or missing entry. An unhydrated mutation key or required
constraint posting fails closed before metadata merge. Constraint-support rows
that remain unchanged contribute zero to the detached row-count delta.

Candidate discovery is also storage-owned. First,
`RelationalState::plan_sparse_transaction_hydration` derives direct primary-key
point reads, predicate tables that require a bounded complete range read, and
generation-pinned UPSERT conflict probes from the schema and logical
transaction. Direct keys retain explicit absence; point reads covered by a
predicate table scan are deduplicated. Second,
`RelationalState::prepare_sparse_transaction_for_authoritative_live` replays
the transaction without publishing it and returns the exact replay-access set
plus all unique-key, foreign-key-target, and parent-delete referrer probes
derived from the real row and index change captures. A primary-key rewrite may
therefore add a missing destination key after the first preparation pass.
`GraphStore::hydrate_sparse_relational_live_workspace` demand-hydrates every
newly discovered replay key and every posting returned by those probes, repeats
preparation until the set is closed, and enforces one cumulative
entry/read/resident-byte budget across all passes. One generation-pinned row
snapshot and one authoritative index reader remain fixed for the complete
closure and final validation. The workspace builder admits each present or
missing key atomically and conservatively reserves the later replay-access
ledger before retaining the row. Preparation never changes detached counts,
appends WAL, or publishes a live view. The final authoritative live stage
remains the only semantic validation whose captures may cross the durability
boundary.

`GraphStore` uses that bounded path whenever a metadata-only state reaches the
relational commit boundary. It stages canonical metadata plus row and index
live-view candidates before WAL, requires both view candidates to be valid,
then appends WAL and advances all three visible epochs together. Failure or
budget rejection before WAL leaves the prior state, WAL LSN, and read views
unchanged. A crash after WAL may discard unpublished candidates; recovery must
replay the durable transaction and reconstruct the same epoch.

A multi-statement `DatabaseTransaction` pins the committed row and index views
once. Each successful schema-stable statement first prepares a replacement
metadata state, a private immutable row batch, and private authoritative index
changes. The transaction installs the row batch and index ledger only after
both pass cumulative entry and resident-byte admission. A rejected statement
therefore leaves both private views and the accumulated logical transaction
unchanged. Reads use the transaction row snapshot plus the transaction index
workspace, so read-your-own-writes never requires a materialized database copy.
Final commit revalidates the complete transaction against the then-current
canonical views before the ordinary WAL-before-visibility boundary.

A metadata-only checkpoint also remains bounded. Changed keys are resolved
from the final recovery/live row overlay rather than the intentionally empty
metadata state. Overflow publication streams and conservatively retains the
complete pinned base descriptor set while merging only dirty-page references;
exact unreachable-extent reclamation is a separate full-scan maintenance job.
The row candidate is published next. Required indexes are then rebuilt by
paging that new row root in bounded batches and feeding the existing
memory-capped, spillable index sorter. The outer checkpoint manifest binds the
row, overflow, and index candidates atomically. It does not emit the legacy
full-row relational checkpoint, because doing so would require database-sized
hydration. A later reopen with a residency/index mode that requires that legacy
artifact fails closed instead of interpreting metadata-only state as empty.

`OutOfCore` plus `Authoritative` is the production selector for canonical
metadata-only relational rows. A checkpointed database MUST construct relation
schema and exact detached counts from the self-describing row root without
decoding checkpoint rows. Clean opens require current row and index views at the
checkpoint epoch. WAL opens either reuse exact source-bound recovery artifacts
for a read-only handle or replay every authenticated access through bounded
sparse builders for a writable handle. Both views must match the final database
epoch before open succeeds. Snapshot WAL and schema-changing WAL are rejected
until a new canonical checkpoint exists. Other residency or index modes retain
the materialized path only for checkpoints they created; mode switching after
a metadata-only checkpoint is intentionally rejected in unreleased v1.

Selector activation is implementation readiness, not deployment qualification.
Representative-copy cold/warm latency, RSS, page-fault, cache, pin, WAL, and
write-amplification evidence remains mandatory before a workload is declared
production-qualified.

## Identities and terminology

- **Commit epoch**: monotonically increasing visibility identity; one atomic
  graph/relational WAL batch owns one epoch.
- **Manifest generation**: immutable physical publication identity.
- **PageId**: stable logical page identity within one owning root namespace. A
  relational `PageId` is scoped to one table.
- **RowId**: stable logical record identity. A physical rewrite MUST NOT change
  the row's externally visible identity.
- **Page descriptor**: compact metadata containing page kind, key bounds,
  record count, extent offset/length, checksum, and generation.
- **Root descriptor**: the bounded entrypoint for one row or index tree.
- **Dirty overlay**: bounded, mutable transaction or recovery state newer than
  the published root.
- **Index delta page**: immutable disk-backed flush of a dirty index overlay.
- **Page pin**: an RAII lease preventing eviction or physical reclamation.
- **Derived projection**: rebuildable state selected by source epoch and schema
  identity but excluded from canonical recovery.

## Canonical row pages

### Relational layout

1. A relational table MUST have a primary key.
2. Leaf row pages MUST contain complete encoded rows ordered by primary key.
3. Page directories MUST support binary search without decoding every row.
4. A point read MUST decode only the selected row and requested fields.
5. Ordered pagination MUST stop after the admitted `LIMIT` and MUST NOT drain
   later pages.
6. A mutation creates new immutable page images or bounded dirty pages; it
   MUST NOT mutate a page visible to a pinned reader.
7. Every table root MUST carry the complete validated table schema, its digest,
   a non-zero column count, and the exact row count derived from published page
   descriptors. Publication rejects missing or mismatched schema bytes and a
   dirty or reused page whose column count differs. Recovery-delta schemas must
   match the digest and column count, and demand read treats any later
   page/root mismatch as corruption.

### Relational row-page v1 codec

The canonical relational leaf codec is implemented independently from serving
and publication. The byte representation is `SKINROW1`, version `1`, and uses
one fixed 140-byte header followed by exact-length variable regions:

```text
header
lower primary-key bound
upper primary-key bound
row slot directory
ordered primary-key payload
encoded row payload
```

The header stores the manifest generation, source commit epoch, non-zero page
identity, row and column counts, region lengths, schema SHA-256 digest, CRC32C,
and SHA-256. Generation and commit epoch are independent identities; both are
non-zero, while their agreement with a selected root is enforced by the later
publication protocol rather than by numeric comparison.

Each 16-byte row slot stores `(key_offset, key_length, row_offset, row_length)`
as little-endian `u32` values relative to its key and row payload regions. Slots
and both payloads MUST be contiguous, gap-free, non-overlapping, and cover their
regions exactly. Keys use the same reversible, order-preserving encoding as
persistent relational indexes. They MUST be strictly increasing, and the first
and last slot keys MUST equal the header bounds. These properties permit binary
search without decoding row values.

Each row starts with a column count and an 8-byte slot per value. A value slot
stores its payload offset and length. Value slots also cover their payload
exactly. A projected decode validates the selected row's complete value-slot
shape but materializes only the requested, strictly increasing column ordinals.
It does not decode any other row in the page. Full decode is the symmetric
validation path used by tests, scrub, and tooling.

Inline values use fixed tags plus bounded length prefixes. An overflow value
stores the logical descriptor as `(scalar type, u64 compressed length, u64
uncompressed length, 32-byte SHA-256 digest)`. Runtime state uses the same
fixed-width digest and lengths; it does not allocate a hexadecimal digest
string per row reference. The digest is the immutable location identity; a
publication-generation overflow manifest resolves it to a physical extent, so
page bytes do not embed a stale file offset.

This is the only version-1 overflow representation. HawDB has not shipped a
prior durable format, so readers MUST NOT recognize or migrate a legacy
string-digest encoding.

The row-root table payload likewise has one version-1 shape: table name,
complete encoded table schema, schema digest, non-zero column count, exact row
count, allocator state, descriptor range, and key bounds. The complete schema
and count live inside the checksummed generation manifest, so a future cold
open can reconstruct the relational catalog without decoding canonical row
pages or retaining the legacy materialized-row checkpoint. No reader for an
older root payload exists.

The shared `SKOVFL01` envelope begins with a fixed 32-byte header containing the
codec, scalar type, zero flags, both lengths, and the decoded CRC32C. Its
content SHA-256 is carried by the logical descriptor. Encoding admits the
uncompressed value before compression. Decoding verifies the exact header,
zero flags, descriptor binding, physical length, output length, CRC32C, and
hydration budget before publishing either the value or charged counters.
Overflow descriptors are valid only for `TEXT` and `BYTEA`. Unknown tags,
invalid UTF-8, invalid ordered keys, impossible lengths, descriptor or checksum
mismatches, non-zero fixed-slot tails, and trailing bytes fail closed.

The default codec envelope is one MiB and 256 rows, matching the current COW
row-page split target. It separately limits columns, key bytes, row bytes,
inline-value bytes, logical overflow bytes, and projected-field count. The
encoder applies the same limits as the decoder and rejects a page before
accumulating payload beyond the page budget. Checkpoint-bound roots now use the
codec as a recovery dependency. Shared-cache admission, projected field
decoding, and selected-field late hydration are implemented by the typed
generation-pinned demand reader. SQL selection and recovery/live overlay merge
remain a separate activation stage.

### Relational overflow-root v1 publication

`RelationalOverflowPublisher` publishes one complete logical set through four
immutable or publish-last artifacts:

```text
relational-overflow-{generation}.extents.hawdb
relational-overflow-root-{generation}.descriptors.hawdb
relational-overflow-{generation}.manifest.hawdb
relational-overflow.manifest.hawdb
```

The extent artifact contains only envelopes first introduced by the new
generation. The caller supplies the exact digest set reachable from the row
generation being prepared. A reachable digest already selected by the base
root reuses its immutable physical generation and byte range; a base digest
omitted from that set is absent from the new root. A pinned older root retains
its own logical set and physical closure independently.

Descriptors are exactly 120 bytes, strictly ordered by binary SHA-256 digest,
and contain the logical reference, physical generation, physical byte range,
envelope CRC32C, and a SHA-256 binding over the owning root generation,
descriptor ordinal, and first 88 descriptor bytes. Compressed and uncompressed
lengths remain `u64`; reserved bytes MUST be zero. The envelope byte length MUST
equal the compressed length plus the fixed `SKOVFL01` header. A complete valid
descriptor moved between an ordinal or root generation is therefore rejected.

The fixed 208-byte `SKOVRM01` manifest binds generation, source commit epoch,
optional previous generation, total and newly written extent counts, exact
length plus CRC32C/SHA-256 metadata for the extent and descriptor artifacts,
and the exact root-set digest. The manifest carries its own CRC32C and SHA-256.
Generation is always non-zero. Source commit epoch zero is valid only for the
empty root needed to back up and reopen a newly created database.
Normal open reads only this fixed manifest and the two current-generation file
lengths. It does not enumerate descriptors or hash extent payloads.

Publication holds one directory-scoped exclusive lock and synchronizes the new
extent artifact, descriptor root, and immutable generation manifest in order.
It then revalidates the caller's selected base. Standalone publication may
atomically replace `relational-overflow.manifest.hawdb` last. Canonical
checkpoint preparation instead stops at `CanonicalSelectionDeferred` and
returns typed generation artifacts; only `HAWDB_MANIFEST_V1` may select that
exact generation together with its row root. A target generation is fresh and
immutable. A crash or stale publisher can leave only unbound generation
artifacts. A reader pins one immutable root manifest; reused descriptors retain
their physical generation, so the pinned reader stays readable while a newer
root is published. Reclamation enumerates descriptors for the current and
immediately previous canonical roots before deletion and retains every
referenced physical extent generation.

`RelationalOverflowRootReader` binary-searches descriptors without loading the
root, validates the selected descriptor binding, and admits the declared
compressed and decompressed bytes plus the peak input-envelope and decoded
output memory before opening the physical extent or allocating its input
buffer. It then reads exactly one physical range, verifies its CRC32C and
content SHA-256, and invokes the shared envelope decoder. Admission or
corruption does not partially charge the caller hydration budget.

### Exact relational overflow compaction

Normal metadata-only checkpoints intentionally retain the complete pinned
overflow descriptor root. Exact reachability is established only by the
separately invoked `Database::compact_relational_overflow` maintenance
operation. It is valid only for a writable durable database serving canonical
metadata-only rows through the current `OutOfCore` plus `Authoritative` row and
index views.

The operation first requests one background control permit with one CPU slot,
one I/O slot, and the complete compaction-specific working-memory reservation.
The ordinary checkpoint stages retain their own existing bounded builders. The
operation then pins one row snapshot and scans every current table and row
through `visit_projected_range_unhydrated`. Page, row, encoded-read-byte,
overlay-entry, overlay-resident-byte, reference-occurrence, sort-memory,
spill-byte, spill-run, and rewrite-byte limits are hard admission bounds. The
scan decodes row fields but preserves every `TEXT` or `BYTEA` overflow
descriptor; it MUST report zero payload hydrations. References are sorted and
deduplicated either in memory or through checksummed fixed-record temporary
runs. The resulting set is repeatable because publication preflight and
candidate writing consume it independently.

Before creating a candidate, publication merges that exact set with the pinned
base descriptors, rejects conflicting digest metadata, validates every newly
introduced inline envelope, and proves that descriptor and physical rewrite
limits are sufficient. Every reachable base envelope is copied one at a time,
without decompression, into the fresh generation. This physical rewrite is
required: retaining a descriptor that points into an old mixed live/dead
artifact would keep the unreachable bytes physically reachable. References
absent from the exact set are omitted. The fresh overflow generation is
published first, followed by the row and required index candidates; only the
outer checkpoint manifest selects the complete generation.

Admission failure, cancellation, stale source identity, corruption, or I/O
failure before outer-manifest replacement leaves the previous checkpoint
selected. Candidate cleanup removes temporary runs and unpublished generation
artifacts. Current, immediately previous, and reader-pinned generations retain
their complete physical closure. Consequently
`reclaimable_base_extent_count` reports descriptors excluded from the new root,
not files already deleted; physical deletion occurs only after generation
retention and reader pins permit it.

The default operation reserves approximately 154 MiB of working memory: 8 MiB
for reference sorting, 16 MiB for row overlays, two one-MiB pages, and a
conservative two-value overflow envelope. This is compatible with the
separately configured 512 MiB low-memory capability profile, but 512 MiB is
neither HawDB's default nor a universal host limit. On an 8 GiB host, automatic
HawDB capacity remains dynamically bounded to at most 2 GiB and normally falls
within 1--2 GiB. Scan, spill, and rewrite limits govern I/O and disk work; they
do not increase the admitted resident-memory reservation. Production-copy RSS,
page-fault, elapsed-time, write-amplification, and reclaimed-byte evidence
remains an activation gate rather than an implementation invariant.

`run_production_content_store_overflow_compaction_qualification` is the typed
collector for that activation gate. Its input is an existing caller-owned
disposable writable replica bound to the exact source revision, target,
configuration, dataset fingerprint, and initial commit epoch. The collector
MUST NOT copy or mutate the source production database. It requires
`OutOfCore` plus `Authoritative` serving, a metadata-only row checkpoint, a
non-zero segment cache, an overflow-bearing row artifact larger than that
cache, a declared memory profile, and frozen parameterized SQL verification
cases with offline result digests and hard row, payload, intermediate-row,
page, and byte budgets.

The collector records process steady and peak RSS, supported page-fault
counters, elapsed time, runtime-governor admission, scan and spill bounds,
new-or-grown durable artifact bytes, write amplification relative to the
published live overflow extent bytes, reclaimable descriptors, and physically
removed extent files and bytes. It verifies the SQL result digests before and
after compaction. It then advances the disposable replica by one parameterized
Cypher qualification marker and publishes a later checkpoint so physical
reclamation is observable, scrubs the selected durable closure, reopens the
replica, and repeats the SQL verification. The report contains no replica path
or business payload. Missing counters requested by policy, mismatched
generation or epoch identity, payload hydration during the closure scan,
budget excess, absent physical deletion, scrub failure, or reopen drift blocks
activation.

The 512 MiB resource kind is an explicitly configured low-memory capability
run. It is not selected automatically and does not redefine the shared-host
capacity policy. The 8 GiB shared-host kind retains dynamic admission and a 2 GiB
maximum HawDB capacity; its measured RSS limits remain separately declared in
the qualification input.

`hawdb-content-store-overflow-compaction-qualification` is a thin developer
and evidence wrapper over this typed collector. It accepts exactly one existing
caller-owned disposable replica plus one bounded JSON plan; it MUST NOT copy,
create, migrate, or identify the source database in retained output. The plan
serializes every scan, overlay, rewrite, sort, spill, result, RSS, page-fault,
latency, write-amplification, and physical-reclamation bound so the release
evaluator can recompute admission from raw evidence. The final release bundle
requires two current-identity artifacts: one explicit 512 MiB capability run
and one dynamic 8 GiB shared-host run. Neither artifact can satisfy the other's
profile obligation, and a top-level `ready` value cannot override inconsistent
raw policy, generation, digest, resource, or reclamation fields.

### Relational row-root v1 publication

`RelationalRowPagePublisher` publishes a generation through five immutable or
publish-last artifacts:

```text
relational-row-pages-{generation}.pages.hawdb
relational-row-root-{generation}.descriptors.hawdb
relational-row-root-{generation}.keys.hawdb
relational-row-pages-{generation}.manifest.hawdb
relational-row-pages.manifest.hawdb
```

The page artifact contains only dirty page images from the new generation in
fixed one-MiB slots. Clean logical pages retain their prior physical generation
and slot. A generation root is a complete, streaming-written directory over
the selected base root plus inserted, replaced, and deleted logical page ids;
the publisher does not materialize the complete page map in memory. Root
metadata may be rewritten sequentially while row payload write amplification
remains proportional to dirty pages.

Each root descriptor is exactly 136 little-endian bytes:

```text
u64 logical_page_id
u64 physical_generation
u64 physical_slot
u64 page_source_commit_epoch
u32 row_count
u32 exact_encoded_page_length
u64 lower_key_offset
u32 lower_key_length
u64 upper_key_offset
u32 upper_key_length
u32 page_slot_crc32c
u8[32] page_slot_sha256
u32 descriptor_binding_crc32c
u8[32] descriptor_binding_sha256
```

The descriptor binding digest covers the owning root generation, global
descriptor ordinal, first 100 descriptor bytes, and referenced lower and upper
key bytes. This rejects corrupted physical identity, slot, bounds, lengths, or
offsets and prevents complete valid descriptors from being moved between root
generations or ordinals without detection when that descriptor is selected.
The slot digest covers the complete fixed page slot, including the required
zero tail. Table descriptors are contiguous and ordered by disjoint primary-key
bounds. A table root in the compact manifest stores its complete digest-bound
schema, exact descriptor-derived row count, the next never-issued table-scoped
`PageId`, descriptor range, and outer bounds. The allocator value is non-zero,
never decreases, and is strictly greater than every active, dirty, or deleted
page id admitted by that publication.

The `SKRPGM01` version-1 manifest has a fixed 316-byte header followed by a
bounded table-root payload. Its header binds generation, source commit epoch,
optional previous generation, slot size, dirty and root page counts, exact
length plus CRC32C/SHA-256 metadata for the page, descriptor, and key artifacts,
the table-root-set SHA-256, and an optional exact overflow-root generation,
source epoch, and root-set digest. The manifest has its own CRC32C and SHA-256.
Table names and key bounds are length-prefixed and bounded before allocation.
Table descriptor ranges MUST be contiguous and cover the declared root page
count exactly. Source commit epoch zero is valid only when both the table set
and root page count are empty.

Standalone publication holds one directory-scoped exclusive lock and follows
this order:

1. pre-admit table count, dirty page count, fixed-slot dirty bytes, root pages,
   root-key bytes, and manifest bytes before creating a candidate;
2. write and synchronize dirty page slots;
3. stream and synchronize the complete root descriptor and key artifacts;
4. publish the immutable page artifact, then both root artifacts;
5. publish the immutable generation manifest;
6. re-read and compare the selected latest generation with the caller's
   expected base;
7. atomically replace `relational-row-pages.manifest.hawdb` last.

Canonical checkpoint preparation uses the same first six steps but calls
`persist_generation`, records `CanonicalSelectionDeferred`, and returns typed
generation artifacts instead of updating the independent latest selector. The
publish-last `HAWDB_MANIFEST_V1` binds the row generation, source commit epoch,
root-set digest, generation-manifest length, CRC32C, and SHA-256 together with
the exact overflow generation. Row and overflow bindings are both mandatory
for every non-empty canonical checkpoint.

A target generation is immutable and MUST be fresh. A stale publisher or a
generation whose files already exist fails without replacing the latest
manifest. A crash may leave page, root, or generation-manifest candidates, but
none is reachable until the latest manifest is replaced. Retrying uses a fresh
generation. Publication does not reclaim old artifacts, so a generation-pinned
`RelationalRowPageRootReader` continues to resolve its complete
cross-generation descriptor closure while a newer root is published.

Normal root open reads and validates only the bounded manifest and exact
artifact file lengths. It MUST NOT hash or enumerate every descriptor or row
page. Descriptor/key binding checks occur when a descriptor is selected; page
slot integrity remains a demand-read obligation. Full artifact digest checking
belongs to scrub. A row generation containing overflow descriptors MUST be
published with the overflow root having the same generation and source commit
epoch. Every referenced digest is resolved before row candidate creation, and
the row manifest records the exact overflow root binding. A missing, corrupt,
or differently bound root rejects the complete row publication.

`RelationalRowPagePublicationReport.events` has two fixed refinement traces.
Standalone publication ends with `LatestManifestPublished`; canonical
candidate persistence ends with `CanonicalSelectionDeferred`:

```text
CandidateStarted
CandidatePagesDurable
CandidateRootDurable
CandidateManifestDurable
BaseRevalidated
LatestManifestPublished | CanonicalSelectionDeferred
```

These events map in order to `BeginCheckpoint`, `PersistCandidatePages`,
`PersistCandidateRoot`, `PersistCandidateManifest`, the generation fence, and
`PublishCheckpoint` in `HawDBCowPagePublication.tla`. The canonical trace does
not make the candidate visible at its final publisher event; the outer
checkpoint manifest selects both roots atomically. Physical page demand reads
and SQL serving are specified by the later demand/snapshot sections rather
than by this publisher-local trace.

### Relational row-page mutation planning

Every relational table persists one monotonically increasing `next_page_id` in
its table root. Allocation is table-scoped. Deleted ids are never reused, even
after their physical pages become reclaimable, so a pinned old root cannot
observe ABA aliasing. Allocator exhaustion rejects the complete plan without
publishing a partial delta.

`RelationalRowPageMutationPlanner` accepts exact primary-key row changes and
produces one `RelationalRowPageTableDelta`. It binary-searches the ordered root
descriptors by encoded upper bound and reads only affected base leaves. Sorted
changes targeting the same leaf reuse the selected descriptor. A key below the
first leaf targets that leaf; a key between leaves or above the last leaf
targets the first following leaf or the last leaf respectively. The planner
does not enumerate a table's descriptors or decode unaffected pages. Changes
coalesce by encoded key and retain the existing transaction-capture envelope of
100,000 distinct entries and 64 MiB; replacing the same key replaces its charge
rather than growing the count or byte total.

Insert, update, and delete are deterministic over the encoded primary-key
order. The first non-empty output for an affected leaf retains that leaf's
logical `PageId`; additional right-side split pages consume new ids from the
persisted allocator. An empty output deletes the old leaf. A later split after
that deletion still consumes the next never-issued id rather than recycling
the deleted id. Dirty-page count and fixed-slot byte reservations are admitted
before the plan can be published.

Empty-table bootstrap consumes strictly ordered rows through a callback. It
retains at most one page plus the candidate row that proves the page boundary,
and emits only candidate pages. The caller MUST keep emitted pages unreachable
until `finish` succeeds and MUST discard the candidate after any callback or
encoding error. A bootstrap instance fails closed after its first error and
cannot be resumed.

The canonical checkpoint path activates this planner without making it a SQL
serving API. Before publishing a candidate it opens the exact row root selected
by the current outer checkpoint and compares generation, source epoch, and root
digest with the pinned read-view identity. A present but mismatched view fails
closed; it never silently falls back to a rebuild from a different base.

For an exact view, checkpoint capture traverses the immutable recovery-delta
runs and live DML batches to collect only distinct `(table, primary key)`
identities changed after the base epoch. Repeated keys coalesce before row
materialization. Final rows or tombstones are then resolved once from the
current canonical `RelationalState`, ordered by table and primary key, and fed
to `RelationalRowPageMutationPlanner`. The transient key set, encoded capture,
and conservative simultaneous resident peak share the existing 100,000-entry
and 64 MiB envelope. An admission, traversal-integrity, or base-identity error
rejects checkpoint preparation without publishing a partial candidate.

Only the first row root or an explicitly unavailable view, including a schema
replacement, uses the bounded streaming full-row bootstrap. Ordinary DML
rewrites only affected leaves. A graph-only checkpoint produces no relational
table deltas and copies the complete base root by descriptor, so it writes zero
new row-page slots. Clean descriptors retain their immutable physical
generation and slot even while the new logical root is bound to the new outer
checkpoint and overflow root.
`HawDBRowPageMutation.tla` covers persistent allocator
monotonicity, split identity, deletion without reuse, one-leaf point mutation,
pinned-base immutability, and bounded streaming bootstrap.

### Disk-backed relational row-root recovery

The recovery foundation serves SQL through a mandatory checkpoint-bound row
and overflow root pair plus its exact recovery/live overlays. Writable checkpoint
preparation persists both candidates first, verifies their exact generation and
source commit epoch, and publishes their identities atomically in
`HAWDB_MANIFEST_V1`. Open never consults either subsystem's independent latest
selector. It opens only the exact bound generations and rejects the database if
a selected generation manifest is missing, corrupt, or identity-mismatched.
Unbound future candidates are ignored and reclaimed by writable open.

Mounting reads each bounded row and overflow generation manifest exactly once.
For each manifest, the same bounded byte image validates the outer length,
CRC32C, and SHA-256 binding and supplies the internal decode. Mounting then
checks exact current-generation artifact file lengths, row-to-overflow binding,
and table schemas, but does not read row-page slots or hash database-scale
payload artifacts. Descriptor and page integrity checks remain demand-read or
scrub obligations.

An `OutOfCore` plus `Authoritative` handle builds a metadata-only
`RelationalState` directly from the validated row root. With an empty
post-checkpoint WAL it pins the canonical row/index views directly. With a
non-empty WAL, a read-only handle validates the complete source without
applying relational DML and requires matching pre-published row and index
recovery artifacts before it may adopt the recovered logical counts and visible
epoch. A writable handle replays schema-stable DML through bounded sparse
workspaces, publishes source-bound row and index recovery artifacts, and then
opens those exact views. Both paths retain complete schemas, exact
manifest-derived logical row counts, and overflow resolvers while reporting
zero materialized row count and bytes. SQL, transaction-private reads, later
schema-stable writable DML, and metadata-only checkpoints must then use the
pinned row pages and persistent indexes. Differential qualification, HawDB
Lightning export, schema-changing WAL, and snapshot WAL fail closed until a
complete canonical checkpoint can be published; they must not treat detached
rows as an empty database or silently fall back. Derived repair opens retain
materialized rows until their writable repair phase completes.

Relational transaction apply derives exact primary-key changes from the
authoritative before and after states. Multiple relational fragments in one
global commit epoch are consumed in order, and graph-only commits advance the
same epoch with an empty change. `GraphStore` sends every fragment directly to
`RelationalRowDeltaBuilder`. Its dirty map is capped at 100,000 entries and
64 MiB of charged resident state; pressure flushes a complete immutable run
rather than rejecting an otherwise admitted WAL suffix or retaining a
database-sized `BTreeMap`.

Recovery never skips a durable WAL record. A corrupt checkpoint-bound base root
rejects database open. After that base is pinned, an individual capture outside
its hard entry/byte envelope, an epoch gap, or row-delta run/manifest budget
exhaustion makes the serving WAL overlay unavailable without changing the
already recovered mutation state. SQL then fails closed; it does not select
materialized rows. A partial batch poisons the builder and its already durable
candidate runs remain unreachable.

A schema-changing relational record is distinct from an overlay admission
failure: it requires a new schema-bound canonical row root. During ordinary
operation HawDB stages that requirement before WAL, permits the canonical DDL
to become durable, and then synchronously performs a full-row schema checkpoint
barrier before returning success. The barrier writes row, overflow, and required
index candidates before publishing the outer checkpoint manifest last. If the
process stops after WAL durability but before that publication, writable open
replays the complete WAL first and retries the same barrier. A metadata-only
read-only open rejects schema-changing or snapshot WAL before materializing
checkpoint rows, never writes the repair candidate, and leaves the reader
explicitly unavailable.
Failure of the post-WAL barrier poisons the current handle and reports that the
durable commit will be retried by writable reopen.

After the complete WAL prefix is consumed, a writable open synchronizes all
runs, publishes the immutable delta-generation manifest, revalidates the
selected row root and previous delta selector, and atomically replaces the
latest delta manifest last. It then reopens that exact generation and exposes
one immutable `Arc` view containing the pinned base root, disk delta reader,
and recovered visible epoch. A read-only open never writes recovery artifacts;
it may reuse only already-published row and index deltas whose base identities,
visible epoch, WAL generation, LSN interval, and ordered-record digest exactly
match the replayed database. Both are required before authoritative serving,
and the row manifest supplies the metadata-only state's exact per-table counts.
`GraphStore::snapshot()` retains
that exact view only for the matching commit epoch.

Every later relational commit stages the next row view before appending WAL.
Relational DML derives one strictly ordered immutable primary-key batch from
the same before/after state used to stage the canonical mutation. The batch
recomputes and verifies its declared encoded byte charge, conservatively
charges the retained table/key/value allocation envelope, then moves row
payloads behind `Arc` ownership. Advancing a view installs one immutable
persistent-chain head and reuses the prior head; it never copies earlier batch
handles or clones the complete recovery overlay or canonical row set. An
already-durable graph-only commit stages only the identity advance immediately
before its global epoch becomes visible and does not add an empty batch. The
default cumulative live envelope is 100,000 entries and 64 MiB.

Only a successful durable commit installs the staged view. An epoch gap,
unordered or undercharged capture, or cumulative admission failure rejects an
ordinary live commit before WAL append; it cannot convert the canonical SQL
reader into a silently unavailable post-commit state. A DDL/schema rewrite
instead enters the mandatory schema checkpoint barrier described above. A live or recovery
row may retain a content-addressed overflow reference without copying its large
payload into the row overlay. Such a reference is unresolved storage evidence,
not a result value: a serving query MUST resolve it through the exact
`RelationalState` pinned at the same visible epoch under the statement hydration
budget before predicate, aggregate, sort, or projection evaluation. A missing
row, missing digest, type mismatch, or different value at that key is
corruption; admission or cancellation is non-poisoning. The status retains both the last
visible and failed commit epochs and whether a schema checkpoint is mandatory.
Already pinned snapshots retain their prior immutable view while a new root is
published. Metadata-only writable transactions read their bounded private row
view, while materialized-mode transactions retain their staged-state path; in
both cases read-your-own-writes never consults a lagging read view.

SQL pins this view once per statement. Primary-key probes, secondary-index row
fetches, full scans, join probes, blocking-operator locator replay, and aggregate
replay all obtain projected rows from that same view. Only a transaction-private
workspace and a database before its first checkpoint read canonical memory.
Selected unresolved overflow references are resolved through the exact pinned
`RelationalState`; checkpoint values come directly from row pages. The same
recovery and live identities drive bounded canonical checkpoint mutation
planning.
Checkpoint publication, backup, restore, scrub, orphan cleanup, and generation
reclamation select and preserve the exact bound base roots.
Reclamation computes the physical row-page and overflow-extent closure of the
current and immediately previous roots before deleting older metadata. The
disk-backed view is the sole unreleased v1 recovery design.

Backup copies the current row/overflow manifests and descriptor roots plus
every physical page or extent generation reachable from their descriptors; a
generation-local filename is not a complete backup boundary. Backup validation
and restore demand-verify every reachable row page and overflow envelope before
publishing the destination. Deep scrub applies the same descriptor closure, so
corruption in an older physical artifact reused by the current root fails
closed even after that artifact's obsolete root manifest has been reclaimed.

### Immutable relational row-delta v1 generation

`RelationalRowDeltaBuilder` writes one complete, non-serving delta generation
over a pinned row root. A generation binds the exact base generation, base
source commit epoch, base root-set digest, ordered table schema set, fully
consumed visible commit epoch, and optional overflow root. It consumes only
exact primary-key before/after change evidence. It does not scan base row pages
or infer mutations from SQL or WAL text.

The mutable dirty map is ordered by `(table ordinal, encoded primary key)` and
coalesces repeated keys to their latest row or tombstone. Both entry count and
charged resident bytes are hard limits. The charge includes the map/value
allocation envelope, fixed run descriptor, encoded key, and encoded row. When
the next change would exceed either dirty limit, the complete current map is
flushed as an immutable run before that change is inserted. A partial
batch error poisons the builder; already durable candidate runs remain
unreachable and the builder cannot publish a manifest. Changes at or before
the immutable base epoch and gaps in the global epoch sequence fail closed.

Each run is named
`relational-row-delta-{base_generation}-{delta_generation}-{ordinal}.run.hawdb`.
`SKRDLT01` version 1 uses one fixed 176-byte header, a contiguous array of
80-byte entry descriptors, and contiguous key/row payloads. The header binds
the base identity, delta generation, schema-set digest, run ordinal, epoch
range, entry count, and exact region lengths. Every entry descriptor stores
the table ordinal, present/tombstone kind, last-modified epoch, exact payload
offsets and lengths, CRC32C, and a SHA-256 binding over the base, generation,
run and entry ordinals, descriptor prefix, key, and row. Reserved bytes are
zero. Entries are strictly ordered and every region is gap-free. Run bytes are
written and synchronized without building a second run-sized encoding buffer;
the configured cumulative run-byte and run-count limits are checked before a
new candidate file is created.

One generation has both an immutable manifest
`relational-row-delta-{base_generation}-{delta_generation}.manifest.hawdb`
and the publish-last selector `relational-row-delta.manifest.hawdb`.
`SKRDMF01` version 1 has a fixed 304-byte header followed by 48-byte table
descriptors plus names and 100-byte run descriptors plus lower/upper keys. It
contains the same 56-byte recovery-source identity used by index recovery and
rejects an empty or epoch-length-mismatched LSN range. It binds each table's
schema identity and exact final row count from the same
`RelationalState` that produced the recovery changes, plus exact table and
run-set digests, optional overflow-root identity, run artifact length and
digest, total entries, and the complete epoch fence. A schema or table-set
change requires a new checkpoint rather than publishing counts against a
different catalog. Table count, run count, manifest bytes, dirty bytes, row/key
sizes, and cumulative run bytes are independently bounded. Normal open
validates this bounded manifest and exact run file lengths only; descriptor,
payload, and full-run integrity are demand-checked while visiting the selected
runs. Callback effects remain provisional until a complete visit returns
success. An explicit early stop verifies each emitted entry binding but does
not read or hash the unselected suffix.

Pooled range cursors are lazy: constructing the admitted source vector MUST
NOT open run files. Priming a source reads its header, descriptors, and first
entry through one file lease. The shared LRU pool reports peak retained files,
successful opens, hits, and misses through the range-read report. A successful
snapshot range report MUST include the final pool counters rather than the
counters observed only while source cursors were being constructed.

`relational_row_delta_runs` is the production-shaped evidence protocol for
run fan-in. Release mode measures 32, 256, and 4096 immutable runs with file
pool capacities 8, 32, and 64. Every run updates one shared hot key and adds one
new key, forcing the ordered merge to revisit interleaved sources. Setup is
outside the timed read. The benchmark reports reader-open, first-row, and full
scan p50 latency together with file-pool and overlay-residency counters. On the
2026-08-18 qualification host, capacity 32 reduced the 32-run scan from 2.19 ms
to 1.66 ms and halved opens from 64 to 32. Capacity 64 did not reduce opens;
at 256 runs its 7.26 ms scan matched capacity 32's 7.24 ms, and at 4096 runs it
was slower (125.86 ms versus 116.10 ms). Therefore the cross-platform default
remains 32 files. Increasing the pool beyond 32 is not an accepted substitute
for folding recovery into a canonical checkpoint.

The preferred checkpoint-fold threshold is 256 recovery runs; the 4096-run
limit remains a hard recovery admission bound so delayed maintenance does not
make an otherwise valid WAL prefix unopenable. The fold publishes a new
canonical row root and recovery fence. HawDB MUST NOT compact an arbitrary
subset of recovery runs into a replacement run because that would require a
new manifest-last binding protocol and duplicate-version proof. Operators can
observe both the threshold and whether it has been crossed through the
relational row residency report.

Publication is manifest-last:

1. synchronize every immutable run;
2. validate every overflow reference against the exact visible-epoch root when
   one is published; otherwise retain the reference for the mandatory pinned
   state resolver;
3. synchronize and publish the immutable generation manifest;
4. acquire the row-root publication lock and re-read the latest row root;
5. acquire the delta publication lock and revalidate the expected previous
   delta generation;
6. atomically replace `relational-row-delta.manifest.hawdb` last.

The lock order is row root before row delta. A concurrent row-root publisher or
delta publisher therefore makes the candidate stale rather than allowing a
manifest bound to the wrong base or previous generation to become selected.
Crashes before step 6 retain the prior latest manifest and may leave only
unreachable immutable candidates. A reader pins the immutable generation
manifest and remains readable after a newer generation publishes.

This is the only relational row-delta representation. HawDB has not published
a durable database format, so the reader recognizes no legacy magic, version,
layout, filename, or migration path. WAL recovery and immutable live views now
pin `base + delta + live` through this representation. Exact base checkpoint
binding is active; SQL row selection and delta-generation lifecycle
reclamation remain separate activation contracts.

### Graph layout

1. Nodes and relationships MUST use stable ids and independently addressable
   row pages.
2. Graph record pages MAY cluster small declared properties with an entity
   when benchmark evidence shows a point-read benefit.
3. Dense or unbounded adjacency MUST use separate pages ordered by
   `(node id, relationship type, direction, neighbor id, relationship id)`.
4. Sparse adjacency MAY be inlined only behind an evidence-derived byte and
   degree threshold.
5. Relationship create/delete MUST preserve endpoint and adjacency-index
   agreement in the same commit epoch.
6. A live typed adjacency overlay MUST retain `(neighbor_id, relationship_id)`
   order in its posting state. Its read cursor MUST merge immutable and live
   entries without constructing a degree-sized query buffer, and MUST stop
   before reading the next entry after downstream cancellation or `LIMIT`.
7. Ordering across unspecified relationship types MAY use a blocking merge only
   when its complete compact-key state is admitted by the query root ledger.

### Stable identity export mapping

The physical-id to logical stable-identity mapping used by HawDB Lightning is
not the graph `id` property index. Declared `id` properties use the ordinary
generation-bound property projection. `stable_ids.hawdb` exists only for
records that need a durable export/import identity because their canonical
property is missing or non-unique. A persisted overlay replaces the ambiguous
property value for that physical export; mappings that are no longer required
are removed after the next explicit export publication and the final reader
pin on their immutable generation has been released.

1. The mapping MUST use its own monotonic generation because initial import
   publishes it before appending the graph WAL batch. It MUST NOT be falsely
   bound to a checkpoint generation that does not yet contain the imported
   graph.
2. Publication MUST write and synchronize every fixed-size mapping page into
   an immutable generation-named artifact before atomically replacing the
   small checksummed selector. A crash before selector replacement leaves the
   prior complete generation selected; a complete but unselected artifact is
   ignored and may be reclaimed or replaced by a retry.
3. The generation header records generation, covered graph commit epoch, page
   size, page count, and node/relationship entry counts. The selector embeds
   that complete header plus the exact artifact length under CRC32C and
   SHA-256. Initial-import WAL append is permitted only after a complete
   selected mapping declares coverage for the target graph epoch.
4. Normal open reads only the bounded selector and selected generation header,
   validates their exact agreement and the exact artifact length, closes the
   selector handle, and pins the generation artifact. It MUST NOT decode every
   stable identity or warm mapping pages.
5. Pages are strictly ordered by `(entity kind, physical id)`, independently
   checksummed, fixed-size, and demand-read through the shared segment cache.
   Point lookup uses bounded page and storage-byte admission. A selected corrupt
   page poisons the mapping reader and fails the identity operation closed. The
   writer admits the cumulative artifact size before each page write.
6. Full materialization is allowed only at an explicit export/import boundary
   with entry, storage-byte, and estimated resident-byte limits. It is not
   mandatory database residency.
7. Full storage scrub MUST validate every page checksum, global key order,
   entry count, and value encoding while retaining at most one page and one
   decoded value. It MUST NOT warm the shared page cache. A physical failure
   poisons the storage handle.
8. Mapping generation is independent from query indexes. Graph property,
   relationship-property, composite, full-text, and adjacency projections keep
   their canonical generation fences and cannot use this sidecar as a query
   fallback.
9. Publication MUST NOT replace an opened generation artifact. A reader keeps
   its exact immutable generation across later selector publication on every
   supported platform. Reclamation may remove only an unselected generation
   with no in-process reader pin. Backup MUST copy the selector and exactly the
   generation artifact bound by its pinned reader; a selector without that
   artifact, an unreferenced generation, or a cross-generation header mismatch
   fails validation closed.

### Large values

Strings, JSON, binary values, and vectors above the inline threshold MUST use
separate immutable extents. The row stores `(type, length, digest)` and the
generation-bound overflow root resolves the physical location. A
query that does not project the value MUST NOT read, decode, or clone its
payload. A single value remains subject to an explicit maximum size even when
it is file-backed.

## Persistent index pages

### Index classes

The persistent page contract applies to:

- relational primary-key trees;
- unique and secondary indexes;
- graph declared property indexes, including an indexed `id` property;
- forward and reverse adjacency indexes.

BM25, vector ANN, optional columnar scan structures, and algorithm outputs are
derived projections and follow the projection contract below.

### Page structure

1. Every index has one compact root descriptor selected by the manifest.
2. Root, interior, leaf, and posting pages MUST be independently addressable
   and checksummed.
3. Interior entries contain separator keys and child `PageId` values. Leaf
   entries contain index keys and bounded inline postings or posting-page
   references.
4. Oversized posting lists MUST be split into independently addressable pages.
5. Page encoders MUST enforce the same size and count limits as decoders.
6. Unknown optional fields MAY be skipped. Unknown required page kinds or
   versions MUST fail closed.
7. Page descriptors MUST be sufficient to reject impossible offsets, lengths,
   key bounds, generations, and page kinds before allocating the page body.

### Generation agreement

1. A row root, all required index roots, catalog identity, and schema epoch
   become visible through one manifest generation.
2. An index root MUST NOT lead or lag the canonical row epoch it claims to
   index.
3. A unique index used for mutation validation MUST be ready at the same epoch
   as the rows. Missing, stale, or corrupt uniqueness state makes the database
   unusable for writes.
4. A non-constraint query index MAY enter an explicit rebuild state. The
   planner MUST either use a correct canonical fallback or fail the query; it
   MUST NOT silently use stale postings.
5. Rebuild publication is compare-and-publish against its source generation.
   A stale builder cannot replace a newer root.
6. A non-authoritative checkpoint MAY omit its relational-index binding after
   candidate failure. A present binding MUST name the same generation and
   source commit epoch, contain the exact catalog/root-set digests, and include
   complete page and generation-manifest length, CRC32C, and SHA-256 metadata.
7. An authoritative checkpoint MUST include the complete binding. A candidate
   failure MUST abort before canonical manifest publication.
8. An authoritative relational mutation MUST validate its primary, unique,
   UPSERT, and foreign-key decisions through one current pinned index view and
   stage the next live view before appending WAL.
9. A rejected authoritative mutation MUST NOT advance the WAL LSN, canonical
   row epoch, or index visible epoch. A successful mutation publishes row and
   index visibility only after its WAL batch is durable.

## Database open and recovery

### Startup contract

Normal open proceeds in this order:

1. acquire the exclusive embedded-directory lease;
2. read and verify the superblock and active manifest;
3. open the catalog, schema, and compact row/index root descriptors;
4. replay every WAL batch required by strict recovery;
5. publish the usable in-process root handle.

WAL replay may make total startup slow. HawDB MUST NOT skip a valid batch,
truncate a durable prefix, or change correctness merely to meet a startup
latency target.

Index startup has a stronger bound:

- normal open MUST NOT scan row pages;
- normal open MUST NOT enumerate, decode, or warm all index pages;
- normal open MUST NOT rebuild all postings;
- mandatory index residency MUST be bounded by catalog, root-descriptor, and
  dirty-recovery state, not index entry or leaf-page count.

Production qualification makes that boundary observable. Before the first
user query on each fresh open, the shared segment payload cache MUST report raw
residency, pin, hit, miss, eviction, admission-rejection, and digest-mismatch
counters. Mandatory system-schema validation MAY issue bounded canonical row
reads, but the plan MUST cap their total cache requests and resident bytes.
Exceeding either ceiling, or observing a pin, eviction, rejection, or digest
mismatch, blocks release evidence. Manifest and WAL work remains visible in
the separate open-phase timing report.

`Materialized`, `Shadow`, and `DemandPaged` checkpoint decode retain
`rebuild_indexes()` as the transitional differential oracle. `Authoritative`
checkpoint decode omits those posting maps, requires the bound persistent view
before serving, and derives WAL-recovery plus live changes directly from
before/after rows. Constraints are checked against that view before WAL; replay
of an already-durable authoritative transaction does not revalidate it through
the absent posting oracle.

Every schema-stable DML record that has canonical row capture MUST also carry a
strictly ordered, duplicate-free replay access set containing every primary key
needed to reproduce the complete transaction. This is a predicate-read and
mutation working set, not a net-change list: every row evaluated by an
`UPDATE` or `DELETE` predicate remains present even when it does not match, as
does a row inserted and deleted in one transaction, a primary key changed and
later restored, or an existing conflict row read by `UPSERT ... DO NOTHING`.
Capture is incremental, uses the canonical row overlay entry and byte limits,
and rejects before WAL append when either limit is exceeded; a broad predicate
scan therefore cannot create an unbounded recovery contract merely because it
changes few or no rows. The WAL envelope authenticates the set with the logical
transaction. The encoder MUST enforce the WAL decoder's entry, value, and
record-byte limits before append, so custom capture limits cannot create a
durable record that reopen rejects. Writable metadata-only recovery hydrates
exactly this authenticated set from the pinned checkpoint and previously staged
recovery deltas into a bounded sparse workspace before replaying logical
`UPDATE`, `DELETE`, and `UPSERT`. It recomputes the access set and rejects any
drift as corruption; it MUST NOT infer a smaller set, construct the complete
materialized state, or fall back to a database-sized row scan. Schema-changing
records remain checkpoint barriers and do not use this contract.

An ordinary materialized mutation against an omitted state fails closed instead
of silently bypassing constraints. Generation publication derives entries from
canonical rows and never consumes the oracle. When postings are omitted, SQL
planning uses a conservative row-count estimate (or one row for a complete
unique key) and leaves the actual bounded cardinality discovery to the demand
reader; planning MUST NOT rebuild or scan the persistent index merely to obtain
an estimate.

### WAL index recovery

1. WAL replay applies changes after the published index epoch to a bounded
   dirty overlay.
2. When the overlay reaches its byte budget, recovery MAY flush immutable
   index delta pages and continue replay. It MUST NOT load the complete base
   index to merge the delta during open.
3. Point lookup and range iteration merge the base root, ordered recovery delta
   pages, and the remaining dirty overlay under one visibility rule.
4. Delta ordering and duplicate suppression MUST produce the same result as a
   complete rebuild at the recovered commit epoch.
5. A crash during recovery-delta flush leaves either the previous selected
   root plus replayable WAL or a completely published newer recovery root.

The current persistent implementation satisfies these rules for DML whose
checkpoint schema fence remains unchanged. DDL and relational snapshot WAL
records deliberately make a non-authoritative view unavailable. They do not
weaken canonical recovery and MUST NOT trigger an implicit full index rebuild
in this path. `DemandPaged` SQL may fall back observably to the materialized
oracle. `Authoritative` rejects schema-changing mutations before WAL and never
falls back from a missing recovery/live view.

### Integrity boundary

Normal open validates the manifest, root descriptors, file lengths, and the
pages it reads. Cold leaf and posting checksums are verified on first access.
Consequently, a fast normal open is not a full-media scrub. A checksum failure
during query execution poisons the handle and fails closed. Explicit
doctor/deep-scrub mode visits every reachable page and projection artifact.

Graph manifest open is separately admitted. The canonical segment, property
spill, adjacency, and property-projection manifests share one aggregate encoded
byte budget for an open or manifest installation. Each file MUST be rejected
from its durable binding and filesystem metadata before allocation when either
the aggregate configured budget, its format limit, or its exact selected length
would be exceeded. Length, CRC32C, and SHA-256 verification MUST consume the
same bounded byte image that is decoded; a verifier MUST NOT reopen the path.
The configured limit is a storage-subsystem budget and MUST NOT be derived by
assuming that 512 MiB is the default process limit.

This bound remains a fail-closed transition guard for selected graph manifests.
Canonical adjacency descriptors are no longer part of that graph-size-dependent
manifest image: the outer durable manifest binds one compact descriptor root,
and normal open admits and verifies only that root while page payloads remain
cold. Property-projection block descriptors follow the same rule: the selected
compact property manifest contains bounded definitions plus the exact
descriptor-root identity, never the per-block descriptor vector. Canonical
segment descriptors use the same page format. The compact manifest binds their
root and aggregate per-kind counts without retaining a descriptor vector; point
and scan execution use the selected demand reader. Other manifest-backed
graph structures MUST still report both the configured aggregate limit and
selected encoded manifest bytes, and MUST reject a selected generation that
exceeds the limit.

### Graph descriptor page v1

Graph descriptor roots use a separate immutable page format because canonical
segment descriptors can contain three bounded Bloom filters and do not fit the
relational posting-list page contract. The format is introduced independently
of publication and serving activation:

- every page binds its graph artifact class, non-zero physical generation,
  source commit epoch, non-zero page id, entry count, payload length, CRC32C,
  and SHA-256;
- a parent reference carries the artifact id, physical generation, page id,
  exact file range, both integrity digests, and inclusive lower/upper key
  bounds;
- leaf keys and descriptor values are non-empty, keys are strictly increasing,
  and values are explicitly byte-bounded; interior child ranges are strictly
  ordered and disjoint, and never reference a future physical generation;
- a bound decode validates the outer range, digest, artifact class, physical
  identity, source epoch, and key range against the same admitted page bytes;
- physical generation belongs to the page reference rather than the selecting
  root. The format can therefore support later COW reuse, but canonical
  adjacency v1 requires every reachable page to belong to the selected
  generation. Cross-generation graph references remain rejected until a
  retained-closure and reclamation contract is activated;
- the default page ceiling is 512 KiB and the default descriptor-value ceiling
  is 448 KiB. These are per-page format admissions, not resident-memory policy
  or evidence that 512 MiB is HawDB's default process budget.

`ImmutableGraphDescriptorPage` now serves canonical adjacency through a compact,
generation-bound root selected by the outer durable manifest. Descriptor pages
remain cold until a prefix scan or explicit deep scrub reads them. Page payloads
are admitted independently of the graph-manifest open budget and use the shared
byte-bounded storage cache.

### Canonical segment demand descriptor root v1

Every canonical checkpoint publishes and activates a descriptor tree beside the
compact canonical segment manifest:

- the leaf key is `(kind tag, big-endian maximum record id, big-endian segment
  id)`. Node keys sort before relationship keys, ranges within one kind follow
  record order, and point readers lower-bound seek by `(kind, requested id)`
  without scanning preceding descriptors;
- the value carries an eight-byte magic, version, zero flags, segment id, kind,
  exact artifact range, CRC32C, inclusive record bounds, non-zero record count,
  and three length-delimited binary Bloom filters. Every Bloom filter carries a
  non-zero hash count and bounded word count. Encode and decode require the key
  and value identities to agree, and the default 448 KiB descriptor admission
  accommodates the three independently bounded Bloom filters;
- segment flush streams each descriptor into `GraphDescriptorTreeBuilder` and
  retains only aggregate node/relationship record and segment counts. Manifest
  residency is therefore independent of segment count;
- the canonical data artifact is synchronized and published before descriptor
  pages and the bounded root. The canonical manifest then binds the exact source
  commit epoch, segment count, and root length, CRC32C, and SHA-256; the outer
  checkpoint manifest selects that canonical manifest last;
- normal open verifies the manifest-to-root identity, descriptor count, root
  integrity, and page-artifact length without reading any descriptor page
  payload. Point lookup performs one bounded lower-bound seek and reads at most
  one selected canonical segment;
- scans advance through fixed-size descriptor batches. Each batch independently
  admits pages, page bytes, descriptor count, and tree height, then releases its
  traversal state before the next lower bound. Node/relationship iterators retain
  at most one decoded canonical segment and never materialize all descriptors;
- read reports separate descriptor page/cache/I/O work from canonical segment
  cache/I/O work. Bloom pruning remains descriptor-local. A physical descriptor
  or canonical data failure sticky-poisons the reader, while segment-size or
  descriptor-resource admission remains retryable and non-poisoning;
- explicit deep scrub hashes both complete artifacts without warming the shared
  cache, decodes every descriptor and record framing, validates property value
  shape, and proves contiguous physical ranges, strictly increasing segment ids,
  grouped kinds, disjoint per-kind record ranges, and exact aggregate counts.
  A spill-reference value is valid only when a property-spill reader with the
  same generation and source epoch is selected and the referenced id is below
  that spill manifest's exact value count;
- backup, restore, storage scrub, checkpoint discard, abandoned-candidate
  cleanup, and generation reclamation treat canonical data, compact manifest,
  descriptor pages, root, and every selected property-spill reference as one
  physical closure. Backup and storage scrub MUST open and exhaustively verify
  the selected spill artifact before accepting canonical record framing;

Publication refines the candidate-data, candidate-page, and publish-root
transitions of `HawDBGraphDescriptorPaging.tla`; installing the compact manifest
refines the separate activation transition. `RejectDemandAdmission` models the
non-poisoning resource boundary, while physical corruption follows the modeled
fail-closed poison transition.

### Property-spill demand descriptor root v1

Every canonical checkpoint persists and activates a same-generation descriptor
tree for the large-property spill artifact:

- the leaf key is the big-endian maximum spill id. Blocks contain contiguous
  spill ids, so byte order is the writer's logical order and a point reader
  seeks the first upper bound greater than or equal to the requested id;
- the fixed 68-byte value carries an eight-byte magic, version, zero flags,
  block id, exact artifact offset and non-zero length, block CRC32C, inclusive
  spill-id bounds, non-zero value count, and a zero reserved tail. Encode and
  decode require the key to agree with the value and require the inclusive id
  range to contain exactly the declared count;
- the incremental spill writer streams each completed block descriptor into
  `GraphDescriptorTreeBuilder` and retains only aggregate counts. The compact
  property manifest has constant-size metadata and no per-block vector;
- the spill data artifact is synchronized and atomically replaced before the
  immutable descriptor page artifact and root are published. The property
  manifest binds source commit epoch and exact descriptor-root length, CRC32C,
  and SHA-256. The outer checkpoint manifest selects that manifest last;
- normal open verifies the bound root, source epoch, descriptor count, and page
  artifact length without reading descriptor page payloads. A lookup below the
  declared value count performs one lower-bound descriptor seek under
  independent page, page-byte, descriptor-count, and tree-height limits, then
  reads only the selected data block;
- descriptor pages and spill data blocks use distinct identities in the shared
  byte-bounded cache. The read report separates descriptor page storage,
  decode, cache, selected block bytes, and data-cache outcomes. A block-size
  admission failure remains request-local;
- physical descriptor, identity, range, digest, data, or I/O failure poisons
  the reader. Explicit deep scrub bypasses both caches, hashes the complete
  data and page artifacts, visits every descriptor, decodes every block, and
  requires contiguous block ids, byte ranges, spill-id ranges, block count,
  value count, and final artifact length;
- backup, restore, storage scrub, checkpoint discard, abandoned-candidate
  cleanup, and generation reclamation retain or verify data, manifest,
  descriptor pages, and root as one closure. Backup and storage scrub run the
  exhaustive descriptor/data closure check rather than accepting only
  top-level artifact hashes. They also bind this exact reader into canonical
  deep scrub, so a missing, different-generation, or out-of-range spill
  reference fails before a backup or scrub is accepted.

The publication and demand-read paths refine `HawDBGraphDescriptorPaging.tla`:
a descriptor root can be published only after its same-generation data artifact
is durable, open pins the selected generation without warming descriptor pages,
and a physical demand-read failure makes later reads fail closed.

### Property-projection demand descriptor root v1

Every property-projection checkpoint writes the production descriptor
representation for equality, range, full-text, composite equality,
relationship equality, and relationship range blocks:

- the leaf key is `(kind order, label or relationship-type id, escaped UTF-8
  property identity, block id)`. Numeric fields are big-endian. A zero byte in
  the property is escaped and the property terminator sorts before every
  continuation, so byte order matches the writer's logical definition order;
- the versioned value carries the complete block identity, kind, label, entry
  count, exact data-artifact range and CRC32C, property identity, and encoded
  minimum and maximum keys. Decode rejects non-zero reserved fields, malformed
  lengths, zero ranges/counts, invalid composite arity, and every key/value
  identity disagreement;
- the spill merge writes each completed block to the existing immutable data
  artifact and simultaneously streams its descriptor into the bounded generic
  tree builder. The data artifact is synchronized and atomically replaced
  before the immutable page artifact and root are published. Existing
  same-generation page or root destinations are never overwritten;
- the descriptor root binds `PropertyProjection`, generation, source commit epoch,
  exact page-artifact integrity, descriptor count, page counts, height, and the
  root-page reference. Backup validation, derived-repair quarantine,
  checkpoint discard, orphan cleanup, and generation reclamation treat its
  pages and root as the same generation as the selected projection artifact;
- the compact selected property manifest binds the data artifact and exact
  descriptor-root length, CRC32C, and SHA-256. The outer durable manifest binds
  that compact manifest. Normal open verifies this chain and the descriptor
  page-artifact length without reading page payloads;
- production lookup and candidate estimation scan only the ordered descriptor
  prefix under independent page, page-byte, descriptor-count, and tree-height
  limits. Selected block ranges and CRC32C identities are verified before
  decode. Estimates return their descriptor-read report and propagate physical
  failure instead of silently choosing a plan from corrupt metadata;
- descriptor and selected data pages share the byte-bounded cache but use
  distinct representation identities. Warm-cache hits do not waive logical
  traversal admissions. Read reports separate descriptor storage/decode/cache
  costs from selected block storage/decode/cache costs. Descriptor estimation
  remains observable when it selects the cheaper adjacency fallback, so query
  accounting cannot hide I/O performed before the final access-path choice;
- physical descriptor, range, digest, or data corruption poisons the shared
  reader. Admission and cache-capacity rejection remain request-local;
- explicit deep scrub bypasses descriptor and data caches, hashes both complete
  artifacts, visits every reachable descriptor, decodes every block, and
  requires contiguous block ids and byte ranges with exact descriptor, block,
  entry, and artifact-byte counts. Backup validation and derived-artifact
  health use this full closure.

`HawDBGraphDescriptorPaging.tla` is instantiated once per descriptor class.
For property projection, the Rust refinement now covers candidate creation,
durable page completion, publish-last root publication, atomic outer-manifest
selection, pinned demand readers, bounded page residency, corruption poison,
and crash cleanup. The same activation transition used by canonical adjacency
is therefore enabled for this descriptor class.

### Canonical adjacency demand descriptor root v1

Every canonical adjacency checkpoint persists one complete adjacency artifact
and descriptor tree, then selects their exact identity through the outer durable
manifest:

- the leaf key is the big-endian tuple `(direction, endpoint, relationship
  type, minimum neighbor, block id)`, so byte order is descriptor order;
- the fixed-width value carries a version, block identity, direction, layout,
  endpoint/type, record count, neighbor bounds, exact adjacency artifact range,
  and block CRC32C. Decode rejects reserved fields, zero lengths/counts, range
  overflow, and any key/value disagreement;
- construction retains one leaf or interior group. Completed page references
  are written to a bounded intermediate run. Each later level streams the
  previous run into interior pages and another run, so descriptor count does
  not determine builder RSS. Interior fanout must be at least two, ensuring
  that every non-root level strictly reduces the reference count;
- page count, page-artifact bytes, and cumulative intermediate bytes are hard
  admissions. Checked counters reject overflow rather than saturating. The
  build report records descriptor, leaf/interior page, artifact, root, spill,
  and conservative peak-resident bytes;
- the adjacency artifact is synchronized and atomically replaced first. The
  descriptor page artifact was already synchronized, is published second, and
  the bounded checksummed root is synchronized and published last. Descriptor
  artifacts are immutable within one generation and publication refuses to
  replace an existing destination. A root publication failure therefore
  leaves no newly selectable root;
- the root binds graph class, selecting generation, source commit epoch, page
  artifact identity/length/CRC32C/SHA-256, counts, tree height, and the exact
  immutable root-page reference. Empty adjacency publishes an empty artifact
  with the integrity digest of the empty byte string;
- the outer durable manifest binds the selected generation and source commit
  epoch, exact relationship and two-direction entry counts, the adjacency
  artifact length/CRC32C/SHA-256, and the descriptor-root
  length/CRC32C/SHA-256. Manifest-last publication is the only activation
  transition. Missing fields, generation drift, source-epoch drift, count
  disagreement, or root/data identity drift fail open closed;
- root-only reopen reads at most `max_root_bytes`, validates its checksum and
  structure, and checks the page artifact length without reading page payload.
  Demand prefix scans independently admit page count, storage bytes, emitted
  descriptors, and tree height. Each page is verified against its exact parent
  reference before decode and cached by store, selected generation, page
  identity, content digest, and representation kind;
- endpoint and optional relationship-type scans stop after the ordered prefix
  ends, then read only the referenced adjacency blocks. Reports expose selected
  generation, descriptor pages/bytes/cache outcomes, block reads, decoded
  records, and early stop. Capacity rejection is local to the request;
- physical page, block, digest, key-range, or generation corruption poisons the
  shared reader handle. Admission and cache-capacity rejection do not poison it;
- explicit deep scrub bypasses the cache, hashes the complete descriptor-page
  and adjacency artifacts, visits every reachable page and descriptor, decodes
  every block, and requires a contiguous block-id/range closure with exact page,
  descriptor, block, record, and byte counts;
- backup, restore validation, derived-artifact repair, checkpoint discard,
  abandoned-candidate cleanup, and generation reclamation retain or verify the
  adjacency artifact, descriptor page artifact, and descriptor root as one
  selected closure.

The production path does not write or reopen a text adjacency manifest and does
not retain a graph-sized descriptor vector. A resident manifest remains only as
a codec-test oracle; it is not a durable compatibility path.

## Demand paging and cache ownership

1. HawDB manages page-in/page-out through its own byte-bounded cache. OS swap
   is neither an accounting mechanism nor a correctness dependency.
2. The default cross-platform path uses bounded positional file reads. `mmap`,
   `io_uring`, and platform-specific direct I/O are optional evidence-gated
   optimizations.
3. A cold point lookup reads only admitted root-to-leaf paths and required
   posting pages. A range query reads an admitted leaf window.
4. Cache identity includes store, manifest generation, page identity, digest,
   and representation kind.
5. A clean unpinned page may be evicted. A pinned page is not evictable. A
   dirty page is not evictable until its WAL and page-publication obligations
   are satisfied.
6. Pin lifetime is bounded to one cursor window or pipeline wave. Query
   cancellation, timeout, error, and panic MUST release every pin.
7. Prefetch is optional, bounded by request count, bytes, pins, deadline, and
   cancellation. Database open MUST NOT launch an unbounded warmup.
8. Oversized pages are rejected before insertion. The cache MUST NOT exceed its
   capacity in order to admit one exceptional entry.

### Relational row-page demand reads

`RelationalRowPageDemandReader` pins one exact row-root and overflow-root pair;
it never follows an independent latest selector. Point lookup binary-searches
the bounded root descriptors, reads one selected physical slot, binary-searches
its row directory, and decodes only the requested field ordinals. Ordered range
lookup starts at the first matching descriptor and streams rows through a
callback. It applies the lower bound only to the first page, stops before a page
whose lower bound is beyond the upper bound, and does not collect rows or drain
later pages after callback stop.

Each operation independently admits descriptor-search height, row-page count,
fixed slot bytes, decoded rows, one-page pin residency, cancellation, and the
existing compressed/decompressed overflow hydration ledger. A callback result
is provisional until the range method returns `Ok`. Page, byte, tree-height,
row, overflow-hydration, and cache-residency admission failures do not poison
the reader. Descriptor, page, checksum, immutable cache identity, generation,
and overflow corruption or durability failure poison it; concurrent operations
observe that poison at their next cancellation checkpoint and emit no later
row.

The shared cache key uses store id, the descriptor's physical generation and
slot, CRC32C content identity, and `RelationalRowPageSlot` representation. The
physical rather than logical root generation is required because a clean COW
descriptor may retain a page from an older immutable artifact. A cold read
validates the complete fixed slot CRC32C and SHA-256, zero padding, the encoded
page integrity, directory, page id, physical generation, source epoch, row
count, encoded length, and key bounds exactly once. Cache admission retains only
the compact `encoded_len` bytes and binds them to the complete physical-slot
CRC32C and SHA-256. A verified hit checks that strong source tag and the cheap
page identity fields; it MUST NOT hash or validate the complete slot again.
Cache resident and pinned bytes are the compact allocation, not the padded
physical slot length. An entry-too-large or temporarily all-pinned cache falls
back to the same one-operation compact page buffer; it never expands cache
capacity. At most one compact page `Arc` is retained by a cursor wave, and
cancellation, error, callback stop, or unwind drops it.

Projected decode validates the selected row's complete slot layout but decodes
inline variable-width values as page-backed `RelationalValueRef` values. The
row-page cursor is a lending GAT cursor: the row it returns borrows the cursor's
reused primary-key and projected-field scratch and cannot survive the next
mutable cursor step. Text and binary values borrow the verified page slot;
fixed-width values are copied. Ordered-key decode reuses the existing key
vector and same-typed variable-width value capacity. The callback boundary is
higher-ranked and therefore cannot retain a row view after the callback.

The lending boundary MUST reach residual predicate evaluation, projection, and
`OFFSET`/`LIMIT` before selected output is materialized. Converting every row
to an owned row immediately after borrowed decode is a compatibility path, not
an optimized path. A checkpoint row remains borrowed when all required fields
are inline. A row that requires overflow hydration is materialized exactly
once before hydration. Recovery and live overlay rows remain owned because
their values already cross page and generation boundaries. The common row
view reports which representation reached the callback; an owned compatibility
wrapper MUST report every emitted row as owned even if its implementation uses
the lending decoder internally.

An overflow descriptor is resolved only if its field was selected, so
unselected text, JSON, binary, or vector payloads are not read, decompressed,
or cloned. All selected overflow values in one row stage against a private copy
of the hydration ledger; a later field failure publishes neither partial row
values nor partial budget charges. Reports separate descriptor reads, logical
page and slot bytes, physical file reads, cache hits/misses/rejections, decoded
and emitted rows, borrowed and owned callback rows, hydration bytes, peak pins,
and early stop.

`RelationalRowPageDemandReader` remains the immutable checkpoint primitive. It
MUST NOT be selected by SQL at an epoch newer than that checkpoint. Serving a
base-only reader at a later visible epoch would be stale and is forbidden.

### Relational row snapshot composition

`RelationalRowPageSnapshotReader` pins one exact
`RelationalRowPageReadView` and composes three storage-owned sources in the
following precedence order:

1. the newest immutable live batch at or below the pinned visible epoch;
2. the newest disk-backed recovery-delta version after the pinned checkpoint;
3. the immutable checkpoint row page.

The reader validates the read-view identity against the pinned row root before
serving. Recovery runs MUST bind the same base generation, source commit epoch,
root-set digest, table schema digest, and column count. The checkpoint overflow
root and an optional recovery overflow root are separately generation-bound.
When the recovery root exists, every selected overlay reference is resolved and
validated through it. When it does not exist, the snapshot reader returns the
content-addressed reference only to the internal SQL row runtime; that runtime
MUST resolve it through the exact `RelationalState` pinned at the same visible
epoch before the value participates in query semantics. It MUST NOT consult a
newer state, the checkpoint overflow root, or a latest-generation selector.

Point lookup retains at most one selected overlay row and otherwise delegates
to one checkpoint point read. A tombstone suppresses the checkpoint without
opening its row page. Range lookup treats every intersecting recovery run and
non-empty live batch as one strictly ordered source. Source count is admitted
before run files are opened. Recovery sources share a configurable LRU handle
pool, so retained run descriptors are bounded independently of the number of
intersecting runs; eviction never changes positioned-read state or integrity
hashing. Each source contributes at most one projected head
to a min-heap, and advancing a source replaces that head instead of retaining
the rest of the run or batch. Equal keys are coalesced at the heap frontier; a
higher commit epoch replaces a lower one and an equal-epoch duplicate is
corruption. The selected head then merges directly with the checkpoint page
cursor. A tombstone consumes the matching checkpoint key, and an insertion
after the final checkpoint page remains visible.

The merge validates the complete source row shape but owns only requested
fields in a head. A recovery cursor reads and hashes the exact encoded key and
complete encoded row, verifies the entry binding, walks the complete value
directory, checks every value's bounds and encoding, and constructs owned
`RelationalValue` objects only for requested ordinals. It MUST NOT first build a
full `RelationalRow` and then project it. Live values are already owned by their
immutable batch and are projected before heap insertion. An unselected inline
TEXT, BYTEA, JSON, or vector payload therefore has one bounded encoded-row read
but no second owned value allocation. The resident envelope is one projected
head per source, one selected/working head, and one pinned checkpoint page; it
is independent of the total number of overlay rows in the requested range. A
callback may observe the first ordered row after source heads are primed,
without consuming the complete overlay. Recovery entry bindings remain
demand-verified, and a fully consumed run additionally verifies its complete
content and artifact digests.

Overlay streaming has explicit distinct-entry, merge-source, and peak
resident-byte limits in addition to
the shared descriptor-height, page, slot-byte, decoded-row, pin, hydration,
cancellation, and deadline limits of the demand reader. Large inline values are
conservatively charged when selected even if their source row is `Arc`-shared.
Point overlay projection is admitted by the same byte envelope before cloning.
Every projected row that resolves one or more overflow values consumes exactly
one hydration-row unit. Resolution stages all counters and values, so admission
or reference mismatch publishes neither a partial row nor partial budget.
The reported identity, selected point source, recovery runs and bytes, live
entries, distinct overlay entries, replacements, merge-source count, peak
buffered entries, peak open recovery files, peak overlay resident bytes, page
reads, cache behavior, emitted rows, and hydration bytes make each read
explainable.

Admission, cancellation, deadline, callback stop, and callback unwind do not
poison the pinned reader. Checksum, binding, epoch, schema-shape, immutable
identity, bound overflow-closure, or durability failures poison it and make
later operations fail closed. A missing or mismatched reference in the pinned
state resolver is also corruption at the SQL composition boundary. The storage
reader invokes that fallible resolver before the row callback. Callback effects
remain provisional until both snapshot reading and pinned-state resolution
return `Ok`.

The public bounded PostgreSQL read entrypoint for a pinned transaction MUST
accept a `RuntimeTaskContext` and propagate it through planning, index
traversal, row hydration, and result construction. A cancelled or expired
statement MUST leave that pinned transaction usable by a later statement.

This is the only v1 snapshot-composition contract. HawDB is not released, so
there is no legacy row-root reader, manifest migration, compatibility fallback,
or base-only serving mode to preserve. Production SQL now selects the exact
snapshot reader as its sole ordinary read path. Differential execution remains
qualification evidence only; an unavailable reader fails closed instead of
selecting a materialized compatibility path.

The host snapshot retains the immutable base overflow root, shared segment
cache, and store identity required to construct that reader after the mutable
durability handle is removed. `Missing` means no first checkpoint exists and
therefore selects the canonical in-memory state. `Stale`, `Unavailable`, and
`LiveUnavailable` are not aliases for `Missing`; they reject the read. This is
an authority transition inside the single v1 format, not a format-version
fallback.

The canonical relational snapshot reader uses the existing shared `SegmentCache`
for immutable base-page slots and WAL recovery-delta pages. Cache entries retain
the complete physical identity: store, manifest or delta generation, page
identity, verified content digest, and representation kind. A relational base
page is inserted as compact encoded bytes only after the complete padded source
slot has passed CRC32C, SHA-256, zero-tail, encoded-page, directory, and root
descriptor validation. The cache separately retains the full source SHA-256 as
a verification tag because the compact bytes intentionally do not have the same
digest or length as the physical slot. Untagged lookup cannot observe such an
entry. The cache rejects any later attempt to associate a different source tag
or different compact bytes with that immutable identity. Recovery descriptors
already carry the complete delta-page digest and therefore use exact-key lookup.

Opening either reader validates bounded manifests and artifact lengths but does
not open the page artifact or populate the cache. The first lookup uses
cross-platform positioned reads and reports logical pages/bytes separately from
file pages/bytes, cache hits/misses, and cache admission rejections. An
oversized entry or temporarily pinned cache does not make an otherwise admitted
query incorrect: the reader keeps the strongly verified page only for the
current bounded operation and bypasses cache residency. Corruption, digest
collision, or immutable-identity collision poisons the reader. Raw-page cache
leases end before decoded traversal continues, so cancellation, early stop,
error, and panic cannot retain a cache pin through the cursor lifetime.

## Transaction and lock contract

Ordinary snapshot reads do not acquire row locks. Locking reads and mutations
use logical identities rather than physical page addresses:

- `Row(table, primary_key)`;
- `UniqueKey(index, key)`, including an absent key;
- `KeyRange(index, lower, upper)` for indexed predicates and gaps;
- `GraphNode(node_id)` and `GraphRelationship(relationship_id)`;
- `NodeDeleteGuard(node_id)`;
- `AdjacencyRange(node_id, type, direction, bounds)`.

`SELECT ... FOR SHARE` and `SELECT ... FOR UPDATE` acquire shared or exclusive
key/range locks through SQL. Exact UPDATE and DELETE acquire exclusive row
locks. INSERT, UPSERT, and MERGE acquire unique-key locks before checking
existence. Foreign-key validation acquires shared referenced-key locks.

Requests are normalized and acquired in deterministic namespace/key order. A
wait-for graph detects cycles and returns a retryable victim. The lock table is
bounded by count and bytes; admitted escalation uses an observable table or
adjacency-range lock. Statement rollback releases locks obtained after its
savepoint. Commit, rollback, cancellation, timeout, and panic release all
transaction locks.

Graph mutation locking uses a two-pass COW protocol. The first pass stages the
mutation only in the transaction-private workspace and captures its exact WAL
footprint. HawDB restores that statement workspace, acquires the derived
logical identities, and deterministically replays the statement. Node and
relationship ID allocation locks prevent two pinned snapshots from allocating
the same physical identity. Shared node-delete guards held by relationship
creation conflict with an exclusive guard held by node deletion. Typed incoming
and outgoing adjacency locks protect the posting groups changed by relationship
create/delete. Label and relationship-type locks cover uniqueness and other
catalog constraints; schema changes and any footprint that cannot be derived
completely use the database lock.

The concrete graph identities are valid only for the snapshot used by the
first pass. If the published epoch advances before a newly derived lock set is
admitted, HawDB rejects the transaction instead of refreshing and replaying
against an uncovered access set. A property write covered by a uniqueness
constraint takes exclusive constraint-subject coverage; a non-unique property
write retains shared subject coverage plus its exclusive entity lock.

A bounded lock timeout restores the statement's prior lock set and leaves the
transaction usable. A deadlock victim or lock-budget rejection aborts the whole
transaction and releases all locks. A failure during either staging or replay
restores the statement's graph workspace and prior lock set. Successful
statements retain their logical locks until commit or transaction rollback.

Row locks remain correct while index pages are evicted or physically rewritten
because lock identities never contain a `PageId`, file offset, or cache lease.

## Vectorized execution over row pages

Persistent row orientation does not require scalar execution. The runtime
uses:

```text
PhysicalPlanSpec -> OperatorSpec -> OperatorState -> bounded typed Batch
```

A row-page scan decodes only required fields into reusable typed vectors and a
selection. `RowLocator` values defer large-value hydration until filtering and
ranking select admitted rows. A batch has row and byte limits. Page pins live
for at most one pipeline wave.

Scan, filter, project, expand, and limit SHOULD retain typed batches. Scalar
`Binding` values remain compatibility and final-result boundaries. Sort,
aggregate, distinct, and join MUST charge resident state and spill when their
admitted workarea is exhausted.

The row-page lending cursor is intentionally local to the synchronous
scan-filter-project-limit fragment. It MUST NOT replace the object-safe storage
facade or cross sort, aggregate, distinct, join, spill, or public result
boundaries, all of which may retain values after the next input step. Existing
closed numeric batches remain the typed primitive fast path. Variable-width or
additional primitive batches MUST NOT be activated merely because a GAT can
express them; activation requires a production-shaped differential benchmark
that preserves semantics and demonstrates either at least 15% throughput gain
or at least 50% allocated-byte reduction without a point-read regression.

## Resource governance

One hierarchical byte ledger MUST account for:

- root descriptors and catalog metadata;
- raw and decoded row/index cache pages;
- page pins and dirty overlays;
- WAL and recovery buffers;
- execution batches and blocking operator state;
- result materialization and spill staging;
- derived projection maintenance.

Resident cache bytes and pinned bytes are distinct. Shared `Arc` values are
charged once to the owning cache and separately reported as pinned while a
lease exists. Allocating another reference MUST NOT charge the payload again.

Reclamation order is derived projection cache, cold index/row pages, background
pause, spillable execution state, writer backpressure, and finally rejection
of new work. Process RSS and page-fault observations validate the ledger. An
RSS hard watermark may reject new work even when logical accounting claims
headroom.

## Derived projections

Column groups, deletion vectors, BM25, vector ANN, statistics, and analytics
artifacts are selected by source commit epoch, schema identity, algorithm/index
identity, and manifest generation. Publication is durable-before-visible and
generation fenced. A projection may be rebuilt or discarded without changing
canonical row/index recovery.

SearchIndex remains external derived state. Search documents, ANN internals,
and search cache state MUST NOT enter the canonical graph WAL. Incremental
projection cursors may consume commit-ordered change evidence, but canonical
commit acknowledgment does not depend on projection freshness.

## Formal obligations

`HawDBTransactionConcurrency.tla` already owns the transaction-level subset
of this contract. The remaining model names below are planned ownership
boundaries and MUST land before their corresponding production activation:

The row-page codec is a pure byte transformation and does not add a visible
state transition. Its evidence is exact round-trip, ordered-key differential,
projected-decode, shared-limit, and corruption testing. Shadow COW publication
is the first stateful use of these bytes. Its fixed runtime event trace, stale
generation fence, immutable artifacts, crash boundaries, and pinned
cross-generation descriptors refine `HawDBCowPagePublication.tla`. WAL recovery
and serving activation are separate lower-level obligations composed by the
snapshot runtime. The bounded base-plus-WAL recovery view refines
`HawDBRowRecovery.tla`; its SQL authority, pre-checkpoint exception,
schema-checkpoint barrier, and unavailable-reader rejection refine
`HawDBRelationalRowSnapshotRead.tla`. Its physical one-head-per-source range
merge, newest-epoch coalescing, and peak buffer admission refine
`HawDBRelationalOverlayStreamingMerge.tla`.
Immutable disk-backed row-delta publication refines `HawDBRowDeltaRuns.tla`;
it remains outside the recovery mount and therefore does not discharge
checkpoint binding, demand-read, lifecycle, or serving obligations.

- `HawDBTransactionConcurrency.tla`: logical lock namespaces, compatibility,
  wait-for deadlocks, escalation, savepoint release, and durable publication.
- `HawDBCowPagePublication.tla`: WAL ordering, immutable page publication,
  reader pins, crash recovery, and reclamation.
- `HawDBIndexPublication.tla`: atomic row/index root agreement, durable and
  generation-fenced publication, stale-builder rejection, cold open, on-demand
  leaf loading, corrupt-page fail-closed behavior, authoritative constraint
  acceptance/rejection before WAL, durable-before-visible mutation
  publication, row/index visible-epoch agreement, absence of materialized
  postings on authoritative handles, and recovery after a crash between WAL
  durability and in-process publication.
- `HawDBRelationalIndexShadowPublication.tla`: optional checkpoint-bound
  relational-index identity, complete root-set publication, candidate-failure
  isolation, cold open, and mode-specific corruption handling. Its optional
  candidate contract remains the `Shadow`/`DemandPaged` boundary; the runtime
  authoritative mode strengthens that binding separately.
- `HawDBIndexRecovery.tla`: base root plus ordered WAL delta equivalence,
  bounded dirty overlays, immutable candidate generations, crash recovery,
  schema invalidation, no partial replay visibility, and sound exact-key
  constraint qualification only from a current pinned view.
- `HawDBTransactionIndexOverlay.tla`: pinned committed row/index bases, bounded
  transaction-private immutable row/index overlays, version agreement,
  rejected-statement atomicity,
  read-your-own-writes, rollback, and durable-before-visible publication. The
  concrete Content Store qualification composes multiple SQL statements and
  verifies ordered merge, transaction-workspace routing, and failure atomicity;
  entry/byte exhaustion remains a focused Rust refinement obligation. The same
  qualification propagates cancellation through a pinned Content Store point
  read, proves zero leaked page pins and later-reader usability, and exercises a
  `FOR UPDATE` point lock against a same-key UPSERT with bounded timeout,
  aborted-waiter, rollback, unchanged-row, and unchanged-epoch evidence. Its
  corruption probe creates and restores a public backup into a disposable
  directory, bit-flips the current v1 row-page artifact, requires full scrub to
  poison that handle and reject later SQL, and rechecks that the retained source
  database is unchanged.
  The resource probe records the declared profile kind and available bytes,
  detected host/cgroup memory, concrete query/cache/executor/WAL/delta budgets,
  warm-read percentiles, steady and lifetime-peak RSS, page faults, exact WAL
  append bytes, and bytes in newly named immutable generation artifacts. Its
  write-amplification value is explicitly a lower bound over WAL append plus
  those new artifacts; it MUST NOT be presented as block-device bytes written.
  The cumulative relational-index read-byte budget is independent of segment
  cache residency capacity: a bounded streaming query may read and evict more
  bytes than can be resident simultaneously. Both values remain explicit in
  resource evidence.
  Result payload admission is likewise independent of the cumulative overflow
  hydration budget. A small aggregate result may scan larger admitted inputs;
  the input hydration limit remains explicit and is enforced separately.
  `Capability512Mib` records whether the observed process peak fits the named
  512 MiB capability profile. Production evidence additionally binds the
  explicit 512 MiB runtime-governor ceiling and its admission/completion
  counters; it does not require an OS-level 512 MiB limit. The exact value
  identifies qualification evidence; it is not the default engine limit or an
  activation threshold.
  `ConfiguredWorkload` accepts a different caller-declared budget without
  substituting 512 MiB as a universal cutoff. Production-copy evidence remains
  a separate runner over the imported replica and its actual resource profile;
  this synthetic fixture MUST NOT claim that qualification.
  The production graph runner retains the complete ordered cold/warm resource
  series instead of only the final warm profile. It records per-run streaming,
  row/payload, RSS, page-fault, and cache-residency evidence, derives an
  aggregate from that series, and records process memory across the entire
  open/read/cancellation lifecycle.
  The final release evaluator independently recomputes the aggregate and
  rejects a missing, reordered, incorrectly phased, truncated, over-budget, or
  inconsistent series. Per-query page-fault limits apply to each run; the
  lifecycle total is separate recorded evidence and MUST NOT be compared to a
  single-query limit.
  The same typed runner covers `content_documents`, `thread_messages`,
  `content_chunks`, and `content_anchors`. Its base, WAL-recovered, and live
  phases execute parameterized PostgreSQL-dialect statements through canonical
  row pages. Graph Source identity, chunk rows, message occurrences, and typed
  anchors publish in one mixed transaction epoch. The occurrence fixture uses
  two distinct `content_message_id` values with one shared legacy `message_id`
  and requires both anchors to remain visible.
  Source replacement then uses the frozen `upsert_source_chunks` statement
  group to publish graph count, exact relational chunks, and document summary
  in one epoch. A shorter replacement proves stale-suffix removal; a rejected
  duplicate order proves statement rollback; an empty replacement proves zero
  rows and counts. Both phases checkpoint, reopen, and retain identical ordered
  output digests. `HawDBContentSourceReplacement.tla` models this operation.
  Source ownership qualification reseeds an exact chunk set after the empty
  replacement and executes the graph Source and relational document workspace
  updates in one mixed transaction. It proves read-your-own-writes, graph and
  relational owner agreement, unchanged chunk count and payload digest, live
  overlay visibility, checkpoint/reopen identity, and a missing-owner no-op
  that does not advance the commit epoch.
  `HawDBContentSourceOwnershipMove.tla` models this operation.
  Thread ownership qualification seeds rows in different source workspaces,
  moves graph Thread, relational document, and messages through guarded writes
  in one transaction, and keeps a stale-preview row unchanged. It proves
  read-your-own-writes, two successful moves out of three requested moves,
  payload preservation, live overlay visibility, and checkpoint/reopen
  identity. `HawDBContentThreadOwnershipMove.tla` models the guarded batch.
  Space-merge ownership qualification then selects those Threads together with
  a Source under one source-space guard. Eligible graph owners, relational
  documents, messages, and Source chunk views publish at one epoch; the stale
  Thread remains unchanged. Non-ownership payloads and ordered output remain
  identical after checkpoint/reopen.
  `HawDBContentSpaceMergeOwnership.tla` models this cross-kind batch.
- `HawDBRowRecovery.tla`: checkpoint-correlated row-root mount, exact ordered
  primary-key WAL overlay, graph-only epoch advancement, whole-fragment
  admission, fail-closed invalidation, complete-prefix view publication, cold
  page slots, pinned generation stability, and live invalidation before the
  separate SQL serving refinement.
- `HawDBRowDeltaRuns.tla`: bounded coalescing and immutable run flush,
  overflow closure, run-before-generation-manifest durability, row-root and
  previous-delta fencing, exact final row-count publication, manifest-last
  selection, crash isolation, poisoned candidate rejection, and pinned
  generation stability.
- `HawDBRelationalRowDemandRead.tla`: exact root-generation pinning, cold page
  residency, ordered streaming, page/byte/row/tree-height/hydration bounds,
  requested-field-only overflow hydration, one-page pins, cancellation and
  panic cleanup, cache eviction safety, and corruption-only poison.
- `HawDBPageCacheAdmission.tla`: clean immutable page residency, pin-safe
  eviction, cancellation release, caller-carved foreground reserve, corrupt
  admission rejection, cold open, and background hit/admit/bypass progress.
  Dirty row-page publication remains owned by
  `HawDBCowPagePublication.tla`; it is not inferred from this clean-cache model.

Existing `HawDBCompactionVisibility.tla`, `HawDBColumnGroupManifest.tla`, and
`HawDBColumnarShadowIntegration.tla` continue to prove derived column-group
behavior. They do not define canonical row/index recovery.

## Evidence and activation gates

Correctness evidence includes:

- row/index lookup differential tests against the current in-memory oracle;
- restart at every WAL/page/manifest publication boundary;
- missing, stale, truncated, bit-flipped, and cross-generation page rejection;
- same-row serialization, disjoint-writer progress, absent-key uniqueness,
  range phantom prevention, and deadlock victim cleanup;
- cold and warm point/range queries under cache eviction;
- cancellation and panic pin cleanup;
- row-versus-batch execution differential and fuzz corpora.

Performance and resource evidence records separately:

- manifest/root-open latency;
- WAL replay latency;
- total open latency and peak RSS;
- first cold point/range query latency;
- warmed point/range query latency;
- page reads, bytes, cache hits, evictions, resident bytes, and pinned bytes;
- dirty and WAL bytes, spill bytes, page faults, and write amplification.

`StorageResidencyReport` MUST expose relational rows and indexes independently
from graph residency. Each relational report is derived from the currently
pinned serving view rather than a directory scan: it records the base and
recovery generations, base and visible epochs, immutable artifact bytes, and
bounded live-overlay counts and bytes. Row residency additionally records root
descriptors, root keys, overflow extents, conservative live resident bytes,
whether checkpoint rows remain materialized, materialized row count and bytes,
whether the checkpoint state was built from canonical metadata without a row
decode, and the exact logical row count retained after detachment;
index residency records roots, base pages, and immutable recovery-delta pages.
If no read view is current at the database epoch, `serving` MUST be false and
the report MUST NOT manufacture a generation from stale files. Production-copy
qualification uses these fields to prove that the selected relational
artifacts exceed the shared cache while recovery and live overlays remain
bounded.

The 512 MiB profile is a supported low-memory capability profile, not a
universal process limit or a production activation cutoff. Its evidence names
the bounded workload and proves admission and eviction keep that workload
within the configured budget. Production-copy latency, RSS, page-fault, and
write-amplification gates use the replica's actual configured resource profile
and retain that profile in the evidence identity.

With fixed schema and WAL input, manifest/root-open work and mandatory index
residency MUST remain independent of row, index-entry, and leaf-page counts.
Total startup MAY grow with the WAL replay work. Point reads and low-concurrency
commits MUST NOT regress by more than five percent at p95 when a production
path is activated. A vectorized fragment requires at least twenty percent
throughput improvement or fifteen percent lower CPU per row.

## Delivery discipline

Codec, shadow publication, demand reader, WAL recovery delta, and production
activation are separate commits. New formats remain unselected until their
reader, differential oracle, corruption tests, formal model, and resource
evidence are present. Relational and graph activation are separate so either
path can remain on the current oracle without changing durable canonical
bytes.
