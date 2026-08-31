# Projection Generation Replacement Specification

## Status and scope

This specification defines the development-phase embedded library contract for
owner-scoped replacement of deterministic derived projections. It does not
authorize Skein in stable or GA Mem artifacts.

The contract coordinates candidate staging, validation, publication, pinned
reads, recovery, and reclamation. Those operations require a typed Rust API.
Ordinary application graph and relational behavior remains parameterized
Cypher or PostgreSQL-dialect SQL. A route-specific projection DTO or host-side
scan, join, prune, or keep-ID comparison is not part of this contract.

## Identity and member model

Each candidate is identified by:

- a projection name;
- an opaque owner key;
- a caller-supplied generation identity;
- a source watermark;
- a non-zero projection version; and
- an optional expected active-generation identity.

A generation contains storage-neutral members. Each member has a collection,
an opaque key, and an opaque payload, allowing relational and graph projection
encodings to share the publication catalog. Members MUST be appended in
strictly increasing `(collection, key)` order. This canonical order makes the
complete digest deterministic and permits sealing without retaining a keep-ID
set in memory.

Every append call is admitted against explicit row, aggregate payload,
collection-name, and key-byte limits. Readers use independent row, payload,
and encoded-record limits. Exceeding a limit fails the operation; it does not
return or publish a partial logical result.

## Lifecycle

1. `begin_candidate` creates or resumes one unpublished generation.
2. `append_batch` synchronizes one bounded canonical batch. Durable candidate
   bytes are not reachable through an owner head.
3. `seal` verifies caller-supplied total member count, payload bytes, and the
   streaming CRC32C plus SHA-256 digest. It then durably writes one immutable
   manifest containing those values and bounded rollup metadata.
4. `publish` validates the expected head and source watermark, then atomically
   replaces the owner head. The head contains the complete selected manifest,
   digest, counts, rollup metadata, and monotonically increasing publication
   commit epoch.
5. `open_active` resolves and pins that exact head and manifest. All pages from
   the returned reader remain generation-bound even after a replacement.
6. `reclaim` removes only bounded numbers and bytes of inactive, unpinned
   generations or abandoned candidates.

Omitting a member from the replacement generation performs the logical prune
at step 4. Physical deletion is deferred to step 6.

## Atomicity and recovery

Candidate data is synchronized before its immutable manifest. The complete
manifest is embedded again in the owner head, and the head is published last
with durable file replacement. This head replacement is the generation
publication commit boundary: readers observe the previous complete head or the
replacement complete head. The returned publication report is the bounded
publication barrier and carries its commit epoch.

This catalog is deliberately independent of the graph mutation WAL. It does
not claim that candidate batches are graph commits. The publish-last head is an
equivalent atomic durable boundary for rebuildable projection state, so normal
graph WAL checkpointing and pruning cannot leave a selected generation without
its bound manifest and synchronized data artifact.

A torn final candidate record is truncated to the last checksummed boundary
when an unpublished candidate is resumed. Corruption before that boundary,
corrupt manifests, identity drift, and head-to-manifest drift fail closed.
Crashing before head replacement preserves the previous active generation;
crashing after replacement recovers the complete new generation.

## Concurrency and idempotency

Only one writer for the same generation identity can be open through one
catalog handle. Different owners can stage independently. Publication holds the
catalog coordination lock while it validates and replaces the head.

The expected-head comparison prevents a stale same-owner publisher from
overwriting an intervening publication. A source watermark older than the
active watermark is rejected even when other metadata appears valid. Reusing a
generation identity with different begin metadata, content, or manifest fails
closed. Re-sealing and republishing the same durable generation and manifest is
idempotent while it remains the selected head.

## Read and query integration

`ProjectionGenerationReader` is a bounded, generation-pinned kernel read
contract. It is not permission for an application route to perform its own
graph or relational scan.

`ProjectionRelationalReadBinding` binds a projection name, owner key, required
projection version, and a non-empty set of durable relational table names.
`Database::begin_projection_read_transaction` resolves the active generation
once and retains both its reader pin and the database read view for the whole
transaction. PostgreSQL-dialect SQL reads bound tables from that generation;
unbound tables continue to use the same canonical database snapshot. Bound
tables MUST have durable DDL and MUST contain no canonical rows, preventing a
query from silently mixing generation and canonical ownership.

Relational members use the durable table name as the collection, the canonical
ordered primary-key encoding as the member key, and Skein's versioned
relational row codec as the payload. Encoding validates the row against the
durable schema. Decoding independently validates the row shape, scalar types,
nullability, primary-key encoding, and agreement between the member key and
decoded row.

The initial planner contract deliberately exposes bound tables as bounded full
scans. It does not advertise canonical primary-key or secondary-index access
for data that lives in generation artifacts. Every projection page is charged
to the SQL row and payload budgets, and exhaustion fails the statement instead
of returning a partial result. A future generation-native index may replace
this access path only when it preserves the same pinned-generation and
admission contract.

The SQL execution profile and `EXPLAIN ANALYZE` expose the
`projection_generation` runtime path, generation identity, source watermark,
projection version, publication commit epoch, and page accounting. Applications
therefore express projection reads as parameterized PostgreSQL SQL while the
typed Rust API remains limited to publication and transaction coordination; a
route-specific typed CRUD API remains out of scope.

## Observability

Exact-owner diagnostics are available through `status`; no unbounded all-owner
listing exists. The status identifies staging, abandoned, sealed, published,
failed, or missing state, the active generation, source watermark, member and
payload counts, publication commit epoch, pin state, and a corruption blocker
when available. `scrub` verifies the complete selected artifact against its
manifest. Reclamation reports scanned files, reclaimed generations and bytes,
active and pinned skips, and whether bounded work remains.

## Formal refinement and verification

`docs/tla/SkeinProjectionGenerationReplacement.tla` models candidate
invisibility, sealed-before-publish ordering, expected-head publication,
generation-pinned readers, crash recovery, and pin-safe reclamation.

Targeted Rust tests cover bounded staging, digest and count validation,
idempotent publication, expected-head and watermark conflicts, prune by
omission, multiple pins, independent owners, abandoned-candidate GC, torn-tail
resume, generation-bound paging, complete scrub, and durable `Database`
reopen. Relational query tests additionally cover candidate invisibility,
old-reader pinning after publication, prune-by-omission visibility, multi-table
PostgreSQL joins, canonical-source isolation, and query-profile evidence.
