# Block-Level COW Checkpoint Generations

Status: proposed for review, November 27, 2026.

Tracking: [#2020](https://github.com/nowledge-co/hawdb/issues/2020).

Related: [#1385](https://github.com/nowledge-co/hawdb/issues/1385),
[#1960](https://github.com/nowledge-co/hawdb/issues/1960),
[#1521](https://github.com/nowledge-co/hawdb/issues/1521) and
[#1737](https://github.com/nowledge-co/hawdb/issues/1737).

Requirement keywords describe the proposed acceptance contract. They do not
announce implemented capabilities. Until this contract is implemented and
qualified, [`../STORAGE.md`](../STORAGE.md) remains authoritative.

## Scope

This contract defines immutable data blocks and block-referencing generation
manifests for canonical graph checkpoints. It replaces the assumption that one
generation's canonical node/relationship data must be one monolithic
`canonical.<generation>.hawdb` artifact.

The first delivery covers canonical nodes and relationships. The same block
contract SHOULD then cover adjacency and persistent property projections. The
contract is deliberately independent of search projections, relational row
pages, WAL framing and branch lifecycle.

Non-goals:

- changing the default checkpoint-generation retention policy;
- making `CurrentOnly` the default;
- filesystem-specific reflinks as a correctness mechanism;
- a second WAL, a new crate or a mutable B-tree page store;
- search-projection body deduplication;
- decoding the old monolithic canonical layout in the new reader.

## Current behavior and evidence

A changed checkpoint rebuilds the complete canonical/spill/property-projection
graph bundle. Node property updates rebuild canonical while adjacency can be
carried; relationship property updates rebuild both. The complete closure is
published into the content-addressed object store, and the default retention
policy keeps the selected and previous generations.

The result is two full graph generations for a local change. On a synthetic
Mem-shaped fixture (5,000 nodes with 512-byte bodies and 4,999 relationships),
one generation is about 5.89 MB unique-inode bytes; two generations after a
relationship update are about 9.27 MB unique and 18.5 MB logical over the data
root and object-store aliases. A 1,000-node/999-edge fixture retains about
1.81 MB unique after one relationship update, while `CurrentOnly` retains about
0.92 MB.

The same fixture shows why adjacency must join the block contract rather than
wait for a later full rewrite: adjacency data plus descriptors is about 456
bytes per edge because sparse `(direction, endpoint, rel_type)` groups each
publish a block and descriptor.

Existing foundations this contract uses:

- `ImmutableObjectStore` already deduplicates content-addressed objects and
  hard-links unchanged checkpoint producers where supported.
- canonical and adjacency artifacts already expose descriptor trees and
  immutable block/segment boundaries;
- reader pins, prepared candidates and branch creation bases already define the
  retained-root mark used by reclamation;
- relational row pages already publish immutable pages and rewrite only dirty
  pages;
- `DurableStore::reusable_graph_bundle` already carries an entirely unchanged
  graph bundle.

## Definitions

- **Block**: an immutable, checksummed, content-addressed byte sequence holding
  one ordered slice of records plus a block header.
- **Block reference**: `(kind, format version, byte length, content digest,
  bucket bounds, record count, spill namespace identity)`. The digest is the
  authoritative block identity; the spill namespace is part of the block's
  decoding contract.
- **Bucket**: a stable key interval used to assign records to blocks. Node and
  relationship buckets are derived from their immutable internal IDs, not from
  the byte size of the current generation.
- **Generation manifest**: the checksummed metadata selecting the ordered block
  reference table, descriptor root and generation identity.
- **Spill namespace**: an immutable property-spill artifact root, identified by
  its content digest, against which a canonical record's bare spill ID is
  interpreted. Spill IDs are not generation-global; they are only meaningful
  inside one spill namespace.
- **Carry**: retaining a block reference from the selected generation without
  rewriting its bytes.
- **Dirty key set**: the complete set of node and relationship IDs created,
  updated, deleted or otherwise invalidated since the selected checkpoint. A
  partial or unknown set MUST be treated as a full rebuild.

## Block model

Every block MUST contain:

```text
magic | format version | block kind | bucket start | bucket end |
record count | payload length | payload | CRC32C | SHA-256
```

`kind` distinguishes canonical nodes, canonical relationships, adjacency and
property-projection blocks. A reader MUST reject an unknown kind, version,
length or digest, a mismatched bucket range, a non-monotonic record key, or a
payload whose records do not exactly fill the declared length.

A block MUST be immutable once published. Its digest MUST cover every header
and payload byte that a reader interprets. Block identity MUST NOT depend on a
generation number. A block whose payload contains bare spill IDs MUST NOT be
reused across spill namespaces merely because its bytes hash to the same
digest; reuse requires the same kind, bucket, dictionary identity and spill
namespace, or the writer MUST re-encode the block.

The authoritative block namespace MUST be content-addressed and deduplicated
per project. Storing the same block under two independent copies is prohibited.
A data-root alias MAY be a hard link to the authoritative object or MAY be
absent; a copy fallback MUST be classified as an independent object and become
reclaimable with the same reachability rules. A block MUST be fully written,
checksummed and synchronized before any manifest references it.

## Stable bucket boundaries

Rebalancing a block because a preceding record changed would defeat sharing.
Block assignment MUST therefore use stable boundaries:

- nodes use a fixed-width `NodeId` bucket (for example, a power-of-two ID
  range) and an optional part ordinal when one bucket exceeds the block byte
  target;
- relationships use a fixed-width `RelId` bucket, with a deterministic part
  ordinal;
- adjacency uses a fixed-width endpoint bucket plus direction and relationship
  type; sparse groups SHOULD be packed into shared blocks;
- property projections use stable value/key range buckets once their
  implementation is migrated.

A record insert, update or delete MUST affect only its bucket's blocks and,
where required, the tail. It MUST NOT shift records into later buckets.
Deleting every record in a bucket MAY leave an empty bucket; an empty bucket
MUST NOT publish a block.

The block target size is a writer policy with a bounded maximum. The initial
target SHOULD be 1-4 MiB and MUST be benchmarked against point-read latency,
scan throughput, descriptor size and sharing ratio. A block reader MUST NOT
assume the target size of a writer.

## Generation manifest

A generation manifest MUST bind:

- the generation identity and source commit epoch;
- the ordered block-reference table, including kind, bucket bounds, part,
  record count, byte length, digest and spill namespace identity;
- every immutable spill namespace referenced by a block, with its authenticated
  object identity and complete closure;
- the descriptor tree root and its identity;
- the property-key dictionary;
- the manifest checksum.

The descriptor tree maps ordered key ranges to block references. A point read
MUST be able to locate one block from the descriptor without reading the
complete block table payload. A scan MUST be able to stream blocks in key order
through the bounded block cache.

A generation manifest MAY reference more than one spill namespace. This is
required when an unchanged canonical block is carried from an older generation
while newly written blocks use the current spill artifact. Each block MUST
resolve every bare spill ID through its own recorded spill namespace, never
through a global "selected spill file" chosen by the reader. A carried block
MUST be rebuilt instead if its spill namespace is not retained in the new
manifest closure.

The manifest MUST NOT embed a complete data block. Manifest size MAY grow with
the number of blocks, but MUST be charged to the existing graph-manifest open
budget and bounded by an explicit block-count and manifest-byte limit.

## Write protocol

A checkpoint writer MUST:

1. Capture a coherent source and its complete dirty key set.
2. Carry a selected-generation block when its bucket does not intersect the
   dirty set, its logical schema and dictionary dependencies are unchanged, and
   its recorded spill namespace remains retained for the new generation. A
   carried block MUST keep its original spill namespace; it MUST NOT be
   reinterpreted against a newly allocated spill artifact.
3. Re-encode every touched bucket from the captured source, split by the stable
   boundary rules.
4. Compute each block digest. If an equivalent block already exists, reuse its
   reference after verifying length and identity; otherwise publish a new block
   through the immutable-object protocol.
5. Publish and synchronize every new block before publishing the descriptor
   tree.
6. Write the new manifest through a temporary file, synchronize it, atomically
   rename it and synchronize its parent directory.
7. Publish the branch head/root selecting the new manifest. The superseded
   manifest remains readable while a reader, candidate or creation base pins it.
8. Run the existing bounded post-publication mark/sweep. A failed sweep MUST be
   recorded as retryable debt and MUST NOT change the checkpoint outcome.

If the dirty key set is incomplete, the writer MUST rebuild all blocks. A
schema, property-key dictionary, block format, block-kind or block-boundary
change MUST rebuild all affected blocks. It is never valid to carry a block
whose record encoding or key interpretation changed.

Spill-ID stability is part of that rule. Spill IDs are allocated in traversal
order and can be renumbered when an earlier record gains or loses a spilled
value. A block checksum therefore does not prove that its spill references are
still valid under a rebuilt spill artifact. Phase 1, which leaves spill storage
monolithic, MUST apply one of these explicit rules:

- bind each block to the immutable spill namespace it was encoded against,
  retain that namespace and its closure while the block is retained, and allow
  mixed carried/new blocks to select different namespaces; or
- rebuild every block that may contain a spill reference whenever the spill
  namespace changes.

The first rule is the target design. The second is a safe Phase 1 fallback when
per-block namespace binding or closure retention is not yet available. A
generation manifest that merely lists spill references without binding them to
blocks does not satisfy this contract.

The writer MUST NOT copy a carried block merely to change its generation name.
The new manifest references the existing content identity.

## Read protocol

A reader MUST:

- open the selected manifest and validate its generation, checksum and bounds;
- resolve descriptor entries to block references without making startup
  residency proportional to data bytes;
- verify a block's length and digest before decoding, or use a verified cache
  entry that retains the same identity and immutable bytes;
- decode only the selected properties where the point/scan path permits it;
- fail closed on a missing, truncated, reordered, wrong-kind or wrong-digest
  block;
- keep reader-pinned manifests and their complete block lists reachable for the
  lifetime of the pin.

A missing block is storage integrity failure, not an invitation to silently
fall back to the previous generation. The selected generation remains the
serving authority. Explicit recovery may select a retained previous root only
through the existing manifest/head recovery contract.

## Recovery and crash ordering

Block publication and manifest publication are separate durable boundaries:

- a block written before a crash but never referenced by a selected manifest is
  garbage and MAY be reclaimed;
- a complete manifest published before a crash is authoritative only if every
  referenced block was synchronized before it;
- a crash between manifest rename and branch-head publication follows the
  existing checkpoint/head selection rules;
- a crash after head publication leaves the previous root for reader/pin
  reclamation and the new root complete.

Recovery MUST verify the selected manifest and the blocks it touches before
serving data. It MUST NOT recreate an empty generation, repair a block by
guessing, or delete a block referenced by any retained root. Torn or reordered
physical writes remain a platform qualification concern, not a reason to weaken
these logical barriers.

## Reclamation

Reclamation MUST derive block reachability from retained roots, not from file
names or generation numbers. Roots include:

- selected branch heads and the catalog creation baseline;
- every generation manifest retained by the configured policy, including the
  previous generation under `CurrentAndPrevious` even when no reader,
  candidate or owner pin names that generation. The policy-retained previous
  manifest protects its previous-only blocks;
- reader snapshots and their physical generation pins;
- prepared checkpoint candidates and uncertain publication evidence;
- live branch owners and explicit branch-creation roots;
- every spill namespace referenced by any retained block, including namespaces
  used only by carried blocks;
- backups or export staging that the existing contract protects.

A block is reclaimable only when no retained root references its digest. A spill
namespace is reclaimable only when no retained block references it. The
existing bounded mark/sweep, quarantine evidence, debt reporting and explicit
`reclaim_branch_storage` path MUST apply. Reclaiming a block MUST NOT require
rewriting a live manifest.

Current and previous generations naturally share blocks because their manifests
name the same digests. Retaining a previous generation therefore costs its
unshared delta, descriptor/manifest metadata and pinned roots rather than a
second complete graph bundle.

## Backup, branches, scrub and format

- Backup MUST include every block referenced by the copied roots and MUST
  restore a manifest whose block table is complete.
- Branch creation from a sealed root MUST share block objects. A branch may not
  rewrite or delete a block while another branch root references it.
- Scrub MUST verify every block, descriptor entry, manifest checksum and
  closure reference, and MUST report an unreferenced but corrupt block without
  treating it as live data.
- The persistent format version MUST be bumped because an older reader cannot
  interpret the block-referencing manifest. Logical dump remains the supported
  cross-version transfer path.
- The block codec, manifest revision and `docs/persistent-format.lock` MUST move
  together. Development databases may be recreated; authoritative legacy or
  Cloud data must use the logical dump.

## Resource and performance budgets

The writer MUST bound:

- one in-flight block payload and its encoded/decoded workspace;
- dirty bucket count and new block count per checkpoint;
- block publication descriptors and directory operations;
- descriptor/manifest bytes and block count;
- cache residency per block and total cache bytes;
- cancellation and restart work.

The reader MUST bound:

- one block read/decode allocation;
- descriptor pages visited per lookup;
- scan page/block batches;
- cache admission and eviction;
- memory retained by reader pins.

Acceptance thresholds MUST be measured on the same source revision and fixture
for the monolithic and block layouts:

- point-read p50/p99 and scan throughput;
- checkpoint bytes written and checkpoint time;
- current + previous unique-inode bytes and allocated bytes;
- reopen time and peak RSS;
- block hit/miss and bytes read;
- crash/reopen recovery time.

A local improvement does not establish a remote CI or physical-device claim.

## Validation

Required coverage:

- block codec round trip for every kind, bucket boundary, part ordinal, maximum
  size, empty bucket and corrupt header/digest/length;
- carry and rebuild decisions for create, update, delete, tail append, schema
  change, dictionary change and an incomplete dirty set;
- two canonical buckets containing spilled values: add or remove a spill in
  the earlier bucket, carry the unchanged later bucket against its original
  spill namespace, then verify point reads, scans, reopen and GC with both
  namespaces retained; also cover the conservative full-rebuild fallback when
  namespace binding is unavailable;
- point/scan equivalence against the existing monolithic reader over the same
  records before the old path is removed;
- manifest missing/extra/reordered block and descriptor mismatch fail closed;
- checkpoint cuts at every block publication, descriptor, manifest, head and
  reclamation boundary;
- reader pins across multiple checkpoints, prepared candidates and branch
  creation bases;
- backup/restore, scrub, branch create/read/write/delete and explicit
  reclamation;
- format-version refusal, logical-dump round trip and development database
  recreation;
- a TLA safety model for block-root reachability and no-live-block removal.

The Mem-shaped qualification fixture SHOULD contain 100,000 documents with
approximately 1.3 KB bodies, 1,536-dimensional vectors, 1-3 edges per node and
repeated 1-5% updates. It MUST record the sharing ratio separately from the
write amplification and query-latency results.

## Phasing

| Phase | Deliverable | Boundary |
|---|---|---|
| 0 | This contract, issue and benchmark fixture | No format change |
| 1 | Canonical node/relationship block store and carry | Format bump; adjacency/property remain monolithic |
| 2 | Adjacency block COW with sparse-group packing and compact descriptors | Reuses the Phase 1 block contract |
| 3 | Property spill/property-projection block sharing | Reuses the Phase 1 block contract |
| 4 | Compression and default-retention evaluation | Only after sharing and query-latency evidence |

Phase 1 MUST NOT change the default retention policy. Default
`CurrentAndPrevious` continues to provide the previous-generation fallback;
block sharing makes that fallback cheap. Because Phase 1 leaves spill storage
monolithic, Phase 1 MUST either bind blocks to immutable spill namespaces or
fully rebuild spill-dependent canonical blocks whenever the spill mapping
changes; carrying a block against a newly renumbered spill artifact is
forbidden.

## Open questions

- Exact default bucket widths and block target size, subject to the benchmark
  fixture.
- Whether `CheckpointBlock` becomes a new `ObjectKind` or uses a dedicated
  content-addressed namespace with equivalent reachability and backup rules.
- Whether the data root keeps hard-link aliases for hot blocks or always reads
  the authoritative object.
- Which mutation paths can supply a complete dirty key set without a full
  source scan, and which must conservatively rebuild.
- Whether block compression belongs in the block codec or in a later
  projected-value layer.
