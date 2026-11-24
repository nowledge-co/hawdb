# Incremental graph checkpoints: independent adjacency origin

Issue: https://github.com/nowledge-co/hawdb/issues/1960.

## Scope and representation

Stage 1 retains the existing canonical, property-spill and property-projection
bundle G/E, and gives adjacency its own physical origin A/EA. A manifest v3
binds both roots by their existing metadata and digests. The common checkpoint
C covers the complete logical transaction prefix through commit S and its
replay-start LSN. A/EA names where adjacency was built, not where replay starts.
For example, adjacency built at commit 40 can remain valid through checkpoint
42 if commits 41 and 42 only update node properties. Recovery starts after 42.

Canonical/spill/projection still rebuild on a node property update. There is no
block copy-on-write, new crate, second WAL, Bf-Tree backend, directory-sync
batching, weaker durability mode or query-quality change in this stage.

## Eligibility and commit boundaries

`WalOp::changes_adjacency` excludes `SetNodeProperty` and the existing non-graph
operations; it recurses into batches. All other graph operations invalidate
adjacency. In particular, relationship property updates invalidate adjacency:
its entries can inline complete relationship records, so unchanged topology
alone is insufficient. Unknown future graph changes must be classified
conservatively by `changes_graph`.

The foreground path records the classification only after successful WAL
append, and advances its constant-space adjacency fence when the transaction
is installed. Failed appends clear pending classification. Replay classifies
the same complete WAL record. A recovered base starts the fence at checkpoint
coverage S, rather than EA. Reuse requires fence <= S, a published base, the
same complete Catalog, default adjacency build configuration and an admitted
adjacency reader/binding. Other mutations and repair without an admitted
reader rebuild. No unbounded dirty-ID collection is introduced.

A checkpoint source captures the fence with its snapshot. Candidate suffix
replay advances the candidate fence at actual suffix commit epochs. Publishing
a base captured at S never clears invalidations after S or changes its physical
coverage to the newer live tip. The common WAL suffix remains authoritative
for those transactions. A relationship update in that suffix forces the next
checkpoint to rebuild, even when the candidate carried adjacency successfully.

## Ownership, integrity and maintenance

`PublishedReadView` captures C, G and A. Its typed `PhysicalGenerationPins`
retains adjacency independently, including when A differs from both C and G.
Reclamation protects current A, previous A and reader-owned A. Retaining an
adjacency origin does not retain unrelated canonical/spill/projection files
from that generation. Candidate staging cleanup removes only its new
checkpoint generation. Durable branches continue to retain their sealed
closures regardless of runtime leases; their root bindings already enumerate
every physical path and immutable reference.

Open, closure construction, standalone backup/restore, scrub and derived repair
resolve adjacency paths through its authenticated binding A/EA. Repair
quarantine derives both family origins from its checksum-bound source manifest,
including after replacement publication. Descriptor root/page identities remain
checked against the selected family binding. Carried artifact hashing and the
prior sealed-root reference comparison from #1956 remain active. This stage
saves adjacency construction and writes; it makes no O(dirty) total-time or
read-I/O claim. Full closure verification still scales with retained data.

Default synchronized commit/checkpoint/head publication barriers are unchanged.
The outer manifest is v3 and engine persistent format is 8; older binaries
reject the version before interpreting independent origins. This reader retains
v1/v2 uniform-bundle decoding with their original constraints. Payload codecs
and immutable-object reference domains are unchanged. Development databases
must not be silently recreated on an incompatible open.

## Validation and measurements

The existing storage unit owner covers repeated carry/reopen in both residency
modes, byte identity, backup/restore/scrub, inline relationship invalidation,
independent reader retention/release, Catalog/nondefault configuration refusal,
one-epoch mixed node-property/relational replay and candidate suffix replay. Manifest unit
tests cover version refusal, legacy uniform origins, independent origins and
unchanged checkpoint replay boundaries. The existing root integrity fixture
now corrupts an adjacency descriptor carried through node property updates,
while preserving its filesystem identity stamp, and requires failed publication,
unchanged head and invalidated reader admission. Existing interrupted repair
and lost/torn/reordered power-loss fixtures now start from A<G roots. No new CI
lane is added. An embedded read transaction also retains an A<G base across
four relationship-property rebuilds, compares original traversal rows and
checks independent adjacency reclamation after release.

The manual `graph_checkpoint_carry` benchmark accepts a new fixture directory,
node count, relationship count, measured rounds and expected behavior (`carry`
or `rebuild`). It uses `SyncOnEveryWrite`, OutOfCore residency, two warmups and
one complete checkpoint per sample. It measures node property updates and a
relationship-property rebuild control, emits JSONL durations and fresh graph
and adjacency file bytes, verifies every updated record, then scrubs and reopens.
The companion `graph_checkpoint_carry_bench` example uses public embedded
`Database::checkpoint`, including sealed-root closure validation/publication,
and emits the same measurements with an explicit API label. Its typed runtime
configuration disables background maintenance so checkpoint work cannot move
into the untimed mutation phase. Durability stays at the default. Both harnesses
record mutation time separately, including synchronized commit. Keep low-level
GraphStore and embedded API timings separate.

Fixture construction, mutation and post-checkpoint verification are outside the
checkpoint timer. File bytes describe new graph artifacts, not filesystem block
allocation or the full durable write volume. Results must report exact source,
compiler/profile, workload and host limitations. This workload does not measure
foreground commit throughput, tail-latency acceptance or full LDBC/TPC-C.

Later stages replace canonical/spill blocks with copy-on-write and then update
local adjacency/projection blocks. They depend on these ownership and recovery
contracts but require separate implementation, validation and performance data.
