# Carrying an unchanged graph bundle

Issue: https://github.com/nowledge-co/hawdb/issues/1641.

A checkpoint has a physical generation C and a logical source commit. Its
canonical graph, property spill, adjacency and property projection form one
immutable bundle G/E. All four retain their existing filenames, source epoch,
descriptor roots, lengths, CRCs and SHA-256 identities when unchanged. V2
manifests bind G/E explicitly. Legacy v1 manifests resolve them to the original
checkpoint generation/commit. V2 is emitted even when G=C; the version line
precedes new fields so old readers refuse before interpreting unknown fields.
Published v2 manifests require non-`none` values for both G and E, including
G=C. Only an initial manifest without a checkpoint may omit their values.
Payload codecs and immutable-object reference domains are unchanged.

## Eligibility and publication

`DurableStore::reusable_graph_bundle` requires a published checkpoint, no graph
change since that checkpoint, an identical full graph Catalog snapshot, all
four validated metadata bindings and admitted graph readers, and default
graph builder configuration. `WalOp::changes_graph` covers node, relationship,
property, graph schema and import mutations. Direct Catalog edits are caught
by equality even if they do not advance the WAL epoch. Recovery captures the
checkpoint Catalog before replay; graph replay advances the mutation fence.
Reopen conservatively starts that fence at the checkpoint source commit, so
eligibility compares it to the latest checkpoint commit rather than old E.

Checkpoint preparation retains G/E while producing fresh non-graph artifacts
at C. It skips canonical/spill serialization, adjacency iteration, property
projection definition/record enumeration and their writers. Statistics retain
their existing graph samples with an updated basic commit. Publication still
validates the complete closure and applies the existing synchronization and
head-selector barriers. An unchanged graph does not mean no graph reads:
closure hashing and descriptor traversal remain, and projected graph/source
sidecar preparation has its own costs. No constant-time checkpoint claim is
made. This change does not remove the full relational row checkpoint envelope.

Candidate replay can add graph changes after base capture. Those changes stay
in the candidate WAL suffix above G/E; a later checkpoint sees their mutation
fence and builds a new bundle. Dropping an unselected candidate removes only
its newly staged generation, never the carried files. Adoption transfers the
preceding graph root before reclamation.

## Readers, reclamation and maintenance

`PublishedReadView` captures C and G from the live durable store before making
a detached snapshot. `PhysicalGenerationPins` keeps checkpoint and graph sets
separate. `PublishedReadView::physical_generation()` exposes C and
`graph_generation()` exposes G in both residency modes. The typed pin sets stay
separate; hidden low-level Rust publication/reclamation methods now take this
typed set rather than a bare set of checkpoint integers. Callers construct it
from their actual published views. Low-level owners can separately declare an
explicit branch-private WAL with `with_branch_wal_generation`; that declaration
retains only that WAL family and acquires no lease or descriptor. Public host transaction APIs retain pins
automatically. Retaining G never retains an unrelated old row/WAL family G.

Generation cleanup protects current G, preceding G and reader-owned G; a
candidate retains its source through the existing checkpoint owner. Branch
immutable-object closure references include the original graph paths. Backup
and scrub resolve graph filenames/source epochs through G/E, while checkpoint
payload/WAL checks continue using C. Repair plans bind the source manifest
identity; quarantine audit derives old G from those checksum-bound bytes even
after the new head is published. Missing derived readers prevent reuse during
repair, producing a complete new graph bundle at the repair generation.

## Executable model and regression owners

`HawDBGraphBundleCarry` separates physical base C/G/E/source coverage from
candidate replay tip, live graph state and each reader's captured visible tip.
The four-generation/three-commit instance exercises a nonempty graph, relational
carry, a later graph WAL suffix, catch-up/synchronization/selection, the next
forced graph rebuild, reader delta ownership and independent graph/row cleanup.
The first checkpoint must allocate G>0; reopening can conservatively raise the
graph fence to physical base coverage without changing old E. A reachability
control requires selecting a carried nonempty base with a newer graph suffix;
negative controls reject omitted suffix application, a lost graph fence,
a lost reader delta, dirty reuse and missing current/reader G retention.

See the [invariant argument and Rust mapping](GRAPH_BUNDLE_CARRY_PROOF.md).
These are complementary bounded safety checks. There is no mechanically
proved composition or Rust refinement: `HawDBAutomaticCheckpoint` covers
transaction/WAL-prefix completeness, selection barriers and crash choices;
`HawDBImmutableRootBindings` covers every required physical path despite
shared references; the carry model covers G and logical-tip interaction.
Hash validation, concrete byte decoding, filesystem behavior, allocation
ownership, liveness and performance require separate source/runtime evidence.

Storage regressions also checkpoint after reopening a carried bundle and track
projection debt in both live and detached views: a bundle valid through the
checkpoint has no graph debt after relational-only commits or reopen, while
new graph mutations report debt until rebuilt.

Storage regressions cover complete graph-file names/bytes (and Unix inode
identity), repeated relational writes/DDL, backup/restore, scrub, direct Catalog
mutation, WAL replay and candidate cancellation/catch-up. Root API regressions
hold a real out-of-core read transaction with C!=G across four checkpoints and
a graph rewrite, then release/reclaim it; interrupted repair resumes on either
side of head publication. Physical recorder images exercise head publication
and acknowledgement under both durability policies with lost, torn and
reordered unsynchronized operations. These models do not qualify physical
devices, APFS extent accounting, Windows namespace behavior or SF1 throughput.

Run the ordinary repository Cargo lint/checks, the applicable storage/root
Bazel owners, the full storage model/evidence suite and its separate mutant
gate, and the mandatory local Shiro/fuzz selection. Record actual outcomes and
source revisions in the PR; commands listed here are not execution evidence.
