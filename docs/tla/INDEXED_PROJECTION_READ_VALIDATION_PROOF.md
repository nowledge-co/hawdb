# Indexed projection reads: collector argument and finite boundary

`HawDBIndexedProjectionReadValidation` specifies the capture-to-read lifetime of
exact scalar-index dependencies. It supplements the publication fence and source
durability models, rather than replacing their obligations.

## Independent specification

The source has two row slots with an indexed value and a non-key payload. A row
may be absent or carry an oversized/unqualified value. Capture fixes the source
epoch and immutable rows before the query/value is chosen. Queries include an
exact point, two-key membership, disjunction, whole-domain scan and an outer
point filter after a bounded rank-selection phase.

The optimized index records representable old/new keys only while a preparation
is registered. An unpruned independent oracle records complete before/after
identities and payload changes. Validation must agree with that oracle on every
accepted dependency and with the actual query output over the captured/live rows.
The oracle never consults the prunable index or its fault flags.

## Conditional collector argument

Assume canonical ownership from capture/pin registration through snapshot capture,
complete finalized graph operations, exact canonical Value equality, immutable
captured schema plus existing schema/database barriers, and retention of every
stamp newer than the oldest surviving capture.

- Start summary tracking at capture, before its owner is released. Choosing a
  property/value later during preparation must not leave an untracked interval.
  Writers cover every declared equality/range index while any preparation lives;
  the registry is not populated lazily by the first query.
- Every node create stamps its indexed keys. Every existing node update/deletion
  stamps all old indexed keys, including when only a non-index property changes.
  An indexed assignment additionally stamps its new key. Therefore entering,
  leaving, changing or deleting any matching result leaves a newer relevant stamp.
  Same-batch creates plus later assignments cover their initial and assigned keys;
  repeated assignments preserve the relevant old/final identities without exposing
  partial canonical mutations.
- Readers qualify exact keys only for declared scalar indexes. Point and multi-key
  access, or a necessary EQ/IN predicate under only AND ancestors on one complete
  node candidate source, supplies a complete finite key set. This does not depend
  on whether the cost optimizer chooses a seek or a scan.
- An OR/NOT branch is not a necessary key. Nor can an outer condition move across
  LIMIT, TopN, aggregation or projection bindings: another row may change which
  candidate survives before that condition. Those shapes keep a domain dependency
  or fail closed until their complete collector is available.
- The same bounded helper constructs reader/writer keys using canonical Value
  equality. Compound or oversized values use the always-maintained domain at read;
  skipping their exact write key is safe. Unknown/over-budget old metadata instead
  leaves a read barrier, which every exact/domain consumer checks.
- Destination write intents are validated before read summaries are appended to
  the admitted staged index. Thus two stale disjoint writers do not conflict on
  shared read metadata. Unrelated known scalar keys do not invalidate an exact read.

These are source-level mathematical arguments about the qualified paths, not a
machine-checked Rust refinement proof or proof of every query family.

## Native source and bounds

| Obligation | Source |
| --- | --- |
| Key identity and paired representability | `version.rs::node_property_read_key`; boxed scalar Values, maximum 4096 estimated key bytes, shared by readers and writers |
| Capture activation and pin | `GraphStore::capture_read_dependencies`; older ordinary snapshots cannot start tracking |
| Complete old/new keys | `store/read_dependencies/indexed_nodes.rs` and finalized WAL-operation walk in `read_dependencies.rs` |
| Cold preimage | Admitted projected point read of declared indexed properties; no full body ownership requested |
| Bounded fallback | At most 1024 examined descriptors, 64 KiB estimated property-name metadata and 64 KiB admitted projected ownership; refusal leaves `ReadBarrier` |
| Plan/predicate qualification | `api/dependency_read.rs`; schema check, local binding and necessary EQ/IN traversal through AND only |
| Observable precision | `PreparedReadDependencies::exact_node_property_count`, `node_domain_count`, and whole-database fallback flag |
| Write independence and admission | `graph_commit.rs`: destination validation before read invalidation, complete union before WAL |

These are bounded component estimates/ownership limits, not global RSS or a
wall-clock I/O bound. Physical metadata read failures propagate and poison the
source as appropriate; they are not converted into successful partial metadata.

## Local evidence

The finite configuration uses two rows and `MaxEpoch = 2`. Main TLC passed
**167,251 generated / 74,969 distinct states**, depth 9, using pinned tools 1.7.4
(SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`)
and Azul Java 21.0.9 on macOS arm64.

All nine fault controls returned TLC status 12 and `NoStaleAcceptance`:
`LateTracking`, `SkipOldKey`, `SkipNewKey`, `SkipPayloadInvalidation`,
`IgnoreReadBarrier`, `TrustUnindexedPlan`, `NarrowDisjunction`,
`NarrowBoundedPhase`, and `PrunePinned`. `DisjointWitness` returned status 12 and
its intentionally false `NoDisjointAcceptance`, establishing a reachable successful
exact validation after another source commit. Syntax/runtime failures do not
qualify as controls. Configurations and expectations are in `mutants/` and its
manifest. The registered safety target is
`//docs/tla:HawDBIndexedProjectionReadValidation_check`.

Native default/minimal consumers passed 20 tests. They include same-label disjoint
writes before the first prepared query; payload changes in that same interval;
insert/delete/move-in/move-out; absent and present IN keys; disjunction and bounded
pipeline refusal; cumulative statement budgets; and forced out-of-core nodes with
1 MiB bodies. Twenty storage tests cover the shared kernel plus stale disjoint
writers, old/final batch keys, ABA/pruning, descriptor-limit barriers and known
oversized values retaining domain protection.

The model has one capture, a static schema and abstract completed commits. It does
not prove physical codecs, arbitrary composite/range/relational/adjacency predicates,
multiple concurrent preparations, allocator RSS, fairness or end-to-end Search
activation. Its bounded rank query explains a qualification boundary; the current
public consumer rejects the unqualified GraphMatch pipeline rather than claiming
that complete path is already supported. The full #231/#232 ledger remains open.
