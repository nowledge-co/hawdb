# Skein CRDT Replication Specification

## Implementation Status

This is a design-stage contract with a machine-checked model and **no
implemented surface in the Skein crates yet**. Nothing here describes shipped
behavior, and the single-process embedded scope in `TODO.md` still stands: a
replication layer is a product decision, not an implied backlog item. The
document exists so that decision can be made against a verified design, and
it is the layer the deferred replica-assisted repair task depends on.

## Scope

This specification defines the convergent replication contract that lets two
Skein nodes synchronize the canonical property graph without a coordinator.
The design is a delta-state CRDT layered above the existing single-node
transactional engine: local transactions keep their existing semantics, and
replication ships the committed effects of those transactions.

The pairwise (two-node) deployment is the first supported topology. Every
normative clause is written so that the same state, join, and delivery rules
extend to N replicas without a format change.

### Non-Goals

- Consensus, leader election, or synchronous quorum commit.
- Cross-replica ACID transactions or global serializability.
- Replicating derived state. Full-text, vector, analytics, and plan-cache
  artifacts remain rebuildable local projections and MUST NOT be shipped.
- Replacing PostgreSQL as the Nowledge Cloud canonical store.
- Tolerating Byzantine or actively malicious peers. Peers are trusted after
  transport authentication; corruption defense is checksum-and-fail-closed.

## Replica and Operation Identity

- Each replica persists a stable `ReplicaId` minted once at replica
  initialization and recorded in the durable manifest. A copied or restored
  database directory MUST NOT reuse the source `ReplicaId` for new writes
  without an explicit re-identity step; two writers sharing a `ReplicaId`
  is a fork and MUST fail closed when detected (see Fail-Closed Conditions).
- Every replicated committed operation mints a **dot** `(ReplicaId, counter)`.
  Counters are contiguous per replica starting at 1 and are assigned at commit
  publication, in commit order, alongside the WAL LSN.
- A **causal context** is a version vector `VV: ReplicaId -> counter`. Because
  per-replica counters are contiguous and deltas are delivered as per-origin
  gap-free prefixes, the context stays a compact vector; no dot cloud is
  required.
- **Occurrence identity.** A `CREATE`/first-`MERGE` mints a fresh occurrence
  dot for the created node or relationship. Occurrence identity MUST NOT be
  derived from a payload hash or from the logical key. A delete, recreate,
  and re-delete of the same logical entity therefore produces three distinct,
  durable, totally-ordered-per-origin identities, and stale-sequence
  regressions are detectable by counter comparison.

## Replicated State

The replicated object is the canonical graph only. Its CRDT state per replica
is:

- `context`: the replica's causal context (a version vector).
- `nodes`: a set of live **node occurrences**
  `[key: (label, primary key), dot, props]` with at most one record per dot.
- `edges`: a set of live **edge occurrences**
  `[dot, type, src: node dot, dst: node dot, props]`.
- `props`: per occurrence, a map from property name to an LWW register
  `(value, ts)` where `ts = (hlc, ReplicaId)` and `hlc` is a hybrid logical
  clock timestamp maintained per replica. The clock MUST tick on every local
  commit and MUST merge past every timestamp observed in an applied delta
  batch (receive-max), so that a causally-later write always carries a
  greater `ts` than any register it overwrites. A raw per-replica commit
  counter is **not** a valid `hlc`: it lets a replica overwrite a register
  it has already seen with a smaller timestamp, and the stale value then
  wins the join (this failure is reproduced by the TLA+ model when the
  clock merge is removed).

Removal is represented **without tombstones** (ORSWOT style): a dot that is
covered by `context` but absent from `nodes`/`edges` is removed. Storage for
removed occurrences is reclaimed immediately; only the vector survives.

### Read Projection

Cypher reads see a deterministic projection of CRDT state:

- A logical node `(label, key)` is **visible** iff it has at least one live
  occurrence. Concurrent creates on both replicas may leave multiple live
  occurrences for one logical node; `MATCH` MUST return one logical row, and
  each property MUST resolve to the register with the greatest `ts` across
  the live occurrences.
- An edge occurrence is **visible** iff its own dot is live **and both
  endpoint occurrence dots are live**. An edge whose endpoint was removed is
  *masked*: it is retained in CRDT state for convergence but MUST NOT be
  returned by any read, traversal, index, or projection.

Referential integrity of the visible graph therefore holds by construction:
no visible edge can reference an invisible node.

## Operation Semantics

Each committed local mutation becomes one CRDT delta, identified by its dot:

| Cypher operation | CRDT delta |
| --- | --- |
| `CREATE` node / rel | Add a fresh occurrence dot (with initial property registers). |
| `MERGE` (no visible match) | Same as `CREATE`. |
| `MERGE` (visible match) | Bind to the existing live occurrences; no new occurrence. |
| `SET` / `REMOVE` property | Write LWW registers (fresh `ts`) on every locally live occurrence of the matched entity. |
| `DELETE` edge | Observed-remove of the locally live edge dots. |
| `DELETE` node | Observed-remove of the locally live node occurrence dots; MUST fail locally if a visible incident edge exists (existing Cypher rule). |
| `DETACH DELETE` node | Observed-remove of the locally live node occurrence dots and of every locally live incident edge dot, atomically in one delta. |

"Observed-remove" means the delta removes exactly the dots covered by the
local context at commit time. A remove never affects occurrences the replica
has not yet seen.

### Convergent Conflict Matrix

Concurrent (causally unordered) operations resolve deterministically:

| Conflict | Outcome |
| --- | --- |
| `CREATE` vs `CREATE` (same logical key) | Both occurrences survive; the read projection merges them into one logical entity. **Add-wins.** |
| `CREATE` vs `DELETE` (same logical key) | The delete removes only observed occurrences; the concurrent fresh occurrence survives. **Add-wins** for the logical entity. |
| `SET` vs `DELETE` of the same occurrence | The occurrence dies; the concurrent property write is discarded with it. **Remove-wins per occurrence.** |
| `SET` vs `SET` on the same property | Greater `(hlc, ReplicaId)` wins. Unique because `hlc` values are per-replica monotone and ties break on `ReplicaId`. |
| `DETACH DELETE` node vs concurrent incident edge `CREATE` | The edge occurrence survives in state but is **masked** (endpoint dot removed). The visible graph shows neither the node nor the edge. |
| `DELETE` vs `DELETE` of the same occurrence | Idempotent; the occurrence is removed once. |

A recreated logical entity starts from fresh occurrence dots with fresh
property registers; property values of removed occurrences MUST NOT
resurrect through recreation.

## Anti-Entropy Synchronization Protocol

Synchronization is pull-based, symmetric, and runs over an authenticated
transport session between the two replicas.

1. **Digest.** The requester sends its persisted `context`.
2. **Delta response.** The responder replies with its own `context` plus, for
   each origin replica, the gap-free segment of op deltas whose counters lie
   above the requester's context entry, in counter order. The responder MUST
   NOT ship a segment with holes; if requested history has been compacted
   away it MUST ship its full joined state for the affected origin instead
   (state transfer degenerate case).
3. **Join.** The receiver applies the batch as one local transaction through
   the normal WAL and group-commit path, using the join rules:
   - A local live record whose dot is covered by the sender's context and
     absent from the sender's shipped state is removed.
   - A shipped record whose dot is covered by the receiver's context and
     locally absent is discarded (it was removed here).
   - A record present on both sides merges property registers per-key by
     greatest `ts`.
   - `context := pointwise-max(context, sender context)`.
4. **Acknowledgment.** Only after the WAL sync boundary makes the joined
   batch durable does the receiver acknowledge its new `context` to the peer.
   The responder MAY use acknowledged peer contexts for delta retention and
   masked-edge GC, and MUST NOT use unacknowledged ones.

Delivery obligations:

- **Per-origin FIFO prefixes.** Together with join closure this guarantees
  causal delivery: a shipped edge occurrence's endpoint dots are always
  covered by the receiver's context once the batch is applied. A batch that
  would violate this MUST be rejected before apply.
- **Idempotent retry.** The join is idempotent, commutative, and
  associative. After a crash, a lost acknowledgment, or a duplicated
  response, the receiver simply re-runs a round from its persisted context.
- **No epoch skew.** Both replicas MUST run the same schema fingerprint; a
  delta batch carries the fingerprint and MUST fail closed on mismatch.

### Masked-Edge Reclamation

A live-but-masked edge occurrence (endpoint removed) MAY be physically
dropped once the removing operation's dot is covered by every peer's
**acknowledged** context (the removal is stable), and MUST NOT be dropped
before then. Dropping it earlier could resurrect the edge as visible if the
peer independently re-delivered it alongside a surviving endpoint.

## Durability and Recovery Integration

- Remote deltas are ordinary WAL batches: they receive contiguous local
  LSNs, participate in group commit, and obey the whole-batch-or-nothing
  replay rule of the storage durability contract.
- The replica's own `context`, the peer's last acknowledged `context`, and
  the `ReplicaId` are persisted in the checkpoint manifest and recovered
  before the first post-restart round.
- Recovery replays WAL, reconstructs `context`, and resumes anti-entropy
  from persisted state. An ambiguous crash between apply and acknowledgment
  is resolved by retry; idempotent join makes re-application safe.

### Fail-Closed Conditions

The session MUST terminate and refuse further replication, surfacing a typed
error, when it observes:

- a peer dot counter regression or a re-used dot with different content
  (fork / restored-backup detection);
- a delta segment with a per-origin gap that the responder did not declare
  as a state transfer;
- a schema fingerprint mismatch;
- a checksum failure in a delta frame (quarantine per the storage spec).

## Consistency Boundary

- Per replica, transactions keep their existing strict local guarantees;
  replication never makes uncommitted or non-durable remote state visible.
- Across replicas the guarantee is **strong eventual consistency**: replicas
  that have applied the same set of deltas have identical CRDT state, and
  under continued anti-entropy all replicas converge. There is no global
  total order and no cross-replica uniqueness enforcement; DDL constraints
  that require global uniqueness beyond logical-key merge semantics MUST be
  rejected while replication is enabled.
- DDL replication is out of scope for this revision: schema changes deploy
  out-of-band to both replicas and are guarded by the fingerprint check.

## Verification

`docs/tla/SkeinCrdtReplication.tla` is the executable model of this
contract, checked by `scripts/check-storage-tla.sh`. The model abstracts
delta segments as state joins (their semantic foundation) and checks:

| Contract obligation | Model invariant / property |
| --- | --- |
| Equal delivered contexts imply identical state | `ConvergedWhenContextsEqual` |
| Continued anti-entropy converges both replicas | `EventualConvergence` under sync fairness |
| Every live record and property timestamp is causally covered | `LiveRecordsAreCovered` |
| A received edge never references an unknown occurrence | `EdgeEndpointsCovered` |
| Observed-removed dots never resurrect | `RemovedDotsStayRemoved` |
| Occurrence dots are unique and contexts never exceed minted ops | `DotsAreUnique`, `ContextBoundsMintedDots` |
| Visible-graph referential integrity | By construction via `VisibleEdges`; masking is exercised by the `DETACH DELETE` vs concurrent edge-create interleavings |
| An acknowledged peer context never runs ahead of what that peer applied | `AcknowledgedContextNeverExceedsPeer` |
| A crash between a durable apply and its acknowledgement is safe to retry | `Crash` drops only the owed acknowledgement; the idempotent join keeps `ConvergedWhenContextsEqual` |

The model abstracts the hybrid logical clock as a Lamport clock that ticks
on every mint and merges on every sync round, which is exactly the
causality obligation stated above; physical-time quality is outside the
model. It does not check transport security, delta-segment encoding, or
the WAL durability boundary; the last is covered by
`SkeinStorageDurability.tla`.

### Why Masked-Edge Reclamation Is Not Verified Here

Masked-edge GC is deliberately absent from the model rather than merely
unchecked, because adding it at this abstraction would produce false
confidence.

The model represents anti-entropy as a **state join**: every round carries
the sender's full state and context. Under that abstraction, physically
dropping a masked edge is indistinguishable from an ordinary
observed-remove — the receiver sees a dot covered by the sender's context
with no accompanying record and removes it, so convergence holds whether
or not the drop waited for peer acknowledgment. A GC action added here
would therefore pass with the stability guard *and* without it, proving
nothing about the guard.

The hazard the guard exists for lives one level down, in the delta-segment
refinement. A real delta segment carries only the per-origin counter
ranges the peer has not seen. A replica that forgets a masked edge without
retaining a removal the segment can carry has no way to tell a lagging
peer that the edge is gone, and that peer keeps it live. Verifying the
guard therefore requires modeling segments as segments, with their own
retention state — a separate model, not an action bolted onto this one.

Until that model exists, the `MUST NOT` in *Masked-Edge Reclamation* rests
on review and implementation tests, not on machine checking.

### Instance Bound

The checked instance is two replicas, one key, two values, and two
operations per replica, sized by mutation testing rather than by state
count. Three-replica instances were
attempted and are not exhaustively checkable at this shape: tracking each
replica's view of every peer's acknowledged context adds a version vector
per ordered pair, and the smallest three-replica configuration still
exceeded seven million distinct states without terminating. Transitive
delivery — where a fault would show as `a` and `c` diverging while each
agrees with `b` — is consequently argued from the join's commutativity and
associativity, not machine-checked. The spec's claim that the rules extend
to N replicas without a format change is a design claim, not a verified
one.
