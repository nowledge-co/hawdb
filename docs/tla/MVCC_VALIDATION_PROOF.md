# Per-key MVCC validation: model and proof boundary

`HawDBMvccValidation.tla` supplements the older whole-epoch optimistic branch
of `HawDBTransactionConcurrency.tla`. It models the current graph version
validator, checkpoint/pressure integration of version-history pruning, and restart with no
surviving transactions. It does not claim that #231/#232 are complete.

## Independent specification

Each transaction captures epoch `E` and an immutable prefix of canonical
history. Each durable record has a read epoch, a nonempty write set, and a
deletion flag. Two records overlap if their write sets intersect or either
contains the database/schema barrier.

`ReferenceValid` rejects a transaction exactly when a durable record newer than
its snapshot overlaps its writes. This oracle scans full history; it does not
read the optimized version index. `IndexValid` instead matches
`GraphStore::validate_version_writes`: test all collected key stamps, test both
barrier stamps, and require no epoch drift for a broad writer.

`ValidationMatchesHistory` checks equivalence, not only implication, whenever
no publication is in progress. This checks both missed conflicts and false
version conflicts for disjoint writes. `FirstCommitterWins` independently
checks every durable record against all intervening overlapping records.

## Inductive argument

Assume complete key derivation, monotonically increasing epochs, serialized
validation/publication, and a pin set containing every snapshot that could
still serve or create a transaction at an older epoch.

- Initially there is no history, all stamps are absent, and both validators
  accept every write set.
- Beginning a transaction fixes its epoch and history prefix. Later commits
  only append history, so the saved prefix remains unchanged.
- Preparing a commit reserves the sole publisher. No second commit can
  interleave validation and publication. The sync step adds exactly one
  record; the publication step writes its epoch to every affected identity.
- For a narrow writer, a newer overlapping narrow record leaves a newer key
  stamp; a newer broad record leaves a newer barrier stamp. Either causes
  rejection. Conversely, any newer tested stamp witnesses an overlapping
  record. Unrelated narrow commits cannot affect those tests.
- For a broad writer, any intervening record overlaps, including records that
  never wrote a broad stamp. The explicit current-epoch check is therefore
  necessary in addition to checking barrier stamps.
- Any version stamp at `d` is removed only if every pinned epoch is strictly greater
  than `d`. It cannot witness a conflict for any surviving transaction, and
  transactions created later from an older source are protected by that
  source's own pin. Removing the stamp
  therefore preserves validator equivalence. Retaining at equality is the
  implementation's conservative boundary.
- Crash discards all active transactions and restores the durable history
  prefix. Every subsequent read epoch is at least the recovered epoch `R`;
  pre-crash stamps are all at most `R` and cannot affect a `stamp > E` test.
  Clearing them is safe only under that transaction-lifetime assumption.

`Sync` extends durable history only after successful validation, giving the
first-committer-wins invariant by induction. `Publish` requires that sync, so
visible history remains a durable prefix. A crash before sync discards the
candidate; a crash after sync restores it even if it was not acknowledged.
These are mathematical arguments about the abstract transitions. TLC adds
exhaustive checking of the configured finite instance, not an unbounded
machine-checked proof of the Rust implementation.

## Source mapping and abstractions

| Model | Source or explicit assumption |
| --- | --- |
| `IndexValid` | `crates/storage/src/store/graph_commit.rs`: `validate_version_writes`; `crates/storage/src/version.rs`: `first_conflict` |
| Stamp publication | `commit_prepared_mutation_ops` and `VersionIndex::apply` |
| Write sets | Assumed complete inputs corresponding to `collect_version_writes`; narrow identities abstract graph node/relationship/adjacency classes |
| Broad writes | Database/schema singleton sets; mixed sets containing a barrier collapse to that barrier for conflict semantics |
| `Begin`, snapshots | Transaction-private snapshot capture in `src/api/concurrent.rs` and `GraphStore::begin_mutation_transaction` |
| `Prepare`/`Sync`/`Publish` | Externally visible serialized commit boundary; not individual machine instructions or the internal group-sync schedule |
| `Prune` | `GraphStore::reclaim_version_history` uses the minimum registered storage-snapshot epoch; `VersionIndex::prune_before` keeps equality conservatively |
| Retained source | `GraphStore::snapshot` registers before the snapshot escapes; `begin_mutation_transaction` and savepoints capture further registered snapshots |
| `Crash` | Abstract canonical replay plus invalidation of all pre-crash handles; no byte-level WAL decoder, checkpoint or torn-tail model |

Two transaction slots and one retained source snapshot represent pin holders.
The source can create a later transaction at its old epoch. There is no
unbounded reader population, allocation/uniqueness semantics,
pessimistic lock acquisition, statement savepoint, page allocator, or byte
budget. Immutable prefixes abstract snapshots; actual row values and read sets
are not modeled. Keys come from a finite universe, so bounded model state is
not evidence of bounded production version memory. Group commit's internal
pre-fsync state is hidden behind the sequencer and is covered separately by
`HawDBWalGroupCommit.tla`; this model does not prove composition with it.

## Reproduction and negative controls

```sh
bazel test //docs/tla:HawDBMvccValidation_check
scripts/check-storage-tla.sh --check-mutants
```

The positive model is registered in `storage_models.bzl`. With two transaction
slots, two narrow keys, both barriers, one retained source snapshot, three commit
epochs and at most one restart, the model checks types, one publisher, stable
snapshots,
durable-before-visible, validator equivalence and first-committer-wins.
Deadlock checking is disabled because bounded completion is an expected
terminal state; no fairness, starvation or liveness theorem is claimed.

The mutant manifest runs seven independent defects and requires the named
invariant violation (a parse error or arbitrary nonzero exit is insufficient):

| Defect | Required failure |
| --- | --- |
| Skip commit validation | `FirstCommitterWins` |
| Omit epoch check for a broad writer | `ValidationMatchesHistory` |
| Omit newer barrier stamps for a narrow writer | `ValidationMatchesHistory` |
| Prune a version stamp despite older pins | `ValidationMatchesHistory` |
| Ignore a retained source snapshot that can create a later writer | `ValidationMatchesHistory` |
| Clear the index while transactions remain active | `ValidationMatchesHistory` |
| Publish before WAL sync | `DurableBeforeVisible` |

Reachability probes `NoDisjointWitness`, `NoRestartCommitWitness`,
`NoPruneWitness`, and `NoLivePruneWitness` deliberately assert that useful paths do not exist. To run one,
copy the positive `.cfg` to a temporary file, append `INVARIANT` followed by the
probe name, then run TLC on `HawDBMvccValidation.tla` with that configuration.
Each must fail with that specific probe, demonstrating respectively a stale
but disjoint successful commit, a successful post-restart commit, actual tombstone removal without a restart, and removal of a live stamp
without a restart. They are not positive-suite invariants or
production mutants. Logs must distinguish these expected witnesses from a
failure of a safety invariant.

## Writer pin ownership refinement

`DatabaseTransactionState::from_database` captures a `ReaderPin` using the same
registry and `PublishedReadView` as explicit read transactions. On the concurrent
path the database sequencer protects capture and workspace construction. On the
exclusive path the caller holds the database borrow. Thus the pinned epoch and
physical generation correspond to the captured workspace.

The state owns the pin rather than the submitting `ConcurrentDatabaseTransaction`:
`take_for_commit` moves the pin with the workspace into the queued task. Moving
an `Option<ReaderPin>` transfers one owner without running its destructor; the
emptied submitting state cannot release it. Rollback first discards durable
views and then takes/drops the pin. Normal destruction drops the pin field
last, after the workspace fields. Success and error returns drop the queued
state. If a first pessimistic statement refreshes its snapshot, construction of
the replacement state registers the new pin before assignment drops the old
state, while the sequencer excludes concurrent publication. These transitions
preserve at least one pin for each usable private workspace, and release that
ownership once it can no longer execute.

This maps a queued workspace to an active pin holder in the abstract model;
it does not model a separate queue scheduler. Physical generation eligibility
continues to use the existing registry and generation-reclamation protocol.
The four `writer_pin` regressions cover actual out-of-core files, transaction
exits (both modes plus optimistic conflict), first-statement snapshot refresh,
and transfer to a queued workspace. The out-of-core case originally failed
because checkpoint deleted `canonical.1.hawdb` while the writer still owned its
snapshot. It now retains that generation, reclaims unrelated generations 2 and
3, preserves private read-your-own-writes, and reclaims generation 1 after
rollback and the next checkpoint. This is a regression-backed ownership
argument, not a proof that all storage-internal transaction entry points share
the upper-layer registry. The storage-level registry described below protects
version validation independently.

## Storage snapshot watermark and checkpoint cleanup

`VersionSnapshotPins` is a shared epoch/count map. Every `GraphStore::snapshot`
owns a `VersionSnapshotPin`, so read views, transaction workspaces, savepoints,
checkpoint sources, and snapshots used to create later transactions all count.
The original live store has no self-pin. A descendant inherits the minimum of
its parent's pin epoch and workspace epoch: private statement commits may
advance the workspace clock, but must never raise the transaction's original
protection floor. Savepoint restore can therefore discard a parent without
losing the earlier pin. The abstract model keeps `readEpoch` fixed; the storage
savepoint regression checks this source-level refinement.

Registering a child occurs while the
parent still exists; dropping the parent cannot open a gap. A new snapshot of
the live store cannot race its mutable cleanup borrow. Snapshot drops can only
raise the minimum after cleanup reads it, making that observed minimum
conservative. Mutex serialization protects registration and count removal.

After successful durable checkpoint publication, and on the in-memory
checkpoint path, cleanup removes all version stamps strictly below the minimum.
With no snapshots it uses `commit_epoch.saturating_add(1)`; saturation at the
maximum epoch conservatively retains equal-epoch stamps. Failed checkpoint
publication never invokes cleanup. No stamp update enters the WAL or changes
canonical rows. Existing historical COW maps remain immutable.

The storage tests cover both durable and in-memory checkpoints, an old source
that outlives its initial writer, a later writer created from that source,
savepoint restore after multiple staged statements, exact watermark equality, a newer snapshot that does not block older deletion
cleanup, retention of live stamps at equality, and 32 unpinned create/delete/checkpoint
cycles. Disabling snapshot registration makes the retention test fail before
its conflicting writer can commit. A separate helper test verifies that a
cleanup with no eligible stamps does not detach shared COW pages.

Cleanup scans the index and may detach shared pages when entries are removed;
it is checkpoint maintenance, not an extra per-commit scan. This change does
not establish a global version-memory budget or bounded checkpoint latency.
Long-lived snapshots may retain conflict stamps and historical COW maps.
Reclaiming the live index does not bound the total memory owned by snapshots.
Full #231 resource qualification remains outstanding.

The live-history regression creates 24 records across three epochs, then
advances snapshot ownership and checks that the live index shrinks from 24 to
16 to 8 to zero. Historical snapshots retain their original metadata and rows.
A subsequent conflicting update is still rejected, and durable reopen restores
all 24 winning values. A helper regression covers live, deleted, database and
schema stamps, strict equality, shared-page preservation on a no-op cleanup,
and new conflict publication after pruning. The older public tombstone-only
helper retains its original semantics; checkpoint and budget pressure use `prune_before`.

## Direct mutation coverage

The direct graph/catalog/import helpers finish through
`GraphStore::finish_non_relational_commit`. They do not provide the normalized
write set used by `commit_prepared_mutation_ops`, so their already-durable
publication now also installs a live `Database` stamp at the new epoch.
This maps to a broad writer in the model: for any older transaction at `E`,
the newer barrier `V(Database) > E` rejects publication. A transaction started
after the direct commit has `E >= V(Database)` and is not rejected by that stamp.
The stamp is a fixed identity; it cannot exhaust the write-set entry limit
and introduces no new fallible validation after the durable mutation.

This is conservative coverage, not a claim of precise write identities for
those older helpers. Their direct data, schema and import changes may cause
unrelated older optimistic writers to retry. The canonical graph transaction
path continues to use its own collected keys. No-op or rejected direct calls
that do not complete a commit do not install a barrier.

`direct_legacy_commits_invalidate_stale_optimistic_workspaces` covers property,
catalog and delete commits in memory and on disk; it checks the retryable
conflict, unchanged WAL bytes/epoch on rejection, and the preserved winning
state after reopen. Before the barrier fix the property case let the stale
writer commit successfully. A separate regression confirms integer-overflow
rejection and a missing-label no-op leave the original transaction committable.
The existing model's newer-barrier negative control covers omission of the
stamp check; no model transitions change with this source mapping.

## Reopen regression coverage

`src/api/tests/concurrent_transactions.rs` connects part of the abstract restart
argument to the public embedded API:

- `optimistic_mvcc_reopen_preserves_disjoint_commits_and_same_key_conflicts`
  opens each of three persisted layouts (WAL, checkpoint, checkpoint plus WAL
  suffix), commits disjoint writes from one snapshot, checks that the other
  writer still reads the older value, then rejects a same-key writer with the
  expected typed conflict epochs. A second reopen must restore exactly the
  accepted rows and commit epoch.
- `optimistic_mvcc_reopen_preserves_both_database_barrier_directions` runs both
  graph-before-relational and relational-before-graph commit orders against
  each layout. The stale committer must report the database barrier conflict;
  reopening must contain only the winner's mutation.

Each rejection compares raw active WAL bytes and the commit epoch before and
after rejection. These tests exercise ordinary close/reopen, not process-kill
crashes or partial writes. They do not cover every canonical identity or schema
barrier. Temporarily removing `VersionIndex::apply` from the commit path makes
both tests fail because the stale writer incorrectly succeeds; this negative
control is not part of the committed production code.

Still required: source-level completeness of the collector, global version
resource bounds and representative reclamation qualification, canonical recovery tests across
actual WAL/checkpoint boundaries, and the #232 fairness/scaling qualification.

The [fixed-work writer benchmark](../CONCURRENT_WRITER_BENCHMARK.md) maps each
worker to a disjoint modulo partition of graph identities. Its complete-row and
reopen checks connect successful commits to canonical data for that workload.
This is measurement evidence in addition to the model, not a new fairness or
performance invariant. In particular, elapsed-time overlap and finite worker
completion cannot prove scheduler starvation freedom.

Optimistic commit admission now uses a dedicated lock mode compatible with
itself and incompatible with ordinary S/X owners. The serialized version
validator remains unchanged. See the [admission proof](OPTIMISTIC_COMMIT_ADMISSION_PROOF.md)
for ownership through the shared sync, intra-group conflict validation and
mixed-owner exclusion. Neither model establishes fair writer admission.

## Write-set byte admission

Let M map resident identities to dispositions, N be its entry limit, B its
estimated-byte limit, and w(k) be `VersionKey::cow_page_bytes()` plus the inline
`VersionWrite` size. The implemented invariant is:

    |M| <= N  and  C = sum(k in dom(M), w(k)) <= B.

Here C is `estimated_bytes`; the weight is the existing COW payload estimate,
not allocator-resident bytes. A key weight that cannot be added representably
is rejected. The invariant follows by induction on `record` calls:

1. Empty construction sets C = 0 and M empty, including zero limits.
2. If k is already resident, only its disposition changes. The resident key is
   preserved, so neither the domain nor any resident weight changes. Admission
   does not reject this replacement even when either budget is exactly full.
3. If k is new and the entry budget is full, return before mutation.
4. Otherwise checked additions compute C + w(k). Overflow or a result greater
   than B returns before mutation, preserving M and C exactly. A successful
   addition inserts k and then assigns the computed C. This preserves equality
   and both inequalities. Clone copies M and C, so it also preserves them.

The public methods cannot delete entries, mutate resident keys or change limits.
Thus these cases cover all transitions. This is a source-level mathematical
argument; the existing finite MVCC model does not model byte budgets. It need
not change: successful collections retain exactly the same complete footprint,
and rejected collections append no record. Commit proofs remain conditional on
successful admission. An error after earlier collector insertions discards the
local partial footprint rather than committing it as a complete write set.

Tests enumerate all 6^4 operation sequences over three identity types and two
dispositions at four budgets (5,184 cases). An independent index/disposition map
recomputes the weight sum after every call, including rejection. Boundary tests
cover a large text primary key, exact fit, one-byte-short rejection, duplicate
disposition changes at capacity, and equality of the complete map after failure.
A typed relational commit repeats a long table identity over otherwise small
rows, exceeding the default version budget while passing row/payload admission;
it rejects without advancing the commit epoch, stamps or canonical rows. Removing
the byte guard makes this integration test fail (0 passed / 1 failed); the probe
was restored before final validation.

This bounds one retained write-set estimate, not allocations while constructing
an incoming key, B-tree/allocator overhead, spare capacities, all concurrent
write sets, or retained canonical/snapshot version pages. #231's total version
memory acceptance remains open. The additional accounting cost is not a
single-stream latency qualification.

## Current-index budget and reserved barrier

Let D denote the Database identity, r = weight(D), and H the current stamp map.
The accounting invariant is:

    C(H) = r + sum(k in dom(H) \ {D}, weight(k)).

Weights use the COW key estimate plus `size_of::<VersionStamp>()`. The r term
is charged even when D is absent. The production limit L defaults to 64 MiB.
For a deduplicated write set W, admission computes

    Cnext = C(H) + sum(k in W \ (dom(H) union {D}), weight(k)).

Every addition is checked; overflow or Cnext > L rejects before WAL. Initially
H is empty and C = r <= L. Assuming a previously admitted production state:

- Replacing an existing stamp only changes epoch/disposition, not its weight.
- Publishing a newly admitted non-D identity adds exactly its preflight weight.
- Publishing D adds nothing to C, including the legacy direct-commit path that
  does not carry a typed write set. It cannot exceed the prepaid reservation.
- Hence applying W after successful admission produces exactly Cnext <= L.
  Serialized validation/WAL/publication excludes an intervening canonical
  insertion between admission and application, including grouped callbacks.
- Safe pruning deletes a subset of stamps and recounts retained non-D weights;
  C cannot increase. Removing D leaves r reserved. A clone copies the map root,
  counter and limit; subsequent COW mutation leaves the clone's invariant intact.

Budget-pressure pruning uses precisely the same oldest-snapshot watermark as
checkpoint pruning, so the existing validator-equivalence proof still applies.
If the retried footprint does not fit, neither WAL nor canonical user data
changes. The index may have safely retired obsolete history during the failed
attempt; that is not a user-data commit. Existing identities remain updateable
at capacity when their complete footprint introduces no additional identities.
No live snapshot or required conflict stamp is discarded for budget recovery.

This argument covers the production GraphStore call graph. Low-level public
VersionIndex construction/publication utilities can build an unadmitted map;
they do not establish the production bound. Tests configure a small internal
limit to reach exact capacity, preserve old snapshots, check pre-WAL refusal
byte-for-byte, exercise a legacy barrier at capacity, then reclaim and resume.
They also exercise automatic pressure reclamation without checkpointing every
commit, memory/durable storage, and exact post-reopen row counts/epochs. Removing the
pre-WAL index admission makes the growth regression fail (0 passed / 1 failed);
the guard is restored before final checks.
Unit tests cover live-to-tombstone replacement, equality at the prune boundary,
reclaimed charge reuse and a retained COW snapshot's independent counter.

The original MVCC model abstracts resource admission: a refused commit adds no
history, and safe Prune remains an allowed transition. The separate budget model
below checks accounting and validation under pressure. This source-level
induction establishes a current-root estimate, not the sum of all retained roots
or real allocation peaks. Global
version memory, sustained pressure cost and single-stream latency qualification
remain #231 acceptance work. The pressure path can scan/recount the index and
detach shared COW pages; that cost is not hidden by the constant-time counter.

### Finite budget model and negative controls

`HawDBVersionHistoryBudget.tla` uses three narrow identities (weights 1, 2, 1),
a Database identity of weight 1, budget 3 and at most four commits. One pinned
epoch abstracts the oldest retained snapshot. Committers may use that epoch or
the current epoch, so newer writers can continue while an old reader remains.
Validation is checked for **every epoch between the oldest pin and current
state**, against independent full commit history. This is stronger than checking
only the next selected writer. With no pin, current-epoch validation is checked.

`Commit` validates before pressure reclamation, preflights the resulting full
footprint and atomically appends history/publishes stamps only if it fits.
`Refuse` may reclaim safe obsolete stamps but leaves history unchanged. `Prune`
represents checkpoint reclamation. `Legacy` appends a Database-barrier commit
without resource admission, exercising the prepaid identity directly. An
independent sum of resident key weights checks the tracked charge, including
refund after pruning. Actual resident weight must not exceed charged weight,
and charged weight must not exceed the budget.

The final positive check explored **158,921 generated / 25,916 distinct states**,
depth 10, with an empty queue. Three separate false-invariant probes reach
budget refusal, legacy commit at capacity and successful pressure reclamation
(`NoRefusalWitness`, `NoLegacyAtCapacityWitness`, `NoPressureReclaimWitness`).
They demonstrate reachable paths; they are not liveness/fairness theorems.

Four registered mutants must violate the named independent property:

| Mutation | Required violation |
| --- | --- |
| Skip the admission limit | `BoundedCurrentPayload` |
| Prune stamps still needed by the oldest pin | `ValidationMatchesHistory` |
| Fail to refund pruned weights | `ExactAccounting` |
| Remove the Database reservation, leaving legacy publication unguarded | `BoundedCurrentPayload` |

Reproduce the positive and negative checks with:

```sh
bazel test //docs/tla:HawDBVersionHistoryBudget_check
scripts/check-storage-tla.sh --check-mutants
```

For a reachability probe, copy the positive `.cfg`, append `INVARIANT` followed
by the relevant witness name, and run TLC against the same module. Require the
named invariant violation, not an arbitrary nonzero exit. All three probes and
all four mutants were checked on the final model; the complete mutant manifest
now checks 30 named violations.

This model collapses serialized commit/WAL publication into one transition.
It does not model failed fsync, byte-level replay, historic COW allocations,
allocator overhead, more than one independent pin holder, integer overflow or
preemption inside a commit. The older MVCC and group-admission models retain
those separate boundaries where applicable; this is not their machine-checked
composition. Production checked arithmetic and the inductive argument cover
representable budgets. Finite weighted identities are not a proof of total
process memory bounds. No runtime algorithm changes accompany this model.
