# Per-key MVCC validation: model and proof boundary

`HawDBMvccValidation.tla` supplements the older whole-epoch optimistic branch
of `HawDBTransactionConcurrency.tla`. It models the current graph version
validator, the checkpoint integration of version-history pruning, and restart with no
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
helper retains its original semantics; checkpoint uses `prune_before`.

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
