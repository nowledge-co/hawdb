# Per-key MVCC validation: model and proof boundary

`HawDBMvccValidation.tla` supplements the older whole-epoch optimistic branch
of `HawDBTransactionConcurrency.tla`. It models the current graph version
validator, a proposed safe integration of the existing tombstone-pruning
helper, and restart with no surviving transactions. It does not claim that
production pruning is already integrated or that #231/#232 are complete.

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
validation/publication, and a pin set containing every transaction that could
still use an older snapshot.

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
- A tombstone at `d` is removed only if every pinned epoch is strictly greater
  than `d`. It cannot witness a conflict for any surviving transaction, and
  new transactions begin at an epoch at least as large. Removing the stamp
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
| `Prune` | `VersionIndex::prune_tombstones_before` with all active writer pins included; production watermark integration remains future work |
| `Crash` | Abstract canonical replay plus invalidation of all pre-crash handles; no byte-level WAL decoder, checkpoint or torn-tail model |

Two transaction slots represent pin holders that can also write. There is no
separate read-only reader population, allocation/uniqueness semantics,
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
slots, two narrow keys, both barriers, three commit epochs and at most one
restart, the model checks types, one publisher, stable snapshots,
durable-before-visible, validator equivalence and first-committer-wins.
Deadlock checking is disabled because bounded completion is an expected
terminal state; no fairness, starvation or liveness theorem is claimed.

The mutant manifest runs six independent defects and requires the named
invariant violation (a parse error or arbitrary nonzero exit is insufficient):

| Defect | Required failure |
| --- | --- |
| Skip commit validation | `FirstCommitterWins` |
| Omit epoch check for a broad writer | `ValidationMatchesHistory` |
| Omit newer barrier stamps for a narrow writer | `ValidationMatchesHistory` |
| Prune a tombstone despite older pins | `ValidationMatchesHistory` |
| Clear the index while transactions remain active | `ValidationMatchesHistory` |
| Publish before WAL sync | `DurableBeforeVisible` |

Reachability probes `NoDisjointWitness`, `NoRestartCommitWitness`, and
`NoPruneWitness` deliberately assert that useful paths do not exist. To run one,
copy the positive `.cfg` to a temporary file, append `INVARIANT` followed by the
probe name, then run TLC on `HawDBMvccValidation.tla` with that configuration.
Each must fail with that specific probe, demonstrating respectively a stale
but disjoint successful commit, a successful post-restart commit, and actual
tombstone removal without a restart. They are not positive-suite invariants or
production mutants. Logs must distinguish these expected witnesses from a
failure of a safety invariant.

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

Still required: source-level completeness of the collector, production writer
watermark integration and bounded reclamation, canonical recovery tests across
actual WAL/checkpoint boundaries, and the #232 fairness/scaling qualification.
