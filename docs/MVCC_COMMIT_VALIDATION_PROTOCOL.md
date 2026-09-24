# MVCC Commit Validation Protocol

Status: partial implementation for #231 and #232, audited at `a703cc0f`.
This document separates the current protocol from the remaining acceptance
work. It does not declare either issue complete.

## Current implementation and boundaries

HawDB has immutable COW snapshots, transaction-private workspaces, one
process-local database owner, and one serialized WAL publication stream.
`ConcurrentTransaction` executes statements against its private state outside
`CommitSequencer`; commit enters the sequencer. Concurrent statement execution
is already implemented, not contingent on implementing a new version index.

Optimistic graph commits use per-key first-committer-wins validation.
Pessimistic commits can rebase under their acquired locks. The restricted
optimistic conflict-noop SQL path can also rebase; it recomputes mutation
outcomes at commit. These are distinct protocols: permitting either rebase
path does not demonstrate per-key optimistic SQL validation.

| Mechanism | Current source and scope |
| --- | --- |
| Version storage | `crates/storage/src/version.rs`: `VersionIndex` is a COW map containing the latest epoch and live/tombstone disposition for each recorded identity. |
| Write-set collection | `crates/storage/src/store/graph_commit.rs`: `collect_version_writes` derives an ordered, deduplicated set from the final canonical operations, before WAL append. |
| Optimistic validation | `validate_version_writes` checks recorded keys plus database/schema barriers against the transaction's `base_commit_epoch`. Conflicts become retryable `HawDBError::TransactionConflict`. |
| Private statement execution | `src/api/concurrent.rs`: statements operate on transaction-owned state; `commit_with_result` submits publication to the sequencer. |
| Rebase selection | `commit_with_result` enables it for pessimistic transactions and the narrowly checked conflict-noop-only optimistic transaction shape. |
| Publication and group sync | `src/api/concurrent/coordinator.rs` serializes commit tasks and retains the database mutex through the shared durability barrier. |
| Reclamation helpers | `VersionIndex::prune_tombstones_before` exists and has unit coverage, but no production caller currently invokes it. |
| Recovery baseline helper | `VersionIndex::from_live_keys_at_epoch` exists but is not wired into open or replay. The current index starts empty on a new store handle. |

No general serializable isolation, time travel, multi-process writer, or
persisted row-version API follows from these mechanisms. Relational predicate
capture and pessimistic locks have their own contracts; their existence does
not make graph optimistic validation a serializable read-set validator.

## Identities actually emitted

`VersionKey` declares more identities than the commit collector currently uses.
A declared enum variant alone is not evidence that a domain has fine-grained
validation.

| Canonical operation | Recorded identity |
| --- | --- |
| Node create/property update | `GraphNode(id)`, live |
| Node delete | `GraphNode(id)`, tombstone |
| Relationship create/delete | `GraphRelationship(id)` plus source/outgoing and target/incoming `GraphAdjacency` identities; relationship deletion records a tombstone |
| Relationship property update | `GraphRelationship(id)`, live |
| Relationship delete without known endpoints | Relationship tombstone plus conservative `Database` barrier |
| Catalog/index/constraint changes | `Schema` barrier |
| Relational, relational snapshot, append, graph projection, initial-import marker | `Database` barrier |
| Nested canonical batch | Recursively collect its operations |

`RelationalRow`, `RelationalIndex`, `ForeignKey`, and `AppendTable` are reserved
identities, not emitted by this collector. Narrowing those domains still needs
complete identity derivation and constraint/recovery coverage. Pessimistic SQL
point-lock concurrency must not be presented as evidence that these variants
are active.

`VersionWriteSet` contains a `BTreeMap<VersionKey, VersionWrite>` and an entry
limit, defaulting to `DEFAULT_MAX_WAL_BATCH_OPERATIONS`. Repeated writes to a key
replace its disposition without consuming another entry. The read epoch belongs
to the transaction, not this map. The entry cap is not a byte budget for the
whole version index, and does not bound lifetime tombstone accumulation.

## Commit and visibility order

The non-rebased path passes the transaction's base epoch into
`commit_prepared_mutation_ops`. The commit path stages canonical graph,
relational and append changes against current state, collects their version
keys, validates them, checks publication requirements and graph constraints,
and appends the WAL. A version conflict occurs before WAL append or live root
publication. Earlier staging/constraint errors can occur before version
validation; not every rejected overlapping operation necessarily returns the
version-conflict variant.

After successful WAL append, the path applies canonical operations, advances
`commit_epoch`, and applies the version stamps. These updates are sequential
Rust operations protected by the sequencer, not one hardware-atomic root swap.
With group commit enabled, internal roots and stamps may advance before the
shared fsync so later tasks in that same group can validate against earlier
ones. The database mutex remains held through `finish_wal_sync_group`, and
successful requests are delivered after the barrier. A group sync failure
rejects the requests and poisons the handle; callers must close and reopen.
Thus the intended boundary is durable-before-external-visibility and successful
acknowledgement, not an assertion that no internal mutation precedes fsync.

Read-only transactions do not supply a write-validation epoch. Failed
statements restore their private state/operations; version keys are collected
from the surviving final operations at commit. There is no independently
accumulated transaction version-key set requiring a separate savepoint today.

## Validation argument and its limits

Let `E` be a transaction's read epoch, `C` the current commit epoch, `W` its
collected keys, and `V(k)` the latest recorded epoch (zero if absent). Ignoring
earlier semantic errors, the current validator accepts exactly when:

```text
for every k in W: V(k) <= E
for each b in {Database, Schema}:
    V(b) <= E and (b not in W or C <= E)
```

This gives the following conditional safety argument:

1. Assume the collector includes every conflicting write identity, epochs
   increase, and relevant stamps have not been removed. If a first commit
   writes `k` at `c > E`, a later commit with `k` in its set observes
   `V(k) >= c > E` and rejects before WAL append. Serialization prevents a
   competing publication between validation and stamp application.
2. With disjoint keys and no newer database/schema barrier, the first commit
   changes none of the second commit's tested stamps. Version validation alone
   therefore permits both. Other constraints, allocation collisions and
   resource limits can still reject them.
3. A broad writer rejects any intervening epoch, even if preceding writers
   emitted only narrow keys. Conversely, a newer broad stamp rejects a stale
   narrow writer. Both directions are necessary; checking only identical keys
   would leave the narrow-before-broad direction unprotected.
4. A deletion stamp at epoch `d` cannot safely be discarded while a transaction
   with `E < d` can still commit against that identity. The helper retains it
   when `d >= oldest_reader_epoch` and removes it only when strictly older.
   This argument assumes the watermark includes **all** active writers as well
   as readers; helper unit tests do not establish that production integration.

These are deductive arguments about the validation rule under stated
assumptions, not a machine-checked refinement proof of the Rust implementation.
In particular, complete key derivation, crash recovery, resource bounds, and
watermark integration require separate evidence.

### Recovery does not require surviving transaction history

A process restart invalidates every pre-crash transaction. If recovery restores
a consistent root at epoch `R`, every new transaction starts at `E >= R`.
All pre-restart stamps would be at most `R`, so omitting them cannot change a
comparison of the form `V(k) > E`. New commits must still record their stamps,
and broad writers must still check the current epoch. An empty process-local
validation index is therefore not, by itself, evidence of corrupt recovery.

This supersedes the original proposal's unconditional requirement to persist a
non-empty version index alongside every non-empty database. It is a conditional
restart argument, not permission to reset the index while old transactions
remain alive. Canonical WAL/checkpoint recovery, epoch restoration, and rejection
of corrupt or incomplete durable state remain mandatory. Exact pre-crash stamp
identity need not equal post-restart stamp identity; canonical data and observable
commit behavior must agree with the recovered serial order.

## Existing evidence and remaining work

`src/api/tests/concurrent_transactions.rs` already includes:

- `optimistic_transactions_commit_disjoint_graph_updates_from_one_snapshot`;
- `optimistic_transactions_prepare_in_parallel_and_reject_the_conflicting_committer`;
- `optimistic_transaction_reads_its_private_workspace`;
- `disjoint_primary_key_point_locks_allow_both_pessimistic_writers_to_commit`;
- `repeated_covered_point_read_keeps_its_snapshot_after_a_disjoint_commit`;
- `wal_group_commit_shares_one_sync_without_changing_record_order`;
- `wal_group_sync_failure_rejects_commit_and_poisons_until_reopen`.

The version module separately tests same-key conflict, disjoint keys,
disposition replacement, entry limits, tombstone watermark boundaries, and COW
page sharing. These are bounded cases, not full #231/#232 acceptance.

The existing [transaction model](tla/HawDBTransactionConcurrency.tla) still uses
whole-epoch optimistic validation: `OptimisticFirstCommitterWins` requires
`commitEpoch = baseEpoch + 1`. That property intentionally excludes a successful
stale but disjoint graph writer and is **not** the current per-key invariant.
Its lock/publication checks remain useful within that restricted model; they
cannot certify the added per-key executions. The new
[per-key validation model](tla/MVCC_VALIDATION_PROOF.md) separately checks the
version-index rule against a full-history oracle, including broad barriers,
restart, and a proposed safe tombstone-pruning integration. See the [model scope](tla/README.md#optimistic-and-pessimistic-transaction-publication).

Remaining acceptance work, without reimplementing existing mechanisms:

1. Extend the bounded per-key model evidence to source-level completeness and
   composition with the real recovery/group-sync paths. The finite model and
   its negative controls do not alone discharge these obligations.
2. Establish complete writer-pin/watermark integration before enabling
   production tombstone cleanup; demonstrate bounded retained state and
   progress after pins retire.
3. Narrow relational/append identities only with complete constraint and
   recovery coverage. Preserve conservative paths for unsupported shapes.
4. Qualify canonical crash/reopen equivalence and post-restart conflict behavior
   across WAL/checkpoint boundaries; do not require identical discarded
   process-local stamp history.
5. Produce the #232 writer scaling, fairness and starvation evidence, plus
   single-stream latency and group-commit comparisons. Existing concurrency
   unit tests do not substitute for this workload evidence.

The late pessimistic lock-acquisition rule remains conservative: after earlier
successful statements, acquiring a new resource against a changed epoch can
still require retry. Per-key **write** validation does not validate the earlier
reads of a newly acquired resource and cannot justify removing that guard.
