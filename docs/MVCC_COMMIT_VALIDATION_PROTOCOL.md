# MVCC Commit Validation Protocol

Status: partial implementation for #231 and #232.
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
outcomes at commit. These are distinct protocols: permitting either rebase path alone does not
demonstrate per-key optimistic SQL validation. The qualified explicit-key SQL
path below now has separate per-key validation evidence.

| Mechanism | Current source and scope |
| --- | --- |
| Version storage | `crates/storage/src/version.rs`: `VersionIndex` is a COW map containing the latest epoch and live/tombstone disposition for each recorded identity. |
| Write-set collection | `crates/storage/src/store/graph_commit.rs`: `collect_version_writes` derives graph keys from final canonical operations; typed relational/append staging extends the same bounded set before WAL append. |
| Legacy direct commits | `finish_non_relational_commit` publishes a `Database` stamp after applying an already-durable direct graph/catalog/import mutation. These entry points lack a proven complete narrow write set. |
| Optimistic validation | `validate_version_writes` checks recorded keys plus database/schema barriers against the transaction's `base_commit_epoch`. Conflicts become retryable `HawDBError::TransactionConflict`. |
| Private statement execution | `src/api/concurrent.rs`: statements operate on transaction-owned state; `commit_with_result` submits publication to the sequencer. |
| Rebase selection | `commit_with_result` enables it for pessimistic transactions and the narrowly checked conflict-noop-only optimistic transaction shape. |
| Publication and group sync | `src/api/concurrent/coordinator.rs` serializes commit tasks and retains the database mutex through the shared durability barrier. |
| Writer snapshot lifetime | `DatabaseTransactionState` owns a `ReaderPin` from snapshot capture through private execution and queued commit; rollback/drop releases it and first-statement refresh replaces it. |
| Version reclamation | Every `GraphStore::snapshot` registers an epoch in a shared storage registry. Successful durable checkpoint publication, in-memory checkpoint and version-budget pressure call `reclaim_version_history`, retaining stamps at or after the oldest snapshot epoch. |
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
| Prepared strict-append rows, including materialized generated-order rows | `AppendTable(table)` live stamp for every written table |
| Prepared strict-append table creation | `Schema` barrier |
| Qualified explicit relational insert/replacement or primary-key deletion | `RelationalRow(table, primary_key)`, live or tombstone; every intent is retained, including net-zero effects |
| Complete-primary-key UPDATE/DELETE on unconstrained/unreferenced tables, without primary-key assignment | `RelationalRow` intent even for absent rows or false residuals |
| Pure relational Error-mode insert with unique/FK constraints or incoming references | `RelationalRow` plus each non-NULL `RelationalIndex` unique identity; shared parent reads are not write identities |
| Other relational predicate/UPSERT/DDL, destructive unique/FK-related writes, opaque relational WAL/snapshot, opaque append WAL, graph projection, initial-import marker | `Database` barrier |
| Nested canonical batch | Recursively collect its operations |
| Legacy direct graph/catalog/import commit | `Database` barrier at the completed commit epoch |

`RelationalIndex` now identifies the unique values of qualified pure inserts;
`ForeignKey` remains reserved. Unconstrained explicit replacements and primary-key
deletes keep row identities. Constrained/referenced tables qualify only when all
of their operations are pure Error-mode inserts; destructive operations keep
both Database-barrier directions. The [relational intent proof](tla/RELATIONAL_MVCC_PROOF.md),
[point-predicate refinement](tla/PRIMARY_KEY_PREDICATE_MVCC_PROOF.md)
and [constrained-insert refinement](tla/CONSTRAINED_INSERT_MVCC_PROOF.md) explain
NULL handling, shared-parent reads, eligibility and the complete intent union.
Typed strict-append staging now emits `AppendTable` from the same prepared
transaction that is encoded into WAL. Generated sequence counters are table-wide,
so different partitions in one table still conflict. The opaque WAL collector
retains its database fallback because it has no typed preparation evidence.
See the [append footprint proof](tla/APPEND_MVCC_PROOF.md). Narrowing relational
identities still needs complete constraint and recovery coverage. Pessimistic SQL point-lock concurrency alone is not evidence of complete
optimistic SQL validation. Legacy direct commits likewise use the conservative barrier;
older optimistic workspaces retry even when their keys appear unrelated.

`VersionWriteSet` contains a `BTreeMap<VersionKey, VersionWrite>` with entry
and estimated-byte limits. Defaults are `DEFAULT_MAX_WAL_BATCH_OPERATIONS`
and `DEFAULT_MAX_WAL_RECORD_BYTES` (16 MiB), respectively. `with_limits` permits
explicit budgets; `new(max_entries)` keeps the default byte budget. Charges use
`VersionKey::cow_page_bytes()` plus the inline `VersionWrite` size. Repeated keys
retain their resident key and only replace disposition, without charging again.
Checked addition rejects overflow or a byte-budget breach before map mutation.
Collectors propagate failure before WAL publication and discard partial local
preparation. See the [admission proof](tla/MVCC_VALIDATION_PROOF.md#write-set-byte-admission).

This is an estimated retained key/write payload budget, excluding B-tree node
and allocator overhead, spare capacity and the incoming key allocation. It is
not a global bound across transactions, canonical/history COW pages or snapshot
lifetimes. The read epoch belongs to the transaction, not this map.

## Read-consumer metadata

Embedded read transactions, descendants of published read snapshots and runtime
planning snapshots use `GraphStore::snapshot_for_read`. Their data/catalog view
and physical/storage pins are unchanged, but they retain only a Database barrier
at the capture epoch instead of all historical conflict-index pages. Epoch-zero
captures need no stamp. Writable workspaces/savepoints retain precise indexes
through the ordinary `snapshot` method.

A new transaction starts at or after capture, so older stamps cannot affect its
validation. A pre-capture transaction submitted to the low-level read-optimized
store is conservatively rejected by the barrier. The facade read API rejects
writes outright. See the [conditional proof](tla/MVCC_VALIDATION_PROOF.md#read-consumer-conflict-baseline).
This reduces read-consumer conflict metadata to constant size; it does not bound
all retained data pages, writer snapshots or active pin counts.

## Current-index payload admission

The production commit path additionally limits the current `VersionIndex` to a
64 MiB key/stamp estimate (`DEFAULT_MAX_VERSION_INDEX_BYTES`). Its counter
includes a permanent Database-barrier reservation, even when no barrier stamp
exists. Updating an existing identity does not consume additional budget.
Before WAL append, admission counts new identities in the complete write set.
If it cannot fit, the store first performs the existing safe watermark-based
history reclamation, then retries admission. Failure returns a storage error
before WAL/data publication. It does not drop still-required stamps, invalidate
old readers or substitute a broad conflict identity.

The reservation matters because legacy direct commits publish their Database
barrier after WAL. That stamp is an inline fixed field; all other stamps remain
in the COW map, so updating the barrier does not detach shared index pages. They can always use that reserved identity without a new
post-WAL resource failure. Successful pruning recomputes the current estimate;
cloned indexes retain their own consistent counter and immutable pages.

This is a bound on the production current-index estimate, not total version
memory: historical COW roots, B-tree/page overhead, transient copy-on-write
allocations, spare capacities, and the number of concurrent snapshots remain
outside it. Low-level `VersionIndex::apply` and `from_live_keys_at_epoch` are
unmanaged construction/test primitives, not admission APIs; the bound is enforced
by `GraphStore`'s serialized commit path. The latter constructor has no current
production callers. Reopen resets process-local conflict history as before.
Long-lived pins can reject index growth; callers must release old snapshots
and checkpoint before retrying. There is no automatic transaction cancellation.
The 64 MiB default is a fixed admission policy, not a measured global memory
requirement or an allocator guarantee.

## Current-epoch candidate baseline

Before preparing the next index, the store checks its shared snapshot pins. If
none exists or the oldest pin is at least the current commit epoch C, it stages
from a constant Database barrier at C rather than copying earlier narrow-key
history. Every usable writer has E >= C, so previous stamps cannot affect its
validation. New writes still receive their exact identities and next epoch.
Any older source, workspace or descendant disables this replacement and keeps
precise conflict history. The candidate still passes both payload admissions;
WAL rejection refunds it without publishing the baseline.

Exclusive store access and registration-before-escape ensure no older pin can
appear between reading the minimum and using it. Descendants inherit an
already-live ancestor floor, including across private commits and savepoints.
The [source refinement and updated finite models](tla/MVCC_VALIDATION_PROOF.md#current-epoch-candidate-compaction-and-pin-registration)
state these assumptions explicitly. Strict-watermark pressure/checkpoint
reclamation remains available when an older pin prevents baseline replacement.

## Shared retained-history admission

Each open GraphStore and its snapshot/workspace descendants share a separate
256 MiB budget (`DEFAULT_MAX_RETAINED_VERSION_HISTORY_BYTES`) for non-Database
version-root payload estimates. Clones share one reference-counted lease without
charging again. A changed candidate root reserves its entire resulting estimate
before COW preparation or WAL; the previous root stays charged until its final
holder drops. Different roots may share physical pages and are deliberately
charged separately. Read baselines retain no narrow-key payload, but inherit the
same budget for any subsequent low-level storage writes.

After safe reclamation and one retry, insufficient credit returns a storage
error before WAL/data/epoch publication. Cancellation and WAL refusal drop the
prepared root and refund its lease. Successful publication installs that root;
final-owner retirement refunds the old charge. A replacement of existing keys
can therefore fail this aggregate admission even though it fits the current
index limit. No snapshot is cancelled automatically.

Partial pruning of a shared root reserves the full input estimate before
`retain`, because that operation copies input pages before filtering. It refunds
the removed portion afterward. Insufficient copying credit defers that pruning
safely. A unique root reuses its charge and shrinks it; replacing a fully obsolete
root with an empty map requires no copying credit. Inline Database barriers do
not grow or detach the narrow-key map, so legacy publication remains infallible
with respect to this admission.

The [lease proof](tla/MVCC_VALIDATION_PROOF.md#shared-retained-history-leases)
separates this per-open-lineage estimate from total allocator/RSS memory,
per-handle headers and pin counts, internal transient page restructuring, and
independent database opens. Neither the 256 MiB default nor conservative whole-root
charging is a measured workload qualification.

## Commit and visibility order

The non-rebased path passes the transaction's base epoch into
`commit_prepared_mutation_ops`. The commit path stages canonical graph,
relational and append changes against current state, collects their version
keys, validates them, checks publication requirements and graph constraints,
reserves and prepares the complete version root, and appends the WAL. A version conflict occurs before WAL append or live root
publication. Earlier staging/constraint errors can occur before version
validation; not every rejected overlapping operation necessarily returns the
version-conflict variant.

After successful WAL append, the path applies canonical operations, advances
`commit_epoch`, and installs the prepared version root. These updates are sequential
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
   This argument requires the watermark to include **all** storage snapshots,
   including sources from which a writer could be created later. The shared
   storage snapshot registry now supplies that watermark; upper-layer physical
   generation pins remain a separate mechanism.

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
- `optimistic_mvcc_reopen_preserves_disjoint_commits_and_same_key_conflicts`;
- `optimistic_mvcc_reopen_preserves_both_database_barrier_directions`;
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
restart, retained source snapshots, and safe version-history pruning. See the [model scope](tla/README.md#optimistic-and-pessimistic-transaction-publication).

The [group-admission proof](tla/OPTIMISTIC_COMMIT_ADMISSION_PROOF.md#mixed-domain-subprocess-crash-qualification)
now also records 28 subprocess crash cases: two residency modes, disjoint or
overlapping writers, and seven WAL/shared-sync/checkpoint exit points. Exact
graph, relational and generated-append rows must recover as a complete serial
prefix, preserving prior acknowledgements; fresh post-restart conflicts and a
second reopen are checked. This is selected process-loss evidence, not hardware
power-loss or every-instruction recovery qualification.

The [40-pair single-stream measurement](CONCURRENT_WRITER_BENCHMARK.md#shared-history-single-stream-result-2026-09-25)
compares runtime `0b9906f9` against PR-base `a703cc0f` using isolated builds.
All 480 cases pass correctness, but the predeclared zero-increase latency gate
fails: memory and ungrouped durable commit p50 show regression evidence, while
the other four endpoints remain inconclusive. The earlier five-pair result is
not final-runtime acceptance; correction and requalification remain required.

Remaining acceptance work, without reimplementing existing mechanisms:

1. Extend the bounded per-key model evidence to source-level completeness and
   composition with the real recovery/group-sync paths. The finite model and
   its negative controls do not alone discharge these obligations.
2. Qualify total version-memory overhead and cleanup cost under representative
   churn and long-lived pins. Checkpoint cleanup now reclaims eligible
   live and deleted stamps, including barriers, but long-lived pins and historical
   COW maps still retain metadata within the shared payload-estimate budget.
   Allocator and per-handle overhead are excluded; neither a global RSS bound nor bounded
   checkpoint latency follows.
3. Extend relational identities beyond the qualified explicit-key path only
   with complete predicate/constraint and recovery coverage. Typed append writes now have table identities; finer partition
   identities would require separating table-wide generated-order state.
   Preserve conservative paths for unsupported/opaque shapes.
4. Qualify canonical crash/reopen equivalence and post-restart conflict behavior
   across WAL/checkpoint boundaries; do not require identical discarded
   process-local stamp history.
5. Produce the #232 writer scaling, fairness and starvation evidence, plus
   single-stream latency and group-commit comparisons. Existing concurrency
   unit tests do not substitute for this workload evidence. The local
   [fixed-work writer benchmark](CONCURRENT_WRITER_BENCHMARK.md) provides
   1/4/8-writer lifecycle controls and full result/reopen checks; its outcomes
   must be assessed rather than treating the presence of a harness as acceptance.

The late pessimistic lock-acquisition rule remains conservative: after earlier
successful statements, acquiring a new resource against a changed epoch can
still require retry. Per-key **write** validation does not validate the earlier
reads of a newly acquired resource and cannot justify removing that guard.

## Concurrent optimistic commit admission

Optimistic transactions acquire a database `OptimisticCommit` logical lock
before enqueueing. These permits coexist, but remain incompatible with all
ordinary shared/exclusive locks until retirement. The commit sequencer still
validates/applies each task serially, including conflicts with earlier tasks in
the same unsynced group. This permits shared durability without exposing
pre-fsync results or weakening pessimistic coordination. See the
[admission proof and model](tla/OPTIMISTIC_COMMIT_ADMISSION_PROOF.md).
The [lock-wait protocol](tla/LOCK_WAIT_FAIRNESS_PROOF.md) now prevents new
conflicting holders from bypassing an older queued request. This is conditional
per-request progress, not fairness of whole transactions or their retries.

## Governed transaction lifetime

Hosts can admit one mutation request through their existing RuntimeGovernor and
pass the permit and task context to `begin_admitted_transaction`. The transaction
retains admission across statements, queued commit and durability, without a
second admission. The queue retains an independent owner after callback
consumption. Plain `begin_transaction` remains caller-managed. See the
[lease invariant and scope](tla/TRANSACTION_ADMISSION_LEASE_PROOF.md); this does
not complete whole-transaction fairness or total workspace memory accounting.

The optimistic admission model also retains acknowledgement evidence across
crash and allows complete uncertain WAL records to survive a failed group sync.
Its prefix invariant does not equate a returned storage error with rollback.
The two-writer failure regression checks exact serial prefixes, unchanged WAL
on poisoned-handle rejection, atomic two-node batches, strict torn-tail/LSN
rejection and fresh-handle commits after reopen. Filesystem/apply/checkpoint
composition remains outside that finite model; see the updated
[shared-durability proof](tla/OPTIMISTIC_COMMIT_ADMISSION_PROOF.md).


## Conditional progress with full-capacity host admission

A separate [governed writer proof and workload](tla/GOVERNED_WRITER_PROGRESS_PROOF.md)
now covers a same-priority large writer requesting all CPU slots from a shared
RuntimeGovernor while smaller requests repeatedly arrive. With stable capacity,
eventual retirement/service, finite valid work and every mutator participating,
queue priority lets capacity accumulate, and the retained permit prevents
competing governed snapshots until the large transaction publishes and retires.
The memory/durable fixture commits 64 statements atomically ahead of 128
recurring small transactions and checks exact reopen state and resource refunds.
This is an explicit host policy, not a default database-wide exclusion or a
guarantee for arbitrary-size, mixed-priority or ungoverned transaction retries.

The [aged-background writer refinement](tla/AGED_WRITER_PROGRESS_PROOF.md) extends
the full-capacity governed progress policy to one background large waiter under
recurring foreground arrivals, assuming deadline passage and fair host retry.
Partial-capacity retry fairness and arbitrary priority mixtures remain separate.
