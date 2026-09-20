# MVCC Commit Validation Protocol

Status: design baseline for #231. No per-key version validation is implemented
yet.

## Purpose and boundary

HawDB already provides immutable COW snapshots, reader generation pins, one
process-local writer, and manifest-last durable publication. Those mechanisms
make read snapshots stable, but the current write path still validates a
transaction against one database-wide `base_commit_epoch`. A commit to an
unrelated key therefore aborts a transaction that began at the same epoch.

This document defines the storage contract that replaces that coarse check. It
does not change the following boundaries:

- One process owns a durable database directory and one WAL publication stream.
- Commit validation, WAL append, durable sync, and root publication are one
  serialized critical section.
- The first isolation level is snapshot isolation, not serializable isolation.
- Graph and relational statements retain their existing transaction-private
  workspaces and read-your-own-writes behavior.
- A non-provable access set remains conservative. It uses a broad version key
  or the existing lock fallback; it never becomes an unvalidated write.

Issue #232 builds concurrent transaction-body execution on this protocol. It
does not move execution out of the `CommitSequencer` before this protocol has
durable validation, recovery, and reclamation coverage.

## Why keys, not pages

The version identity is per logical write key. A per-page stamp would make two
unrelated records conflict merely because their COW map page is shared. That
would preserve the false-conflict ceiling that this work is intended to remove.

`VersionKey` is internal storage state. It is not a user-visible row-version or
time-travel API. Its variants cover every canonical mutation domain:

| Domain | Version identity | Reason |
| --- | --- | --- |
| Graph node | Node id | Create, delete, label, and property changes invalidate the same entity key. |
| Graph relationship | Relationship id | Relationship endpoints, type, and properties share one identity. |
| Graph adjacency | Node id plus direction | Relationship changes alter bounded adjacency traversal results. |
| Relational row | Table identity plus primary key | Row update, delete, and primary-key insert conflict at row granularity. |
| Unique/index entry | Table identity, index identity, and encoded index key | Concurrent inserts or key changes must preserve uniqueness. |
| Foreign-key target | Referenced table and key | Delete and referencing insert must validate the same constraint identity. |
| Append sequence | Table identity | Generated-key allocation is one ordered state per table. |
| Schema/catalog | One schema identity | DDL, constraint, and index changes invalidate every workspace built from the old catalog. |
| Conservative access | Table or database identity | Unknown graph predicates, joins, and unbounded access sets stay correct before narrower derivation exists. |

An operation may produce more than one key. For example, deleting a graph node
stamps the node, each deleted relationship, and affected adjacency identities;
an indexed relational update stamps the row plus its old and new index entries.
Deduplication and ordering happen before validation, so one transaction cannot
produce conflicting duplicate entries for the same `VersionKey`.

## Snapshot and workspace state

Under this protocol, transaction start records a `read_epoch` equal to the
published commit epoch and captures the existing catalog, graph snapshot,
relational state, append state, and a reader pin. The private graph and
relational workspaces continue to serve all reads and writes for the lifetime
of that transaction.

The planned transaction state also owns a bounded `VersionWriteSet`:

```text
VersionWriteSet {
    read_epoch,
    keys: ordered, deduplicated VersionKey values,
    broad_scope: optional table or database VersionKey,
}
```

The write set is derived from the normalized graph operations and relational or
append mutation outcomes, after a statement succeeds in its private workspace.
A statement savepoint restores its write-set additions together with the
workspace when that statement fails. A rollback drops the write set without
changing live state.

Read-only snapshots do not validate a write set. Snapshot isolation deliberately
permits read-write skew between disjoint write sets. A future serializable mode
needs separately specified read/range validation; it must not be inferred from
this protocol.

## Commit protocol

When enabled, only the commit sequencer executes these steps. A failed step
leaves the published root and `VersionIndex` unchanged.

1. Reject a transaction that has no active snapshot or has exceeded its
   write-set memory/key budget.
2. Normalize and deduplicate its `VersionWriteSet`. Reject an impossible or
   unsupported identity rather than degrading it to an unprotected narrow key.
3. For every key, read the latest committed stamp from the live `VersionIndex`.
   Validation succeeds only when every observed stamp is at most `read_epoch`.
4. On a newer stamp, return a typed retryable `TransactionConflict` containing
   the transaction read epoch, current epoch, and the conflicting internal key
   class. Do not append a WAL entry, advance an epoch, or publish workspace
   state.
5. Recheck graph constraints, relational constraints, and append sequence
   allocation against the same live root used for validation. Their identities
   are already members of the write set, but constraint evaluation remains the
   authoritative semantic check.
6. Build the existing canonical WAL batch. The batch and its stamp update have
   one candidate commit epoch.
7. Append and durably sync the WAL batch using the existing group-commit
   barrier. A sync failure rejects the entire candidate and does not publish
   its root or stamps.
8. Publish the canonical root, then atomically set every validated
   `VersionKey` to the candidate commit epoch. The in-memory publication order
   is one indivisible sequencer action; readers see either the old root and
   stamps or the new root and stamps.

The WAL must carry enough information for replay to reconstruct exactly the
same stamp updates. The preferred representation derives keys deterministically
from each normalized canonical WAL operation and its persisted schema state.
If any key cannot be reconstructed unambiguously during replay, the WAL format
must include the normalized key set in the same record before this protocol is
enabled. Checkpoint state must persist the current `VersionIndex` or a canonical
equivalent so reopen never silently resets validation history.

## Persistence and reclamation

The planned `VersionIndex` is a COW, ordered storage-owned map from `VersionKey`
to its last committed epoch. It keeps one latest stamp per live identity, not an
unbounded heap of version chains. Snapshot readers retain historical COW pages
through their existing pins, while new commits replace only affected pages.

Deletion tombstones are different: their version entries remain until the
oldest active transaction read epoch is newer than the tombstone epoch.
Otherwise a transaction that began before the deletion could recreate or update
the same identity without detecting the conflict. The existing reader registry
must therefore register concurrent write transactions as well as explicit read
transactions. A cleanup pass removes a tombstone only after that watermark
advances and after confirming that no live canonical or constraint state still
needs the key.

Checkpoint publication must record the version-index checkpoint identity and
covered commit epoch with the canonical checkpoint. WAL replay must rebuild or
validate the index before the database reports itself writable. A checksum,
decode, or epoch mismatch fails closed; a reconstructed empty index is never a
valid fallback for a non-empty database.

Physical generation reclamation remains governed by the existing pinned
generation set. Version-index page reclamation follows the same snapshot
lifetime and cannot delete a physical generation referenced by an active reader.

## Integration slices

Each slice is independently reviewable and preserves the current public
transaction entry points until the typed conflict variant is introduced.

1. **Storage identity and persistence.** Add internal `VersionKey`,
   `VersionIndex`, deterministic key derivation for canonical graph,
   relational, append, constraint, and schema mutations, and checkpoint/WAL
   replay support. Do not change concurrent execution yet.
2. **Commit validation.** Replace the `GraphMutationTransaction`
   `base_commit_epoch` rejection with write-set validation in the serialized
   commit path. Add `HawDBError::TransactionConflict` as the typed retryable
   result. Keep all transaction bodies serialized for this slice.
3. **Snapshot registration and cleanup.** Give write transactions the same
   epoch/generation pin lifetime as read transactions, retain deletion stamps
   correctly, and reclaim obsolete tombstones/pages only after the oldest pin
   advances.
4. **Concurrent execution (#232).** Snapshot and execute transaction bodies
   outside `CommitSequencer`; enter it only for validation, WAL append, and
   publication. Preserve the single WAL stream and existing group-commit
   behavior.
5. **Narrower access coverage.** Replace conservative graph/table scopes only
   when statement analysis can prove a complete write set. Keep broad scopes for
   joins, unbounded predicates, and unsupported graph shapes.

## Verification gates

The implementation is not ready to activate until all of these are covered by
source-level tests, model checking, and the repository's required verification
targets.

- A transaction pinned at epoch `E` observes only epoch-`E` state while later
  commits and checkpoints complete.
- Two disjoint graph, relational, and append write sets prepared from one epoch
  both commit. The same-key, unique-index, foreign-key, adjacency, and schema
  cases return the typed conflict deterministically without WAL publication.
- A statement failure and transaction rollback discard their tentative keys.
  Savepoint restore cannot retain a key from a failed statement.
- Crash injection before WAL sync, after sync, and during checkpoint/reopen
  produces the same canonical root and version index as the serial committed
  order. Torn tails, corrupted stamps, and checkpoint/index mismatches fail
  closed.
- A long-lived reader or writer snapshot limits tombstone cleanup but does not
  freeze unrelated physical-generation reclamation. Releasing the oldest pin
  enables the next cleanup pass.
- A new TLA+ model covers snapshot selection, write-set validation, WAL
  durability, publication, conflicts, and reclamation. It is added to the
  existing `docs/tla:storage_models` manifest with a mutant that violates
  validation-before-publication.
- The #232 writer matrix measures 1/4/8 disjoint writers, verifies one WAL
  order and no lost updates, and compares single-stream commit latency against
  the pre-concurrency baseline.

## Explicit non-goals

- No multi-process writer, distributed transaction, or general time-travel
  interface.
- No serializable isolation or predicate read-set validation in the first
  protocol.
- No weakening of current lock fallbacks, integrity checks, crash recovery, or
  durable-before-publish ordering to obtain apparent concurrency.
- No page-level stamp shortcut that reintroduces false conflicts for disjoint
  records.
