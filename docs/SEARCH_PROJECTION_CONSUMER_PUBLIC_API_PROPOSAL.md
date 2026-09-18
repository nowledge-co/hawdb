# Durable search projection consumers: proposed public contract

Status: approved by the owner on September 15, 2026; production implementation in progress.
This proposal addresses [issue #455](https://github.com/nowledge-co/hawdb/issues/455).
The approved proposal was based on main
`1d9970f382b1896e3e6d450cef93f8b709192c88` and the private source/state proofs
described below. Implementation now incorporates main through PR533 using a
normal merge. This document does not claim that issue 455 is complete.

## Problem and decision

A known consumer needs a durable, verifiable checkpoint and an explicit rebuild
reason when its replay window is lost. Current trimming already retains every
change while both configured limits fit. Adding a cursor cannot extend that
window beyond the limits; it supplies ownership, minimum-consumer accounting and
explicit invalidation. Preserve the legacy no-early-reclamation behavior.

Use an opt-in owned consumer to coordinate initialization, existing batch
hydration, projection checkpoint and cursor publication. Preserve all existing
unregistered query/catch-up signatures and results. New consumer operations take
`&mut Database`; do not change existing `&self` methods or use a global registry.

Two alternatives were rejected:

- A caller-supplied integer or ordinary SearchProjectionFreshness is not a durable
  identity proof. Equal freshness can describe different databases, and direct
  upsert can change projection contents without changing either freshness epoch.
- Ordinary relational cursor rows advance the source commit epoch and appear in
  the changefeed. Suppressing their capture alone would still create a moving
  catch-up target. Do not add ordinary SQL metadata writes to every batch.

## Approved public surface

Export these types through `hawdb`; internal owners remain implementation details:

- `SearchProjectionConsumerId`: validated owned string, 1-128 ASCII letters,
  digits, `.`, `_` or `-`; `new(value: impl Into<String>) -> Result<Self>` and
  `as_str(&self) -> &str`. Equality/order/hash use the exact validated bytes.
- `SearchProjectionConsumerOptions`: private fields; `new(max_idle_commits:
  NonZeroU64) -> Self`, `max_idle_commits(&self) -> NonZeroU64`. No implicit wall
  clock, environment variable or background task controls expiry.
- `SearchProjectionConsumer`: opaque, non-Clone owner of one persistent
  SearchIndex, its binding and exclusive projection publication lease;
  `id(&self) -> &SearchProjectionConsumerId` and
  `search_index(&self) -> &SearchIndex`. There is no mutable index accessor.
- `SearchProjectionConsumerState`: `Unverified`, `Active`, or
  `RebuildRequired(SearchProjectionConsumerRebuildReason)`.
- `SearchProjectionConsumerRebuildReason`: `DatabaseIdentityMismatch`,
  `ProjectionIdentityMismatch`, `CheckpointMismatch`, `SourceRewound`,
  `RetentionLimitExceeded { resume_floor_commit_epoch: u64 }`,
  `Expired { expires_at_commit_epoch: u64 }`, `UntrackedProjectionMutation`,
  `RegistryUnavailable`.
- `SearchProjectionConsumerStatus`: public fields `consumer_id`, `state`,
  `durable_complete_through_commit_epoch: Option<u64>`,
  `expires_at_commit_epoch: u64`, `minimum_valid_consumer_commit_epoch: Option<u64>`,
  and `changefeed: SearchProjectionChangefeedStatus`.
- `SearchProjectionConsumerReadiness`: public fields
  `consumer: SearchProjectionConsumerStatus` and
  `changefeed: SearchProjectionChangefeedReadiness`; `is_ready(&self) -> bool`
  requires an active consumer and existing changefeed readiness.
- `SearchProjectionConsumerCatchUpReport`: public fields
  `catch_up: SearchProjectionCatchUpReport` and
  `consumer: SearchProjectionConsumerStatus`.
- `SearchProjectionConsumerError`: `Database(HawDBError)`, `InvalidHandle`,
  `AlreadyRegistered`, `RegistryFull`, `SourceNotDurable`, or
  `RebuildRequired(SearchProjectionConsumerRebuildReason)`; implements Display
  and Error. `SearchProjectionConsumerResult<T>` aliases Result with this error.

New Database methods:

```rust
pub fn create_search_projection_consumer<F>(
    &mut self,
    id: SearchProjectionConsumerId,
    projection_directory: impl AsRef<Path>,
    options: SearchProjectionConsumerOptions,
    initialize: F,
) -> SearchProjectionConsumerResult<SearchProjectionConsumer>
where
    F: FnOnce(&mut DatabaseReadTransaction, &mut SearchIndex) -> Result<()>;

pub fn open_search_projection_consumer(
    &mut self,
    id: &SearchProjectionConsumerId,
    projection_directory: impl AsRef<Path>,
) -> SearchProjectionConsumerResult<SearchProjectionConsumer>;

pub fn catch_up_search_projection_consumer<F>(
    &mut self,
    consumer: &mut SearchProjectionConsumer,
    max_change_operations_per_batch: usize,
    max_projection_operations_per_batch: usize,
    max_batches: usize,
    batch_hydrator: F,
) -> SearchProjectionConsumerResult<SearchProjectionConsumerCatchUpReport>
where
    F: FnMut(
        &mut DatabaseReadTransaction,
        &SearchProjectionChangeBatch,
    ) -> Result<SearchProjectionRelationalDelta>;

pub fn renew_search_projection_consumer(
    &mut self,
    consumer: &SearchProjectionConsumer,
) -> SearchProjectionConsumerResult<SearchProjectionConsumerStatus>;

pub fn unregister_search_projection_consumer(
    &mut self,
    id: &SearchProjectionConsumerId,
) -> SearchProjectionConsumerResult<()>;

pub fn search_projection_consumer_status(
    &self,
    id: &SearchProjectionConsumerId,
) -> SearchProjectionConsumerResult<SearchProjectionConsumerStatus>;

pub fn search_projection_consumer_readiness(
    &self,
    consumer: &SearchProjectionConsumer,
    max_operations: Option<usize>,
) -> SearchProjectionConsumerResult<SearchProjectionConsumerReadiness>;
```

There is no public `advance(epoch)` method. The library derives the advancement
from its completed batch and successful checkpoint. Repeating the same verified
checkpoint is idempotent. A smaller epoch, an epoch above the durable checkpoint,
or a different checkpoint at an already acknowledged epoch is rejected.

Existing status/readiness structs retain their fields and struct-literal
compatibility. The new reports compose them rather than adding a required field
to every existing status literal. No new HawDBError variant is required.

## Initialization and projection ownership

Creation requires a persistent writable database and a destination that does not
yet exist; its parent must exist.
An existing projection is not silently adopted on the strength of its epoch.
The library builds in its own staging directory, holds a pinned database read
transaction, and gives the initializer a fresh staging SearchIndex. The initializer
must retain the supplied staging index and populate the complete application
projection from that snapshot, including
host-owned relational content when applicable. It can use the existing typed
rebuild path and parameterized bounded queries. It must propagate incomplete
hydration, row/payload-budget failure and initialization errors.

The library supplies the source identity and complete-through epoch from that
pinned transaction; the callback cannot supply an advancement epoch. The
initializer owns projection semantics, just as the existing batch hydrator does;
HawDB does not infer or validate arbitrary application mapping logic. This is an
explicit initialization contract, not automatic adoption of unknown existing data.
Initialization does not claim to finish #392/#529's separate SearchIndex memory
ownership work.

The current implementation requires the default analyzer lexicon. Existing
snapshots do not persist arbitrary analyzer rules, so initialization rejects a
custom lexicon before publication instead of silently changing query semantics on
reopen. Unregistered SearchIndex configuration remains unchanged. Other ephemeral
initializer runtime settings are retained during the owning process, with the
existing defaults on reopen.

Only after initialization and the complete projection checkpoint succeed does
creation publish the destination and durable registry entry. An error before
publication discards the owned stage; an error after projection publication leaves
an unregistered artifact and requires explicit retry/rebuild. It does not report
an active consumer. Existing unrelated directories and artifacts are not removed.

An active consumer holds the projection publication lease. Registered checkpoint
internals reuse that lease rather than reacquiring it. Another registered opener
cannot concurrently write the same projection. Existing search/query methods are
available through the shared SearchIndex reference. Ordinary public mutation or
checkpoint entrypoints cannot publish a registered projection outside the consumer
owner; methods that cannot return an error invalidate the private binding before
any later publication. Default, unregistered SearchIndex behavior stays unchanged.
Opening a registered projection for mutation through the ordinary SearchIndex
lifecycle must fail closed; the consumer lifecycle is the explicit opt-in path.

## Persistent representation and crash order

1. Lazily create a database UUID using the existing core UUID generator when the
   first consumer is created. Persist it as an optional, checksummed graph
   checkpoint record named `search_projection_database_identity`. The existing
   graph manifest binds that checkpoint's length and digest. This one-time
   checkpoint does not advance the source commit epoch. A pathname-derived cache
   StoreId and external-import fingerprint are not substitutes.
2. Registered search snapshots carry an optional `projection_consumer_binding`
   record containing database UUID, projection UUID, consumer ID, registration
   UUID and a fresh checkpoint UUID. These records and the existing source epoch
   are covered by the snapshot integrity check. The text record is the tab-separated
   tag followed by the five fields in that order; the consumer ID excludes tabs.
   UUIDs use canonical lowercase hyphenated form. The binding immediately follows
   the snapshot header so publication guards can inspect bounded control records
   without reading the document corpus. Duplicate identity/binding
   records, malformed UUIDs and impossible source epochs fail closed.
   An empty graph at source epoch zero is permitted. Normal unregistered
   snapshots omit the record.
3. A separate database-local `projection_consumers.meta` registry stores the
   database UUID, a bounded array of consumer records, their last acknowledged
   binding/checkpoint UUID, snapshot length/SHA-256, durable complete-through
   epoch, idle-commit interval and expiry epoch. Use a UTF-8 JSON envelope with exactly `protocol`, `payload` and
   `payload_sha256` fields. The protocol is `hawdb-projection-consumers-v1`.
   `payload` has exactly `database_uuid` and `consumers`; each consumer has exactly
   `id`, `projection_uuid`, `registration_uuid`, `checkpoint_uuid`,
   `snapshot_encoded_len`, `snapshot_sha256`, `durable_complete_through_epoch`,
   `max_idle_commits` and `expires_at_commit_epoch`. UUIDs use canonical lowercase
   hyphenated form; digests are 64 lowercase hexadecimal characters. Consumer
   records sort by ID. Canonical payload JSON sorts object keys lexicographically,
   contains no insignificant whitespace and uses unsigned integer fields.
   `payload_sha256` hashes those canonical payload bytes. The envelope ends in
   one newline; duplicate/unknown fields and trailing data are rejected. A fixed sibling temporary file is synced,
   then published with `durable_replace_file`; clean up only that owned temporary.
4. For each whole successful batch: apply the validated delta; finish the complete
   projection checkpoint and all mandatory artifact publication; capture its
   receipt while retaining the publication lease; sync and publish the cursor
   registry; only then count that batch as durably acknowledged in the report.
   The snapshot digest should be obtained while writing, not through an extra
   unbounded allocation. All snapshot/source epochs are checked against the pinned
   source and the current database identity.

Normal graph writes retain their existing WAL sync boundary. Consumer create,
catch-up and renewal reject an active WAL sync group with SourceNotDurable; they
must not certify a projection from a source commit whose WAL sync is deferred.
A later explicit retry after the group has successfully flushed is allowed.

A crash before projection checkpoint leaves the old cursor. A crash after
projection checkpoint and before registry publication leaves an old or mismatched
cursor. Recovery must validate exact identities and checkpoint receipts; it may
require a rebuild at that boundary rather than invent evidence for a newer epoch.
A crash after cursor publication must reopen with the corresponding durable
projection. A registry-publication error is surfaced without rolling back or
misreporting a projection checkpoint that has already committed.

No new WalOp variant or per-batch relational metadata transaction is proposed.
Existing development databases without identity records may continue unregistered;
consumer creation initializes the new identity. Older binaries are not promised
compatibility with databases using the new optional records. This follows the
project's greenfield development policy and is part of the persisted contract
being proposed, not an implicit production migration.

## Bounded retention, expiry and recovery

Support at most 64 records initially, including expired/invalid records retained
for diagnostics. Bound the complete registry at 64 KiB before decoding; reject
oversized IDs, duplicate consumers, duplicate fields, unknown versions, malformed
UUIDs/digests, invalid enum tags, impossible epochs and trailing data. The parser
and temporary publication need corresponding allocation and corrupt-file tests.
Unregister removes a record and frees its slot; a new registration always uses a
fresh registration UUID, so an old in-process handle cannot regain ownership.

Compute the soft floor from the minimum active, verified durable consumer epoch.
The existing entry and byte ceilings always win. When the hard floor overtakes a
consumer, its status becomes RetentionLimitExceeded and registered catch-up returns
the typed rebuild-required error. Only the affected consumers are invalidated.
Do not prune anything earlier merely because every current consumer has caught up;
legacy anonymous readers retain the existing bounded window.

Expiry is inclusive at `graph_commit_epoch >= expires_at_commit_epoch`; creation
and successful renewal set the deadline with checked addition of the configured
idle-commit interval. An expired handle requires a fresh registration/rebuild and
cannot silently renew itself. This clock is persisted source progress, not wall
clock time. An idle database produces no additional retained history, while a busy
database cannot be pinned indefinitely. Unregister is idempotent and does not
remove projection files. Dropping an owner releases its publication lease but does
not silently unregister its durable consumer.

Validity is derived from current source identity, recovered hard floor, deadline
and exact projection receipt. Reopened entries start Unverified and do not become
active merely because their integers parse. Opening the consumer verifies them
against the supplied projection. A registry missing/corrupt on reopen makes the
consumer unavailable and requires explicit reinitialization; ordinary database
operations retain their existing hard retention behavior. No graph commit waits
for registry disk I/O just to prune an already over-limit log.

Explicit creation can reinitialize a missing, corrupt or foreign-database registry
from a complete source snapshot. It preserves available diagnostics until the
initializer and projection publication succeed. A transient registry I/O failure
first reloads the durable file, so rebuilding one consumer cannot discard other
valid registrations. Reinitialization never adopts an existing projection.

If a larger retention limit on restart restores a complete replay window from
still-present WAL, activation still requires explicit receipt validation; there
is no promise that a historical invalidation is a permanent revocation. Explicit
unregister/registration UUID changes provide revocation. Backups may omit the
rebuildable registry, in which case consumer recovery fails closed. A copied
registry is useful only with its matching database and projection checkpoint;
rollback/replacement mismatches require rebuild. Checkpoint, pruning and cleanup
must preserve the live registry and identity record.

## Production implementation and verification

The owner approved this contract on September 15, 2026 in draft PR535. The original
57-test feasibility evidence and two private model negative controls remain in
commit `afb2bb99b7aa114f137b4e2382691e78fad3534d`; the private model/scaffolding is
replaced by production-path tests in the implementation.

Production ownership and streamed snapshot receipts live in
`crates/search/src/consumer.rs`. The facade and bounded registry live in
`src/api/search_projection_consumer/`; external callers are covered by
`tests/search_projection_consumer.rs`. Optional database identity is propagated
through graph checkpoint writing, recovery and pinned snapshots.

The actual-path tests cover pinned mixed graph/relational initialization, failure
cleanup, exact receipt reopen, process crashes at all three publication boundaries,
registry I/O failure, missing/corrupt registry, entry/byte/zero retention, selective
invalidation, inclusive expiry, unregister/re-registration, capacity, source sync
groups, source rollback and backup identity, malformed bindings, and public facade
use. Default and minimal profiles, strict owner Clippy and bounded registry decoder
allocation are qualification gates. Complete results belong in the PR delivery
receipt; partial intermediate runs do not close issue 455.

The unchanged default Bazel root, search, external facade and mandatory local
fuzz command currently fails before test execution on missing external rules_shell
files. No build settings, workloads, timeouts or fuzz CI policy were changed.
This environment failure is not a passing Bazel result.

This contract does not relax retention limits, authorize a background worker,
change existing query semantics, or complete incremental artifact compaction (#291)
and SearchIndex analyzer workspace ownership (#392/#529).
