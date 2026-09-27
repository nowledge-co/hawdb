# Durable copy-on-write branching

Status: active P0 implementation contract for [#774](https://github.com/nowledge-co/hawdb/issues/774).
This specification defines planned behavior; it does not claim that branching is
available. The lifecycle model checks the bounded protocol described below.
Implementation and release qualification remain separate gates.

## Scope and current implementation boundary

HawDB MUST support cheap, isolated writable branches within one local project.
Creating a branch shares a sealed durable root and creates only metadata and an
empty branch-local WAL. It MUST NOT scan, import, reconstruct, or copy the
parent's logical dataset. Sealing may stream/hash the active WAL, but MUST NOT
require a full checkpoint or replay of the parent just to create the branch.

The existing storage contract remains [STORAGE.md](../STORAGE.md). Relevant
implementation seams at this specification's introduction are:

- `crates/storage/src/durable_manifest.rs`: `DurableManifest` selects one
  checkpoint and one writable WAL; `STORAGE_VERSION` is `hawdb-storage-v1`.
- `crates/storage/src/store/durable/checkpoint.rs`: complete artifacts and a new
  WAL precede manifest replacement; runtime state follows the durable selector.
- `crates/storage/src/ownership.rs`: `DatabaseDirectoryLease` excludes a second
  opener of a canonical database directory and retains the lock-file inode.
- `crates/storage/src/store/durable/reclamation.rs`: physical generations are
  retained for the current root, previous root, and pinned local readers.
- `crates/storage/src/durability.rs`: synced file replacement uses rename plus
  parent-directory sync on Unix and write-through replacement on Windows.

Single-directory generation arithmetic MUST NOT authorize deletion of shared
branch objects. Branch-aware storage stays unavailable until all paths that can
replace, repair, truncate, or reclaim its objects obey this contract.

P0 excludes point-in-time source selection, implicit merge, promotion,
schema-only branches, remote replication, and historical retention policy.
Lineage identifies origin; it does not define a merge algorithm. Publishing
HawDB crates does not authorize stable Mem activation.

## Identity, names, and revisions

`BranchId` is a project-scoped, immutable, non-nil UUID. IDs MUST NOT be reused,
including after deletion. The engine generates a fresh UUID for a create; a
caller can subsequently address the branch by that UUID without a name lookup.
The bootstrap `main` branch also receives a persisted UUID, never a sentinel ID.

Selectors are typed alternatives `Id(BranchId)` and `Name(BranchName)`. There
is no heuristic that interprets a UUID-looking name as an ID. UUID text accepts
the standard hyphenated hexadecimal form, case-insensitively, and output uses
lowercase. A UUID alone is a supported stable selector.

A custom name MUST satisfy all of these rules:

- 1 through 128 ASCII bytes, case-sensitive, without normalization or trimming;
- slash-separated, nonempty components containing only letters, digits, `_`,
  `-`, and `.`; a component MUST NOT equal `.` or `..`;
- no leading/trailing slash, backslash, whitespace, control byte, or NUL;
- `main` is reserved for the bootstrap branch; the `agent/` prefix is reserved
  for engine-generated names.

An omitted name generates `agent/<branch-uuid>`. Store the generated name once
with the result identity; retries MUST NOT generate a new UUID or name. Names
are catalog keys, never filesystem paths. `Foo` and `foo` are distinct even on a
case-insensitive filesystem because storage paths use canonical UUIDs only.

A name remains reserved in `Creating`, `Ready`, `Expired`, and `Deleting`.
After `Deleted`, a new UUID may reuse a custom name. A delete by name MUST carry
the expected UUID as well as revision, so a delayed request cannot delete a new
incarnation. Deleting or renaming `main` is rejected in P0. Rename is a catalog
operation with atomic old-name removal/new-name reservation; it changes neither
UUID nor lineage and is not required in the initial facade API.

Use three distinct counters, with checked overflow and no wraparound:

| Token | Meaning and comparison scope |
| --- | --- |
| Catalog revision | Monotonic project metadata publication sequence; protects catalog read/modify/write and GC snapshots. |
| Branch metadata revision | Monotonic per-UUID lifecycle/name/owner/expiry revision; compared by administrative mutations. |
| Source revision | `(BranchId, commit_epoch)` from the branch's committed logical state; compared by branch creation. |

Ordinary writes MUST NOT rewrite the project catalog. Physical checkpoint or
WAL rotation may change the head generation without changing the source
revision. The caller's source revision is checked under the source publication
barrier immediately before sealing/pinning. It MUST NOT silently advance to a
newer commit. This keeps a retry or checkpoint from changing the intended fork.

## Durable records and runtime state

The project catalog contains version, project UUID, catalog revision, branch
records, name mappings, and create-request outcomes. A branch record contains:

| Field | Contract |
| --- | --- |
| `branch_id`, `name` | Immutable identity and current validated unique name. |
| `parent_id`, `source_revision` | The resolved source UUID and commit epoch; absent only for bootstrap `main`. |
| `base_root_digest` | Immutable sealed-root digest captured at create; absent for bootstrap `main`, whose current sealed root is selected by its head. |
| `metadata_revision`, `state` | Lifecycle compare-and-swap token and state described below. |
| `owner`, `expires_at` | Optional bounded host metadata and UTC expiration instant; owner is not an authorization mechanism. |
| `create_request_key`, `request_fingerprint` | Stable operation identity and canonical request fingerprint. |
| `create_outcome` | Reserved UUID/name plus pending, succeeded, or terminal-aborted result; preserved after deletion. |

The branch-local head selector contains its own format version, project and
branch UUIDs, monotonic physical generation, sealed-root digest, logical commit
epoch, and active-WAL identity with replay start LSN. The catalog locates the
selector by UUID, not by a duplicate copy of its current head digest. This
avoids a two-file transaction on each normal commit/checkpoint.

Leases, reader pins, publication guards, object-cache entries, GC candidates,
and clock samples are runtime state. They MUST NOT be inferred from a persisted
PID, cached lease count, or lock-file presence after restart. Active in-process
handles retain their owning OS lease; a crash releases only that process's
leases, not leases held by a different process.

Catalog/name/request data have explicit decode/entry/byte limits. Exceeding a
limit returns a typed resource error before durable mutation; there is no
silent eviction of idempotency receipts. Durable tombstones and request receipts
are retained throughout P0. A future receipt-expiration policy needs a separate
contract; TTL of a branch is not TTL of its request key.

## On-disk compatibility and immutable objects

The project selector uses a distinct `HAWDB_BRANCH_PROJECT_V1` header in
`manifest.hawdb`. Branch catalogs, sealed roots, and branch heads each have
their own v1 format tag. An older single-root engine MUST reject the project
header before opening a WAL or running generation reclamation. Readers MUST
reject unknown versions, duplicate fields, invalid identities, and incomplete
references; absence/corruption of the catalog MUST NOT trigger legacy fallback.

New empty projects bootstrap `main` directly. When adopting an existing
development-only database, hold its directory lease, durably create a catalog
and `main` head referencing the existing complete manifest/artifacts, then
replace the top-level selector last. Before that replacement the legacy root
is authoritative; after it the catalog's `main` is authoritative. Preserve the
old artifacts until the new closure is durable. This is metadata adoption,
not a requirement for a compatibility decoder or a data-copy migration. An
unsealed adopted main may open through the existing library entrypoint, but
cannot be a branch source until immutable object publication is implemented.

Logical layout (paths are internal, not part of the facade API):

```text
project/
  manifest.hawdb                 # branch-project selector, published last
  metadata.hawdb.lock            # stable project metadata lock inode
  catalog.hawdb                  # checksummed catalog, atomically replaced
  objects/<kind>/<sha256>        # immutable payloads and sealed roots
  branches/<uuid>/
    owner.hawdb.lock             # stable branch writer/handle lock inode
    head.hawdb                   # checksummed branch-local selector
    active.<uuid>.wal.hawdb      # private mutable WAL
    staging/                    # private, operation-owned staging files
```

An object reference is `(kind, format_version, byte_length, sha256)`. Hash exact
encoded bytes with a domain separating object kind/version; the complete tuple
is the identity. Equal IDs MUST resolve to equal validated bytes. A publisher
finding an existing ID validates it and reuses it; mismatch is corruption or
collision and MUST fail closed, never overwrite. Never mutate a hard-linked
shared file, including during repair. Immutable reuse is allowed; hard links
are not a substitute for reachability accounting.

A sealed root binds an ordered, validated checkpoint closure, zero or more
ordered sealed WAL objects with exact non-overlapping LSN intervals, and the
resulting commit epoch. All authoritative graph, relational, append, schema,
property-spill, and overflow dependencies MUST be included transitively. A
root MUST NOT name mutable paths or depend on a parent's current head. Derived
indexes/caches may be omitted and rebuilt branch-locally; any included immutable
derived object obeys the same identity and reachability rules. Local generation
numbers and physical record IDs are interpreted within their recorded source
object/branch context, never as project-global IDs.

Root and catalog encodings MUST be deterministic, versioned, checksummed, and
bounded. Physical byte encoding is owned by the respective catalog/object
implementation; it MUST preserve the fields, identity hashing, ordering, and
reject behavior here and document its exact v1 codec alongside its decoder.
Object publication requires private staging, complete encoding, file sync,
checksum/length validation, exclusive immutable-name installation, and durable
directory publication before a selector can refer to it. Existing
`durable_replace_file` supplies selector replacement, not an implicit license
to overwrite an immutable object. Uncertain sync/replace results poison the
affected publication handle until reopen; the caller receives no success.

## Locks and publication ownership

Project metadata serialization and the branch writer lease are separate. One
project may have writable parent and child handles concurrently. A second
independent writer for the same UUID is rejected; host clones share the same
lease owner as the current embedded facade. Lock inodes MUST NOT be unlinked
while another process can hold or acquire them.

Never wait for a branch lease while holding the metadata lock. Acquire the
needed branch lease/publication barrier first, then take the metadata lock
briefly and revalidate identity/state/revision. A caller using an already-open
parent supplies its existing internal lease, rather than recursively opening
the parent directory. For open-by-name, resolve under metadata serialization,
release it, acquire the UUID lease, then revalidate the mapping, state and TTL
under metadata serialization before exposing a handle. A changed mapping is a
conflict, not permission to return the replacement branch.

All new references, removals, and GC sweeps participate in a common project
reachability barrier. A publisher registers a protected candidate before
releasing metadata serialization to write objects; that protection is retained
until a durable selector assumes ownership or the operation is abandoned.
Cross-process candidate protection MUST be backed by a durable pending record
or a verifiable OS lease that GC honors, never only a process-local registry.
Ordinary branch-local WAL appends do not need a project-wide lock.

GC may mark outside the exclusive publication span, but MUST revalidate under
that span before unlinking each bounded batch. A changed catalog/head/pin or
new candidate invalidates stale reachability evidence. The initial conservative
implementation may skip sweeping whenever an unrelated branch is leased; it
MUST NOT guess that an uninspectable owner has no pins. Lock hold time and
retained debt are observable maintenance results.

## Sealing and create protocol

Sealing holds the source branch's commit/publication barrier, checks the
expected source revision, drains/syncs its acknowledged WAL prefix, and closes
the old active WAL to further writes. A torn/invalid suffix blocks sealing;
only the existing explicitly authorized doctor workflow can discard an
incomplete tail. Sealing MUST NOT automatically repair or truncate shared data.

Publish the immutable WAL object(s), a complete sealed root, and a new empty
branch-local active WAL, all durably, before switching the parent's head.
The old head plus old WAL remains recoverable before the switch. The new head
references the sealed prefix exactly once and starts the active suffix strictly
after it. After the switch, append only to the new active WAL. A crash or lost
acknowledgement may expose either complete head, never a duplicated/lost
acknowledged prefix. Sealing does not change the source logical commit epoch.
`SyncOnCheckpoint` writes not yet durable are explicitly flushed by a successful
seal; a failed seal does not strengthen their prior durability guarantee.

Create then follows these durable transitions:

1. Validate request syntax/limits and consult its idempotency receipt first.
   For a new request, resolve and lock the source; revalidate its expected
   revision, state and TTL; seal if needed.
2. While the sealed source is pinned, atomically publish a `Creating` catalog
   record with UUID, reserved name, immutable base digest, full request
   fingerprint, and pending outcome. It becomes a global GC root immediately.
3. Create and sync the child's empty active WAL and private head referencing
   that base. Parent and child never share a mutable WAL or selector.
4. Atomically publish `Ready` and a successful create outcome in the catalog.
   Only then return the branch descriptor. A lost reply is resolved by receipt
   replay. Failure before step 2 leaves no branch; unreferenced objects are
   later collectible. Failure after step 2 requires recovery, not a new ID.

If the parent advances or is subsequently deleted, the child's base digest and
parent UUID remain unchanged. Lineage does not retain the parent's directory;
the child's own root references retain the required immutable objects.

## Idempotency, lifecycle and rejects

Create uses a nonempty, bounded, project-scoped opaque key. Its versioned
fingerprint covers the caller's typed parent selector, expected source token,
optional name (including the distinction between absent and explicit), owner,
and expiry. Encode options and byte lengths unambiguously. Store the resolved
UUID separately. Retries compare the original request before resolving a name
again, so rename, deletion, or name reuse cannot redirect a recorded request.

The same key/fingerprint returns the reserved original identity and its current
pending/succeeded/aborted/deleted outcome. It does not reopen an expired branch
or recreate a deleted one. Different input with the same key is always
`IdempotencyConflict`, including after deletion. In-progress create can return
`RecoveryPending` with the original ID; it cannot report a usable `Ready`
handle before publication. A terminal-aborted result requires a new key to
request a new branch.

| Operation/state | Required outcome |
| --- | --- |
| Create with invalid name/UUID/options | Typed `InvalidName` / `InvalidRequest`, no durable mutation. |
| New create with an occupied name | `NameConflict`, no second identity reservation. |
| Generated UUID collides with any live record or tombstone | Generate another UUID before reservation; a duplicate UUID in persisted metadata is an integrity error. |
| Unknown UUID or name | `UnknownBranch`; never infer a path. |
| Source token or mutation revision mismatch | `StaleRevision`, no rebasing or implicit retry against newer state. |
| Open `Creating` | `RecoveryPending`; no partial handle. |
| Open `Ready` before TTL | Acquire/revalidate lease and return the typed embedded database handle. |
| Open an independently leased UUID | `AlreadyOpen`; other branch UUIDs remain independently openable. |
| Open expired / `Expired` | `Expired`; elapsed TTL blocks admission even before a sweeper persists the state. |
| Open `Deleting` / `Deleted` | `Deleting` / `Deleted` by retained UUID; removed names may be unknown. |
| Delete `Ready` / `Expired` | Publish `Deleting`, reject new opens and new work on its existing handle. |
| Delete already `Deleting` / `Deleted` with the same identity | Idempotent current outcome; cannot affect a reused name. |
| Delete/rename `main` | `ProtectedBranch`. |
| Decode, checksum, unknown version, or incomplete selected closure | Typed storage integrity error; no usable handle or sweep. |

`describe` and bounded/paginated `list` expose UUID/name, lineage, revision,
state, owner and expiry without mutable paths. Their catalog revision makes
pagination changes explicit. Owner metadata is descriptive; hosts remain
responsible for authorization. Limits and all lifecycle errors belong to typed
Rust library APIs; JSON/CLI wrappers may later derive from those APIs.

Expiry affects new admission. A handle admitted before expiry keeps its lease
and may finish and perform work until it closes, unless explicit deletion has
begun. A delete permits already-admitted commits/checkpoints to finish or abort
atomically and rejects new transactions. It MUST NOT revoke a pin, truncate a
WAL under a reader, or interrupt publication into a partially durable state.
TTL uses a host-supplied clock policy; a persisted `Expired` state never returns
to `Ready` after clock rollback. Before that state is recorded, admission is
relative to the sampled wall clock, not a claim of globally monotonic time.

## Recovery, deletion and reclamation

Recovery validates the project selector, catalog, and selected per-branch heads
before serving them. It holds metadata serialization for state transitions and
revalidates leases; another process's live pending operation is not a crash.

| Durable interruption point | Recovery decision |
| --- | --- |
| Objects/staging exist without a catalog create record | Retain while an owner is live; otherwise mark as orphan candidates. |
| `Creating`, no durable child head | Publish terminal-aborted `Deleted` with the same receipt/UUID; release name reservation. |
| `Creating`, complete child head and base closure | Finish `Ready` with the same successful receipt/UUID (or `Expired` if its TTL elapsed). |
| `Creating`, existing but corrupt/ambiguous child head | Fail closed; retain all possibly referenced objects and report integrity failure. |
| Head switch during commit/seal/checkpoint | Use the complete selected old or new head and its exact WAL replay interval; do not mix generations. |
| `Deleting`, any live owner/publication/snapshot pin | Keep it non-openable and retained; report cleanup pending. |
| `Deleting`, no remaining owner/pins | Persist `Deleted` before physical cleanup and name reuse. |
| `Deleted`, interrupted directory cleanup or sweep | Retry bounded cleanup; retain receipt and identity tombstone. |

GC computes the transitive closure of all `Ready` and `Expired` heads/bases,
all `Creating`/`Deleting` recovery records, all live leases and reader pins,
all publication candidates, and any explicitly registered historical pins.
The catalog itself, active WALs, and stable lock inodes are not ordinary object
sweep candidates. Expired-but-not-deleted branches retain their complete state.
Deletion of one branch MUST NOT invalidate a surviving child's or sibling's
root, even if its parent record is already a tombstone.

Unreachable means proven absent from the complete current closure, not simply
older than the active generation or absent from one directory. Unreadable
catalog/head metadata, incomplete traversal, lost lease visibility, cancellation,
or budget exhaustion MUST retain candidates and report retry-required evidence.
After mark, sweep revalidates candidates under the reachability barrier;
objects protected since mark are retained. Deletions and directory cleanup
are restart-idempotent, and failure reports retained/reclaimed counts and bytes
without changing a successful logical-delete receipt into a false failure.

## Model and implementation qualification

[`HawDBBranchLifecycle.tla`](../tla/HawDBBranchLifecycle.tla) models two branches,
one non-reused child ID, four complete root choices, per-object persistence,
separate parent/child leases, pinned readers, staged publication, expiry,
two-phase deletion, crash/recovery, and mark/revalidated sweep. Parent
checkpoint advancement drops its old checkpoint from the current head while a
child may still need it. This makes shared-base retention observable.

Checked invariants cover selected-closure durability, pinned and pending-create
retention, immutable captured lineage, complete logical publication, parent
isolation from child writes, deletion lease safety, and open admission.
Negative controls independently permit early head publication, parent mutation,
omission of a pending create from GC, stale sweep, deletion under lease, and an
expired open. False-invariant witness configurations require reachable create
completion/abort after crash, deletion recovery, simultaneous branch leases,
expiry with an existing handle, and independent parent/child writes. See the
[model evidence and limitations](../tla/BRANCH_LIFECYCLE_PROOF.md).

The model abstracts atomic metadata replacement and immutable object identity;
it does not prove byte codecs, hashes, OS locking, clocks, arbitrary branch
counts, admission bounds, or Rust refinement. Both writers may progress in the
finite state graph, but no scheduling fairness or latency theorem is claimed.
Crashes lose both modeled process-local handles; cross-process lease loss must
be checked separately in implementation tests.

Implementation delivery is specification -> catalog (#777) and immutable
objects (#779) -> isolated branch opening (#780) -> lifecycle facade (#775)
and global GC (#778). Each PR targets `main` directly. Before P0 is available:

- fault-inject every object/catalog/head sync and replacement boundary,
  including ambiguous completion, and verify complete old-or-new recovery;
- verify custom/generated names, UUID equivalence, name reuse, stale revisions,
  exact idempotency through restart/delete, and typed reject classes;
- observe branch creation without logical source reads/copy/import, then run
  parent/child/sibling independent writes, checkpoints and reopen in both orders;
- exercise conflicting same-branch opens, concurrent different-branch leases,
  expiry/open/delete races, corrupt closure discovery and publication during GC;
- cover every canonical artifact family, minimal/default facade profiles,
  focused Cargo/Bazel tests and the repository's required local fuzz command.

No issue is complete merely because a model passes; the corresponding runtime
acceptance criteria require source-level and executable implementation evidence.
