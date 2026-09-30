# Durable copy-on-write branching

Status: active P0 implementation contract for [#774](https://github.com/nowledge-co/hawdb/issues/774).
This specification defines planned behavior; it does not claim that branching is
available. Earlier lifecycle-model results do not qualify the revised contract.
Implementation and release qualification remain separate gates.

## Configurable durability decision (2026-09-29)

Power-loss-safe transaction commits are the default (`SyncOnEveryWrite`). Hosts
may explicitly select `SyncOnCheckpoint` for branch-local DDL/DML through typed
Rust configuration. Branch metadata publication remains synchronously durable
in both modes. Existing implementation and historical model evidence still
require qualification; this document does not claim they pass.

- After `create_branch()` returns success, the same branch UUID and name MUST
  survive power loss and restart. Branches persist until explicitly deleted;
  closing a handle, losing an OS lock, or having no open handles MUST NOT delete
  a branch or make its reachable objects eligible for GC. TTL/automatic expiry
  is not required at this stage.
- In `SyncOnEveryWrite`, successful DDL/DML commits MUST survive power loss.
  Persist WAL and all recovery dependencies, including required directory
  entries, before acknowledgment. Group commit callers wait for the shared sync.
- In explicitly selected `SyncOnCheckpoint`, flush the complete transaction WAL
  to the OS before acknowledgment; recent unsynchronized transactions may be
  lost after power failure. Successful checkpoint/seal synchronizes its covered
  committed prefix. There is no fixed time bound on the unsynchronized window.
  Loss may omit whole transactions, never recover partial schema/data changes.
- The effective policy comes from the project opener's explicit `durability`
  argument, not `DatabaseConfig` or persisted branch metadata. It applies to that
  execution context's transactions and is fixed for its lifetime. Default opening
  uses `SyncOnEveryWrite`. `USE BRANCH` preserves that context's explicit policy;
  it never inherits the target branch's previous writer policy. Reopening without
  an override resets to the synchronous default. WAL disabling is outside this
  decision.
- A response lost after durable publication may leave a committed operation.
  Branch creation retries MUST recover the same idempotent outcome and identity.
  An interrupted transaction MUST recover atomically, never as partial schema
  or data changes; lack of acknowledgment does not imply rollback.
- Checkpoint, sealing, and head replacement MUST recover a complete old or new
  selection with every commit covered by a completed durability barrier
  recoverable. Recovery MUST NOT mix generations, discard durable commits,
  or replace damaged state with an empty branch. Ambiguous or corrupt state requires a fail-closed error
  and preservation of possibly referenced files.
- Runtime open locks provide concurrency exclusion and are reacquired after
  restart. Durable catalog/head reachability provides branch retention even
  without any live lock. The lock file's existence is not proof of a live owner.
- File writes and atomic rename alone are insufficient. Use the platform's
  required file/directory persistence barriers and propagate failures without
  acknowledging a durable commit or metadata publication. The guarantee assumes
  the filesystem and storage device honor those barriers; platform-specific
  requirements must be stated.
- Qualification MUST inject lost unsynchronized writes, torn writes, and write
  reordering at WAL, object, catalog, head, checkpoint, and GC publication
  boundaries. Verify synchronous commits and acknowledged metadata operations
  survive; relaxed recovery preserves all barrier-covered commits and transaction
  atomicity. Verify retries do not duplicate branches and surviving descendants
  retain their dependencies. Process termination/reopen tests alone are insufficient.

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
schema-only branches, remote replication, TTL/automatic expiry, and historical
retention policy. Branch-local schema changes and branching from a child are
required P0 behavior, not schema-only branching.
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

A name remains reserved in `Creating`, `Ready`, and `Deleting`.
After `Deleted`, a new UUID may reuse a custom name. A delete by name MUST carry
the expected UUID as well as revision, so a delayed request cannot delete a new
incarnation. Deleting or renaming `main` is rejected in P0. Rename is a catalog
operation with atomic old-name removal/new-name reservation; it changes neither
UUID nor lineage and is not required in the initial facade API.

Use three distinct counters, with checked overflow and no wraparound:

| Token | Meaning and comparison scope |
| --- | --- |
| Catalog revision | Monotonic project metadata publication sequence; protects catalog read/modify/write and GC snapshots. |
| Branch metadata revision | Monotonic per-UUID lifecycle/name/owner revision; compared by administrative mutations. |
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
| `owner` | Optional bounded host metadata; not an authorization mechanism. No expiry field is required by P0. |
| `create_request_key`, `request_fingerprint` | Stable operation identity and canonical request fingerprint. |
| `create_outcome` | Reserved UUID/name plus pending, succeeded, or terminal-aborted result; preserved after deletion. |

The branch-local head selector contains its own format version, project and
branch UUIDs, monotonic physical generation, sealed-root digest, logical commit
epoch, and active-WAL identity with replay start LSN. The catalog locates the
selector by UUID, not by a duplicate copy of its current head digest. This
avoids a two-file transaction on each normal commit/checkpoint.

Leases, reader pins, publication guards, object-cache entries, and GC candidates
are runtime state. They MUST NOT be inferred from a persisted
PID, cached lease count, or lock-file presence after restart. Active in-process
handles retain their owning OS lease; a crash releases only that process's
leases, not leases held by a different process.

Catalog/name/request data have explicit decode/entry/byte limits. Exceeding a
limit returns a typed resource error before durable mutation; there is no
silent eviction of idempotency receipts. Durable tombstones and request receipts
are retained throughout P0. A future receipt-expiration policy needs a separate
contract; explicit branch deletion does not expire its request receipt.

## Writable branch API and schema isolation

Branch creation, inspection, selection/opening, and deletion are SQL operations
executed by the embedded query runtime. A dedicated public `open_branch()` method
is not required. Rust opens the project and configures its execution context;
SQL selects the branch. The implemented inspection surface is:

```sql
SHOW BRANCHES LIMIT 100;
SHOW BRANCHES LIMIT $1 OFFSET $2;
SHOW BRANCH NAME $1;
SHOW BRANCH ID $1;
SHOW CURRENT BRANCH;
```

`SHOW BRANCHES` requires an explicit unsigned `LIMIT` literal or positional
parameter, and accepts an optional unsigned `OFFSET`; the configured query row
budget rejects an oversized requested page rather than truncating it. Results are
accounted against the configured payload budget. Each catalog row contains
`branch_id`, `name`, `parent_id`, `source_commit_epoch`, `state`, and `owner`.
`SHOW BRANCH NAME` and `SHOW BRANCH ID` accept only a string literal or a bound
string parameter. The `ID` form validates UUID syntax after binding, while the
`NAME` form never applies a UUID-looking-name heuristic. Catalog inspection is
metadata-only and does not admit or retain a data-branch runtime.

Selection will use:

```sql
USE BRANCH dev;
USE BRANCH NAME $1;
USE BRANCH ID $1;
SHOW CURRENT BRANCH;
```

The first form accepts a validated branch-name identifier (double-quoted when
needed); the `NAME` and `ID` forms accept a string literal or bound parameter.
Names remain case-sensitive under the branch-name contract, with no UUID-looking
name heuristic. UUID values are parsed and validated as UUIDs. Parameters are
bound as values by the parser/runtime, never interpolated into SQL text.
`SHOW CURRENT BRANCH` returns one bounded row containing branch UUID, name, and
effective durability policy; a metadata-only context returns null UUID/name.

The same Rust query entrypoints execute inspection SQL. `USE BRANCH` is not yet
implemented; the following remains the target use after its admission protocol
lands:

```rust
let mut db = Database::open(project_path)?; // default: SyncOnEveryWrite
// Proposed SQL, resolved inside this project:
db.query_sql("USE BRANCH dev")?;
db.query_sql("ALTER TABLE documents ADD COLUMN kind TEXT")?;
db.query_sql("SHOW CURRENT BRANCH")?;
```

An explicit relaxed context uses the existing configuration-bearing opener:

```rust
let mut db = Database::open_with_durability_and_config(
    project_path,
    DurabilityPolicy::SyncOnCheckpoint,
    DatabaseConfig::default(),
)?;
db.query_sql("USE BRANCH dev")?; // retains the explicitly selected policy
```

`DatabaseConfig` supplies read-only, recovery, and resource settings; the
separate `DurabilityPolicy` argument is the sole policy source. No environment,
parent-branch policy, catalog preference, or SQL switch can weaken the default.
Read-only contexts may select branches but cannot mutate data or branch metadata.
Branch metadata publication stays synchronously durable even in relaxed mode.

### Selection, locks, and transactions

Selection belongs to a mutable execution context, never a global project setting.
For direct `Database::query_sql`, that context is the exclusively borrowed
`Database`. `DatabaseSession` borrows the same context; a successful selection
remains on that database after the session ends. Independent contexts retain
their own branch selections. Reopening defaults to `main`, not the last selection.
Project metadata access must not require obtaining the `main` writer lock:
validate the project/catalog on open, and defer branch lock acquisition and data
recovery until `USE BRANCH` or the first data statement against default `main`.
After default/selected branch admission fails, never silently route to another
branch. Metadata operations remain possible without an admitted data branch.

`USE BRANCH` performs real open admission: resolve and pin the target identity,
try its open lock, validate/recover its state, revalidate its catalog identity,
and only then replace the context's active branch. Do not wait indefinitely for
a target lock while retaining the source lock: return a typed busy error. A
failed switch keeps the original context usable; it must not replace the source
catalog/store before target admission completes. Release the old context's open
lock only after switching, retaining any independently owned reader/job pins.
Selecting the already admitted UUID is a no-op after lifecycle revalidation.

Reject selection during an explicit transaction, including read-only transactions;
never implicitly commit, roll back, or move writes. Existing independent read
snapshots remain pinned to their original branch. Prepared plans and cached state
must be invalidated or bound to the branch identity/schema version; a statement
prepared before switching cannot silently write into the new branch. Background
jobs retain their original branch identity and ownership until completion.

`ConcurrentDatabase` currently shares one runtime through `Arc`. P0 selection
happens before converting a mutable database into that shared concurrent runtime;
`USE BRANCH` on the shared runtime or its transactions returns a typed unsupported
context error. It must never retarget all clones. Future independently selectable
concurrent sessions require their own selection state; adding them is separate
from enabling SQL selection on the mutable embedded context.

### SQL lifecycle surface and implementation boundary

Use `CREATE BRANCH`, `SHOW BRANCHES`, `SHOW BRANCH`, and `DROP BRANCH` for lifecycle
operations. Creation does not implicitly select its result; `USE BRANCH` never
creates a missing branch. Creation must expose expected source revision and an
idempotency key as bound values; deletion must expose expected UUID/revision so
name reuse cannot redirect a delayed request. Listing requires explicit bounded
pagination and payload accounting. These grammar details and result schemas must
be finalized with parser tests before implementation qualification; they must not
be hidden solely in route-specific Rust methods. Create/drop/use are rejected
inside user transactions and never implicitly commit them. Dropping the selected
branch is rejected until the caller switches away; other contexts follow the
explicit deletion/admission protocol below.

Add explicit SQL AST variants and dispatch lifecycle/selection before the normal
implicit data-transaction wrapper. Use one storage lifecycle kernel for locking,
sealing, publication, and recovery. Existing typed helpers may support that kernel
or compatibility callers, but SQL must exercise the same semantics and error
classes. Do not implement branch commands by host-side string matching, shelling
out, or copying a database directory. No new crate is required for this surface.

[Dolt's branch SQL](https://www.dolthub.com/docs/sql-reference/version-control/branches/)
provides a precedent for session-scoped `USE` and `DOLT_CHECKOUT`; its
[checkout implementation](https://github.com/dolthub/dolt/blob/main/go/libraries/doltcore/sqle/dprocedures/dolt_checkout.go)
calls session `SwitchWorkingSet`. HawDB adopts session-scoped selection, not
Dolt's multi-branch transaction or implicit-commit behavior.

Acceptance must cover parameterized name/UUID selection, missing/busy/corrupt
targets leaving the source intact, selection in active transactions, cross-context
isolation, shared-runtime rejection, stale prepared statements, reader/job pins,
read-only selection, default synchronous policy, explicit relaxed selection, and
reopen defaulting to `main` with synchronous durability.

Opening validates the selected branch's complete immutable closure and replays
its sealed WAL plus private active WAL exactly once. It MUST NOT derive schema,
artifact paths, or replay intervals from the parent's current manifest. Recovery
may materialize bounded runtime state, but creating a branch must not copy the
source dataset. A recovery helper that copies a source directory is not the
production branch-opening implementation.

Any ready branch can be a source, including one with committed DDL and DML.
Capture schema and data at one committed source revision. A source revision
returned for future forks must describe current committed state, not the
creation-time lineage epoch. Under the source publication barrier, compare the
requested revision, seal its committed WAL suffix, and publish the child from
that exact state. Reject stale revisions; do not silently fork an older sealed
head or include uncommitted changes. Creating a child does not commit an active
user transaction. An in-transaction fork request must be rejected explicitly.

Schema is branch-owned database state: graph descriptors, relational and append
table definitions, constraints, index definitions, and migration records are
captured with the data. A child inherits their committed state at its fork;
subsequent parent, child, and sibling changes are isolated. The same table or
index name may have different definitions in different branches. Local object
IDs are interpreted in their branch/object context, not as global identities.

P0 reuses supported DDL rather than adding new syntax: the current relational
compiler supports `CREATE TABLE`, `CREATE INDEX`, and `ALTER TABLE ADD COLUMN`;
graph and append operations retain their existing supported semantics and
restrictions. Unsupported DDL remains an explicit error. Schema-only branching
and schema merge are separate capabilities and are not needed for local DDL.

DDL follows the ordinary transaction path:

1. Bind and validate against the transaction's branch schema snapshot.
2. Prepare schema and associated data changes privately. Serialize schema
   publication within the branch; detect incompatible concurrent schema changes
   before committing a writer bound to an older definition.
3. Append one atomic transaction containing all required schema/data WAL
   records. Before acknowledgment, synchronize its recovery dependencies in
   `SyncOnEveryWrite`, or flush the complete WAL transaction to the OS in
   explicitly selected `SyncOnCheckpoint`. Schema follows the same policy as data.
4. Publish the committed snapshot and invalidate affected branch-local plans.
   Readers holding older snapshots retain matching schema/data until release.

Prepared plans and caches must not leak across branch identities or schema
versions. Initially keep them branch-local, and rebind or invalidate stale plans
on schema publication. System migration records are inherited at fork and then
advance independently; existing protections on direct system-table writes apply.
Engine schema upgrades must use the same branch-local atomic durability path.

DDL that requires row-page checkpointing must publish new branch-owned artifacts
and switch only that branch's head. Shared schema, pages, indexes, and overflow
objects remain immutable. Preserve objects reachable from other branches or
reader snapshots. A checkpoint failure after a durable transaction commit cannot
undo that commit; recovery must reconstruct it from WAL. Any resulting error
must distinguish maintenance failure from a definitely aborted transaction.

### Required schema and fork acceptance scenarios

- Fork `main -> dev`, commit an added column and data on `dev`, then fork
  `dev -> experiment` without requiring a manual checkpoint. The grandchild
  inherits both changes; `main` and pre-existing siblings do not.
- Run independent DDL/DML on parent, child, and sibling; checkpoint each and
  reopen in different orders. Schema, constraints, indexes, and migration
  records must remain isolated and paired with the correct data.
- Roll back DDL/data transactions and reject invalid constraints without partial
  changes. Race DDL publication with fork creation and verify exact-revision
  success or a typed stale-revision rejection.
- Execute identical SQL against diverged branch schemas and verify plan-cache
  isolation; retain an old reader during DDL without mixing schema versions.
- Delete a parent and sweep while its child/grandchild survives. Reopen the
  descendants using their own closure, without the parent's directory.
- Inject power loss before and after commit, seal, checkpoint, and create
  acknowledgment in both modes. Every acknowledged branch and synchronous
  transaction survives; relaxed recovery preserves the last completed barrier
  and transaction atomicity. Idempotent create retries preserve identity.

### Current implementation gaps

The lifecycle facade currently creates nested branch metadata from sealed heads;
this is not yet the complete writable workflow above. Required work includes:

- SQL branch selection/lifecycle AST and dispatch, deferred branch admission,
  context-local writable opening, and private active-WAL replay;
- sealing current committed state when a modified child becomes a fork source;
- branch-local DDL/checkpoint publication, snapshot and plan invalidation;
- updating the historical expiry-bearing model; runtime catalog/facade expiry
  fields and transitions have been removed in catalog v2;
- exposing the effective durability mode and auditing platform persistence
  barriers in both modes, including metadata publication and uncertain completion;
- shared project FD accounting, lazy file residency, and bounded descriptor
  admission across branch switching and maintenance;
- branch-aware locking, global GC, and the acceptance scenarios above.

There is no production compatibility obligation for earlier development-only
branch formats. Update the greenfield format deliberately and reject unsupported
old versions; do not add expiry compatibility migrations solely for old dev data.
These are implementation gaps, not claims that this documentation fixes runtime
behavior.

## On-disk compatibility and immutable objects

The project selector uses a distinct `HAWDB_BRANCH_PROJECT_V1` header in
`manifest.hawdb`. Branch catalogs use the v2 format described below; sealed
roots use the v2 format and branch heads retain their own v1 format tag. An
older single-root engine
MUST reject the project header before opening a WAL or running generation
reclamation. Readers MUST
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

A sealed root binds an immutable copy of its durable manifest, an ordered,
validated checkpoint closure, canonical normalized relative-path bindings for
every object in that closure, zero or more ordered sealed WAL objects with exact
non-overlapping LSN intervals, and the resulting commit epoch. The manifest
object and path bindings let recovery resolve the exact root-owned checkpoint
state without consulting the source database's current manifest or current
head. The root
does not name a mutable selector path. All authoritative graph, relational,
append, schema, property-spill, and overflow dependencies MUST be included
transitively. Derived indexes/caches may be omitted and rebuilt branch-locally;
any included immutable derived object obeys the same identity and reachability
rules. Local generation numbers and physical record IDs are interpreted within
their recorded source object/branch context, never as project-global IDs.

Root and catalog encodings MUST be deterministic, versioned, checksummed, and
bounded. Physical byte encoding is owned by the respective catalog/object
implementation; it MUST preserve the fields, identity hashing, ordering, and
reject behavior here and document its exact codec version alongside its decoder.
Object publication requires private staging, complete encoding, file sync,
checksum/length validation, exclusive immutable-name installation, and durable
directory publication before a selector can refer to it. Existing
`durable_replace_file` supplies selector replacement, not an implicit license
to overwrite an immutable object. Uncertain sync/replace results poison the
affected publication handle until reopen; the caller receives no success.

### Immutable object publication kernel

`crates/storage/src/immutable_object.rs` implements the first #779 publication
boundary. Its reference is `(kind, format_version, byte_length, sha256)`. The
SHA-256 input is the fixed domain tag, the one-byte kind, the little-endian
format version and byte length, followed by the exact payload bytes. Therefore
two different object families or format versions cannot silently share an
identity, and a reference computed from a payload is deterministic.

The publication argument is an induction over the operation's checked stages:

1. Before staging, the reference validator establishes the identity and size
   invariant for the caller's bytes.
2. A private staging file is created with `create_new`, written completely, and
   synchronized. A bounded read-back recomputes its length, full bytes, and
   domain-separated digest, so the installed candidate satisfies the same
   invariant before it can become visible.
3. Installation uses an exclusive hard link into the kind directory. If the
   destination already exists, the publisher reads and compares the complete
   bytes and reference; it never replaces that path. Consequently, every
   successful destination names exactly one validated immutable payload, even
   when two publishers race.
4. The object and staging directory entries are synchronized before success is
   reported. An error after exclusive installation is publication-uncertain and
   poisons that in-memory publisher; reopening creates a fresh publisher which
   revalidates the existing object. Thus an uncertain result cannot be retried
   through a possibly stale handle or acknowledged as a durable selector.

This is a source-linked deductive proof of the publication kernel's identity,
exclusive-installation, and fail-closed boundaries. It does not prove the
future sealed-root closure, WAL interval ordering, filesystem crash model, or
the branch selector protocol; those remain obligations of the later #779
stages.

### Sealed-root reference codec

`crates/storage/src/sealed_root.rs` provides the bounded v1 metadata codec for
the next #779 stage. A root contains a nonempty canonical checkpoint-reference
list, a checkpoint and commit epoch, a replay-start LSN, and zero or more
sealed-WAL references. Each WAL interval is half-open, strictly nonempty, and
must begin exactly where the previous interval ended. Checkpoint and WAL
references are restricted to their object kinds and nonzero format versions.

The codec proof follows the reader offset by induction. Every fixed-width read
checks the remaining slice before advancing, so each decoded field belongs to
the checksum-covered prefix. The declared counts are checked against bounded
limits before vector allocation. Checkpoint references must be strictly
increasing under their complete typed identity. Each such reference must have
exactly one canonical, bounded, non-traversing relative-path binding, and the
root's durable-manifest reference must be a valid immutable manifest object.
The WAL invariant carries the expected next LSN from one record to the next;
therefore duplicates, overlaps, gaps, unbound artifacts, and source-manifest
substitution cannot enter a validated root. Exact-end checking rejects unparsed
bytes, and the CRC32C footer rejects mutations before validation. The encoder
emits the same field order and validated lists, so equivalent roots have one
byte representation and their sealed-root object identity is deterministic.

This proves codec boundaries and interval admission only. The references are
not yet populated from every live checkpoint artifact family, and the codec
does not itself seal a WAL, publish a root, or prove crash recovery; those
remain integration obligations of #779.

### Explicit checkpoint-closure publication

`crates/storage/src/checkpoint_closure.rs` defines the boundary between a
validated durable manifest and immutable object publication. The caller gives
one input for every manifest-bound artifact; the publisher never scans the
directory and never derives a dependency from a filename. For each input it
first proves path and typed-reference uniqueness, reads the complete file,
recomputes the domain-separated object identity, and only then invokes the
exclusive immutable publisher. Therefore induction over the input list gives:

1. every returned reference denotes exactly the bytes named by one explicit
   manifest binding;
2. a missing file, wrong kind, digest/length mismatch, duplicate path, or
   duplicate reference aborts before that binding is acknowledged; and
3. the returned references are a deterministic sorted set suitable for the
   sealed-root encoder, while already-published earlier objects remain safe to
   share and are never replaced.

This proof covers the no-inference and per-artifact identity boundary. It does
not claim that the caller has enumerated every artifact family; the manifest
reader and the eventual active-writer/head handoff must provide that complete
list before a root or selector can be published. The root publisher snapshots
the validated durable-manifest bytes as a distinct immutable object and derives
one normalized relative-path binding for every closure input.
`build_sealed_root` then accepts only that returned canonical reference set
together with those bindings and delegates manifest, epoch, and WAL-interval
validation to the sealed-root codec. Thus root construction cannot silently add
an unpublished checkpoint dependency, substitute a later source manifest, or
bypass interval checks.

`DurableManifest::manifest_artifact_inputs` is the first manifest-side
population boundary. It derives the known checkpoint, manifest, adjacency,
relational, and append manifest paths from validated generation bindings and
their recorded lengths/raw content digests; it verifies those bytes first and
only then derives the domain-separated immutable identity, so the two digest
domains cannot be confused. It never enumerates directory entries. Each
family reader must append its physical pages and descriptor descendants from
the decoded manifest before calling the closure publisher. This split keeps
the proof compositional: manifest bindings prove names and expected identity,
family readers prove their internal transitive ranges, and the publisher
proves byte-for-byte immutable installation.

`CheckpointClosurePlan` makes the family-reader obligation executable. Before
publication each canonical, adjacency, property, relational, and append family
must either contribute its validated descendants or be explicitly marked
empty. The plan rejects duplicate family decisions and rejects publication
with any undecided family, so a caller cannot accidentally turn a partial
manifest walk into a branch root.

### Sealed WAL validation boundary

`crates/storage/src/sealed_wal.rs` validates one stable binary WAL generation
and hands its exact bytes to the immutable-object publisher. The caller must
hold the source publication barrier; this helper intentionally does not close
the active writer or switch a branch head.

Its proof is a prefix induction over cursor events. The WAL reader first proves
the generation header and replay-start LSN. For each `Entry`, the next expected
LSN is the previous LSN plus one, so the induction preserves a contiguous
half-open interval. A corrupt record or torn tail exits before publication,
while `Eof` is accepted only after the complete valid prefix. The file is then
read with the original bounded length and rejected if its length changes during
the read. Only those bytes are passed to the content-addressed publisher, which
rechecks their identity before installation. Thus a successful publication
corresponds to exactly the validated WAL interval; no partial suffix can be
selected. Rotation, new-WAL creation, head switching, and crash recovery remain
separate protocol obligations.

`prepare_wal_rotation` extends that boundary without acknowledging a head
switch. It first obtains the validated sealed-WAL result, derives the
successor start LSN from the validated end LSN, creates the successor with
`create_new`, writes its exact binary header, synchronizes the file, and then
synchronizes its parent directory. The old WAL is retained byte-for-byte. By
the sequence of durable prerequisites, a caller can expose the successor only
after its complete header is durable; a failure before selector publication
leaves the old head and old WAL authoritative, while a failed candidate is
removed where its outcome is known. The remaining uncertain directory-sync
case is returned as an error and must be handled by the caller's publication
poison/reopen protocol.

If that parent-directory synchronization fails after the successor file has
been written, `prepare_wal_rotation` removes the known candidate before
returning. The retry invariant is therefore preserved: a subsequent attempt
can use the same successor path and `create_new` remains exclusive, while the
old WAL and head stay authoritative throughout.

### Branch-head selector codec and CAS

`crates/storage/src/branch_head.rs` defines the v1 branch-head selector. It
binds nonzero project and branch identities, a strictly positive physical
generation, a sealed-root object reference, the logical commit epoch, and the
active WAL generation/start LSN/length/digest. The selector has a fixed field
order and CRC32C footer; unknown, truncated, checksum-invalid, trailing, or
incomplete values fail closed before a handle can use them.

The codec proof follows the bounded reader offset exactly as for the root
codec: every field advances only after a checked slice, and the final exact-end
check excludes hidden state. Validation establishes the identity and reference
invariants before encoding or publication. Head publication then reads the
current selector, requires matching project/branch identities and the caller's
exact physical generation, and accepts only a strictly newer generation. The
candidate is fully written and synchronized before `durable_replace_file`
changes visibility, so a failed pre-replacement operation leaves the old bytes
selected. A successful replacement selects one complete old or new selector;
the later crash-recovery integration must still prove how an uncertain
filesystem result is poisoned and reopened.

`active_wal_identity_from_file` supplies the active-WAL binding used by that
selector. It reads the complete bounded successor file and hashes those exact
bytes; the path and metadata length are never sufficient evidence. Thus, if a
rotation preparation has made the successor header durable, the recorded
length and digest identify the same byte image that recovery will open. A
length-limit failure occurs before an identity is returned, preserving the
old head as the only acknowledged selector.

`publish_prepared_wal_rotation` is the final handoff operation. It first
re-reads the current head and checks the caller's generation and branch
identity, then reads and hashes the already-prepared successor, constructs the
next physical generation, and invokes the generation-checked atomic selector
replacement. By this ordering, a missing or oversized successor leaves the
old selector untouched; once replacement succeeds, the selector's active WAL
identity is exactly the successor byte image prepared by the preceding
rotation step. The sealed root is an explicit precondition, so this helper
cannot acknowledge a head that has not already named an immutable root.

`publish_prepared_wal_rotation_with_root` closes the root-to-head ordering
boundary for the integrated handoff. It validates the supplied root, requires
that its ordered WAL references contain the exact `(start_lsn, end_lsn, object)`
tuple returned by rotation preparation, encodes the root, and publishes that
immutable root object before invoking the selector publication above. Induction
over the root's validated references shows that every object named by the new
selector was already installed before the selector became visible. If root
publication fails, the old selector remains authoritative; if selector
publication fails, the newly published root is unreachable and therefore safe
for later conservative reclamation. A root omitting the prepared WAL is
rejected before either publication, so the head cannot acknowledge a replay
range that the root does not prove. This is the ordering proof for the handoff
helper; crash recovery still needs filesystem-level old-or-new integration
tests in the branch-opening work.

### Catalog codec invariants

The catalog codec uses `HBCATV2\0` and version 2, little-endian integer fields,
length-prefixed bounded ASCII strings, UUID-order branch records, and a CRC32C
over every byte before the checksum footer. Decoding
rejects an unknown header/version, a truncated field, an invalid UTF-8 or
out-of-limit string, an unknown enum or optional-value marker, duplicate UUIDs
or names, a checksum mismatch, and trailing bytes. The project UUID is not a
branch UUID; `main` is parentless; generated `agent/` names are accepted only
as already-generated catalog values, while the custom-name constructor rejects
both reserved forms.

Catalog v2 removes the expiry timestamp field and the `Expired` state. State
bytes are `Creating=0`, `Ready=1`, `Deleting=3`, and `Deleted=4`; retired tag 2 is
rejected rather than reassigned. Both the old v1 header and an old version under
the new header are rejected. Development-only v1 catalogs require database
recreation; no compatibility decoder or automatic migration is provided. Branch
create fingerprints use the `hawdb-branch-create-v2` domain and omit expiry.
Reopen tests cover ready descendants, explicit parent deletion, and retained
idempotency receipts; these tests are not whole-system power-loss qualification.

The codec's safety argument is by induction over the record stream. The reader
starts at the header boundary and advances only after a checked slice exists;
therefore every successfully decoded field is within the checksum-covered
prefix. The per-record validation invariant establishes a non-nil UUID, a
valid name, bounded request metadata, and a legal lifecycle state. Assuming it
for the first *n* records, duplicate detection against the sorted UUID/name
order and validation of record *n+1* preserve the invariant for *n+1*. After
the declared count, the exact-end check proves that no unparsed bytes can be
treated as catalog state. Encoding sorts records before emitting them and
recomputes the checksum over the complete prefix, so equivalent validated
catalogs have one byte representation and any byte mutation is rejected unless
the integrity footer is also recomputed. This is a source-linked deductive
proof of codec boundaries; it is not a machine-checked refinement of the
future publication protocol.

The catalog state transitions use the same candidate-validation discipline. A
new reservation first checks the request receipt, parent identity/state/source
epoch, UUID/name uniqueness, and checked revision increments. Replaying an
identical key and fingerprint returns the retained UUID without changing any
byte; a different fingerprint is a conflict. Completion and abort accept only
`Creating/Pending`, while rename and the two delete phases each accept
only their predecessor state and the caller's exact metadata revision. Every
successful transition increments the catalog revision and the branch revision,
validates the complete candidate, and swaps it into the handle only after
validation. Thus, by induction over successful transitions, uniqueness,
parent-reference closure, protected `main`, and the lifecycle-state graph are
preserved; a failed precondition leaves the previous candidate untouched. The
metadata lock serializes these read/modify/validate/publish steps, while the
durable candidate protocol makes the replacement boundary explicit. This is a
protocol proof over the current implementation seam; branch sealing, object
publication, and crash recovery remain separate proof obligations.

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
release it, acquire the UUID lease, then revalidate the mapping and state
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

## File descriptor budgets and branch residency

Persisted branch count MUST NOT determine the number of resident file descriptors
(FDs). Creating or listing branches uses bounded temporary descriptors and closes
them after the operation. An unopened branch retains its durable catalog/head
and reachable objects without a permanently open per-branch lock, WAL, or data
file. Open-lock loss or FD-cache eviction never authorizes branch deletion.

Admitted branches retain their required open locks and private active WALs.
Data files are opened lazily. Within one process/project runtime, immutable
objects should share cached read handles keyed by their complete object identity
and canonical project identity; a child must not open a separate persistent
handle merely because it references the same object as its parent. Shared reads
must use positional I/O or equivalent synchronization, not a shared mutable file
offset. Mutable WALs and branch ownership locks are not immutable-cache entries.

A typed Rust resource configuration MUST supply a finite FD budget, with a safe
finite default, for all engine-owned descriptors in that runtime. Include
non-evictable branch locks and WALs, immutable-file cache entries, and temporary
open/recovery/checkpoint/seal/GC descriptors. The file cache uses the remaining
budget; limiting cache size alone is insufficient. Independently opened contexts
for the same project in the process must share the accounting domain rather than
multiply its budget. A budget configuration conflict must fail explicitly.
Other projects, host files, and other processes consume resources outside this
domain; do not claim the engine budget prevents every OS descriptor-limit error.

Reserve descriptor capacity before opening files, including transient operations.
Every failure/cancellation path releases its reservations and temporary handles.
Idle cached handles may be evicted under pressure; active I/O handles and open
locks cannot be evicted. When eviction cannot provide capacity, fail with a typed
resource-limit error and requested/available counts. OS descriptor exhaustion
must also produce a typed resource error without leaking handles or publishing
partial branch state. Do not introduce an unbounded wait or an unlimited fallback.

`USE BRANCH` must reserve enough capacity for target admission while preserving
the source branch. If the target cannot open within budget, release its temporary
resources and leave the original selection intact. After a successful switch,
release source ownership when no source reader, job, or other owner needs it.
An idle branch runtime must not retain non-evictable locks/WALs indefinitely.
Unsynchronized relaxed-mode writes remain subject to their documented durability
policy; closing descriptors is not a substitute for a completed sync barrier.

A reader snapshot or durable branch root pins object reachability, not necessarily
an open descriptor for every object. Evicting a cached FD leaves its logical pin
in place; reopening the immutable file must validate its identity. Active I/O
retains its handle until completion. GC must honor both durable roots and reader,
job, and publication pins regardless of whether the object currently has an FD.

Recovery, branch listing, and GC process heads and object metadata in bounded
batches, closing temporary handles promptly. They must not open one descriptor
per catalog entry or descendant at once. Checkpoint and background-maintenance
concurrency also participates in the same admission budget.

Expose typed metrics for admitted branch runtimes, engine-owned open/reserved FDs,
non-evictable and cached handles, high-water usage, cache hits/misses/evictions,
and descriptor-budget/OS-limit rejections. These are engine-domain counts, not
claims about the total process FD count. Monitoring must not open every branch.

Qualification must use deterministic small budgets and platform FD measurements
where available to prove:

- Creating/listing many unopened branches does not retain a descriptor per branch;
  repeated listing, multi-level forks, reopen, and failed creation leak no FDs.
- Parent/child/sibling reads reuse cached immutable handles, and eviction/reopen
  preserves identity and data isolation without sharing mutable file offsets.
- Active locks/WALs count against admission; oversubscription and target-switch
  failure leave the source usable and descriptor/reservation counts bounded.
- Repeated switches release unneeded source resources; a surviving snapshot/job
  keeps its original data reachable even when idle data-file FDs are evicted.
- Concurrent maintenance, GC, and admission stay within budget, including
  temporary descriptors. Inject OS-limit and I/O errors after each acquisition
  and verify no leaks, partial publication, or deletion of referenced objects.

These are implementation and resource-verification requirements; existing open
helpers and caches are not claimed to satisfy them merely because branches share
immutable storage.

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
durable prefix. Sealing does not change the source logical commit epoch.
In synchronous mode, acknowledged source commits are already durable. In relaxed
mode, sealing MUST synchronize the complete selected committed prefix before
publishing either head. Thus a successful fork makes its captured schema/data
power-loss durable in both parent and child. Failure before completing that
barrier does not strengthen the earlier relaxed acknowledgments. Publication
uncertainty requires reopen/recovery, not an assumption that sealing rolled back.

Create then follows these durable transitions:

1. Validate request syntax/limits and consult its idempotency receipt first.
   For a new request, resolve and lock the source; revalidate its expected
   revision and state; seal if needed.
2. While the sealed source is pinned, atomically publish a `Creating` catalog
   record with UUID, reserved name, immutable base digest, full request
   fingerprint, and pending outcome. It becomes a global GC root immediately.
3. Create and sync the child's empty active WAL and private head referencing
   that base. Parent and child never share a mutable WAL or selector.
4. Atomically publish `Ready` and a successful create outcome in the catalog.
   Only then return the branch descriptor. A lost reply is resolved by receipt
   replay. Failure before step 2 leaves no branch; unreferenced objects are
   later collectible. Failure after step 2 requires recovery, not a new ID.

The current storage implementation makes steps 2--4 explicit in
`branch_catalog::create_branch_from_parent`. `reserve_create_file` durably
records `Creating` and returns the exact metadata revision used by completion.
`create_child_branch_head_from_parent` reads the parent selector and rejects a
generation, commit epoch, project identity, or sealed-root mismatch before it
creates either child file. `create_child_branch_head` creates the child WAL and
head with exclusive creation, syncs both files and their parent directory, and
holds the child directory lease through catalog completion. Therefore, by
induction over these durable boundaries, a successful `Ready` record implies a
complete private child WAL/head pair whose root is the validated parent root;
the parent selector and mutable WAL are never modified.

`recover_create_file` is the corresponding restart transition. A missing head
or WAL is a known incomplete operation and becomes terminal `Deleted`; a
complete pair must match the catalog project/branch identity, source epoch,
root digest, WAL generation, start LSN, length, and digest before `Creating`
can become `Ready`. Any mismatch or uncertain I/O leaves `Creating` unchanged
and retains the candidate for a later retry. This proves the implementation's
old-or-new admission boundary, while full logical-data replay remains a
database integration obligation.

If the parent advances or is subsequently deleted, the child's base digest and
parent UUID remain unchanged. Lineage does not retain the parent's directory;
the child's own root references retain the required immutable objects.

## Idempotency, lifecycle and rejects

Create uses a nonempty, bounded, project-scoped opaque key. Its versioned
fingerprint covers the caller's typed parent selector, expected source token,
optional name (including the distinction between absent and explicit), and owner.
Encode options and byte lengths unambiguously. Store the resolved UUID separately.
Retries compare the original request before resolving a name
again, so rename, deletion, or name reuse cannot redirect a recorded request.

The same key/fingerprint returns the reserved original identity and its current
pending/succeeded/aborted/deleted outcome. It does not recreate a deleted branch.
Different input with the same key is always `IdempotencyConflict`, including
after deletion. In-progress create can return
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
| `USE BRANCH` on `Ready` | Acquire/revalidate the open lock, recover the selected state, and bind the current context, preserving its configured read-only and durability settings. |
| Open an independently leased UUID | `AlreadyOpen`; other branch UUIDs remain independently openable. |
| Open `Deleting` / `Deleted` | `Deleting` / `Deleted` by retained UUID; removed names may be unknown. |
| Delete `Ready` | Publish `Deleting`, reject new opens and new work on its existing handle. |
| Delete already `Deleting` / `Deleted` with the same identity | Idempotent current outcome; cannot affect a reused name. |
| Delete/rename `main` | `ProtectedBranch`. |
| Decode, checksum, unknown version, or incomplete selected closure | Typed storage integrity error; no usable handle or sweep. |

`describe` and bounded/paginated `list` expose UUID/name, lineage, revision,
state and owner without mutable paths. Their catalog revision makes
pagination changes explicit. Owner metadata is descriptive; hosts remain
responsible for authorization. Lifecycle statements return bounded query results and typed library errors.
JSON/CLI wrappers may later derive from the embedded query runtime.

There is no time-based admission or automatic expiry in P0. An explicit delete
permits already-admitted commits/checkpoints to finish or abort atomically and
rejects new transactions. It MUST NOT revoke a pin, truncate a WAL under a reader,
or interrupt publication into a partially durable state. A successful logical
delete requires a durable tombstone before acknowledging success; physical
cleanup may remain pending without resurrecting the branch after restart.

## Recovery, deletion and reclamation

Recovery validates the project selector, catalog, and selected per-branch heads
before serving them. It holds metadata serialization for state transitions and
revalidates leases; another process's live pending operation is not a crash.

| Durable interruption point | Recovery decision |
| --- | --- |
| Objects/staging exist without a catalog create record | Retain while an owner is live; otherwise mark as orphan candidates. |
| `Creating`, no durable child head | Publish terminal-aborted `Deleted` with the same receipt/UUID; release name reservation. |
| `Creating`, complete child head and base closure | Finish `Ready` with the same successful receipt/UUID. |
| `Creating`, existing but corrupt/ambiguous child head | Fail closed; retain all possibly referenced objects and report integrity failure. |
| Head switch during commit/seal/checkpoint | Use the complete selected old or new head and its exact WAL replay interval; do not mix generations. |
| `Deleting`, any live owner/publication/snapshot pin | Keep it non-openable and retained; report cleanup pending. |
| `Deleting`, no remaining owner/pins | Persist `Deleted` before physical cleanup and name reuse. |
| `Deleted`, interrupted directory cleanup or sweep | Retry bounded cleanup; retain receipt and identity tombstone. |

GC computes the transitive closure of all `Ready` heads/bases,
all `Creating`/`Deleting` recovery records, all live leases and reader pins,
all publication candidates, and any explicitly registered historical pins.
The catalog itself, active WALs, and stable lock inodes are not ordinary object
sweep candidates. Unopened branches retain their complete state indefinitely.
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

### Unreferenced DDL artifacts after relaxed commits

In `SyncOnCheckpoint`, table/index artifacts may reach disk before their WAL
transaction becomes durable. Power loss can then recover a catalog that never
committed those artifacts. Such files are candidates for reclamation, not proof
of a committed schema change. Startup first recovers and validates catalog,
heads, and WAL; it must not infer tables or indexes from directory contents.

A subsequent bounded maintenance sweep may reclaim only inventoried candidates
proven unreachable from every surviving branch, reader/job pin, and pending
publication. Revalidate under the reachability barrier before unlinking. A file
unreferenced by the current branch alone is not an orphan: another branch or
pending create may own it. Unreadable metadata, incomplete recovery, or unknown
ownership retains the file and reports retry-required evidence. Eager startup
GC is not required for successful opening; automatic repair/truncation of shared
WAL remains prohibited. Interrupting cleanup must be restart-idempotent.

Fault injection must cover an unsynchronized DDL transaction with artifacts
already persisted, a shared artifact still used by a sibling, a pending create
using the same root, and a crash during cleanup. Recovery must preserve durable
commits and schema/data atomicity; reclamation must never remove a live dependency.

### Conservative immutable-object sweep implementation

`ImmutableObjectStore::reclaim_unreachable` implements the storage-only part of
this protocol. Its caller supplies the complete object inventory currently
owned by the catalog/lease layer and the sealed-root references that survived
the mark phase; the object store never treats a directory listing or a
filename as an ownership proof. Before unlinking anything it reads and
identity-validates every reachable root, recursively decodes each sealed root
to mark its durable-manifest, checkpoint, and sealed-WAL references, and
validates every inventoried candidate. Therefore an unreadable root, closure
member, or candidate fails before the first deletion, while files outside the
explicit inventory remain untouched. A missing unreachable candidate is accepted as an
already-completed unlink, so retry after a crash is idempotent; a missing
reachable object remains fatal. The returned typed report counts retained
inventory entries and reclaimed objects/bytes. Branch leases, pending catalog
records, and directory cleanup still belong to the caller and must be included
in the inventory/root snapshot before invoking this primitive.

## Model and implementation qualification

The [previous model report](../tla/BRANCH_LIFECYCLE_PROOF.md) is superseded as
qualification evidence for this contract. A revised model must cover persistent
branches without expiry, multi-level forks, atomic schema/data publication,
open-lock loss without branch loss, explicit deletion, and GC reachability.
Model changes require rerunning positive invariants, negative controls, and
reachability witnesses. Abstract atomic publication does not establish torn-write,
write-reordering, OS-locking, or filesystem durability behavior; those require
implementation-level power-loss fault injection. No revised-model result is
claimed here.

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
  open/delete races, corrupt closure discovery and publication during GC;
- cover every canonical artifact family, minimal/default facade profiles,
  focused Cargo/Bazel tests and the repository's required local fuzz command.

### Follow-up delivery slices

Each slice targets `main` directly after its prerequisite lands; none is a
stacked PR against an unmerged feature branch. Keep the tracker open until the
runtime and qualification obligations are complete:

1. **Model revision (#776):** remove expiry transitions; add multi-level forks,
   selection/open-lock loss, and atomic schema/data state. Model synchronous and
   relaxed acknowledgment separately. Rerun invariants, negative controls, and
   witnesses; replace historical evidence only with actual results.
2. **Catalog lifecycle (#777, #775):** remove `expires_at` and `Expired` from
   requests, codecs, admission, and recovery. Update the greenfield format and
   tests without compatibility migrations for earlier development databases.
3. **SQL selection and writable recovery (#780, #775):** add AST/dispatch and
   context-local `USE BRANCH`, deferred admission, source-preserving switch
   failure, shared project FD budgets, and complete private active-WAL replay. Finalize administrative SQL
   grammar/result budgets and ensure SQL uses the storage lifecycle kernel.
4. **DDL and durable fork publication (#779, #780):** isolate schema, migration
   records, caches, and checkpoint artifacts; fork from the exact committed
   state of modified children. Qualify both durability modes and metadata sync.
5. **Reclamation and power-loss qualification (#778, #774):** implement bounded
   orphan cleanup after validated recovery, preserve descendants and pending
   publications, and inject lost writes, torn writes, reordering, and interrupted
   sweep. Verify bounded temporary descriptors and orphan-cleanup FD accounting.
   Register focused regression and local fuzz coverage. Runtime slices
   need their own failure tests; this final slice does not defer their safety.

No issue is complete merely because a model passes; the corresponding runtime
acceptance criteria require source-level and executable implementation evidence.
