# Externally owned search projection kinds

Issue: #1814.

## Problem

Graph changefeed catch-up derives one `SearchProjectionRow` per changed node
carrying a projection label and replaces the whole search document. Every
`SearchProjectionKind` is derived this way (`search_projection_kind_from_label`):
`Memory`, `Message`, `Entity`, `Source`, `SourceChunk` (also `source_chunk`
and `chunk`) and `Community`, each also in lowercase. Derived rows carry `embedding: None` and copy
every node property into metadata. A host that writes these documents itself
through `apply_search_projection_delta_and_checkpoint` (its own text,
metadata shape and embedding) lost them on the next graph-only update, such
as an access-count bump.

## API

```rust
let options = EmbeddedStoreOpenOptions::with_search_projection(graph, search, mode)
    .with_externally_owned_search_kinds([
        SearchProjectionKind::Memory,
        SearchProjectionKind::Entity,
    ]);
```

For the declared kinds, every changefeed catch-up entry point of the embedded
store (`catch_up_search_projection*`, scheduled and background graph deltas,
resident and incremental out-of-core) derives no rows:

- a graph-only update leaves the host's document byte-identical, embedding
  included;
- deleting the graph node still removes the document;
- a node created after opt-in has no document until the host supplies one.

Other kinds stay graph-derived, and host deltas, including rows returned by
a catch-up batch hydrator, are applied as before. The default is the empty
set, which keeps today's behaviour for every kind.

Projection consumers keep their own catch-up and are unaffected. A host
that drives `Database` directly can set the policy on its index with
`SearchIndex::set_externally_owned_kinds`.

`runtime_status()` reports the effective set as `externally_owned_search_kinds`
and in JSON as `projection.externally_owned_kinds`: the set recorded for the
projection, read on every call. It equals the requested set once recorded, and
is empty when nothing was requested or when an open that may not record finds
no record yet (see the open rules below). The policy installed on the index,
and `EmbeddedStore::externally_owned_search_kinds()`, stay the requested set
in every case, so catch-up never derives documents of a declared kind.

## Rebuilds, metadata repair and the rows-required fence

HawDB never rebuilds a search projection on its own. A changefeed gap makes
catch-up refuse, and background maintenance only ranks a rebuild candidate;
the host runs the rebuild. Both rebuild paths carry host documents over, so
search keeps serving them, and both raise the rows-required fence, so the
host re-pushes:

- **Resident** (`Database::rebuild_search_projection` and its background and
  scheduled variants, all `SearchIndex::rebuild_from_graph`): for every live
  node of an owned kind, the document the index holds is carried over
  unchanged instead of being derived. Documents whose node no longer exists
  are dropped, and a node without a host document gets none. Metadata repair
  (`repair_metadata_from_graph`) skips owned kinds. Both raise the epoch
  within the current projection incarnation before they start.
- **Out-of-core** (`EmbeddedStoreHandle::rebuild_out_of_core_search_projection`):
  graph rows of owned kinds do not reach the host's `hydrate_graph_rows`.
  - In the build pass, each live node's document is looked up in the store's
    current projection and written unchanged. Lookups are bounded to 64 MiB
    per hydration batch; a larger batch fails the rebuild with the typed
    `ReadBudgetExceeded`, so lower `hydration_batch_rows`. If the current
    projection is unreadable at the start, owned rows are dropped.
  - The verification pass excludes owned kinds from the compared digests,
    and only the document total includes them. A host writing to the current
    projection during the rebuild therefore cannot make verification fail.
  - `visit_host_rows` output is applied as before.
  - The set record and a rows-required state with a fresh projection
    incarnation are written into the stage before it is installed, so they
    appear atomically with the generation. The source directory's state is
    not used to construct this fence.

The fence is always raised or replaced after a rebuild or repair. Host writes made to
the current projection during an out-of-core rebuild, or after installation
until the host reopens on the target, are not in the target. Host deltas are
not in the graph change log, so catch-up cannot replay them. A high-water
mark kept in memory could not cover the window between installation and the
host's reopen, which can span processes.

When HawDB cannot prove the host's documents are present, it raises a
durable rows-required fence. The fence is an
`ExternallyOwnedRowsToken { incarnation, epoch }`:

- ownership first recorded for a projection directory creates an incarnation
  and epoch 1;
- resident rebuild and metadata repair increment the epoch in that same
  incarnation;
- an out-of-core rebuild into another directory creates a fresh incarnation
  at epoch 1;
- a reset rewrites the records with a fresh incarnation (see below).

`runtime_status().externally_owned_rows_required` (JSON
`projection.externally_owned_rows_required`, with `incarnation` and `epoch`
fields) is `Some(token)` while rows are required. It is read from the
directory on every call, together with the set record, so read-only opens
report it too, including a fence recorded by a writable open after they
opened.

After re-pushing its rows, the host calls
`EmbeddedStoreHandle::acknowledge_externally_owned_rows_restored(token)` with
the complete token it observed:

- if that token is still the required one, the call clears the requirement
  and returns `Ok(true)`;
- if the token is stale (a later raise or a different projection incarnation
  happened) or nothing is required, the call returns `Ok(false)` and changes
  nothing. The host re-reads the token, re-pushes and acknowledges again;
- read-only opens are refused as `Semantic`.

Epochs only grow within an incarnation. Incarnations are immutable and never
copied to a rebuilt directory, so an old acknowledgement cannot match a new
projection even when both epochs are 1.

## Unreadable records

A record that cannot be parsed returns the typed
`HawDBError::SearchProjectionOwnershipMarkerCorrupt { projection, marker,
message }`. It is classed as execution, not `StorageIntegrity`, so hosts do
not treat it as poisoning, and nothing is changed:

- an unreadable set record refuses the open, because HawDB cannot tell which
  kinds catch-up may derive;
- an unreadable or missing rows-required state on an owned projection makes
  open, `runtime_status`, the token query and acknowledgement fail with this
  error. They never report the rows as restored. An out-of-core rebuild
  already running from that projection does not trust the unreadable state;
  its target receives a fresh incarnation.

Recovery is an explicit, writable open:
`EmbeddedStoreOpenOptions::with_externally_owned_search_kinds_reset()`
together with the intended set. It rewrites both records from that set and
creates a fresh incarnation at epoch 1. Recovery does not derive an identity
from wall-clock time or from unreadable bytes.
Read-only and qualified out-of-core opens refuse a reset as `Semantic`.

Qualified out-of-core opens never record the set; only maintenance opens
do. A preparation flow that opens in qualified mode must therefore still
declare the set, and the projection must be recorded by a maintenance open
before its rows-required fence applies; until then the qualified open reports
no fence and an empty effective set.

## Durable records

The set is recorded in the sidecar file `.externally_owned_search_kinds`,
and the rows-required state in `.externally_owned_rows_required`. Both live
in the projection directory, beside `.reindex_needed` and
`.projection_metadata_repair_needed`:

```text
hawdb externally owned search kinds v1
memory
entity
```

```text
hawdb externally owned rows v2
incarnation 019a9f2e-6eb1-7a34-8c15-217df59e3822
epoch 2
required
```

The state line is `required` or `restored`.

It is not a selector manifest field. HawDB 0.31 through 0.33 refuse unknown
manifest fields, so a manifest field would make the projection unreadable
after a downgrade. Those releases ignore both sidecar files:

- generation cleanup only considers names that parse as artifacts
  (`CleanupCandidate::parse`), and this name does not;
- the headless-root check of captured cleanup refuses only `search_` names;
- the resident scratch sweep only removes `search_projection*` and
  `search_lexical.manifest` temporaries;
- the born-reader control-child check names only the two repair markers.

Cleanup on this release also excludes both records and their write
temporaries explicitly.

Open rules (`EmbeddedStoreOpenOptions` with a search projection):

| Record | Requested set | Result |
| --- | --- | --- |
| absent | empty | nothing is written; behaviour unchanged |
| absent | non-empty | rows-required fence created or raised, then the set recorded durably (temporary file, sync, rename, directory sync) |
| equal | empty | open proceeds |
| equal | non-empty, valid rows state | open proceeds |
| equal | non-empty, missing or unreadable rows state | typed `SearchProjectionOwnershipMarkerCorrupt`; explicit reset required |
| different, including empty against non-empty | any | `HawDBError::SearchProjectionOwnershipMismatch`, nothing changed |

Read-only (`ShadowReadOnly`) and qualified out-of-core opens may not record.
For them the absent/non-empty row differs: the open writes nothing and the
projection counts as not opted in yet (#1845). This happens, for example,
for a host that opts in after its projections were already prepared.

After open, `externally_owned_rows_required()` and `runtime_status()` re-read
the set record on every call when the requested set is non-empty:

| Record now | Open that recorded or verified the set | Open that may not record |
| --- | --- | --- |
| absent | typed `SearchProjectionOwnershipMarkerCorrupt` (record removed out of band) | `None`, effective set empty |
| equal | rows state as recorded (`None` or `Some(token)`); missing or unreadable state is `SearchProjectionOwnershipMarkerCorrupt` | same |
| different | `SearchProjectionOwnershipMismatch` | same |

So a read-only opener observes the fence on its next call once a writable
open records the set, without reopening. An empty requested set reports
`None` and an empty set without reading the record, as before.

Every opener (for example a preparation flow and the runtime) must pass the
same set; a difference is refused rather than diverging silently. A missing
record, for example removed by an operator, is restored by the next
maintenance open. Read-only (`ShadowReadOnly`) and qualified out-of-core
opens compare but never write. Declaring kinds without a search projection
is refused as `Semantic`. Changing the set requires rebuilding the projection
into a new directory.

## Tests

- `embedded_store::tests::externally_owned_search_kinds_tests` covers:
  - resident and out-of-core: a graph-only update keeps the host document, a
    node delete removes it, other kinds stay derived, and reopen keeps the
    mode;
  - the default open replaces the document as before and writes no record;
  - a different set is refused without changing anything, and a missing
    record is restored;
  - all six kinds owned at once keep their host documents until their nodes
    are deleted;
  - the rows-required fence: created on first record and raised on restore,
    the race where a stale acknowledgement does not clear a newer epoch,
    read-only visibility, and refusal of a read-only acknowledgement;
  - a manufactured changefeed gap (a change log of two entries), recovered by
    the resident rebuild plus metadata repair and by the out-of-core
    rebuild. Documents of live nodes are carried over byte-identical, the
    document of a node deleted during the gap is dropped, a node without a
    host row gets none, other kinds are derived, and the target receives a
    fresh incarnation. With carry-over made impossible, owned documents
    are absent until the host re-pushes them;
  - a host write to the current projection during an out-of-core rebuild:
    the rebuild still verifies, and the target publishes a fresh fence;
  - missing and corrupt source rows states during out-of-core rebuild: the
    target receives a fresh incarnation and rejects the retained source token;
  - unreadable set and rows records: typed refusals; a read-only reset is
    refused; a reset reopen rewrites both records with a fresh incarnation and
    rejects acknowledgements from the previous incarnation.
  - read-only and qualified out-of-core opens of an unrecorded projection
    with a non-empty set: no rows required and an empty effective set; a
    later writable record becomes visible on the same handle; a later record
    that differs is a mismatch; a recording open whose record disappears
    fails closed (#1845).
- `projection_ownership::tests` (hawdb-search) covers the record formats, the
  refusal, acknowledgement of only the current token, missing/malformed
  records, the reset, and observing a record written after open.
- `generation_cleanup::tests::cleanup_preserves_the_externally_owned_records`.
