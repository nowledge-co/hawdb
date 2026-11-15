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

`runtime_status()` reports the active set as `externally_owned_search_kinds`
and in JSON as `projection.externally_owned_kinds`.

## Rebuilds, metadata repair and the rows-required epoch

HawDB never rebuilds a search projection on its own. A changefeed gap makes
catch-up refuse, and background maintenance only ranks a rebuild candidate;
the host runs the rebuild. Both rebuild paths carry host documents over, so
search keeps serving them, and both raise the rows-required epoch, so the
host re-pushes:

- **Resident** (`Database::rebuild_search_projection` and its background and
  scheduled variants, all `SearchIndex::rebuild_from_graph`): for every live
  node of an owned kind, the document the index holds is carried over
  unchanged instead of being derived. Documents whose node no longer exists
  are dropped, and a node without a host document gets none. Metadata repair
  (`repair_metadata_from_graph`) skips owned kinds. Both raise the epoch of a
  persistent index before they start.
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
  - The set record and a raised rows-required state (the previous
    directory's epoch plus one) are written into the stage before it is
    installed, so they appear atomically with the generation.

The epoch is always raised after a rebuild or repair. Host writes made to
the current projection during an out-of-core rebuild, or after installation
until the host reopens on the target, are not in the target. Host deltas are
not in the graph change log, so catch-up cannot replay them. A high-water
mark kept in memory could not cover the window between installation and the
host's reopen, which can span processes.

When HawDB cannot prove the host's documents are present, it raises a
durable rows-required epoch:

- ownership is first recorded for a projection directory, either a new
  directory or a removed record that is being restored;
- any rebuild or metadata repair runs, as described above;
- a reset rewrites the records (see below).

`runtime_status().externally_owned_rows_required_epoch` (JSON
`projection.externally_owned_rows_required_epoch`) is `Some(epoch)` while
rows are required. It is read from the directory on every call, so
read-only opens report it too.

After re-pushing its rows, the host calls
`EmbeddedStoreHandle::acknowledge_externally_owned_rows_restored(epoch)` with
the epoch it observed:

- if that epoch is still the required one, the call clears the requirement
  and returns `Ok(true)`;
- if the epoch is stale (a later raise happened) or nothing is required, the
  call returns `Ok(false)` and changes nothing. The host re-reads the epoch,
  re-pushes and acknowledges again;
- read-only opens are refused as `Semantic`.

Epochs only grow and are never reused.

## Unreadable records

A record that cannot be parsed returns the typed
`HawDBError::SearchProjectionOwnershipMarkerCorrupt { projection, marker,
message }`. It is classed as execution, not `StorageIntegrity`, so hosts do
not treat it as poisoning, and nothing is changed:

- an unreadable set record refuses the open, because HawDB cannot tell which
  kinds catch-up may derive;
- an unreadable rows-required state makes `runtime_status`, the epoch query
  and acknowledgement fail with this error. They never report the rows as
  restored. A rebuild that finds it unreadable raises a time-based epoch.

Recovery is an explicit, writable open:
`EmbeddedStoreOpenOptions::with_externally_owned_search_kinds_reset()`
together with the intended set. It rewrites both records from that set and
raises the epoch. An unreadable epoch is replaced by the current time in
microseconds, which exceeds every epoch a counter could have reached.
Read-only and qualified out-of-core opens refuse a reset as `Semantic`.

Qualified out-of-core opens never record the set; only maintenance opens
do. A preparation flow that opens in qualified mode must therefore still
declare the set, and the projection must have been recorded by a maintenance
open.

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
hawdb externally owned rows v1
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
| absent | non-empty | rows-required epoch raised, then the set recorded durably (temporary file, sync, rename, directory sync) |
| equal | any | open proceeds |
| different, including empty against non-empty | any | `HawDBError::SearchProjectionOwnershipMismatch`, nothing changed |

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
  - the rows-required epoch: raised on first record and on restore, the race
    where a stale acknowledgement does not clear a newer epoch, read-only
    visibility, and refusal of a read-only acknowledgement;
  - a manufactured changefeed gap (a change log of two entries), recovered by
    the resident rebuild plus metadata repair and by the out-of-core
    rebuild. Documents of live nodes are carried over byte-identical, the
    document of a node deleted during the gap is dropped, a node without a
    host row gets none, other kinds are derived, and the epoch is raised past
    the acknowledged one. With carry-over made impossible, owned documents
    are absent until the host re-pushes them;
  - a host write to the current projection during an out-of-core rebuild:
    the rebuild still verifies, and the target raises the epoch;
  - unreadable set and rows records: typed refusals; a read-only reset is
    refused; a reset reopen rewrites both records, and the time-based epoch
    exceeds every earlier one.
- `projection_ownership::tests` (hawdb-search) covers the record formats, the
  refusal, acknowledgement of only the current epoch, malformed records, and
  the reset.
- `generation_cleanup::tests::cleanup_preserves_the_externally_owned_records`.
