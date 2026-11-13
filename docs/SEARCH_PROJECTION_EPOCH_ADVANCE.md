# Advancing a search projection's source epoch without rewriting it

Issue: `nowledge-co/hawdb#1756`.

A host often needs the search projection's source graph commit epoch to match
the graph epoch after graph commits that changed nothing the projection
indexes, such as import bookkeeping or a property `DEFAULT` declaration. Before
this change the only way was an empty apply plus a checkpoint. For a
resident projection that rewrites every snapshot, payload, lexical, vector and
layout file (126–182 MiB per generation in one host benchmark, about 20 times
per preparation run). The incremental out-of-core path built and published a
new generation for the empty delta.

`EmbeddedStoreHandle::advance_search_projection_source_epoch(target)` moves the
epoch by publishing **one new selector manifest** instead, after HawDB proves
from its own change log that the skipped commits did not affect the
projection.

## API

```rust
let report = handle.advance_search_projection_source_epoch(graph_epoch)?;
```

The same operation is available below the facade as
`EmbeddedStore::advance_search_projection_source_epoch`,
`SearchIndex::advance_source_graph_commit_epoch` (resident) and
`SearchOutOfCoreReader::advance_source_graph_commit_epoch` (out-of-core). The
two search-crate methods publish without checking the graph. Only the embedded
store calls them after the change-log proof below.

It covers both projection modes:

| Mode | Used by | What is published |
| --- | --- | --- |
| Resident | `apply_initial_import_projection_delta_and_checkpoint`, `apply_search_projection_delta_and_checkpoint` without incremental maintenance | A selector at generation `G + 1` over generation `G`'s snapshot, segment, lexical, vector and layout files. |
| Incremental out-of-core | `with_incremental_search_maintenance`, catch-up | A selector at generation `G + 1` over the active segments and mutation runs. |

`SearchProjectionEpochAdvanceReport` returns the epochs before and after,
whether anything was published (`advanced`), the active selector generation,
`content_source_graph_commit_epoch` (the epoch stamped in the content files),
`metadata_bytes_written` and the number of change-log entries checked.

- `target == source` returns `advanced == false` and writes nothing.
- `target < source` is a `Semantic` error.

## Cost

One manifest write of a few kilobytes (about 2.5 KiB in the tests), one file
sync, one rename and one directory sync. The cost does not depend on corpus
size. No file is created, linked, replaced or rewritten other than the
selector. The tests compare every other file's inode and length before and
after the call.

The change-log check reads the retained entries in the range and looks up the
current state of each captured node. That is proportional to the number of
captured changes in the range, not to the corpus. A resident projection also
proves that its in-memory documents are exactly the checkpointed ones by
comparing their documents digest with the selector's. That hashes the
resident documents once in memory, with no projection I/O.

The superseded selector generation's files are the active ones, so nothing
becomes reclaimable. A later content publication or cleanup pass treats the
advanced generation like any other.

## Safety contract

Advancing from `S` to `T` asserts that no graph commit in `(S, T]` changed
anything that maps to a search projection document. HawDB does not trust the
caller. Under the store's write lock it checks the database's own search change
log (`search_projection_capture`, the same capture incremental catch-up
replays) and advances only if:

1. `T` is not beyond the graph's commit epoch.
2. The change log covers the whole range. Its resume floor must not be above
   `S`. A commit whose capture could not be recorded raises that floor, and so
   do trimming and the reopen boundary of the retained log.
3. Every retained capture in the range is empty for the projection:
   - it deletes no search document;
   - it changes no relational primary key and requires no relational rebuild
     (host-mapped rows are never assumed unaffected);
   - every captured node that still exists has no label that maps to a search
     projection kind (`Memory`, `Message`, `Entity`, `Source`, `SourceChunk`,
     `Community`).

Commits that create or update nodes of other labels pass, and so do
additive schema statements, whose captures are empty since
`nowledge-co/hawdb#1749`. Labels are fixed at node creation. A captured node that
no longer exists is skipped, exactly as catch-up hydration skips it. If that
node was projected, its deletion recorded the document deletion and the range
is refused through that. If it was created and deleted inside the range, it
never reached the projection.

Otherwise nothing is published and the call returns
`HawDBError::SearchProjectionEpochAdvanceRefused { reason,
source_graph_commit_epoch, target_graph_commit_epoch, message }`. This is not
a storage failure. The graph and the projection are unchanged, and the host
falls back to an ordinary apply or catch-up:

| `reason` | Meaning |
| --- | --- |
| `ProjectionChanged` | A commit in the range touched projected data. The message names the first commit. |
| `ChangeLogGap` | The retained change log starts after `S`. |
| `RangeUnavailable` | The projection has no source epoch, or `T` is beyond the graph's commit epoch. |
| `UncheckpointedChanges` | A resident projection has applied pages, an applied epoch or documents that are not checkpointed, so its durable files do not describe it. Checkpoint first. |

Bindings report the kind `search_projection_epoch_advance_refused`. Python
raises `ExecutionError` with `kind`, `reason`, `source_graph_commit_epoch`,
`target_graph_commit_epoch` and `retryable = False`.

Registered Consumer projections are refused (`Semantic`), because their owner
publishes their generations.

## Format and reopen

The selector gains an optional field, `content_source_graph_commit_epoch`. It
is present only after an advance and names the epoch the content files keep.
The advance copies every other field and every artifact reference unchanged,
increments `generation` and sets `source_graph_commit_epoch` to `T`. Chained
advances keep the original content epoch. The next content publication or
resident checkpoint writes a selector without the field.

- **Resident reopen.** The snapshot, lexical manifest and vector artifact are
  validated against the content epoch. The projection then reports `T` as
  both its source and durable epoch. Alias repair after a crashed checkpoint
  uses the content generation.
- **Out-of-core reopen and refresh.** Each segment keeps validating against
  its own epoch, so a refresh reuses every open segment.
- **Verification.** All of these accept the advanced projection:
  `SearchOutOfCoreReader::open`, `SearchIndex::open`, the full closure
  verification of a cleanup pass (`retry_search_projection_cleanup`),
  catch-up, and `SearchDocumentSetDigest` comparison with
  `rebuild_out_of_core_search_projection`. The tests check the digest after
  the host's catch-up.

Manifests without the field serialize byte-identically to before. An older
HawDB refuses to open an advanced selector because it has an unknown field.
HawDB makes no on-disk compatibility promise yet.

## Crash safety

The selector is written to a temporary file, synced, and renamed over the
active selector, followed by a directory sync. That rename is the only commit
point, as for every projection publication. After a crash or reopen the
projection is entirely at `S` (rename not durable) or entirely at `T`. The
in-memory epoch moves only after the rename returns. A leftover temporary
selector is never read.

## Tests

- `out_of_core::epoch_advance::tests` (hawdb-search) cover:
  - one selector written and no other file changed, for resident and
    out-of-core closures;
  - reopen, refresh and an out-of-core reopen of a resident directory at the
    target;
  - chained advances, and a later checkpoint or delta that clears the content
    epoch;
  - injected aborts before and after the selector rename, which reopen at
    exactly one epoch with identical documents and hits;
  - stale-reader conflicts and resident `UncheckpointedChanges` refusals.
- `embedded_store::tests::search_epoch_advance_tests` (hawdb) cover:
  - the resident initial-import path and the incremental path, each advancing
    across bookkeeping commits and a `DEFAULT` declaration over a 300-document
    corpus while writing only the selector;
  - a typed refusal after one `Memory` write and after one `Memory` deletion;
  - a typed refusal across a trimmed change log, and a target beyond the graph;
  - reopen at the advanced epoch;
  - catch-up after the advance, whose epoch and `SearchDocumentSetDigest`
    match `rebuild_out_of_core_search_projection`.
