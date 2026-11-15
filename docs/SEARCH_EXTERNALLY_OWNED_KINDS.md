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

Not covered: full rebuilds (`rebuild_search_projection`,
`rebuild_out_of_core_search_projection`) and metadata repair from the graph
still derive every kind; reapply the host's documents after them. Projection
consumers keep their own catch-up and are unaffected. A host that drives
`Database` directly can set the policy on its index with
`SearchIndex::set_externally_owned_kinds`.

`runtime_status()` reports the active set as `externally_owned_search_kinds`
and in JSON as `projection.externally_owned_kinds`.

## Durable record

The set is recorded in the sidecar file `.externally_owned_search_kinds` in
the projection directory, beside `.reindex_needed` and
`.projection_metadata_repair_needed`:

```text
hawdb externally owned search kinds v1
memory
entity
```

It is not a selector manifest field. HawDB 0.31 through 0.33 refuse unknown
manifest fields, so a manifest field would make the projection unreadable
after a downgrade. Those releases ignore the sidecar file:

- generation cleanup only considers names that parse as artifacts
  (`CleanupCandidate::parse`), and this name does not;
- the headless-root check of captured cleanup refuses only `search_` names;
- the resident scratch sweep only removes `search_projection*` and
  `search_lexical.manifest` temporaries;
- the born-reader control-child check names only the two repair markers.

Cleanup on this release also excludes the record and its write temporaries
explicitly.

Open rules (`EmbeddedStoreOpenOptions` with a search projection):

| Record | Requested set | Result |
| --- | --- | --- |
| absent | empty | nothing is written; behaviour unchanged |
| absent | non-empty | recorded durably (temporary file, sync, rename, directory sync) |
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

- `embedded_store::tests::externally_owned_search_kinds_tests`: resident and
  out-of-core graph-only update keeps the host document, node delete removes
  it, other kinds stay derived, reopen keeps the mode, the default replaces
  the document as before and writes no record, a different set is refused
  without change, a missing record is restored, and all six kinds owned at
  once keep their host documents until their nodes are deleted.
- `projection_ownership::tests` (hawdb-search): record format, refusal and
  malformed records.
- `generation_cleanup::tests::cleanup_preserves_the_externally_owned_kinds_record`.
