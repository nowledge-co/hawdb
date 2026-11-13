# Rebuilding an incremental search projection from the graph

Issue: `nowledge-co/hawdb#1738`.

An incremental-maintenance (out-of-core) search projection is opened from an
existing generation and caught up by replaying the search-projection change log
from its `durable_source_graph_commit_epoch`. The change log is bounded by
`DatabaseConfig::max_search_projection_change_log_entries` and
`max_search_projection_change_log_bytes`. When the projection's cursor falls
below the retained start, catch-up refuses with
`... full search projection rebuild required`, and a damaged or missing
projection cannot be opened at all. The resident `rebuild_search_projection`
path builds every document in memory and targets only the resident index.
Both the trimmed-change-log refusal and the resident snapshot mismatch refusal
name `EmbeddedStoreHandle::rebuild_out_of_core_search_projection` as the
recovery path, because incremental catch-up is replay-only.

`EmbeddedStoreHandle::rebuild_out_of_core_search_projection` rebuilds such a
projection from the current graph with bounded working memory, into a new
directory.

## API

```rust
pub fn rebuild_out_of_core_search_projection(
    &self,
    target: impl AsRef<Path>,
    options: SearchProjectionRebuildOptions,
    source: &mut dyn SearchProjectionRebuildSource,
    task: RuntimeTaskContext,
) -> Result<SearchProjectionRebuildReport>
```

- `target` is a directory name that must not exist yet. The rebuild never
  writes the current projection directory, so it remains evidence.
- `options.maintenance` is the `IncrementalSearchMaintenanceOptions` the host
  will open `target` with. Its build options supply the embedding manifest,
  vector binding, analyzer lexicon and admission limits; their
  `source_graph_commit_epoch` and `import_source_graph_commit_epoch` must be
  unset because the rebuild stamps the snapshot epoch.
  `working_memory_bytes` is the hard memory reservation of the whole rebuild.
- `options.priority` selects the runtime lane. `Background` (default) competes
  with other maintenance; an explicit operator command may use `Foreground`.
  Admission does not wait: a refusal is a retryable error that has written
  nothing.
- `options.hydration_batch_rows`, `options.sorting` and `options.sort_step`
  bound the host batch, the external sort and the logical work between
  cancellation checks.
- `source` supplies what the canonical graph mapping cannot derive:
  `hydrate_graph_rows` adjusts one bounded batch of graph rows (for example,
  attaches external embeddings), `visit_host_rows` emits host-owned rows after
  the graph scan, and `progress` observes phase and counter changes. Each
  callback receives a read transaction pinned at the same snapshot as the
  graph scan. `GraphOnlySearchProjectionRebuild` rebuilds only the graph rows.
- `task` carries cancellation and deadline. Every batch, sort step, artifact
  phase and the install check it.

The report names the installed directory and the snapshot epoch E, and carries
the scan counters, the external-sort report (runs, spill and read bytes, peak
buffer), the generation build report, both verification digests, the working
memory reservation and the elapsed time.

## Algorithm

1. Refuse an existing `target`. Create a private sibling stage
   `.<target name>.hawdb-search-rebuild-<pid>-<nanos>-<seq>` with `create_dir`,
   so the stage is always a directory this call created.
2. Admit the rebuild as `Maintenance` work in the selected lane with
   `working_memory_bytes`, and bind the task to that memory reservation.
3. Pin one graph read snapshot at epoch E. A forked transaction over the same
   pin serves the host callbacks. Writers are not blocked.
4. Stream the canonical graph rows (the same mapping graph deltas use,
   including business labels resolved through each node's `HAS_LABEL`
   adjacency) in node order. Every `hydration_batch_rows` rows go to
   `hydrate_graph_rows`, then into
   `SearchOutOfCoreGenerationWriter::create_unordered_replacement_with_context`,
   an external sort that spills sorted runs into the stage. Host rows follow.
   The sort rejects a document ID supplied twice.
5. Merge the runs and build one generation stamped
   `source_graph_commit_epoch = E` in the stage. It is the stage's first
   selector, so it never replaces another head.
6. Verify before installation. Open the stage with exactly
   `options.maintenance` (the incremental open path the host will use), check
   the stamped epoch, and read every document of the generation into an
   order-independent `SearchDocumentSetDigest` (document count plus the sum
   modulo 2^128 of each canonical encoding's SHA-256 prefix). Scan the pinned
   graph again through the same source into a second digest. Counts and
   digests must be equal, and equal to the build report's document count.
7. Install with one no-replace rename (`renameat2(RENAME_NOREPLACE)` on Linux,
   `renamex_np(RENAME_EXCL)` on macOS, `MoveFileExW` without replace on
   Windows) of the stage onto `target`, then synchronize the moved directory
   and its parent.

## Switch semantics

The rebuild does not swap projections. It returns a verified directory and
leaves the switch to the host:

1. close the handle;
2. reopen with `search_projection_path = report.target_path` and
   `with_incremental_search_maintenance(options.maintenance)`;
3. run catch-up. The projection's durable epoch is E, so catch-up replays
   exactly the commits after E, including writes made during the rebuild.

Catch-up needs the change log to still retain the commits after E. If the host
writes more than the retained window during the rebuild, catch-up refuses again
and the rebuild is repeated. Keeping the old directory, deciding when to delete
it and recording which directory is current are host responsibilities.

## Failure, cancellation and crash

| Event | Result |
| --- | --- |
| Refused admission, existing target, invalid options | Error before any write; nothing created. |
| Cancellation, deadline, input/sort/build failure | Error; the stage this call created is removed; `target` never appears. |
| Verification mismatch | `StorageIntegrity` error; the stage is removed; nothing installed. |
| `target` appears during the rebuild | The no-replace rename refuses; the stage is removed; the other directory is untouched. |
| Error after the rename may have happened | `PublishOutcomeUnknown { destination }`: `target` holds this call's complete verified result, but its name may not be durable. Reopen it, or rebuild into another name. |
| Process crash or power loss | At most a stage directory is left. It is never at `target`, never selected, and the next rebuild uses a new stage name. The host may delete `.<target name>.hawdb-search-rebuild-*` leftovers. |

The current projection directory is never opened for writing, renamed or
deleted in any of these cases.

## Memory

Rows are held for one hydration batch at a time. The external sort holds at
most `sorting.max_buffer_records` / `max_buffer_payload_bytes` of input before
spilling a run. Artifact construction runs under the generation writer's
ledger, which charges the task's `working_memory_bytes` reservation and refuses
with `ResourceBudgetExceeded` instead of growing past it. The fixed part of
that reservation includes the zstd workspace (about 8 MiB), the lexical build
memory and the vector build memory from the build options, plus one content
closure; size `working_memory_bytes` to cover them.

Verification opens the generation as a serving reader. Its resident range
index is bounded by the reader configuration (`max_descriptor_working_bytes`),
the same residency a host pays when it serves the projection. Graph-derived
rows copy every node property into metadata, so descriptor statistics include
the body field; corpora with long bodies have larger descriptors.

## Measurements

Synthetic corpora on a shared 8-core Linux host under concurrent load. See the
PR for the recorded table.
