# Initial search-projection import: apply and checkpoint separately

Issue: `nowledge-co/hawdb#1707`.

A host that imports an existing search corpus, such as Mem migrating a legacy
LanceDB projection, sends it to the resident `SearchProjection` page by page.
Each resident checkpoint writes a complete new generation: the snapshot plus
the lexical index (`search_lexical.N.hawdb`), segment, metadata and vector
payload files. If the host checkpoints after every page, the bytes written grow
with pages × index size, which is quadratic in the corpus.

## API

`EmbeddedStore` and `EmbeddedStoreHandle` expose:

| Method | Effect |
| --- | --- |
| `apply_initial_import_projection_delta(delta, import_source_graph_commit_epoch)` | Validates the frozen legacy source epoch, stamps the local graph commit epoch, applies the page to the resident projection and records the import provenance. Writes nothing to disk. |
| `checkpoint_initial_import_projection()` | Publishes one new resident generation that contains every page applied so far, together with the recorded `import_source_graph_commit_epoch`. Returns `SearchCheckpointReport`. |
| `initial_import_projection_pending()` | Returns the number of applied but not yet checkpointed deltas and their estimated input bytes (`InitialImportProjectionPending`). `None` if no resident projection is open. |
| `apply_initial_import_projection_delta_and_checkpoint(delta, epoch)` | Unchanged behavior: apply followed by checkpoint. |

The handle admits each call as background maintenance. Apply admits the
delta's estimated input bytes plus the blocking-operator working memory.
Checkpoint admits the blocking-operator working memory.

## Crash and resume contract

The resident projection has no write-ahead log or durable overlay. Durable
state changes only when a checkpoint publishes a new generation.

1. After `apply_initial_import_projection_delta` returns `Ok`, the page is
   searchable in this process only. Nothing about it is durable, including
   the `import_source_graph_commit_epoch` it recorded.
2. After `checkpoint_initial_import_projection` returns `Ok`, every page applied
   before the call is durable, together with `import_source_graph_commit_epoch`,
   and the pending counters reset to zero. If the checkpoint returns `Err`,
   the host must not advance its cursor. A later reopen may observe either the
   previous checkpoint or this one; both are safe starting points for replay
   (rule 5). The applied pages remain resident, so the host may also retry the
   checkpoint.
3. A crash, process exit or dropped store before the next checkpoint discards
   all un-checkpointed pages. Reopening yields exactly the last checkpointed
   generation: its documents, lexical and vector artifacts, and its
   `import_source_graph_commit_epoch`. If no checkpoint ever happened, the
   projection reopens empty and the epoch is `None`.
4. HawDB does not store the host's page cursor. The host persists its own
   cursor (for example, "pages `0..n` are durable") only after
   `checkpoint_initial_import_projection` returns `Ok`. After a crash it replays
   every page after its durable cursor, in the original order.
5. Replay is idempotent. Upserts and deletes are keyed by projection document
   id, so replaying a page that is already durable is harmless. This matters
   when the host crashes after the checkpoint but before it persists its cursor.
   Replaying from the durable cursor therefore skips no page and duplicates no
   document. The tests assert this, including a crash in the last and largest
   gap of a geometric schedule.

`import_source_graph_commit_epoch` is immutable provenance for the whole import,
not a per-page cursor. Every page of one import must pass the same value. A
different value is rejected before any state changes.

## Memory between checkpoints

In full-residency maintenance mode, the resident projection keeps every
document in memory whether or not it has been checkpointed. Deferring
checkpoints therefore does not add a second copy of the corpus. Un-checkpointed
pages add only the following state:

- The lexical mini-delta overlay. It is bounded by
  `LexicalProjectionConfig::mini_delta_bytes` (8 MiB by default). Past that
  bound the overlay is dropped and text queries fall back to resident BM25 over
  the same documents until the next checkpoint rebuilds the lexical index. The
  results are the same; text queries are slower until then.
- The vector projection's dirty-document tracking, proportional to the number
  of changed ids.

Neither this change nor the existing per-page path holds a long-lived runtime
admission for resident projection growth. The per-call admission covers only
the delta being applied. Hosts that need a bound on loss or memory between
checkpoints use `initial_import_projection_pending().uncheckpointed_input_bytes`
to schedule checkpoints.

## Recommended checkpoint schedule

Every checkpoint is still a full rewrite whose cost is proportional to the
corpus imported so far. The schedule alone decides the total cost:

- **Per page**: about `P / 2` full index writes in total for `P` pages. This is
  quadratic.
- **Geometric (doubling)**: checkpoint when the un-checkpointed input bytes
  reach the bytes already checkpointed, and once after the last page. The
  checkpoint sizes form a geometric series, so the total written is at most
  about 2× the final index size (plus per-generation overhead), and there are
  `O(log P)` generations. This is linear.

The trade-off is the loss window. Under a doubling schedule, the gap between
the last two checkpoints covers about half the import. A crash just before the
end can therefore discard, and require replaying, up to about half the pages.
This is acceptable for an offline initial import, because replay costs the same
as the original apply and nothing is corrupted.

To bound that window, cap the gap: **double until the cap, then checkpoint
every `cap` bytes**:

```text
due = pending_bytes >= checkpointed_bytes
   || pending_bytes >= max_bytes_between_checkpoints
```

With a cap `C` and a corpus of `N` bytes, the total stays about 2× the final
size until `N` exceeds `C`. Beyond that, each further `C` bytes cost one full
rewrite, so the total grows as `O(N² / C)`. Pick `C` from the replay time or
the uncheckpointed memory you can tolerate.

Hosts can also merge several pages into one larger delta. This reduces the
number of apply calls but does not change the checkpoint arithmetic above.

A follow-up on `#1707` tracks a segment-append checkpoint for the resident
index. That checkpoint would write only the segments added since the previous
generation, which would make each checkpoint's cost proportional to the new
data.

## Tests

`src/embedded_store/tests/initial_import_checkpoint_tests.rs` (owned by
`//:hawdb_unit_general_tests`):

- `initial_import_1707_deferred_checkpoint_matches_per_page_checkpoint`: applies
  12 pages with one final checkpoint, and separately with a checkpoint per page.
  Document contents, text and hybrid hits (ids and exact score bits), delta
  reports and the reopened state are all identical. Pages after the first
  also update and delete earlier documents.
- `initial_import_1707_crash_resumes_from_last_checkpoint_without_skips_or_duplicates`:
  crashes before the first checkpoint, inside an early gap, and inside the
  last and largest gap of a doubling schedule (24 pages; the durable cursor is
  the final doubling checkpoint). Reopen returns exactly the last checkpointed
  state and import epoch. Replaying from the durable cursor, including one
  already-durable page, reaches the same final state as an uninterrupted run.
- `initial_import_1707_geometric_schedule_bounds_write_amplification`: for 8 and
  32 pages, asserts that a doubling schedule writes at most 3× the final
  checkpoint's bytes in at most `log2(P) + 2` generations. Per-page
  checkpointing writes at least `P / 4` times the final size. A capped schedule
  bounds the un-checkpointed gap.

Replay with
`cargo test --locked -p hawdb --lib initial_import_1707 -- --test-threads=1`.
