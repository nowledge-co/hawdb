# Incremental search segment power-loss qualification

The #291 search matrix uses the existing storage IO recorder to capture real
incremental publication and overlapping compaction. It supplements the
process-abort and injected replacement-failure tests; it is not physical-device
certification or proof of the complete Mem activation path.

## Reproduce

Use the repository's pinned toolchain and locked dependencies:

```sh
bash scripts/cargo-test-required.sh --locked -p hawdb --all-features --lib api::tests::power_loss::search_projection:: -- --nocapture --test-threads=1
```

The two cases are registered in the existing facade library test target under
the Unix-only `test-support` module. The required-test wrapper rejects a
zero-test selection. The ordinary Bazel facade target does not enable this
recorder; its passing status must not be reported as execution of these cases.
The existing platform workflow's full `api::tests::power_loss::` selection also
discovers these cases. Its configuration alone does not establish Linux or
macOS qualification for a candidate revision.

## Boundaries and oracle

Each fixture attaches the recorder before creating its search directory.
Bootstrap publishes two documents with embeddings, and a crash image that loses
all uncovered operations must retain that acknowledged generation. This also
checks the search directory's installation in its durable parent. File
publication within a new directory alone does not persist its parent's entry;
writer creation admits the project and synchronizes the directory ancestry
before preparing a generation.

The incremental update inserts an interleaved ID, replaces another document,
deletes an existing document, and advances the source epoch. The compaction case
merges the resulting overlapping artifacts. Both capture three real IO cuts:

| Cut | Fault schedules | Required result |
| --- | --- | --- |
| First private-stage write, before its file barrier | Lose all uncovered operations, retain all, reverse their persistence order, retain each operation separately, and retain two torn ranges of the actual uncovered write | The complete previous generation remains selected. |
| Before active-manifest rename | Lost, complete, reversed and isolated uncovered operations | The previous or newly selected generation has a complete artifact closure. |
| After active-manifest rename, before its directory barrier | Lost, complete, reversed and isolated uncovered operations | The previous or newly selected generation has a complete artifact closure. |
| Successful publication response boundary | Lose every uncovered operation | The exact new generation and its source epoch survive. |

Every materialized image reopens through `SearchOutOfCoreReader`. Its generation,
source epoch, document count, and complete hydrated documents (including
embeddings and metadata) must equal the selected old or new oracle. Empty
recreation, partially applied replacement/deletion, and a new manifest without
its dependencies fail the test. Each image is removed after its reader closes
to bound scratch occupancy.

Capture audits the visible native tree against the recorded IO. Unrecorded
mutations cannot silently qualify. The log reports actual pending-operation and
generated-plan counts; a single-operation cut does not establish nontrivial
write reordering. The torn-write cases assert that the observed write actually
has uncovered bytes. Snapshots precede later flushes and handle closure, which
cannot strengthen their durability.

## Resident checkpoint

The full-residency checkpoint (`SearchIndex::checkpoint` on an unregistered
projection) used to replace `search_projection.hawdb` before it wrote the
generation files and the selector. A crash in that window paired a new snapshot
with the previous selector, and reopen refused the projection (#1711). The
checkpoint now has one commit point:

1. Write and sync the RaBitQ/DiskANN artifact, then the snapshot as
   `search_projection.<g>.hawdb`. Next write the segment payload and descriptor,
   the lexical artifact and `search_lexical.manifest.<g>.hawdb`, and the
   metadata/vector sidecars and layout. Each file is synced and renamed with a
   directory sync. No current name and no selector changes.
2. Verify each generation file against its object reference, then sync and
   rename `search_projection.out_of_core.manifest.hawdb`. Its resident receipt
   binds the snapshot's object reference. This rename is the commit point.
3. Replace `search_projection.hawdb`, `search_projection_segment_payloads.hawdb`,
   `search_projection_segments.hawdb` and `search_lexical.manifest.hawdb`, in
   that order, by hard links (or synced copies) to the selected generation.
4. Run the ordinary current-plus-previous generation cleanup. The generation
   snapshot is a content-generation artifact, so the previous checkpoint's
   snapshot is retained until a newer selector has been published.

`SearchIndex::open` repairs step 3 if it was interrupted. It applies only when
the selected generation snapshot exists and matches the selector's receipt.
For each current name that is neither the same inode as its generation file nor
equal to the selector's object reference, it verifies the generation file
against that reference and re-links it. Otherwise it leaves the name unchanged, and the existing
validation reports the failure. Older layouts without generation snapshots
therefore keep their existing behavior. Registered Consumer checkpoints keep
their legacy order.

Two owners qualify the protocol:

- `api::tests::power_loss::resident_search_checkpoint` (Unix `test-support`)
  drives `EmbeddedStore` with full residency through the runtime
  `apply_search_projection_delta_and_checkpoint` path,
  `apply_initial_import_projection_delta_and_checkpoint`, and the deferred
  `apply_initial_import_projection_delta` plus
  `checkpoint_initial_import_projection` path. Three
  checkpoints are acknowledged and the fourth is cut at the first checkpoint
  write (lost, complete, reversed, isolated and torn), after the generation
  snapshot rename, after the layout rename, before and after the selector
  rename, after the snapshot and lexical alias renames, and after the first
  cleanup unlink. Every image must reopen through `EmbeddedStore`. Its source
  and import epochs, documents and Text, Vector and Hybrid hits must equal the
  previous or the new checkpoint. Hits include every score's bits, ranks,
  matched terms and fallback codes. Cuts before the selector must reopen the
  previous checkpoint. A lost acknowledgement must retain the new checkpoint.
- `resident_checkpoint_atomicity_tests` in `hawdb-search` stop the checkpoint
  as a process abort would after each step: RaBitQ, snapshot, segment
  artifacts, lexical, before and after the selector, after each alias, and
  before cleanup. Each reopen must equal the previous or the new oracle, retain
  both generation snapshots, and checkpoint again. Further cases cover a crash
  on the Nth of repeated small deltas, an old layout reopening and upgrading, an
  old-layout mixed snapshot that still fails closed, and a damaged generation
  snapshot that is never linked.

Replay with:

```sh
cargo test --locked -p hawdb --features test-support --lib api::tests::power_loss::resident_search_checkpoint -- --nocapture --test-threads=1
cargo test --locked -p hawdb-search --lib resident_checkpoint_atomicity
```

## Assumptions and remaining evidence

The model assumes reliable completed POSIX file and directory synchronization,
atomic same-directory rename, and an already durable anchor on an existing
mount. It preserves inode identity across hard links. It does not emulate a
controller that lies about synchronization, power-cycle hardware, certify a
filesystem, or qualify Windows namespace semantics. Native execution on each
required platform must be associated with the exact tested revision.

This bounded matrix addresses search publication closure. It does not establish
tens-of-GB corpus write amplification, sustained RSS under bulk ingestion, full
Mem release artifacts, or storage cutover readiness. Those #291 and Mem
integration acceptance criteria require their own evidence. See the
[branch qualification](BRANCH_POWER_LOSS_QUALIFICATION.md) for canonical
WAL/checkpoint/catalog/GC coverage and its separate limits.
