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
