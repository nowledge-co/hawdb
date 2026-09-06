# Publication path ownership and the commit gate

Generation publication prepares original names and every source, destination and
temporary path under the existing `BuildMemory` root before reading staged
checksums or crossing its manifest-last commit gate. `publication::paths` is a
private ownership boundary, not a new storage API or an independent budget.

The fixed six-name owner reserves formatter growth before allocation and retains
actual string capacities. Each format, including a full-width `u64`, fits 128
bytes; the initial envelope is `6 * 3 * 128`. Existing manifest-string copies
still have their own overlapping reservation before JSON serialization.
The original-name owner exposes only an immutable view, so moving or mutating
its strings cannot detach their payload from the retained reservation.

Six transfers retain source, destination and temporary paths; layout and active
manifest outputs retain destination and temporary paths. Optional RaBitQ adds
one transfer: 22 paths without it, 25 with it. Struct fields express the mapping
explicitly without allocating a per-publication registry. Data drops before its
lease, including on partial preparation failure. Temporary cleanup guards borrow
these owners instead of allocating duplicate paths.

## Native paths and temporary names

Destination/source joins use `OwnedPath`'s standard-library native join semantics
and the envelopes in `BUILD_CONTEXT_ADMISSION.md`. Temporary extensions keep the
existing `tmp.<pid>.<sequence>` format and shared atomic sequence. Preparation
may consume sequence values without publishing; uniqueness does not depend on
contiguous publication. No persistent format or generation naming changes.

Temporary-extension formatting reserves 384 bytes before allocation and checks
that the result capacity fits 128 bytes. `OwnedPath::with_extension` then reserves
native path bytes plus extension bytes plus one dot. Rust 1.97.1
`std/src/path.rs::_with_extension` reserves the full result before copying;
keeping the old extension in the estimate is conservative. The actual result
capacity is checked and retained while temporary excess is released. Native
`OsStr` bytes are not converted lossily or reconstructed by hand.

These are pinned-toolchain capacity envelopes, not allocator/RSS guarantees.
Recheck them when changing the toolchain. Filesystem-internal path conversion,
allocation-failure recovery and retained public errors are not newly covered.

## Publication behavior

Names, all paths, checksums, layout/manifest serialization and generation-size
admission complete before the final cancellation checkpoint. After that gate,
publication only borrows the prepared paths: no new path admission or task
cancellation checkpoint can prevent the remaining manifest-last operations.
Hard-link publication, copy-and-sync fallback, file synchronization, verification
and durable replacement retain their existing behavior. The active manifest is
still written after every referenced artifact is published and verified.

Admission or cancellation before the gate leaves the active generation untouched.
An I/O failure after some artifact links may leave unreferenced generation files,
as before; temporary-file guards remove the attempted temporary file on failure.
The stage guard retains its existing cleanup behavior. Durability errors during
rename/directory synchronization are unchanged: this is not a new rollback or
power-loss proof, nor does it undo a replacement that already completed.

The synchronous resident publication wrappers use the same borrowed-path helpers
but retain their existing unadmitted input/temporary-path contract. No new public
API, `v1` artifact, backend, dependency, Bazel runtime setting or CI job is added.

## Verification

Normal regressions independently derive name and temporary-extension envelopes,
retain owners after root-handle drop and test exact/one-short limits. Complete
path-set trials compare with direct standard-library construction, including
optional RaBitQ, full-width generation/sequence values and competing memory.
Sequences are deterministic through the same preparation implementation so that
concurrent publishers cannot change an exact trial's path length.

Published-reader tests cover admission denial before name allocation, cancellation
after path preparation, late cancellation with the entire remaining root occupied,
and an injected layout replacement failure after artifact linking. They verify
active-manifest bytes, generation/document readback, removal of temporary/stage
files and complete release of tracked memory. Borrowed-helper tests check failed
link/write cleanup and prove a replacement does not overwrite the source inode.

The existing segment publication-denial regression injects competing memory
after path preparation, immediately before encoding. It still requires zero
layout allocations when that buffer is denied and one when layout fits but
manifest admission fails, then compares every old artifact byte and checks full
release. Earlier path denial must not mask either of these encoding boundaries.

The ignored seed `0x206a71f5` campaign covers 128 varied path sets with exact and
one-short retries, and 24 real generation updates: 12 published, 12 rejected and
six successful commits with late cancellation and no available path budget.
Eight successful updates publish vector artifacts when vector search is enabled;
minimal-feature builds omit those vectors. Reopened documents are compared with
the original test inputs. Observed-peak campaign boundaries complement, rather
than replace, independent normal-test formulas.

Eleven negative controls omit original-name, temporary-format or extension admission;
release name/path charges early; disable temporary cleanup; publish the active
manifest early; allocate paths after the commit gate; use an independent name
root; map a payload to the wrong staged source; or move encoding-budget injection
ahead of path preparation. Each must compile and fail its
intended assertion, with exact source hashes restored before positive checks.

```bash
cargo test -p skein-search --all-features publication::tests -- --nocapture
cargo test -p skein-search --no-default-features publication::tests -- --nocapture
cargo test -p skein-search --all-features extension_paths -- --nocapture
cargo test -p skein-search --all-features publication_root_denial -- --nocapture
cargo test -p skein-search --all-features publication_admission_campaign -- --ignored --nocapture
cargo test -p skein-search --no-default-features publication_admission_campaign -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The manual publication campaign belongs to the explicit local fuzz suite, not
default or dedicated CI. Artifact-builder and lexical/RaBitQ backend path copies,
publication-lock discovery/registry paths, cleanup and host-reader lifetimes,
resident delta maps, combined component limits and retained public outputs still
need their respective owners. Full #206 also requires tokenizer/Jieba workspace,
representative-corpus reduction and exact-head native qualification.
