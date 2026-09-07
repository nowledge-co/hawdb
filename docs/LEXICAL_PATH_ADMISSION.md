# Lexical backend path ownership

The compact lexical writer shares the generation's existing `BuildMemory` root
for private names, paths and run registries. The reusable crate-private
`build_memory::path::OwnedPath` has moved out of the generation writer; its native
copy, join and extension envelopes are unchanged. No external API is added.

## Ownership and publication

The backend borrows its root and the artifact path instead of cloning them.
The publication owner prepares artifact, artifact-temporary, manifest and
manifest-temporary paths before creating payloads. The existing v1 filenames,
extension replacement, checksums and manifest-last order are unchanged.

Manifest format/layout strings and the original artifact filename are admitted
before allocation. The filename formatter reserves 384 bytes for overlapping
buffers and checks its resulting capacity against 128 bytes. The two fixed
strings add their exact lengths. Actual retained capacities move with the
`ManifestBody` into a private owner and release only after that body drops.

There is no new path admission or task checkpoint after the final publication
gate. The borrowed temporary guards allocate no path copies. This preserves the
existing commit boundary; it does not make multiple file replacements atomic or
turn an I/O failure after artifact replacement into a rollback guarantee.

Doclist and dictionary sidecars receive owned, admitted extension paths and the
real task context before `create_new`. A collision never installs a cleanup guard
for the existing file. In each writer, the file field precedes the cleanup guard,
so the handle closes before deletion, including on Windows.

## External runs

Each run name is bounded and reserved before formatting. Its native joined path
stays charged through temporary-file cleanup. Run sequence advances only after
successful path construction; spill-byte and run-count limits remain unchanged.

The registry owns its path vector and capacity lease together. Growth reserves
the old and replacement arrays concurrently, allocates an empty replacement,
checks cancellation, then moves the paths. A destination slot is prepared before
file creation. Registering a completed run cannot fail due to a later allocation.

Compaction moves the old registry, including its lease and cleanup obligation,
into a local owner. New outputs have a separate owner until the pass completes.
Decode, admission, cancellation and source-deletion errors therefore retain
cleanup responsibility for both inputs and completed outputs. Cleanup remains
best effort; inaccessible files can remain. Merge readers consume borrowed paths
without constructing another path list.

## Verification

Normal tests cover independent exact/one-short path bounds, native path parity,
manifest-name handoff, cancellation after allocation, registry old/new overlap,
failed path construction without sequence advance, sidecar collisions, and real
constructor context before I/O. A real update cancels the task and fills the
remaining root budget immediately after the publication gate, then verifies
manifest publication, old-reader identity, reopen and complete charge release.

Existing tests for downstream encoding and frame denial now include the actual
retained path capacities. Their allocation/phase probes remain unchanged: a test
must still reach its intended failure point. Production limits are not increased.
Existing corrupt-run and source-deletion tests continue to check all-file cleanup.

Negative controls must fail for early name-charge release, a registry on an
independent root, omitted run cleanup, omitted sidecar/artifact constructor
checkpoints and a new path allocation after the publication gate. Each control
must compile, fail its intended regression, and restore the exact source hash
before positive verification.

The manual campaign uses seed `0x206bac4`: 128 native path sets and 128 generated
spill/merge groups, each with exact and one-short roots. Cases vary path lengths,
full-width generations, run counts and fan-in. A competing live owner shares each
retry root. Run bytes are checked against direct v1 serialization, merged rows
against the generated sequence, and all outcomes must release charges and files.
Observed merge peaks are boundary probes, not independent resource-size oracles;
normal tests separately check the capacity formulas.

```bash
cargo test -p skein-search --all-features lexical_projection::paths::tests
cargo test -p skein-search --no-default-features lexical_projection::paths::tests
cargo test -p skein-search --all-features lexical_path_admission_campaign -- --ignored --nocapture
cargo test -p skein-search --no-default-features lexical_path_admission_campaign -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

This is source-qualified requested-capacity accounting, not allocator/RSS proof.
The standard-library native-path caveats in `BUILD_CONTEXT_ADMISSION.md` apply.
It does not cover filesystem internals, vector-backend copies, returned public
outputs/readers, resident delta maps,
combined component limits or tokenizer/Jieba workspace. Representative-corpus
reduction and exact-head native-platform qualification remain full-issue gates.
No I/O backend, io_uring, dependency, v1 format, Bazel runtime setting or fuzz CI
change is included.

The later [publication-lock checkpoint](PUBLISHER_MEMORY_ADMISSION.md) separately
covers canonical Rust output and per-publisher registry ownership, including its
native resolver exclusions and direct platform dependency edges.
