# Artifact-builder path and name ownership

The fused generation builder uses private `artifact_paths` owners on its existing
`BuildMemory`. This extends startup and publication ownership without changing
public APIs, artifact formats, storage backends or cancellation infrastructure.

## Segment builder

The real task context is passed to `SegmentArtifactBuilder::new_with_context`
before reservation, path allocation or file creation. The test-only default
constructors delegate to this same implementation. Attaching a context after
construction is no longer possible.

Construction admits document, metadata, vector, descriptor and descriptor-temp
paths before opening any output file. The first three paths are released after
their handles have opened. The descriptor pair remains owned through final
encoding, replacement and metadata checks. No duplicate stage path is retained,
and finish does not repeat the descriptor join or allocate a temporary path.
The existing `skein.tmp` extension, file sync and durable replacement are retained.

Path admission failure cannot truncate any payload file. I/O failure after one
file has opened can still leave private staged outputs, as before. The enclosing
writer's stage guard owns cleanup; this is not a standalone builder rollback
guarantee. Partially constructed handles and path leases drop on error.

## Artifact names and checksum paths

`Name` admits the original lexical or RaBitQ name before formatting, exposes an
immutable string view and retains actual capacity until the name drops. The
fixed formats and full-width generation fit 128 bytes; an initial 384-byte
reservation includes overlapping formatter growth. Names are produced by the
existing format functions. A cancellation checkpoint after formatting releases
both the payload and its reservation on failure.

Formatting the same bytes with a different `format!` argument layout can choose
a different string capacity. Tests compare name bytes with independent literals
but check retention against the actual owned string capacity, not the capacity
of that independently formatted oracle.

The RaBitQ wrapper retains an admitted native path through backend creation,
push, finish and checksum verification. Its returned summary moves the name
owner rather than separating a string from a lease. Zero-vector builds still
skip backend creation. The lexical post-build checksum paths have the same
owner pattern; those two paths drop before returning the retained name in the
generation artifacts. Both names remain charged through publication.

Native joins and extension changes use `OwnedPath` and the source-qualified
Rust 1.97.1 envelopes in `BUILD_CONTEXT_ADMISSION.md` and
`PUBLICATION_PATH_ADMISSION.md`. These track requested capacities, not allocator
metadata, mappings, page cache, filesystem-internal conversions or RSS.

The vector backend's own copied paths and the lexical backend's internal paths
remain separate ownership work. A wrapper lease is not proof that those backend
allocations or returned public readers are admitted. No cross-crate ownership
contract is introduced here. The test-only three-pass reference builder adopts
the typed name owner but retains its independent legacy I/O implementation.

## Verification

Normal tests derive segment and lexical path limits independently using direct
standard-library capacities and non-verbatim native paths. Exact and one-byte-
short attempts share a competing owner and require complete release. Native
verbatim normalization is covered by the separate `OwnedPath` tests, not this
non-verbatim formula. Other tests cover cancellation before/after allocation,
unmodified payload sentinels on constructor denial, descriptor finish with only
encoding capacity available, name lifetime after root-handle drop, real retained
artifact summaries, and reopened generations after admission/I/O/cancellation
failures. The full-feature suite also checks RaBitQ path admission before any
backend entry and its actual retained path capacity.

The manual seed `0x206a471f` campaign has 128 name, 128 segment and 128 lexical
cases: 384 exact and 384 one-short attempts. Sixteen real updates include four
published generations, four constructor admission failures, four partial-file
creation failures and four cancellations. Every rejected update keeps all old
artifact bytes and reader contents unchanged. Full-feature successful updates
include vectors; minimal builds exercise the same lifecycle without vectors.

The existing RaBitQ campaign retains the real input and constructed builder,
then pads their combined occupancy to 256 KiB before subsequent backend phases.
This prevents path length differences from changing those phases' exact budget.
Independent constructor tests retain their own admission boundaries. The padding
is test-fixture normalization, not a production budget increase or a new root.

Twelve negative controls omit name/path admission, release a name early, ignore
post-format cancellation or the constructor context, use an independent root,
open a payload before path admission, choose a wrong segment path or checksum
source, allocate a descriptor path during finish, or normalize backend occupancy
before the builder exists. Each must compile and fail its intended regression;
source hashes are restored before complete positive verification.

```bash
cargo test -p skein-search --all-features artifact_paths::tests -- --nocapture
cargo test -p skein-search --no-default-features artifact_paths::tests -- --nocapture
cargo test -p skein-search --all-features artifact_path_admission_campaign -- --ignored --nocapture
cargo test -p skein-search --no-default-features artifact_path_admission_campaign -- --ignored --nocapture
cargo test -p skein-search --all-features rabitq_admission_campaign -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/optimizer:skein_optimizer_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The campaign is manual and local only. No fuzz CI or Bazel runtime configuration
change is included. Full #206 still needs backend/public-output/host-reader
ownership, publication-lock and cleanup paths, resident delta maps, combined
component limits, tokenizer/Jieba workspace, representative-corpus reduction and
exact-head native qualification.
