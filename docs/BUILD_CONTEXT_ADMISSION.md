# Generation build context ownership

The generation writer moves its build options into private `context_memory::Options`
under the existing `BuildMemory` root before constructing the writer. Delta updates
admit those options before input conversion and carry both into the same writer
root. This is operation ownership after API entry, not admission of earlier caller
allocations. No public signature or new independently budgeted root is introduced.

## Options and identity

The options owner includes the alias-rule vector capacity, each input/alias vector
capacity and all string capacities, including unused capacity. Stopwords include
string capacities and the existing 1,024-byte per-entry B-tree allowance. An empty
set conservatively reserves one possible retained leaf. Supplied embedding model
and version string capacities are included; other build-option fields are inline.
Checked arithmetic and cancellation checkpoints precede ownership admission.

Delta identity validation borrows the active manifest's model, version and
dimension. A matching supplied identity keeps its allocation, rather than cloning
the reader identity to compare and then cloning it again to replace the input.
When omitted, model/version lengths are reserved before a single inherited copy.
The copied identity and its lease remain paired through a final cancellation
checkpoint before any identity or epoch field is committed to the options owner.
Source epoch, import provenance, embedding identity and analyzer mismatch ordering
is unchanged within binding. Memory admission can now fail before binding begins.

Data fields drop before their leases. Both raw options and inherited identity
keep their original root charged through the writer's lifetime, including an
abandoned prepared update. The borrowed reader manifest is not newly admitted.

## Startup paths

`OwnedPath` admits root copies and standard-library joins for the stage and spool
paths before allocating. It exposes borrowed `Path` access, not mutable or cloned
owners. After the operation, actual retained capacity is checked and excess
temporary allowance released. Spool scans borrow the existing spool path.

Path lengths use native `OsStr::as_encoded_bytes`, not lossy Unicode conversion.
Standard `Path::join` preserves Unix, drive-relative and Windows verbatim-prefix
semantics. Let `L` be parent bytes plus child bytes plus one separator:

- Ordinary joins reserve `3 * max(L, 8)` for the copied buffer and growth overlap.
- Verbatim-prefix joins additionally collect components and reconstruct a native
  string. They reserve `4 * max(L, 8) + 4 * max(L, 4) * size_of::<Component>()`.
- Stage-name formatting reserves 384 bytes for growth overlap before formatting
  the bounded PID/sequence name; its final capacity must not exceed 128 bytes.

These conservative envelopes follow Rust 1.97.1 `std/src/path.rs` (`_push`) and
`alloc/src/raw_vec/mod.rs` growth, not a stable allocation guarantee or RSS proof.
Recheck them when changing the toolchain or path construction. Allocation failure
handling remains the standard library's; admission does not make OOM recoverable.

Options and root copying are admitted before root creation. Later stage admission
failure can leave an empty root directory. A created stage remains guarded on
constructor, cancellation and build failure. The spool field drops before stage
removal, so cleanup does not rely on deleting an open spool file. Directory removal
retains its existing best-effort error handling; the intentionally sticky
publication-lock file is not treated as a leaked stage. Other artifact paths,
publication-lock registry/canonicalization and filesystem-internal buffers are
not covered by this startup owner.
Generation publication now owns its source/destination/temporary paths separately
on the same root; see `PUBLICATION_PATH_ADMISSION.md`. Artifact-builder and
publication-lock discovery/registry paths remain outside these owners.

## Verification

Normal regressions check independently calculated exact/one-short limits, spare
capacities, pointer-preserving moves, inherited-copy count, shared-root overlap,
mismatch/cancellation atomicity and complete release after failure. Path tests
check copy/join admission, checked overflow and charge retention after root-handle
drop. Native Unix tests preserve opaque bytes. Native Windows tests cover verbatim,
drive-relative and unpaired-surrogate paths; a non-Windows run does not execute
those tests or qualify Windows allocation behavior.

The manual campaign uses seed `0x206c017e`: 128 generated option cases and 128 path
joins each retry exact/one-short root limits with a competing live owner. Inputs
are regenerated from the same recipe to preserve capacities, not cloned between
boundary attempts. These limits use the observed admitted peak; independent normal
tests check the formulas. Sixteen real generation updates alternate matching and
inherited identities, preserve the active manifest until finish, and verify
identity, source epoch and document count after reopen. Their writer ledger must
release all tracked memory. Semantic path parity uses standard `Path::join` as the
oracle, not an independent platform path implementation.

Eleven negative controls omit raw admission, spare rule slots or identity capacity;
omit inherited-copy admission or release it early; clone a matching identity;
omit the final binding cancellation checkpoint; omit path admission or release
path capacity early; omit stage-name admission; or create an independent options
root. Each must compile and fail its intended assertion. Restore exact source
hashes before positive verification; compiler errors do not count as detection.

Existing RaBitQ exact-budget trials now keep variable-length startup paths charged
under a fixed competing-occupancy fixture. The path-length regression and its
negative control preserve the original phase's exact/one-short checks; see
`RABITQ_BUILD_ADMISSION.md`. No production budget is increased to accommodate it.

```bash
cargo test -p skein-search --all-features context_memory -- --nocapture
cargo test -p skein-search --no-default-features context_memory -- --nocapture
cargo test -p skein-search --all-features context_admission_campaign -- --ignored --nocapture
cargo test -p skein-search --no-default-features context_admission_campaign -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The context campaign is manual and belongs to the explicit local fuzz suite, not
default or dedicated CI. Full #206 still needs shared backend/public-output and
host-reader ownership contracts, resident delta maps, combined component limits,
matched-span/tokenizer/Jieba workspace, representative-corpus reduction and
exact-head native qualification. This changes no public API, v1 artifact,
dependency, I/O backend, Bazel runtime configuration or release policy.
