# Streamed document lifecycle

This implementation advances [#392](https://github.com/nowledge-co/hawdb/issues/392)
and the [large-document specification](specs/LARGE_DOCUMENT_LIFECYCLE_SPEC.md).
It preserves document, spool, segment and mutation-run wire grammars and analyzer
identity. Source, encoded-record, token and owned-result defaults are unchanged.
It does not authorize Mem activation or close the qualification issue.

## Embedded API and ownership

Use the ordinary `hawdb` Rust facade. A host supplies a bounded owned
`SearchDocumentHeader` and a one-shot UTF-8 `Read` body with a declared
`SearchDocumentBody` length and optional CRC32c. Keep the canonical source
snapshot pinned until capture returns; a computed checksum cannot establish
consistency of a concurrently changing upstream source.

| Operation | Embedded entrypoint | Contract |
| --- | --- | --- |
| Initial build | `SearchOutOfCoreGenerationWriter::push_reader` | Capture once; validate complete input before accepting its record; poison the private writer on failure |
| Append, replace, delete, restore | `prepare_streamed_delta_with_context`, then `SearchOutOfCoreMutationWriter::upsert_reader` / `delete` / `finish` | All operations share strictly increasing nonempty IDs, a borrowed base reader and one publication fence; duplicates or failed input poison the batch |
| Governed build and mutation | `SearchGenerationAdmission::create_writer` / `prepare_streamed_update` | Retain the existing host permit across preparation, publication and deferred cleanup |
| Candidate queries | `search_candidates_with_options` / `search_candidates_with_context` | Share filtering, ACL, scoring, fusion, top-k and page selection with owned results; return exact generation/content identities |
| Full content transfer | `open_verified_body` or the admission wrapper | Validate the complete required source segment, including unselected suffixes, before returning a sealed private reader |
| Reopen | `SearchOutOfCoreReader::open_with_source_policy` | Enforce reader-local limits, retain term ranges and independently reanalyze exact target versions using bounded spill |
| Segment compaction | `prepare_segment_compaction_with_context` or existing governed scheduling | Copy one visible body through private disk; share the build ledger and preserve target-bound retractions |
| Deferred private cleanup | `SearchOutOfCoreGenerationWriter::retry_staging_cleanup` | Bounded explicit retries; report retained stages, conservative disk limits, cleanup memory and typed descriptor denial |

The host calls `finish` to durably publish immutable generations. This does not
route through the resident `SearchIndex` snapshot or its mini-delta. Existing
`SearchIndex::upsert`, `upsert_projection_row`, `apply_projection_delta` and
`checkpoint` retain their resident requirements. The owned generation delta API
streams existing-target reanalysis, but its legacy non-append clean-generation
merge can still hydrate bodies; use the streamed mutation builder for a bounded
large-document operation. No guard is removed from these legacy paths.

`SearchVerifiedBody` borrows its originating reader, retaining the generation
pin through transfer. Copying a candidate alone does not pin its generation.
Read until successful EOF and check `is_complete()`; a short consumer read or a
later I/O/cancellation error does not establish completed delivery. Initial
verified output requires Linux and a filesystem supporting `O_TMPFILE`.
Unsupported platforms/filesystems return an explicit error before output.

## Resource profiles

Capability selection is explicit. Account for the hex-encoded record separately
from the logical source and set both writer and reader limits. Header fields,
embeddings and the largest indivisible identifier/Jieba unit remain resident.
Continuous Chinese is never split using an approximate overlap; an unadmittable
unit fails without changing token semantics. A blocking host `Read` is only
cooperatively cancellable between calls.

The Linux test generator emits `streamed graph document ` followed by spaces in
each 128-byte block. The producer and consumer retain bounded buffers, including
the 128 MiB case. These fixtures use default analyzer identity, no embedding,
a small title and one metadata pair, and explicitly admit 8,000,000 weighted
tokens. The high-cardinality generator emits 65,536 distinct identifiers in
512-byte blocks across a 32 MiB body. See the complete fixture configuration in
`crates/search/src/out_of_core/verified_body/tests.rs`; these are repeatable test
profiles, not production defaults.

| Fixture | Body | Build/mutation operation | Reopen reanalysis | Verified transfer |
| --- | --- | --- | --- | --- |
| Initial build and byte-for-byte transfer | 8 / 32 / 128 MiB | 16 MiB | No mutation closure | 4 MiB |
| Append, compaction, repeated replace, delete/restore, old reader | 32 MiB | 16 MiB; delete also runs with 8 MiB | 8 MiB | 4 MiB |
| Exact high-cardinality retractions | 32 MiB | 16 MiB | 8 MiB plus 64 KiB mutation-header/workspace cap | Separate from this fixture |

Reader metadata, descriptor/index state and mutation headers have their own
existing limits. The reanalysis figure above is not a total-reader or RSS
ceiling. Shared dictionaries, allocator overhead, OS page cache, host source
buffers and downstream consumers are independent owners. The ledger and native
workspace allowances do not measure process RSS.

Source spool, generation artifacts, lexical spill, old-body staging and term
files have explicit finite per-operation limits. A cleanup ticket conservatively
retains their combined limits and the original host admission until deletion.
This reports reserved occupancy, not measured live bytes or cumulative writes;
it does not introduce a project-wide disk governor. Private stages use a fixed
256-owner registry and fail admission when full. The registry is process-local;
post-crash orphan-stage discovery/recovery is still unqualified.

## Validation and remaining gates

Ordinary regressions cover token/chunk equivalence, byte-identical artifacts,
complete scalar/range validation, exact source sizes and checksums, poisoned
input, cancellation, stale publication, output-mode parity, ACL/metadata filters,
old-reader visibility, repeated mutation, compaction and real descriptor pressure
during cleanup. Empty contribution arrays consume the shared term-file quota.
Late valid-syntax changes to stored term ranges are rejected by range integrity.
Decoder admission failure retains its primary cause even if a second drain would
otherwise fail while parsing an already-consumed frame header.

The [finite-model receipt and Rust mapping](tla/LARGE_DOCUMENT_LIFECYCLE_PROOF.md)
cover capture/seal, validation before delivery, publication, pins and cleanup
retention; the adjacent mutation model covers exact versions and compaction.
Neither model establishes physical power-loss safety or unconditional cleanup
progress under permanent resource denial.

Run the embedded and search suites with the pinned toolchain:

```console
cargo test --locked -p hawdb-search --all-features --lib
cargo test --locked -p hawdb-search --no-default-features --lib
cargo test --locked -p hawdb --all-features --test search_generation_context
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
prek run --all-files
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

### Local receipt: 2026-10-05

The tested tree combines implementation commit
`cdf37a668759822fd7bce0f1adc1eab465dc1886` with main at
`e1305decb20f29e9275989761a403ba044383d44`. Its SHA-256 source fingerprint is
`8bb823fe041dce7befee56ade1c895216d9ece6b17442312e82f435ae5ca2b82`.
It covers 1,603 tracked or nonignored new source/configuration files: sorted
paths ending in `.rs`, `.toml`, `.lock`, `.bzl`, `.tla`, `.cfg`, plus
`BUILD.bazel` and `MODULE.bazel`; hash each path, a NUL, then the raw SHA-256 of
its complete bytes into the aggregate. Markdown receipt edits are excluded.

Environment: Linux 7.1.10-zen1-1-zen, x86_64, pinned Rust 1.97.1
(`8bab26f4f68e0e26f0bb7960be334d5b520ea452`), locked dependencies, debug tests.

| Check | Observed outcome |
| --- | --- |
| Complete search library, all features including ACL/vector/text | 756 passed, 20 ignored, zero failed |
| Complete search library, no default features | 585 passed, 20 ignored, zero failed |
| Facade `search_generation_context`, all features | 11 passed, zero failed |
| `prek run --all-files` | Workspace format and strict all-target/all-feature Clippy both passed |
| Minimal WASM strict Clippy, library and `in_memory_portable` test | Passed with the command in `AGENTS.md` and compiler/archiver setup in `WASM.md`; Clang 23.1.1 was temporarily unpacked from the official package after checking its repository SHA-256 |
| Required local Bazel fuzz command | See [PR #847](https://github.com/nowledge-co/hawdb/pull/847) for the full-suite run and timeout follow-up receipt |

The initial implementation also passed the text-only candidate/owned parity
case (one selected test, 700 filtered) before the merge. Full lifecycle and
high-cardinality fixtures above are included in the complete search result,
not ignored benchmarks. The 20 pre-existing ignored tests are not claimed as
coverage.

The initial fuzz attempt ran zero tests because cached `rules_rust` source and
BUILD files had disappeared. The host placed repository sources inside its
action-cache GC root; extracted archive timestamps made those sources eligible
for deletion. The repair separated the repository cache into a sibling
directory, preserved the old configuration and damaged external tree, and
reused complete immutable downloads before refetching. Repository Bazel
configuration, dependency versions, test selection and timeouts are unchanged.
See the cache-layout guidance in [CONTRIBUTING.md](../CONTRIBUTING.md).

The merge from main includes #829, #839 and #841. The streamed stage owner
retains deferred cleanup while reusing the upstream path-capacity bounds;
vector artifacts now use the upstream counted storage bridge.

Remaining release gates include complete supported-feature/platform coverage,
private-stage recovery, full source/token/indivisible-unit boundary qualification
and independent semantic negative controls. Benchmark matched host workloads for
throughput, foreground p95/p99, RSS, cumulative writes and cancellation response before
selecting adaptive profiles or changing defaults. The complete identical-corpus
acceptance in #206 and Mem release/full-verification policy remain independent.
