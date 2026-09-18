# Streaming selected-document hydration

This private implementation slice advances #392 without changing its complete
large-document acceptance criteria or selecting the pending #325 term policy.

`SearchOutOfCoreReader::hydrate_documents` and search-hit hydration now scan each
selected source segment through positioned reads and streaming zstd decoding.
They no longer retain a complete compressed range, decompressed segment string,
or vector of every decoded document. Only requested documents survive the scan.
Each underlying read is at most 8192 bytes, and each segment range is read once.
Concurrent readers retain independent offsets and pinned file handles.

## Integrity and admission

The existing compressed-envelope header grammar is shared with the resident
decoder. Compressed range admission remains the reader's existing open-time
policy; declared output must fit its uncompressed limit before decompression.
Inflation stops at the declared size plus one byte, not the larger reader limit.
The entire selected segment is checked for range and compressed checksums,
compressed and uncompressed lengths, uncompressed checksum, UTF-8 and document
syntax, document count, first/last identities and strictly increasing IDs.
Unrequested documents are also decoded and validated. Finding the requested
document never skips a damaged suffix or exposes a partial successful result.
Range checksum errors retain precedence over syntax errors, without a checksum
prepass or a second file traversal.

Hydrated output admission is cumulative across segments. Requested IDs must
remain unique; public output follows request order. Empty requests, missing IDs,
old-reader retention and all-or-nothing public return semantics are unchanged.
The update writer's existing full-segment visitor remains unchanged: it validates
a complete segment before invoking its consumer. This query-only slice must not
silently weaken that publication/recovery boundary.

## Remaining resident floors and limitations

- The largest encoded document line and current decoded document coexist. The
  reusable line capacity can remain until the segment scan ends. Fields,
  embedding vectors and metadata allocations remain document-sized.
- Requested results, ID sets, the descriptor and dictionary remain resident.
- The envelope header is bounded by existing compressed-range admission; this
  change does not impose a new cap on noncanonical accepted header spellings.
- zstd's internal workspace is not accounted by an engine reservation here.
- `peak_segment_document_bytes` measures logical decoded bytes retained by the
  segment scan: selected documents so far plus the current document. It excludes
  line/header capacities, collection overhead, decoder workspace and results
  retained from earlier segments. It is not process RSS or a total-memory cap.
- Output-byte admission is checked after decoding one document. It does not
  become a pre-allocation limit on that document's source or fields.

Typed streaming input, shared host-memory reservations, cancellation/deadlines,
adaptive profiles, update/delete floors and complete-corpus qualification remain
separate #392 obligations. No 4 MiB default, analyzer behavior, published v1 bytes,
public API, release policy, Bazel configuration or CI job is changed.

## Local verification

The private scanner is compared with the retained resident decoder using random
documents, Unicode/control characters, metadata and embedding values. Boundary
tests cover exact/one-short output and decode limits, CRLF and final-line grammar,
concatenated zstd frames, corrupt tails, descriptors, short/interrupted reads and
every byte-cut/error position of a small envelope. Public-path tests cover
retained decoded working sets, request order, budgets and pinned old readers.

The 512-case campaign is explicit and local-only:

```sh
bazel test //crates/search:hawdb_search_hydration_fuzz_tests
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```
