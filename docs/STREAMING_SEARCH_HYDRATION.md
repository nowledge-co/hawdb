# Streaming selected-document hydration

This private implementation slice advances #392 without changing its complete
large-document acceptance criteria or selecting the pending #325 term policy.

For bodies larger than the operation reservation, the additive
[candidate and verified-body APIs](STREAMED_DOCUMENT_LIFECYCLE.md) validate into
private disk and return a sealed reader. The owned hydration path described here
retains its full-output admission contract.

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
remain unique; a repeated ID is refused as `HawDBError::Semantic`, and more IDs
than `SearchOutOfCoreConfig.max_hydrated_documents` as a typed
`ResourceBudgetExceeded` naming that option (#1754); its budget owner is
`search hydration by id` (#1767). Public output follows request order. An ID the
published projection does not contain, including a deleted one, refuses the
whole request as `HawDBError::Semantic` (#1767); the resident embedded path
refuses it the same way instead of skipping it, so a successful result always
has one document per requested ID. The same absence during search-hit
hydration is index damage and remains `HawDBError::Storage`. Empty requests,
old-reader retention and all-or-nothing public return semantics are unchanged.

Hosts that need to ask which IDs are present, for example to verify a
projection against its source, use lookup by id instead (#1786):
`SearchOutOfCoreReader::lookup_documents` and, under one pinned projection on
either the resident or the out-of-core path,
`EmbeddedReadSnapshot::lookup_search_projection_documents`. The result names
every requested ID once, as a hydrated document or as a missing ID, both in
request order. An ID is missing when the published projection does not contain
it: never published, or deleted (hidden by deletion visibility). Lookup by id,
like hydration by id, takes no access-control context, so search
access-control policy does not hide a document from it. Duplicate IDs remain
`HawDBError::Semantic`. On the out-of-core path more IDs than
`SearchOutOfCoreConfig.max_hydrated_documents` is a typed
`ResourceBudgetExceeded` naming that option, with budget owner
`search lookup by id`, and the hydrated-byte cap applies as in hydration by id.
The snapshot call also refuses a request with more IDs than the snapshot's
remaining `max_rows` before any read, and charges found documents to its rows
and payload bytes. Absence is decided by the published document routes alone:
an ID they list whose document cannot be read or verified is still
`HawDBError::Storage`, never reported missing. No on-disk format changes.
The streamed update and compaction paths share complete-segment validation and
keep consumer writes private until the validation and publication fences pass.
Later corruption discards the candidate; it cannot publish a valid prefix.

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
