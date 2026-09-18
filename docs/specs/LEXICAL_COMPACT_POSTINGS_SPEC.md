# Lexical Compact Postings and Encoding-Aware Scan Specification

## Implementation Status

This is a design-stage contract with **no implemented surface in the HawDB
crates yet**. It defines the target on-disk encoding and read-path contract
for [#206](https://github.com/nowledge-co/hawdb/issues/206) (compact
postings layout) and the encoding it must expose for
[#292](https://github.com/nowledge-co/hawdb/issues/292) (block-max pruning)
to build on. Nothing here describes shipped behavior. The current lexical
projection format (`crates/search/src/lexical_projection.rs`,
`block_encoding.rs`, `manifest_encoding.rs`) is unaffected until an
implementation lands under a new manifest format version, following the
existing generation model: a new generation, never an in-place migration of
an existing artifact.

## Scope and Motivation

Today a `Posting` (`lexical_projection.rs:319`) stores its full `term:
String` and `document_id: String` on every entry, and a query resolves a
term to candidate blocks purely by block-level `min_key`/`max_key` string
bounds (`posting_blocks()`, `lexical_projection.rs:845`), then decodes and
linearly filters every posting in every matching block. A term that appears
in a large fraction of documents — the common case for the CJK n-gram
analyzer's short shingles — repeats its own string bytes once per posting,
and a query pays full decode cost for postings that belong to blocks whose
key range happens to contain the term but whose entries mostly belong to
other terms.

HawDB's positioning is a fixed small memory budget (0.5-2 GiB) against
on-disk data that can reach the tens of GB on a home PC — see
`docs/SEARCH_BUILD_RESOURCE_OWNERSHIP.md` for the resource-accounting
infrastructure this must plug into. Under that
ratio, on-disk bytes and bytes-decoded-per-query are the metrics that matter
more than raw throughput, which is why this spec treats **encoding-aware
scan** — the read path's ability to skip decoding work it does not need —
as a first-class requirement alongside the on-disk size reduction, not a
follow-on optimization.

### Non-Goals

- **No lossy or approximate compression.** Every encoding here is a
  reversible transform (varint, delta, dictionary reference) over the exact
  same logical postings the current format stores. Query results (hit sets,
  scores, BM25 statistics) MUST be bit-identical to the current format's,
  verified by differential test — the same bar #292 already sets for block-max
  pruning.
- **No generalized per-encoding compute-kernel framework.** This is not an
  adoption of a Vortex-style universal "operate on any encoding" execution
  layer. The scan-side contract below (single term-decode per matched run,
  lazy document-ID resolution, block-level skip via a reserved max-tf field)
  is grafted onto the specific read paths #206 and #292 already need. A
  generalized encoding-aware compute layer is out of scope unless a third
  consumer independently needs the same shape.
- **No in-place migration.** An existing generation's artifact and manifest
  remain readable under their current format version forever. A build under
  this spec always starts a new generation; reopening an old generation uses
  the old decode path unchanged.
- **No change to logical limits or query semantics.** `max_term_bytes`,
  `max_document_tokens`, `mini_delta_bytes`, BM25 parameters, and the
  existing finite term/source policies are unchanged. The mini-delta overlay
  (small, resident, and always scored exhaustively, per
  `docs/LEXICAL_MINI_DELTA_OWNERSHIP.md`) is unaffected; this spec only
  touches immutable segment postings.

## Document Ordinal Assignment

Each lexical generation assigns a dense, contiguous `DocumentOrdinal(u32)`
per document, `0..document_count`, in the same ascending-ID order the
`Documents` blocks (`BlockKind::Documents`, `block_encoding.rs`) already
write. This reuses an established pattern in this codebase — the out-of-core
generation writer already assigns dense `vector_ordinal`s per-generation for
the vector projection (`out_of_core/generation_writer.rs:741-752`) — but the
lexical ordinal space is its own, independent of the vector one: the two
projections have independent generation lifecycles and MUST NOT be coupled
by sharing a numbering space.

- The ordinal table is derived, not stored separately: ordinal `i` is the
  `i`-th entry (by ascending document ID) across the generation's
  `Documents` blocks. A reader reconstructing ordinal → ID needs only the
  existing `Documents` blocks it already decodes for other purposes; no new
  artifact section is required for the forward mapping.
- Build order changes: because postings must be written with ordinals
  resolved, the build pipeline needs the complete document ID set assigned
  before postings encode. `SegmentArtifactBuilder`'s existing per-generation
  document set (already fully known before postings for that generation are
  finalized — see `docs/SEARCH_BUILD_RESOURCE_OWNERSHIP.md`'s two-phase
  writer/spool flow) already has this property; no additional pass over the
  document set is required, only reordering within the existing pipeline
  stage.
- `DocumentOrdinal` values are per-generation and MUST NOT be persisted or
  compared across generations. A reopened index resolves ordinals fresh from
  that generation's `Documents` blocks.

## On-Disk Encoding

### Varint and Delta Format

Unsigned integers (`DocumentOrdinal` deltas, per-posting `term_frequency`,
`document_len`, dictionary offsets) encode as unsigned LEB128: 7 bits of
payload per byte, high bit set on all but the final byte. This is a
well-understood, allocation-free, self-describing format requiring no new
external dependency.

### Posting List Layout

A posting block's entries for one term are a **run**: the term string is
recorded once (via the term dictionary below, not inline per posting), and
the run's postings are sorted ascending by `DocumentOrdinal` — a new sort
key replacing today's implicit document-ID-string order within a term. Each
posting in a run encodes as:

```text
delta_ordinal: varint   // DocumentOrdinal - previous run entry's DocumentOrdinal
                         // (first entry: DocumentOrdinal - 0)
term_frequency: varint
document_len: varint    // unchanged semantics from today's u32 field
```

`document_id: String` is dropped from the per-posting record entirely. A
consumer that needs the string ID (only true for postings that survive
filtering into the final top-k, or that a diagnostic explicitly requests)
resolves it lazily via the ordinal → ID mapping described above. This is the
core encoding-aware-scan property: **decoding a posting's rank contribution
never requires resolving or copying a document-ID string.**

### Per-Block Term Dictionary

Each postings block gains a dictionary section, written after its posting
runs, mapping every distinct term in the block to:

```text
term: length-prefixed UTF-8 string  // unchanged wire representation
run_offset: varint                  // byte offset of the run within this block
run_posting_count: varint
document_frequency: varint          // folds in what TermStatistics carries today
max_term_frequency: varint          // reserved for #292; block-local upper bound
                                     // over this run's term_frequency values
```

This replaces two things at once, closing both #206's fix #3 and its stated
DF-double-read resolution:

- **Block-level key bounds remain** (`BlockDescriptor.min_key`/`max_key`,
  unchanged) as the coarse first-pass filter deciding which blocks to open at
  all — this part of `posting_blocks()` is retained.
- **The per-block dictionary replaces linear posting-by-posting term
  filtering inside a matched block.** A reader opens a candidate block,
  decodes only its dictionary section (small relative to the block: one
  entry per distinct term, not per posting), looks up the query term, and if
  present, seeks directly to `run_offset` and decodes exactly that run — no
  other term's postings in the block are touched.
- The top-level manifest's `Vec<TermStatistics>` (`ManifestBody.term_statistics`,
  `lexical_projection.rs:171`) is retired for compact-format generations; a
  term's `document_frequency` comes from summing its per-block dictionary
  entries across the manifest's blocks. Manifest sizing/decode-budget
  accounting (`docs/SEARCH_BUILD_RESOURCE_OWNERSHIP.md`'s manifest decode
  boundary) MUST account for the dictionary sections the same way it
  accounts for `term_statistics` today — this is a redistribution of that
  same budgeted data, not new unbounded growth.

### Format Versioning

`ManifestBody.format` gains a new value (the existing field is already a
string tag, not a fixed enum, so this is additive). A manifest at the new
format version implies compact postings and per-block dictionaries for every
`Postings` block it references; there is no mixed-format manifest. Readers
MUST reject a manifest whose `format` they do not recognize rather than
attempt to interpret its blocks under the wrong layout — this is the
existing fail-closed posture for manifest validation, unchanged.

## Encoding-Aware Scan Contract

This section is normative for both this spec's own read path and for #292,
which depends on it.

- **A block's dictionary decodes at most once per query per block**, even
  when the query has multiple terms that both hash into the same block's key
  range. Implementations MUST cache the decoded dictionary for the duration
  of one block's processing within one query, not re-decode it per term.
- **A run decodes only when its term matches a query term.** Blocks (or,
  once dictionaries are in place, individual runs within an opened block)
  whose dictionary entry does not match any query term MUST NOT have their
  posting bytes read past the dictionary lookup.
- **Document-ID string resolution is deferred to the final hit set.**
  Scoring, top-k ranking, and BM25 accumulation operate entirely on
  `DocumentOrdinal` and the decoded `term_frequency`/`document_len` fields.
  Only postings that survive into the caller-visible result (or are needed
  for the `allowed()` filter callback — see below) resolve their ordinal to
  a document-ID string, via the `Documents` blocks' existing decode path.
- **Filtering (`allowed()` callbacks — ACL/metadata) currently takes a
  `&str` document ID.** This spec requires either (a) widening the filter
  contract to accept `DocumentOrdinal` with the string resolved once and
  cached per ordinal for the query's duration, or (b) resolving eagerly per
  candidate before filtering. Option (a) is preferred: it keeps the
  ordinal-only property intact for the (common) case where a candidate is
  filtered out, avoiding a string resolution that scoring didn't otherwise
  need. This is an open decision for the implementing PR, not resolved here.
- **`max_term_frequency` is reserved, not consumed, by this spec.** This
  spec's own scan path does not do block-skip scoring; it populates the
  field so #292 can implement WAND-style pruning against it without a
  further format bump. #292's differential-test and `blocks_skipped`
  observability requirements apply once that layer lands, not to this one.

## Resource Contract

Dictionary decode, run-seek buffers, and the ordinal-delta decode state MUST
go through the same `BuildMemory`/`ReservedMemory` admission ledger the rest
of the #392 series already uses (`docs/SEARCH_BUILD_RESOURCE_OWNERSHIP.md`
and its per-stage extensions for spill, analyzer, and token ownership) — this
spec does not introduce a new resource-accounting mechanism, it is a
consumer of the existing one. Query-side
decode (as opposed to build-side) uses the existing `query_memory_bytes`
budget (`LexicalProjectionConfig`) the same way today's decode does; the
budget's meaning does not change, only what it is spent on (dictionary +
run bytes instead of every posting's inline strings).

## Verification

- **Differential test against the current format**: for a range of corpora
  (including the CJK n-gram heavy-tail case this spec is motivated by),
  compact-format and current-format builds of the same input produce
  identical BM25 scores, identical top-k hit ordering, and identical
  document-frequency statistics.
- **Size reduction acceptance gate** (from #206, restated): on-disk posting
  bytes drop by an order of magnitude on a representative corpus with
  high-frequency terms; record before/after in the artifact manifest
  counters, per the existing convention in this codebase's PR validation
  sections.
- **Decode-avoidance observability**: query reports gain a counter for
  postings bytes decoded vs. postings bytes present in opened blocks, so a
  regression that silently falls back to per-posting term filtering (instead
  of using the dictionary) is visible in existing benchmark/qualification
  tooling, not just in a targeted unit test.
- **Reopen/generation compatibility**: a generation written under the
  current format remains fully readable after this lands (no manifest
  format bump forces a rebuild); a reopened index only produces compact
  postings for generations built after the implementing PR.

```sh
cargo test -p hawdb-search
cargo test -p hawdb-search --no-default-features --lib
cargo clippy -p hawdb-search --all-targets -- -D warnings
bazel test //crates/search:all //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```
