# Incremental lexical frequencies and token admission

Generation construction no longer builds an entire document token vector before
checking its token count and accumulating frequencies. The analyzer visits each
field and identifier, accumulating the same weighted frequencies directly.

The serving token-list implementation and the build path share borrowed recipes
for identifier spans, CJK n-grams and suffix variants. The tests freeze the
preceding allocating algorithms from `2b9caa56` as an independent reference; they
do not call those new production recipes. The incremental path preserves:

- title term-frequency weighting without duplicating the title token vector;
- local deduplication within one identifier, including normalized/alias tokens;
- repeated contributions from the same token in separate identifiers;
- boundary bigrams and their expansions, deduplicated across the whole field;
- field-local deduplication reset between title, content and each indexed
  metadata field (`kind`, `external_id`, `source_id`, `space_id`);
- ignored metadata remaining unindexed, while all metadata still counts toward
  the existing source-byte limit.

The token limit is checked before each contribution, and term/build-resident
limits before a new frequency key. A long repeated-word document stops near its
token limit without analyzing the rest of its identifiers. Within one identifier,
Skein now emits tokens through a fallible visitor instead of first collecting the
whole output. Cancellation is checked before generation, before Jieba, and while
emitting tokens/parts/pairs. The dependency call itself is not interruptible.

## Borrowed recipes and scratch

Identifier splitting walks borrowed UTF-8 spans with one-character lookahead.
It allocates neither a character vector nor a list of lowered parts. CJK n-grams
borrow two/three-character substrings in the preceding emission order. Adjacent
pairs revisit the borrowed parts after all single parts have been emitted.
Cross-identifier boundaries retain a slice of the original field, not a cloned
previous-part string. Alias expansion visits borrowed configuration strings one
at a time, with no temporary cloned alias vector or recursive alias expansion.

Skein-owned normalized and boundary strings are sized without allocation, then
reserved before construction. Identifier-local deduplication admits its B-tree
node allowance and key before invoking the consumer or cloning that key. The
consumer enforces token/term limits before the local key is copied. The field
set and frequency map reserve their own copies; all simultaneous owners share
the same operation root. Fallible visitors release both scratch and dedup state.

The full raw token deliberately retains `str::to_lowercase`, including contextual
Greek final sigma. Identifier parts still use per-character lowercasing. For
non-ASCII raw tokens, the pinned Rust 1.97.1 implementation starts with input-byte
capacity and pushes mapped characters into a geometrically growing string. The
preflight reserves `3 * max(input_bytes, mapped_bytes, 8)`, conservatively covering
old and replacement allocations. It then reduces the charge to returned capacity
only after construction. This source-backed envelope must be rechecked on a Rust
upgrade; it is not a public standard-library allocation guarantee or RSS claim.
ASCII and Skein-constructed strings request their exact computed byte capacity.

## Ownership and budget sharing

The generation writer passes its existing `BuildMemory` to the lexical writer.
It does not create a second independent copy of the task budget. A standalone
lexical writer creates its own operation ledger from its task context. The three
accounts remain fixed for the operation, rather than growing per document.

Frequency keys and field-deduplication entries use checked, conservative B-tree
node allowances plus string capacity. A frequency key is admitted before cloning
and insertion. Local dedup sets and token scratch release their charges with
their data. Frequency data retains its lease until its consuming
iterator drops; a posting chunk reserves ownership of moved term capacities so
the analyzer cannot release still-live terms from the shared budget.
The writer keeps the result owner intact across fallible checks. Splitting its
data and lease into local bindings could reverse error-path destruction order;
the owner instead drops the data before its lease, including before iteration.

Posting chunks separately charge term capacities and allocated vector slots.
Vector growth reserves the new slots while the old slots remain charged,
covering replacement overlap. A successful spill drops terms and releases their
charges but keeps the reusable slot allocation charged. All chunk capacity is
dropped before external merge. A failed spill/build releases state on unwind
without selecting a partial generation.

The mini-delta path uses the same incremental frequency semantics and existing
component limits. It does not yet retain an operation-wide ledger across its
long-lived delta/base maps; that is separate remaining query/delta work.

## Verification and remaining boundaries

Normal regressions compare complete frequencies against the preceding token-list
path across empty input, punctuation, case/identifier boundaries, aliases,
stopwords, CJK/supplementary Han and Unicode lowercasing. They also check early
limit/cancellation rejection, pre-insertion admission, constant retained-state
usage for repeated terms, term ownership after analyzer drop, slot-reallocation
overlap, failed publication cleanup and the actual fused writer's shared root.

The manual `skein_search_lexical_analyzer_fuzz_tests` target contributes 12,000
seeded document cases and 12,000 identifier cases to mandatory local fuzz,
including four lexicons, title/content/metadata mixtures, ordered token equality,
exact/one-short token and identifier-memory limits, and cancellation. It also
checks lowercase admission/release for all 1,112,064 Unicode scalars.
It is ignored by ordinary Rust tests and is not a CI fuzz job. Removing title
weighting must fail the differential campaign; detaching the fused shared root
must fail the integration regression. Omitting token scratch admission must fail
before-allocation evidence; breaking a shared identifier boundary must fail the
frozen-oracle campaign. These negative controls are restored before verification.

The ledger covers retained frequencies, field sets, ordering IDs, posting chunks
and Skein-owned token scratch/deduplication, not all analyzer allocations. In pinned
`jieba-rs` 0.10.3, `cut_internal` builds DAG/route/token vectors, while its HMM
context is thread-local and retains scratch capacity beyond a call. Releasing a
per-call lease would not account for that retained TLS capacity. The shared
dictionary initialization is also not included in these operation counters.

Jieba's borrowed word visitor avoids a second Skein-owned `Vec<String>`, but its
dependency-owned result vector still needs admission alongside DAG/HMM state
before claiming complete analyzer admission. Artifact document buffers/directories,
manifest output and checksum scratch now share the build root as described in
`LEXICAL_ARTIFACT_ADMISSION.md`; run readers, heap/current postings, frame and
doclist encoding follow `LEXICAL_MERGE_ADMISSION.md`; grouping-term/dictionary
staging and FST follow `LEXICAL_DICTIONARY_ADMISSION.md`.
Segment codecs, descriptors and publication follow `SEGMENT_BUILD_ADMISSION.md`.
The generation RaBitQ sink follows `RABITQ_BUILD_ADMISSION.md`.
Published-reader and outer query/delta ownership still need
the same whole-operation treatment. Existing component estimates, new ledger
peaks and token-parity tests do not establish allocator/RSS bounds or #206's
native-platform and representative-corpus acceptance gates. No analyzer digest,
v1 wire format, production backend or public API changed in this step.
