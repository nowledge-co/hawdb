# Incremental lexical frequencies and retained-state admission

Generation construction no longer builds an entire document token vector before
checking its token count and accumulating frequencies. The analyzer visits each
field and identifier, accumulating the same weighted frequencies directly.

The serving token-list implementation remains the differential reference. Both
paths share identifier normalization, CJK/Jieba generation and alias rules, but
not field traversal or frequency accumulation. The incremental path preserves:

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
token limit without analyzing the rest of its identifiers. This is not a claim
that one identifier's tokenizer call is interruptible or admitted yet.

## Ownership and budget sharing

The generation writer passes its existing `BuildMemory` to the lexical writer.
It does not create a second independent copy of the task budget. A standalone
lexical writer creates its own operation ledger from its task context. The three
accounts remain fixed for the operation, rather than growing per document.

Frequency keys and field-deduplication entries use checked, conservative B-tree
node allowances plus string capacity. A frequency key is admitted before cloning
and insertion. Field-local sets and previous identifier parts release their
charges with their data. Frequency data retains its lease until its consuming
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
seeded differential cases to mandatory local fuzz, including four lexicons,
title/content/metadata mixtures, exact/one-short token limits and cancellation.
It is ignored by ordinary Rust tests and is not a CI fuzz job. Removing title
weighting must fail the differential campaign; detaching the fused shared root
must fail the integration regression.

The ledger covers retained frequencies, field sets, ordering IDs and posting
chunks, not all analyzer allocations. `identifier_parts`, `identifier_tokens`
and boundary-token helpers still allocate temporary token state. In pinned
`jieba-rs` 0.10.3, `cut_internal` builds DAG/route/token vectors, while its HMM
context is thread-local and retains scratch capacity beyond a call. Releasing a
per-call lease would not account for that retained TLS capacity. The shared
dictionary initialization is also not included in these operation counters.

Identifier/Jieba scratch needs an explicit bounded ownership design before
claiming complete analyzer admission. Artifact document buffers/directories,
external merge, FST, codecs, RaBitQ and outer query/delta ownership likewise need
the same whole-operation treatment. Existing component estimates, new ledger
peaks and token-parity tests do not establish allocator/RSS bounds or #206's
native-platform and representative-corpus acceptance gates. No analyzer digest,
v1 wire format, production backend or public API changed in this step.
