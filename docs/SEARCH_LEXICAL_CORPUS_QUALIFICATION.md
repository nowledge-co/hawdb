# Search lexical complete-corpus qualification

`examples/search_lexical_corpus_qualification.rs` is a developer-only wrapper
over HawDB's embedded writer and reader. It takes an explicit local JSONL corpus
whose records contain `content_message_id` and `content`, verifies every record,
rejects empty or duplicate IDs, and pushes the complete input in UTF-8 ID order.
It writes only aggregate JSON evidence to stdout.

The build command selects the same explicit term policy and lexical-manifest
budget for the writer and reader. The output root must not exist. A report is
emitted only after successful publication, physical artifact reconciliation, and
reopen under the same reader limits.

```sh
cargo test --locked --example search_lexical_corpus_qualification
bazel test //:hawdb_search_lexical_corpus_qualification_tests
cargo run --locked --release --example search_lexical_corpus_qualification -- \
  build /path/to/thread_messages.jsonl /path/to/compact-output 334844 1048576 536870912 \
  > compact-report.json
```

The report records source counts and aggregate source bytes, the selected
policies, build report, physical document/posting artifact extents, lexical
manifest length, and the reopened document count. It intentionally does not
report source text or document IDs. Its source index belongs to the harness and
is not process-memory evidence for HawDB.

Issue #206 requires a physical comparison against the preserved pre-ordinal
layout using the identical immutable source and mapping. Produce that legacy
report with the maintained old-layout qualification collector, then compare the
two reports:

```sh
cargo run --locked --release --example search_lexical_corpus_qualification -- \
  compare legacy-report.json compact-report.json
```

Comparison rejects unequal source counts or bytes, source mapping/order, term
policy, manifest budget, document count, posting count, total document length, analyzer digest,
or document digest. Only then does it compute the physical posting-byte ratio
and state whether it meets the required order-of-magnitude gate.

The legacy report must declare `legacy-string-postings-v1`; the compact report
must declare `HAWDB_LEXICAL_ORDINAL_FST_V1`. Unknown layouts are not interchangeable
with the measured legacy baseline. Both reports must record the same nonzero
integer `manifest_budget_bytes`; old receipts that omit it must be regenerated
with the selected writer/reader policy, not patched with an assumed value.
Keep independent source hashes with the receipts: matching aggregate counts and
the engine's document digest are not cryptographic proof of identical input.

For positive physical byte counts L and C, the gate is exactly L >= 10C.
Both operands originate as u64 and are widened to u128 before multiplication.
Since 10(2^64 - 1) < 2^68 < 2^128, the product cannot overflow, and widening
preserves the natural-number ordering. The displayed floating-point ratio is
informational and cannot affect the decision. Saturating multiplication is
incorrect here: when C > floor((2^64 - 1)/10), it can turn a failing comparison
with L = 2^64 - 1 into a pass. Boundary regressions cover that counterexample,
exact tenfold ratios, and ratios immediately below and above the threshold.

The ordinary Bazel test target is included in `//:hawdb_unit_tests`. It exercises
synthetic evidence validation and does not read a private corpus or constitute
a corpus qualification receipt. No fuzz CI job is added.

This collector covers artifact bytes and reopen only. BM25 and ranking parity,
update/delete behavior, cancellation, corruption recovery, host-resource
governance, latency, write amplification, and the rest of the full lifecycle
remain independent qualification gates.
