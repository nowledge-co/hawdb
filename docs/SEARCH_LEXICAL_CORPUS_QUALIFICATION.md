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
policy, document count, posting count, total document length, analyzer digest,
or document digest. Only then does it compute the physical posting-byte ratio
and state whether it meets the required order-of-magnitude gate.

This collector covers artifact bytes and reopen only. BM25 and ranking parity,
update/delete behavior, cancellation, corruption recovery, host-resource
governance, latency, write amplification, and the rest of the full lifecycle
remain independent qualification gates.
