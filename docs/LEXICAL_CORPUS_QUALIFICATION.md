# Complete-corpus lexical artifact qualification

Issue #206 requires a measured order-of-magnitude reduction in physical posting
bytes, with unchanged logical results. A nominal estimate of the legacy record
size, a synthetic repeated-term fixture, or an incomplete build is not that
evidence. The issue remains open until the complete artifact pair and its other
resource, semantic, and lifecycle gates are qualified.

## Reproducible host input

`examples/lexical_corpus_qualification.rs` is a developer-only measurement wrapper
over HawDB's embedded generation writer and reader. It is not a production
control plane. Supply an immutable UTF-8 JSONL file containing
`content_message_id` and `content` strings, and the expected complete record count
from the export manifest. Record the archive and extracted-file SHA-256 before
the run, verify the source hash again afterward, and retain both receipts.

The wrapper validates every record, rejects empty or duplicate IDs rather than
filtering them, and orders the complete input by UTF-8 document ID. It preserves
IDs and body content, with empty title/metadata and no embedding. Its offset/ID
index is harness-owned memory, not database working-memory evidence. It reports
aggregate counts and bytes, never private source text or IDs.

Use the same source, mapping, default analyzer, explicit finite term policy, and
unchanged execution budgets for both layouts. The term and manifest limits are
required arguments, passed through the public writer and reader controls. They
do not change the 4096-byte term or 256 MiB manifest product defaults or waive any
independent resource limit. Source/term normalization, sampling, truncation, and
skipped records are not permitted to make a measurement pass.

Build the legacy receipt with the final pre-FST layout and build the compact receipt
with `HAWDB_LEXICAL_MANIFEST_V3` / `HAWDB_LEXICAL_ORDINAL_FST_V1`. Each output root
must be new. The compact invocation receives the legacy receipt and fails unless
the source facts, logical lexical counters, selected policies, and the required
tenfold physical-posting reduction agree.

```sh
cargo test --locked --example lexical_corpus_qualification
cargo build --locked --release --example lexical_corpus_qualification

# Run from the legacy-layout checkout, then preserve legacy-receipt.json.
./target/release/examples/lexical_corpus_qualification /path/to/thread_messages.jsonl /path/to/legacy-generation 334844 1048576 536870912 > legacy-receipt.json

# Run from the FST-layout checkout with the same source and limits.
./target/release/examples/lexical_corpus_qualification /path/to/thread_messages.jsonl /path/to/fst-generation 334844 1048576 536870912 legacy-receipt.json > fst-receipt.json

bazel test //:hawdb_lexical_corpus_qualification_tests
```

The binary is also available as the manual Bazel target
`//:hawdb_lexical_corpus_qualification`. No corpus or fuzz campaign is added to CI.
The 1 MiB term policy and 512 MiB manifest budget above are owner-approved for
qualification. All remaining build options are default values and are included
in the report alongside the selected term policy, manifest budget, and analyzer
digest.

## Artifact comparison

The output directory must not exist; measurements never overwrite a prior
generation. A report is emitted only after successful generation publication,
physical-file length checks, category reconciliation, and reopening with the
same reader-side policy. Counts must match both the source and manifest.

- Legacy posting bytes are the sum of actual contiguous posting-block extents,
  including their headers. Its JSON term statistics remain in the separately
  counted manifest.
- Compact posting bytes are the exact physical `Postings` block extents. The FST
  dictionary is co-encoded in those blocks; there is no separate dictionary byte
  category. The legacy nominal posting counter is reported for diagnosis but is
  never used as the comparison baseline.
- Compare complete document counts, document digest, analyzer digest, posting
  counts, and total document lengths before reporting the size ratio. Record
  both physical artifacts and manifest sizes, not only the favorable category.

The two category regressions distinguish actual extents from nominal payload
estimates and reject inconsistent category/offset accounting. Logical BM25,
ranking, update/delete, cancellation, corruption, admission, and reopen
differential tests remain separate required gates. This wrapper does not claim
those gates merely because artifacts have matching aggregate counters.

Failed runs retain no successful qualification result. Failed-build timing or
RSS is diagnostic evidence only, never a performance comparison. The wrapper's
elapsed time is not foreground latency, and neither its offset index nor the
database's logical counters establish a whole-process memory bound.
