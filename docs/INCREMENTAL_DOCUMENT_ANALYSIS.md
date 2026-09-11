# Incremental Lexical Document Analysis

This is the private token-emission prerequisite of #392. Lexical generation
builds and mini-delta upserts/deletes no longer collect all document tokens
before checking their limits and accumulating frequencies. It does not remove
the 4 MiB source limit, change #325's long-token policy, or complete large-document
support.

## Semantic contract

`analyzer_stream::visit_token_list` shares one field traversal with the existing
collecting tokenizer. It emits owned tokens and their occurrence kind to a
fallible callback; it does not create another whole-field sequence itself.

The existing analyzer has three distinct repetition rules:

- Inside one identifier, normalized forms, CJK terms, suffixes and aliases are
  locally deduplicated by the existing identifier analyzer.
- Ordinary occurrences from subsequent identifiers retain their multiplicity.
- Adjacent-identifier phrases and their expansions are emitted only if that
  term has not appeared earlier in the same field, including as an ordinary
  identifier occurrence. Empty delimiters do not reset the preceding part.

`DocumentAnalysis` stores the frequency and last-seen field on each distinct
term. A unique-in-field emission is ignored only when its marker matches the
current field. Every accepted ordinary occurrence updates the marker too.
There is no separate document-wide seen-term set or occurrence-order vector.

The title keeps weight two; content keeps weight one. The selected metadata
fields remain `kind`, `external_id`, `source_id`, and `space_id`, each with
weight one and an independent phrase-uniqueness scope. Title weighting is
applied directly to counts and document length, without materializing or
re-analyzing a duplicated title token sequence in the lexical accumulator.
The collecting wrapper retains the original emitted sequence for callers
that still request a vector of tokens.

Identifier splitting semantics, Unicode lowercasing, Jieba, CJK n-grams, stopwords,
aliases, field selection, analyzer fingerprint and persisted artifact format
are unchanged. Physical chunks are not introduced as separate documents or
as independent analyzer scopes.

## Identifier working memory

Field traversal decomposes each raw identifier once, reusing the same parts for
cross-word phrases, individual part tokens, and adjacent-part pairs. The private
splitter uses one-character lookahead instead of collecting the entire input as
`Vec<char>`. ASCII case/digit boundaries, acronym lookahead, underscores, Unicode
lowercase expansion, and the exact token order are preserved. Matched-span
callers still defer their split until the CJK helpers have released their scratch.

This removes a source-sized character buffer and a duplicate parts collection,
not the parts/output working set itself. The splitter's lookahead is constant
space; its returned `Vec<String>`, whole-run token sequence, CJK buffers and Jieba
allocations remain input-dependent. This is neither a new allocation-admission
boundary nor a whole-process memory guarantee.

## Admission and failure behavior

The source-byte guard is still checked before analysis and still counts title,
content, and all metadata keys/values, including metadata not selected for
search. Term size and weighted token count are checked as terms are emitted.
New frequency-map entries are admitted before insertion; their existing
estimated term/entry bytes also include the temporary field-marker overhead.
The completed delta retains only plain frequencies, so its resident-byte
estimate does not retain temporary marker charges.

Token errors now report the minimum count observed at the point of rejection,
not a count obtained by analyzing the rest of a rejected document. When
multiple limits are violated, the first detected emission limit may win;
source-size admission still runs first. Very small build budgets can reject
earlier because previously uncounted marker state and the empty accumulator's
base bytes are now admitted explicitly.

Callback errors propagate immediately: no later identifier is visited. A
rejected document never enters the mini-delta, and failed generation builds
retain the published manifest/artifact. This callback boundary is not a new
public cancellation API or proof that opaque analyzer calls can be interrupted.

## Remaining minimum working units

This change removes the whole-document token collection from lexical build
and delta analysis, not every document-sized allocation:

- `SearchDocument` still owns complete source strings. Spool decode, encoding,
  hydration, and legacy/in-memory collecting consumers retain their existing
  input/output working sets.
- Identifier parts, lowercase strings, local token deduplication, CJK run
  buffers, Jieba output and alias expansion still operate on a complete raw
  identifier/run. A single long uninterrupted run can still require a large
  working set before the first emitted token is checked.
- Distinct-term frequencies remain resident until a document is complete.
  Field markers are bounded by that same distinct-term map, not by occurrence
  count, but document-local spilling/reduction is still needed for large
  high-cardinality documents.
- Map entry accounting is an estimate, not a full allocator/RSS ledger. Input,
  analyzer scratch, map-node conversion, posting buffers and other concurrently
  live state still need shared admission and measured peak-memory evidence.

Keep #392 open for document-local spill, bounded input/decode/hydration/update
paths, shared resource profiles, analyzer minimum-unit decisions, and complete
large-document lifecycle qualification before removing the fixed source cap.

## Verification

The tests retain the pre-streaming field/document traversal and the original
character-vector splitter as independent references. Only unchanged CJK and
term-expansion helpers are shared, not identifier decomposition, the new visitor,
or the accumulator. Ordinary cases and a seeded 768-case local campaign
compare complete token sequences, frequencies, lengths and retained estimates
across empty/default/application lexicons, repeated identifiers, mixed scripts,
aliases, stopwords, punctuation and all selected metadata fields.

Additional contracts cover exact/one-short marker admission, propagation of
callback errors, early token-limit rejection, and repeated-term frequency
accumulation with a small map budget. The early-stop guard counts actual
identifier visits; returning to a buffered visitor must fail even if all
successful query results are identical. Corpus tests compare every reference
term's exact matching count and BM25 score through publication, update/delete,
failed replacement, checkpoint/reopen and failed publication. Existing spill,
ACL, corruption and recovery tests remain required.

The identifier boundary test exhausts all sequences of up to four characters
from a mixed-script eight-character alphabet. A separate allocation-test binary
includes the production splitter and wraps the system allocator only within
that binary. It compares exact output and cumulative requested allocation bytes
(including reallocations) against the frozen splitter on long ASCII,
camel-case/digit, and expanding-Unicode inputs. Inputs are created outside the
measurement window. The assertion requires savings of at least one complete
character buffer, without a timing threshold or a claim about RSS/live peaks.
An actual split-call counter also rejects duplicate decomposition in the field
visitor, including its early-error path.

```sh
cargo test --locked -p skein-search --test identifier_allocation -- --nocapture
bazel test //crates/search:skein_search_identifier_allocation_tests
```

The seeded campaign is ignored by ordinary Cargo tests, tagged manual in
Bazel, and included in the existing explicit local fuzz suite. No CI fuzz job
or test timeout/resource change is added.

```sh
cargo test -p skein-search --all-features -- --include-ignored
cargo clippy -p skein-search --all-targets --all-features -- -D warnings
bazel test --nocache_test_results //crates/search:presubmit_tests //:skein_unit_tests //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
```
