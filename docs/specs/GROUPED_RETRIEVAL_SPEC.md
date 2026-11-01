# Complete lexical retrieval and bounded grouped evidence

Issue Number: ref #1119

Status: owner authorized the complete lexical / collect_top / scalar MAX direction.
Implementation and qualification remain in progress. This document is not a
performance, runtime acceptance, or complete #1119 delivery claim.

## Delivery sequence and gates

1. Complete lexical domain. Reuse the pinned resident/immutable text projection,
   shared BM25 scorer and original query accounts. Add an explicit exhausted-domain
   receipt and refuse complete requests in window-only providers. Enter after
   confirming source/scorer ownership; exit with resident/persisted differential
   scores, zero/boundary/refusal tests and cancellation. Keep existing windows.
2. Grouped query execution. Add opt-in complete/maxCandidates and collect_top,
   scalar MAX binding, physical/report evidence and bounded per-group state. Enter
   only after the producer can attest exhaustion. Exit with engine/reference parity
   for skew, NULL keys, duplicates, ties, permutations, eligibility and mutations.
   A final LIMIT never limits producer coverage or discards an unfinalized group.
3. Qualification. Exercise old/new query behavior, allocation-before-use and retained
   snapshot replacement, then measure size/skew/group/evidence/eligibility matrices
   with frozen source and raw receipts. Run required lint, CI and local fuzz. Enter
   on a frozen integrated tree; exit only with explicit correctness evidence and
   measured resource/latency results, including unfavorable observations.

Graph-aware eligibility and composite ordered seeks remain required subsequent
slices of #1119. The existing publication protocol owners remain authoritative.
No new selector, persistent format, route-specific API or production activation.

## Complete lexical contract

CALL text_search($text, complete := true, maxCandidates := $candidate_cap) emits
every visible positive-BM25 match in one retained projection domain, using the full
live corpus statistics before metadata/canonical filtering. Both options are
required for complete mode; ordinary topK/limit are mutually exclusive with it.
maxCandidates is a nonnegative admission ceiling before canonical graph filtering.
Zero succeeds only if the searched domain is empty. A larger ceiling is not an
instruction to allocate that capacity. Exceeding a row, memory or work allowance
fails explicitly; no retained prefix becomes a successful complete output.

Complete mode disables document Top-K pruning. Providers explicitly attest
exhaustion; cardinality less than the ceiling is not itself proof. Unsupported
providers refuse at their boundary. LIMIT 0 may skip execution and must not report
exhaustion. A semantic intermediate LIMIT retains its ordinary meaning.

## Grouped evidence contract

collect_top(document_id, score, M) retains at most M distinct non-null String
producer document IDs per group, using each ID's maximum finite Float score.
M is positive and bound before execution. Evidence order is score descending,
then ID ascending, using total_cmp for Float comparisons including signed zero.
Groups rank by their maximum document score; equal groups use ordinary engine
key order. Scalar MAX preserves existing value ordering and NULL behavior.
The initial ranked aggregate stage accepts collect_top and MAX together; other
aggregate combinations explicitly refuse instead of falling back to unbounded
COLLECT buffering. Ordinary aggregation without collect_top retains its behavior.

Supported group keys for this operator are NULL, Bool, Int, String and Uuid;
missing keys join the NULL group. Other types explicitly refuse. All observed
groups retain a bounded evidence summary until input exhaustion. Losing groups
cannot be discarded early: later candidates can promote them. Charge keys,
summary insertion/replacement, output and any spill before allocation. The first
implementation may refuse when memory is exhausted instead of spilling; it may
never silently omit groups. Cancellation never returns a successful partial result.

## Evidence required

Use an exhaustive reference independently of the optimized state. Cover the issue's
A/B/C fixture, empty/fewer-than-K groups, skew, duplicate IDs/max scores, ties,
NULL/missing/unsupported keys, cancellation and exact resource boundaries. Compare
resident and immutable projections against full-corpus BM25, with multiple segments,
updates/tombstones and retained-view replacement. Ordinary window behavior is a
separate regression family. Use allocation probes to compare actual live/peak heap
against admitted bytes; ledger counters alone do not establish heap/RSS bounds.

Performance qualification records dataset size, match/eligibility selectivity,
number/skew of groups, evidence width, configuration, source and cache state.
Report visits, decoded bytes, peak admitted/actual memory, spill, CPU and latency.
Do not claim sublinear exhaustive enumeration or a numerical speedup from counters.

## Coverage and work reports

The owned execution profile exposes `text_seed_execution_reports` and
`grouped_evidence_reports`. EXPLAIN ANALYZE and the embedded query-report JSON
forward the same evidence. Operator IDs use physical-plan preorder ordinals.
A retained but unexecuted text operator reports `skipped`; a constant LIMIT 0
may remove the operator altogether. Neither case attests source exhaustion.
`complete` requires execution and explicit provider exhaustion. Candidate count
is measured before canonical eligibility and downstream LIMIT. Native resident
providers expose corpus/matching counts; unknown generation and I/O fields stay
absent. Immutable providers forward their pinned generation, source epoch and
lexical scoring I/O counters. Metadata routing/decoding I/O is not included in
those counters. These identities are diagnostics, not snapshot compatibility
or publication permission.

Grouped input exhaustion describes the declared semantic aggregate input, which
may itself contain an explicit intermediate LIMIT. It does not imply complete
lexical or graph coverage. Reports distinguish all groups considered, bounded
retained evidence and groups emitted by the aggregate before downstream sorting
and LIMIT. Peak state is admitted bytes, not measured physical memory. Initial
execution does not spill and reports zero spilled bytes.

## Reproducible scalar-stage benchmark

Build with `cargo bench --locked --bench grouped_evidence --no-run`, then invoke
the resulting executable with rows, group count, hot percentage, eligibility
stride, M, K, ID width and repetition count. For example:

```sh
cargo bench --locked --bench grouped_evidence -- 10000 100 90 1 8 10 32 7
```

Each run compares exact query output with streaming complete-ID maximum plus
full sorting. It alternates path order, discards one warmup per path and emits
raw JSON latency samples with group/state counts. This is an in-memory scalar
stage qualification; it excludes lexical I/O and graph traversal. Process peak
RSS is cumulative and must not be interpreted as incremental aggregate memory.
Separate allocation probes and end-to-end lexical/graph measurements remain
required. Preserve all samples, source identity and unfavorable observations;
passing the equality assertion alone is not performance acceptance.


The ranked aggregate normalizes its declared input through the existing read-plan
rules and required-property scan selection. It preserves outer aggregate aliases
so ORDER BY/LIMIT can keep bounded TopN selection after all groups are finalized.
Other multi-WITH statements normalize their whole plan. Simple repeated
empty or scalar-column group keys borrow the retained key for lookup instead of
allocating another owned copy. New groups and computed/compound keys retain the
original admitted key-preparation path. `borrowed_group_key_rows` records actual
reuse. Allocation probes exercise a repeated key wider than one MiB and retain
its original admission through the output callback.


Raw resident scoring uses canonical field token occurrence semantics: phrase
aliases deduplicate against preceding terms in that field, while ordinary
identifier repetition retains term frequency. This fixes a raw-versus-persisted
BM25 discrepancy for repeated phrases in both window and complete requests.
The deduplication table and retained token markers remain in the original query
working account. Query windows retain their admission/selection semantics; the
corrected raw scores can differ from earlier incorrect values.


## Native lexical and graph benchmark

`grouped_evidence_native` runs the public raw, resident or immutable lexical
producer through an admitted embedded external-provider adapter. The query
performs canonical one-hop graph eligibility before grouping. Compare complete
retrieval plus grouped evidence against a full-ID host maximum/sort reference
and a known-corpus full-window control. The control is complete only because
this fixture's corpus size is known; it does not attest arbitrary window requests.

```sh
cargo bench --locked --bench grouped_evidence_native -- disk /tmp/hawdb-native-case 1000 100 90 2 3 8 10 32 7
```

Arguments are backend, new artifact path, corpus size, group count, hot percentage,
lexical match stride, graph eligibility stride, M, K, ID width and repetitions.
The artifact path must not exist. Each path discards one warmup and alternates
execution order. JSON retains raw wall samples, producer work, query admission
and grouping reports. Measure whole-process CPU/RSS separately; setup, warmup
and all three paths share that process. This is a warm-cache embedded workload,
not a cold device-I/O or publication-concurrency qualification.

The immutable fixture retains a base layer, replaces its first ID, inserts later
IDs and deletes a tombstoned document. The resulting sparse delta bounds overlap
unchanged base IDs. Metadata resolution uses the existing query-admitted exact
membership route rather than trusting descriptor ID ranges. It decodes metadata
only for the resolved layer, retaining one admitted segment at a time. This fixes
both complete and ordinary window lookup for sparse overlapping artifacts without
retaining an additional corpus-wide ID map.
