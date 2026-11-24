# BM25 and checkpoint namespace measurements (#1952)

This is a fresh, source-bound local campaign after the temporary-directory
samples were lost. Earlier summaries are not acceptance evidence for this run.

## Environment and source

- macOS 27.2.1, arm64, Apple M5 Max, 18 logical CPUs, 36 GiB RAM; shared developer host.
- Rust 1.99.1, locked dependencies, default Search/Storage features.
- Repository release profile: opt-level 3, one codegen unit, thin LTO, no assertions or overflow checks; no target-specific flags.
- Baseline: `d9268486009171e9576768ac2bbfbe7eb875fb95` (main after #1944/#1948).
- Search candidate production source: `b49f12018b805da45bdb1ad7ced9dd75e0d8e72e`.
- Final Storage candidate: that revision plus the internal eight-file window
  recorded in `batch8.patch` and `batch8-source.json`. Search production source
  is byte-identical; the later documentation commit contains both components.
- The same two checked-in Rust example drivers run against both revisions.
  The later driver edit only spells the synthetic Chinese text with Unicode escapes;
  it changes no generated text, runtime parameters or production implementation.
- Baseline first release build: 322.492s; candidate first release build: 295.797s.
  Both used one Cargo job and completed before any timed comparison. Build costs
  are not execution speedup measurements or CI speedup claims.

`source.json`, `build.json`, `dataset-hashes.json`, fixture build reports and each
phase's `binaries.json` bind the source, parameters, data and frozen executable
hashes. No executable is replaced during measurement. Search results and RSS controls use the frozen `b49f12018` executables;
subsequent Storage-window experiments are identified separately below, with
their patch, source hashes and executable hashes. Search production files are
unchanged by those Storage experiments.

## Measurement protocol

`compare.py` freezes each executable, opens one long-lived reader per arm,
performs three warmup pairs per request, then alternates A/B and B/A calls.
Only the Rust library call is timed. Reader open, fixture creation, JSON I/O,
correctness verification, reopen and cleanup are outside the timer.
The complete untrimmed per-call samples are retained as gzip JSONL. Every call
must succeed and its semantic output must agree with the other arm.

`baseline_p50_ms` / `candidate_p50_ms` are separate medians. Paired median ratio
is the median of candidate/baseline ratios for adjacent equal-request calls;
its seeded 2,000-resample percentile bootstrap interval summarizes local noise,
not a cross-machine confidence guarantee. Empirical p95/p99 use nearest rank.
No samples are discarded, and no target-local compilation profile is used.

## Full BM25 queries

ScoresOnly, text mode, no query embedding, limit 10, rank window 100, default
fusion weights; metadata/offset controls are explicit in raw requests.
The independent baseline comparison checks every returned ID, final score bit,
text score bit, RRF score bit, text rank, total, truncation flag and document
count. No tokenizer cap, ANN parameter, scoring precision or tie policy changes.

### SciFact

All 5,183 corpus documents are sorted lexically by `_id` and split at 2,592 rows.
All 300 test query IDs are selected from the published test qrels, with no
relevance-based tuning. Each has three warmup pairs and nine measured pairs:
2,700 measured + 900 warmup pairs, all equal, zero refusals.

| Metric | Baseline | Candidate | Change |
| --- | ---: | ---: | ---: |
| p50 | 0.637334 ms | 0.430313 ms | -32.5% |
| p95 | 1.271167 ms | 0.799583 ms | -37.1% |
| p99 | 1.790750 ms | 1.136250 ms | -36.5% |
| Mean | 0.686848 ms | 0.455362 ms | -33.7% |
| Lexical-reader bytes across measured calls | 961,473,906 | 592,155,756 | -38.4% |

The existing byte counter includes lexical ordinal document lookup; it is not
physical disk traffic or body hydration. Equal ranked bits preserve the returned
ranking on these queries; this is not a new relevance benchmark against LanceDB.

Source: [BEIR SciFact](https://huggingface.co/datasets/BeIR/scifact) and
[test qrels](https://huggingface.co/datasets/BeIR/scifact-qrels). The raw sample
requests reproduce these public test-query texts, licensed CC-BY-SA-4.0; those
texts retain that license and attribution. The corpus is not redistributed.
Parquet files were downloaded from the public resolve URLs after the ZIP endpoint
failed TLS and the paged API returned HTTP 429. Conversion uses temporary PyArrow,
not a repository dependency. Sorted corpus and selected test-query hashes match
the prior downloaded corpus, independently of transport format.

### Synthetic controls

16,384 deterministic documents, four 4,096-document content segments; each document
has 32 dense words, eight bucketed sparse words and a unique word. Every seventeenth
has repeated database/storage/recovery and Chinese content; scope cycles over four
values. Raw word counts below differ from analyzer term counts.
Thirteen request shapes have 51 measured pairs each: 663 measured + 39 warmup
pairs, all equal. Single-term, dense, sparse, repetition, Chinese, missing terms,
filters and offsets remain visible rather than selecting only favorable cases.

| Cell | Baseline p50 ms | Candidate p50 ms | Baseline p95 ms | Candidate p95 ms | Paired median ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| sparse-1 | 1.137209 | 1.098083 | 1.212083 | 1.219667 | 0.968 |
| sparse-4 | 2.376375 | 1.792917 | 2.634125 | 1.932042 | 0.748 |
| sparse-8 | 3.943083 | 2.951417 | 4.902250 | 3.691792 | 0.760 |
| sparse-16 | 7.238292 | 5.439625 | 13.488208 | 7.455125 | 0.746 |
| dense-1 | 1.573541 | 1.393458 | 1.674000 | 1.456000 | 0.891 |
| dense-4 | 4.197625 | 3.667167 | 4.313459 | 3.774417 | 0.877 |
| dense-8 | 7.664875 | 6.911500 | 7.878625 | 7.124333 | 0.902 |
| dense-16 | 14.715458 | 13.377125 | 14.938459 | 13.536833 | 0.906 |
| repeated | 0.403292 | 0.376750 | 0.433958 | 0.410708 | 0.942 |
| chinese | 0.431041 | 0.392709 | 0.464292 | 0.434459 | 0.909 |
| filtered | 11.118666 | 10.756125 | 11.688708 | 11.114958 | 0.978 |
| absent | 0.031584 | 0.024709 | 0.043167 | 0.034000 | 0.775 |
| mixed | 3.073000 | 2.130334 | 3.746917 | 2.737500 | 0.696 |

All p50 cells improved. Single sparse-word empirical p95 was +0.6%, so this run
does not claim every latency quantile improves. Its paired median interval and
complete samples allow inspection. Larger multi-term improvements repeated in
the earlier exploratory campaigns, whose lost samples are not included here.

Optional posting retention uses at most 2 MiB, one eighth of working admission,
and one eighth of the reader workspace. Runtime-accounted reads charge entries
and release them on pressure; ordinary ScoresOnly latency measurements do not measure admitted peak.
Three separate Darwin `/usr/bin/time -l` controls each run all 300 SciFact
queries per arm, including process startup and reader open. Median process
maximum RSS is 13.953125 MiB baseline versus 14.343750 MiB candidate:
+0.390625 MiB (+2.8%). This is a process-lifetime observation, not a per-query
ledger charge or a guaranteed RSS bound. All three controls also compare complete
semantic outputs. `rss-summary.json` and the six `rss-*-arm-*.txt` files retain
the source counters. The first sandbox-denied resource-stat attempt is not a
passing control; the three recorded runs used authorized native statistics. Search CI independently owns admission/refusal,
cancellation and cache-integrity controls. Concurrent throughput, cold-storage
latency, admitted hydration, ANN, TPC-C and Mem production acceptance are not
measured by this campaign.

## Directory-barrier controls

The existing Storage CI unit owner covers the counter and COW controls.
The original 64-file implementation passed that owner; the eight-file boundary
update must pass it again at the final source revision.
These count only the final object-directory and staging-directory barriers,
not file-content fsyncs, namespace ancestry establishment, WAL or head barriers.
Each fixture below uses one object kind, with eight KiB per producer file.

| Files | New objects, separate -> batch | Reused objects, separate -> batch |
| ---: | ---: | ---: |
| 8 | 16 -> 2 | 8 -> 1 |
| 64 | 128 -> 16 | 64 -> 8 |

The same owner checks 8/9/17/64/65/129-file boundaries, full bytes and later
producer writes. The final window is eight files; these are asserted operation
counts, not a measured wall-time speedup. Mixed object kinds can require more than one destination directory;
the implementation synchronizes each distinct required directory per bounded
window, retains all file-content fsyncs and finishes before head publication.
The manual ignored publication latency profile was not rerun in this campaign;
no component latency claim is inferred from these asserted counts.

## Branch checkpoint measurements

Every call uses a fresh admitted branch under `SyncOnEveryWrite`, two bootstrap
nodes and 0/1/128/2,000 additional `Document` nodes, each with an integer ID and a
512-byte body. `prior=True` first checkpoints that state, appends one node, then
times the second complete checkpoint. Every node is compared before/after and
again after close/re-admission. Setup, inserts/commits, verification and cleanup
are outside the timer. Ordinary GraphStore opens do not enter sealed branch
publication; this campaign uses the affected admitted-branch path.

### Initial 64-file window: exploratory, not final tail acceptance

Eight cases have 51 measured + 3 warmup pairs each (408 + 24 pairs). All succeeded
and every before/after/reopen comparison agreed. Complete distributions and raw
samples remain in `checkpoint-*`; they are not replaced by favorable repeats.
With 51 samples, nearest-rank p99 is the maximum, so it is particularly noisy.

| Additional nodes | Prior checkpoint | Baseline p50 ms | Candidate p50 ms | Baseline p95 ms | Candidate p95 ms | Baseline p99 ms | Candidate p99 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | False | 449.370 | 366.336 | 736.408 | 540.493 | 920.580 | 589.944 |
| 0 | True | 489.082 | 393.270 | 632.252 | 497.968 | 915.869 | 714.829 |
| 1 | False | 487.132 | 407.767 | 755.969 | 553.922 | 1047.839 | 736.856 |
| 1 | True | 470.389 | 389.855 | 531.231 | 525.077 | 575.359 | 718.364 |
| 128 | False | 437.946 | 366.105 | 670.260 | 515.447 | 746.808 | 658.338 |
| 128 | True | 466.128 | 408.067 | 534.018 | 631.999 | 682.247 | 818.232 |
| 2000 | False | 477.527 | 395.366 | 770.192 | 729.174 | 879.808 | 992.397 |
| 2000 | True | 478.217 | 396.823 | 568.782 | 585.545 | 786.835 | 1364.041 |

The 64-file window improves every median, but initial p95 increases for the
128- and 2,000-node second-checkpoint cases. Both were repeated separately with
101 measured + 3 warmup pairs, using the same frozen executables and parameters:

| Additional nodes, prior=True | Baseline p50 ms | Candidate p50 ms | Baseline p95 ms | Candidate p95 ms | Baseline p99 ms | Candidate p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 128 | 491.142 | 410.209 | 973.209 | 720.847 | 1072.609 | 893.130 |
| 2000 | 487.720 | 405.648 | 815.706 | 699.626 | 1132.780 | 1378.813 |

The repeated p95 values improve, but the 2,000-node p99 remains adverse. Those
results motivated testing an eight-file window instead of accepting the larger
burst from its median alone. See `checkpoint-repeat-*` and
`checkpoint-repeat-large-*` for both retained repeats.

### Final eight-file window

The final production change bounds each window to eight files. The two adverse
second-checkpoint shapes each have 101 measured + 3 warmup pairs; the empty additional-node
control has 51 measured + 3 warmup pairs. There are 253 measured + 9
warmup pairs in total. The remaining five original matrix shapes were not
remeasured with the final window after unrelated host activity became visible;
the complete initial 64-file matrix remains above. All succeed and preserve complete before/after/reopen
node contents. `batch8.patch` records the sole production change since the
Search candidate; `batch8-source.json` and per-case binary hashes bind this run.

| Additional nodes | Prior checkpoint | Pairs | Baseline p50 ms | Candidate p50 ms | Baseline p95 ms | Candidate p95 ms | Baseline p99 ms | Candidate p99 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | False | 51 | 742.096 | 617.002 | 1334.773 | 1321.614 | 1414.497 | 1771.118 |
| 128 | True | 101 | 973.846 | 889.829 | 1467.003 | 1325.003 | 1515.776 | 1830.407 |
| 2000 | True | 101 | 489.615 | 413.744 | 852.732 | 599.615 | 969.764 | 769.726 |

The 128-node final-window p99 is 1,515.776 -> 1,830.407 ms (+20.8%);
the zero-additional-node p99 is 1,414.497 -> 1,771.118 ms (+25.2%). These
observations are adverse and are not hidden by the median improvements. Near
the end of that run, native host inspection found unrelated libtest processes,
about 9.6 GiB compressed memory and less than 1 GiB unused physical memory.
This is evidence of shared-host interference, not proof that the implementation
cannot regress. Quiescent-host p99 non-regression remains unqualified; review
must not treat these observations as an unconditional performance acceptance.

These are local warm-filesystem checkpoint observations, not synchronized
transaction throughput, TPC-C, import, schema-registry startup or a device
power-loss qualification. Initial larger-window results remain above, including
all adverse tails; later repeats do not erase them. Filesystem tail quantiles on
this shared host are less stable than medians; no universal tail bound or
cross-platform performance guarantee is inferred.

## Replay

Copy the same Rust examples into a source checkout of the baseline, then build
each revision from its own repository root:

```sh
uv run --with pyarrow python docs/benchmarks/pr1952/fetch_scifact.py NEW_DATA_DIRECTORY
cargo build --locked -p hawdb-search -p hawdb-storage --release --examples -j 1
target/release/examples/bm25_query_profile build NEW_SYNTHETIC_ROOT 16384 4096
target/release/examples/bm25_query_profile import NEW_SCIFACT_ROOT SORTED_CORPUS_JSONL 2592
python3 docs/benchmarks/pr1952/compare.py search BASELINE_BM25 CANDIDATE_BM25 SYNTHETIC_ROOT NEW_OUTPUT --repetitions 51
python3 docs/benchmarks/pr1952/compare.py scifact BASELINE_BM25 CANDIDATE_BM25 SCIFACT_ROOT NEW_OUTPUT --queries TEST_QUERIES_JSON --repetitions 9
python3 docs/benchmarks/pr1952/compare.py checkpoint BASELINE_CHECKPOINT CANDIDATE_CHECKPOINT NEW_PARENT NEW_OUTPUT --repetitions 51
python3 docs/benchmarks/pr1952/compare.py checkpoint BASELINE_CHECKPOINT CANDIDATE_CHECKPOINT NEW_PARENT NEW_REPEAT_OUTPUT --repetitions 101 --case nodes2000-priorTrue
python3 docs/benchmarks/pr1952/rss.py BASELINE_BM25 CANDIDATE_BM25 SCIFACT_ROOT TEST_QUERIES_JSON NEW_RSS_OUTPUT
```

`fetch_scifact.py` pins upstream dataset commits and refuses any raw or converted
hash mismatch. PyArrow is a temporary manual-tool dependency only.

Replace uppercase binary and data placeholders with absolute paths. Use a new
output/fixture directory for every run. Do not run builds or other
local test campaigns concurrently with measurement. This is manual evidence,
not an added CI benchmark lane, target-specific profile or timeout increase.
