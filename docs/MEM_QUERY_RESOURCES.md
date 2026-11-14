# Mem query resource qualification

This manual benchmark accompanies #1287. It covers the embedded query phases
used by Mem candidate ranking and graph expansion. The byte-budget follow-up
below supersedes the earlier entry-only logical-template retention policy.
Historical measurements retain their original source and executable identities.
It does not measure a whole
Mem endpoint, embedding provider, durable storage, or a production cutover.

## Reproduce a cell

Use the pinned toolchain and one fresh process per cell:

```sh
cargo bench --locked --no-default-features --bench mem_query_resources --no-run
# Run the executable Cargo printed:
<benchmark-executable> 100 8192 16384 20 0 ranking
<benchmark-executable> 100 8192 16384 20 0 expand
<benchmark-executable> 100 8192 16384 20 0 mem-ranking
```

Arguments are candidate count, rows per Space, body bytes, repeats, background
writer (0/1), and workload. The fixture has two equally sized Spaces. Ranking
changes an input score between samples while preserving its parameter shape.
Expansion retains a source record while producing one result per neighbor.
`mem-ranking` adds Mem's actual scoped-read predicates, canonical usage columns,
floating point inputs and nullable confidence. It excludes the unchanged host
scorer's decay/authority callback.

Build a separate executable with `--features qualification` to collect Rust
allocation requests and query preparation counters. Instrumented timings must
not be substituted for ordinary release timings. The counting allocator covers
all Rust threads and requested bytes; it does not count foreign heaps,
allocator rounding, or prove physical memcpy traffic.

Each cell prewarms the query, checks the complete candidate ID cohort, and
records raw latency samples, nearest-rank p95, process CPU, process peak RSS,
query-account peaks, scan candidates, intermediate/output payload, and CASE
dispatch counts. CPU includes the background writer when selected. Completed
background writes are reported because faster foreground completion changes
how much work that saturating writer can perform. Compare foreground-only CPU
for equal-work query costs. RSS includes fixture setup and is a process high
water mark, not an isolated per-query peak.

The ordinary query memory ledger continues charging retained graph payload
conservatively even when immutable records are shared. Reduced allocator
traffic therefore does not imply that every ledger peak decreases.

## Retained exploratory evidence

The [manifest](benchmarks/mem_query_resources_2026_10_28/manifest.json),
[combined matrix](benchmarks/mem_query_resources_2026_10_28/matrix.csv), and
compressed raw release/allocation JSONL files in the same directory retain 96
release cells plus 96 instrumented cells. These cover 20/100/512 candidates,
512/8192 rows per Space, 64/16384-byte bodies, and foreground/background modes.
The frozen v2 harness and per-cell executable hashes preserve the measured
protocol. That exploratory matrix does not contain `mem-ranking`; the separately
qualified matched-main canonical matrix is described below.

These measurements used a shared development workstation; some cells
coincided with compilation. They are exploratory, not a final-head performance
acceptance gate. Representative foreground cells at 8192 rows per Space and
16384-byte bodies are below. Allocation traffic comes from the separate
instrumented run; latency comes from release.

| Phase | Candidates | Baseline p95 ms | Candidate p95 ms | Baseline requested bytes/read | Candidate requested bytes/read |
|---|---:|---:|---:|---:|---:|
| Ranking | 20 | 3.429 | 0.499 | 16,845,446 | 2,441,777 |
| Ranking | 100 | 72.993 | 2.514 | 291,593,407 | 12,005,034 |
| Ranking | 512 | 2110.107 | 15.647 | 6,043,998,754 | 63,006,945 |
| Expansion | 20 | 0.855 | 1.086 | 952,768 | 540,370 |
| Expansion | 100 | 0.780 | 0.741 | 4,695,651 | 2,525,013 |
| Expansion | 512 | 2.166 | 1.904 | 29,138,458 | 15,621,468 |

The small expansion cohort illustrates why allocation savings alone do not
justify a universal latency claim. Mem's native collect/list and complete
search-router measurements remain separate qualification obligations.

The executor constructor test
`scan::binding_tests::expansion_inherits_graph_payload_without_copying_and_detaches_mutations`
audits actual inherited node/relationship record identity at each fanout. It
covers the same three cohort sizes and two payload sizes, then verifies that
mutating one result detaches its records. Its copied inherited payload count
excludes target decoding, scalar value copies, slot-array allocation, and final
result serialization. A production deep-copy control must make that test fail;
a successful restored run proves the guard is active.

## Actual scoped Mem ranking shape

The canonical harness also includes the actual scoped Mem ranking join. Its
boolean scope comparisons exposed a fallback missed by the simplified shape:
markers folded out of boolean comparisons must retain their exact boolean
bindings, rather than removing a referenced parameter. The complete rebound
plan remains the proof boundary. The scoped-CASE regression test permits
shape-compatible score changes but refuses changed, missing or wrongly typed
scope booleans; compatible template reuse makes zero internal planning calls.

Matched main is `0a214f761f` (HawDB 0.18); the measured engine source is
`22cee11bcb` after the complete partial-folding guard repair.
The [canonical manifest](benchmarks/mem_query_resources_2026_10_28/canonical-manifest.json),
frozen canonical harness and raw preparation JSONL retain six instrumented
cells: 20/100/512 candidates, 512 rows per Space, 64-byte bodies, no background
writer, three measured requests after warmup. These instrumented windows ran separately from release timings after this
task finished compilation and tests. Instrumented timing is not release latency
evidence. Allocation requests include harness/profile bookkeeping as well as
the query; they exclude foreign heaps and allocator rounding.

| Candidates | Baseline requested bytes/read | Candidate requested bytes/read | Baseline allocation calls/read | Candidate allocation calls/read |
|---|---:|---:|---:|---:|
| 20 | 19,128,910 | 2,783,735 | 92,604 | 13,749 |
| 100 | 301,751,014 | 13,115,895 | 1,314,047 | 61,776 |
| 512 | 6,093,997,303 | 66,444,690 | 29,436,912 | 298,396 |

Lower allocation traffic does not imply lower peak memory. At 512 candidates,
process peak requested Rust bytes rise from 11.3 MB to 47.7 MB, including fixture
setup and initial template admission. Live requested bytes immediately before
the measured window rise from 2.50 MB to 7.74 MB, reflecting retained templates.
Both windows retain the same additional 2595 bytes after collecting three
samples. The cache has the configured entry bound, but its additional retained
state and cold preparation peak are real costs.

Preparation metric `cypher_plan_calls` counts outer phase invocations, not every
internal logical-planning call; its delta is three for both variants. The
zero-internal-planning reuse claim comes from the targeted template unit test,
not from relabeling those phase counters. The [canonical release matrix](benchmarks/mem_query_resources_2026_10_28/canonical-matrix.csv)
and compressed raw canonical release JSONL contain all 48 paired cells:
20/100/512 × Space512/8192 × body64/16384 × foreground/background × two variants,
20 requests per cell, zero errors. Each optimized sample reports five CASE
tables and `5 * candidates` lookups. Every background cell completed writes
(range14–63294); foreground duration changes the writer's completed work.

The benchmark uses ConcurrentDatabase, warms shared logical binding/catalog
state, and opens a fresh read transaction per sample. Physical caches are
private to those read forks (`DatabaseReadSnapshot::fork`), so this is not a
persistent physical-cache hit-latency measurement. The separate streaming
facade regression uses one read owner and checks physical-cache correctness.
Both variants use max_plan_cache_entries=16 and one query shape per process;
cache saturation and shape churn are outside the measured protocol.

Foreground reference at Space8192/body16384:

| Candidates | Baseline p95 ms | Candidate p95 ms | Baseline CPU ms/read | Candidate CPU ms/read | Baseline RSS MiB | Candidate RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| 20 | 94.710 | 0.893 | 5.689 | 0.662 | 581.3 | 577.2 |
| 100 | 92.527 | 3.583 | 84.523 | 3.092 | 583.6 | 588.5 |
| 512 | 2209.826 | 20.160 | 2137.340 | 19.626 | 599.2 | 638.9 |

The 20-row baseline's slow tail is disproportionate to its process CPU cost;
the cause was not traced. Keep the raw observations, and do not interpret its
p95 ratio as an isolated speedup. CPU and allocation work provide independent
context. The 512-row reference reduces CPU markedly but increases RSS.

Dispatch table lookups are measured work, not total expression evaluations or
binary-search comparisons. For this complete unique-key fixture the reference
linear evaluator's branch-key evaluations derive to `5*N*(N+1)/2`; that formula
is an analytical count, not an instrumented baseline counter. No unmeasured
expression-comparison reduction is claimed.

Before the partial-folding repair, the actual streaming regression returned
`[0]` instead of `[0,1]` when a parameter was shared between a folded condition
and simple CASE. AST-derived exact equality guards repair that domain;
nullness remains protected by complete type shapes. Current validation passes
31 plan-cache tests,515 executor tests (16 intentionally ignored),2 facade
regressions and all100 focused/local fuzz targets. The frozen canonical harness
matches the measured source; its old warmup comment is superseded by the cache
lifetime clarification above.

## Review follow-up: shape churn and memory accounting

The binding cache now selects a bucket by normalized query text and a recursive
parameter-shape fingerprint. List lengths, element types, NULL positions, map
keys and vector dimensions participate; scalar payloads do not. A fingerprint
is only a selector: the template still validates every shape, exact folded
dependency and reserved-marker boundary before reuse. Hash collisions therefore
cause conservative replanning rather than accepting an invalid template.
Different shapes consume the same global LFU entry allowance; there is no
separate unbounded map per query. Same-shape changes to an exact folded guard
still conservatively replace that bucket. An explicitly unbounded host cache
configuration remains unbounded, as with the physical cache.

The fixed-text `IN $ids` regression alternates lengths 20/17 and a NULL mask:
three admissions/misses followed by 69 hits, without eviction at capacity 3.
The four-thread churn regression exceeds capacity 8, checks every returned plan
against fresh parameterization, observes eviction and verifies at most 8 retained
entries. LFU can immediately evict a new cold entry among frequent incumbents;
correctness never depends on admission making the next request a hit. Shape
validation now traverses borrowed values without allocating a comparison tree.
The proven logical tree and validity key now use shared immutable ownership;
cache admission and successful reuse clone handles instead of either tree.
The regression checks shared ownership as well as actual rebound results.
Request-owned physical planning and binding remain allocation costs.

The `mixed-ranking` manual workload holds the Mem ranking query text and its
CASE branches fixed while alternating the candidate-ID list between N and N-3.
It primes both shapes and validates complete returned IDs for every request.
This models variable candidate filtering independently of the separate fact
that changing the number of generated CASE branches changes Mem's query text.
Physical caches are still private to each read fork.

The profiling allocator for this harness has a separate quiescent-reset window
peak, leaving the original process high-water counter intact. Its first cold
query, warm measurement window and isolated logical-template probe have distinct
observations. The probe separately measures marker-planning peak, the heap
payload retained by one template, and transient allocation from one reuse handle.
It excludes normalized-query keys, LFU storage, the outer cached-template Arc
header and physical plans. Shared logical-tree and validity-key Arc allocations
are included. This is the meaning of the raw probe's abbreviated scope label. Multiplying its
representative template payload by 128 is an extrapolation for 128 equally sized
entries, not a universal byte limit: query length and exact parameter payloads
can be larger. Entry-count boundedness must not be presented as byte boundedness.

For 512 branches across five CASE expressions, the dispatch vectors reserve
5*512*size_of::<(&str,usize,&ProjectionExpression)>() plus five table descriptors
(82,160 bytes on this 64-bit target). Keys and result expressions are borrowed.
This layout accounting is not an allocator ablation. Graph-slot Arc/slot state
is included in the measured query window but is not isolated from the remaining
executor allocations. The existing constructor audit establishes zero inherited
record-payload copies, rather than zero allocation or zero slot overhead.

Mem's actual browse page is 20 rows in both the
[main Memories tab](https://github.com/nowledge-co/mem/blob/7e3f5ba8d44c648cd9ecac26b978f9f96bb38781/nowledge-graph/src/components/app-shell.tsx#L974)
and [advanced browser](https://github.com/nowledge-co/mem/blob/7e3f5ba8d44c648cd9ecac26b978f9f96bb38781/nowledge-graph/src/components/advanced-search.tsx#L326).
The [CLI list default](https://github.com/nowledge-co/mem/blob/7e3f5ba8d44c648cd9ecac26b978f9f96bb38781/nmem-rs/crates/nmem-cli/src/cli.rs#L3787)
and [CLI search default](https://github.com/nowledge-co/mem/blob/7e3f5ba8d44c648cd9ecac26b978f9f96bb38781/nmem-rs/crates/nmem-cli/src/cli.rs#L3632)
are 10. The generic useMemories hook defaults to 50, overridden by the main tab.
Thus 512 is a stress cohort rather than a default UI or CLI page; candidate
ranking size and final response page size also need not match. The Mem list
benchmark's 512-row hydration tradeoff must not be extrapolated to its usual
20-row UI request. Mem PR #8687 is merged as the cited commit; that does not
upgrade the containing Mem artifact to this unreleased HawDB implementation.

### Final review measurements

The [review manifest](benchmarks/mem_query_resources_2026_10_28/review-manifest.json),
[CSV](benchmarks/mem_query_resources_2026_10_28/review-matrix.csv), compressed raw
JSONL, frozen harness/allocator and engine patches preserve both review variants.
The `deepclone` control already has the multi-shape cache; `shared` additionally
shares immutable logical/validity state. Its reconstructed control sources
match the hashes recorded before measurement. Each variant has 30 ordinary
release cells (24 Mem ranking matrix cells plus 6 alternating-length cells),
20 requests per cell, and 6 separate instrumented cells with 3 requests each.
All completed successfully. These are separate shared-host windows; further
CPU/latency differences between variants are descriptive, not randomized paired
causal estimates. The earlier matched-main baseline remains historical evidence.

Final shared-template instrumentation, Space512/body64/foreground, bytes:

| Candidates | First cold-query peak | Warm-window peak | Template heap payload | 128 same-sized template payloads | Reuse-handle transient heap |
|---|---:|---:|---:|---:|---:|
| 20 | 3,722,465 | 3,725,944 | 226,845 | 29,036,160 | 0 |
| 100 | 10,220,154 | 10,227,393 | 1,034,445 | 132,408,960 | 0 |
| 512 | 42,605,708 | 42,632,311 | 5,049,337 | 646,315,136 | 0 |

This corrects a cold-only interpretation of the original 47.7 MB peak: the
review control's 512-row **warm** peak was 47,676,592 bytes. Sharing eliminates
5,046,385 bytes of transient template cloning at that size and lowers the warm
peak to 42,632,311. Warm live bytes before/after collecting three samples are
7,741,449/7,768,108; the difference includes harness and query/cache activity,
and is not an isolated leak measurement. The isolated template probe returns
to its starting live count after both handles are dropped. Its marker-planning
peak above start is 17,812,060 bytes. Request-owned physical planning/binding and
executor/harness work remain in the warm peak; their residual is not separately
attributed to graph slots. The five dispatch tables' 82,160-byte layout alone
cannot explain the multi-megabyte peak. Shared ownership retains original Vec
capacities, so the smaller templates can retain slightly more heap than a
compacting deep clone, despite avoiding its per-request copying.

Final requested allocator bytes/read for 20/100/512 are 2,576,763/12,123,057/
61,389,910, with 12,635/56,531/271,864 allocation calls/read. These remain distinct
from process RSS and instrumented query-ledger accounting.

With both candidate-list shapes resident, warm live bytes are 2,448,732/
4,203,093/12,978,761 and warm peaks are 3,959,140/11,296,477/47,869,623 for
20/100/512. The deep-clone control's 512-row mixed peak was 52,911,800. The cache
unit's 69 hits/3 misses establishes alternating-shape reuse; this mixed workload
supplies complete execution and resource evidence, without relabeling fresh
physical-cache misses as logical-cache misses or claiming a measured production
hit rate. The unit churn/concurrency check proves an entry-count bound for its
fixed request domain, not a universal heap-byte bound.

Final release reference, Space8192/body16384/foreground:

| Candidates | Fixed-shape p95 ms | Fixed-shape CPU ms/read | Fixed-shape RSS MiB | Mixed-length p95 ms | Mixed-length CPU ms/read | Mixed-length RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| 20 | 0.599 | 0.546 | 581.8 | 0.529 | 0.514 | 582.1 |
| 100 | 2.843 | 2.639 | 584.8 | 2.689 | 2.568 | 587.9 |
| 512 | 19.163 | 17.979 | 627.5 | 18.356 | 17.890 | 626.4 |

Historical decision at `83d43b0f8e` (superseded for logical-template retention
by the byte-budget follow-up below): retain the CPU/allocation optimization with the peak
and retained-template costs explicitly qualified. It is not a memory-first
recommendation or a claim that all resource metrics improve. A 128-entry default
is not a byte budget: 646 MB above is only 128 equally sized measured 512-row
templates, excludes outer cache storage, and is not an absolute worst case.
Hosts with tight memory budgets must evaluate a smaller typed
`max_plan_cache_entries` setting (the measured host uses 16); `Some(0)` disables
admission and loses reuse. No host production-memory acceptance or HawDB
activation is established here. The broader replacement issue remains open.

### Merged Mem reference-host qualification

[Reference-host receipt](benchmarks/mem_query_resources_2026_10_28/review-mem-reference-host.json)
records merged Mem 7e3f5ba8d44c648cd9ecac26b978f9f96bb38781 against engine 83d43b0f8e1373a5626b9ae30d6f720a05cc79d7:
20 passed, 0 failed, 0 ignored, 60 filtered out. The Bazel source override resolves
to the engine checkout and its template hash matches the measured source.
The actual Rust reader exercises exact identities/raw Spaces/temporal values,
complete bounded labels/JSON, structured budgets and HTTP/MCP refusal causes,
legacy authority, closed dispatch and shadow/dual-read mismatch behavior.
This is the reference-reader suite, not all 80 integration tests or production
containing-artifact/activation acceptance. The Mem dependency pin is unchanged.

The first attempt timed out before compilation in isolated Cargo dependency
fetch. The successful retry reused the existing build cache with
`CARGO_BAZEL_ISOLATED=false` and `CARGO_NET_OFFLINE=true`, explicitly passed to
Bazel's repository environment. Dependency pins and runtime configuration were
unchanged; generated local module-lock metadata was inspected and restored.

The benchmark profiles materialized output while the caller still owns it.
A positive `query_completion_bytes` therefore includes retained result ownership;
it must not be interpreted as a leaked streaming lease. The facade's existing
owned-versus-streamed regression explicitly checks positive owned completion
and zero streaming completion. All final release samples stay within their
reported query peak and expose five CASE tables/five lookups per returned row;
all background cells complete writes (21–584 per measured window).


## Byte-bounded logical-template retention

`DatabaseConfig::max_parameter_binding_cache_bytes` defaults to 16 MiB; zero
suppresses logical-template admission. The existing entry bound still applies.
The logical cache uses weighted LFU admission with a conservative requested-heap
charge for both Arc-owned plan/key trees, the outer template Arc, strings and
list capacities, B-tree nodes, exact parameter payloads, normalized-query keys
and spare hash-table storage. Shared state is charged independently per cache
entry. Unknown plan/expression representations or arithmetic overflow decline
cache admission and preserve normal planning/execution semantics.

Hash-table storage uses a conservative capacity high-water bound because
removal tombstones may reduce `HashMap::capacity()` without releasing the
underlying buffer. Equal-key replacement retains the incoming key covered by
its charge. Empty-cache eviction releases the bucket allocation. These native
layout bounds depend on the pinned toolchain and need requalification when it
changes. Count-only LFU users do not supply key/value heap charges; the new
logical-template budget always uses weighted admission.

This bounds quiescent logical-cache retention, not total process memory.
Allocator rounding, fixed cache metadata, request-owned plans and clones held
by in-flight readers are outside the cap. Existing physical-plan and relational
cache retention also remain separate. New parameterized CASE physical trees
specifically bypass that count-only physical cache and remain request-owned.
The report exposes `parameterized_case_template` as the physical bypass reason;
`parameter binding cache hit: proven logical template` still reports logical
reuse. Ordinary cacheable statements keep their existing physical-cache policy.
Persistent handles re-optimize these CASE physical plans for each request.
The Mem benchmark already uses fresh private physical caches; it does not
qualify the cost for long-lived physical-cache hits. CASE slot classification
is revalidated after restoring all exact guards, for
both grouped and individual-parameter fallback proofs. Mem read forks share the
bounded logical cache and retain their private physical-cache ownership. No production-memory, released-dependency
or activation acceptance follows from this cap.

Regressions exercise zero/tiny caps, oversized/overflowing admissions, changing
key capacities, concurrent 512+ element shapes, and eight distinct 512-candidate
five-CASE templates against the default cap. The manual allocation harness
checks each template's charge against its measured retained requested heap. A
separate 4096-insertion weighted-cache probe checks, after every admission, that
actual retained requested bytes are no larger than the charge and the charge
is no larger than 64 KiB. These retention checks exclude transient admission and
query-preparation peaks; those remain separately recorded.


An independent allocator regression,
`//crates/plan-cache:hawdb_template_retention_tests`, checks real retained
requested bytes for 20/100/512 five-CASE templates, a large exact map/list/binary/
vector payload, and 4096 weighted-cache admissions. Allocations are counted on
the test thread independently of the cache estimator; each owner drop must
release the full tracked payload. It passes with the repository Rust 1.97.1 and
the Mem host Rust 1.98.1. This bounded test joins the existing crate test surface without adding a CI job.

Two production controls protect the retention boundaries: disabling byte
pressure must fail the burst's original byte-cap assertion; permitting CASE
physical admission must fail the property/column query test's zero-retained-
physical-entries assertion. Both controls restore byte-identical production
source and rerun the same assertion GREEN. The physical ownership regression
also checks the complete output, dispatch tables/lookups, logical reuse trace,
and explicit physical-bypass reason.


### Final cap and ownership qualification

The [final manifest](benchmarks/mem_query_resources_2026_10_29/manifest.json)
retains 30 ordinary release cells, 6 instrumented cells and 48 interleaved
release comparisons against the already-optimized `83d43b0f8e` binary. Frozen
source, binary hashes, a reversible source patch, raw samples, both production
controls and the merged-Mem reference-host receipt accompany the results.
Earlier measurements above retain their historical source identities.

| Candidates | Actual template heap bytes | Conservative template charge bytes |
|---|---:|---:|
| 20 | 226,845 | 253,141 |
| 100 | 1,034,445 | 1,166,437 |
| 512 | 5,049,337 | 5,718,017 |

The separate 4096-insertion probe observes at most 58,312 retained requested
bytes and 65,384 charged bytes under its 65,536-byte cap. Exact large map/list/
binary/vector payloads also pass the independent allocator oracle. Each owner
drop releases all tracked heap. The default 16 MiB logical cap includes keys and
LFU storage as well as these template charges; three representative 512-row
templates cannot fit, while the two measured mixed-length shapes do fit.

The final 30-cell matrix covers the full fixed-shape grid and the large-Space /
large-body mixed-length foreground/background grid. In the fixed 512-row,
8192-row Space, 16 KiB-body foreground cell, p95 is 17.264 ms, CPU 17.163 ms/read,
and process peak RSS 628.0 MiB. Its instrumented counterpart allocates 60,747,571
bytes and 268,172 calls/read, versus 61,389,910 bytes and 271,864 calls at 83.
Warm requested-heap peak is 42,019,874 versus 42,632,311 bytes. This does not
eliminate the larger transient peak relative to the original pre-optimization
main baseline.

To check shared-host timing variation, four alternating-order fresh-process
pairs run per candidate count/workload at 8192 rows per Space / 16 KiB bodies /
foreground. The table pools all 80 latency samples per variant, averages CPU
over those 80 requests, and takes the median of four process RSS high-water
observations. This comparison isolates the retention follow-up against 83;
it is separate from the historical pre-optimization main comparison.

| Candidates / shape | 83 p95 ms | Final p95 ms | 83 CPU ms/read | Final CPU ms/read | 83 RSS MiB | Final RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| 20 / fixed | 0.645 | 0.611 | 0.556 | 0.543 | 581.0 | 577.5 |
| 20 / mixed | 0.581 | 0.585 | 0.535 | 0.528 | 581.2 | 577.9 |
| 100 / fixed | 2.670 | 2.574 | 2.555 | 2.458 | 584.7 | 583.1 |
| 100 / mixed | 2.719 | 2.588 | 2.566 | 2.463 | 585.2 | 586.3 |
| 512 / fixed | 18.905 | 18.569 | 17.492 | 17.117 | 624.4 | 621.1 |
| 512 / mixed | 18.852 | 18.129 | 17.596 | 16.959 | 630.0 | 624.0 |

All raw windows are retained. The small mixed 20 p95 and mixed 100 RSS do not
improve; these shared-workstation results do not establish a universal latency
or RSS reduction. The retention guarantee concerns the named logical-cache
requested-heap budget and the absence of cached CASE marker physical trees.

Final local verification: 39 plan-cache unit tests, 58 root cache tests, 3 ordinary
scoring-cache tests, 2 CASE property/column facade tests, allocator coverage on
Rust 1.97.1 and 1.98.1, 100 native Bazel fuzz/focused targets, and strict native /
minimal WASM Clippy all pass. Staged license/format/Clippy hooks pass.
`prek run --all-files` still stops at 626 unchanged baseline license headers;
workspace format and strict Clippy pass independently. The current merged Mem
`044a0c2f3ed6c8dbd76142a234558e1c81bdf602` reference host executes 20 tests,
0 failures, 0 ignored, 60 filtered out with the exact source override. This is a
development library qualification; a published Mem dependency pin, release,
production-memory acceptance and activation remain separate gates.
