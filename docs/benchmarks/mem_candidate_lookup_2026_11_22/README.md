# Mem candidate lookup qualification

This measures the current Mem search ranking query against HawDB base
`f80136d41d9ade2ae8ad02f9bfc1f73cf4b3ba51` and the bounded constant-list lookup
rewrite in [production.patch](production.patch). The exact query comes from
[Mem `bc7a7b8`](https://github.com/nowledge-co/mem/blob/bc7a7b8eea681633bab0cb3324c1128cdcea0061/nmem-rs/crates/nmem-server/src/hawdb_search_scoring.rs).
Both builds use the same `mem_query_resources` harness, whose source hash is in
the [manifest](manifest.json). No Mem code or dependency pin was changed.

The rewrite turns `[candidate IN $candidates WHERE candidate.id = m.id][0]`
into a proven equivalent CASE expression. The existing executor prepares one
bounded sorted dispatch table per query, then performs one lookup per Memory.
This removes the repeated full candidate-list traversal. Other list expressions
keep their general evaluator; see [the scope and guards](../../MEM_QUERY_RESOURCES.md).

## Protocol and provenance

- Apple M5 Max, 36 GiB RAM, macOS 27.0.1 (26A434), aarch64, Rust 1.99.1.
- Repository release profile, default Cargo features, two build jobs. Separate
  `qualification` executables collect allocation requests; their timings are
  excluded from the latency table.
- In-memory database, two equal Spaces, indexed Memory IDs, 16 cache entries.
  Each read opens a snapshot transaction with a private physical plan cache.
  The shared state is prewarmed; one score changes between samples. This is not
  an unchanged-parameter physical-cache-hit benchmark.
- Three alternating process pairs per release cell, 30 reads per process:
  baseline/candidate, candidate/baseline, baseline/candidate. There are 900
  release reads across 30 processes. Six separate allocation cells contain
  10 reads each. Setup is outside the read timer.
- Every read checks the complete candidate IDs, their order, all five ranking
  inputs and the newly rebound score. Every candidate sample reports exactly
  one dispatch table and one lookup per output row.
- This was a shared developer desktop; unrelated processes and compilation
  were observed during the campaign. It was not a dedicated benchmark host.
  All accepted final-source samples are retained, including slower individual
  pairs. These observations do not establish production or remote CI speedups.

The [compressed raw cells](raw.jsonl.gz) include commands, process wall time,
per-read samples, allocation/plan counters, writer counts and executable hashes.
The [CSV](summary.csv) retains the summary values without display rounding.
The manifest records source hashes, build features, binary hashes and artifact
checksums. Each successful build's executable was copied before another build
could reuse the shared Cargo target directory.

After capture, a test-only Box assignment was changed for strict Clippy.
[validation-only.patch](validation-only.patch) records that change; the manifest
checks that the optimizer runtime definitions before `cfg(test)` and the
benchmark remain byte-identical, and records the final test-source hash.

The first baseline build in this worktree took 414 s, the baseline ordinary
release build 287 s, the candidate ordinary build 335 s,
and the final candidate allocation build 294 s. These are build costs on an
existing shared target directory, not clean-machine or query execution times.

## Ordinary release results

Values are medians of the three process results. Each process p95 uses the
nearest-rank percentile of its 30 reads. CPU is process CPU divided by reads;
it includes result verification and benchmark bookkeeping. RSS includes fixture
setup and is a process high-water mark.

| Candidates | Rows per Space | Body bytes | Background writer | p95 before/after (ms) | Change | CPU before/after (ms/read) | RSS before/after (MiB) |
| ---: | ---: | ---: | :---: | ---: | ---: | ---: | ---: |
| 20 | 512 | 64 | No | 0.557 / 0.496 | -11.0% | 0.434 / 0.412 | 19.23 / 19.06 |
| 100 | 512 | 64 | No | 2.219 / 1.699 | -23.4% | 1.982 / 1.528 | 20.80 / 20.84 |
| 512 | 512 | 64 | No | 19.475 / 9.270 | -52.4% | 18.866 / 8.404 | 30.64 / 29.45 |
| 512 | 4096 | 16384 | No | 32.497 / 24.716 | -23.9% | 29.999 / 20.871 | 309.67 / 313.33 |
| 512 | 512 | 64 | Yes | 23.344 / 11.840 | -49.3% | 40.483 / 19.606 | 32.05 / 30.77 |

The background writer runs until foreground completion. Faster reads shorten
its window, so its CPU row is not an equal-write-work comparison. Preserve the
raw completed-write counts when interpreting that cell. No synchronized commit
throughput or power-loss claim is made by this in-memory fixture.

## Allocation results and tradeoffs

These are cumulative Rust allocator requests per read, including the shared
harness bookkeeping, at 512 rows per Space and 64-byte bodies. They are neither
physical bytes copied nor live/RSS memory savings.

| Candidates | Requested bytes before/after | Change | Allocation calls before/after |
| ---: | ---: | ---: | ---: |
| 20 | 1,334,605 / 1,356,521 | +1.6% | 10,429.6 / 10,527.7 |
| 100 | 5,740,389 / 5,611,985 | -2.2% | 38,783.6 / 39,361.7 |
| 512 | 39,228,134 / 31,838,977 | -18.8% | 175,214.8 / 178,264.8 |

CASE expansion introduces plan/key allocations, and allocation-call counts
increase. At 512 candidates the query-account peak increases by 16,432 bytes
for dispatch state. The instrumented warm requested-byte high-water also rises
from 9,938,487 to 10,627,974 bytes, including transient planning. Large-body RSS
increases in the ordinary matrix. This change reduces CPU and bulk cumulative
allocation traffic; it does not establish a universal peak-memory reduction.

The measured scope is the embedded graph query phase. Host scoring, vector/text
retrieval, final body hydration and the complete Mem endpoint remain outside
this benchmark. The generic fallback and non-matching query shapes are not
assigned a speedup from these results.

## Replay

Use the same harness on the base source and on the base plus `production.patch`.
Build and copy each executable separately, preserving its identity:

```sh
CARGO_BUILD_JOBS=2 cargo build --locked --release --bench mem_query_resources --message-format=json > build.jsonl
# Use the compiler-artifact executable for mem_query_resources from build.jsonl.
<executable> 20 512 64 30 0 current-mem-ranking
<executable> 100 512 64 30 0 current-mem-ranking
<executable> 512 512 64 30 0 current-mem-ranking
<executable> 512 4096 16384 30 0 current-mem-ranking
<executable> 512 512 64 30 1 current-mem-ranking
```

Repeat all cells in the alternating order above. For allocation observations,
build separately with `--features qualification`, then run 20/100/512 candidates
with `512 64 10 0 current-mem-ranking`. Do not run compilation or another local
test campaign concurrently with these measurements.
