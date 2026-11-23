# Ordinary out-of-core Search navigation beam

Issue: https://github.com/nowledge-co/hawdb/issues/1614.

The reader default becomes 256; generic, resident and canonical index serving
retain 1024. Ordinary Rust Search precedence is per-query
`VectorSearchExecutionOptions::with_navigation_beam`, then
`SearchOutOfCoreConfig::default_navigation_beam`, then the built-in reader
default. Python bindings do not expose this execution option. Canonical
`vector_index_search` bypasses the ordinary reader setting; its single-graph
recall qualification remains protected at 1024.

The fresh macOS arm64 release run uses SIFT's first 100,000 training vectors
(128 dimensions) and first 128 held-out test vectors from the ANN Benchmarks
[dataset](https://ann-benchmarks.com/sift-128-euclidean.hdf5). `provenance.json`
records input SHA-256 values and selection. The fixture has 13 segment graphs;
all variants query the same persisted index, top 10, candidate window 128,
`ScoresOnly`, one query worker, 128 distinct queries repeated twice, and rotated
variant order. Ground truth is independent scalar f64 squared L2 over the
eligible base vectors with stable ID ties.

| Beam, unfiltered | Recall@10 | Mean distance comparisons | Generated candidates | Raw reranked |
|---|---:|---:|---:|---:|
| Reader default 256 | 1.0 | 22,832.34 | 1,664 | 128 |
| Explicit 128 | 1.0 | 15,624.19 | 1,664 | 128 |
| Explicit 256 | 1.0 | 22,832.34 | 1,664 | 128 |
| Explicit 512 | 1.0 | 32,787.86 | 1,664 | 128 |
| Explicit 1024 | 1.0 | 45,952.73 | 1,664 | 128 |

The reader default executes exactly the explicit 256 work, 50.31% fewer distance
comparisons than 1024 on these queries. These are comparison counts, not a
count of distinct corpus vectors or raw reranking work. The raw artifact
`results.jsonl` also records p50/p95, QPS, adaptive-exact scan counts and fallback
codes. Builds were running concurrently, so timings do not qualify quiet-host
throughput or cross-engine speed. GloVe/cosine scale qualification was not
freshly repeated; the issue's prior GloVe recall evidence is separate.

The deterministic 10% and 1% filters score their complete 10,000/1,000 eligible
domains. They are full-domain controls, not evidence of approximate filtered
recall. Adapter adaptive-exact counts explain some of those paths; a zero
adaptive counter alone does not establish partial-domain ANN traversal.

Use the repository toolchain, locked dependencies and release profile, with a
target directory dedicated to this worktree:

```sh
cargo build --locked --release --example vector_navigation_beam --target-dir "$BEAM_TARGET"
"$BEAM_TARGET/release/examples/vector_navigation_beam" build "$BEAM_INDEX" "$BASE_FVECS" 100000
"$BEAM_TARGET/release/examples/vector_navigation_beam" compare "$BEAM_INDEX" "$BASE_FVECS" "$QUERY_FVECS" 128 2
```

Convert the selected HDF5 rows to little-endian fvecs: each row is a u32
dimension followed by that many f32 coordinates. Use the recorded hashes to
check identical inputs. The open receipt reports the compiled reader 256 and
generic 1024 defaults. A shared-target run that reused baseline dependencies was
discarded; it is not part of this result. Independent build directories avoid
Cargo accepting another worktree's newer artifacts against older source mtimes.

A smaller beam trades recall for less work on other data. Candidate/result
floors, exact requests, memory admission and explicit wider searches retain
their existing contracts. This fixture does not qualify the canonical
single-graph 95% family recall gate or justify lowering its separate default.
