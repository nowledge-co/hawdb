# Ordinary projection vector scoring

`SearchVectorScoring` is an immutable projection policy, selected before adding
documents. It is independent of a schema-bound `VectorIndexDescriptor`.

| Policy | Candidate distance | Exposed vector score |
| --- | --- | --- |
| `ProjectionMetric` (default), ordinary | Cosine | Positive cosine similarity |
| `ProjectionMetric`, schema-bound Euclidean | Euclidean | `1 / (1 + distance)` |
| `LinearSquaredL2`, ordinary | Euclidean | `max(0, 1 - squared_l2 / 2)` |

The last policy preserves Mem's existing LanceDB score formula for unnormalized
Float32 embeddings. It does not normalize stored vectors or query vectors, change
the embedding model, or reinterpret an existing cosine index. Zero vectors have
ordinary L2 distances; a host's missing-embedding eligibility remains its concern.

```rust
use hawdb::{SearchIndex, SearchVectorScoring};

let mut index = SearchIndex::in_memory();
index.set_vector_scoring(SearchVectorScoring::LinearSquaredL2)?;
# Ok::<(), hawdb::HawDBError>(())
```

For a streaming build, set `SearchOutOfCoreGenerationBuildOptions::vector_scoring`
to the same policy. The existing embedded `CALL vector_search` path returns its
scores directly. No host-side projection scan or Lance dependency is required.

## Selection and numerical behavior

Exact selection and ANN reranking use squared distances before score clipping.
They retain far neighbors whose exposed score is zero, order them by distance,
and apply offset/limit before converting the public scores. Hybrid fusion keeps
its existing reciprocal-rank behavior. Hits, scores-only results, candidates and
retriever diagnostics expose the same score scale.

The squared-distance kernel subtracts and accumulates in Float32 with 16 fixed
portable lanes and a separate tail. An internal positive, lossless ordering key
encodes the nonnegative Float32 distance bits; it is never a public similarity.
This preserves Float32 distances through ranking, including overflow to infinity
(zero public similarity). Different legacy platform SIMD reductions can round
distances differently. This contract preserves the L2 formula and precision
class, not bit-for-bit equivalence to every historical Lance SIMD build. The
1536-dimensional regression fixture pins values on both sides of Mem's 0.80
threshold without reduction-order ambiguity.

DiskANN builds and navigates with native Euclidean distance, then reranks its
candidate window with this policy. ANN recall remains approximate; use existing
exact execution controls for an exhaustive result. Existing work, memory,
cancellation, filtering and output admission limits still apply.

## Identity and compatibility

The policy is included in resident snapshots, retained read generations,
out-of-core manifests and the DiskANN source identity. Incremental updates inherit
the pinned base policy; changing it requires a full replacement. Schema-bound
descriptors and cosine-only RaBitQ artifacts are refused with `LinearSquaredL2`.
Explicit required compressed search is unsupported for this policy. Ordinary
adaptive reads use the native DiskANN/exact path.

Default projections retain their existing serialized metadata and scoring.
Opt-in snapshots add `vector_scoring\tlinear_squared_l2`; manifests add
`"vector_scoring": "linear_squared_l2"`. Older readers reject these unknown
records rather than silently interpreting them as cosine. No authoritative
legacy data migration or Mem stable-release activation is authorized by this
engine capability.

See [#1541](https://github.com/nowledge-co/hawdb/issues/1541) and the owning
[test families](TEST_INDEX.md).
