# RaBitQ Vector Projection Contract

## Scope And Naming

HawDB exposes vector retrieval through the default `vector-search` Cargo
feature. `RaBitQ` identifies the 1-bit and 4-bit candidate encodings described
in this contract. The implementation is native Rust and has no C++ FFI or
third-party vector-quantization runtime dependency.

The default is 1-bit standard RaBitQ, matching Faiss `IndexRaBitQ`'s default
`nb_bits`. The 4-bit multi-bit variant is an explicit build option.

The `hawdb-vector-projection` crate owns rebuildable vector encodings and scan
kernels. The embedded `hawdb` crate remains the supported application facade.
Raw embeddings remain canonical search data; a RaBitQ artifact is a
derived candidate projection and MUST NOT become the source of truth.

The exported HNSW, upsert-only delta, and index-advisor primitives are standalone
experiments, not part of this serving contract. Their explicit retention and
integration gates are in the [vector experiment roadmap](../VECTOR_EXPERIMENT_ROADMAP.md).
Keeping these APIs does not authorize ANN dispatch or automatic maintenance.

## Encoding

The `rabitq` algorithm descriptor currently means all of the following:

1. L2-normalize each finite, non-empty input vector.
2. Apply the manifest's deterministic signed block-Hadamard orthogonal
   transform. This bounded fast transform replaces a dense QR rotation and
   requires `O(d)` scratch rather than `O(d^2)` resident state.
3. Normalize transformed coordinate magnitudes and choose the per-vector
   rescaling factor that maximizes alignment with the offset-binary code.
4. Encode a Faiss-style LSB-first sign plane: coordinate `i` occupies bit
   `i % 8` in byte `i / 8`. In 1-bit mode this is the complete code. In 4-bit
   mode, append a separate LSB-first three-bit refinement plane, where the
   refinement bits for coordinate `i` start at bit `3 * i`.
5. Store a per-vector affine reconstruction scale and offset. Candidate scores
   are `scale * dot(query, codes) + offset * sum(query)`.

The manifest MUST identify the algorithm, transform, quantizer, calibration,
bit width, metric, seed, dimension, embedding model and version, source epoch,
source digest, and generation. Format version 1 uses
`rabitq_sign_then_refinement_scalar_1bit_v1` or
`rabitq_sign_then_refinement_scalar_4bit_v1`; its footer magic is `SKRQBF01`.
The current calibration value is `none`.

The sign/refinement code representation is intentionally aligned with Faiss
`IndexRaBitQ`, but HawDB stores its reconstruction factors in segment arrays
rather than inside each Faiss flat code. HawDB's signed block-Hadamard transform
is an explicit pre-transform selected by its manifest; Faiss expects any random
rotation to be performed externally. Therefore HawDB artifacts are not binary
interchangeable with Faiss artifacts even though their packed quantizer planes
have the same bit order and code semantics. Centroid calibration, other bit
widths, native SIMD scoring, and ANN integration require production evidence
before adoption.

## Candidate And Rerank Semantics

RaBitQ scores are approximate and MUST only select a bounded candidate
set. The embedded search executor MUST read canonical raw embeddings and
rerank every returned candidate before applying the final TopK. Public low-level
types therefore use `CandidateProjection`, `CandidateScanOptions`, and
`CandidateOutput` naming and MUST NOT be presented as final search results.

Metadata, tenant, lifecycle, and ACL eligibility MUST be resolved before
candidate scoring. An allowlist is converted to a compact row bitmap. A
32-row block with no eligible row MUST be skipped before distance scoring;
ineligible rows in a partially selected block MUST never enter the TopK heap.
Post-filter-only over-fetch is not an authorization boundary.

## Document Ordinal Boundary

Resident and out-of-core search use the same dense vector ordinals: traverse
documents in ascending UTF-8 document-ID order, omit documents without an
embedding, and number the remaining vectors from zero. No document-ID hashing
or collision rejection participates in this mapping. Candidate ties follow
ordinal order, which is also document-ID order within that generation.

An ordinal belongs to one projection generation, not to a stable document
identity. String IDs and canonical embeddings remain persisted together in the
search snapshot; their sorted vector-bearing subset defines the ordinal-to-ID
table. Resident reopen reconstructs that table from the snapshot and validates
the artifact's dimension, vector count, generation, embedding/source identity,
and ordered `(ordinal, embedding)` source digest before using it. Out-of-core
metadata persists the corresponding string ID and vector ordinal directly.
No second resident mapping sidecar or v1 format change is required.

Changing the ordered vector stream invalidates the derived artifact. Changing
only nonvector documents, or renaming an ID while preserving that stream, can
reuse it: candidates resolve through the current snapshot's IDs. A stale or
earlier sparse-ID artifact is not applicable to the new mapping and can be
rebuilt from canonical embeddings; a source mismatch is not corruption.
The raw projection crate still accepts arbitrary strictly increasing numeric
IDs and carries no string-ID table. Its IDs MUST NOT be treated as stable host
document identities or compared across generations without the search mapping.

## Resource And Concurrency Contract

Projection construction MUST be streaming and segment-bounded. At most one
admitted compressed segment, one transformed vector, and one packed row may be
mutable build intermediates. If the requested segment size exceeds the memory
budget, the builder MUST reduce the admitted row count or fail with a stable
resource-budget error; it MUST NOT collect all transformed vectors.
Input numeric IDs MUST be strictly increasing. This permits constant-memory
duplicate detection during build and artifact validation instead of retaining
an unbounded uniqueness set. The embedded facade supplies the dense ordinals
defined above, in increasing order, to the projection crate.

Candidate scans MUST use a fixed-memory TopK, a bounded segment buffer, a
bounded filter bitmap, and a transformed-query buffer. Unfiltered scans MUST
not materialize a full-document allowlist. The caller supplies
`max_working_bytes`, `max_parallelism`, and an optional cancellation context.
Admission accounts for the global and per-worker TopK, segment buffers, filter
bitmaps, and bounded worker stacks. Worker count MUST be the minimum of the
requested parallelism, segment count, and memory-admitted parallelism. HawDB
MUST NOT create a Rayon pool, Tokio runtime, or other global vector-search
executor.

The portable scalar kernel is the only active kernel and the correctness
reference. On Arm, an explicit NEON preference is accepted but dispatches to
that scalar kernel and reports `scalar`, matching Faiss's current RaBitQ NEON
fallback. Explicit AVX2, and NEON requests on non-Arm targets, fail closed.
Any future accelerated kernel MUST preserve scalar candidate ordering and score
tolerance for the same artifact, filter, query, and TopK before it can become
eligible for automatic dispatch.

## Artifact And Recovery Contract

Checkpoint publishes `search_rabitq.<generation>.hawdb` as an immutable
generation. Segment payloads precede a checksummed JSON manifest and fixed
footer. Each segment and the complete payload carry checksums. Publication
MUST create a unique temporary file, flush it, atomically rename it to a new
generation, and retain the previous generation during cleanup. Generation
cleanup MUST run only after publication, use a bounded pending-file queue, and
revalidate every queued generation before retry. Delete failures MUST NOT turn
a durable checkpoint into a failed checkpoint. They MUST remain observable in
`SearchProjectionCleanupReport`, be retried during a later open, checkpoint, or
explicit cleanup cycle, and block production qualification while pending.

Open MUST validate footer magic, format version, all size arithmetic,
checksums, unique IDs, segment offsets, source digest, document identity,
dimension, embedding identity, and source epoch. A corrupt derived artifact MAY
be quarantined and rebuilt from canonical raw embeddings. A valid but stale or
inapplicable artifact MUST be skipped without classifying it as corrupt. Neither
case may make canonical storage unreadable or repair an artifact in place.

## Observability And Production Admission

Search reports MUST expose the selected kernel, admitted worker count, segment
counts, scored and filtered documents, scanned and skipped blocks, projection
payload bytes, admitted working bytes, candidate score source, raw-vector bytes
read, and raw rerank source. Readiness MUST expose artifact format and
generation identity and MUST fail closed when a required projection is absent,
stale, corrupt, or mismatched. The artifact manifest MUST retain configured and
admitted build memory so reopen reports do not erase resource evidence.

Unit, scalar-reference parity, corruption, and cross-platform CI establish code
correctness only. Production admission additionally requires generation-bound
evidence from representative Mem data: recall against raw-vector truth,
filtered and ACL parity, P50/P95/P99 latency, steady and peak RSS, page faults,
payload bytes, build amplification, cancellation, mixed-load behavior, and
reopen/corruption recovery. Until that evidence passes, the capability is
eligible for shadow or canary use but is not independently production-qualified.
Local kernel-regression methodology and directional SIMD results are recorded
in the [RaBitQ vector projection benchmark](../RABITQ_VECTOR_PROJECTION_BENCHMARK.md);
they do not satisfy production admission.

Cross-platform CI MUST execute the deterministic scalar corpus natively on
Linux x86_64, Linux AArch64, macOS AArch64, and Windows x86_64. Arm targets
MUST also prove the explicit-NEON scalar fallback; non-Arm targets MUST prove
that the same request fails. Adding a native SIMD kernel changes this
requirement to include ordering-parity execution on each supported SIMD target.

The typed production evidence protocol is
`hawdb-vector-recall-production-qualification-v1`. It wraps, but does not
replace, the bounded recall probe. Validation recomputes readiness against the
currently opened file-backed projection and an explicit current release
identity; a stale revision, feature set, configuration, dataset fingerprint,
canonical graph epoch, projection generation, source digest, or embedding
identity fails closed. A generation-zero in-memory projection remains useful
for development differential tests but cannot satisfy production admission.
The bounded probe measures two distinct values against canonical raw-vector
TopK: recall of the quantized candidate window before raw reranking, and recall
of the final raw-reranked TopK. Candidate capture is enabled only inside this
probe, is capped independently from TopK, and document identifiers are never
serialized into the qualification report.

The full release collector is `run_production_vector_qualification`, which
emits `hawdb-production-vector-qualification-v1`. It adds named unfiltered,
metadata-filtered, and feature-conditional ACL cases; dispatched-versus-scalar
candidate parity; execution and process resource metrics; and disposable-copy
lifecycle probes for incremental updates, checkpoint/reopen, stale readers,
corruption, cancellation, and mixed foreground/background work. The companion
`hawdb-production-vector-qualification-matrix-v1` evaluator requires Linux
x86_64, Linux AArch64, macOS AArch64, and Windows x86_64 reports bound to the
same corpus and release identity.

The qualification collector records native scalar-reference evidence for every
case: dispatched and scalar candidate digests, final exact-result parity, and
out-of-core serving parity. This guards a future dispatch implementation from
silently changing candidate selection while keeping all production data paths
inside the embedded Rust library. It is a consistency check, not an independent
claim of ANN recall; recall remains measured against canonical raw-vector truth.
