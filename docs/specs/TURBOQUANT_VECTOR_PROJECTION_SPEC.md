# TurboQuant Vector Projection Contract

## Scope And Naming

Skein exposes vector retrieval through the default `vector-search` Cargo
feature. `TurboQuant` identifies the quantization algorithm family. `turbovec`
identifies an optional upstream Rust implementation used only as a differential
or shadow oracle. Production reports MUST NOT call the Skein implementation
`turbovec`.

The `skein-vector-projection` crate owns rebuildable vector encodings and scan
kernels. The embedded `skein` crate remains the supported application facade.
Raw embeddings remain canonical search data; a TurboQuant artifact is a
derived candidate projection and MUST NOT become the source of truth.

## Encoding

The `turboquant` algorithm descriptor currently means all of the following:

1. L2-normalize each finite, non-empty input vector.
2. Apply the manifest's deterministic signed block-Hadamard orthogonal
   transform. This bounded fast transform replaces a dense QR rotation and
   requires `O(d)` scratch rather than `O(d^2)` resident state.
3. Derive a 16-level Lloyd-Max scalar codebook from the Gaussian
   `N(0, 1/d)` coordinate model. The codebook is determined by dimension and
   encoding version, not trained from the indexed corpus.
4. Pack two 4-bit coordinate codes per byte.
5. Store one length-renormalization scalar per vector to correct quantization
   shrinkage during candidate scoring.

The manifest MUST identify the algorithm, transform, quantizer, calibration,
bit width, metric, seed, dimension, embedding model and version, source epoch,
source digest, and generation. The current calibration value is `none`.
Per-coordinate TQ+ calibration, 2-bit encoding, AVX-512, and ANN integration
are not part of this contract and require production evidence before adoption.

## Candidate And Rerank Semantics

TurboQuant scores are approximate and MUST only select a bounded candidate
set. The embedded search executor MUST read canonical raw embeddings and
rerank every returned candidate before applying the final TopK. Public low-level
types therefore use `CandidateProjection`, `CandidateScanOptions`, and
`CandidateOutput` naming and MUST NOT be presented as final search results.

Metadata, tenant, lifecycle, and ACL eligibility MUST be resolved before
candidate scoring. An allowlist is converted to a compact row bitmap. A
32-row block with no eligible row MUST be skipped before distance scoring;
ineligible rows in a partially selected block MUST never enter the TopK heap.
Post-filter-only over-fetch is not an authorization boundary.

## Resource And Concurrency Contract

Projection construction MUST be streaming and segment-bounded. At most one
admitted compressed segment, one transformed vector, and one packed row may be
mutable build intermediates. If the requested segment size exceeds the memory
budget, the builder MUST reduce the admitted row count or fail with a stable
resource-budget error; it MUST NOT collect all transformed vectors.
Input numeric IDs MUST be strictly increasing. This permits constant-memory
duplicate detection during build and artifact validation instead of retaining
an unbounded uniqueness set. The embedded facade sorts its stable hashed IDs
and fails closed on hash collision before calling the projection crate.

Candidate scans MUST use a fixed-memory TopK, a bounded segment buffer, a
bounded filter bitmap, and a transformed-query buffer. Unfiltered scans MUST
not materialize a full-document allowlist. The caller supplies
`max_working_bytes`, `max_parallelism`, and an optional cancellation context.
Admission accounts for the global and per-worker TopK, segment buffers, filter
bitmaps, and bounded worker stacks. Worker count MUST be the minimum of the
requested parallelism, segment count, and memory-admitted parallelism. Skein
MUST NOT create a Rayon pool, Tokio runtime, or other global vector-search
executor.

The portable scalar kernel is the correctness reference. x86_64 may dispatch
to AVX2 only after runtime feature detection. AArch64 may use NEON. Every
accelerated kernel MUST preserve scalar candidate ordering and score tolerance
for the same artifact, filter, query, and TopK. Unsupported requested kernels
MUST fail rather than silently changing the request.

## Artifact And Recovery Contract

Checkpoint publishes `search_turboquant.<generation>.skein` as an immutable
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
dimension, embedding identity, and source epoch. A corrupt or stale derived
artifact MAY be quarantined and rebuilt from canonical raw embeddings. It MUST
NOT make canonical storage unreadable and MUST NOT be repaired in place.

## Observability And Production Admission

Search reports MUST expose the selected kernel, admitted worker count, segment
counts, scored and filtered documents, scanned and skipped blocks, projection
payload bytes, admitted working bytes, candidate score source, raw-vector bytes
read, and raw rerank source. Readiness MUST expose artifact format and
generation identity and MUST fail closed when a required projection is absent,
stale, corrupt, or mismatched. The artifact manifest MUST retain configured and
admitted build memory so reopen reports do not erase resource evidence.

Unit, scalar/SIMD parity, corruption, and cross-platform CI establish code
correctness only. Production admission additionally requires generation-bound
evidence from representative Mem data: recall against raw-vector truth,
filtered and ACL parity, P50/P95/P99 latency, steady and peak RSS, page faults,
payload bytes, build amplification, cancellation, mixed-load behavior, and
reopen/corruption recovery. Until that evidence passes, the capability is
eligible for shadow or canary use but is not independently production-qualified.
Local kernel-regression methodology and directional SIMD results are recorded
in the [TurboQuant vector projection benchmark](../TURBOQUANT_VECTOR_PROJECTION_BENCHMARK.md);
they do not satisfy production admission.

Cross-platform kernel CI MUST execute the deterministic scalar/SIMD corpus
natively on Linux x86_64, Linux AArch64, macOS AArch64, and Windows x86_64.
Cross-compiling the AArch64 crate is useful syntax coverage but is insufficient
because it does not execute NEON selection or scoring. Every target MUST also
prove that explicit unsupported-kernel requests fail rather than falling back.

The typed production evidence protocol is
`skein-vector-recall-production-qualification-v1`. It wraps, but does not
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
emits `skein-production-vector-qualification-v1`. It adds named unfiltered,
metadata-filtered, and feature-conditional ACL cases; dispatched-versus-scalar
candidate parity; execution and process resource metrics; and disposable-copy
lifecycle probes for incremental updates, checkpoint/reopen, stale readers,
corruption, cancellation, and mixed foreground/background work. The companion
`skein-production-vector-qualification-matrix-v1` evaluator requires Linux
x86_64, Linux AArch64, macOS AArch64, and Windows x86_64 reports bound to the
same corpus and release identity.

When `skein-qualification/turbovec-oracle` is enabled, the collector builds the
upstream `turbovec` implementation only as an offline differential oracle. The
dependency and its full-residency index are owned by `skein-qualification`;
the production `skein` crate has no `turbovec` feature, backend, or checkpoint
artifact. Oracle candidates re-enter Skein through a validation-only ingress so
metadata filtering and canonical raw-vector reranking still execute in the
serving implementation. That ingress is compiled only by the non-default
`skein/qualification` feature forwarded from the oracle feature. The report
records candidate-set overlap and final-result comparisons without treating
oracle agreement as correctness truth.
