# Raw vector scans and retained score ownership

The out-of-core scalar scan and RaBitQ raw rerank now carry the same query
memory root already used by candidate buffers and lexical scores. This is a
private call boundary: no public options, reader signatures or v1 artifact
formats change. The two operation accounts are retained; neither a per-segment
account nor another independent full-query allowance is created.

## Raw vector sidecars

Positioned reads and decompression use the admitted reader described in
`CANDIDATE_QUERY_ADMISSION.md`: raw bytes are reserved before I/O, and native
workspace plus exact advertised output are reserved before decompression.
The pinned modern zstd-frame qualification applies to vector sidecars as well.

An allocation-free pass validates the header, fixed four-field row shape,
actual row count, checked ordinal sequence, ID hex syntax, exact coordinate
count and finite f32 values. Only then may the row decoder allocate its exact
row slots, IDs and vector coordinates. UTF-8 IDs and their ordering/bounds are
also validated by the decoder. A corrupt dimension/count cannot drive an
initial vector capacity or append unbounded rows. Error and cancellation paths
release all admitted row data; a successfully returned row set owns its lease
even after query and reader handles have been dropped.

Scalar scan and raw rerank borrow these rows, check cancellation within the
row loop and reserve incoming score-ID scratch before cloning an ID. The input
row and returned score are distinct owners during that transfer.

## One score collector

The previously separate vector collector is removed. A private shared
`score_collector` module retains the existing lexical heap/map algorithm,
tie-breaks, byte limits and data-before-lease result owner. Lexical and vector
call sites supply their own diagnostic labels; no vector-specific copy of
the admission and replacement rules is maintained.

Vector results now share the score account with any retained lexical results.
The task's result cap therefore cannot be reused independently by each
retriever. Heap slots and the existing 256-byte per-ID container/conversion
allowance are charged. Rejection preserves existing owners; shrinking a top-k
replacement releases the removed ID before reducing its charge.
`VectorScoreScan` is no longer `Clone`, and moving its `AdmittedScores` out
cannot detach the charge. The score owner remains intact through ranking and
hydration. This does not yet admit the separate ranking/hydration allocations
or the public returned result payload.

RaBitQ selected ordinals also have an explicit exact-capacity owner while raw
rerank proceeds. Converting projection hits to ordinals uses a separately
admitted buffer rather than depending on an iterator's possible allocation
reuse. The preceding projection-hit allocation is a distinct remaining owner,
not retroactively covered by the selected-ordinal lease.

## Verification

Normal regressions cover retained rows and scalar/reranked scores, exact and
one-short roots, shared lexical/vector result-limit rejection, before-I/O
rejection, cancellation, and corrupt sidecars with repaired outer checksums.
The latter must reject malformed counts/dimensions/non-finite coordinates
before the row decoder allocation boundary. Publication remains readable after
the test reader rejects its corrupt replacement fixture. An I/O admission
probe verifies that selected ordinals, the score heap and the incoming raw
buffer remain charged simultaneously when reranking starts.

The fixture explicitly uses the generation writer and asserts an attached
RaBitQ artifact when the feature is enabled. `Required` tests cannot silently
pass by taking an unavailable-projection error or a scalar fallback.

The manual `skein_search_vector_admission_fuzz_tests` target uses seed
`0x206cec70` for 128 queries, with exact scalar and RaBitQ-rerank scans of the
same published fixture. An independent cosine/top-k oracle covers positive,
negative and zero query vectors, metadata filtering, five retention windows,
competing score owners and exact/one-short root retries. Every outcome releases
tracked capacity and preserves the two-account bound. The candidate cap covers
all fixture vectors: this checks exact rerank parity, not approximate recall.

```bash
cargo test -p skein-search --all-features vector_admission -- --nocapture
cargo test -p skein-search --all-features vector_admission_campaign -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Omitting row charges, score-ledger growth, selected-ordinal charges or the
finite-value preflight must fail targeted regressions. Restore these deliberate
negative controls before complete positive verification. Fuzz remains manual
and ignored in ordinary runs, with no default/dedicated fuzz CI additions.

## Remaining full-issue boundaries

This is not complete vector/query/RSS admission. RaBitQ candidate generation in
`crates/vector-projection/src/scan.rs::search_projection` remains outside the
shared ledger: its transformed-query allocation precedes the standalone memory
check, and its local/segment top-k, conversion and merge lifetimes need a full
overlap envelope plus retained projection-hit ownership. That envelope must be
connected before backend entry, not charged from a report after allocation.
The combined vector component limit also needs to cover every live phase,
not only its existing score-entry and projection allowances.

Pruning/report containers, rank/fusion copies, hydration, public output,
persistent delta and published-reader host ownership remain full-query work.
The resident vector path is not newly instrumented by this change. Jieba
workspace maintenance, exact-head native qualification and representative-corpus
posting-size reduction remain independent full #206 gates. No dependency,
backend, v1 migration, Bazel runtime/timeout setting or release policy changes.
