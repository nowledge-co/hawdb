# Out-of-core ranking and page admission

The query's existing two-account memory root now covers private rank arrays,
fusion scratch and the owned candidate page consumed by late hydration. No new
account, public option/output API, backend, crate or v1 artifact is introduced.

## Representation and semantics

Each retriever's rank workspace borrows document IDs from its admitted score
owner. One array remains in ID order; an ordinal array is sorted by descending
score and ascending ID to assign one-based ranks. Windows are predicates on the
original ranks, not cloned maps or renumbered ranks. Retriever reports copy only
their requested top IDs/candidates directly from this ordering.

Fusion merges the two ID-ordered arrays without building a full ID union set.
Each document is counted once. Existing mode-specific score selection, RRF
weights/window semantics, positive-score filtering and deterministic ID tie
ordering are preserved. Exact match counting traverses the complete union even
when only a small result page is retained.

A worst-first heap retains at most `offset + limit` candidates, capped by the
sum of input counts; a zero limit retains none. Heap candidates borrow IDs.
Consuming the heap into a vector reuses its allocation, then sorts in place.
Only the actual page's IDs are copied for hydration. Rank arrays are explicitly
dropped before hydration; the candidate page retains its own result-account
lease throughout hydration and after the query handle is dropped in tests.

## Admission and overlap

For input count `N`, rank arrays reserve
`N * (size_of::<RankedScore>() + size_of::<usize>())` before allocation. Both
arrays have exact requested capacity. Arithmetic and individual allocation
sizes are checked against overflow and `isize::MAX` before entry.

Fusion reserves `heap_capacity * size_of::<Reverse<Candidate>>()` from the
working account before creating the heap. The selected page reserves its vector
slots plus selected ID byte lengths from the existing result account before
copying. Both rank arrays, heap and page overlap during copying; all their
leases remain live for that overlap. The task's result limit applies in addition
to its total query limit. Errors and cancellation drop values before charges.

These are requested-capacity budgets, not allocator/process RSS measurements.
The input score maps retain their separate, already existing charges. The
ordering itself never clones an ID or allocates a window map. Sorting is
in-place with no separate heap-allocated sort buffer.

## Verification

Normal regressions cover borrowed pointer identity, independent array/heap/page
capacity arithmetic, exact/one-short budgets with competing owners, returned
page lifetime, task result-component denial, cancellation during page copying,
all modes/windows/weights, deterministic ties, empty/zero/overflow paths and a
4,096-score input whose heap capacity remains three for offset one/limit two.
A real published-reader query verifies that rank, heap and page admission are
actually wired before late hydration.
Without the vector feature, a companion integration regression proves that
the capability gate rejects the request before entering ranking; all eight
private ranking/page regressions still execute without default features.

The manual seed `0x206f0510` campaign generates 384 two-retriever inputs and
checks all three modes: 1,152 cases, each retried with exact/one-short root
capacity and competing owners. Missing/overlapping IDs, ties, Unicode/NUL,
long IDs, empty windows, zero/asymmetric fusion weights and out-of-range pages
are included. An independent pairwise-rank and full-union/full-sort oracle
checks every returned score/rank and exact count. Retriever report parity also
uses the legacy helpers. These are ranking semantics and ownership checks, not
BM25/vector-scoring or approximate-recall qualification.

```bash
cargo test -p skein-search --all-features ranking_ -- --nocapture
cargo test -p skein-search --all-features ranking_admission_campaign \
  -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Negative controls omit rank/heap/page-ID charges, bypass the result account,
allocate a full-union heap despite a small page, or ignore hybrid rank windows.
Each must fail assertions and be restored before complete positive checks.
The dedicated fuzz target is manual; no default or dedicated CI fuzz is added.

## Remaining #206 boundaries

This is not complete query/output ownership. Public retriever report payloads
and cloned candidate reports, parsed filter/ACL inputs, escaped hydrated documents
and public hits still need retained contracts. Internal hydration raw/text/document
owners now share the operation root (`HYDRATION_QUERY_ADMISSION.md`), independently
of matched-span/tokenizer scratch and escaped output. A temporary ranking or
page lease does not cover those objects after they escape. Cross-crate backend
admission and retained projection hits remain behind the pending public API
decision. Combined component limits, persistent delta, reader/mapping host
ownership, Jieba workspace, representative-corpus reduction and exact-head
native qualification remain separate full-issue gates.
