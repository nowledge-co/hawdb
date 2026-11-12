# Checked candidate exhaustion

Tracking issue: https://github.com/nowledge-co/hawdb/issues/1713

## Contract

Search candidates are a bounded projection result. A successful top-k query
does not prove that the projection contains no other query matches. In
particular, a host may reject every returned candidate while an eligible
candidate remains outside the returned window.

All high-level candidate entrypoints return sealed `SearchCandidateOutput`,
including resident/out-of-core projections, store handles and bounded read
snapshots. Raw result fields are no longer public, and no `Deref` or implicit
conversion extracts a `SearchResultSet`. Every caller must explicitly accept
bounded coverage or request exhaustion.

Construction is private to the retrieval owner. Candidate evidence is emitted
by the resident index, its retained read generation or the out-of-core reader
using the request that actually executed. A host cannot repackage a raw result
with an unrelated request or report and obtain the checked view.

`SearchCandidateOutput::require_exhaustive()` returns a borrowed
`ExhaustiveSearchCandidates` view only when the existing execution evidence
proves exhaustion of the query's matching candidates within the selected
projection and request filters. The view exposes immutable hits and their
report, has no public constructor, and borrows the original output. Validation
does not rerun search, widen the request, allocate another candidate list or
discard a valid bounded result.

The validator requires:

- Exact metadata eligibility evidence.
- An exactly empty eligible universe, or all of the following conditions.
- No skipped prefix, truncated page, truncation reason, or missing reported hit.
- No ranking window smaller than the eligible universe. This conservative
  rule can refuse a result even if a particular query happened to match fewer
  rows than the window.
- Every requested retrieval leg was available and has exact input eligibility.
- Known exhaustive scoring evidence: BM25/segmented BM25 for text, and raw
  vector candidate generation plus raw vector final scoring for vectors.
- No lossy query-term reduction in the text leg. `TextQueryTermsReduced`
  refuses exhaustion even when the retained terms use exact BM25, the result
  window is complete, or an exact vector leg also ran. `MostSelective` without
  actual term reduction remains eligible for a proof.

ANN candidate generation is not an exhaustion proof, even after raw vector
reranking, with exact metadata filters, with fewer than k results, or when the
small fixture happens to return all matches. Unknown candidate sources refuse
conservatively. An exactly empty eligible universe does not need retrieval
execution and does not claim that a particular backend ran.

`SearchCandidateExhaustionError` distinguishes eligibility, result-window,
ranking-window, unavailable-retriever and unproven-retriever refusals. It is a
consumer-contract refusal, not a search execution failure. Diagnostics emitted
by the validator contain fixed reasons and retrieval-leg names, never queries,
hit bodies, metadata or provider messages.

## Host use

Call the check before treating a post-filtered candidate set as complete:

```rust
use hawdb::{EmbeddedStoreHandle, SearchCandidateRequest};

fn no_matching_candidates(
    handle: &EmbeddedStoreHandle,
    query: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let request = SearchCandidateRequest::text(query, 100);
    let output = handle.search_candidates_with_report(&request)?;
    let complete = output.require_exhaustive()?;
    Ok(complete.hits().is_empty())
}
```

For bounded top-k, explicitly call `accept_bounded()` for an immutable result
borrow or `into_bounded()` for owned result/report/metrics. The bounded
choice provides no proof of completeness. Reports and metrics remain independently
inspectable without exposing hits. These choices allocate no extra hit list.
If exhaustive consumption refuses, retain the bounded result as partial
evidence, widen/retry only within the host's explicit budget, or propagate the
refusal. Do not convert it into a successful exhaustive empty response.

This intentionally changes the public Rust source API.
Old direct field access and entrypoints that implicitly returned raw candidate
hits must migrate. `OutOfCoreSearchCandidateOutput` is now an alias of the same
sealed output; out-of-core metrics use the common optional metric accessor.
Report JSON, retrieval behavior, resource budgets and on-disk formats are
unchanged. Low-level `SearchIndex`/reader query primitives retain their bounded
contracts; they are not exhaustive-consumption APIs.

## Authority and freshness

The proof applies to the engine-produced output, its projection generation,
query and request filters. It does not prove
canonical-store completeness, certify projection freshness, or authorize an
external graph/database to revalidate identities. Host predicates outside the
request are not pushed into candidate generation by this check.

Use the existing pinned graph/search snapshot and admission contracts for
source identity, freshness and canonical hydration. A host that routes HawDB
hits through a legacy-only graph can still drop valid hits; adopting this
exhaustion guard does not repair that authority mismatch.

## Regression ownership

The existing Search feature-matrix owners run
`candidate_evidence::consumption::tests`. The fixtures distinguish a complete
query with zero matches from a zero-size/truncated window, cover offsets and
ranking windows, check missing/unknown retrievers and safe diagnostics, and
reject ANN generation followed by raw reranking. A two-document persisted
fixture executes out-of-core exact text/vector/hybrid queries and the default
graph candidate path; it also retains the original metrics.
Six-document resident, retained-generation and persisted-reader regressions
retain documents matching only a dropped term. They distinguish exact and
unchanged `MostSelective` queries from reduced text and hybrid queries, and
verify that refusal preserves the bounded result and report.

The existing root `fallible_search_contract` default/minimal owners verify the
public facade export and explicit bounded result/report behavior. Compile-fail
documentation guards refuse direct result access. These
cases establish consumption correctness; they make no recall, performance,
cross-store hydration or runtime-activation claim. See
[the test index](../TEST_INDEX.md).
