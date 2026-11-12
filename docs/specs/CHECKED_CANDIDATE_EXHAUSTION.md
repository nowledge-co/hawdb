# Checked candidate exhaustion

Tracking issue: https://github.com/nowledge-co/hawdb/issues/1713

## Contract

Search candidates are a bounded projection result. A successful top-k query
does not prove that the projection contains no other query matches. In
particular, a host may reject every returned candidate while an eligible
candidate remains outside the returned window.

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

For bounded top-k, use the existing result directly under its bounded contract.
If exhaustive consumption refuses, retain the bounded result as partial
evidence, widen/retry only within the host's explicit budget, or propagate the
refusal. Do not convert it into a successful exhaustive empty response.

This additive API preserves existing fields, signatures, report JSON and
on-disk formats. Existing callers can still discard the report, so they must
adopt the checked view at boundaries requiring exhaustion. A future removal of
unchecked access is a separate source-compatibility decision.

## Authority and freshness

The proof applies to the engine-produced output, its projection generation,
query and request filters. It does not validate host-modified evidence, prove
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

The existing root `fallible_search_contract` default/minimal owners verify the
public facade export and preserve unchecked result/report behavior. These
cases establish consumption correctness; they make no recall, performance,
cross-store hydration or runtime-activation claim. See
[the test index](../TEST_INDEX.md).
