# Default Cypher clause pipeline

Issue: https://github.com/nowledge-co/hawdb/issues/158

Ordinary `MATCH` statements, including reads, shortest paths and matched
mutations, parse into `Statement::Pipeline`. Vector-search `YIELD` retains its
typed procedure call and feeds the same MATCH clause parser. The AST keeps
clause order, explicit anonymous-node identity and original UTF-8 byte spans.
DDL, session control and standalone primitive mutations/procedures retain their
existing statement representation.

The legacy MATCH catalogue, its mutually exclusive optional fields, template
parser and lowering arms are removed. Shared scalar/property expressions and
parameter binding remain reusable grammar and planning primitives. Public MATCH
parsing does not speculatively parse the same query twice. The grammar-only
`parse_pipeline` seam remains available for binder admission tests.

## Preserved behavior

Binding validates scopes, types, parameters and bounded relationship admission
before plan normalization. Simple public OPTIONAL MATCH parsing retains its
one-hop boundary. Previously public queries with at least two WITH clauses keep
their existing bounded OPTIONAL grammar and binder admission; the migration does
not withdraw that capability. The grammar-only seam lets binder tests prove
rejection before either raw or normalized planning reaches execution. Unsupported atomic mutations fail
before any writes on both empty and populated databases.

Typed clause/procedure classification preserves plan-cache eligibility: reads
and graph algorithms retain their prior policy; mutations, UNWIND, vector search
and graph projection remain ineligible. Procedure capabilities are checked
before parameter binding, planning, cache accounting or catalog mutation.
Statement kinds, fast-path reasons and ordering/pagination reports are derived
from the AST and preserve existing host reports.

Optional plans with target-node properties retain GraphMatch when Expand cannot
represent those predicates. Filtering above an optional Expand would discard
the required NULL-extended row when no endpoint satisfies the properties. Runtime
guards cover rows and counts with matching, rejected, isolated and missing sources,
including one-WITH and existing multi-WITH paths.

Newly migrated shapes use structural logical-plan normalization. Queries that
already used the public multi-WITH pipeline retain their existing logical
representation, EXPLAIN shape and exact-value cache behavior. WITH windows remain
at the WITH boundary; RETURN windows remain after lookup/aggregation. See
[WITH semantics](CYPHER_WITH_SEMANTICS.md) and
[cache eligibility](CYPHER_PLAN_CACHE_ELIGIBILITY.md).

## Frozen corpus qualification

The original 1,427 queries, their parameters, 94 Mem source hashes and historical
outcome manifest remain unchanged. Tests compare every record with its frozen
outcome or an explicit contract in
[`migration_default_pipeline_v1.json`](../crates/cypher/fixtures/migration_default_pipeline_v1.json).

Two parser-to-binder transitions are intentional:

- `mem-0344`: the grammar retains the filtered OPTIONAL relationship. Missing
  `$memory_id` still rejects at binding. Supplying it permits the read; embedded
  tests protect the relationship filter, null extension, count zero, missing
  source, row bound and fresh parameter rebinding.
- `mem-0361`: the grammar retains the multiple MATCH patterns and SET assignments.
  Binding rejects the unsupported atomic mutation with or without parameters.
  Embedded tests verify the same rejection on empty/populated stores and prove
  both nodes remain unchanged.

All other frozen parser rejections retain their stage. Historical accepted and
rejected counts remain checked separately from the two current transitions;
none of the frozen records is skipped. The 1,327 historically accepted core
query shapes remain covered by the clause grammar.

Of 371 applicable logical goldens, 369 retain their exact representation. The
complete current plans for `probe-0040` and `probe-0042` are separately pinned:
these are the equivalent keyset comparison and infallible RETURN projection
placements documented in WITH semantics. Exact plan pinning supplements runtime
result regressions and does not permit restoring the incorrect group keys or
pre-lookup RETURN limit.

## Verification boundary

Parser/planner corpus checks and the deterministic parser backtracking campaign
protect syntax and plans. Embedded runtime, portable runtime, required local
fuzz and strict lint checks must pass on the final delivery revision; passing an
individual layer does not establish completion of the migration or issue.
