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
and graph projection remain ineligible. `LOAD CSV` statements also parse into
`Statement::Pipeline`; see [LOAD CSV](CYPHER_LOAD_CSV.md). Procedure capabilities are checked
before parameter binding, planning, cache accounting or catalog mutation.
Statement kinds, fast-path reasons and ordering/pagination reports are derived
from the AST and preserve existing host reports.

Optional plans with target-node properties retain GraphMatch when Expand cannot
represent those predicates. Filtering above an optional Expand would discard
the required NULL-extended row when no endpoint satisfies the properties. Runtime
guards cover rows and counts with matching, rejected, isolated and missing sources,
including one-WITH and existing multi-WITH paths.

All read pipelines, including statements with two or more WITH clauses, use
structural logical-plan normalization, so their MATCH clauses can lower to index
seeks and native expands (issue #1425). Parameters that normalization moves into
a seek key, expand or filter remain template slots only when rebinding the marked
plan reproduces the actual plan; otherwise they remain exact cache guards.
A comma pattern that continues from a node bound earlier in the same MATCH,
such as `(a)-[:R]->(b), (b)-[:S]->(c)`, joins the same native chain when its
relationship types are fixed and distinct and the repeated node adds no label.
Conjuncts over variables an Expand does not bind move below it, so an anchor
equality still reaches its index seek under a longer chain.
Ranked evidence normalizes only its aggregate input. WITH windows remain
at the WITH boundary; RETURN windows remain after lookup/aggregation. See
[WITH semantics](CYPHER_WITH_SEMANTICS.md) and
[cache eligibility](CYPHER_PLAN_CACHE_ELIGIBILITY.md).

## Row-bound node properties

Read-only pipelines may start with a parameter or list-literal `UNWIND` source
and carry values through `WITH`. Both `MATCH` and `OPTIONAL MATCH` node property
maps accept scalar bindings such as `{id: candidate_id}` and property access
such as `{id: candidate.id}`. These constraints remain inside their MATCH clause,
preserving OPTIONAL MATCH null extension. Read-only UNWIND retains the existing
plan-cache bypass policy; template-rebinding tests do not establish public-query
cache hits for this shape.

The lookup normalization has a narrow boundary: an eligible single-node MATCH
introducing a fresh node variable, with no remaining constant properties and
exactly one equality between that node's property and an input scalar column,
can use `NodeColumnLookupExec`. The bare `{id: candidate_id}` form can qualify.

Shapes with additional constant properties, a residual `WHERE` condition,
property-access input values, or multiple pattern steps can fall back to
`GraphMatchExec`. For example,
`OPTIONAL MATCH (n:Item {id: candidate_id, space_id: $space})` scans the label for
each input row even when an index on `Item(id)` exists. Dynamic predicates are
evaluated after the pattern steps, so expansions can add work before rejecting a
candidate. Work can grow with input rows times label size, plus expansion work.

Inspect EXPLAIN before adopting extended shapes in a high-cardinality route and
keep candidate batches bounded. Row/payload budgets and cancellation still
apply, but do not make a scan use an index. Moving a predicate above OPTIONAL
MATCH to change its plan can discard the unmatched row and change the result.

## Bounded relationship mutation input

The atomic mutation binder accepts two independently bound, non-optional node
endpoints with one optional parameter/list `UNWIND` before relationship `MERGE`.
For example, the Mem candidate-evidence writer can use:

```cypher
MATCH (s:Skill {id: $id})
UNWIND $ids AS memory_id
MATCH (m:Memory {id: memory_id})
MERGE (s)-[r:SYNTHESIZED_FROM]->(m)
ON CREATE SET r.weight = 1.0, r.occasion_key = $occasion_key, r.created_at = $now
RETURN m.id
```

Endpoint equalities may use pattern maps or AND-connected WHERE predicates.
Scalar UNWIND elements and map-element properties are bound only after their
row variable is introduced. A final RETURN projects the existing endpoints;
relationship projections, aggregates, result modifiers, additional mutation
clauses, optional matches and correlated graph traversals remain refused.
The same endpoint RETURN is available without UNWIND. This does not enable
general-purpose read-only UNWIND composition.

Lowering produces the existing `UnwindMutationExec` with a typed matched-edge
operation. It resolves the complete bounded input before submitting one storage
batch; it does not run one committed query per row. Input duplicates retain
result multiplicity while MERGE reuses the same relationship. Missing or NULL
endpoint equalities produce no rows or invented nodes. ON CREATE changes only
new relationships. Mutation plans remain excluded from parameter-template reuse.

Endpoint keys are resolved once per `(label, property names)` pattern against
that statement's store, including earlier writes in an explicit transaction.
Resident scalar indexes are used when available; other patterns use one admitted
projected scan. The lookup map and rewritten ID filters obey the host/runtime
query memory budget. The complete pair count is checked before copying per-row
ID lists; an empty pair retains no IDs. Storage rechecks labels and non-ID
predicates by point lookup, without repeating label scans or ID membership
searches for each candidate. Uniform MERGE batches index pending edges by
source/target/type, retaining property-subset matching and input multiplicity.
NULL relationship match properties are refused before any write.

Node-only RETURN values are independent of the new edge. Autocommit preflight
reuses the resolved endpoint IDs and checks output limits before the
atomic write. Explicit transactions project the staged result under the existing
statement savepoint; output rejection restores that statement while retaining
earlier statements. Ordinary rollback and WAL/reopen boundaries remain unchanged.
This supplies exact member rows to a host count guard; it does not silently
accept a partial match or implement Mem's complete Skills adapter/activation.
Tracked in [#1397](https://github.com/nowledge-co/hawdb/issues/1397).

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
