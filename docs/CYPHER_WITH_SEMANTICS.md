# WITH identity and pagination boundaries

[Issue #757](https://github.com/nowledge-co/hawdb/issues/757) corrects two legacy
planning paths and a subsequent-aggregation window error discovered while auditing the clause-pipeline migration in
[issue #158](https://github.com/nowledge-co/hawdb/issues/158). This does not enable
the generic pipeline for every statement or finish the AST migration.

## Group first, project afterward

For `WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids`,
the grouping key is the node c. It is not the value of c.id. Neither the
property's spelling nor its use as an application identifier proves uniqueness.
Two distinct nodes may have equal values or both lack the projected property.

Let R be the multiset of matched rows. Define r ~ s iff their c bindings have
the same node identity. For each equivalence class E, compute the collection
from exactly E and then project the retained node's requested property.
The result has one row per class, including classes whose projected values
coincide. Grouping instead by p(c) uses a coarser equivalence relation whenever
p is not injective: c1 != c2 and p(c1) = p(c2) gives two original classes but
one substituted class. Their collections combine and one result row disappears.
The two-node regression is a concrete counterexample; NULL properties give
another. No such substitution is valid without a proved functional dependency
strong enough to preserve the original identity partition.

`plan_collect_with_match_return` now emits an Aggregate whose group projection
is the node variable, followed by a Project selecting `ColumnProperty` and the
collected column. The existing aggregate machinery represents node identity in
the retained value. RETURN aliases apply at this final projection, including
the alias of the collected result. This also uses the same grouping boundary as
the generic clause-pipeline planner.

## Keep windows on their source clauses

Let G be the ordered aggregate rows and J map each row to its matching lookup
rows. A required lookup can yield zero or many rows. An optional lookup yields
the matches, or one NULL-extended row if none exist. Extend J to sequences by
concatenation. Let W(o,k) select positions [o, o+k), with the usual exhausted
and zero-limit behavior, and let S apply the requested ordering.

A RETURN window following lookup computes W(S(J(G))). A window on WITH before
lookup computes J(W(S(G))). These are not generally equal:

- One group with two lookup matches: RETURN LIMIT 1 yields one row; WITH LIMIT 1
  followed by lookup yields two rows.
- A highest-ranked group with no required match: limiting that group before
  lookup may yield no rows, while lookup before LIMIT can select a lower-ranked
  matching group.
- SKIP counts positions in the sequence at its clause boundary; expanded rows
  are not interchangeable with groups.

The parser now retains `with_order_by`, `with_offset` and `with_limit` when a
post-WITH lookup or RETURN aggregate exists. `order_by`, `offset` and `limit` continue to describe
RETURN. The planner applies the WITH window before `NodeColumnLookup` and the
RETURN window after it. Existing restrictions on simultaneously specifying the
same window component in both clauses remain unchanged.

A subsequent global aggregate is another cardinality boundary. Let A(G) return
one row containing |G|, including when G is empty. For n groups and nonnegative
offset o and limit k, A(W(o,k)(G)) must return exactly one row with count
min(k, max(0, n-o)). Conversely, W(o,k)(A(G)) either retains or drops the one
count row and, if retained, its count is n. In particular, WITH LIMIT 0 followed
by RETURN COUNT(*) returns a zero count; RETURN COUNT(*) LIMIT 0 returns no row.
The planner therefore applies retained WITH windows before the second Aggregate,
while final RETURN windows remain above it. This also preserves WITH ordering
before group selection rather than treating it as an order on the final count.

This is a source-linked deductive argument, not a machine-checked proof of the
Rust implementation. It assumes the existing aggregate, lookup, sort and limit
operators obey their contracts. It makes no claim that order among equal sort
keys is defined without a query-specified tie breaker.

## Regression and migration evidence

The embedded aggregate regressions cover:

- distinct nodes with equal properties, through both the legacy path and a
  two-WITH clause-pipeline query, against an independent two-row expectation;
- NULL/missing properties and RETURN aliases;
- all 16 SKIP/LIMIT pairs from 0 through 3 on two lookup matches, both with and
  without a property index;
- the opposite WITH-before-lookup window, which must still return two rows;
- required lookup filtering and optional NULL extension before final LIMIT.
- all 16 SKIP/LIMIT pairs around a second COUNT aggregation, independently
  checking both WITH-before-count and RETURN-after-count placement.

The original code failed the equal-property grouping and final-LIMIT tests.
Parser tests separately pin which AST fields retain each clause's window.
Only migration goldens `probe-0042` and `probe-0050` change: they previously
encoded the defective semantics. The WITH-before-lookup `probe-0041` remains
unchanged. Query text, parameters, corpus membership and rejection expectations
are preserved.

The normalized pipeline now exactly matches 369 of the 371 applicable golden
plans. The remaining IDs are pinned explicitly rather than merely counting
differences: `probe-0040` uses a property comparison versus an equivalent scalar
comparison; `probe-0042` places its infallible RETURN projection on the other
side of Sort. Both planners now keep that query's LIMIT above the lookup.
These representation differences remain migration work, not authorization to
restore the incorrect plans.

Retaining node-valued group keys can use more memory than retaining one scalar
property, and applying final pagination after lookup may examine more rows.
The existing aggregate/sort/output budgets remain in force; silently combining
groups or limiting an earlier clause is not a valid resource fallback. No
schema, persistent format, production API or default budget changes are made.

Pipeline migration must also retain cache eligibility independently of WITH
semantics. The [cache eligibility proof](CYPHER_PLAN_CACHE_ELIGIBILITY.md)
classifies every clause and procedure: adding WITH clauses must not admit a
mutation or vector-search procedure that bypasses the legacy plan cache.
