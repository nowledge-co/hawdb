# Row goals in graph query costing

Small pages can stop a streaming producer before it exhausts its input. The
graph cost model estimates this work from the full output cardinality and a
startup/variable-work split. A limit asks for `OFFSET + LIMIT` input rows using
saturating arithmetic; `LIMIT 0` consumes no rows, including its offset.

For example, a label scan of 100,000 rows followed by a filter with 1% estimated
selectivity needs about 2,000 input rows to produce 20 results. The scan's startup
cost remains. A projection, column lookup, adjacency probe, optional degree or
streaming expansion retains its child's startup and scales variable work by
the requested fraction of its full output. Expansion estimates include fanout.
Nested windows retain skipped work and cannot consume beyond their inner window.
The existing cardinality floor of one remains an estimate convention.

Sort, TopN, aggregation, distinct, hash joins, candidate unions and opaque
retrieval producers retain full work before producing output. In particular,
an exact top-K scan or ANN candidate limit never becomes a proportional corpus
scan estimate. Unknown operators take this conservative blocking rule.

The profile is computed in the existing single postorder costing traversal.
It adds no metadata traversal or per-operator allocation. A bounded ordered
composite alternative is compared with the cheaper of Sort and TopN over the
same projection-fused competing producer. A prefix discount can select an
ordered alternative only if its residual is absent or consists solely of exact
equality-prefix constraints, true constants, or the exact strict keyset cursor
already enforced by the native seek. Cursor bounds, ordered suffix and direction
must match; additional rejecting residuals still require full work. Otherwise
that alternative retains full work for selection: NDV cannot prove that rejecting matches are evenly
distributed in index order. `LIMIT 0` is safe regardless of distribution.
The same projection-fused comparison also applies to unbounded ORDER BY. A
prefix-only ordered path then consumes its full estimated output, reports
`row_goal=None` and receives no limit discount.
Offline qualification can perturb only candidate ranking, including the
Sort/TopN competitor choice; consumed-work reports retain their raw estimates.
Existing ordering, type, uniqueness, capability and directive guards
continue to determine which alternatives are legal.
Keyset implication relies on the ordered capability's enforced homogeneous suffix
types; arbitrary mixed-type total ordering alone does not prove a typed residual.

This is costing context, not a new physical property or runtime budget. The
graph memo stores logical expressions and does not cache a best cost by group;
each bounded alternative is costed with its own window. There is no graph
memo best-plan entry that can accidentally reuse an unbounded cost for a bounded
request. Relational required-property memoization is unchanged. Physical cache
binding retains its existing window-value guards and invalidation rules.

The selected root's `selected_plan_cost_breakdown` in explain/query reports
contains the consumed-work estimate; child cardinalities keep their full
estimates for selectivity and diagnostics. A selected ordered alternative also
reports `row_goal`, `ordered_cost` and `unordered_cost` in optimizer decisions.
Cost units are relative planner weights, not elapsed time, bytes or a promise
of an exact number of source reads.

Uniform distribution/fanout is an assumption. Correlated or clustered matches
can require much more input, including the full source. Row goals do not change
the executor, parameter semantics, ordering/ties, cancellation, result budgets,
query memory ceilings or hard work limits. A budget failure remains an error;
an estimate cannot justify a successful partial page.

A rejected prototype used average residual selectivity to select an ordered
path. With one million rows, 100 kind values, and the selected kind clustered
at the end, it visited 990,020 candidates for 20 outputs instead of using the
10,000-entry kind seek plus TopN. The final selection guard deliberately
forgoes that prototype's uniform-distribution gain. Existing ordered plans can
still be chosen on full cost; the guard prevents this new discount from
introducing the observed regression. General cardinality statistics and their
existing limitations remain separate from this consumed-work model.

See [test ownership](TEST_INDEX.md) for the existing regression owners. The
manual `row_goal_planning` benchmark compares complete embedded calls, selected
plans, source-work reports and admitted peak memory over uniform and clustered
cohorts. It is not a CI performance gate or a device-cold storage benchmark.
The [source-bound Apple Silicon experiment](benchmarks/row_goal_costing_macos_2026_10_31/README.md)
establishes the selection boundary and consumed-work report change; its final
matrix does not establish a query-latency improvement.
