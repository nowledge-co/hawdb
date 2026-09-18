# SQL HAVING and multiple FROM items

This extends the explicit CROSS JOIN slice of [#157](https://github.com/nowledge-co/hawdb/issues/157)
on the expression IR from [#156](https://github.com/nowledge-co/hawdb/issues/156).
Existing embedded SQL entrypoints execute comma-separated base tables and HAVING.
The duplicate-result-name requirement remains open, as recorded in the
[result-column audit](SQL_RESULT_COLUMNS_CONTRACT_AUDIT.md).

## FROM binding

`SqlJoin.on_scope_start` records the first visible input of each ON clause; the
base relation is ordinal zero and the current right input ends the scope.
Explicit joins in the first FROM item use zero. Each comma item starts a new
scope, including its synthetic INNER/TRUE join to the preceding product. Thus
`a CROSS JOIN b JOIN c ON a.id = c.id` and
`a, b JOIN c ON a.id = c.id` retain different binding scopes. Schema binding
rejects the latter before reading any rows, including when the base is empty.

Binding qualifies references against the actual table aliases before the join
optimizer runs. Unqualified ON columns resolve only within that item's chain;
forward references, hidden table names and duplicate aliases fail. WHERE and
output expressions use the complete FROM scope. Existing LEFT JOIN null extension
and explicit-chain order are preserved. Optimizer-generated joins use scope zero
after their predicate references have been bound.

The public AST adds `SqlJoin.on_scope_start: usize` and
`SelectStatement.having: Option<SqlPredicate>`. Complete struct literals need
these fields; existing single-FROM constructors use zero and no HAVING uses None.
The query request/response types and storage formats do not change.

## HAVING binding and execution

HAVING filters completed groups before OFFSET, LIMIT and output admission. With
no GROUP BY it creates one implicit group, even on empty input or without a visible
aggregate. An explicit GROUP BY over empty input produces no groups. Only TRUE
passes; FALSE and SQL UNKNOWN are excluded.

Visible projections and HAVING operands are checked against the current schema
before scanning. A non-aggregate column must be grouped or functionally determined
by all columns of its relation's primary key. Output aliases are not input-column
bindings. Aggregate arguments, FILTER inputs, scalar types, missing columns and
unsupported nested aggregates are checked on empty inputs as well.

HAVING uses private value slots and existing aggregate states, independently of
output names. A typed group-filter state is included in existing aggregate memory
accounting. Hidden DISTINCT sets, retained scalar values and finish-time value
scratch remain subject to that budget. The filter shares the row predicate
implementation for three-valued comparisons, AND/OR/NOT, IS NULL, IN and LIKE.
The compiled slots bind parameters for each execution; the cached source template
retains the original parameter references and source spans.

The supported aggregate profile is COUNT(* or column), SUM and MAX over the
existing column/literal/OCTET_LENGTH inputs, optional aggregate DISTINCT/FILTER,
and same-type COALESCE. HAVING scalar operands include grouped columns, literals,
parameters and these aggregate expressions. Numeric comparisons reconcile BIGINT
and DOUBLE PRECISION; text UUID constants use existing UUID conversion. This is
not a general PostgreSQL coercion, scalar-function, subquery or arithmetic layer.
The existing restrictions on statement DISTINCT/ORDER BY with aggregation remain.
Ordinary WHERE/ON expression profiles are unchanged. System-table and append-store
SELECT owners reject HAVING explicitly rather than ignoring it.

HAVING uses the general aggregate path; columnar and single-COUNT-DISTINCT shortcuts
do not bypass the group filter. Inputs referenced only by HAVING participate in
scan and hydration planning. COUNT and OCTET_LENGTH can use overflow metadata;
DISTINCT, MAX(body) and value-sensitive FILTER predicates still require hydration.
Grouped HAVING rereads admitted scan fields after sorting, including all output
state inputs, so the replay does not force metadata-only inputs to be hydrated.
Non-HAVING grouped replay retains its existing field contract.

EXPLAIN places a logical `SelectionExec` with `phase=having` above aggregation.
Its predicate includes hidden aggregate expressions, FILTER and DISTINCT. Aggregate
memory reports include the hidden filter state. Input-work, intermediate-row,
cancellation and output budgets retain their existing owners.

## Verification scope

Regression coverage includes comma-vs-CROSS precedence, alias visibility, empty
inputs and mixed LEFT chains; grouped/implicit HAVING, hidden aggregates, NULL
logic, parameter/cache rebinding, output pagination and resource rejection;
and repeated canonical row-page reopen with metadata-only overflow aggregation
and value-sensitive hydration failures. Parser source metadata and parameter
namespace checks include projection, WHERE, FILTER, HAVING and bounds together.

The differential frontend corpus has 1,007 cases. Two reviewed production
acceptance changes and thirteen additions are recorded in
[SQL frontend corpus](SQL_FRONTEND_CORPUS.md); frozen workload bytes remain intact.
Use the standard owner/root presubmits and all three local fuzz targets. No test
workload, timeout, Bazel configuration, persisted format or query entrypoint is
changed for qualification.
