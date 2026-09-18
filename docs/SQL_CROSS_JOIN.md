# Explicit SQL CROSS JOIN

This document records the first slice. The current combined implementation is
described in [SQL SELECT support](SQL_SELECT_SUPPORT.md).

This is the first execution slice of [#157](https://github.com/nowledge-co/hawdb/issues/157),
built on the shared expression IR from [#156](https://github.com/nowledge-co/hawdb/issues/156).
It adds explicit `CROSS JOIN` between supported base tables through the existing
embedded SQL entrypoints.

## Representation and execution

The parser lowers an unconstrained CROSS JOIN to `SqlJoinKind::Inner` with a
synthetic Boolean TRUE expression. The private relational predicate evaluator
accepts that Boolean literal. This uses existing public AST types and does not
change query request/response types or persisted formats. Existing result-column
names must remain unique; use projection aliases when joined tables share names.

The parser requires `JoinConstraint::None`: CROSS JOIN with ON or USING must not
silently discard a supplied constraint. Ordinary INNER/LEFT joins retain their
existing ON-expression profile. The change does not enable arbitrary scalar
predicates in WHERE, ON or mutation statements.

The optimizer's current requirement that every join conjunct reference at least
two bindings causes the constant predicate to retain syntax order. The existing
physical nested-loop path scans the right input for each left row, preserving
intermediate-row, candidate-work, memory, cancellation, row and payload admission. Reports retain
the syntax-order reason and physical nested-loop cardinalities. No unbounded
Cartesian executor or new join-search feature is introduced.

[PostgreSQL table expressions](https://www.postgresql.org/docs/18/queries-table-expressions.html#QUERIES-JOIN)
define this product as INNER JOIN ON TRUE. The supported flat explicit join chain
preserves left-to-right nesting. Following ON clauses can reference prior inputs;
LEFT JOIN null extension applies before or after the product according to syntax.

## Verification

Parser regressions cover lowering, shared parameter positions and unsupported
constraints/join forms. Execution regressions cover every pair of zero through
three input rows, NULL payloads, duplicate projected values, wildcard output with
unique column names, COUNT/DISTINCT, mixed INNER/LEFT chains, empty inputs, cache
reuse after data changes, output budgets, candidate/intermediate budgets,
cancellation and physical-plan observability. The frontend corpus retains the
original SQL bytes, outcomes and waivers and separately inventories the new parser
cases.

Use the normal owning-crate and embedded-query presubmits, with all three required
local fuzz targets. No CI configuration, workload or timeout changes are needed.

## Follow-up scope

The continuation implements comma-FROM scopes and HAVING through the existing
embedded SQL entrypoints. See [SQL SELECT support](SQL_SELECT_SUPPORT.md) for its
execution contract and [the result-column audit](SQL_RESULT_COLUMNS_CONTRACT_AUDIT.md)
for the remaining duplicate-name acceptance requirement in #157.
