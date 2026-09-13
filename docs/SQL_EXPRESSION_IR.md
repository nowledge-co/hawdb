# Shared SQL expression IR

Issue [#156](https://github.com/nowledge-co/skein/issues/156) separates expression
representation from syntax acceptance and index matching. The query entrypoints
remain `parse_postgres_sql`, `prepare_postgres_sql`, and the existing database
query APIs. This refactor preserves the production SQL language profile;
HAVING, additional FROM items, arbitrary ORDER BY expressions, and arithmetic
predicates remain separate language work.

## Representation and source migration

`skein_expression::sql::Expr { kind, span }` is the single owned SQL tree.
The `skein_sql` facade reexports it. `SqlExpression` and `SqlPredicate` are
aliases for `Expr`, preserving import names but not old enum constructors or
patterns. Consumers match `expression.kind` using `ExprKind`.

| Previous source form | Shared IR form |
| --- | --- |
| `SqlExpression::Column(column)` | `Expr::column(column)` |
| `SqlExpression::Value(value)` | `Expr::value(value)` |
| `SqlPredicate::Compare { left, op, right }` | `ExprKind::Compare` with boxed column and value expressions |
| `SqlPredicate::CompareColumns { left, op, right }` | The same `ExprKind::Compare` with two column expressions |
| `SqlPredicate::IsNull { column, negated }` | `ExprKind::IsNull { expression, negated }` |
| `SelectProjection::Column { name, alias }` | `SelectProjection::Expression` containing a column expression |
| `SqlOrderItem.column` | `SqlOrderItem.expression` |
| `CreateIndexStatement.columns: Vec<SqlOrderItem>` | `Vec<SqlIndexColumn>`, retaining column, direction, and null ordering |

Function arguments, aggregate FILTER, boolean operators, IN lists and LIKE
operands all contain the same expression type. Wildcard projection/arguments
remain explicit syntax forms. Group keys, mutation assignments, bounds and
index DDL retain their existing restricted contracts. Index DDL does not gain
expression-index semantics by sharing a query-order type.

`Expr::unspanned` constructs synthetic expressions. `visit` walks the tree in
preorder, including function arguments and FILTER. `try_visit_mut` visits
children before their parent and stops on an error; preceding edits are not
rolled back. Binding and qualification work on owned clones and preserve source
metadata while changing value or column nodes. Parameter discovery uses the
same traversal across projection, joins, selection and ordering, retaining
separate LIMIT/OFFSET and mutation validation.

## Source provenance

`SqlSourceSpan` records the contributing token range reported by sqlparser.
Locations are one-based line/character columns; the end is exclusive. Zero
locations mean no source information. These ranges are not guaranteed to cover
every syntactic token: parentheses, NOT and IS NULL may inherit child ranges.
They must not be treated as exact UTF-8 byte slices.

Structural `Expr` equality includes spans. Optimizer matching compares column
references and `SqlValue` leaves, so repeated cursor parameters at different
source locations still denote the same keyset value. Template cache entries
remain parameter-neutral and keyed by the original SQL text. Synthetic AND
nodes created by join planning have unknown spans and retain their original
children's provenance. Expression memory accounting includes the larger tree
and its owned children.

## Execution and index selection

Sargability extraction lives in `skein_optimizer::relational_sargability` and
recognizes the existing bare-column/value equalities, column join equalities,
and strict two-column keyset cursor. Arbitrary expressions remain residuals;
OR/NOT boundaries, duplicate constraints and null-order checks are preserved.
Lock planning retains its independent AND intersection and OR union rules.

The shared tree does not merge execution semantics: relational predicates keep
three-valued logic and existing type coercion; system predicates keep their
existing boolean behavior. Mutation compilation still rejects column-to-column
comparisons and LIKE/ILIKE. Null-rejection proofs remain a separate abstract
optimizer representation, not an executable expression tree.

## Verification contract

The existing 981-case frontend corpus keeps its SQL bytes, outcomes, parameter
positions, IDs and waivers. Source inventory changes update only reviewed
locations and source hashes, including the creation-binder fixture's reference
to that corpus. Tests additionally cover multiline and Unicode locations,
FILTER traversal, cached template isolation, binding provenance, syntax
boundaries, and keyset matching across distinct source spans. Existing query,
index-selection, lock, streaming, aggregate and EXPLAIN tests remain the
behavioral oracles.
