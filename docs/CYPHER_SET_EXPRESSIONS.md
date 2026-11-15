# Cypher SET expressions

HawDB accepts a general expression on the right-hand side of `SET` in
`MATCH ... SET`, with the semantics Kùzu (and Ladybug) give it (#1791):

```cypher
MATCH (e:Entity {id: $id})
SET e.kind = CASE WHEN e.kind IS NULL OR e.kind = '' THEN $kind ELSE e.kind END,
    e.score = e.score * 0.5 + $delta,
    e.label = e.name + ' ' + $suffix,
    e.flag = e.rank > $threshold
RETURN e.score AS score

MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity)
WHERE (a.id IN $ids OR b.id IN $ids) AND r.space = $from
SET r.space = CASE WHEN a.space <> $from THEN a.space ELSE b.space END
```

## Planning

The single-pattern SET commands (`SetNodeProperties`,
`SetNodePropertiesReturn`, `SetRelationshipProperty`, ...) evaluate only
literals, parameters and a few fixed read-modify-writes of the written
property, such as `n.p + 1`, `coalesce(n.p, 0) + 1` and the clamped
decrement. Statements that use only those forms keep their plans.

A statement with any other SET value is planned as `MatchSetExec`:

- The clauses before `SET` become an ordinary read plan, optimized as its own
  root, that returns one column per variable in scope: the id of each node or
  relationship and the value of each scalar. Anonymous pattern nodes are not
  returned.
- Each SET item is planned as a projection expression over those variables
  and the statement parameters. A fixed form the single-pattern commands
  support on the written property keeps its historical semantics, so a
  statement behaves the same whichever plan it gets; for example `n.c + 1`
  still treats a missing `n.c` as `0`.
- `RETURN` items are projections over the same variables. Aggregation after
  such a `SET` is refused.

`EXPLAIN` shows `MatchSetExec` with its bindings, assignments and returned
columns above the read input.

## Execution

1. The read input runs against the statement's snapshot: the store of the
   autocommit statement, or the working store of the explicit transaction.
   Every row may write, so more input rows than `max_mutation_affected_rows`
   refuse the statement.
2. Each node or relationship a row binds is read by id once.
3. SET items are applied in statement order. One item is evaluated for every
   row before it is applied, against the values the earlier items left.
   Within a row this is the sequential openCypher order
   (`SET n.a = n.a + 1, n.b = n.a` gives `n.b` the new `n.a`). Across rows of
   the same entity, every row of an item reads the value before that item, as
   in Kùzu: `MATCH (a), (b) SET a.n = a.n + 1` adds one, not one per row.
4. `RETURN` projects every row after all items were applied.
5. The writes are committed as one mutation of per-entity values. Nothing is
   written before every row evaluated, so a failing row leaves no effect.

A `SET` whose target variable is `NULL` (an unmatched `OPTIONAL MATCH`) is a
no-op. A statement that matches no row writes nothing.

## Value semantics

The expression evaluator is the one `RETURN` uses:

- Integer arithmetic stays integral (`7 / 2 = 3`); an integer and a float
  give a float. Overflow and division by zero are errors.
- A numeric operator with a `NULL` operand gives `NULL`.
- `+` concatenates two strings. Like Kùzu, a `NULL` operand of a string
  concatenation reads as the empty string: `NULL + 'x'` is `'x'`. Two `NULL`
  operands give `NULL`, since HawDB has no static string type to infer.
- A comparison gives a boolean, or `NULL` when an operand is `NULL`.
- Mixing types that have no operator, such as a string and an integer, is an
  error for the whole statement. Kùzu rejects it when binding the statement.

Properties declared with a schema type are still validated on write, as are
`NOT NULL` and uniqueness constraints; Kùzu instead casts a value to the
column type (a float written to an `INT64` column is rounded). Declared
`DEFAULT` values still apply only when a record is created.

## Storage and change capture

The mutation is `GraphMutation::SetEntityProperties`: ordered property writes
per node or relationship id. It produces the same `SetNodeProperty` and
`SetRelationshipProperty` WAL records as the existing SET commands, so
recovery, constraint validation, the change feed and search projections see
these updates like any other SET. It adds no WAL or on-disk format.
