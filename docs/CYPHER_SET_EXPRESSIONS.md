# Cypher SET expressions

HawDB accepts a general expression on the right-hand side of `SET` in
`MATCH ... SET` (#1791), read pipelines such as `WITH` before `SET`, and
`DISTINCT`, `ORDER BY`, `SKIP` and `LIMIT` after `SET ... RETURN` (#1795), with
the semantics Kùzu (and Ladybug) give them:

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

MATCH (g:Group {id: $id}) WHERE g.key = $key
WITH g
MATCH (m:Member) WHERE m.group_id = g.id
SET m.space = $space
RETURN m.id ORDER BY m.id LIMIT 10
```

## Planning

The single-pattern SET commands (`SetNodeProperties`,
`SetNodePropertiesReturn`, `SetRelationshipProperty`, ...) evaluate only
literals, parameters and a few fixed read-modify-writes of the written
property, such as `n.p + 1`, `coalesce(n.p, 0) + 1` and the clamped
decrement. Statements that use only those forms keep their plans.

A statement is planned as `MatchSetExec` when it has any other SET value,
when the clauses before `SET` include `WITH`, `UNWIND`, `OPTIONAL MATCH` or a
named path, or when its `RETURN` has `DISTINCT`, `ORDER BY`, `SKIP` or
`LIMIT`. Only further `SET` clauses and one final `RETURN` may follow:

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
- `ORDER BY` keys read a returned column by its name, which shadows a
  variable of the same name, or any variable of the row, such as a property
  that is not returned. After `RETURN DISTINCT` the keys must name returned
  columns.

`EXPLAIN` shows `MatchSetExec` with its bindings, assignments and returned
columns above the read input.

## Execution

1. The read input runs against the statement's snapshot: the store of the
   autocommit statement, or the working store of the explicit transaction.
   Every row may write, so more input rows than `max_mutation_affected_rows`
   refuse the statement.
2. Each node or relationship a row binds is read by id once.
   The input rows, these entity copies, every computed value, the RETURN
   rows and the values of the final mutation are charged to one account of
   the statement's query memory (`ExecutionMemoryConfig.query_memory_bytes`
   or the runtime grant), separate from the read input's own account.
   Exceeding it refuses the statement with `ResourceBudgetExceeded` before
   anything is written (#1801).
3. SET items are applied in statement order. One item is evaluated for every
   row before it is applied, against the values the earlier items left.
   Within a row this is the sequential openCypher order
   (`SET n.a = n.a + 1, n.b = n.a` gives `n.b` the new `n.a`). Across rows of
   the same entity, every row of an item reads the value before that item, as
   in Kùzu: `MATCH (a), (b) SET a.n = a.n + 1` adds one, not one per row.
4. `RETURN` projects every row after all items were applied. `DISTINCT`
   keeps the first row of each distinct result, then `ORDER BY` sorts stably
   (equal keys keep the input order) and `SKIP`/`LIMIT` select the returned
   rows. Without `ORDER BY` and `DISTINCT`, rows past `SKIP + LIMIT` are not
   projected.
5. The writes are committed as one mutation of per-entity values. Nothing is
   written before every row evaluated, so a failing row leaves no effect.

A `SET` whose target variable is `NULL` (an unmatched `OPTIONAL MATCH`) is a
no-op; the row is still returned. A statement that matches no row writes
nothing.

### ORDER BY and LIMIT after SET

`ORDER BY`, `SKIP` and `LIMIT` after `SET ... RETURN` order and limit the
returned rows only; the update applies to every matched row. This is what
Kùzu does, checked on Ladybug:
`MATCH (e:E) SET e.a = 10 RETURN e.id ORDER BY e.id LIMIT 1` returns one row
and sets `a` on every `E`. To update only some rows, limit the rows before
`SET`: `MATCH (e:E) WITH e ORDER BY e.id LIMIT 1 SET e.a = 10` updates one.
Sort keys read the values after the update.

## Relationship CREATE and MERGE from rows

A relationship `CREATE` or `MERGE` between two nodes bound by earlier clauses
takes the same plan when the two-endpoint commands decline its read clauses:
`IN` lists, pattern predicates such as `NOT EXISTS { MATCH ... }`, `UNWIND`
rows, `WITH`, `OPTIONAL MATCH`, more than two node patterns, or the same node
at both ends (#1790):

```cypher
MATCH (o:Owner {id: $owner}), (i:Item) WHERE i.id IN $items
MERGE (o)-[r:HAS]->(i) ON CREATE SET r.created = $now

MATCH (o:Owner {id: $owner}) UNWIND $items AS item MATCH (i:Item {id: item})
CREATE (o)-[:HAS {key: item}]->(i) RETURN i.id
```

- The pattern is one directed relationship of one type between two bound
  nodes without labels or properties in the pattern; new endpoint nodes are
  not created from rows. An incoming pattern is written as its outgoing form.
- For every input row, in order, the property map and `ON CREATE SET` values
  are evaluated over the row. `CREATE` writes one relationship per row.
  `MERGE` reuses a relationship of the type between the two nodes whose
  properties include the map, including one an earlier row of the same
  statement created, and creates one otherwise (Kùzu, checked on Ladybug).
- A row with a NULL endpoint (unmatched `OPTIONAL MATCH`) writes nothing.
- `ON MATCH SET` on a relationship `MERGE` runs through the two-endpoint
  command (#1793) but is not evaluated over rows: such a `MERGE` fed by a
  general read is refused.
- `RETURN` projects the matched variables of each row; the created
  relationship is not in its scope.
- All rows commit as one batch of the existing two-endpoint create and merge
  commands, addressed by node id, so constraints and limits apply as for
  them. Ladybug's self-loop `MERGE` neither returns nor creates a row; HawDB
  merges the loop like any other pair.

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
