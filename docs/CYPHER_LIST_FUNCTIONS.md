# Cypher list functions and list comprehensions

HawDB's Cypher scalar expressions support the openCypher list operations used
by LDBC SNB Interactive style queries:

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
WITH p, collect(f) AS friends
WHERE size(friends) > $min
RETURN p.id AS id,
       size(friends) AS total,
       head([x IN friends WHERE x.age > 30 | x.name]) AS older,
       [x IN friends | x.name][-1] AS last_name
ORDER BY total DESC
```

## Functions

| Function | Result |
| --- | --- |
| `size(list)` / `size(string)` | Element count / Unicode scalar count (`Int`). |
| `head(list)` | First element, or `NULL` for an empty list. |
| `last(list)` | Last element, or `NULL` for an empty list. |
| `tail(list)` | The list without its first element; `[]` for an empty list. |
| `reverse(list)` / `reverse(string)` | Reversed list / reversed Unicode scalars. |
| `range(start, end[, step])` | Integers from `start` to `end` inclusive. |
| `coalesce(a, b, ...)` | First non-`NULL` argument (existing function). |

Function names are case-insensitive. A `NULL` argument returns `NULL`,
including for `range()`. Wrong argument counts are parse errors; arguments of
the wrong type (for example `head('text')`) fail the query.

`range()` accepts integers only. `step` defaults to `1`, must not be `0`, and
may be negative for a descending range. A range whose direction does not reach
`end` (for example `range(5, 1)`) is empty. One call may produce at most
1,000,000 elements (`hawdb_core::MAX_RANGE_LENGTH`); larger ranges fail before
any allocation. The query memory budget applies in addition.

Within one evaluated row, list expressions may spend at most 10,000,000 work
units (`hawdb_core::MAX_LIST_EXPRESSION_WORK`): elements produced by `range()`,
list comprehension iterations, elements copied by `tail`, `reverse` and slices,
elements compared by `IN` (including elements of nested lists), and bytes
scanned by string `size` and `reverse`. Nested comprehensions can filter
everything they visit without retaining memory, so this work limit, not the
memory budget, bounds them; exceeding it fails the query.

Lists are charged to the query memory account. Where an evaluation has no
account (aggregate arguments, mutation preflight), the bytes list expressions
materialize in one evaluation may not exceed 64 MiB
(`hawdb_core::MAX_UNACCOUNTED_LIST_BYTES`); exceeding it fails the query.

## List comprehensions

`[x IN list]`, `[x IN list WHERE predicate]`, `[x IN list | expression]` and
`[x IN list WHERE predicate | expression]` are supported, including nested
comprehensions. A `NULL` list returns `NULL`. Elements whose predicate is
`false` or `NULL` are dropped; a non-boolean predicate fails the query.

`x` is visible only inside the brackets. It must not reuse the name of a
variable or column that is already in scope (including an enclosing
comprehension's variable); such a query is rejected with
`list comprehension variable 'x' shadows a variable that is already in scope`.
`x.key` reads a key of a map element (for example nodes produced by
`collect(n)`), returning `NULL` for a missing key or `NULL` element. Graph
functions such as `id(x)` and `label(x)` are not available for comprehension
variables.

The predicate uses the CASE condition grammar (comparisons, `AND`, `OR`,
`NOT`, `IS [NOT] NULL`, `CONTAINS`, `IN`, `list_contains`). The projection is a
scalar expression (properties, literals, parameters, functions, `CASE`, nested
comprehensions, indexing). Arithmetic is not part of the scalar grammar, so
`[x IN xs | x * 2]` is not yet supported.

## Indexing and slicing

`list[i]` returns the element at `i`; a negative index counts from the end
(`list[-1]` is the last element). An out-of-range index returns `NULL`.
`list[a..b]` returns elements `a` (inclusive) to `b` (exclusive); either bound
may be omitted, negative bounds count from the end, and bounds are clamped to
the list. A `NULL` list, index or bound returns `NULL`; non-integer indexes
fail the query. Map access by key (`map['key']`) is not supported.

## `IN`

`x IN list` follows openCypher three-valued logic: `true` when an element
equals `x`; otherwise `NULL` when `x` is `NULL` (and the list is not empty) or
some comparison involved `NULL` (including inside nested lists); otherwise
`false`. `x IN []` is `false`, and `x IN NULL` is `NULL`. `WHERE` keeps only
`true` rows, so `NOT x IN [1, NULL]` filters out every `x <> 1`.

Constant lists and parameters on the right side (`n.id IN [1, 2]`,
`n.id IN $ids`) keep the existing value-list predicates. Any other list
(`n.id IN tags`, `$id IN n.ids`, `country IN [countryX, countryY]`) uses the
generic operator, which `WHERE` compares with `true` internally.

## List literals

A list literal may hold any scalar expression: variables, properties,
parameters, literals, functions, `CASE`, conditions and nested lists, for
example `[n.name, r.classYear, m.name]` or `[a, b]`. A list of only literals
and parameters stays a constant value, so it keeps the value-list predicates
and index pushdown above. Any other list literal is evaluated per row, in
element order; a missing OPTIONAL MATCH value is a `NULL` element. Node and
relationship elements compare as `n = m` does, so `x IN [a, b]` matches by
identity.

## Where expressions are accepted

The functions, comprehensions and indexing are scalar expressions. They work
in `RETURN` and `WITH` items, in `WHERE` comparisons (`size(xs) > 1`,
`xs[0] = 'a'`, `x IN xs`), in `ORDER BY`, inside `CASE`, `coalesce` and each
other. A list function may also wrap an aggregate directly, for example
`size(collect(x))`, `head(collect(x))` or `size(collect(x)) + 1`; the function
is applied after aggregation. Other expressions over aggregates still require
a `WITH` boundary, for example
`WITH collect(x) AS xs RETURN [v IN xs WHERE v > 1]`.

## Planning, caching and memory

Comprehension variables are planned as expression-local values, not row
columns, so optimizer column and property analyses ignore them while still
collecting properties of outer variables referenced inside comprehensions.
Plan fingerprints include each list expression kind and its operands.
Parameters inside list expressions are detected by the plan cache and keyed
exactly, like other parameters in generic scalar expressions; changing such a
parameter plans again instead of reusing a stale template.

Every newly built list (`range`, `tail`, `reverse`, slices, comprehension
results and row-evaluated list literals) reserves query working memory for its header, element slots and
copied payloads before the elements are stored. Results derived from an
already computed list reuse that list's reservation.

## Pattern predicates and pattern comprehensions

A pattern is a boolean expression wherever a condition is accepted: in
`WHERE`, with `NOT`, `AND` and `OR`, in `CASE WHEN` and in comprehension
predicates (LDBC IC10's
`[p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]`). A
`RETURN`/`WITH` item can negate one in function-call form, as in LDBC IC7's
`not((liker)-[:KNOWS]-(person)) AS isNew`. A bare `NOT` remains a boolean
connective, so `1 + NOT x` and `a = NOT b AND c` are rejected. A pattern is
an existential subquery over the whole pattern: true when the pattern has at
least one match, otherwise false. As in openCypher, a bare pattern is not a
value, so `RETURN (a)-->(b) AS x` and `[(a)-->(b), 1]` are rejected.

`[pattern WHERE predicate | projection]` is the list of `projection` over the
matches of `pattern` that satisfy the optional `predicate` (only `true`
keeps a match). Its element order is unspecified. LDBC IC14 nests one inside
`reduce` inside a list comprehension over `relationships(path)`.

Both run as a correlated MATCH, through the same runtime as a MATCH clause,
once per evaluation. A pattern variable that the enclosing scope binds
(a row variable, a column such as a node carried through `WITH`, or a list
comprehension or `reduce` element) correlates with it; any other variable,
and every anonymous node, is introduced by the pattern and visible only
inside it. Relationships are unique within the pattern, as in MATCH, and
bounded variable-length relationships are supported as in MATCH. Matching
starts at the first bound node, so `[(p:Post)-[:HAS_CREATOR]->(a) | p]`
expands from `a` rather than scanning posts. Path variables, shortest paths
and unbounded variable-length relationships are rejected inside pattern
expressions. Row-dependent node properties (`(b {id: a.id})`) become
predicate conjuncts.

A node or relationship bound to NULL (an unmatched OPTIONAL MATCH) matches
nothing: the predicate is false, so `NOT pattern` is true, and the
comprehension is `[]`. The openCypher TCK has no scenario for this case.
HawDB uses the semantics of the equivalent `EXISTS { MATCH ... }` subquery.
A Neo4j staff member called a NULL pattern comprehension result for a NULL
outer variable a bug
([community thread](https://community.neo4j.com/t/pattern-comprehension-possible-bug-handling-null-variables/19549)).

`WHERE (x)-[:R]->()` over a node the clause binds natively uses the one-hop
adjacency check; over a node value carried by `WITH` (for example an UNWIND
element) it runs as the pattern expression, so both spellings agree.

`startNode(r)` and `endNode(r)` also accept relationship values, such as the
elements of `relationships(p)` or a relationship carried through `WITH`. The
value must name a stored relationship with the same endpoints; any other map
fails rather than being read as a relationship. The stored source or target
is then read from the graph, a NULL value returns NULL, and a relationship
deleted since the value was read fails as MATCH does for a bound node that
disappeared.

Under a node access-control policy, every pattern program nested in an
expression admits only visible nodes, as a MATCH clause does, and an
endpoint the policy hides reads as NULL.

The produced lists, imported values and records read are charged to the
query memory account (in mutation preflight, the statement's account), every
candidate node or partial match a pattern visits is a list work unit, so a
pattern that scans is bounded even when nothing matches, and evaluation
checks the task's cancellation between candidates. `ORDER BY` keys are planned
over the projected names first, so an alias wins over an input variable of
the same name; a key that reads the graph is computed into a column before
sorting.
Plan-derived prepared source reads refuse pattern expressions, whose reads
their dependency collector cannot see.
