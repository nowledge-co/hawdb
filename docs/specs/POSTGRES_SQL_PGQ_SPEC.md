# HawDB PostgreSQL SQL/PGQ Compatibility Specification

## Scope

HawDB supports graph queries through two language surfaces:

- a Cypher-compatible surface for existing embedded graph workloads; and
- PostgreSQL-dialect SQL, including the SQL/PGQ property-graph extension.

The PostgreSQL compatibility baseline is PostgreSQL master commit `3d00537f`.
The relevant PostgreSQL surface is ISO/IEC 9075-16 SQL/PGQ, expressed through
`CREATE PROPERTY GRAPH` and `GRAPH_TABLE`. It is not the standalone ISO/IEC
39075 GQL language. HawDB MUST describe this surface as PostgreSQL SQL/PGQ and
MUST NOT claim standalone GQL conformance from SQL/PGQ coverage.

This contract is independent from the scoped relational Content Store contract
in `POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`. SQL/PGQ is a general query
frontend over HawDB-owned graph and relational state; it is not a requirement
for the first SQLite Content Store cutover.

## Language And API Boundary

SQL/PGQ statements use the existing `Database::query_sql*`, preparation,
transaction, streaming, explain, and query-report surfaces. HawDB MUST NOT add
a route-specific API or a separate production `query_gql` entrypoint for this
compatibility layer.

Cypher and SQL/PGQ own distinct syntax ASTs. After binding, both MUST lower to
the same HawDB-owned typed expressions, graph logical operators, optimizer,
physical operators, snapshot, and resource admission. SQL/PGQ MUST NOT be
implemented by rendering Cypher text, and the executor MUST NOT inspect raw SQL
or syntax AST nodes.

The initial PostgreSQL SQL/PGQ surface consists of:

- `CREATE [ TEMP | TEMPORARY ] PROPERTY GRAPH`;
- `ALTER PROPERTY GRAPH` and `DROP PROPERTY GRAPH` only after create/query
  semantics are qualified;
- `GRAPH_TABLE (graph_name MATCH graph_pattern COLUMNS (...))` as a `FROM`
  item in PostgreSQL `SELECT`;
- ordinary PostgreSQL aliases, joins, filters, grouping, ordering, limits, and
  parameters around the tabular output of `GRAPH_TABLE`;
- directed and undirected vertex/edge patterns, element variables, label
  predicates, property predicates, and explicit output columns required by
  active workloads.

Unsupported SQL/PGQ syntax MUST fail during parsing or binding with a
structured error and source span. It MUST NOT silently fall back to Cypher or a
relational join interpretation with different semantics.

## Owned Syntax Frontend

`hawdb-sql-syntax` owns PostgreSQL-oriented lexical tokens, byte spans, syntax
errors, and syntax ASTs. It has no dependency on storage, planning, execution,
or the embedded facade. `hawdb-sql` owns semantic lowering and remains the only
SQL dependency exposed to the root crate.

Tokens and syntax nodes retain byte spans into the caller-owned SQL text rather
than cloning identifiers and literal payloads. Scalar expressions use a bounded
precedence AST recognized by PEG rules; semantic binding owns only normalized names and typed values that
survive past parsing. This keeps parse memory proportional to token and node
count while preserving exact source locations for diagnostics.

The owned statement envelopes use token PEG rules with ordered alternatives,
packrat caching of successes and failures, and a separate AST transformer.
SQL/PGQ composes into the statement and FROM-item rules before matching starts.
Expressions, graph patterns and property-graph definitions are recognized by
pure PEG rules; the recursive syntax parsers and native SQL callbacks are removed. See [SQL PEG parser](../SQL_PEG_PARSER.md)
for the resource and grammar-validation contract. This internal composition
mechanism does not establish a public loadable-dialect API.

The owned frontend MUST be implemented incrementally. Existing relational
statement families may continue to use upstream `sqlparser` until the owned
parser has equivalent positive and negative corpus coverage. Parser selection
MUST be explicit by statement family; retrying another parser after a syntax or
semantic failure is forbidden.

The first owned `SELECT` slice recognizes `GRAPH_TABLE` and qualified relations
as `FROM` items, bounded scalar expressions, `INNER`, `LEFT`, and `CROSS` joins,
projection, filtering, grouping, ordering, limits, offsets, and locking clauses.
It deliberately rejects subqueries, right/full/natural joins, join `USING`,
`DISTINCT ON`, and other unqualified shapes. The default relational SELECT and EXPLAIN SELECT paths now use the PEG
relational entry and lower into the existing relational IR, including constants,
vector search, aggregate FILTER and LIKE/ILIKE. Supported DDL/DML and
materialized-view statements now use an executable PEG grammar importing the
same expression/SELECT rules and lowering directly to HawDB IR. Branch commands
also use PEG. The upstream SQL parser and its live test oracle are removed;
independently exported frozen IR/error fixtures preserve migration coverage.
PGQ syntax nodes alone do not establish a production execution contract. An
owned-parser failure MUST be returned directly and MUST NOT trigger a retry
through the upstream parser.

PostgreSQL source is a grammar and semantic reference, not copied production
code. The implementation MUST preserve HawDB's Apache-2.0 licensing and MUST
NOT copy PostgreSQL C parser code or generated parser tables. PostgreSQL source
locations used as the initial reference are:

- `src/backend/parser/gram.y` for property-graph and `GRAPH_TABLE` grammar;
- `src/include/nodes/parsenodes.h` for raw syntax shapes;
- `src/backend/parser/parse_graphtable.c` and `parse_clause.c` for binding
  restrictions;
- `src/backend/commands/propgraphcmds.c` for catalog validation;
- `src/test/regress/sql/create_property_graph.sql` and graph query regression
  files for compatibility cases.

The lexer MUST provide byte-accurate spans, quoted identifiers, string and
numeric literals, dense PostgreSQL parameters, punctuation and operators,
line comments, and nested block comments. Arbitrary UTF-8 input MUST either
produce tokens or a structured error without panicking. Token, nesting, and
input limits MUST be configurable or bounded by the caller's parse budget.

The syntax corpus is adapted from PostgreSQL regression scenarios and records
the exact upstream revision and source files. HawDB may reduce and rename those
scenarios, but MUST preserve whether PostgreSQL accepts them in raw parsing or
rejects them later during graph binding. PostgreSQL output strings and C parser
implementation details are not part of the HawDB test contract.

## Exact numeric execution

Relational SQL supports native `DECIMAL(p,s)` / `NUMERIC(p,s)` with precision
1 through 38 and scale 0 through precision. Omitted precision/scale means
`DECIMAL(38,0)`. The Rust facade exposes `Decimal`, `DecimalType`, and
`Value::Decimal` / `ValueRef::Decimal`. A scalar stores a signed coefficient
of at most 38 digits and scale 0 through 38 in 17 inline bytes.

Integer literals fitting BIGINT remain integers. Other numeric literals,
including decimal points and exponents, retain their exact decimal digits;
they no longer pass through `f64`. `+`, `-`, `*`, `/`, `%`, unary signs,
parameters and numeric `CAST` / `::` share one kernel across projections,
predicates, INSERT values, defaults, UPDATE assignments and aggregate inputs.
UPDATE expressions read the original row. An expression, conversion or
constraint failure must leave the statement's changes unpublished.
Numeric arithmetic propagates NULL after operand type validation. Assigning
that NULL to a NOT NULL column still fails the complete statement.

BIGINT and Decimal promote exactly. Decimal addition, subtraction,
multiplication and remainder reject unrepresentable results, including
nonzero underflow; they never silently round. Division rounds once from the
original rational to at most 38 coefficient digits and scale 38, with ties
away from zero; very small quotients may round to zero. Assignment and an
explicit Decimal cast rescale with the same tie rule and check declared
precision after rounding. Division's result rounding and a subsequent column
assignment are separate SQL operations. Division by zero and overflow fail.
SUM uses these checked semantics; NULL-only and empty inputs yield NULL.

BIGINT/DOUBLE PRECISION comparisons use numeric ordering without first rounding
the integer through `f64`. Serializable dependency capture still refuses a
cross-domain primary-key point it cannot represent; an exactly integral Decimal
can instead bind to a BIGINT key. Valid ordinary execution does not bypass that
capture boundary.

Decimal/DOUBLE PRECISION arithmetic and comparisons require an explicit cast.
COALESCE retains its same-domain argument rule; for example, a DOUBLE vector
distance needs `CAST(9.0 AS DOUBLE PRECISION)` as a decimal-literal fallback.
Assignment to a declared DOUBLE PRECISION column permits approximate conversion;
assignment of a floating value to Decimal requires a cast. Decimal-to-BIGINT
casts reject fractions. Float-to-Decimal casts parse the float's shortest
round-trip decimal spelling and apply the declared rescale; they do not recover
precision already lost in a float. Explicit text-to-numeric casts reject invalid
or out-of-range values. NaN and infinity are not Decimal values.

This bounded numeric contract is not PostgreSQL numeric compatibility: division
uses HawDB's 38-digit/scale ceiling, fractional Decimal-to-BIGINT casts fail,
and exact multiplication fails when its nonzero result requires scale above 38.
Decimal literals keep that type beside DOUBLE operands; callers must cast the
literal or the operand explicitly rather than relying on contextual coercion.

Decimal equality, hashing and ordered keys compare numeric value, so `1.0` and
`1.00` identify the same key. Index predicates and joins must not rescale their
query boundaries to the stored column's scale. Row, WAL, checkpoint and spill
payloads preserve the original scale. New scalar/schema/expression tags are
additive; older readers cannot read databases containing the new values or
expressions. Existing tags retain their meaning. Decimal graph properties use
residual storage instead of float or dictionary coercion. This extension does
not enable Decimal property types in the separate SQL/PGQ creation binder.

Cypher graph range predicates compare Decimal/Decimal and Decimal/Int exactly,
including integer boundaries above 2^53. Decimal/Float pairs remain incomparable
under the existing graph mixed-kind predicate rules. Equality and IN preserve
typed identity, while Decimal values with different scales compare equal.
ORDER BY orders Decimal values numerically and retains the existing type order
for mixed kinds. Scalar/composite range indexes and checkpoint pruning preserve
the same results. Decimal values do not yet populate property histograms, so
cost-based planning may use fallback selectivity and choose a scan despite an
available range index. Decimal and native temporal values have distinct wire tags.

Cypher temporal values retain their behavior and persisted representation;
relational SQL timestamps are specified below.

## SQL timestamps

Relational SQL supports `TIMESTAMP [ (p) ] [ WITHOUT TIME ZONE ]` and
`TIMESTAMP [ (p) ] WITH TIME ZONE` / `TIMESTAMPTZ [ (p) ]` (#1453), following
PostgreSQL 18 [date/time types](https://www.postgresql.org/docs/18/datatype-datetime.html).
Both store a signed 64-bit count of microseconds since 1970-01-01 00:00:00.
`TIMESTAMP` is a local date and time without a zone. `TIMESTAMPTZ` is a UTC
instant: an input offset is applied and then discarded. HawDB has no session
`TimeZone`, so it behaves like PostgreSQL with `TimeZone = 'UTC'`: zone-less
`TIMESTAMPTZ` text is UTC, output is UTC, and conversions between the two types
go through UTC. The precision `p` is 0 to 6 (default 6). Assignment rounds to
it, with halfway values rounded away from 2000-01-01, as PostgreSQL's
`AdjustTimestampForTypmod` does. Text with more than six fractional digits is
rounded half up to the microsecond.

Accepted text is `YYYY-MM-DD` (meaning midnight), optionally followed by a
space or `T` and `HH:MM[:SS[.fraction]]`, with surrounding whitespace. For
`TIMESTAMPTZ` an optional offset follows: `Z`, `±HH`, `±HHMM`, `±HH:MM` or
`±HH:MM:SS` up to ±15:59:59, optionally after one space. The forms are
`TIMESTAMP '…'`, `TIMESTAMPTZ '…'`, `TIMESTAMP WITH TIME ZONE '…'`, string
literals and text parameters assigned to a timestamp column, and string
literals or text parameters compared with a timestamp column. A comparison
operand is parsed as the column's type but is not rounded to its precision.
`CAST(x AS TIMESTAMP[(p)] [WITH[OUT] TIME ZONE])`, `CAST(x AS TIMESTAMPTZ[(p)])`,
`CAST(x AS TEXT)` and the `::` forms convert between TEXT and both types, in
projections and in constant expressions such as `VALUES`, defaults and comparison
operands. TEXT output uses PostgreSQL's ISO style without trailing fractional
zeros (`2026-11-05 12:00:00.5`); `TIMESTAMPTZ` text ends in `+00`.

Equality, ordering, `ORDER BY`, `GROUP BY`, `DISTINCT`, `MIN`/`MAX`, joins,
primary keys, unique constraints, secondary indexes, range seeks and range
locks order values chronologically, and `NULL` keeps its usual SQL semantics.
Comparing a `TIMESTAMP` with a `TIMESTAMPTZ` is an error; cast one side
explicitly. Host parameters and results use the Cypher temporal values:
`Value::LocalDateTime` for `TIMESTAMP` and a UTC `Value::DateTime` for
`TIMESTAMPTZ`. A zoned parameter keeps its instant, and nanoseconds round half
up to the microsecond. `Value::Date`, `Value::LocalTime` and `Value::Duration`
are not SQL values. `information_schema.columns` reports `timestamp without
time zone` / `timestamp with time zone`, the `timestamp` / `timestamptz` UDT
names and `datetime_precision`.

Deviations from PostgreSQL:

- The range is chrono's: proleptic Gregorian dates from -262143-01-01 through
  +262142-12-31 23:59:59.999999. PostgreSQL accepts 4713 BC through 294276 AD.
  Years follow ISO 8601 astronomical numbering (year 0 is 1 BC, earlier years
  are negative). Years outside 0000–9999 carry an explicit sign on input and
  output; the `BC` suffix is not accepted.
- An offset in `TIMESTAMP` text is rejected, where PostgreSQL silently ignores it.
- `TIMESTAMP(7)` and higher are rejected, where PostgreSQL warns and uses 6.
- Special values (`infinity`, `-infinity`, `epoch`, `now`, `today`, …), named
  time zones, `AT TIME ZONE`, other PostgreSQL date styles and session
  `TimeZone` are not supported.
- Comparing `TIMESTAMP` with `TIMESTAMPTZ` needs an explicit `CAST`.
- Foreign keys, column-to-column `UPDATE … SET a = b` assignments and
  dependency-read join keys require identical precision (`TIMESTAMP(0)` and
  `TIMESTAMP(6)` do not match; a dependency-read join falls back to a
  database-wide key). This is stricter than PostgreSQL and matches Decimal.
- Strict append tables apply the same assignment conversion as ordinary tables,
  and their partition and order-key lookups parse text without rounding, so a
  finer-precision partition value matches no rows.
- `CAST` to TEXT accepts only TEXT, `TIMESTAMP` and `TIMESTAMPTZ` operands.
  Timestamp casts of columns inside `WHERE`, `HAVING` or `UPDATE` arithmetic are
  not supported; cast the constant operand instead.
- `DATE`, `TIME` and `INTERVAL` types and literals, interval arithmetic,
  `EXTRACT`/`date_part`/`date_trunc`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP` and
  `now()` are not supported yet.
- SQL/PGQ property graphs refuse timestamp columns as properties and refuse
  timestamp literals and time-zone casts in property expressions.

Persistence uses new additive tags with an 8-byte little-endian payload: value tag
10 (`TIMESTAMP`) and 11 (`TIMESTAMPTZ`) in row pages, relational WAL and
checkpoint values and schema defaults; scalar type tags 9 and 10 followed by the
precision byte in schemas and index-shadow digests; and ordered-key tags 10 and
11 with a sign-flipped big-endian payload. No format version changed. Existing
tags keep their meaning, so new binaries read old databases, and older binaries
reject databases containing timestamps on the first unknown tag. Binaries
released before this change report that as an invalid tag (for example
"invalid relational scalar type tag 9" or "invalid relational row value tag
10") rather than as data written by a newer version; their messages cannot be
changed. From this change on, the relational schema, value, row-page,
column-default and ordered-key decoders say that an unknown tag is "corrupt, or
written by a newer HawDB". Decoders treat an out-of-range microsecond count or
precision as corruption.

## Property Graph Catalog

A property graph is logical catalog metadata over existing table-like objects;
`CREATE PROPERTY GRAPH` MUST NOT copy or materialize base data. Vertex and edge
elements bind to stable source objects, keys, labels, and property expressions.
An edge definition binds source and destination keys to declared vertex
elements.

Creation MUST reject at least:

- missing or duplicate element aliases;
- missing, nullable, or type-incompatible keys;
- an edge endpoint that does not reference a declared vertex element;
- inconsistent property names or types for a shared label;
- duplicate properties exposed by one label;
- references to absent or future catalog objects.

Property-graph definitions are versioned catalog state and participate in the
same transaction, WAL, checkpoint, backup, restore, and system-schema upgrade
boundary as their source schema. A definition MUST be invalidated or rejected
when a referenced source object changes incompatibly; it MUST NOT keep a stale
physical pointer.

The PostgreSQL-compatible information-schema views for property graphs,
element tables, keys, endpoints, labels, and properties are part of the final
compatibility contract. They may land after the first executable query slice,
but absence MUST be reported as incomplete PostgreSQL SQL/PGQ compatibility.

## Binding And Execution

`GRAPH_TABLE` binds the named graph and produces a typed relational schema from
its `COLUMNS` clause. Element variables form a graph-local namespace. The first
slice follows PostgreSQL's current restrictions:

- one graph pattern per `GRAPH_TABLE`;
- no nested `GRAPH_TABLE`;
- no subqueries inside the graph pattern or `COLUMNS` expressions;
- no aggregate, window, or set-returning functions inside `COLUMNS`;
- a complex output expression requires an explicit column name.

The initial read-only binder resolves property-graph names, graph-local variable
slots, vertex and edge labels, properties, outer correlated columns, expression
types, and the typed `GRAPH_TABLE` output schema. It preserves structured source
spans on semantic errors and rejects PostgreSQL raw-parse shapes that violate
the qualified graph-transform subset. It does not publish property-graph state
or activate production SQL routing.

The first executable lowering slice accepts one linear graph path, zero or one
exact label per element, single-hop directed or undirected edges, graph-local
property predicates, bound PostgreSQL parameters, and explicit projection
columns. It lowers those shapes into the existing `NodeScan`, `Expand`,
`Filter`, and `Project` logical operators. Anonymous elements receive
deterministic collision-free internal variable names. Reused path variables,
label alternation, correlated outer-column evaluation, parenthesized paths,
and quantified walks fail during lowering until the shared logical operators
can represent their identity, row-multiplicity, and walk semantics exactly.
These failures retain the source span from the bound SQL/PGQ IR.

Passing this lowering boundary does not activate the owned parser in
`Database::query_sql*`. Production routing remains on the existing statement
families until catalog durability, surrounding relational planning, resource
admission, result qualification, and error-class differential coverage are all
complete.

The executable binder MUST lower vertex scans, edge expansion, label/property
predicates, and projection into shared graph logical operators. The surrounding
SQL query then treats the result as a normal typed table source. Cross-source
joins are planned as one logical plan; the host MUST NOT collect a graph result
and join it in application code.

The complete statement pins one database snapshot and one catalog epoch.
Graph expansion, relational operators, and result hydration share one admitted
query budget and cancellation token. Explain output MUST identify the
`GRAPH_TABLE` source, named property graph, selected graph access path,
estimated and actual rows, expansion budget, memory, spill, and truncation or
failure reason without retaining query parameters or user payloads.

## Compatibility And Qualification

The checked-in SQL/PGQ corpus MUST record the PostgreSQL source revision,
statement, parameters, expected columns and values, expected error class, and
the HawDB supported-subset classification. Parser acceptance alone is not
coverage.

Qualification requires:

- lexer no-panic and bounded-input fuzzing;
- parser golden and parse-format-parse tests for the owned syntax AST;
- binder tests for graph namespace, key, label, property, and type errors;
- differential result and error-class tests against the pinned PostgreSQL
  revision for the shared supported subset;
- equivalent-plan tests proving Cypher and SQL/PGQ lower to compatible graph
  logical operators where their semantics overlap;
- snapshot, cancellation, row, payload, expansion, memory, and spill tests;
- checkpoint/reopen and catalog-migration tests for property-graph metadata.

Standalone ISO GQL, graph mutation through GQL, unbounded path enumeration,
and broad PostgreSQL SQL/PGQ completeness remain out of scope until a separate
specification and workload justify them.
