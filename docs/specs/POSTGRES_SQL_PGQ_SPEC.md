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
Pratt AST; semantic binding owns only normalized names and typed values that
survive past parsing. This keeps parse memory proportional to token and node
count while preserving exact source locations for diagnostics.

The owned frontend MUST be implemented incrementally. Existing relational
statement families may continue to use upstream `sqlparser` until the owned
parser has equivalent positive and negative corpus coverage. Parser selection
MUST be explicit by statement family; retrying another parser after a syntax or
semantic failure is forbidden.

The first owned `SELECT` slice recognizes `GRAPH_TABLE` and qualified relations
as `FROM` items, bounded scalar expressions, `INNER`, `LEFT`, and `CROSS` joins,
projection, filtering, grouping, ordering, limits, offsets, and locking clauses.
It deliberately rejects subqueries, right/full/natural joins, join `USING`,
`DISTINCT ON`, and other unqualified shapes. These syntax nodes are not a
production execution contract: existing relational SQL continues through the
upstream parser until logical-plan lowering has equivalent corpus coverage. An
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
