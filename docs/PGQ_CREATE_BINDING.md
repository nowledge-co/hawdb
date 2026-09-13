# Read-only property graph creation binding

The source catalog and creation binder were approved on September 13, 2026 for
[#159](https://github.com/nowledge-co/skein/issues/159). They are additive APIs in
`skein::sql` and `skein_sql`. Existing query binding, execution, parser routing,
and fallback behavior are unchanged. `PgqDataType` adds `Binary` and `Uuid`;
consumers with exhaustive enum matches must handle these variants.

A caller implements `PgqSourceCatalog` over a coherent metadata snapshot, parses
`CREATE PROPERTY GRAPH` with `syntax::parse_postgres_statement`, and calls
`bind_postgres_create_property_graph`. A successful result is an existing
`PropertyGraphSchema`. The caller can insert it into `PropertyGraphCatalog` and
bind GRAPH_TABLE queries through the existing entrypoint. Neither binding
operation publishes a durable catalog, reads source rows, or changes storage.
The caller owns shared catalog state and snapshot lifetime; no process singleton,
new global lock, or query control plane is introduced.

## Source metadata

Lookup accepts decoded identifier components. The caller resolves search paths
and returns a canonical table identity; foreign keys use the same identity.
Quoted dots remain part of an identifier. Each declaration is resolved once.
Metadata must retain ordered columns, scalar types, nullability, primary keys,
and separate foreign-key constraints. The SQL owner has no storage dependency.

An omitted element KEY requires a primary key. Explicit keys require distinct,
existing columns; they can be nullable and need no existing unique constraint.
The binder does not prove uniqueness or non-null data. Explicit endpoints require
distinct existing columns of equal nonzero arity, but no FK and no match to the
vertex's declared graph key. Endpoint shorthand needs exactly one FK to the
canonical vertex table; duplicate constraints are still ambiguous.

Explicit endpoint types must match, except that a BigInt edge column can refer
to a DoublePrecision vertex column. The reverse direction fails. An inferred
FK must have identical types, following Skein relational metadata. Unused foreign
keys do not trigger traversal of other source tables.

Labels shared across elements, including vertices and edges, must expose the
same property-name set. A property name has one type throughout the graph.
Within an element, a repeated property across labels must use the same resolved
expression. Identity ignores source spans and redundant parentheses, but keeps
column identities, literal values, meaningful casts, and operand order.

Only a syntactic column reference, optionally parenthesized or qualified, supplies
an implicit property name. Every other expression requires `AS`, including an
identity cast such as `id::bigint`, even though binding can erase that cast when
comparing expressions across labels.

## Qualified scalar and expression profile

The metadata mapping is exhaustive:

| Source | Graph property |
| --- | --- |
| Boolean | Boolean |
| BigInt | Int64 |
| DoublePrecision | Float64 |
| Text | String |
| Bytea | Binary |
| Uuid | Uuid |

Creation expressions support columns, literals, parentheses, scalar Boolean and
numeric operations, comparisons, Text concatenation, IS NULL, IN, BETWEEN,
`lower(text)`, `upper(text)`, and `abs(bigint|double precision)`. Functions can be
qualified with `pg_catalog`. Unknown functions, aggregates, DISTINCT calls,
parameters, wildcards, outer references, and COLLATE fail explicitly.

Casts support identity, numeric conversions, BigInt/Bytea, Bytea/Uuid, and the
six scalar types to/from Text. Boolean/BigInt and transitive inferred casts are
not supported. Unknown NULL and string constants resolve from context; a final
unresolved constant becomes Text. Typed constants validate the target literal
representation. Numeric unary operations require a resolved numeric type;
modulo requires Int64 and concatenation requires Text operands.

The unchanged owned parser accepts `::type` casts. Its typed-string syntax is
currently limited to DATE/TIME/TIMESTAMP/INTERVAL, which are outside this
binder's six-type profile. Six-type typed-string public ASTs are validated by
the binder, but do not expand parser acceptance. CAST expressions, multiword
cast syntax, narrower PostgreSQL integer/numeric types, typmods, domains,
collations and additional functions remain explicit profile exclusions.

The implementation reference is PostgreSQL revision
[`3d00537feb565c410baf41bb301eee338e4b2317`](https://github.com/postgres/postgres/blob/3d00537feb565c410baf41bb301eee338e4b2317/src/backend/commands/propgraphcmds.c),
with its creation regression and cast/operator catalogs. This is a bounded
implementation profile, not a claim of complete PostgreSQL or ISO conformance.

## Diagnostics and verification

`PgqCreateBindError` exposes a non-exhaustive error code, a valid UTF-8 source
span and diagnostic text. Invalid public AST spans fail without slicing panics;
expression nesting is bounded at 128 levels. No partial schema escapes an error.

`frontend_create_bindings_v1.json` pins the existing frontend corpus hash and
SQL hashes for all 16 creation cases. Full expected schemas cover accepted
creation statements. Twenty additional cases record binding errors and profile
exclusions. Existing 981-case frontend provenance/waiver checks remain intact.
Owner tests also cover actual relational DDL metadata, followed by parsed
GRAPH_TABLE binding. The external facade test imports only `skein` and `std`.

The manual `skein_sql_create_binding_fuzz_tests` target exercises 4,608 explicit
and inferred endpoint combinations against an independent compatibility matrix.
It is included in routine local fuzz and excluded from native CI aggregates.

```sh
cargo test -p skein-sql -p skein-relational -- --include-ignored
cargo clippy -p skein-sql -p skein-relational --all-targets -- -D warnings
bazel test //crates/sql:presubmit_tests //crates/relational:presubmit_tests \
  //crates/sql-syntax:presubmit_tests //:skein_unit_tests //:skein_cli_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```
