# Cypher `LOAD CSV`

Issue: https://github.com/nowledge-co/hawdb/issues/1351

HawDB's Cypher accepts Neo4j-style `LOAD CSV` for importing local CSV files
through ordinary statements. Batched commits (`CALL { ... } IN TRANSACTIONS`)
and the offline bulk importer are separate follow-up work.

```cypher
LOAD CSV WITH HEADERS FROM $persons AS row FIELDTERMINATOR '|'
CREATE (:Person {id: toInteger(row.id), firstName: row.firstName})

LOAD CSV WITH HEADERS FROM $knows AS row FIELDTERMINATOR '|'
MATCH (a:Person {id: toInteger(row.src)}), (b:Person {id: toInteger(row.dst)})
CREATE (a)-[:KNOWS {creationDate: toInteger(row.creationDate)}]->(b)

LOAD CSV FROM $file AS line
RETURN line[0] AS first, size(line) AS fields
```

## Syntax

```text
LOAD CSV [WITH HEADERS] FROM <source> AS <variable> [FIELDTERMINATOR '<c>']
```

- `<source>` is a string literal or a `$parameter` whose value is a string.
- `FIELDTERMINATOR` takes one ASCII character other than `"`, CR or LF, written
  as a string literal; escapes such as `'\t'` are accepted. The default is `,`.
- Keywords are case-insensitive. The clause is parsed by the default Cypher
  PEG frontend, including under `EXPLAIN` and `CYPHER SYSTEM...` wrappers.

## Rows

Without `WITH HEADERS`, each record binds `<variable>` to a list of strings.
With `WITH HEADERS`, the first record names the columns and every later record
binds a map from each header to the field in the same position.

| Input | Without headers | With headers |
| --- | --- | --- |
| `a,b` | `['a', 'b']` | `{h1: 'a', h2: 'b'}` |
| empty unquoted field (`a,,b`) | `NULL` element | `NULL` value |
| quoted empty field (`""`) | `''` | `''` |
| record shorter than the header | – | missing columns are `NULL` |
| record longer than the header | – | extra fields are ignored |

Values are always strings or `NULL`; convert them explicitly with
`toInteger`, `toFloat`, `toBoolean` or `toString`. A repeated header name is an
error.

This follows Neo4j's default CSV configuration, in which an empty unquoted
field is `NULL` in both modes and a quoted empty string stays `''`
(`emptyQuotedStringsAsNull = false`). Reviewers comparing with a particular
Neo4j release should check this table against that release's documentation.

## CSV format

The reader implements RFC 4180 with these rules:

- Fields may be quoted with `"`; a doubled `""` inside a quoted field is one
  quote. Quoted fields may contain the field terminator and line breaks.
- A `"` inside an unquoted field is kept literally. Any character other than
  the terminator or a line break after a closing quote is an error.
- Records end at LF, CRLF or a lone CR. Completely empty lines are skipped.
- A UTF-8 byte-order mark at the start of the file is skipped.
- Fields must be valid UTF-8. An unterminated quoted field is an error.
  Errors name the record number and its starting line.

## Composition

`LOAD CSV` must be the first clause of a query. It is a row source:

- **Reads.** Any following `WITH`, `WHERE`, `ORDER BY`, `SKIP`, `LIMIT`,
  aggregation and `RETURN` use the ordinary pipeline, with the row variable as
  a value column (`row.key`, `line[0]`, `size(line)`, `count(*)`), including
  temporal constructors such as `date(row.day)` or `datetime(row.at)`.
- **Writes.** The following clauses must be one of:
  - `CREATE (:Label {...})` – one node per record;
  - `MERGE (n:Label {...}) [ON CREATE SET n.p = ...]` – one node per record;
  - `MATCH (a:L {...}), (b:M {...}) CREATE (a)-[:TYPE {...}]->(b)` – one
    relationship between every matched `(a, b)` pair per record.

  Property values in these patterns and in `ON CREATE SET` may be literals,
  parameters, `row.key`, `line[i]` (negative indexes count from the end) or a
  conversion function of one of these. A temporal constructor over a row field
  (`date(row.day)`) is not yet accepted here and fails with a semantic error;
  temporal constructors over literals and parameters are. `UNWIND $rows AS r` feeds the same
  per-row mutations, so the matched-relationship form is also available there.

Per-record write semantics follow openCypher: `CREATE` omits `NULL`
properties, `MERGE` with a `NULL` match property fails the statement, and a
`MATCH` endpoint whose property value is `NULL` matches nothing, so that record
creates no relationship. A record whose endpoints match no node also creates
nothing.

Compositions outside this set fail with a semantic error before any write, for
example `WITH` between `LOAD CSV` and a mutation, `WHERE` on the endpoint
`MATCH`, `OPTIONAL MATCH`, `SET`/`DELETE` after `LOAD CSV`, relationship
`MERGE`, `LOAD CSV` after another clause, and a read-pipeline `MATCH` whose
node properties reference the row (row-dependent lookups are supported only in
the write forms above).

## Type conversion functions

The functions work in every Cypher scalar context, not only with `LOAD CSV`:

| Function | Accepts | Invalid string |
| --- | --- | --- |
| `toInteger(v)` | integer; float (truncated toward zero); boolean (`1`/`0`); decimal integer or float string | `NULL` |
| `toFloat(v)` | float; integer; decimal number string, `NaN`, `Infinity` | `NULL` |
| `toBoolean(v)` | boolean; integer (`0` is false); `'true'`/`'false'` in any case | `NULL` |
| `toString(v)` | string; integer; float (Java `Double.toString` format, e.g. `1.0`, `1.0E20`); boolean; UUID; temporal value (ISO 8601, e.g. `2026-10-30`, `2026-10-30T12:34:56.789+01:00`, `P1Y2M3DT5H`) | – |

`NULL` converts to `NULL`. Strings are trimmed of surrounding whitespace before
parsing. A value of a type the function does not accept (for example
`toBoolean(1.5)` or `toString([1])`), or a float outside the integer range in
`toInteger`, fails the query, as in Neo4j.

## Security

`LOAD CSV` is disabled unless the host sets an import directory:

```rust
let config = DatabaseConfig {
    csv_import_root: Some("/srv/hawdb/import".into()),
    ..DatabaseConfig::default()
};
```

With `csv_import_root = None` (the default) every `LOAD CSV` statement fails
with a semantic error before reading anything. When it is set:

- The source may be an absolute path, a path relative to the import directory,
  or a `file:` URL (`file:///x.csv`, `file://localhost/x.csv`, `file:/x.csv`;
  percent escapes are decoded). As in Neo4j, a `file:` URL path is resolved
  relative to the import directory, so `file:///x.csv` reads `<root>/x.csv`. Every other URL scheme,
  including `http` and `https`, is refused. `file:` URLs with another host, a
  query or a fragment are refused.
- The path is canonicalized, resolving `..` components and symbolic links, and
  must name a regular file inside the canonical import directory. A symbolic
  link that leaves the directory is refused.
- The file type is checked before the file is opened, so a FIFO, socket or
  device inside the import directory is refused instead of blocking the query,
  and the opened handle is checked again.
- Resolution and opening are separate system calls. A local process that can
  replace path components inside the import directory between them can race
  the check (for example, swap a FIFO in after the type check, which would
  block the open); keep the directory writable only by trusted users.

Errors are classified so hosts can tell a bad statement from a run-time
condition:

| Condition | Error |
| --- | --- |
| no `csv_import_root` configured; non-local or malformed URL; NUL; non-string `$parameter` | `Semantic` |
| source outside the import directory (after resolving symbolic links, or lexically when the target does not exist) | `Semantic` |
| import root or source missing or unreadable; source not a regular file | `Execution` |
| file, record, field, mutation and memory limits; malformed CSV | `Execution` (memory: `ResourceBudgetExceeded`) |

## Limits and transactions

`DatabaseConfig::csv_import_limits` bounds every source:

| Limit | Default | Failure |
| --- | --- | --- |
| `max_file_bytes` | 4 GiB | checked against the file length before reading and again while streaming |
| `max_record_bytes` | 4 MiB | field bytes in one record |
| `max_fields_per_record` | 4,096 | fields in one record, including the header |

The file is streamed through one 64 KiB buffer; only the current record is
assembled. Reads charge the read buffer and each pending output batch to the
query memory ledger and then follow the ordinary batch, result-row and result
payload budgets (`max_read_result_rows`, `max_read_result_payload_bytes`,
`execution_memory`). An aggregate such as `count(*)` consumes records without
retaining them.

A write statement runs in one transaction, like `UNWIND`. Every record is read
and resolved before anything is staged, then the batch commits atomically or
not at all, in autocommit and in explicit transactions. Records count against
`mutation_limits.max_operations` and their payload against
`mutation_limits.max_result_payload_bytes`. The staged mutations themselves
(property maps, endpoint filters and the list) are charged to the statement's
query memory ledger (`execution_memory.query_memory_bytes`) as they are
built, so a batch that does not fit fails with `ResourceBudgetExceeded`
before anything is written. An `UNWIND` batch keeps its existing contract: its
rows are a parameter the caller already holds, so its staged list is bounded by
the mutation limits and is not charged; the endpoint id lists that relationship
resolution copies into it (below) are new memory and are charged. Persistent databases also enforce
`max_wal_batch_operations` and `max_wal_record_bytes` before the batch is
appended to the WAL. A file beyond any limit fails closed with no partial writes
and an error naming the limit. Split such files until batched commits
(`CALL { ... } IN TRANSACTIONS`) are supported.

Execution checks cancellation and deadlines between record batches.

### Relationship loads

For the `MATCH ... CREATE (a)-[:TYPE]->(b)` form, from `LOAD CSV` or
`UNWIND`, endpoints are resolved for the whole statement before the database
write guard is taken. Each distinct endpoint pattern (label plus the set of
property names, e.g. `:Person {id}`) is resolved once: through a declared
property index on its first property when the store is resident, otherwise by
one label scan that decodes only the pattern's properties. Only the property
values that the records actually use are kept, mapped to node ids, and that
map is charged to the query memory ledger. The scan checks cancellation and
deadlines as it goes. Each record then commits with internal-id endpoints,
which storage resolves by point lookups, rechecking the label and every
property equality on each candidate; the write guard is held for
work proportional to the matched ids (one point lookup each) rather than
O(records x label size).

Each record's endpoint filters carry a copy of the ids its key matched, so a
key that matches many nodes is copied once per record. Before any copy is
made, the matched counts are checked record by record against
`mutation_limits.max_affected_rows` and `max_operations`, as the commit would
check them: a fan-out the commit would refuse (for example
`MATCH (p:Person {country: row.c}), (c:Country {code: row.c})` with tens of
thousands of people per country on many records) fails with the same
`mutation would exceed max_mutation_affected_rows` `Execution` error before the
copies are allocated. A record whose endpoint matches nothing copies no ids for
either side. The copies are charged to the query memory ledger for `LOAD CSV`
and `UNWIND` alike, so a batch whose copies do not fit fails with
`ResourceBudgetExceeded` before anything is written.

Endpoints that cannot be keyed this way keep the per-record label scan under
the write guard: a filter that is not made only of property equalities, an
endpoint without properties (bounded by the affected-row limit), and an
unlabelled endpoint such as `MATCH (a {id: row.src}), ...`. The last scans every
node for every record, O(records x all nodes); give such endpoints a label for
bulk loads.

Results are unchanged: duplicate matching nodes still produce every
`(a, b)` pair, an endpoint that matches nothing still creates nothing, and in
an explicit transaction nodes created by earlier statements are matched. The
autocommit path holds the database handle exclusively for the whole
statement, so no other writer can change the graph between resolution and
commit.

## Plans and caching

Plans store the source as a literal or a parameter name, never a resolved path
or file contents. The file is opened and read again on every execution, so a
cached read plan serves different `$file` values and observes changed file
contents. Explicit-transaction read-dependency tracking treats `LOAD CSV` as
external input and refuses to certify such statements. `EXPLAIN` reports
`LoadCsvExec` or `LoadCsvMutationExec` with the source, header flag, terminator
and variable without opening the file.

## Differences from Neo4j

- Only local files are supported; `http`, `https`, `ftp` and other URLs are
  refused. Neo4j additionally reads remote URLs.
- No `CALL { ... } IN TRANSACTIONS` or `USING PERIODIC COMMIT`; one statement
  is one transaction and must fit the mutation limits.
- `LOAD CSV` must be the first clause; it cannot follow `MATCH`/`WITH`.
- Write pipelines support only the forms listed above; there is no `WITH`
  filtering, `SET`, `DELETE`, relationship `MERGE` or `FOREACH` after
  `LOAD CSV` yet, and write property values cannot yet apply a temporal
  constructor to a row field.
- `linenumber()` and `file()` are not implemented.
- Compressed (`.gz`, `.zip`) sources are not decompressed.
- Strict RFC 4180 quoting: Neo4j's legacy `\"` escape inside quoted fields
  (`dbms.import.csv.legacy_quote_escaping`) is not recognized, and text after a
  closing quote is an error.
- `FIELDTERMINATOR` must be ASCII; Neo4j also accepts other single characters
  and `\uNNNN` escapes.
- Duplicate header names are rejected instead of resolving to one column.
