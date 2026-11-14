# JSON scalar selection in Cypher

[`json_value`](https://github.com/nowledge-co/hawdb/issues/1083) extracts one
scalar from a JSON document stored in a `STRING` property. The path is a string
literal or query parameter and uses `$` followed by one or more dotted field
names. Field names contain ASCII letters, digits, `_`, or `-`; arrays, quoted
keys, and wildcards are not supported. The path is at most 256 bytes.

```cypher
MATCH (m:Memory)
WITH json_value(m.metadata, $path) AS origin, COUNT(*) AS total
RETURN origin, total ORDER BY origin
```

The explicit [`NULL ON NONSCALAR`](https://github.com/nowledge-co/hawdb/issues/1605)
clause uses the same exact path and scalar conversions, but returns `NULL` when
the selected value is an array or object. This is useful when valid legacy
metadata may hold a non-scalar at a field whose scalar value is optional:

```cypher
MATCH (m:Memory)
WHERE coalesce(json_value(m.metadata, $path NULL ON NONSCALAR), 'active') = 'active'
RETURN COUNT(*) AS total
```

`NULL ON NONSCALAR` is not a general `NULL ON ERROR` clause. Malformed JSON,
non-string documents, invalid paths, input or query-memory limits, numeric
conversion failures, cancellation, and storage/runtime failures still fail the
query. Only a successfully selected array or object differs from strict
`json_value`. HawDB intentionally does not accept SQL/JSON `NULL ON ERROR` here:
that broader clause also covers path-evaluation and return-type conversion
errors. PostgreSQL's `json_scalar` is a separate SQL-scalar-to-JSON constructor,
not a selector. See the [PostgreSQL 17 JSON function reference](https://www.postgresql.org/docs/17/functions-json.html).

For `$path = '$.original_source'`, this groups by the extracted value inside
the database. The function also works in scalar predicates, `CASE`, and
`COALESCE`. It returns `NULL` when the document is `NULL`, the path is absent,
or the selected value is JSON `null`. JSON strings become Cypher strings,
booleans become booleans, signed 64-bit integers become integers, and other
JSON numbers become floats. An unsigned integer outside the signed 64-bit
range, a selected object or array, malformed JSON, a non-string document, or
an invalid path fails `json_value`; none is silently converted to a source
name. `NULL ON NONSCALAR` changes only the selected object or array case to
`NULL`.

One JSON document may be at most 1 MiB. The evaluator reserves temporary query
memory before parsing, and the ordinary result row and payload budgets still
apply. The limit is per evaluated document, so callers should use a selective
predicate when possible. This expression does not add an index on JSON members
or alter stored property bytes.
