# HawDB Python bindings

Embed the HawDB database directly from a Python host — same engine, no
separate server process. The binding is a thin translation layer over the
existing embedded Rust API; all query semantics, budgets, and durability
behavior stay in HawDB itself.

## Status

Development phase. The API surface is intentionally small (`open`, `execute`,
`execute_sql`, `QueryResult`) while it proves out against real host
workloads. It is not yet published to PyPI.

## Build from source

Requires the repo-pinned Rust toolchain and Python 3.11+ (abi3 wheels).

```bash
cd bindings/python
uv venv .venv && source .venv/bin/activate
uv pip install maturin pytest
maturin develop
```

## Usage

```python
import hawdb

db = hawdb.open("./graph")
db.execute("CREATE (s:Stock {code: $code})", {"code": "603122"})

result = db.execute("MATCH (s:Stock) RETURN s.code")
print(result.columns)      # ['s.code']
print(result.fetchall())   # [{'s.code': '603122'}]

db.execute_sql("SELECT 1")
db.close()
```

Errors raise `hawdb.exceptions` subclasses (`ParseError`, `SemanticError`,
`StorageError`, `ExecutionError`, `ConflictError`, ...), mapped from
`HawDBError` kinds.

## Test

```bash
pytest tests/
```

## Scope and follow-ups

Not included yet: async calls, explicit multi-statement transactions, Arrow
output, schema introspection helpers. These are tracked for follow-up once
the base surface settles.
