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

From the repository root, Bazel builds the native extension and runs the same
Python smoke tests with a hermetic CPython 3.11 interpreter and locked pytest
dependencies. No virtual environment, maturin, or preinstalled HawDB wheel is
needed:

```bash
bazel build //bindings/python:hawdb
bazel test //bindings/python:hawdb_python_tests
```

The Python package is also available as a `py_library` dependency at
`//bindings/python:hawdb`. Its native module is built from the existing Rust
binding against `//:hawdb`, with the same Python 3.11 stable ABI as the maturin
build. Bazel outputs stay under `bazel-bin/bindings/python/python/hawdb/`.

For wheel packaging or an editable installation, use maturin:

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

## Pydantic rows (optional)

The `pydantic` extra types the rows a statement returned and the parameters a
statement takes with Pydantic v2 models. It does not generate Cypher or SQL.
`import hawdb` does not import Pydantic; `hawdb.pydantic` loads on first use.

```bash
pip install 'hawdb[pydantic]'
```

```python
from pydantic import BaseModel, Field

import hawdb

class Stock(BaseModel):
    code: str
    price: float

class StockColumns(BaseModel):
    code: str = Field(validation_alias="s.code")

db = hawdb.open("./graph")
db.execute(
    "CREATE (s:Stock {code: $code, price: $price})",
    hawdb.pydantic.params(Stock(code="603122", price=12.5)),
)

result = db.execute("MATCH (s:Stock) RETURN s.code AS code, s.price AS price")
stocks = hawdb.pydantic.parse(result, Stock)

# Columns that are not identifiers bind through a validation alias.
codes = hawdb.pydantic.parse(db.execute("MATCH (s:Stock) RETURN s.code"), StockColumns)

# A batch is a list of parameter dicts, loaded by one statement.
listed = [Stock(code="000001", price=9.0), Stock(code="600000", price=7.2)]
db.execute(
    "UNWIND $rows AS row CREATE (:Stock {code: row.code, price: row.price})",
    {"rows": [hawdb.pydantic.params(stock) for stock in listed]},
)
```

`parse` consumes the remaining rows of a `QueryResult`. Unknown columns and
missing fields follow the model's config. Invalid rows raise one
`pydantic.ValidationError` located by row index, and no models are returned.
Rows hold the binding's values: a UUID comes back as `str` and a tuple as a
list, so a strict model needs `Field(strict=False)` on those fields.

`params` uses the keys of `model_dump()` as parameter names and accepts only
values the binding stores as-is (`None`, `bool`, 64-bit `int`, `float`, `str`,
`bytes`, `uuid.UUID`, lists, and string-keyed dicts). A `datetime`, `Decimal`,
or plain `Enum` field raises `TypeError`, and an `int` outside 64 bits raises
`OverflowError`, before the statement runs. Engine errors stay
`hawdb.exceptions`.

## Test

```bash
pytest tests/
```

Without Pydantic installed, the extra's tests skip and the rest prove that
`import hawdb` does not need it. Install `pydantic` to run them. Bazel runs the
two environments as `//bindings/python:hawdb_python_tests` and
`//bindings/python:hawdb_python_pydantic_tests`.

To refresh the Bazel test dependency lock:

```bash
bazel run //bindings/python:requirements.update
```

The separate `Cargo.Bazel.lock` locks the PyO3 dependencies for Bazel; the
binding stays outside the library's Cargo workspace. To update it deliberately,
run `CARGO_BAZEL_REPIN=1 CARGO_BAZEL_REPIN_ONLY=python_crates bazel build //bindings/python:hawdb`.

## Scope and follow-ups

Not included yet: async calls, explicit multi-statement transactions, Arrow
output, schema introspection helpers. These are tracked for follow-up once
the base surface settles.
