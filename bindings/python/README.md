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

`hawdb.open()` without a path returns an empty in-memory database for tests,
notebooks, and other hosts that already hold their data. It answers the same
`execute` / `execute_sql` calls through the same embedded admission path;
`close()` discards everything and a later `open()` does not see it.
`db.path` is `None` for in-memory handles.

```python
db = hawdb.open()
db.execute("CREATE (:Memory {id: $id})", {"id": 1})
db.close()
```

Parameters accept `None`, `bool`, `int`, `float`, `str`, `bytes`,
`uuid.UUID`, lists or tuples, and string-keyed dicts, nested freely. They are
stored exactly or rejected before the statement runs: an integer outside the
signed 64-bit range raises `OverflowError`, and any other type, including
`Decimal`, `Fraction`, or a NumPy `float32` scalar, raises `TypeError`.
Convert such values explicitly, for example with `float(x)` or
`array.tolist()`.

Errors raise `hawdb.exceptions` subclasses (`ParseError`, `SemanticError`,
`StorageError`, `ExecutionError`, `ConflictError`, ...), mapped from
`HawDBError` kinds.

## Test

```bash
pytest tests/
```

`conftest.py` parametrizes statement-level tests over both backends: a
file-backed project under `tmp_path` and the in-memory `hawdb.open()`.
`test_stubs.py` keeps the hand-written `_hawdb.pyi` signatures in sync with
the compiled module's runtime signatures.

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
