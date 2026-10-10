# HawDB Python bindings

Embed the HawDB database directly from a Python host — same engine, no
separate server process. The binding is a thin translation layer over the
existing embedded Rust API; all query semantics, budgets, and durability
behavior stay in HawDB itself.

## Status

Development phase. The API surface is intentionally small (`open`, `execute`,
`execute_sql`, `QueryResult`) while it proves out against real host
workloads. It is not yet published to PyPI.

## Experimental retained numeric buffers

`execute_retained` is an opt-in numeric query surface over the embedded Rust
cursor. It supports catalog-declared integer/float comparisons, projections of
the same property or unsigned `id(n)`, and optional SKIP/LIMIT. Declare the node
table and property type before creating a cursor. Other plans/types, source
reuse and copying requests refuse explicitly.

```python
options = hawdb.RetainedOptions(batch_rows=1024)
query = db.execute_retained(
    "MATCH (n:Item) WHERE n.score >= $min RETURN n.score AS score",
    {"min": 0}, options=options,
)
try:
    while (batch := query.next_batch()) is not None:
        column = batch.column(0)
        selection = batch.selection()
        try:
            with memoryview(column) as values, memoryview(selection) as indices:
                for physical in indices:
                    consume(values[physical])
        finally:
            selection.close()
            column.close()
            batch.close()
finally:
    query.close()
```

Numeric data has one element per physical row; ordered selection identifies the
result rows without gathering. `column.validity()` returns None for all-valid
data, otherwise a separate read-only native u64 bitmap with least-significant
bit order. Schema formats q/d/Q distinguish Int64/Float64/UInt64 identity values;
selection uses I and validity uses Q. Nullable raw values do not imply an ordinary
non-null NumPy array. `schema_copy` preserves schema even for empty completion.

Defaults remain two payload slots, no prefetch, 1,024 inspected records and 1 MiB
per batch, under the existing database/shared byte and handle limits. Positive
batch/slot options cannot bypass those limits; `max_result_rows` can impose a
lower cumulative budget. Python requires at least four shared handles for the
control, batch, exporter and native buffer lease.

`BackpressureError` is retryable: release held views and retry the same cursor.
It never means successful EOF, never waits for the caller, and does not advance
the source. Other `RetainedError` outcomes expose `kind` and `retryable` and remain
terminal. Every batch is provisional until successful completion; live exporter
status observes late failures. `profile_copy` distinguishes native emissions
from successfully delivered Python batches. `retained_snapshot_copy` observes
the shared resource owner.

Close batches/exporters explicitly and release each memoryview when finished.
Each native buffer acquisition has a separately admitted lease; existing views
remain readable after parent or database closure. A derived view shares the
managed lease and a small slice retains the full native capacity. Closed
high-level owners reject new access/export. `retain` creates an independent
admitted owner. GC prevents abandoned-owner leaks, while explicit release is the
way to unblock a stopped same-thread consumer. Owners keep the code module,
not the database or source iterator.

The strict payload path builds no Python row list, result JSON or IPC envelope.
`value_copy` and metadata/diagnostic copy methods are explicit object
materialization. Writable buffer requests and dtype changes refuse before
ownership transfer. The extension imports without NumPy or PyArrow.

Compatible batches expose standard `__arrow_c_array__` capsules and both
batches/cursors expose schema-only `__arrow_c_schema__`. Cursor
`__arrow_c_stream__` exports a demand-driven stream, adopting the cursor;
the original cursor then reports closed. Exporting a capsule does not pull.
Only empty/contiguous selections are eligible; sparse selections and requested
schema arguments refuse explicitly. Capsules are consumed once, and abandoned
capsules release their owners. Arrays already handed to a consumer remain
readable after stream or database close.

With optional PyArrow installed, use its public protocol APIs:

```python
reader = pyarrow.RecordBatchReader.from_stream(query)
try:
    for batch in reader:
        consume(batch)  # release each batch before retaining more than two
finally:
    reader.close()
```

`pyarrow.record_batch(batch)` imports an eligible retained batch. Collecting all
stream batches, including `read_all()`, still obeys shared limits and may raise
an Arrow resource error; it does not enlarge the native pool. EOF confirmation
after the immutable source is exhausted needs no new slot/descriptor.

The optional public-consumer tests are skipped without PyArrow. After building
the default test target, they can be run in a separate environment containing
pytest and PyArrow (locally verified with 26.0.0):

```bash
bazel test //bindings/python:hawdb_python_tests
PYTHONPATH="$PWD/bazel-bin/bindings/python/hawdb_python_tests.runfiles/_main/bindings/python/python" \
  /path/to/consumer-venv/bin/python -m pytest --import-mode=importlib \
  --rootdir="$PWD" bindings/python/tests/test_arrow_consumer.py -v
```

This is experimental. Complete source/planning admission, opaque derived-view
and allocator/RSS accounting, platform and performance qualification remain
open. Current native result-buffer evidence does not bound whole-operation or
interpreter RSS. See [the full scope and remaining gates](../../docs/RETAINED_NUMERIC_FOUNDATION.md).

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

### Lite profile

Constrained hosts — including the Pyodide/JupyterLite build — can link the
extension against the minimal engine instead of the default feature set:

```bash
maturin build --no-default-features --features lite
# or inside the venv: maturin develop --no-default-features --features lite
```

Lite is a build profile of the same package, not a second API: `open`,
`execute`, `execute_sql`, and `QueryResult` behave the same. Statements that
need a capability the minimal engine does not carry (full-text search, vector
search, graph analytics, background maintenance) raise
`hawdb.exceptions.CapabilityError` instead of silently returning an empty
result. `hawdb.capabilities()` reports which capability flags were compiled
into the extension as a read-only mapping:

```python
import hawdb

hawdb.capabilities()["full_text_search"]   # False on a lite build
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

`db.transaction()` opens the engine's single user transaction: statements
inside the `with` block see their own writes, a clean exit publishes them as
one durable commit, and an exception rolls back and re-raises. A nested
`db.transaction()` — and any `db.execute` / `db.execute_sql` on the parent —
fails while a transaction is open.

```python
with db.transaction() as tx:
    tx.execute("CREATE (s:Stock {code: $code})", {"code": "603122"})
    tx.execute_sql("INSERT INTO trades (id) VALUES ($1)", [7])
    tx.rollback()      # abandons both statements; omit it to commit
```

`db.read_transaction()` pins the committed state for a stable read scope;
write statements on it raise the engine's read-transaction error.

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
stored exactly or rejected before the statement runs. Integers, including
NumPy integer scalars, must fit the signed 64-bit range or raise
`OverflowError`. NumPy `float16`/`float32` scalars widen to `float` exactly
and are accepted. Any other type, including `Decimal`, `Fraction`, or NumPy
`longdouble`, raises `TypeError`; convert such values explicitly, for example
with `float(x)`.

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

`conftest.py` parametrizes statement-level tests over both backends: a
file-backed project under `tmp_path` and the in-memory `hawdb.open()`.
`test_stubs.py` keeps the hand-written `_hawdb.pyi` signatures in sync with
the compiled module's runtime signatures.

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

Not included yet: async calls, general-query Arrow output, schema introspection helpers.
These are tracked for follow-up once the base surface settles.
