# HawDB MoonBit native binding

MoonBit programs access HawDB's embedded Rust engine through the native FFI.
The adapter exports Rust functions with the C calling convention for MoonBit.
It does not depend on `bindings/ffi` or start a database server.

## Build and test

The adapter depends on the current HawDB checkout through the repository's
Bazel `//:hawdb` target. Install the MoonBit native toolchain first.
On Linux/macOS, the existing Native Bindings workflow runs:

```sh
bazel build //bindings/moonbit/native:hawdb_moonbit_static
bazel test --test_output=errors --test_env=PATH --test_env=HOME \
  //bindings/moonbit:hawdb_moonbit_tests
```

The suite tests Rust handle lifecycles and MoonBit's real embedded-engine
behavior: SQL/Cypher, typed values, transactions, durable reopen, errors, and
read-only access. The macOS native linker flags are set by
`tools/configure_native_link.py`.

Windows native executable conformance remains in the MoonBit Windows workflow
until the same executable test can run through Bazel on Windows. That job
builds the adapter against the current local HawDB source with Cargo.
This PR does not introduce a separate SDK publisher.

## API

- `open(path)`, `open_in_memory()`, `open_read_only(path)`, and `Database::close()`
- `Database::query(statement, named_params)` for Cypher and
  `Database::query_sql(statement, positional_params)` for SQL
- Materialized `QueryResult.columns`, `rows`, and `get(row, column)`
- `Value` types for null, bool, signed Int64, floating-point numbers, text,
  bytes, UUID, vector, list, and map values
- `HawdbError` for engine, codec, and closed-handle failures
- Write/read transactions: `begin`, `begin_read`, `commit`, `rollback`,
  `with_transaction`, and `with_read_transaction`

SQL/Cypher semantics and durability come from HawDB; they are not reimplemented
by the binding. The internal tagged JSON representation is not a stable public
wire ABI. Native MoonBit handles are not advertised for cross-thread sharing.

See `tests/conformance/main.mbt` for the executable behavior contract.
