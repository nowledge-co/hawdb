# HawDB for Bun

This package embeds HawDB in a Bun process through Bun's built-in FFI and the
existing HawDB C ABI. It does not launch a CLI or helper process and does not
add another Rust integration surface.

Build the shared library from the repository root:

```sh
bazel build //bindings/ffi:hawdb_ffi
```

Load that exact artifact explicitly:

```ts
import { loadHawDB } from "./bindings/bun/src/index.ts";

const hawdb = loadHawDB("bazel-bin/bindings/ffi/libhawdb_ffi.dylib");
const database = hawdb.openInMemory();

database.exec("CREATE (:Person {name: $name})", { name: "Ada" });
const result = database.query("MATCH (p:Person) RETURN p.name AS name");
console.log(result.dicts());

database.close();
hawdb.close();
```

Use `libhawdb_ffi.so` on Linux and `hawdb_ffi.dll` on Windows. The adapter
requires an explicit path; it does not use an environment variable as a
production control plane. `HAWDB_LIBRARY` is used only by the repository test
harness:

```sh
HAWDB_LIBRARY="$PWD/bazel-bin/bindings/ffi/libhawdb_ffi.dylib" \
  bun test bindings/bun
```

## API and ownership

- `loadHawDB(path)` maps the shared library and registers the stable ordinary
  query symbols.
- `library.open(path, { readOnly })` and `library.openInMemory()` return a
  database handle.
- `database.query(cypher, parameters)` and `database.querySQL(sql, parameters)`
  return `QueryResult { columns, rows }`; `dicts()` converts rows to objects.
- Close databases explicitly before closing the library. Both close operations
  are idempotent. The library refuses to unload while a database is live.
- Every native result/error buffer is copied and released before a call returns.
  The adapter never exposes native buffer pointers to application code.
- Calls fail before native dispatch if parameter or option getters close their
  database or library while the arguments are being normalized.

All calls are synchronous and therefore block the Bun event-loop thread. Run
long queries in a Bun Worker and open the database handle inside that Worker.
One native handle serializes its calls; use separate handles when independent
parallel work is required.

## Values

Safe integers return as JavaScript `number`; larger signed 64-bit integers
return as `bigint`. Integer parameters outside JavaScript's safe range must be
passed as `bigint`, and values outside the signed 64-bit range are rejected
before FFI. Floats remain `number`.

Array parameters must be dense. Holes and `undefined` values are rejected,
including in nested lists; use explicit `null` values when needed.

`Uint8Array` parameters use HawDB's `{"$binary": "<base64>"}` boundary form and
binary results become `Uint8Array`. Decimal, UUID, temporal and vector values
retain the C ABI's tagged object forms (`$decimal`, `$uuid`, `$date`,
`$localtime`, `$localdatetime`, `$datetime`, `$duration`, and `$vector`) so no
precision or type identity is silently discarded.

## Stability

The HawDB C ABI owns the database semantics and memory boundary. Bun currently
documents `bun:ffi` itself as experimental and recommends Node-API for the most
stable production native integration. This adapter is consequently an explicit
Bun host layer; it does not expand the HawDB C ABI compatibility promise or
cover the experimental retained/native-column exports.
