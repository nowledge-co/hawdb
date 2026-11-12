# HawDB for Bun

This package embeds HawDB in a Bun process through Bun's built-in FFI and the
existing HawDB C ABI. It does not launch a CLI or helper process and does not
add another Rust integration surface.

## Install

```sh
bun add @nowledge/hawdb-bun
```

Stable releases are published under `latest`; development prereleases are
available as `@nowledge/hawdb-bun@dev` (or `@nightly`). HawDB's on-disk format
carries no compatibility promise until HawDB declares one, so a database may
need to be recreated after an upgrade.

The package installs the prebuilt HawDB C ABI library for the current host
through one optional `@nowledge/hawdb-ffi-<platform>` dependency: macOS arm64
and x86-64, Linux x86-64 and AArch64 with glibc, and Windows x86-64. The
platform table, minimum OS and glibc versions are in
[Prebuilt C ABI libraries](../../docs/PREBUILT_LIBRARIES.md).

```ts
import { loadHawDB } from "@nowledge/hawdb-bun";

const hawdb = loadHawDB();
const database = hawdb.openInMemory();

database.exec("CREATE (:Person {name: $name})", { name: "Ada" });
const result = database.query("MATCH (p:Person) RETURN p.name AS name");
console.log(result.dicts());

database.close();
hawdb.close();
```

`loadHawDB()` without an argument resolves the installed platform package with
ordinary module resolution. It fails with the package it expected when that
package is missing (for example after installing with optional dependencies
disabled) or comes from another release than `@nowledge/hawdb-bun`, and with
`UnsupportedPlatformError` on hosts without a prebuilt library, such as a Bun
process running on musl. The C library is that of the running process, read
from its executable's program interpreter, not guessed from installed loaders.

## Explicit library path

Pass a path to load exactly that library instead, for example one built from
source or downloaded from a GitHub Release:

```sh
bazel build //bindings/ffi:hawdb_ffi
```

```ts
const hawdb = loadHawDB("bazel-bin/bindings/ffi/libhawdb_ffi.dylib");
```

Use `libhawdb_ffi.so` on Linux and `hawdb_ffi.dll` on Windows. The adapter
does not use an environment variable or search path as a production control
plane. `HAWDB_LIBRARY` is used only by the repository test harness:

```sh
HAWDB_LIBRARY="$PWD/bazel-bin/bindings/ffi/libhawdb_ffi.dylib" \
  bun test bindings/bun
```

With `HAWDB_LIBRARY` set, `tests/npm-package.test.ts` assembles the npm
packages from that library, packs them, installs the tarballs into a fresh
`node_modules` tree and loads HawDB there without a path.
`tests/platform.test.ts` covers platform selection and needs no library.

## Publishing

The [Native Release workflow](../../.github/workflows/native-release.yml)
publishes `@nowledge/hawdb-bun` and the platform packages with npm trusted
publishing for every release, at the release version: stable releases under
`latest`, development prereleases under `dev` or `nightly` (see
[release modes](../../docs/PREBUILT_LIBRARIES.md#release-modes)).
[`scripts/assemble-npm.ts`](scripts/assemble-npm.ts) derives the publishable
manifests from this directory's `package.json`, which stays `private` so the
development workspace is never published directly. The one-time bootstrap and
trusted-publisher setup are described in
[npm trusted publishing](../../docs/PREBUILT_LIBRARIES.md#npm-trusted-publishing).

## API and ownership

- `loadHawDB(path?)` maps the shared library and registers the stable ordinary
  query symbols. Without a path it uses the installed platform package;
  `resolvePrebuiltLibrary()` returns that path without loading it.
- `library.open(path, options)` and `library.openInMemory()` return a
  database handle. `options` mirrors the C ABI `hawdb_open` options; omitted
  or `undefined` keys keep the native defaults:
  - `readOnly?: boolean` (default `false`);
  - `durability?: "sync_on_every_write" | "sync_on_checkpoint"` (default
    `"sync_on_every_write"`, power-loss safe; `"sync_on_checkpoint"` may lose
    recently acknowledged transactions after a power failure, never part of one);
  - `maxReadResultRows?: number | bigint | null` (default `null`; `0` or `null`
    means no row ceiling);
  - `maxReadResultPayloadBytes?: number | bigint` (default 64 MiB; must be
    positive);
  - `relationalMonotonicAppendFastPath?: boolean` (default `false`).

  Unknown keys throw `HawDBError` with kind `invalid_argument`; the native
  library validates values with the same kind. Reads over a budget throw
  instead of returning partial results.
- `database.query(cypher, parameters)` and `database.querySQL(sql, parameters)`
  return `QueryResult { columns, rows }`; `dicts()` converts rows to objects.
- `database.transaction((tx) => { ... })` runs the body as one atomic
  transaction: `tx.query`, `tx.querySQL` and `tx.exec` run inside it, it
  commits when the body returns (returning the body's value) and rolls back
  every statement when the body throws, rethrowing the error. The body must be
  synchronous; returning a promise rolls back and throws. While it runs,
  `database.query`/`querySQL`, a nested `database.transaction` and
  `database.close()` throw `HawDBError` with kind `transaction_open`, and a
  `tx` used after its body finished throws `no_transaction`. A commit is never
  partial, but a crash or an error while it publishes leaves the outcome
  unknown (the whole transaction may be committed after a reopen), so
  reconcile or retry idempotently instead of rerunning it blindly; see the
  [C ABI notes](../ffi/README.md#transactions).
- `database.backup(destination)` writes a consistent backup directory of the
  open database (the destination must not exist) and returns
  `{ generation, checkpoint_commit_epoch, file_count, total_bytes,
  manifest_checksum }`. `library.restore(backup, destination)` verifies the
  backup and restores it into a new directory, returning the same fields;
  open the result with `library.open`. `database.scrub()` verifies the
  database's durable files and WAL and returns `{ generation,
  checked_file_count, checked_bytes, sha256_verified_file_count,
  wal_record_count, wal_bytes }`; damage throws `HawDBError` and that handle
  then refuses statements until it is closed and reopened. Report counts are
  `number`, or `bigint` beyond JavaScript's safe range.
- Close databases explicitly before closing the library. Both close operations
  are idempotent. The library refuses to unload while a database is live.
- Every native result/error buffer is copied and released before a call returns.
  The adapter never exposes native buffer pointers to application code.
- Calls fail before native dispatch if parameter or option getters close their
  database or library while the arguments are being normalized.

- `database.export(destination)` writes the database's current snapshot as a
  logical dump directory, and `library.import(source, destination)` builds a
  new database from one. `library.formatVersion()` is the persistent format
  version and `library.dumpFormatVersion()` the newest dump version the
  library reads. Before updating to a library whose `formatVersion()` differs,
  export with the old library and import with the new one
  ([docs/LOGICAL_DUMP.md](../../docs/LOGICAL_DUMP.md)). Both calls refuse an
  existing destination, publish only on complete success, and return the C
  ABI's snake_case JSON report; counts are `number`, or `bigint` above 2^53.
  `library.verifyDump(path)` checks a dump without writing. A
  `publish_outcome_unknown` error means the complete result may already be at
  the destination: keep it, call `library.confirmDumpPublication(destination)`
  (idempotent) and then verify it; every other error published nothing.

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
