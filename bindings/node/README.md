# HawDB for Node.js and Electron

This binding embeds HawDB through Node-API 8. It statically links the existing
Rust C ABI implementation; it launches no database executable, runs no Bun
runtime, and requires no companion shared library. Its native object owns the
database handle and releases it on explicit close or garbage collection.

The package is `@nowledge/hawdb-node`. Publication requires the one-time setup
below; adding this binding does not mean a package is already on npm.

## Use

```js
import { open } from "@nowledge/hawdb-node";

const database = open("/path/to/database");
database.exec("CREATE (:Note {id: $id, body: $body})", { id: 1, body: "Hello" });
console.log(database.query("MATCH (n:Note) RETURN n.body AS body LIMIT 50").dicts());

database.transaction((tx) => {
  tx.querySQL("CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT)");
  tx.querySQL("INSERT INTO notes (id, body) VALUES ($1, $2)", [1, "Hello"]);
});
database.close();
```

Node.js 22.16+ and ESM are supported. A CommonJS application can use dynamic
`import()`. `openInMemory()` opens an independent in-memory database. All calls
are synchronous, including transaction callbacks; use a dedicated Worker or
Electron utility process for long queries. Each database must have one owner;
do not pass native objects between workers or open a writable path independently
in multiple app processes. Serialize requests to that owner.

`query` takes a named parameter object; `querySQL` takes positional parameters.
Results have `columns`, positional `rows` and `dicts()`. Signed int64 values
outside the safe JavaScript integer range are `bigint`; use `bigint` parameters
for those values. Binary values use `Uint8Array`. Decimal, UUID, temporal and
vector values retain the [C ABI tagged forms](../ffi/README.md#value-encoding).
Unsafe numeric integers, sparse arrays and undefined parameter values refuse.
The package build compiles the Bun adapter's pure lossless codec and platform
table; neither generated module imports `bun:ffi`.

Open options are `readOnly`, `durability`, `maxReadResultRows`,
`maxReadIntermediateRows`, `maxReadResultPayloadBytes` and
`relationalMonotonicAppendFastPath`, mapped to the corresponding
[C ABI options](../ffi/README.md#open-options). Unknown options refuse.
Omitted options retain embedded defaults, including power-loss-safe
`sync_on_every_write` commits. The in-memory open uses native defaults.
Budget exhaustion throws rather than returning a partial result. Result JSON
and IPC introduce host-side copies; native budgets do not bound the entire
JavaScript heap.

`HawDBError` exposes `kind`, `message` and `details`, including structured
resource refusals. A transaction callback commits on normal return, rolls back
on a thrown error, and refuses returned promises/thenables. Its transaction
argument expires after the callback. Use that argument for all its statements;
nested transactions, ordinary database queries, maintenance and close refuse
while the callback is active. Close is otherwise idempotent. A failure while
commit publishes may leave the complete transaction committed: reconcile or
retry idempotently, rather than interpreting every error as a rollback.

`database.backup(destination)`, `database.scrub()`, `restore(source, destination)`,
`database.export(destination)`, `importDump(source, destination)`,
`verifyDump(path)` and `confirmDumpPublication(destination)` preserve the C ABI
contracts and return its snake_case reports. `version()`, `formatVersion()`
and `dumpFormatVersion()` report the linked library. The on-disk format carries
no compatibility promise until HawDB declares one; export with the old version
and import with the new version when the persistent format version changes.
Preserve an export/import destination on `publish_outcome_unknown`, confirm its
publication and verify it; see [logical dumps](../../docs/LOGICAL_DUMP.md).

## Electron

Load this package in a utility process. Keep the renderer sandboxed and expose
only the application's bounded operations through preload/IPC. The included
[worker](examples/electron/worker.cjs) is a small note-store example with
parameterized SQL/Cypher and fixed result limits. The main process can start it:

```js
const { app, utilityProcess } = require("electron");
const path = require("node:path");

app.whenReady().then(() => {
  const database = utilityProcess.fork(path.join(__dirname, "worker.cjs"));
  database.on("message", (reply) => console.log(reply));
  database.postMessage({
    requestId: 1,
    operation: "open",
    path: path.join(app.getPath("userData"), "hawdb"),
  });
});
```

The application should correlate replies and wait for open before sending
operations. Close the database before terminating its utility process.
Node-API reduces dependence on V8/Node ABIs, but does not replace Electron
runtime validation. Package `**/*.node` outside ASAR, for example with
`asar: { unpack: "**/*.node" }` in Electron Packager, and include it in the
application's normal signing/notarization process. Keep writable databases
outside the application bundle/ASAR. The CI fixture runs the installed npm
package in an actual Electron utility process, then repeats SQL/Cypher,
transactions, bigint IPC and persistent reopen in an ASAR-packaged application.

## Build and validation

From the repository root, using its pinned Rust toolchain and ordinary profile:

```sh
cargo build --locked --manifest-path bindings/node/Cargo.toml
cd bindings/node
npm ci
npm run build -- --native target/debug/libhawdb_node.dylib
npm run typecheck
npm test
npm run test:electron
```

Use `target/debug/libhawdb_node.so` on Linux or `target/debug/hawdb_node.dll`
on Windows. On a headless Linux runner, use `xvfb-run -a npm run test:electron`.
The fixture's Linux `--no-sandbox` flag is restricted to this CI test; it is not
an application configuration recommendation. Tests create independent temporary
database paths. Failures of Electron qualification preserve their artifacts.

Lint this independent workspace explicitly:

```sh
cargo fmt --manifest-path bindings/node/Cargo.toml -- --check
cargo clippy --locked --manifest-path bindings/node/Cargo.toml --all-targets --all-features -- -D warnings
```

## Prebuilt packages and publishing

The Node Bindings workflow builds and tests macOS arm64/x64, Linux glibc
arm64/x64, and Windows x64. Each `@nowledge/hawdb-node-<platform>` package owns
one `hawdb.node`; the portable root package pins the matching optional
dependencies at exactly its version. There is no install-time compilation or
download script. Missing packages, unsupported platforms and version drift
refuse instead of searching for another library. PR artifacts use the checked
development profile; release artifacts use the repository's global release
profile. Linux releases currently require Ubuntu 24.04-compatible glibc;
macOS releases use deployment target 13.0. Windows arm64 and musl are not covered.

For the first release, reserve the six new npm package names and configure each
trusted publisher for `nowledge-co/hawdb`, workflow `node-bindings.yml`, environment
`npm`. Then set repository variable `NPM_NODE_TRUSTED_PUBLISHING=enabled`.
The existing Bun publishing variable does not authorize these new names.
Until configured, release validation still builds/tests and retains artifacts,
but skips npm publication. PR and validation-only dispatch runs cannot publish.
Release dispatch requires an existing, non-draft GitHub Release whose tag and
prerelease flag match the engine version. Publication reuses the existing
idempotent npm publish helper and release-mode checks: `latest` for stable,
`dev`/`nightly` for their matching prereleases. A partial publication is retried
only with identical tarballs. Publishing HawDB packages does not authorize
including HawDB in stable Mem artifacts.
