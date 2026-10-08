# Experimental browser runtime

HawDB's Rust facade can run an in-memory database on
`wasm32-unknown-unknown` with default features disabled. This is the runtime
foundation for #732, implemented by #733. It is a toy/development capability,
not a production browser database.

The consumer's Rust WASM package depends on the ordinary facade:

```toml
hawdb = { path = "../hawdb", default-features = false }
```

Construct `Database::new()` or `Database::new_with_config(...)`, then use
`query`, `query_with_params`, and the existing transaction APIs. A new handle
starts empty. The database lasts only as long as the WASM instance.
`Database::open*` rejects persistent storage on this target.

The consumer also owns allocator selection in its final WASM `cdylib`.
HawDB does not register an allocator or pull a native allocator into this
target. A host may inject a WASM-compatible `GlobalAlloc` implementation with
`#[global_allocator]`; without one, Rust's target default applies. Native Mem's
`mimalloc` selection does not carry across into a separate WASM module. See
[the allocator contract](ALLOCATORS.md) for target-gated host configuration and
the portable host-allocator regression.

The development example in `examples/wasm_playground.rs` supplies a thin
wasm-bindgen query bridge (#734). `examples/wasm-playground/` supplies the page
and a single **Dedicated Web Worker** (#735). It runs on the user's device,
not in Cloudflare Workers. Neither is a production browser SDK.

## Query playground

Install the pinned Rust toolchain, `wasm32-unknown-unknown` target, LLVM tools,
Python 3, Clang with a WASM backend, and the wasm-bindgen CLI matching
`Cargo.lock` (currently `0.2.126`):

```sh
rustup target add wasm32-unknown-unknown
rustup component add llvm-tools
cargo install wasm-bindgen-cli --version 0.2.126 --locked
bash scripts/build-wasm-playground.sh
python3 -m http.server 8080 --bind 127.0.0.1 --directory examples/wasm-playground
```

Open `http://127.0.0.1:8080/`. The build script uses locked dependencies,
disables default features, links a real `cdylib` example, and generates the
JS/WASM files into the ignored `examples/wasm-playground/pkg/` directory. It
selects Rust's LLVM archiver to avoid Apple's incompatible archive layout;
target-specific `CC_wasm32_unknown_unknown` and `AR_wasm32_unknown_unknown`
overrides remain available. Generated files are local build output, not a
public deployment or a release artifact.

Use **Create graph**, then **Read nodes** or **Traverse**, and press **Run
query** after selecting each example. One in-memory `Database` remains in one
Worker across submissions. Initialization and queries share one serial queue;
the page disables Run while a request is pending. Reloading creates an empty
database. Invalid queries display a structured error and allow another query.
The page inserts result text through `textContent`.
The page and smoke use the same Worker client. A WASM trap or unexpected bridge
exception permanently stops that Worker, rejects pending requests, and disables
Run. It never silently creates a replacement database. Reload is required and
clears the in-memory data; a query interrupted by a trap has an uncertain outcome.
Ordinary structured parse, query, and limit errors remain recoverable.

The Worker accepts `{id, type: "initialize"}` or
`{id, type: "execute", query: "..."}`. Initialization returns
`{id, status: "ready"}`. Execution returns either
`{id, status: "ok", columns: [...], rows: [[...]]}` or
`{id, status: "error", error: {kind, message}}`. Rows retain the facade's
column order and write outcomes. Every value uses `{type, value}`:

- `int64` and `float64` carry decimal strings; integers never pass through a
  JavaScript Number. Non-finite floats retain their string representation.
- `null`, `bool`, and `string` retain their corresponding JSON values.
- `binary` carries an array of byte values; `uuid` carries its canonical string.
- `list` and `map` contain recursively tagged values, including returned graph
  values. Rust database handles remain internal.

The example limits query input to 64 KiB, read results to 256 rows and 1 MiB
of engine-accounted payload, and encoded display output to 8 MiB. These are
independent budgets: value tags, column names, JSON escaping and nested values
add display overhead absent from the engine payload estimate. Wide boolean/null
results can therefore pass engine admission and fail the display limit. A
`response_limit` error explicitly reports `engine_query_succeeded: true` and
`max_encoded_bytes`, with no partial rows; return fewer columns/rows or smaller
nested values. The response buffer never grows past its encoded limit. It supplies
a five-second cooperative query deadline through the existing runtime.
Limit failures return errors without partial rows. Successful writes remain
committed even if their response cannot fit the display limit. These controls
are not a whole-process heap bound or preemptive cancellation guarantee.

### Repeatable playground smoke

After building and serving the page, run in the browser's developer console:

```js
await (await import("./smoke.js")).runSmoke()
```

This creates a separate real Worker and in-memory database, submits requests
before initialization completes, and checks serial write/read ordering,
relationship traversal, int64 precision, scalar/null/graph values,
parse/protocol error recovery, input admission, and result-limit failure with
subsequent recovery. It terminates its Worker afterward and returns the
browser user agent and request count. To check the page itself, submit the
three examples, an invalid query followed by a valid query, and then reload
and read again to confirm empty rows.

On 2026-10-02, the initial playground (`d320f426`) was exercised in Chrome
155 on macOS ARM64: the Worker smoke returned `passed` for 267 requests. The
page's write/read/traversal/error loop disabled Run during each submission;
the maximum int64 remained `9223372036854775807`. Reloading the page and reading
the prior fixture returned zero rows. The review follow-up artifact was also
exercised through the actual page: write/read/traversal, parse recovery, an
engine-admitted nested result exceeding the display budget, smaller-read recovery,
and reload-to-empty all passed. Seven JavaScript regressions use the same client
and queue implementation, injecting a `WebAssembly.RuntimeError` to check stopped
instances, pending-request rejection and initialization/transport failures. This
injection is not qualification of an actual engine panic. Native bridge and
JavaScript regressions (Node.js 22 or newer) are available with:

```sh
cargo test --locked -p hawdb --no-default-features --example wasm_playground
bazel test //:hawdb_wasm_playground_bridge_tests //:hawdb_in_memory_portable_tests
node --test examples/wasm-playground/worker.test.js
```

## Build

Use the repository's pinned Rust toolchain and lockfile:

```sh
rustup target add wasm32-unknown-unknown
cargo build --locked -p hawdb --lib --no-default-features --target wasm32-unknown-unknown
```

This builds a Rust library for a consuming WASM package; the consumer owns its
`cdylib` and wasm-bindgen JS exports. The playground build script above is the
standalone example using this same library.

The existing compression dependency still builds C code. A Clang with a
`wasm32` backend and an LLVM archiver are required. On macOS, Apple's `ar` can
produce archives which subsequently fail WASM linking with undefined `ZSTD_*`
symbols. Use `llvm-ar`, for example the one shipped in Rust's LLVM tools:

```sh
rustup component add llvm-tools
rust_host=$(rustc -vV | sed -n 's/^host: //p')
export CC_wasm32_unknown_unknown=clang
export AR_wasm32_unknown_unknown="$(rustc --print sysroot)/lib/rustlib/$rust_host/bin/llvm-ar"
```

Set these before the build/test commands. They affect only cross-compilation,
not the database's production control plane. `cargo check` alone does not
validate the C archive or the final WASM link.

## Browser verification

`tests/in_memory_portable.rs` exercises the real facade in a dedicated browser
Worker using wasm-bindgen-test. It checks parameterized graph writes and
traversal, error recovery, transaction rollback/commit, SQL UUIDv7 generation,
deadline cancellation, result limits, and explicit persistence/spill rejection.

Install the CLI version matching wasm-bindgen in `Cargo.lock` (currently
`0.2.126`), plus Chrome and a compatible ChromeDriver on `PATH`:

```sh
cargo install wasm-bindgen-cli --version 0.2.126 --locked
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  WASM_BINDGEN_USE_BROWSER=1 \
  cargo test --locked -p hawdb --no-default-features \
    --target wasm32-unknown-unknown --test in_memory_portable
```

The runner launches headless Chrome. `CHROMEDRIVER` can name an explicit driver
path. This is a runtime smoke test, not a comprehensive cross-browser promise.
The common tests also run natively:

```sh
cargo test --locked -p hawdb --no-default-features --test in_memory_portable
bazel test //:hawdb_in_memory_portable_tests
```

`tests/host_allocator.rs` is a separate consumer with an instrumented global
allocator. It checks graph/SQL allocation and database/result deallocation
through the public facade, plus Zstd context/workspace allocation and release,
on native and browser WASM builds. Keeping it separate
preserves the target-default allocator coverage in `in_memory_portable`.

```sh
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  WASM_BINDGEN_USE_BROWSER=1 \
  cargo test --locked -p hawdb --no-default-features \
    --target wasm32-unknown-unknown --test host_allocator
```

Run the affected native Bazel tests and the local fuzz checks required by
`AGENTS.md` when changing shared runtime code. Browser tests do not replace
native regression or recovery coverage.

## Supported boundary

- Query semantics come from the existing engine; no browser-specific parser or
  executor is introduced. Browser clocks back timing/deadlines and UUIDv7 uses
  browser cryptographic randomness. Hosts constructing deadlines can use
  `hawdb::time::Instant`; native builds re-export the standard-library types.
- Execution is serial. The executor's existing degraded/sequential path is
  used without trying to spawn native threads; segment-read waves also execute
  serially. Shared-memory WASM threads and cross-origin isolation are not needed.
- Memory/result limits remain enforced. Disk spill returns an explicit error,
  so queries that cannot complete within memory do not return partial success.
  In-memory mode does not promise every query will fit within the configured budget.
- Persistent storage, OPFS/IndexedDB, snapshots, full-text/vector search,
  background maintenance, Tokio, and multi-tab access are outside this initial
  target. Use `--no-default-features`; native defaults remain unchanged.
- Panic recovery, bundle-size targets, native performance parity, and production
  compatibility are not promised. This work does not authorize HawDB in stable
  Mem release artifacts.
