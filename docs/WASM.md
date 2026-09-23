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

The JS query bridge and playground are separate work (#734 and #735), and may
live in the consuming frontend repository. This change supplies no Worker
message protocol or deployment. A browser **Dedicated Web Worker** is the
intended host; it runs on the user's device, not in Cloudflare Workers.

## Build

Use the repository's pinned Rust toolchain and lockfile:

```sh
rustup target add wasm32-unknown-unknown
cargo build --locked -p hawdb --lib --no-default-features --target wasm32-unknown-unknown
```

This builds a Rust library for a consuming WASM package; the consumer owns its
`cdylib` and wasm-bindgen JS exports. It does not produce a standalone query UI.

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
