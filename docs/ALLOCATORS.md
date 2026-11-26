# Host-owned memory allocation

HawDB is an embedded Rust library. It does not register `#[global_allocator]`,
depend on a custom allocator implementation, or provide an allocator Cargo
feature. The host injects its allocator into the final Rust binary or WASM
`cdylib`, using Rust's stable `GlobalAlloc` contract. HawDB's Rust containers,
query execution, returned Rust values, and Zstd contexts inherit that selection.

"No default allocator" means no HawDB-selected implementation. A host that does
not inject one still uses Rust's target default; HawDB does not reject database
creation in that case. There is exactly one global selection in each linked
Rust artifact. It applies to all databases in that artifact and cannot be
changed through `DatabaseConfig` or switched after allocations are live.

## Native Mem alignment

At Mem main commit `f91c818423453a837b8b133de414c77c882ba10f`,
`nmem-rs/crates/nmem-server/src/bin/nmem_server.rs` registers
`mimalloc::MiMalloc` behind the server's default-on `mimalloc` feature.
`nmem-rs/crates/nmem-server/Cargo.toml` declares `mimalloc = "0.1"` with
`default-features = false`, and `nmem-rs/Cargo.lock` resolves `0.1.52`.
These are host settings, not HawDB dependencies or defaults.

Embedding HawDB into that Rust binary already shares the server's allocator.
Do not add a second registration to HawDB or the Mem library integration.
An independent native host can make the same choice in its own manifest:

```toml
[dependencies]
hawdb = { version = "0.26", default-features = false }

[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
mimalloc = { version = "0.1", default-features = false }
```

Register it in the host's binary crate:

```rust
#[cfg(not(target_arch = "wasm32"))]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

Allocator tuning, initialization, and shutdown remain host responsibilities.
HawDB does not set allocator environment variables or install process hooks.

## Browser WASM

The `wasm32-unknown-unknown` consumer owns its final `cdylib` and allocator.
Use `hawdb` with `default-features = false`, as described in [WASM.md](WASM.md).
Select a backend that supports WASM linear memory and the host's threading
model. A native allocator's availability does not establish browser support.

For example, a host can explicitly choose `dlmalloc` for WASM while using the
native selection above:

```toml
[target.'cfg(all(target_arch = "wasm32", target_os = "unknown"))'.dependencies]
dlmalloc = { version = "0.2", features = ["global"] }
```

```rust
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[global_allocator]
static ALLOCATOR: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;
```

The host can substitute any compatible `GlobalAlloc` implementation. Custom
implementations must uphold layout/alignment and allocation/deallocation
ownership, avoid allocating recursively inside allocator callbacks, and never
unwind from those callbacks. They must support every thread that can use the
artifact. The browser runtime currently executes queries serially in a
Dedicated Worker; this does not qualify shared-memory WASM threading.

The registration lives in the WASM Rust package, not JavaScript. Each separately
linked native library or WASM module has its own allocation boundary. Free Rust
values through their owning artifact; exported byte buffers and handles need
matching release APIs. Host allocator selection does not make WASM linear memory
shrink after deallocation, nor turn query memory budgets into a whole-heap cap.

## Coverage boundary

Rust global allocator injection covers HawDB's ordinary Rust heap allocations.
The default `builtin-zstd` backend creates its compression/decompression contexts with explicit
`ZSTD_customMem` callbacks. The internal codec in `hawdb-storage` uses Rust
`alloc`/`dealloc` for both native and WASM context workspaces. Its aligned prefix
stores the original allocation size, so freeing on another thread recovers the
same layout without relying on the current query or a thread-local allocator.
WASM uses these callbacks directly; it does not add a second allocation prefix
through the dependency's default malloc shim.

This follows seekdb's library-level [Zstd allocator adapter](https://github.com/oceanbase/seekdb/blob/1e113252b81528e9334e485496c7b6a878123b34/src/oblib/lib/compress/ob_compress_util.h).
HawDB does not install seekdb's process-wide malloc hooks or malloc zones.
The [Zstd custom-memory API](https://github.com/facebook/zstd/blob/v1.5.7/lib/zstd.h)
requires static linking. The internal bridge uses two private ABI declarations,
uses the static archive linked by `zstd-sys`, and accepts qualified 1.5.7 and
1.6.0 bindings. The linked library must match the binding version before context
construction. The bridge does
not declare another native-library link or bundle a second archive into
`hawdb-storage`. Do not substitute a dynamic Zstd library through pkg-config.

HawDB writes modern Zstd frames. Pre-1.0 legacy frame magic is rejected before
native decoding, because those historical decoders bypass custom-memory
callbacks. Concatenated modern and skippable frames remain supported.
The codec exposes no dictionaries or native compression workers. Existing
workspace admission leases remain at their callers; callbacks do not charge
the same workspace again. The header overhead fits within the qualified
conservative search workspace allowances.

Injection does not redirect arbitrary C/C++ `malloc`, memory mappings, external
runtimes, or independently linked libraries. Each future native dependency
needs its own explicit allocator adapter and lifetime qualification.

The contract does not provide per-database heaps, allocation-failure recovery,
or a custom allocator requirement at database construction. Rust allocation
failure can terminate the native artifact or trap WASM; existing query admission
and result limits are independent controls.

## Host-provided compression

`hawdb::compression::install_compression_backend(Arc<dyn CompressionBackend>)`
selects one process-wide implementation before the first compression operation.
First use of the built-in backend also fixes the choice. Concurrent installation
and first use have one winner; further installation returns
`CompressionBackendAlreadyInitialized`, including after every database closes.
There is no environment-variable override or runtime replacement. The registry
does not allocate a persistent heap object for the default backend.

`builtin-zstd` is enabled by default. To remove HawDB's native Zstd dependency,
disable that feature and install a host implementation before compressed reads,
writes or full-text dictionary loading. With no backend, operations requiring
compression return an error and installation remains possible. A failed
dictionary load does not poison the process cache. Ordinary uncompressed
operations can still run. Keep `builtin-zstd` explicit in existing minimal builds
that need the default codec:

```toml
[dependencies]
hawdb = { version = "0.40", default-features = false, features = ["builtin-zstd"] }
```

For an injected implementation, omit `builtin-zstd`. All normal dependency paths
to storage disable its defaults, so a different internal consumer cannot
silently re-enable it. Cargo's feature unification still applies if the host
explicitly enables the feature through another dependency. The facade exports
the provider/context traits, `EncoderState`/`DecoderState`, `CodecProgress`,
`EncodeOperation`, parsed `FrameHeader`, and `CheckpointWorkContext`.

The provider creates independent owned streaming contexts and supplies checked
workspace bounds. `EncoderState::new` and `DecoderState::new` admit their boxed
Rust state before allocation and release it before refunding its permit.
Additional native/host allocations remain the implementation's responsibility;
checkpoint factories use `work.reserve_memory` and preserve its typed errors.
Search reserves provider bounds before construction/growth, then checks retained
decoder memory. Buffer replacement must fit the maximum retained bound. Provider
bounds include native/host bookkeeping; they exclude the separately admitted
Rust I/O buffer. A host's foreign allocator is not redirected automatically.

Implementations must emit interoperable Zstd frames and preserve flush, pledged
size, truncation, modern/skippable frame and reset semantics. Invalid reported
positions, stalled streams and post-failure writes are refused. Changing the
implementation across process restarts does not change the persistent format.
ABI/version guards apply only to the built-in adapter. See the
[design contract](specs/COMPRESSION_BACKEND_SPEC.md) and the independent
`tests/compression-consumer` fixture. Its host adapter uses deliberately coarse
workspace admission for testing and is not a production allocator example.

The `Zstd Compatibility` workflow verifies a normal dependency graph containing
no `zstd`, `zstd-safe` or `zstd-sys`, missing-provider errors and installation
races. With `host-zstd`, the same independent workspace checks a host-selected
implementation, frame interoperability, invalid progress/failure fencing and
dictionary loading. Separate processes write a checkpoint with the built-in
backend, reopen/mutate/checkpoint with the host backend, and reopen it again with
the built-in backend. These fixtures do not inherit workspace dev features.

## Built-in Zstd ABI requalification

The compile-time version assertions accept only Zstd 1.5.7 and 1.6.0. They
intentionally reject other patch/minor versions until requalified. Runtime
checks also reject a library whose version differs from the bindings, even if
both versions are individually qualified. Do not widen these guards just to
make a dependency update compile. Before accepting a new version:

1. Compare `ZSTD_createCCtx_advanced` and `ZSTD_createDCtx_advanced` in the new
   `zstd.h` with the private declarations in
   `crates/storage/src/compression/context.rs`. Check each parameter, return
   type, and calling convention, and confirm that the static archive exports
   both symbols.
2. Compare `ZSTD_customMem` with `CustomMemory`: callback signatures, field types
   and order, struct size, and alignment must match on native and WASM targets.
3. Recheck modern/skippable frame boundaries and legacy decoder allocation
   paths. Legacy frame magic must still be rejected before any decoder that
   bypasses the host callbacks is created. Requalify context/workspace sizing
   against the existing search memory envelopes, including allocation headers.
4. Update the qualified compile-time and runtime version guards together in
   storage's `compression/context.rs` and `compression/native.rs`. Search obtains
   its bounds through the selected provider instead of depending on Zstd itself.
   Rerun the codec and host allocator suites listed below, including the
   `legacy_magic_is_rejected_before_creating_a_libc_owned_decoder` regression.
   Run native default/minimal host tests and the browser WASM host tests; verify
   encoding compatibility and balanced allocation/deallocation in each case.

### Zstd 1.6.0 qualification (#1988)

The repository lock retains `zstd 0.14.4` / `zstd-safe 8.3.0` /
`zstd-sys 2.1.3+zstd.1.5.7`. The independent consumer fixture instead requires
`zstd 0.14.5` and `zstd-sys 2.2.0+zstd.1.6.0`, starting without a lockfile.
This reproduces the dependency resolution that a locked workspace build misses.
Cargo's single `links = "zstd"` owner must serve both HawDB and the host.

The 1.6.0 vendored source retains the two advanced constructor signatures and
the allocation/free/opaque field order. `tests/zstd-consumer/custom_memory_abi.c`
checks the signatures, offsets, size and alignment against the actual dependency
header for native and `wasm32-unknown-unknown` compilation. Linked consumer tests
exercise both constructors, host callbacks and destruction.

Source comparison with 1.5.7 found the compression-level parameter table and
`ZSTD_decodingBufferSize_internal` formula unchanged. The streaming decoder still
frees its old input/output buffer before allocating a replacement and routes the
context and buffers through `ZSTD_customMem`. Legacy decoders remain excluded.
The existing 256-KiB context and frame-derived buffer allowances are retained;
decoder tests exercise exact and one-byte-short admission. Storage's
`level_three_native_allocations_fit_search_workspace_admission` additionally
counts simultaneous native allocations **including Rust callback headers** for
empty, small, block-sized and multi-window inputs, with and without pledged
sizes, against the 8-MiB native allowance. Search separately reserves its 32-KiB
writer buffer. This qualifies these codec paths, not arbitrary Zstd options.

The `Zstd Compatibility` workflow owns the fresh consumer's default/minimal
native and minimal browser-worker tests, header ABI probes, and the codec/search
regressions with an ephemeral 1.6.0 workspace lock. Ordinary locked CI retains
1.5.7 coverage. Generated consumer locks are ignored locally and retained as CI
artifacts; they must not replace the fresh-resolution check. Replay from the
repository root:

```sh
cargo generate-lockfile --manifest-path tests/zstd-consumer/Cargo.toml
cargo tree --locked --manifest-path tests/zstd-consumer/Cargo.toml -i zstd-sys
cargo test --locked --manifest-path tests/zstd-consumer/Cargo.toml
cargo test --locked --manifest-path tests/zstd-consumer/Cargo.toml --no-default-features
cargo fmt --manifest-path tests/zstd-consumer/Cargo.toml -- --check
cargo clippy --locked --manifest-path tests/zstd-consumer/Cargo.toml --all-targets --all-features -- -D warnings
```

For browser replay, use the compiler/archiver/runner environment below and add
`--no-default-features --target wasm32-unknown-unknown` to the consumer command.
The runner must match the fixture's resolved `wasm-bindgen` version. The fixture
is an unpublished independent test workspace, excluded from library archives.

## Verification

`tests/host_allocator.rs` registers an instrumented allocator in a separate
consumer test artifact, forwarding to Rust's `System` backend on each target.
It verifies that graph and SQL operations allocate through the host, that
database/result teardown deallocates through it, and that native Zstd context
and compression workspace allocations have matching host deallocations.
Codec unit tests cover alignment, zero-size/overflow/null handling, allocation
failures, damaged/truncated frames (including 1-3 byte magic prefixes), sink
failures, cross-thread destruction, and byte-for-byte encoding compatibility
with the existing Zstd stream codec.
Assertions concern observable allocator activity, not a total-memory estimate.
`tests/in_memory_portable.rs` retains coverage without an injected allocator.

```sh
cargo test --locked -p hawdb --test host_allocator
cargo test --locked -p hawdb --no-default-features --features builtin-zstd --test host_allocator
cargo test --locked -p hawdb-storage --lib compression::tests
bazel test //:hawdb_host_allocator_tests //:hawdb_host_allocator_minimal_tests
```

For a browser run, use the compiler, LLVM archiver, matching wasm-bindgen CLI,
Chrome, and ChromeDriver setup in [WASM.md](WASM.md), then run:

```sh
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  WASM_BINDGEN_USE_BROWSER=1 \
  cargo test --locked -p hawdb --no-default-features --features builtin-zstd \
    --target wasm32-unknown-unknown --test host_allocator

cargo clippy --locked -p hawdb --no-default-features --features builtin-zstd \
  --target wasm32-unknown-unknown --lib \
  --test in_memory_portable --test host_allocator -- -D warnings
```

These checks qualify host registration and the facade paths exercised. A host
must additionally build and test its chosen backend on each supported target.
No allocator is added to HawDB's production dependency graph by these fixtures.
