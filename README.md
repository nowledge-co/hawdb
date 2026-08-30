# Skein

Skein is an embedded Rust graph database intended for the Nowledge local graph
data plane. It uses Cypher as its query language and a Cascades-style optimizer
for deterministic, explainable planning.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the current design.
The staged implementation and compatibility gates are tracked in
[docs/EMBEDDED_DEVELOPMENT_PLAN.md](docs/EMBEDDED_DEVELOPMENT_PLAN.md).
Open development work is tracked in [TODO.md](TODO.md).

## Building

Build the default embedded library with the repository's locked dependency
versions:

```console
cargo build --locked -p skein
```

The default build makes full-text search, vector search, graph analytics, and
bounded background maintenance available. Build-time availability is only an
upper bound: the host must also enable a capability through the typed runtime
configuration. A capability omitted at build time cannot be restored at
runtime and fails with `SkeinError::CapabilityUnavailable` instead of silently
falling back.

Use a minimal build when the host only needs canonical graph storage, WAL and
recovery, transactions, parameterized Cypher, and incremental base indexes:

```console
cargo build --locked -p skein --no-default-features
```

Add back only the capabilities required by a constrained host:

```console
cargo build --locked -p skein --no-default-features \
  --features full-text-search,vector-search
```

### Cargo features

| Feature | Default | Purpose |
| --- | --- | --- |
| `full-text-search` | yes | Makes full-text indexing and search available to the runtime capability matrix. |
| `vector-search` | yes | Enables scalar vector search and Skein's bounded 4-bit TurboQuant candidate projection. Adaptive selection may use the projection, but final ranking always reads canonical raw vectors. |
| `graph-analytics` | yes | Makes bounded graph projection and analytics operations available. |
| `background-maintenance` | yes | Makes QoS-admitted background schema, index, projection, and maintenance work available. |
| `acl` | no | Compiles the optional access-control capability. The host must still provide fresh policy state and enable it at runtime; enabling this feature alone does not establish an authorization boundary. |
| `tokio-runtime` | no | Exposes the optional owned-or-borrowed Tokio adapter for asynchronous host integration. The synchronous embedded facade remains available without it. |
| `opentelemetry` | no | Compiles the host-injected OpenTelemetry metrics adapter. It does not install a global provider, create an OTLP exporter, read an endpoint, or start a network worker. This feature is restricted to nightly non-production monitoring builds. |
| `qualification` | no | Compiles validation-only candidate ingress used by offline qualification crates. It is not a serving capability. |
| `loom-tests` | no | Enables Loom-only concurrency model tests. It is a validation feature, not an application capability. |

The optional `skein-qualification/turbovec-oracle` feature owns the upstream
`turbovec` dependency for offline differential qualification. It is not a
`skein` serving feature and does not publish a production search artifact.

Compile a nightly non-production monitoring variant with the metrics adapter and
Tokio integration explicitly:

```console
cargo build --locked --release -p skein \
  --features opentelemetry,tokio-runtime
```

The nightly host owns the OpenTelemetry SDK, bounded exporter queue, OTLP
endpoint, credentials, shutdown, and flush lifecycle. Skein only records
low-cardinality metrics through the supplied `Meter` and `TelemetrySink`; it
does not export query text, parameters, document identifiers, or database paths.

Production packaging must use an explicit feature allowlist and must not use
`--all-features`, because `opentelemetry` and test-only features are deliberately
outside the production build:

```console
cargo build --locked --release -p skein --no-default-features \
  --features background-maintenance,full-text-search,graph-analytics,vector-search
```

`cargo build --all-features` and `cargo clippy --all-features` remain useful for
development and CI compile coverage, but their outputs are not production
artifacts.

### Bazel validation

Build the embedded library and run every Bazel unit-test target with the default
repository configuration:

```console
bazel build //:skein
bazel test --test_output=errors //...
```

The repository configuration selects Bazel's hermetic `remotejdk_21` runtime
for Java-backed rules, including `rules_tla`; callers do not need to configure
`JAVA_HOME`.

The Bazel graph covers the root library and CLI tests plus every Cargo workspace
crate, including the Tokio runtime, Linux cgroup parser, optimizer fuzz library,
vector projection, and synthetic qualification workload. CI checks that every
Cargo workspace package has a `BUILD.bazel` file and an explicit `rust_test`
target before running the reviewed target lists, so a newly added crate cannot
be silently omitted from Bazel testing.

Bazel unit tests complement rather than replace Cargo feature-matrix, doctest,
ignored resource-profile, Loom, scheduled fuzz, and release soak jobs. Those
remain separate gates because they require different features, platforms, or
runtime inputs.

### Local fuzzing

Run the deterministic fuzz regression and CLI contracts with Bazel:

```console
bazel test //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
```

Fuzz campaigns keep successful console output quiet and write detailed JSON to
`target/fuzz-logs/*-cur.json`. A failure also preserves a
`*-failure.json` reproduction report and prints its path to stderr. Use
`--log-directory <path>` to select another artifact directory or
`--print-report` to explicitly copy the machine-readable report to stdout.

The default `vector-search` implementation keeps raw embeddings canonical and
publishes an immutable, checksummed `search_turboquant.<generation>.skein`
candidate projection. Projection construction is segment-bounded, filtered
scans use a compact allowlist bitmap, and the scalar, AVX2, or NEON kernel is
selected for each query without changing the artifact. Parallel segment scans
are opt-in through a caller-supplied limit and memory budget; Skein does not
create a global vector-search thread pool. See
[`docs/specs/TURBOQUANT_VECTOR_PROJECTION_SPEC.md`](docs/specs/TURBOQUANT_VECTOR_PROJECTION_SPEC.md)
for the algorithm, recovery, and readiness contract.

## Production Boundary

Skein is intended to be embedded by Mem as a Rust library. Production callers
should open Skein in-process and consume typed readiness APIs such as
`nowledge_mem_final_cutover_preflight`; they should not shell out to the `skein`
binary for read routing, migration gates, or previous-wrapper comparison.

Application graph reads and mutations use parameterized Cypher; relational
operations use PostgreSQL-dialect SQL. Skein does not expose route-shaped
`read_graph_*` methods or business-specific CRUD batch facades in release
builds. Typed APIs are reserved for stable multi-statement kernel boundaries
such as transactions, recovery, projections, import, bounded retrieval, QoS,
and readiness. See
[`docs/specs/QUERY_FIRST_PUBLIC_API_SPEC.md`](docs/specs/QUERY_FIRST_PUBLIC_API_SPEC.md).

Compatibility commands that execute external previous-wrapper or shadow compare
processes are quarantined as developer/preflight tools. They require
`SKEIN_ENABLE_COMPATIBILITY_TOOLS=1` and are only for isolated CI, release, or
nightly validation against copied data.
