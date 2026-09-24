# Composition inventory and consumer baseline

This is the first evidence slice for [#647](https://github.com/nowledge-co/hawdb/issues/647),
not a shipped plugin contract or a package-size budget. The public integration
point remains `hawdb`. Runtime capability flags and omitted build dependencies
are different properties and are checked separately.

## Current boundaries

| Capability | Cargo inclusion today | Runtime boundary | What omission actually excludes |
| --- | --- | --- | --- |
| Cypher and graph mutations | Unconditional | Embedded query facade | No single-family build switch yet |
| SQL and relational storage | Unconditional | Embedded SQL facade | No SQL-free composition yet |
| Optimizer framework and graph/relational planning | Unconditional | Built-in optimizer wiring | `hawdb-cascades` is separate internally; no host-selected optimizer provider |
| In-memory and persistent storage | Same implementation dependency | Construction/open mode; WASM rejects persistent open | Selecting in-memory on a native host does not remove durable storage code from the dependency graph |
| Full-text search | `full-text-search` enables capability | Typed unavailable-capability error when disabled | Does not remove `hawdb-search`, Jieba, regex or zstd dependencies |
| Vector search | `vector-search` plus optional dependencies | Typed unavailable-capability error when disabled | Minimal builds omit `hawdb-vector-projection` and `simsimd` |
| Graph analytics | `graph-analytics` enables capability | Capability-controlled operations | `hawdb-analytics` remains reachable from storage/executor |
| Background maintenance | `background-maintenance` enables capability | Library scheduling/admission | Storage/checkpoint implementation remains compiled as a dependency |
| Tokio adapter | Optional `tokio-runtime` dependency | Host-owned runtime adapter | Minimal builds omit `hawdb-runtime-tokio` and Tokio |
| ACL / OpenTelemetry / qualification | Separate opt-in features | Separate contracts | Not enabled by the ordinary default profile; not qualified by this baseline |

Source anchors are the root and search `Cargo.toml`,
`src/compiled_capabilities.rs`, `crates/storage/Cargo.toml`,
`crates/executor/Cargo.toml`, and `crates/optimizer/Cargo.toml`. A public internal
trait or `#[doc(hidden)]` module is not automatically an approved host plugin API.

## One bounded measurement matrix

[`profiles.json`](../tools/consumer-profiles/profiles.json) is the executable
matrix for required/excluded dependencies and consumer feature selections.
[`Cargo.toml`](../tools/consumer-profiles/Cargo.toml) defines the feature wiring;
[`main.rs`](../tools/consumer-profiles/src/main.rs) is a facade-only host. Its
separate workspace prevents repository dev-dependencies from unifying features.

| Baseline | Consumer feature | Executed workload | Bazel counterpart today |
| --- | --- | --- | --- |
| Empty host | none | Prints one marker; no HawDB dependency | Not a database target |
| Current minimal | `database` | Parameterized graph write, relationship traversal, SQL write/read; rejected text/vector calls | `//:hawdb_minimal` |
| Graph/text | `text` | The same graph/SQL workload plus indexed text retrieval; rejected vector call | No dedicated matching graph/text preset yet |
| Current default | `full` | Graph/SQL, text/vector retrieval, projected PageRank, scheduled schema maintenance, and an admitted blocking database task on an owned Tokio runtime | `//:hawdb` |

“Minimal” names the existing no-default-feature surface, **not** the requested
future one-query-family/one-storage-backend composition. The graph/text host
still has SQL. The default workload touches the selected optional capabilities
but is not comprehensive workload or resource qualification. In particular, the
maintenance probe may have no pending schema work. These are measured host
programs, not upper bounds for every program linking the library.

## Reproduce the evidence

```sh
python3 scripts/measure-consumer-profiles.test.py
bazel test //:consumer_profile_evidence_test
python3 scripts/measure-consumer-profiles.py --inventory-only --output /tmp/hawdb-inventory
python3 scripts/measure-consumer-profiles.py --output /tmp/hawdb-footprint
```

Use a new output directory each time. The runner copies the facade-only host to
an external temporary workspace, copies the repository toolchain and lockfile,
and rejects compiler identity mismatch or a resolved dependency version/source
not already present in the root lockfile. Its first offline metadata resolution
adds the external host; subsequent compilation is offline and locked. Populate
the repository dependency cache first when running on a fresh machine.

Release settings are explicit: opt-level 3, thin LTO, one codegen unit, no debug
information, no stripping. Each selected feature set is built in a separate
Cargo invocation; sharing a compiler artifact cache does not unify features
across invocations. The report records the actual compiler, native linker,
target, matrix/source/lock hashes, runtime output and dependency witnesses.
The host asserts real query results and capabilities; an empty main cannot pass
as the database workload.

The native runner supports macOS and Linux. It measures the final executable,
inspects dynamic dependencies, rejects unbundled non-system libraries, and
packages that executable into a deterministic tar/gzip archive. System libraries
are listed separately and not shipped in that archive. Static native code is
already inside the executable. The report does not measure the Cargo target
directory or an unlinked `.rlib`. Compilers may still eliminate unused operations;
the workload and empty-host baseline are part of the result, not optional context.

No hardware-independent size ceiling is inferred. Per-target ceilings and
per-capability growth budgets require reviewed baselines, including additional
release targets and representative host workloads, before CI enforcement.

## Initial macOS arm64 observation

The [raw report](benchmarks/composition-macos-arm64-2026-09-25.json) records the
actual Rust 1.97.1 build and successful executions on macOS 27, with native linker
`ld-27037.1`. Library sources were main `a703cc0f18f549918c73f454cda521edb4f0fb13`;
the new external host and matrix are bound by their SHA-256 digests in the report.

| Host | Executable bytes | Packaged bytes | Normal dependency-closure nodes, including host |
| --- | ---: | ---: | ---: |
| Empty | 459,448 | 178,229 | 1 |
| Minimal | 22,080,232 | 9,714,507 | 124 |
| Graph/text | 22,183,384 | 9,754,192 | 124 |
| Default | 22,754,872 | 9,987,121 | 130 |

All dynamic dependencies in these artifacts are OS-provided libraries/frameworks;
no separately shipped native library was needed for this run. The minimal and
graph/text dependency closures have identical package sets, despite different
capability flags and executed search workloads. This supports the inventory's
claim that the text switch currently does not exclude its exclusive dependencies;
it does not prove a fixed per-feature byte cost. Binary sizes depend on workload,
linker and target, and no threshold is enforced from this single machine.

An initial discarded probe inherited the machine's newer default compiler.
The measured report above was rerun after copying the pinned toolchain to the
external workspace and checking compiler identity; the newer-compiler values are
not used here.

## Evidence-checker argument

For target-filtered Cargo metadata, choose an edge set `G`: normal edges for
the runtime dependency inventory, or normal plus build edges for compile-time
exclusion. Begin with the external host in a visited set and enqueue each
new reachable package once. Each discovered package has a witness formed by
appending one permitted edge to an already valid witness. Induction proves every
reported package reachable. Conversely, induction on shortest path length proves
all reachable packages are discovered when the finite queue is exhausted.
Cycles terminate because a package is enqueued only on its first visit.

A profile passes only if all required names occur in the normal closure and no
excluded name occurs in the larger normal-plus-build closure. A build-only
dependency cannot satisfy a required runtime package, but can violate exclusion;
dev-only edges are excluded from both closures. In addition,
normal proc-macro dependencies can occur in this graph even though they execute
at build time. This is a dependency-closure claim, not proof that every reachable
package contributes bytes to the final executable. Dynamic inspection and the
measured linked artifact are separate evidence. The ordinary script tests check
transitive forbidden dependencies, missing required dependencies, build-only
paths (including forbidden build-only dependencies), cycles, and 128 seeded graphs against an independent fixed-point oracle.

## Next implementation decisions

1. Establish actual compile-time exclusion for one useful query/storage profile.
   Start from the inventory rather than treating capability-disable errors as
   proof of absence. Add equivalent Cargo/Bazel presets and isolated consumers
   together; the graph/text Bazel gap remains explicit.
2. Keep the generic Cascades memo/rule framework internal and reusable. A reviewed
   host optimizer seam should carry identity, required/provided capabilities and
   bounded resources, with composition identity in plan-cache validity. A narrow
   trait-object selection point avoids multiplying public database generic types;
   keep generic implementation internals where they help static optimization.
   This is a proposed tradeoff, not an approved new public signature.
3. Distinguish an I/O/segment adapter from a database storage engine. The latter
   must own snapshot, transaction, commit, replay and lifetime contracts; an I/O
   adapter cannot demonstrate engine substitution. Infallible default construction
   should remain convenient, while unsupported provider compositions must fail
   before mutation through a reviewed fallible construction boundary.
4. Agree those provider boundaries before adding a public plugin API, then prove
   each with a real external implementation. Provider registration, duplicate and
   capability rejection, resource/cancellation ownership, semantic equivalence,
   recovery and cache isolation remain undelivered work.

This baseline does not authorize stable Mem inclusion, define a Rust plugin ABI,
change production defaults, or turn internal storage traits into integration
contracts. Keep #647 open.
