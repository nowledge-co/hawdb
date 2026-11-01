# HawDB

[![Website](https://img.shields.io/badge/website-hawdb.ai-111111?style=flat)](https://hawdb.ai)
[![License](https://img.shields.io/github/license/nowledge-co/hawdb?style=flat)](LICENSE)
[![Release](https://img.shields.io/github/v/release/nowledge-co/hawdb?style=flat)](https://github.com/nowledge-co/hawdb/releases/latest)
[![Rust](https://img.shields.io/badge/rust-1.97.1-dea584?style=flat&logo=rust&logoColor=white)](rust-toolchain.toml)

HawDB is an AI-native embedded database for agents. It keeps the facts an agent reasons over — entities, relationships, documents, and events — in one crash-safe process, returns them as bounded context, and gives each agent its own durable branch to work in.

- **Context layer.** Graph structure, relational rows, lexical search, and vector search share one engine. A retrieval request takes search candidates, filters them, walks the authorized graph, reranks with a typed score, and hydrates the result from the canonical snapshot. See [Context, in one pipeline](#context-in-one-pipeline).
- **Built for agent workloads.** Agents talk to the database through parameterized Cypher and SQL. Every read has an explicit row and payload budget, and an over-budget request fails instead of returning a silently truncated answer. A read transaction pins one published snapshot, so a multi-step plan sees one consistent state.
- **Branches for agents.** A copy-on-write branch costs metadata and an empty WAL, not a copy of the data. An agent can try a plan, write intermediate facts, or run a long task on its own branch without disturbing `main` or other agents. See [Branches for agents](#branches-for-agents).

HawDB is written in Rust and licensed under Apache-2.0. It is built for Nowledge Mem, currently in nightly integration. The library is the product: any host that needs a durable context layer opens it the same way.

The site and in-browser playground are at [hawdb.ai](https://hawdb.ai). Their source is [nowledge-co/hawdb-website](https://github.com/nowledge-co/hawdb-website).

For synchronous SQL materialized views, see [the materialized view guide](docs/MATERIALIZED_VIEWS.md).

## Install

HawDB is a Rust library. Published releases are on crates.io. The repository pins Rust 1.97.1.

```toml
[dependencies]
hawdb = "0.24"
```

To follow the repository tip instead of a release:

```toml
hawdb = { git = "https://github.com/nowledge-co/hawdb" }
```

Or build the default library from a checkout:

```console
cargo build --locked -p hawdb
```

The default build includes full-text search, vector search, graph analytics, background maintenance, and the Tokio adapter. A capability left out of the build cannot be turned back on at runtime. See [Build](#build).

Python bindings live in-tree and are distributed as `hawdb` on PyPI. See [Python](#python).

## Open a database

`Database::open` creates or recovers a durable project directory. One process owns that directory. Inside the process, reads and writes are transactional.

```rust
use hawdb::{Database, Value};
use std::collections::BTreeMap;

fn main() -> hawdb::Result<()> {
    let mut db = Database::open("./knowledge")?;

    let mut note = BTreeMap::new();
    note.insert("title".into(), Value::String("Graph foundations".into()));

    let mut tx = db.begin_transaction()?;
    tx.query_with_params(
        "CREATE (:Note {title: $title})-[:MENTIONS]->(:Entity {name: 'context layer'})",
        &note,
    )?;
    tx.commit()?;

    let found = db.query(
        "MATCH (note:Note)-[:MENTIONS]->(entity:Entity)
         RETURN note.title AS title, entity.name AS name
         ORDER BY title",
    )?;
    println!("{:?}", found.rows.get(0, "title"));
    Ok(())
}
```

The same program is [`examples/open_database.rs`](examples/open_database.rs). The relationship is created in that statement together with both endpoints.

`Database::new` opens an in-memory database with the same query API. Parameterized statements are the application interface. `EXPLAIN` returns the physical plan chosen by the Cascades-style optimizer.

Relational facts use PostgreSQL-dialect SQL on the same handle:

```rust
db.query_sql(
    "CREATE TABLE events (
        stream_id TEXT NOT NULL,
        sequence BIGINT NOT NULL,
        payload BYTEA NOT NULL,
        PRIMARY KEY (stream_id, sequence)
    ) WITH (
        storage_mode = 'strict_append',
        partition_key = 'stream_id',
        order_key = 'sequence'
    )",
)?;
```

Strict-append tables accept parameterized `INSERT` only. Reads name the partition, walk the order key, and set `LIMIT`. Generated `commit_sequence` keys are assigned at the durable commit boundary. The contract is in [`docs/specs/QUERY_FIRST_PUBLIC_API_SPEC.md`](docs/specs/QUERY_FIRST_PUBLIC_API_SPEC.md).

## Context, in one pipeline

Search hits enter the pipeline as candidates.

`Database::retrieve_knowledge` and `DatabaseReadTransaction::retrieve_knowledge` run one fixed pipeline against a pinned graph snapshot:

1. `search_candidate` — lexical or vector hits from the published search projection
2. `metadata_filter` — relevance predicates on projection metadata
3. `authorized_graph_expand` — neighborhood expansion that rechecks scope on canonical nodes
4. `rerank` — `Max`, a weighted sum, or a host-supplied `ScoringSpec`
5. `top_k` — truncate before any output entity is built
6. `canonical_hydration` — load identity, properties, and graph context from the snapshot

A projection epoch is reported beside the graph commit epoch. A stale search hit that no longer matches canonical identity is dropped and counted. Missing score features are reported on the candidate; they are never treated as zero. The whole request fails if it exceeds the query memory budget or the result payload budget. Partial context is not returned.

`ScoringSpec` is a weighted sum of features, each optionally decayed. Features are the search score, the graph-seed score, hop distance, a numeric node property, and a timestamp aged against the request clock. Changing weights does not invalidate a cached plan.

The stage order, snapshot binding, and fail-closed budgets are specified in [`docs/specs/KNOWLEDGE_RETRIEVAL_PIPELINE_SPEC.md`](docs/specs/KNOWLEDGE_RETRIEVAL_PIPELINE_SPEC.md) and modeled in `docs/tla/HawDBKnowledgeRetrievalPipeline.tla`.

```text
host process
│
├─ Cypher ────────────── canonical graph   (WAL, MVCC, checkpoints)
├─ PostgreSQL SQL ────── canonical rows    (tables, strict-append logs)
│
└─ retrieve_knowledge
     search projection ──► filter ──► authorized expand
                                      │
                                      ▼
                              rerank ──► top-k ──► hydrate from the snapshot
```

## Branches for agents

An agent often needs its own copy of the world: to explore a plan, stage facts it is not yet sure of, or run a long task while the user and other agents keep reading `main`. A HawDB branch is that copy.

- **Cheap to create.** A new branch shares the parent's sealed durable root and writes only catalog metadata and an empty branch-local WAL. It does not scan, import, or copy the parent's data, so creation cost does not grow with the database.
- **Fully writable and isolated.** Cypher and SQL reads, writes, and branch-local schema changes run on the selected branch exactly as on `main`. A branch can itself be branched.
- **Named for agents.** Omitting the name generates `agent/<branch-uuid>`; the `agent/` prefix is reserved for these generated names. Every branch also has a stable UUID that is never reused.
- **Safe to retry.** Creation carries a request key, so a retry after a lost response returns the same branch instead of a duplicate. Dropping a branch requires its UUID and metadata revision, so a delayed request cannot delete a newer branch that reused the name.
- **Durable until dropped.** Branches survive handle closure, process crashes, and restarts. There is no TTL; an idle branch keeps its data until it is explicitly dropped.

Branch lifecycle and selection are SQL statements on the same handle:

```sql
CREATE BRANCH FROM main AT REVISION $1 REQUEST KEY $2;  -- returns branch_id, metadata_revision, ...
USE BRANCH ID $1;
SHOW CURRENT BRANCH;
SHOW BRANCHES LIMIT 10;
DROP BRANCH ID $1 AT REVISION $2;
```

`AT REVISION` is the source branch's committed epoch (`Database::commit_epoch`). Branches do not merge: lineage records where a branch came from, and the host writes the results it wants to keep back to the target branch. The durability, identity, and reclamation contract is [`docs/specs/BRANCHING_STORAGE_SPEC.md`](docs/specs/BRANCHING_STORAGE_SPEC.md).

## What the engine stores

| Surface | Role |
| --- | --- |
| Canonical graph | Nodes, relationships, properties, schema, and adjacency. Cypher reads and mutations. Crash recovery replays the WAL onto this state. |
| Canonical rows | PostgreSQL-dialect tables on the same snapshot and commit boundary, including strict-append event logs. |
| Lexical projection | Rebuildable full-text index. Published as its own generation and caught up from the graph. |
| Vector projection | Raw embeddings stay canonical. Default approximate queries traverse a persistent, filter-aware DiskANN (Vamana) graph; a checksummed RaBitQ artifact (`search_rabitq.<generation>.hawdb`) supplies 1-bit or 4-bit candidates. Exact queries stay exact, and final ranking reads the raw vectors. |
| Analytics projection | Immutable CSR/CSC graphs for PageRank and Louvain, invoked with `CALL project_graph`, `CALL page_rank`, and `CALL louvain`. Rebuildable derived state. |

Lexical, vector, and analytics data are projections. Identity and authorization live on the canonical snapshot, and retrieval rebinds to that snapshot before it returns rows.

Vector search is native Rust, and the portable scalar kernel is the correctness reference. Exact raw-vector arithmetic selects AVX2 or NEON at runtime when the CPU supports it and keeps the scalar summation order, so scores are bit-identical. The RaBitQ candidate scorer selects AVX2 automatically; NEON is used only when requested explicitly, and a NEON request fails closed off Arm. Segment scans are opt-in under a caller-supplied concurrency limit and memory budget. HawDB does not start a global vector-search thread pool. The RaBitQ encoding, recovery, and readiness contract is [`docs/specs/RABITQ_VECTOR_PROJECTION_SPEC.md`](docs/specs/RABITQ_VECTOR_PROJECTION_SPEC.md). The DiskANN graph, filter semantics, and memory preflight are in [`docs/specs/FILTERED_DISKANN_SPEC.md`](docs/specs/FILTERED_DISKANN_SPEC.md).

## Query languages

Application graph work is parameterized Cypher: `MATCH`, `OPTIONAL MATCH`, `WHERE`, `WITH`, `UNWIND`, `RETURN`, `ORDER BY`, `SKIP`, `LIMIT`, `CREATE`, `MERGE`, `SET`, `DELETE`, `DETACH DELETE`, and explicit `BEGIN` / `COMMIT` / `ROLLBACK`. The same statements run through `Database`, `DatabaseTransaction`, and `DatabaseReadTransaction`.

Within those clauses:

- Variable-length relationship patterns match as trails in every direction: a path never reuses a relationship.
- `shortestPath(...)`, `allShortestPaths(...)`, and the `ANY SHORTEST` / `ALL SHORTEST` selectors run a bounded bidirectional search between bound endpoints. An unbounded search is capped at 256 hops and fails rather than report no path. See [`docs/SHORTEST_PATH_EXECUTION.md`](docs/SHORTEST_PATH_EXECUTION.md).
- `count`, `sum`, `min`, `max`, `avg`, and `collect` accept a scalar-expression argument, with optional `DISTINCT`.
- List comprehensions, `size`, `head`, `last`, `tail`, `reverse`, `range`, indexing, slicing, and three-valued `IN` are supported. [`docs/CYPHER_LIST_FUNCTIONS.md`](docs/CYPHER_LIST_FUNCTIONS.md) lists their semantics and the remaining differences from openCypher.

Application relational work is PostgreSQL-dialect SQL through `query_sql` and `query_sql_with_params`. Idempotent ingestion uses `INSERT ... ON CONFLICT (...) DO NOTHING RETURNING ...`. Provisional row counts are available before commit; `commit_with_result` recomputes them at the durable boundary. The SQL/PGQ binder in `hawdb-sql` (`CREATE PROPERTY GRAPH`, `GRAPH_TABLE`) is specified in [`docs/specs/POSTGRES_SQL_PGQ_SPEC.md`](docs/specs/POSTGRES_SQL_PGQ_SPEC.md).

`Database::sql_session()` runs parameterized SQL with native rows and mutation counts, and `session.transaction(|transaction| ...)` makes several statements atomic. A statement failure aborts the whole callback, even if its error is caught. See [the SQL session contract](docs/SQL_SESSION.md) for examples, result provenance, and the supported PostgreSQL subset.

Both languages go through one Cascades-style optimizer. Plans are fingerprinted, cached when the statement shape allows it, and explainable. Costing, join enumeration, and predicate pushdown are separate rule families over a shared memo.

Typed Rust APIs are reserved for kernel boundaries: transactions, recovery, projections, import, bounded retrieval, admission, and readiness. Release builds do not expose application-specific CRUD facades.

## Durability and admission

A database directory has one writer lease, held by one process. Opening the same path twice, or from another process, fails immediately. Threads in the owning process share the handle. Read transactions observe a published snapshot.

The default durability policy syncs the WAL on every commit. `SyncOnCheckpoint` defers that flush, and group commit can batch it. Checkpoints then write an immutable generation, and `manifest.hawdb` is the only pointer to that generation. Recovery replays a committed prefix and drops a torn tail. The file format is HawDB's own: ordered immutable segments, a demand-paged descriptor tree, and copy-on-write pages, described in [`docs/STORAGE.md`](docs/STORAGE.md).

Every read carries an explicit row budget and payload budget. Planning, execution, search builds, and background maintenance take admission permits from a local QoS scheduler. Background work — schema, indexes, projection catch-up, compaction — runs only when the host enables it and the scheduler admits it. Exceeding a budget fails the request.

Two deployment profiles select defaults: `SharedHost` for a desktop or server process, and `MobileEmbedded` for a phone-style process. The profile does not change the file format or query semantics. The runtime contract is [`docs/specs/EMBEDDED_RUNTIME_SPEC.md`](docs/specs/EMBEDDED_RUNTIME_SPEC.md).

Optional OpenTelemetry metrics are host-injected. HawDB records low-cardinality counters through a supplied meter. It does not install a global provider, open an OTLP endpoint, or export query text, parameters, document identifiers, or database paths.

### Result budget errors

When selected read results exceed a row or payload limit, HawDB returns
`HawDBError::ReadBudgetExceeded(ReadBudgetError)`. The cause retains
`ReadBudgetResource::Rows` or `ReadBudgetResource::PayloadBytes`, the selected
`limit`, and the existing diagnostic `message`. Hosts can classify the resource
without parsing diagnostic text. Bounded snapshots preserve this cause for
cumulative result limits, and branch SQL rejects budget failures before
publishing a create or delete operation.

The error's display text and execution-category reports remain compatible.
Rust consumers with exhaustive `HawDBError` matches must handle the additional
variant. Execution-memory admission, invalid configuration, cancellation,
deadline, append-storage, and `MutationLimits`/DML `RETURNING` admission failures
retain their separate error contracts.
Limits, read authority, and persistent formats are unchanged.

## Build

```console
cargo build --locked -p hawdb
```

A minimal library keeps canonical graph storage, WAL recovery, transactions, parameterized Cypher, and incremental base indexes:

```console
cargo build --locked -p hawdb --no-default-features
```

Add capabilities explicitly when the host is constrained:

```console
cargo build --locked -p hawdb --no-default-features \
  --features full-text-search,vector-search
```

### Cargo features

| Feature | Default | What it compiles |
| --- | --- | --- |
| `full-text-search` | yes | Lexical indexing and search, gated again by the runtime capability matrix. |
| `vector-search` | yes | Scalar vector search and the bounded 1-bit or 4-bit RaBitQ candidate projection. Final ranking reads canonical raw vectors. |
| `graph-analytics` | yes | Bounded graph projection and analytics operators. |
| `background-maintenance` | yes | QoS-admitted background schema, index, projection, and maintenance work. |
| `tokio-runtime` | yes | Owned or borrowed Tokio adapter. The synchronous facade is still available when this feature is off. |
| `acl` | no | Access-control types. The host still supplies fresh policy and enables the capability at runtime. The feature alone is not an authorization boundary. |
| `opentelemetry` | no | Host-injected metrics adapter. Nightly, non-production monitoring builds only. |
| `qualification` | no | Offline qualification ingress. Not a serving capability. |
| `loom-tests` | no | Loom concurrency-model tests. Not an application capability. |

Build-time features are an upper bound. A capability omitted from the binary fails at runtime with `HawDBError::CapabilityUnavailable`.

Production packages use an explicit allowlist. `--all-features` pulls in `opentelemetry` and test-only features, so it is for CI compile coverage and is not a production artifact.

```console
cargo build --locked --release -p hawdb --no-default-features \
  --features background-maintenance,full-text-search,graph-analytics,vector-search
```

A nightly monitoring build adds the metrics adapter and Tokio integration. The host owns the OpenTelemetry SDK, exporter, credentials, and shutdown:

```console
cargo build --locked --release -p hawdb \
  --features opentelemetry,tokio-runtime
```

Measured dependency footprints are in [`docs/COMPOSITION_BASELINE.md`](docs/COMPOSITION_BASELINE.md).

### Host-owned memory allocator

HawDB does not install or bundle a custom allocator. The final host binary or WASM `cdylib` chooses its Rust allocator with `#[global_allocator]`, and HawDB's Rust allocations and Zstd context workspaces use that choice. Without host injection, Rust's target default applies. Mem's server selects `mimalloc`; embedding HawDB in that binary reuses the existing selection. See [the allocator contract](docs/ALLOCATORS.md) for native and WASM examples, ownership boundaries, and verification commands.

### Bazel

```console
bazel build //:hawdb
bazel test --test_output=errors //...
```

`//:hawdb_graph_text` matches `cargo build -p hawdb --no-default-features --features full-text-search`: graph and SQL plus full-text search, without vector search. Bazel uses a hermetic JDK 21, so `JAVA_HOME` is unnecessary for the TLA+ rules. On macOS the repository keeps Rust proc-macros unstripped to avoid a Mach-O alignment bug in the pinned 1.97.1 toolchain.

The experimental in-memory browser build is documented in [`docs/WASM.md`](docs/WASM.md). The public playground is built from [nowledge-co/hawdb-website](https://github.com/nowledge-co/hawdb-website).

### Fuzzing

```console
make mvcc-fuzz
make shiro-fuzz
make fuzz-help
make fuzz-test
```

`make mvcc-fuzz` runs the standalone MVCC CLI locally for about 30 seconds; `make shiro-fuzz` continuously runs SQL/Cypher multi-oracle checks until Ctrl+C. Override `MVCC_SECONDS`, `MVCC_SEED`, `SHIRO_SECONDS` and `SHIRO_SEED` as needed. `make fuzz` runs all native campaigns and writes JSON reports under `target/fuzz-logs/`. `make fuzz-test` runs the deterministic regression and CLI contracts through Bazel. Campaign details are in [`tools/shiro/README.md`](tools/shiro/README.md).

## Python

The Python package is a thin binding over the same embedded engine. Install a
published release with `python -m pip install hawdb` (CPython 3.11+), then import
`hawdb`. Native wheels and a source distribution are built from each published
GitHub Release; see the [Python release workflow](bindings/python/README.md#publishing).

```python
import hawdb

db = hawdb.open("./knowledge")
db.execute("CREATE (s:Stock {code: $code})", {"code": "603122"})

result = db.execute("MATCH (s:Stock) RETURN s.code")
print(result.columns)     # ['s.code']
print(result.fetchall())  # [{'s.code': '603122'}]

db.execute_sql("SELECT 1")
db.close()
```

Build and test instructions are in [`bindings/python/README.md`](bindings/python/README.md).

## Production hosts

Link HawDB as a library. Production read routing, migration gates, and readiness checks use the typed Rust API. They do not shell out to the `hawdb` binary.

The binary's compatibility commands compare an external previous engine or a shadow process. They run only when `HAWDB_ENABLE_COMPATIBILITY_TOOLS=1` is set, and only against copied data in isolated CI or release validation.

## Verification

Storage publication, MVCC validation, lock fairness, append assignment, and the knowledge-retrieval stage machine have executable TLA+ models under [`docs/tla`](docs/tla/README.md). CI checks them with a pinned TLA+ Tools jar, each over a bounded state space.

Alongside the models, the repository keeps differential fuzzing, crash-recovery evidence, search-generation qualification, and resource-profile gates. Those suites are separate from `cargo test` because they need different features, platforms, or runtime budgets.

## Documentation

| Read this | For |
| --- | --- |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Crate layout, query pipeline, and engine boundaries |
| [`docs/specs/QUERY_FIRST_PUBLIC_API_SPEC.md`](docs/specs/QUERY_FIRST_PUBLIC_API_SPEC.md) | The release-facing API |
| [`docs/specs/KNOWLEDGE_RETRIEVAL_PIPELINE_SPEC.md`](docs/specs/KNOWLEDGE_RETRIEVAL_PIPELINE_SPEC.md) | Retrieval stages, scoring, and budgets |
| [`docs/specs/EMBEDDED_RUNTIME_SPEC.md`](docs/specs/EMBEDDED_RUNTIME_SPEC.md) | Concurrency, durability, resources, telemetry |
| [`docs/STORAGE.md`](docs/STORAGE.md) | On-disk generations, WAL, and checkpoints |
| [`docs/ALLOCATORS.md`](docs/ALLOCATORS.md) | Host-owned global allocator contract |
| [`docs/FILE_DESCRIPTOR_CAPACITY.md`](docs/FILE_DESCRIPTOR_CAPACITY.md) | Descriptor capacity, pins, internal progress and Windows probe isolation |
| [`docs/specs/README.md`](docs/specs/README.md) | Index of normative contracts |
| [`docs/WASM.md`](docs/WASM.md) | In-tree experimental browser runtime |
| [hawdb.ai](https://hawdb.ai) | Public site and playground. Source: [hawdb-website](https://github.com/nowledge-co/hawdb-website) |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Issues, pull requests, and validation expectations |

## Scope

HawDB is a single-process embedded engine.

- One database directory, one writer lease, one process.
- Cypher covers the statements listed above. That is the executed surface, and it is smaller than the whole of openCypher.
- Search and analytics artifacts are rebuilt from canonical state.
- Branches live inside one project directory. There is no branch merge, point-in-time source selection, remote replication, or automatic expiry.
- Multi-process writers, distributed replication, and a standalone database server are outside this release. Design notes for later storage work live with the specs and are not serving features.

## Contributing

Issues and pull requests follow [`CONTRIBUTING.md`](CONTRIBUTING.md). Architecture constraints for changes, including changes written with an assistant, are in [`AGENTS.md`](AGENTS.md). Open work is tracked in [`TODO.md`](TODO.md).

## License

Apache License 2.0. See [`LICENSE`](LICENSE).

Source files carry the standard Apache-2.0 header. Check or repair them with [hawkeye](https://crates.io/crates/hawkeye) and [`licenserc.toml`](licenserc.toml):

```console
hawkeye check
hawkeye format
```
