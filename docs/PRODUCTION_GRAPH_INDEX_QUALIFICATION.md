# Production Graph Index Qualification

This runbook collects the all-class persistent graph-index artifact from one
already imported representative Skein database. The collector is a thin
developer and evidence wrapper over the typed Rust matrix runner. It does not
import data, build indexes, create a database, or derive the reference oracle
from Skein.

## Preconditions

- The database is a caller-owned read-only representative copy, not the live
  Mem store.
- The copy has current, generation-aligned canonical graph, property
  projection, and adjacency artifacts.
- The property or adjacency artifact required by every case exceeds the
  configured segment-cache capacity.
- Reference row counts and SHA-256 result digests come from an offline
  canonical fallback oracle over the exact same dataset and graph epoch.
- The identity records the exact revision, target, features, configuration,
  dataset, schema, and canonical graph epoch being qualified.

## Plan

Start from the parser-tested
[`production_graph_index_plan_example_v1.json`](../crates/qualification/fixtures/nowledge_graph/production_graph_index_plan_example_v1.json).
Its protocol is `skein-production-graph-index-plan-v1`.

Replace every identity, query parameter, reference row count, all-zero digest,
and resource budget. Each query must select its declared index class on the
representative generation. A logically correct query that falls back to the
canonical scan will be rejected because the required class counter remains
zero.

The plan lists the classes once in this exact order:

1. `node_equality`
2. `node_range`
3. `node_full_text`
4. `node_composite_equality`
5. `relationship_equality`
6. `relationship_range`
7. `forward_adjacency`
8. `reverse_adjacency`

Every case declares its redacted Cypher input, typed parameters, output and
intermediate budgets, offline result digest and row count, page and byte
limits, and cancellation-latency limit. The shared section fixes the runtime,
database cache, process limits, measurement count, and release identity for the
whole matrix. Unknown fields, reordered or incomplete cases, zero budgets,
out-of-domain parameter values, invalid digests, and files larger than 32 MiB
are rejected before opening the database.

Use `shared_host_8_gib` only on a separately verified 8 GiB host or cgroup.
It leaves the governor dynamic and caps an accepted plan's peak RSS at 2 GiB;
the effective budget normally falls in the 1--2 GiB range and may shrink below
it under pressure. The observed effective limit and headroom policy still
require the fixed memory profile evidence. Use `capability_512_mib` for the
independent explicitly configured low-memory run. It installs a 512 MiB runtime
ceiling and rejects a larger cache or RSS budget. The 512 MiB profile is not
the shared-host default or a universal machine requirement.

## Execute

```bash
cargo run -p skein-qualification \
  --bin skein-graph-index-qualification -- \
  --database-path /path/to/read-only-representative.skein \
  --plan-json /path/to/graph-index-plan.json \
  > graph-index-matrix-evidence.json
```

The wrapper always constructs `ShadowReadOnly + OutOfCore` open options and a
read-only `DatabaseConfig`. It passes one identical database path, open
configuration, runtime configuration, and production identity to all eight
typed cases. The runner opens a dedicated handle per class, records cold and
warm reads, validates class-specific cache misses and hits, streams a result
digest, performs bounded cancellation, and retains only redacted evidence.

Exit code `0` means the complete matrix report is ready, `1` means the report
is complete but blocked, and `2` means input or execution failed. Local paths,
query text, parameters, and result rows do not enter the report.

Retain the plan, matrix JSON, offline oracle artifact, memory-profile evidence,
database-generation manifest, exact binary revision, and runner environment
together. The parser fixture and synthetic matrix tests establish the contract
only; they cannot satisfy the representative production gate.
