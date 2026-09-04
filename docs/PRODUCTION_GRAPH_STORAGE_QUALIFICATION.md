# Production Graph Storage Qualification

This runbook collects the general graph-storage artifact required by the final
release bundle. The collector is a thin developer and evidence wrapper over
`run_production_graph_storage_qualification`. It does not import, copy, create,
repair, checkpoint, or mutate a database.

This artifact is distinct from the all-class persistent graph-index matrix.
Use it for a representative business-shaped Cypher traversal that demonstrates
bounded out-of-core graph execution. Use
`skein-graph-index-qualification` separately to qualify every persistent index
class.

## Preconditions

- The database is a caller-owned read-only representative copy, not the live
  Mem store.
- The selected canonical graph artifact is larger than the configured segment
  cache.
- The Cypher statement and parameters represent an active bounded Mem read
  shape and have explicit output and intermediate budgets.
- The identity records the exact revision, target, features, configuration,
  dataset, schema, and canonical graph epoch being qualified.
- The detected memory profile has been qualified separately when the plan uses
  the dynamic shared-host profile.

## Plan

Start from the parser-tested
[`production_graph_storage_plan_example_v1.json`](../crates/qualification/fixtures/nowledge_graph/production_graph_storage_plan_example_v1.json).
Its protocol is `skein-production-graph-storage-plan-v1`.

Replace the placeholder identity and workload values. The plan declares one
parameterized Cypher statement, typed parameters, database and execution
budgets, output and intermediate limits, process RSS and page-fault limits, and
at least two measurement runs. Unknown fields, empty statements, zero budgets,
out-of-domain parameter values, or files larger than 32 MiB are rejected before
opening the database.

Use `shared_host_8_gib` only with separate evidence that the effective host
or cgroup limit is 8 GiB. It keeps runtime memory derivation dynamic: automatic
Skein capacity is capped at 2 GiB, normally moves through roughly 1--2 GiB as
headroom changes, and may fall below that range under pressure. It is not a
fixed reservation. Use `capability_512_mib` for a separate explicitly bounded
low-memory capability run. It installs a 512 MiB Skein ceiling; 512 MiB is not
the shared-host default, a universal production cutoff, or a minimum host size.

## Execute

```bash
cargo run -p skein-qualification \
  --bin skein-graph-storage-qualification -- \
  --database-path /path/to/read-only-representative.skein \
  --plan-json /path/to/graph-storage-plan.json \
  > graph-storage-evidence.json
```

The wrapper always derives `ShadowReadOnly + OutOfCore` open options and a
read-only `DatabaseConfig`. The typed runner records cold and warm resource
runs, fully streamed execution, result and intermediate budgets, steady and
peak RSS, page faults, cache residency, runtime admission, and exact release
identity. The retained report contains query and parameter digests rather than
query text, parameter values, result rows, or local paths.

Exit code `0` means the report is ready, `1` means the report is complete but
blocked, and `2` means input or execution failed. Retain the plan, result JSON,
memory-profile evidence, database-generation manifest, exact binary revision,
and runner environment together. The parser fixture and synthetic tests define
the transport contract only; they do not satisfy the representative production
gate.
