# Production Content Store Qualification

This runbook collects read-only and isolated mutation storage evidence from an
already imported, representative Skein database. It does not import SQLite,
create replicas, or derive an oracle from Skein itself.

## Preconditions

- The database is a caller-owned representative copy, not the live Mem store.
- Canonical row and index artifacts both exceed the configured segment cache.
- Each fresh open records raw segment payload-cache activity before its first
  user query. The plan bounds mandatory system-schema validation by cache
  requests and resident bytes; pins, evictions, admission rejections, and
  digest mismatches remain forbidden. Manifest and WAL work remain in the open
  timing partition.
- The database has a clean authoritative checkpoint and the expected canonical
  commit epoch.
- Expected row counts and SHA-256 result digests come from an offline reference
  snapshot owned by the Mem adapter.
- The identity records the exact revision, target, features, configuration,
  dataset, and schema being qualified.

## Plan

Write a bounded JSON plan with protocol
`skein-production-content-store-read-plan-v1`:

The checked-in
[`production_read_plan_example_v1.json`](../crates/qualification/fixtures/nowledge_content_store/production_read_plan_example_v1.json)
is parser-tested and can be copied as the starting point.

```json
{
  "protocol": "skein-production-content-store-read-plan-v1",
  "evidence_binding": {
    "identity": {
      "source_revision": "replace-with-skein-revision",
      "rust_toolchain": "rustc 1.xx.x",
      "target_os": "macos",
      "target_arch": "aarch64",
      "enabled_features": [],
      "durable_format_version": 1,
      "schema_version": 1,
      "configuration_digest": "sha256:replace-with-qualified-config",
      "deployment_profile": "shared-host-8-gib",
      "dataset_fingerprint": "sha256:replace-with-offline-dataset-digest",
      "canonical_graph_commit_epoch": 1,
      "policy_version": 1
    },
    "generated_at_unix_seconds": 1
  },
  "expected_identity": {
    "source_revision": "replace-with-skein-revision",
    "rust_toolchain": "rustc 1.xx.x",
    "target_os": "macos",
    "target_arch": "aarch64",
    "enabled_features": [],
    "durable_format_version": 1,
    "schema_version": 1,
    "configuration_digest": "sha256:replace-with-qualified-config",
    "deployment_profile": "shared-host-8-gib",
    "dataset_fingerprint": "sha256:replace-with-offline-dataset-digest",
    "canonical_graph_commit_epoch": 1,
    "policy_version": 1
  },
  "resource_profile": "shared_host_8_gib",
  "database": {
    "max_read_result_rows": 100000,
    "max_read_result_payload_bytes": 67108864,
    "execution_batch_rows": 256,
    "execution_batch_payload_bytes": 16777216,
    "blocking_operator_bytes": 67108864,
    "segment_cache_capacity_bytes": 268435456,
    "max_relational_index_read_bytes": 67108864,
    "max_relational_hydration_bytes": 67108864
  },
  "measurement_runs": 5,
  "open_payload_cache_limits": {
    "max_requests": 64,
    "max_resident_bytes": 16777216
  },
  "resource_limits": {
    "max_steady_resident_bytes": 2147483648,
    "max_peak_resident_bytes": 2147483648,
    "max_total_page_faults_per_run": null,
    "max_minor_page_faults_per_run": null,
    "max_major_page_faults_per_run": null
  },
  "read_cases": [
    {
      "case_name": "thread-message-page",
      "statement_name": "thread_messages_page",
      "parameters": ["replace-with-thread-storage-id", 100],
      "expected_output_rows": 100,
      "expected_output_sha256": "replace-with-offline-result-digest",
      "max_intermediate_rows": 1000,
      "max_physical_pages_per_run": 128,
      "max_physical_bytes_per_run": 16777216
    }
  ]
}
```

The example values are placeholders, not accepted release evidence. The
`evidence_binding.identity` and `expected_identity` objects must be identical.
Plan parsing rejects unknown fields and files larger than 32 MiB.

Use `capability_512_mib` for the separately configured low-memory capability
run. It sets an explicit 512 MiB Skein runtime ceiling. Use
`shared_host_8_gib` for the dynamic shared-host policy: Skein derives its budget
from current headroom, caps automatic capacity at 2 GiB, normally operates in
the 1--2 GiB range, and may fall below that range under pressure. A custom
profile is represented as:

```json
{
  "configured_workload": {
    "available_memory_bytes": 4294967296,
    "runtime_memory_ceiling_bytes": 1073741824
  }
}
```

## Execute

```bash
cargo run -p skein-qualification \
  --bin skein-content-store-read-qualification -- \
  --database-path /path/to/representative.skein \
  --plan-json /path/to/content-store-read-plan.json \
  > content-store-read-evidence.json
```

Run the same representative read corpus again with a plan whose
`resource_profile` is `capability_512_mib`, whose cache and result budgets fit
that envelope, and whose RSS limit is at most 512 MiB. Retain it separately:

```bash
cargo run -p skein-qualification \
  --bin skein-content-store-read-qualification -- \
  --database-path /path/to/representative.skein \
  --plan-json /path/to/content-store-512-mib-read-plan.json \
  > content-store-512-mib-read-evidence.json
```

The wrapper always constructs `read_only + OutOfCore + Authoritative` and
passes the plan to `run_production_content_store_storage_qualification`. It
does not retain the local database path. Exit code `0` means the complete report
is ready, `1` means the report is complete but blocked, and `2` means input or
execution failed.

Retain the plan, evidence JSON, exact binary revision, and offline oracle
artifact together. A successful local run is not a release gate until those
artifacts are reviewed and included in the production qualification bundle.
The production-profile output and the 512 MiB capability output are independent
mandatory release inputs; a successful capability run does not redefine the
normal shared-host budget.

## Exact Overflow Compaction

Run exact overflow compaction only against an existing caller-owned disposable
replica. The collector mutates, checkpoints, scrubs, and reopens that directory;
it never copies or mutates the production source. The bounded plan protocol is
`skein-production-content-store-overflow-compaction-plan-v1` and must declare
the exact evidence identity, frozen SQL verification cases, database budgets,
scan/overlay/rewrite/sort/spill bounds, and resource/reclamation limits.

```bash
cargo run -p skein-qualification \
  --bin skein-content-store-overflow-compaction-qualification -- \
  --replica-path /path/to/disposable-overflow-replica.skein \
  --plan-json /path/to/content-store-overflow-compaction-plan.json \
  > content-store-overflow-compaction-evidence.json
```

Retain two independent current-revision reports. The `capability_512_mib` plan
installs an explicit 512 MiB Skein ceiling and proves the low-memory capability.
The `shared_host_8_gib` plan observes an 8 GiB host or cgroup envelope and
keeps dynamic memory derivation; automatic capacity cannot exceed 2 GiB, while
pressure may lower the budget below 1 GiB. The release bundle requires both
reports and recomputes their raw scan, spill, rewrite, digest, RSS, page-fault,
write-amplification, physical-reclamation, scrub, and governor evidence.

## Writable Mutation Matrix

The mutation collector requires the read-only source plus four caller-created,
disposable replicas of the same imported generation. The collector never copies
the source. Each replica is consumed by exactly one writer-count case and will
be mutated, recovered from non-empty WAL, checkpointed, and reopened. Never
point a replica argument at live data or reuse one directory for multiple
cases.

Start from the parser-tested
[`production_mutation_plan_example_v1.json`](../crates/qualification/fixtures/nowledge_content_store/production_mutation_plan_example_v1.json).
Its protocol is `skein-production-content-store-mutation-plan-v1`. Replace
every placeholder, including all per-writer operation parameters, offline
verification digests, the accepted commit-latency reference, and the complete
WAL group-commit evidence. The cases and their workers must be explicit and
ordered exactly as 1, 4, 8, and 10 writers. Plan parsing rejects unknown fields
and files larger than 32 MiB.

The resource profiles have the same meaning as for the read collector. The
shared-host profile declares an 8 GiB environment and enforces a peak RSS ceiling
no greater than 2 GiB. The 512 MiB capability profile is a separate explicitly
configured run, not the shared-host default or a universal capacity limit.

```bash
cargo run -p skein-qualification \
  --bin skein-content-store-mutation-qualification -- \
  --source-database-path /path/to/read-only-representative.skein \
  --replica-1 /path/to/disposable-writers-1.skein \
  --replica-4 /path/to/disposable-writers-4.skein \
  --replica-8 /path/to/disposable-writers-8.skein \
  --replica-10 /path/to/disposable-writers-10.skein \
  --plan-json /path/to/content-store-mutation-plan.json \
  > content-store-mutation-evidence.json
```

The wrapper constructs writable `OutOfCore + Authoritative` database
configuration and passes the plan to
`run_production_content_store_mutation_qualification`. The typed runner
canonicalizes all directories before mutation, rejects source/replica aliases,
and verifies that every replica starts at the expected serving identity. Local
paths, SQL parameters, and result rows are absent from retained evidence. Exit
codes have the same meaning as the read collector.

Retain the source-generation manifest, replica construction record, plan,
evidence JSON, accepted latency artifact, exact binary revision, and offline
oracle together. A parser-tested example or synthetic matrix is contract
evidence only; neither can satisfy the representative production gate.
