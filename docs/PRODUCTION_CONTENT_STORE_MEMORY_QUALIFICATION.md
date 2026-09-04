# Production Content Store Memory Qualification

This runbook collects the identity-bound memory-policy matrix required before
Content Store workload evidence can be admitted. The collector evaluates the
dynamic shared-host policy and the explicit 512 MiB capability policy from one
actual host/cgroup resource snapshot. It does not open, copy, create,
checkpoint, or mutate a Skein database.

Policy evidence is not workload evidence. A ready matrix proves that the
governor derived the intended capacities and budgets; separate representative
read and mutation runs must still prove RSS, page faults, latency, cache
behavior, and write amplification.

## Preconditions

- Run on the exact target OS, architecture, binary revision, and feature set
  named by the release identity.
- The shared-host case requires an effective host or cgroup memory limit of exactly
  8 GiB. A larger host can use a separate 8 GiB cgroup v2 envelope.
- The storage path must exist and reside on the same device class used by the
  representative database so the runtime can derive the normal I/O budget.
- The plan identity must match the representative dataset, schema, canonical
  epoch, and database configuration being qualified.

## Plan

Start from the parser-tested
[`production_memory_plan_example_v1.json`](../crates/qualification/fixtures/nowledge_content_store/production_memory_plan_example_v1.json).
Its protocol is `skein-production-content-store-memory-plan-v1`.

Replace every placeholder identity value and generation time. The plan does
not contain resource numbers: host/cgroup limits and available headroom are
detected by the typed runtime. Unknown fields, stale or invalid identities,
files larger than 32 MiB, and missing storage paths fail before evidence is
accepted.

## Execute

```bash
cargo run -p skein-qualification \
  --bin skein-content-store-memory-qualification -- \
  --storage-path /path/to/representative-storage-device \
  --plan-json /path/to/content-store-memory-plan.json \
  > content-store-memory-profiles.json
```

The report always contains both profiles:

- `shared_host_8_gib` leaves the governor dynamic. Capacity is capped at
  2 GiB; the budget is one quarter of detected headroom, normally around
  1--2 GiB and allowed to fall below 1 GiB under pressure.
- `capability_512_mib` applies an explicit 512 MiB Skein ceiling on the same
  host snapshot. It does not claim that the host itself has only 512 MiB.

Exit code `0` means both policy derivations are ready, `1` means a complete
matrix is blocked, and `2` means input or execution failed. Retain the plan,
matrix JSON, exact binary revision, resource environment, and later workload
artifacts together. This collector proves neither a 512 MiB workload nor a
representative production workload by itself.

The final release bundle accepts this matrix through
`--content-store-memory-profiles-json`. It also requires the representative
production read through `--content-store-read-json` and a distinct constrained
run through `--content-store-512-mib-read-json`. All three artifacts must bind
the same exact release identity; neither workload artifact can substitute for
the other.
