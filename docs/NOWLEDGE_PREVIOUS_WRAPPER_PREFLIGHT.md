# Nowledge Previous-Wrapper Preflight

This runbook turns a Nowledge-owned Kuzu/Ladybug wrapper command into Skein
production-replacement evidence. It is intentionally evidence-first: a passing
scanner or a passing protocol smoke is not enough for cutover.

## Safety Boundary

Never open the live Nowledge Kuzu database from a Skein or ad hoc validation
tool. Kuzu's writer lock is process-exclusive, and the desktop/server runtime
may already hold the live handle.

Use a point-in-time copy:

```bash
export NMEM_LIVE_DIR="$HOME/Library/Application Support/NowledgeGraph"
export NMEM_PREFLIGHT_ROOT="/tmp/skein-nowledge-preflight"

rm -rf "$NMEM_PREFLIGHT_ROOT"
mkdir -p "$NMEM_PREFLIGHT_ROOT"

cp -R "$NMEM_LIVE_DIR/nowledge_graph_v2.db" "$NMEM_PREFLIGHT_ROOT/nowledge_graph_v2.db"
cp "$NMEM_LIVE_DIR/content.db" "$NMEM_PREFLIGHT_ROOT/content.db"
cp -R "$NMEM_LIVE_DIR/search_index" "$NMEM_PREFLIGHT_ROOT/search_index"
```

The wrapper command should read only from the copied paths during shadow
validation. Writable validation must use an isolated fixture database, not the
live application directory.

## Required Wrapper Command

Skein does not link `nmem-graph`, Kuzu, or Ladybug. Nowledge owns the wrapper
command process and all graph dependencies. The command must read one JSON
request per line from stdin and write one JSON response per line to stdout.

It must implement:

- `{"op":"query","cypher":"...","parameters":{...}}`
- `{"op":"execute_session","statements":[...]}`
- `{"op":"project_graph",...}`

The response shape is documented in
[`EXTERNAL_SHADOW_PROTOCOL.md`](EXTERNAL_SHADOW_PROTOCOL.md). For real cutover
evidence, `project_graph` must return a full projected-graph payload, not
`primary_only`.

The command should be persistent for full-contract validation so it can keep one
database/session handle open:

```bash
export NOWLEDGE_WRAPPER_COMMAND="/path/to/nowledge-previous-wrapper-command"
export NOWLEDGE_WRAPPER_IDENTITY="nowledge-previous-wrapper:local-copy"
```

## Recommended Bundle Runner

Use the checked-in bundle runner for release preflight. It runs the full
contract, adapter smoke, storage recovery, background maintenance, migration
gate, replacement summary, query runtime preflight, and final preflight
verifier in the fail-closed order documented below:

```bash
scripts/nowledge-previous-wrapper-preflight.sh \
  --preflight-root "$NMEM_PREFLIGHT_ROOT" \
  --nowledge-root /Users/hawkingrei/devel/nowledge/mem \
  --wrapper-identity "$NOWLEDGE_WRAPPER_IDENTITY" \
  --query-runtime-probe-json "$NMEM_PREFLIGHT_ROOT/query-runtime-probes.json" \
  -- "$NOWLEDGE_WRAPPER_COMMAND"
```

The script prints `preflight-check.json` on success and leaves every
intermediate artifact under `$NMEM_PREFLIGHT_ROOT`. Use the manual steps below
when bringing up a new wrapper command or debugging a specific failed stage.

## 1. Export The Contract

```bash
cargo run --quiet --bin skein -- \
  nowledge-fixture-contract nowledge-memory-core \
  > "$NMEM_PREFLIGHT_ROOT/contract.json"
```

The contract is the machine-readable Nowledge business fixture. It includes
setup, Cypher statements, parameters, expected rows, effect checks, and
projected-graph requests.

## 2. Run The Full Wrapper Contract

```bash
cargo run --quiet --bin skein -- \
  nowledge-fixture-contract-command-check \
  --require-full-contract \
  --wrapper-identity "$NOWLEDGE_WRAPPER_IDENTITY" \
  "$NMEM_PREFLIGHT_ROOT/contract.json" \
  --persistent-command "$NOWLEDGE_WRAPPER_COMMAND" \
  > "$NMEM_PREFLIGHT_ROOT/contract-evidence.json"
```

The report must satisfy:

```bash
jq -e '
  .required_contract_ready == true and
  .previous_wrapper_contract_evidence.ready == true and
  .previous_wrapper_contract_evidence.wrapper_identity == env.NOWLEDGE_WRAPPER_IDENTITY
' "$NMEM_PREFLIGHT_ROOT/contract-evidence.json"
```

When bringing up a failing wrapper, use `--stop-after-first-failure`,
`--start-check`, or `--check-name`. Do not feed selected-slice output into the
production migration gate.

## 3. Smoke The External Shadow Adapter

```bash
cargo run --quiet --bin skein -- \
  external-shadow-adapter-smoke \
  --require-previous-wrapper \
  --shadow-trace "$NMEM_PREFLIGHT_ROOT/adapter-shadow.jsonl" \
  previous-wrapper \
  cargo run --quiet --example nowledge_previous_wrapper_shadow_adapter -- \
    --wrapper-identity "$NOWLEDGE_WRAPPER_IDENTITY" \
    --persistent-command "$NOWLEDGE_WRAPPER_COMMAND" \
  > "$NMEM_PREFLIGHT_ROOT/adapter-smoke.json"
```

The smoke report must satisfy:

```bash
jq -e '
  .adapter_smoke_ready == true and
  .engine_kind == "previous_wrapper" and
  .wrapper_identity == env.NOWLEDGE_WRAPPER_IDENTITY and
  .primary_only_checks == 0 and
  .dual_engine_evidence.ready == true
' "$NMEM_PREFLIGHT_ROOT/adapter-smoke.json"
```

This proves protocol shape and adapter identity only. It is still not production
replacement evidence by itself.

## 4. Attach Storage And Background Evidence

Use an isolated Skein database for storage-recovery and background-maintenance
evidence:

```bash
TMPDIR="$NMEM_PREFLIGHT_ROOT" cargo run --quiet --bin skein -- \
  > "$NMEM_PREFLIGHT_ROOT/skein-demo.out"

export SKEIN_PREFLIGHT_DB="$NMEM_PREFLIGHT_ROOT/skein-demo"

cargo run --quiet --bin skein -- \
  storage-recovery-report \
  --max-wal-replay-entries 100 \
  --require-durable \
  --require-checkpoint-boundary \
  --require-bounded-wal-replay \
  --require-clean-tail \
  "$SKEIN_PREFLIGHT_DB" \
  > "$NMEM_PREFLIGHT_ROOT/storage-recovery.json"

cargo run --quiet --bin skein -- \
  background-maintenance-report \
  --require-cutover-ready \
  "$SKEIN_PREFLIGHT_DB" \
  > "$NMEM_PREFLIGHT_ROOT/background-maintenance.json"
```

These reports prove Skein-side recovery and background QoS readiness. They do
not prove Nowledge wrapper parity.

## 5. Run The Migration Gate

Run the migration gate against the Nowledge graph-source checkout and the real
previous-wrapper adapter. Use the `previous_wrapper_contract_evidence` object
from the full contract report:

```bash
jq '.previous_wrapper_contract_evidence' \
  "$NMEM_PREFLIGHT_ROOT/contract-evidence.json" \
  > "$NMEM_PREFLIGHT_ROOT/previous-wrapper-contract-evidence.json"

cargo run --quiet --bin skein -- \
  nowledge-cypher-migration-gate \
  --require-ready \
  --require-cutover-evidence \
  --shadow-ready \
  --shadow-trace "$NMEM_PREFLIGHT_ROOT/migration-shadow.jsonl" \
  --require-storage-recovery-evidence \
  --storage-recovery-report-json "$NMEM_PREFLIGHT_ROOT/storage-recovery.json" \
  --require-background-maintenance-evidence \
  --background-maintenance-report-json "$NMEM_PREFLIGHT_ROOT/background-maintenance.json" \
  --previous-wrapper-contract-evidence-json "$NMEM_PREFLIGHT_ROOT/previous-wrapper-contract-evidence.json" \
  /Users/hawkingrei/devel/nowledge/mem \
  previous-wrapper \
  cargo run --quiet --example nowledge_previous_wrapper_shadow_adapter -- \
    --wrapper-identity "$NOWLEDGE_WRAPPER_IDENTITY" \
    --persistent-command "$NOWLEDGE_WRAPPER_COMMAND" \
  > "$NMEM_PREFLIGHT_ROOT/migration-gate.json"
```

The gate must report:

```bash
jq -e '
  .migration_gate.decision == "ready" and
  .cutover.decision == "ready" and
  .cutover_evidence.eligible == true and
  .cutover_evidence.ready_engine_kind == "previous_wrapper" and
  .cutover_evidence.ready_wrapper_identity == env.NOWLEDGE_WRAPPER_IDENTITY and
  .previous_wrapper_contract_evidence.ready == true and
  .replacement_readiness_per_million == 1000000
' "$NMEM_PREFLIGHT_ROOT/migration-gate.json"
```

## 6. Produce The Replacement Summary

```bash
cargo run --quiet --bin skein -- \
  nowledge-replacement-summary \
  --require-production-ready \
  "$NMEM_PREFLIGHT_ROOT/migration-gate.json" \
  > "$NMEM_PREFLIGHT_ROOT/replacement-summary.json"
```

The replacement summary is the release-facing artifact. It must report:

```bash
jq -e '
  .production_cutover_ready == true and
  .production_replacement_per_million == 1000000 and
  (.blocking_categories | length) == 0 and
  (.missing_evidence | length) == 0 and
  (.next_actions | length) == 0 and
  .dual_engine_evidence.present == true and
  .dual_engine_evidence.ready == true
' "$NMEM_PREFLIGHT_ROOT/replacement-summary.json"
```

If this command fails, inspect `next_actions` first. The action codes are
stable enough for dashboards and release automation.

## 7. Run Query Runtime Preflight

The query runtime preflight independently runs JSON-defined probes through the
read-only Skein runtime with `EXPLAIN ANALYZE`, then emits plan/profile
evidence without rows, parameters, or local paths:

```bash
cargo run --quiet --bin skein -- \
  nowledge-query-runtime-preflight \
  --probe-json "$NMEM_PREFLIGHT_ROOT/query-runtime-probes.json" \
  "$NMEM_PREFLIGHT_GRAPH" \
  > "$NMEM_PREFLIGHT_ROOT/query-runtime-preflight.json"
```

Use probes from active Nowledge Mem route fixtures. Missing or weak probe
evidence keeps the whole previous-wrapper preflight fail-closed.

## 8. Verify The Whole Preflight Bundle

Use the bundle checker to collapse the evidence files into one
release-facing preflight verdict:

```bash
cargo run --quiet --bin skein -- \
  nowledge-previous-wrapper-preflight-check \
  --require-ready \
  --wrapper-identity "$NOWLEDGE_WRAPPER_IDENTITY" \
  --bundle-dir "$NMEM_PREFLIGHT_ROOT" \
  > "$NMEM_PREFLIGHT_ROOT/preflight-check.json"
```

The verifier checks wrapper identity consistency across the full contract,
adapter smoke, migration gate, storage recovery evidence, background
maintenance evidence, replacement summary, and query runtime preflight
artifacts. It fails closed unless every stage is ready, the storage/background
evidence is explicitly required and present in the migration gate, the
replacement summary has no blockers, missing evidence, or next actions, and the
query runtime probes all produce plan/profile evidence.
The final preflight is stricter than compatibility summary generation: the
replacement summary must carry `dual_engine_evidence.present == true` and
`dual_engine_evidence.ready == true`.
When adapter smoke reports include `dual_engine_evidence`, the verifier also
requires `dual_engine_evidence.ready == true` so side-by-side cutover evidence
cannot silently degrade into a primary-only smoke run.
Each per-stage check includes `failed_evidence_fields`, so release automation
can report the exact missing or mismatched field without parsing blocker text.
The final JSON also includes `release_summary`, a compact copy of the wrapper
identity, contract counts, adapter request counts, migration/cutover decisions,
replacement readiness, storage/background readiness, and dual-engine counts
needed by release notes and dashboards, plus query runtime probe counts for
plan-readiness tracking.
For targeted debugging, the same command still accepts explicit
`--contract-evidence-json`, `--adapter-smoke-json`, `--migration-gate-json`,
`--replacement-summary-json`, and `--query-runtime-preflight-json` paths;
explicit files override the standard names loaded from `--bundle-dir`.
