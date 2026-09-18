# Nowledge Previous-Wrapper Preflight

This runbook turns a Nowledge-owned Kuzu/Ladybug wrapper command into HawDB
production-replacement evidence. It is intentionally evidence-first: a passing
scanner or a passing protocol smoke is not enough for cutover.

This is not a production serving path. Mem should embed HawDB as a Rust library
and consume typed readiness APIs for startup and read selection. The CLI commands
used here are quarantined developer/preflight tools and require
`HAWDB_ENABLE_COMPATIBILITY_TOOLS=1`; the checked-in script sets that variable
only around the isolated `cargo run --bin hawdb` calls it owns.

## Safety Boundary

Never open the live Nowledge Kuzu database from a HawDB or ad hoc validation
tool. Kuzu's writer lock is process-exclusive, and the desktop/server runtime
may already hold the live handle.

Use a point-in-time copy:

```bash
export NMEM_LIVE_DIR="$HOME/Library/Application Support/NowledgeGraph"
export NMEM_PREFLIGHT_ROOT="/tmp/hawdb-nowledge-preflight"

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

HawDB does not link `nmem-graph`, Kuzu, or Ladybug. Nowledge owns the wrapper
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
gate, query runtime preflight, replacement summary, and final preflight
verifier in the fail-closed order documented below:

```bash
scripts/nowledge-previous-wrapper-preflight.sh \
  --preflight-root "$NMEM_PREFLIGHT_ROOT" \
  --nowledge-root /Users/hawkingrei/devel/nowledge/mem \
  --wrapper-identity "$NOWLEDGE_WRAPPER_IDENTITY" \
  --require-integration-readiness \
  --search-projection-shadow-primary-probe-json "$NMEM_PREFLIGHT_ROOT/lancedb-search-projection-probe.json" \
  --search-projection-shadow-probe-json "$NMEM_PREFLIGHT_ROOT/hawdb-search-projection-probe.json" \
  --search-candidate-shadow-probe-json "$NMEM_PREFLIGHT_ROOT/search-candidate-shadow-probe.json" \
  --graph-route-query-json "$NMEM_PREFLIGHT_ROOT/graph-route-queries.json" \
  --graph-route-parity-json "$NMEM_PREFLIGHT_ROOT/graph-route-parity.json" \
  --integration-submodule-path vendor/hawdb \
  --integration-legacy-data-retained \
  --integration-coexistence-mode shadow \
  -- "$NOWLEDGE_WRAPPER_COMMAND"
```

The script prints `integration-bundle.json` on success when
`--require-integration-readiness` is enabled; otherwise it prints
`preflight-check.json`. It leaves every intermediate artifact under
`$NMEM_PREFLIGHT_ROOT`. Use the manual steps below when bringing up a new
wrapper command or debugging a specific failed stage.
When `--require-integration-readiness` is enabled, the runner derives
`--integration-submodule-commit` from `--integration-submodule-path` if the
commit is not passed explicitly. Non-git paths fail closed.
If `--query-runtime-probe-json` is omitted, the runner reuses
`--graph-route-query-json` as the query-runtime probe source. That route
inventory must include per-query `query_family`; missing or unknown families
remain fail-closed.
When `--require-integration-readiness` is enabled and the runner generates graph
route evidence from `--graph-route-query-json`, it also requires
`--graph-route-parity-json` and forwards it to `nowledge-graph-route-evidence`.
This keeps production route readiness tied to a real shadow parity artifact;
route inventory `shadow_compare_ready` fields are bring-up diagnostics only.
The runner also probes `$NMEM_PREFLIGHT_ROOT/content.db` by default and derives
the content-store boundary flags when the copied SQLite database contains
`thread_messages` and `source_chunks`. Use `--integration-content-store-path`
only when the content copy lives elsewhere.

## Production Library Embedding

Production nmem should not start HawDB by shelling out to this runner or any
other command. It should embed HawDB like SQLite: keep a long-lived Rust handle
inside the process and call typed functions directly.

```rust
use hawdb::{
    BlackboxRunStatus, DatabaseConfig, Result, HawDBEmbedded,
    HawDBEmbeddedOpenOptions,
};

fn open_hawdb(path: std::path::PathBuf) -> Result<HawDBEmbedded> {
    let config = DatabaseConfig {
        slow_query_log_threshold_micros: 300_000,
        ..DatabaseConfig::default()
    };
    HawDBEmbedded::open_with_options(
        HawDBEmbeddedOpenOptions::new(path).with_config(config),
    )
}

fn flush_observability(engine: &HawDBEmbedded, artifact_dir: std::path::PathBuf) -> Result<()> {
    engine.write_slow_query_log_jsonl(artifact_dir.join("slow-query-log.jsonl"))?;
    engine.write_blackbox_report(
        artifact_dir.clone(),
        artifact_dir.join("blackbox"),
        Some("nmem-run".to_string()),
        BlackboxRunStatus::Completed,
        Some(0),
    )?;
    Ok(())
}
```

`write_blackbox_report` records artifact names, byte lengths, checksums,
protocol/ready state, blocker codes, missing evidence, failed checks, and JSONL
line counts. It does not copy raw artifact payloads, Cypher text, query
parameters, or absolute artifact paths. `write_slow_query_log_jsonl` defaults to
redacted slow-query events and does not include query text.

### Production-copy memory profile

Memory replacement evidence must use a point-in-time HawDB copy populated from
the same production snapshot and the same bounded route queries used for parity.
Run each query family in a fresh process so the process high-water RSS is
attributable to that workload:

```bash
HAWDB_ENABLE_COMPATIBILITY_TOOLS=1 cargo run --quiet --bin hawdb -- \
  nowledge-bounded-read-report \
  --max-rows 512 \
  --max-estimated-payload-bytes 4194304 \
  "$HAWDB_PRODUCTION_COPY" \
  "$BOUNDED_ROUTE_QUERY"
```

The report records `steady_resident_bytes`, `peak_resident_bytes`, total
page-fault deltas, Unix-only minor and major page-fault deltas, cumulative
operator-output `intermediate_rows` and `intermediate_payload_bytes`, and final
`output_payload_bytes`. Intermediate counts intentionally include a row once
per physical operator boundary; they measure pipeline work rather than distinct
result rows. Compare these measured values with the previous wrapper under the
same process isolation, query, parameters, target, and copied snapshot.
Configured buffer-pool sizes are not accepted as memory evidence.

## 1. Export The Contract

```bash
cargo run --quiet --bin hawdb -- \
  nowledge-fixture-contract nowledge-memory-core \
  > "$NMEM_PREFLIGHT_ROOT/contract.json"
```

The contract is the machine-readable Nowledge business fixture. It includes
setup, Cypher statements, parameters, expected rows, effect checks, and
projected-graph requests.

## 2. Run The Full Wrapper Contract

```bash
cargo run --quiet --bin hawdb -- \
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
cargo run --quiet --bin hawdb -- \
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

Use an isolated HawDB database for storage-recovery and background-maintenance
evidence:

```bash
TMPDIR="$NMEM_PREFLIGHT_ROOT" cargo run --quiet --bin hawdb -- \
  > "$NMEM_PREFLIGHT_ROOT/hawdb-demo.out"

export HAWDB_PREFLIGHT_DB="$NMEM_PREFLIGHT_ROOT/hawdb-demo"

cargo run --quiet --bin hawdb -- \
  storage-recovery-report \
  --max-wal-replay-entries 100 \
  --require-durable \
  --require-checkpoint-boundary \
  --require-bounded-wal-replay \
  --require-clean-tail \
  "$HAWDB_PREFLIGHT_DB" \
  > "$NMEM_PREFLIGHT_ROOT/storage-recovery.json"

cargo run --quiet --bin hawdb -- \
  background-maintenance-report \
  --require-cutover-ready \
  "$HAWDB_PREFLIGHT_DB" \
  > "$NMEM_PREFLIGHT_ROOT/background-maintenance.json"
```

These reports prove HawDB-side recovery and background QoS readiness. They do
not prove Nowledge wrapper parity.

## 5. Run The Migration Gate

Run the migration gate against the Nowledge graph-source checkout and the real
previous-wrapper adapter. Use the `previous_wrapper_contract_evidence` object
from the full contract report:

```bash
jq '.previous_wrapper_contract_evidence' \
  "$NMEM_PREFLIGHT_ROOT/contract-evidence.json" \
  > "$NMEM_PREFLIGHT_ROOT/previous-wrapper-contract-evidence.json"

cargo run --quiet --bin hawdb -- \
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

Generate `query-runtime-preflight.json` before this step. The replacement
summary is fail-closed and must consume the same bounded-read, query-family,
search projection, search candidate, and query-runtime evidence that the final
preflight bundle will later verify.

If you have raw probes instead of precompiled evidence, generate the evidence
files first:

```bash
cargo run --quiet --bin hawdb -- \
  nowledge-search-projection-evidence \
  --require-ready \
  "$NMEM_PREFLIGHT_ROOT/hawdb-search-projection-probe.json" \
  > "$NMEM_PREFLIGHT_ROOT/search-projection-evidence.json"

cargo run --quiet --bin hawdb -- \
  nowledge-search-projection-shadow-evidence \
  --require-ready \
  --primary-probe-json "$NMEM_PREFLIGHT_ROOT/lancedb-search-projection-probe.json" \
  --shadow-probe-json "$NMEM_PREFLIGHT_ROOT/hawdb-search-projection-probe.json" \
  > "$NMEM_PREFLIGHT_ROOT/search-projection-shadow-evidence.json"

cargo run --quiet --bin hawdb -- \
  nowledge-search-candidate-shadow-evidence \
  --require-ready \
  "$NMEM_PREFLIGHT_ROOT/search-candidate-shadow-probe.json" \
  > "$NMEM_PREFLIGHT_ROOT/search-candidate-shadow-evidence.json"
```

```bash
cargo run --quiet --bin hawdb -- \
  nowledge-replacement-summary \
  --require-production-ready \
  --query-family-evidence-json "$NMEM_PREFLIGHT_ROOT/query-family-evidence.json" \
  --bounded-read-evidence-json "$NMEM_PREFLIGHT_ROOT/bounded-read-evidence.json" \
  --query-runtime-preflight-json "$NMEM_PREFLIGHT_ROOT/query-runtime-preflight.json" \
  --search-projection-evidence-json "$NMEM_PREFLIGHT_ROOT/search-projection-evidence.json" \
  --search-projection-shadow-evidence-json "$NMEM_PREFLIGHT_ROOT/search-projection-shadow-evidence.json" \
  --search-candidate-shadow-evidence-json "$NMEM_PREFLIGHT_ROOT/search-candidate-shadow-evidence.json" \
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

## 7. Verify The Rust Library Readiness Surface

The final preflight also requires the embedded Rust library surface to open and
aggregate the same cutover evidence. This keeps the release gate tied to the
API that Mem will actually call instead of only checking standalone CLI
artifacts:

```bash
cargo run --quiet --bin hawdb -- \
  nowledge-mem-library-readiness \
  --require-ready \
  --search-projection "$NMEM_PREFLIGHT_ROOT/hawdb-search-index" \
  --bounded-read-evidence-json "$NMEM_PREFLIGHT_ROOT/bounded-read-evidence.json" \
  --query-family-evidence-json "$NMEM_PREFLIGHT_ROOT/query-family-evidence.json" \
  --search-projection-evidence-json "$NMEM_PREFLIGHT_ROOT/search-projection-evidence.json" \
  --search-projection-shadow-evidence-json "$NMEM_PREFLIGHT_ROOT/search-projection-shadow-evidence.json" \
  "$NMEM_PREFLIGHT_ROOT/hawdb-demo" \
  > "$NMEM_PREFLIGHT_ROOT/library-readiness.json"
```

When using the bundle runner, an existing `library-readiness.json` can be
provided with `--library-readiness-json`; otherwise the runner generates it
from the preflight graph and the already materialized bounded-read,
query-family, search-projection, and search-projection-shadow evidence files.
The runner can also materialize those search-projection evidence files from
`--search-projection-probe-json` or from the paired
`--search-projection-shadow-primary-probe-json` and
`--search-projection-shadow-probe-json` inputs. When both a standalone
projection probe and a shadow probe are present, the standalone probe is used
for `search-projection-evidence.json`; otherwise the shadow probe is used as
the HawDB-side projection evidence source.
Automatic generation also needs `--library-readiness-search-projection` to
point at an existing HawDB search projection so `open_report` can prove that
the embedded library opened both graph and search projection state.

Search projection probes must also publish the scan-pruning contract that Mem
relies on during the LanceDB replacement path. The HawDB probe is not ready
unless `predicate_pushdown.persisted_segment_descriptor_ready == true`,
`predicate_pushdown.segment_descriptor_scan_filter_fields_ready == true`, and
`predicate_pushdown.segment_descriptor_field_summaries` covers the required
scan filter fields:

```text
kind, external_id, source_id, space_id, unit_type, importance, confidence,
created_at, updated_at, event_start, event_end, is_latest
```

Shadow evidence carries the same requirement as
`pushdown_evidence.shadow_segment_descriptor_scan_filter_fields_ready == true`.
This keeps route-level replacement gates tied to fields that can be pruned by
segment descriptor metadata instead of only proving row-filter fallback.
Search projection evidence also recomputes incremental update readiness instead
of trusting `incremental_update.ready` alone. A ready probe must prove
`upsert_ready`, `delete_ready`, `watermark_ready`, and a concrete
`source_graph_commit_epoch`; LanceDB/HawDB shadow evidence compares that
watermark so stale or full-rebuild-only projections do not pass as incremental
replacement evidence.
Search projection probes must also include a redacted `document_identity`
summary with `id_space`, `representation`, `document_count`, and `checksum`.
The checksum is computed over sorted projection document IDs; raw IDs are not
emitted in readiness artifacts. Shadow evidence compares this identity summary
in addition to row counts so equal-size but different document sets remain
blocked.

## 8. Verify The Whole Preflight Bundle

Use the bundle checker to collapse the preflight stage artifacts into one
release-facing preflight verdict:

```bash
cargo run --quiet --bin hawdb -- \
  nowledge-previous-wrapper-preflight-check \
  --require-ready \
  --wrapper-identity "$NOWLEDGE_WRAPPER_IDENTITY" \
  --bundle-dir "$NMEM_PREFLIGHT_ROOT" \
  > "$NMEM_PREFLIGHT_ROOT/preflight-check.json"
```

The verifier checks wrapper identity consistency across the full contract,
adapter smoke, migration gate, storage recovery evidence, background
maintenance evidence, replacement summary, query runtime preflight, and Rust
library-readiness artifacts. It fails closed unless every stage is ready, the
storage/background evidence is explicitly required and present in the migration
gate, the replacement summary has no blockers, missing evidence, or next
actions, the query runtime probes all produce plan/profile evidence, and the
embedded library surface opens graph plus search projection with every
readiness area marked ready.
The final preflight is stricter than compatibility summary generation: the
replacement summary must carry `dual_engine_evidence.present == true` and
`dual_engine_evidence.ready == true`.
It also requires
`search_projection_shadow_evidence.pushdown_evidence.ready == true`,
`predicate_pushdown_parity == true`, and shadow segment descriptor scan-filter
coverage, so LanceDB/HawDB shadow parity cannot pass with row-filter fallback
alone.
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
`--replacement-summary-json`, `--query-runtime-preflight-json`, and
`--library-readiness-json` paths; explicit files override the standard names
loaded from `--bundle-dir`.

## 9. Compile Graph Route Readiness

Route readiness is generated from per-route query inventory and shadow evidence
instead of being hand-authored. Mem supplies the active graph read routes and
the Cypher statements each route executes:

```json
{
  "routes": [
    {
      "route": "/graph/overview",
      "shadow_compare_ready": true,
      "primary_ready": true,
      "queries": [
        {
          "name": "overview-memory-lookup",
          "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
          "parameters": {
            "id": "example-memory"
          },
          "require_scan_pruning": true,
          "require_pruned": true
        }
      ],
      "blocker_codes": []
    }
  ]
}
```

Mem also supplies route-level parity evidence from the Kuzu/Ladybug shadow
comparison. The graph route evidence command treats `shadow_compare_ready` in
the query inventory as local debugging input only; production readiness requires
an explicit parity artifact. Each route must independently prove full parity:
`ready: true`, `matched_per_million: 1000000`, a legacy graph primary engine
(`kuzu`, `ladybug`, or `kuzu/ladybug`), and `shadow_engine: "hawdb"`.

```json
{
  "protocol": "nmem-graph-route-parity-evidence-v1",
  "routes": [
    {
      "route": "/graph/overview",
      "ready": true,
      "matched_per_million": 1000000,
      "primary_engine": "kuzu",
      "shadow_engine": "hawdb"
    }
  ]
}
```

Generate query-runtime-backed route evidence with:

```bash
cargo run --quiet --bin hawdb -- \
  nowledge-graph-route-evidence \
  --route-parity-json "$NMEM_PREFLIGHT_ROOT/graph-route-parity.json" \
  "$NMEM_PREFLIGHT_ROOT/hawdb-demo" \
  "$NMEM_PREFLIGHT_ROOT/graph-route-queries.json" \
  > "$NMEM_PREFLIGHT_ROOT/graph-route-evidence.json"
```

Compile readiness with:

```bash
cargo run --quiet --bin hawdb -- \
  nowledge-graph-route-readiness \
  --require-ready \
  "$NMEM_PREFLIGHT_ROOT/graph-route-evidence.json" \
  > "$NMEM_PREFLIGHT_ROOT/graph-route-readiness.json"
```

The command fails closed when any required Nowledge graph read route is missing,
when a route has no shadow-compare evidence, when a route is not primary ready,
or when a ready route lacks `hawdb-nowledge-mem-query-report-v1` evidence
generated by `NowledgeMemGraph::query_with_report`. Each query report must carry
stable query identity (`query_name` and `query_index`) and
profile metadata (`elapsed_micros`, `physical_operator_counts`,
`optimizer_decision_count`, `scan_pruning_report_count`, and
`scan_pruning_reports`) so route readiness proves runtime observability rather
than only proving that a route had a successful row comparison.
`nowledge-graph-route-evidence` also recomputes route coverage from the shared
required-route list and emits `required_route_count`, `covered_route_count`,
`covered_routes`, `missing_required_routes`, and `required_routes_covered`.
It also reports `unknown_routes`, `duplicate_routes`,
`route_coverage_ready`, and `route_coverage_blocker_codes`, so partial,
duplicated, or stale route inventories are rejected at evidence generation time
before the integration readiness compiler consumes them.
Route query inventory can require scan pruning with `require_scan_pruning` and
actual row reduction with `require_pruned`; missing or weak runtime evidence
adds route blocker codes and keeps primary readiness fail-closed. When scan
pruning is required, `scan_pruning_report_count` must match the number of
`scan_pruning_reports`, and each report must include `strategy.kind`. A mismatch
emits `query_scan_pruning_report_count_mismatch`; a report without a strategy
kind emits `query_scan_pruning_strategy_missing`. Raw Cypher and parameters are
not copied into readiness reports by default.
Readiness also requires `shadow_compare_evidence_source` to be
`route_parity_evidence`, so a hand-authored route inventory cannot become
production shadow parity evidence by setting `shadow_compare_ready` alone. The
route readiness compiler also recomputes the nested `shadow_compare` identity
fields and propagates its blocker codes, so stale or manually weakened route
evidence remains fail-closed.

## 10. Run Query Runtime Preflight

The graph route report proves route-level query runtime evidence. The query
runtime preflight independently runs JSON-defined probes through the read-only
HawDB runtime with `EXPLAIN ANALYZE`, then emits plan/profile evidence without
rows, parameters, or local paths:

```bash
cargo run --quiet --bin hawdb -- \
  nowledge-query-runtime-preflight \
  --probe-json "$NMEM_PREFLIGHT_ROOT/graph-route-queries.json" \
  "$NMEM_PREFLIGHT_GRAPH" \
  > "$NMEM_PREFLIGHT_ROOT/query-runtime-preflight.json"
```

Use probes from active Nowledge Mem route fixtures. A probe can require scan
pruning evidence with `require_scan_pruning` and `require_pruned`; missing or
weak probe evidence keeps integration readiness fail-closed. Every probe must
carry a stable `name`, a required graph read `route`, and a Nowledge replacement
`query_family`; anonymous probes, stale routes, or unknown query families are
not accepted as production cutover evidence.
The command also accepts the graph-route query inventory shape used by
`nowledge-graph-route-evidence` and flattens each route query into an
independent runtime probe, so route evidence and runtime preflight can share one
Mem-owned fixture file.
Each successful probe must include a selected plan fingerprint, non-empty
selected plan operator/class counts, optimizer decision count, plan-cache state,
and `execution_profile.scan_pruning_reports` whose length matches
`scan_pruning_report_count`. Each scan-pruning report must include the selected
strategy, `pruned`, `exact_empty`, `candidate_count_before_pruning`,
`pruned_candidate_count`, `candidate_count_before_filter`, `output_count`, and
`filtered_out_count`. This keeps the runtime preflight useful for observability
and prevents a count-only probe summary from being treated as cutover evidence.

## 11. Compile The Mem Integration Bundle

The previous-wrapper preflight proves replacement behavior. The Mem integration
bundle adds the product migration boundary: HawDB must be present as a
submodule, legacy Kuzu/Ladybug and LanceDB data must still be retained
side-by-side, and `content.db` must remain available for message and source
chunk payloads.

Use the Rust bundle composer directly when debugging this final stage or when
the earlier preflight artifacts were produced by another harness:

```bash
cargo run --quiet --bin hawdb -- \
  nowledge-mem-integration-bundle \
  --require-ready \
  --submodule-path vendor/hawdb \
  --submodule-commit "$(git -C vendor/hawdb rev-parse --short HEAD)" \
  --legacy-data-retained \
  --coexistence-mode shadow \
  --content-store-present \
  --content-store-engine sqlite \
  --content-store-messages-available \
  --content-store-source-chunks-available \
  --previous-wrapper-preflight-json "$NMEM_PREFLIGHT_ROOT/preflight-check.json" \
  --replacement-summary-json "$NMEM_PREFLIGHT_ROOT/replacement-summary.json" \
  --bounded-read-evidence-json "$NMEM_PREFLIGHT_ROOT/bounded-read-evidence.json" \
  --graph-route-readiness-json "$NMEM_PREFLIGHT_ROOT/graph-route-readiness.json" \
  --query-runtime-preflight-json "$NMEM_PREFLIGHT_ROOT/query-runtime-preflight.json" \
  --search-candidate-shadow-evidence-json "$NMEM_PREFLIGHT_ROOT/search-candidate-shadow-evidence.json" \
  --library-readiness-json "$NMEM_PREFLIGHT_ROOT/library-readiness.json" \
  > "$NMEM_PREFLIGHT_ROOT/integration-bundle.json"
```

`--require-ready` makes the composer feed its output into
`nowledge-mem-integration-readiness` before returning success, so stale bounded
read evidence, missing route readiness, weak query runtime preflight, missing
library readiness, or unsafe legacy coexistence is rejected before Mem cutover
automation consumes the bundle.
Integration readiness also revalidates
`graph_route_readiness.routes[].query_reports[]`: every required graph read
route must include a ready query report with elapsed time, physical operator
counts, optimizer decision count, scan-pruning profile presence with pruning
before/after counts, and plan-cache state. A hand-authored summary with only
`route_query_runtime_ready=true` is not release-ready evidence.
`nowledge-graph-route-readiness` also independently reports route coverage
diagnostics (`covered_routes`, `unknown_routes`, `duplicate_routes`, and
`route_coverage_blocker_codes`), so direct or legacy evidence inputs remain
fail-closed even if they bypass the evidence generator. When an evidence
envelope carries route coverage fields, readiness compares them with the
recomputed route set and emits `route_coverage_evidence_mismatch` if they have
drifted; if the coverage fields are absent, it emits
`route_coverage_evidence_missing`. The final integration readiness gate consumes
these fields as required evidence, so bundles with legacy route readiness JSON
remain blocked until regenerated from the current graph-route evidence tool.
