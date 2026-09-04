#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 8 ]]; then
  echo "usage: $0 SMOKE SKEIN_CLI SKEIN_SHADOW_SELF PREVIOUS_WRAPPER_ADAPTER APPEND_FUZZ STORAGE_FUZZ OPTIMIZER_FUZZ BENCHMARK..." >&2
  exit 2
fi

readonly smoke="$1"
shift
readonly skein_cli="$1"
readonly skein_shadow_self="$2"
readonly previous_wrapper_adapter="$3"
readonly append_fuzz="$4"
readonly storage_fuzz="$5"
readonly optimizer_fuzz="$6"
shift 6
readonly -a benchmark_smokes=("$@")
readonly optimizer_smoke="${benchmark_smokes[0]}"
readonly optimizer_benchmark_group_size=6
readonly final_optimizer_benchmark_group_start=$((1 + 2 * optimizer_benchmark_group_size))
readonly row_page_lending_benchmark_index=$((final_optimizer_benchmark_group_start + 1))
readonly wal_group_commit_benchmark_index=$((${#benchmark_smokes[@]} - 1))

for executable in \
  "$skein_cli" \
  "$skein_shadow_self" \
  "$previous_wrapper_adapter" \
  "$append_fuzz" \
  "$storage_fuzz" \
  "$optimizer_fuzz" \
  "${benchmark_smokes[@]}"; do
  if [[ ! -x "$executable" ]]; then
    echo "required Bazel executable is missing: $executable" >&2
    exit 1
  fi
done

readonly work_root="${TEST_TMPDIR:-$(mktemp -d)}"
export SKEIN_ENABLE_COMPATIBILITY_TOOLS=1

run_fuzz_smokes() {
  local root="$work_root/fuzz-smokes"
  mkdir -p "$root"
  "$append_fuzz" \
    --seed 7 \
    --cases 8 \
    --steps 64 \
    --log-directory "$root" \
    > "$root/append.stdout" \
    2> "$root/append.stderr"
  "$storage_fuzz" \
    --seed 7 \
    --cases 32 \
    --log-directory "$root" \
    > "$root/storage.stdout" \
    2> "$root/storage.stderr"
  "$optimizer_fuzz" \
    --seed 7 \
    --cases 12 \
    --log-directory "$root" \
    > "$root/optimizer.stdout" \
    2> "$root/optimizer.stderr"
  for stream in \
    "$root/append.stdout" \
    "$root/append.stderr" \
    "$root/storage.stdout" \
    "$root/storage.stderr" \
    "$root/optimizer.stdout" \
    "$root/optimizer.stderr"; do
    test ! -s "$stream"
  done
  python3 - \
    "$root/skein-append-fuzz-seed-7-cases-8-steps-64-cur.json" \
    "$root/skein-storage-fuzz-seed-7-cases-32-cur.json" \
    "$root/skein-optimizer-fuzz-seed-7-cases-12-cur.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as file:
    append = json.load(file)
with open(sys.argv[2], encoding="utf-8") as file:
    storage = json.load(file)
with open(sys.argv[3], encoding="utf-8") as file:
    optimizer = json.load(file)

assert append["protocol"] == "skein-append-state-machine-fuzz-v1"
assert append["campaign_seed"] == 7
assert append["case_count"] == 8
assert append["steps_per_case"] == 64
assert append["success"] is True
assert storage["protocol"] == "skein-storage-parser-fuzz-v1"
assert storage["seed"] == 7
assert storage["requested_case_count"] == 32
assert storage["failed_case_count"] == 0
assert storage["success"] is True
assert optimizer["protocol"] == "skein-multi-oracle-fuzz-v1"
assert optimizer["seed"] == 7
assert optimizer["requested_case_count"] == 12
assert optimizer["failed_case_count"] == 0
assert optimizer["success"] is True
PY
}

run_optimizer_summary_smoke() {
  local root="$work_root/optimizer-smoke"
  mkdir -p "$root"
  "$optimizer_smoke" > "$root/output.txt"
  python3 - "$root/output.txt" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as file:
    lines = file.readlines()

prefix = "optimizer_smoke_summaries_json "
matches = [line[len(prefix):] for line in lines if line.startswith(prefix)]
assert len(matches) == 1, f"expected one {prefix.strip()} line, found {len(matches)}"

summaries = json.loads(matches[0])
assert isinstance(summaries, list) and summaries, "optimizer smoke summaries must be a non-empty array"

required_fields = {
    "case",
    "groups",
    "rows",
    "cost",
    "operator_counts",
    "class_counts",
    "fingerprint",
}
for summary in summaries:
    missing = required_fields - set(summary)
    assert not missing, f"{summary.get('case', '<unknown>')} missing fields: {sorted(missing)}"
    assert isinstance(summary["case"], str) and summary["case"], "case must be a non-empty string"
    assert isinstance(summary["groups"], int) and summary["groups"] > 0, "groups must be positive"
    assert isinstance(summary["rows"], int) and summary["rows"] >= 0, "rows must be non-negative"
    assert isinstance(summary["cost"], (int, float)) and summary["cost"] >= 0, "cost must be non-negative"
    assert isinstance(summary["operator_counts"], dict) and summary["operator_counts"], "operator_counts must be non-empty"
    assert isinstance(summary["class_counts"], dict) and summary["class_counts"], "class_counts must be non-empty"
    assert isinstance(summary["fingerprint"], str) and summary["fingerprint"], "fingerprint must be a non-empty string"
PY
}

run_optimizer_benchmark_group_smoke() {
  local start="$1"
  local length="$2"
  if (( length <= 0 || start + length > ${#benchmark_smokes[@]} )); then
    echo "optimizer benchmark group [$start, $((start + length))) exceeds ${#benchmark_smokes[@]} benchmarks" >&2
    exit 2
  fi
  local root="$work_root/optimizer-benchmarks-$start"
  mkdir -p "$root"
  local benchmark
  for benchmark in "${benchmark_smokes[@]:start:length}"; do
    "$benchmark" > "$root/$(basename "$benchmark").txt"
  done
}

run_final_optimizer_benchmark_group_smoke() {
  run_optimizer_benchmark_group_smoke \
    "$final_optimizer_benchmark_group_start" \
    "$((row_page_lending_benchmark_index - final_optimizer_benchmark_group_start))"
  run_optimizer_benchmark_group_smoke \
    "$((row_page_lending_benchmark_index + 1))" \
    "$((wal_group_commit_benchmark_index - row_page_lending_benchmark_index - 1))"
}

run_fixture_contract_smoke() {
  local root="$work_root/fixture-contract"
  mkdir -p "$root"
  cat > "$root/contract.json" <<'JSON'
{
  "protocol": "skein-nowledge-fixture-contract",
  "fixture": "mini",
  "check_count": 2,
  "setup": [],
  "checks": [
    {
      "index": 0,
      "kind": "cypher",
      "name": "first",
      "execution_mode": "database",
      "setup": [],
      "statement": {
        "command_request": {
          "op": "query",
          "cypher": "MATCH (n) RETURN n",
          "parameters": {}
        }
      },
      "expected_rows": {"kind": "row_count", "count": 1}
    },
    {
      "index": 1,
      "kind": "cypher",
      "name": "second",
      "execution_mode": "database",
      "setup": [],
      "statement": {
        "command_request": {
          "op": "query",
          "cypher": "MATCH (n) RETURN n",
          "parameters": {}
        }
      },
      "expected_rows": {"kind": "row_count", "count": 0}
    }
  ]
}
JSON

  "$skein_cli" nowledge-fixture-contract-command-check \
    --start-check 1 \
    "$root/contract.json" \
    python3 -c 'import json,sys; json.load(sys.stdin); print(json.dumps({"rows": []}))' \
    > "$root/selected.json"
  grep -q '"selected_subset_ready": true' "$root/selected.json"
  grep -q '"full_contract_ready": false' "$root/selected.json"
  grep -q '"full_contract_not_checked"' "$root/selected.json"
  grep -q '"previous_wrapper_contract_evidence"' "$root/selected.json"
  grep -q '"missing_wrapper_identity"' "$root/selected.json"

  if "$skein_cli" nowledge-fixture-contract-command-check \
    --require-full-contract \
    --start-check 1 \
    "$root/contract.json" \
    python3 -c 'import json,sys; json.load(sys.stdin); print(json.dumps({"rows": []}))' \
    > "$root/full.json" 2> "$root/full.err"; then
    echo "expected selected fixture slice to fail full-contract readiness" >&2
    return 1
  fi
  grep -q '"selected_subset_ready": true' "$root/full.json"
  grep -q '"required_contract_ready": false' "$root/full.json"
  grep -q '"required_contract_blocker_codes"' "$root/full.json"
  grep -q '"full_contract_not_checked"' "$root/full.json"
  grep -q "fixture contract command check failed" "$root/full.err"
}

create_demo_database() {
  local root="$1"
  mkdir -p "$root"
  TMPDIR="$root" "$skein_cli" > "$root/demo.out"
}

run_storage_recovery_smoke() {
  local root="$work_root/storage-recovery"
  create_demo_database "$root"
  "$skein_cli" storage-recovery-report \
    --max-wal-replay-entries 100 \
    --require-durable \
    --require-checkpoint-boundary \
    --require-bounded-wal-replay \
    --require-clean-tail \
    "$root/skein-demo" > "$root/recovery.json"
  grep -q '"protocol": "skein-storage-recovery-report"' "$root/recovery.json"
  grep -q '"durable_recovery_observed": true' "$root/recovery.json"
  grep -q '"checkpoint_boundary_present": true' "$root/recovery.json"
  grep -q '"wal_replay_bounded": true' "$root/recovery.json"
  grep -q '"torn_tail_clean": true' "$root/recovery.json"
}

run_background_maintenance_smoke() {
  local root="$work_root/background-maintenance"
  create_demo_database "$root"
  "$skein_cli" background-maintenance-report \
    --require-cutover-ready \
    "$root/skein-demo" > "$root/background.json"
  grep -q '"protocol": "skein-background-maintenance-report"' "$root/background.json"
  grep -q '"total_candidates": 4' "$root/background.json"
  grep -q '"kind": "search_projection_graph_delta"' "$root/background.json"
  grep -q '"kind": "skein_lightning_bootstrap_export"' "$root/background.json"
  grep -q '"priority": "background"' "$root/background.json"
  grep -q '"admission": "admit"' "$root/background.json"
}

write_previous_wrapper() {
  local path="$1"
  cat > "$path" <<'PY'
import json
import sys

for line in sys.stdin:
    request = json.loads(line)
    op = request.get("op")
    if op == "query":
        print(json.dumps({"rows": [{"title": "Adapter Smoke"}]}), flush=True)
    elif op == "execute_session":
        print(json.dumps({"results": [{"rows": [{"title": "Adapter Smoke"}]}]}), flush=True)
    elif op == "project_graph":
        print(json.dumps({"ok": {
            "node_count": 1,
            "edge_count": 0,
            "incoming": [],
            "communities": [],
            "hierarchical_communities": [],
            "page_rank_scores": [[0, 1.0]],
            "page_rank_top_node": 0,
        }}), flush=True)
    else:
        print(json.dumps({"error": {"class": "execution", "message": f"unsupported op {op}"}}), flush=True)
PY
}

run_previous_wrapper_adapter_smoke() {
  local root="$work_root/previous-wrapper"
  mkdir -p "$root"
  write_previous_wrapper "$root/previous_wrapper.py"
  "$skein_cli" external-shadow-adapter-smoke \
    --require-previous-wrapper \
    --shadow-trace "$root/adapter-shadow.jsonl" \
    previous-wrapper \
    "$previous_wrapper_adapter" \
    --persistent-command python3 -u "$root/previous_wrapper.py" \
    > "$root/adapter.json"
  grep -q '"protocol": "skein-external-shadow-adapter-smoke"' "$root/adapter.json"
  grep -q '"engine_kind": "previous_wrapper"' "$root/adapter.json"
  grep -q '"wrapper_identity": null' "$root/adapter.json"
  grep -q '"adapter_smoke_ready": true' "$root/adapter.json"
  grep -q '"matched_checks": 2' "$root/adapter.json"
  grep -q '"primary_only_checks": 0' "$root/adapter.json"
  grep -q '"request_count": 4' "$root/adapter.json"
  test -s "$root/adapter-shadow.jsonl"
}

write_migration_evidence() {
  local root="$1"
  python3 - "$root" <<'PY'
import copy
import json
import os
import sys

root = sys.argv[1]

def write(name, value):
    with open(os.path.join(root, name), "w", encoding="utf-8") as file:
        json.dump(value, file)

previous_wrapper = {
    "ready": True,
    "evidence_kind": "previous_wrapper_contract",
    "wrapper_identity": "nowledge-previous-wrapper:ci-smoke",
    "requires_full_contract_ready": True,
    "requires_wrapper_identity": True,
    "blocker_codes": [],
    "blockers": [],
}
write("previous-wrapper-contract-evidence.json", previous_wrapper)
write("contract-evidence.json", {
    "required_contract_ready": True,
    "full_contract_checked": True,
    "full_contract_ready": True,
    "selected_checks": 2,
    "check_count": 2,
    "required_contract_blocker_codes": [],
    "previous_wrapper_contract_evidence": previous_wrapper,
})
write("adapter-smoke.json", {
    "adapter_smoke_ready": True,
    "engine_kind": "previous_wrapper",
    "wrapper_identity": "nowledge-previous-wrapper:ci-smoke",
    "primary_only_checks": 0,
    "dual_engine_evidence": {
        "ready": False,
        "primary_check_count": 2,
        "shadow_check_count": 2,
        "matched_check_count": 1,
        "primary_only_check_count": 1,
    },
    "blocker_codes": [],
})

probe = {
    "name": "probe:/memories/{id}",
    "route": "/memories/{id}",
    "query_family": "memory_lookup",
    "ready": True,
    "success": True,
    "output_row_count": 1,
    "selected_plan_fingerprint": "ProjectExec(IndexNodeSeek)",
    "selected_plan_operator_counts": {"IndexNodeSeek": 1, "ProjectExec": 1},
    "selected_plan_class_counts": {"access": 1, "relational": 1},
    "optimizer_decision_count": 2,
    "plan_cache_lookup": "miss",
    "plan_cache": {
        "lookup": "miss",
        "bypass_reason": None,
        "cacheable": True,
        "hit": False,
        "miss": True,
        "bypassed": False,
    },
    "execution_profile": {
        "scan_pruning_report_count": 1,
        "pruned_scan_count": 1,
        "scan_pruning_reports": [{
            "label_id": 1,
            "strategy": {"kind": "property_eq", "property": "id"},
            "pruned": True,
            "exact_empty": False,
            "candidate_count_before_pruning": 2,
            "pruned_candidate_count": 1,
            "candidate_count_before_filter": 1,
            "output_count": 1,
            "filtered_out_count": 0,
        }],
    },
    "blocker_codes": [],
}
routes = [
    "/graph/overview",
    "/graph/explore",
    "/graph/expand/{node_id}",
    "/graph/live-preview",
    "/graph/live-preview/{node_id}",
    "/graph/community-members/{community_id}",
    "/library/community/{community_id}/subgraph",
    "/library/community/{community_id}/recent-memories",
    "/library/community/{community_id}/related",
    "/graph/analysis",
    "/graph/augmentation/state",
    "/graph/augmentation/pagerank/plan",
    "/graph/node-details/{node_id}",
    "/graph/orphans",
    "/graph/shortest-path",
]
probes = []
for route in routes:
    route_probe = copy.deepcopy(probe)
    route_probe["name"] = f"probe:{route}"
    route_probe["route"] = route
    probes.append(route_probe)
write("query-runtime-preflight.json", {
    "protocol": "skein-nowledge-query-runtime-preflight-v1",
    "ready": True,
    "database_opened": True,
    "probe_count": len(routes),
    "passed_probe_count": len(routes),
    "failed_probe_count": 0,
    "required_route_count": len(routes),
    "covered_route_count": len(routes),
    "covered_routes": routes,
    "missing_required_routes": [],
    "required_routes_covered": True,
    "unknown_routes": [],
    "duplicate_routes": [],
    "route_coverage_ready": True,
    "route_coverage_blocker_codes": [],
    "blocker_codes": [],
    "failed_checks": [],
    "probes": probes,
})

areas = {
    name: {"ready": True, "blocker_codes": []}
    for name in [
        "graph",
        "query",
        "storage",
        "background",
        "query_family",
        "search_projection",
        "search_projection_shadow",
    ]
}
write("library-readiness.json", {
    "protocol": "skein-nowledge-mem-library-readiness-v1",
    "present": True,
    "ready": True,
    "mode": "shadow_read_only",
    "ready_area_count": 7,
    "blocked_area_count": 0,
    "blocker_codes": [],
    "open_report": {
        "protocol": "skein-nowledge-mem-open-report",
        "mode": "shadow_read_only",
        "graph_opened": True,
        "search_projection_opened": True,
    },
    "readiness_by_area": areas,
})
write("cutover-controls.json", {
    "protocol": "skein-nowledge-mem-cutover-controls-v1",
    "ready": True,
    "controls": {
        "graph_reads": "skein",
        "search_reads": "skein",
        "dual_writes": "enabled",
        "initial_import": "disabled",
        "projection_catch_up": "enabled",
    },
    "graph": {"read_selected_skein": True, "read_effective": True},
    "search": {"read_selected_skein": True, "read_effective": True},
    "work": {
        "dual_writes_enabled": True,
        "initial_import_enabled": False,
        "initial_import_inactive_for_cutover": True,
        "initial_import_cutover_catch_up_ready": False,
        "initial_import_safe_for_read_cutover": True,
        "projection_catch_up_enabled": True,
    },
    "production_status": {
        "graph": {"skein_cutover_effective": True},
        "search": {"skein_cutover_effective": True},
    },
    "redaction": {
        "query_text_copied": False,
        "parameters_copied": False,
        "local_paths_copied": False,
    },
    "blocker_codes": [],
})
write("operations-readiness.json", {
    "protocol": "skein-nowledge-mem-operations-readiness-v1",
    "present": True,
    "ready": True,
    "mode": "writable_cutover",
    "graph": {"open": True, "read_only": False, "commit_epoch": 42},
    "search_projection": {"open": True, "commit_lag": 0, "stale": False},
    "storage_lifecycle": {"ready": True, "action": "ready"},
    "readiness": {
        "storage_lifecycle_ready": True,
        "storage_recovery_ready": True,
        "slow_query_ready": True,
        "background_maintenance_ready": True,
    },
    "redaction": {
        "query_text_copied": False,
        "parameters_copied": False,
        "local_paths_copied": False,
    },
    "blocker_codes": [],
})
PY
}

run_migration_gate_smoke() {
  local root="$work_root/migration-ready"
  create_demo_database "$root"
  "$skein_cli" storage-recovery-report \
    --max-wal-replay-entries 100 \
    --require-durable \
    --require-checkpoint-boundary \
    --require-bounded-wal-replay \
    --require-clean-tail \
    "$root/skein-demo" > "$root/recovery.json"
  "$skein_cli" background-maintenance-report \
    --require-cutover-ready \
    "$root/skein-demo" > "$root/background.json"
  write_migration_evidence "$root"

  mkdir -p "$root/crates/nmem-graph/src"
  cat > "$root/crates/nmem-graph/src/repo.rs" <<'RS'
pub fn query() -> &'static str {
    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
}
RS

  "$skein_cli" nowledge-cypher-migration-gate \
    --require-ready \
    --allow-self-shadow \
    --shadow-ready \
    --shadow-trace "$root/shadow.jsonl" \
    --require-storage-recovery-evidence \
    --storage-recovery-report-json "$root/recovery.json" \
    --require-background-maintenance-evidence \
    --background-maintenance-report-json "$root/background.json" \
    --previous-wrapper-contract-evidence-json "$root/previous-wrapper-contract-evidence.json" \
    "$root" \
    self \
    "$skein_shadow_self" \
    > "$root/migration-gate.json"
  grep -q '"decision": "ready"' "$root/migration-gate.json"
  grep -q '"shadow_run"' "$root/migration-gate.json"
  grep -q '"evidence_kind": "protocol_smoke"' "$root/migration-gate.json"
  grep -q '"self_shadow": true' "$root/migration-gate.json"
  grep -q '"cutover_evidence"' "$root/migration-gate.json"
  grep -q '"eligible": false' "$root/migration-gate.json"
  grep -q '"storage_recovery_present": true' "$root/migration-gate.json"
  grep -q '"storage_recovery_ready": true' "$root/migration-gate.json"
  grep -q '"storage_recovery_protocol_matches": true' "$root/migration-gate.json"
  grep -q '"background_maintenance_present": true' "$root/migration-gate.json"
  grep -q '"background_maintenance_ready": true' "$root/migration-gate.json"
  grep -q '"background_maintenance_protocol_matches": true' "$root/migration-gate.json"
  grep -q '"previous_wrapper_contract_evidence"' "$root/migration-gate.json"
  grep -q '"wrapper_identity": "nowledge-previous-wrapper:ci-smoke"' "$root/migration-gate.json"
  grep -q '"shadow_ready"' "$root/migration-gate.json"
  grep -q '"shadow_trace"' "$root/migration-gate.json"
  grep -q '"request_count"' "$root/migration-gate.json"
  grep -q '"project_graph"' "$root/migration-gate.json"
  test -s "$root/shadow.jsonl"

  if "$skein_cli" nowledge-replacement-summary \
    --require-production-ready \
    "$root/migration-gate.json" \
    > "$root/replacement-summary.json" 2> "$root/replacement.err"; then
    echo "expected protocol smoke replacement summary to fail production readiness" >&2
    return 1
  fi
  grep -q '"production_cutover_ready": false' "$root/replacement-summary.json"
  grep -q '"production_replacement_per_million": 0' "$root/replacement-summary.json"
  grep -q '"cutover_evidence"' "$root/replacement-summary.json"
  grep -q '"shadow_evidence"' "$root/replacement-summary.json"
  grep -q '"previous_wrapper_contract_evidence"' "$root/replacement-summary.json"
  grep -q '"wrapper_identity": "nowledge-previous-wrapper:ci-smoke"' "$root/replacement-summary.json"
  grep -q "nowledge replacement summary is not production cutover ready" "$root/replacement.err"

  "$skein_cli" nowledge-previous-wrapper-preflight-check \
    --wrapper-identity "nowledge-previous-wrapper:ci-smoke" \
    --bundle-dir "$root" \
    > "$root/preflight.json"
  grep -q '"protocol": "skein-nowledge-previous-wrapper-preflight-check"' "$root/preflight.json"
  grep -q '"ready": false' "$root/preflight.json"
  grep -q '"migration_gate"' "$root/preflight.json"
  grep -q '"replacement_summary"' "$root/preflight.json"
  grep -q '"release_summary"' "$root/preflight.json"
  grep -q '"production_replacement_per_million": 0' "$root/preflight.json"
  python3 - "$root/preflight.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as file:
    report = json.load(file)

checks = {check["name"]: check for check in report["checks"]}
assert checks["storage_recovery"]["ready"] is True
assert checks["background_maintenance"]["ready"] is True
assert report["failed_checks"] == [
    "adapter_smoke",
    "migration_gate",
    "replacement_summary",
    "replacement_summary_route_catalog",
    "replacement_summary_graph_route",
    "replacement_summary_bounded_read",
    "query_runtime_preflight",
    "library_readiness",
]
assert checks["adapter_smoke"]["failed_evidence_fields"] == [
    "dual_engine_evidence.ready",
    "dual_engine_evidence.matched_check_count",
    "dual_engine_evidence.primary_only_check_count",
]
assert report["release_summary"]["wrapper_identity"] == "nowledge-previous-wrapper:ci-smoke"
assert report["release_summary"]["adapter_dual_engine_ready"] is False
assert report["release_summary"]["adapter_dual_engine_counts_consistent"] is False
assert report["release_summary"]["adapter_dual_engine_primary_only_check_count"] == 1
assert report["release_summary"]["production_replacement_per_million"] == 0
assert report["release_summary"]["shadow_evidence_ready"] is False
assert report["release_summary"]["background_maintenance_executable_search_projection_graph_delta_count"] >= 0
assert report["release_summary"]["background_maintenance_admitted_search_projection_graph_delta_count"] >= 0
PY

  if "$skein_cli" nowledge-previous-wrapper-preflight-check \
    --require-ready \
    --wrapper-identity "nowledge-previous-wrapper:ci-smoke" \
    --bundle-dir "$root" \
    > "$root/preflight-required.json" 2> "$root/preflight-required.err"; then
    echo "expected protocol smoke preflight bundle to fail readiness" >&2
    return 1
  fi
  grep -q '"ready": false' "$root/preflight-required.json"
  grep -q "nowledge previous-wrapper preflight is not ready" "$root/preflight-required.err"
}

run_migration_gate_blocked_smoke() {
  local root="$work_root/migration-blocked"
  mkdir -p "$root/crates/nmem-graph/src"
  cat > "$root/crates/nmem-graph/src/repo.rs" <<'RS'
pub fn query() -> &'static str {
    "MATCH (m:Memory) WHERE m.id = $id RETURN m.uncovered_property"
}
RS
  if "$skein_cli" nowledge-cypher-migration-gate \
    --require-ready \
    --allow-self-shadow \
    "$root" \
    self \
    "$skein_shadow_self" \
    > "$root/gate.json" 2> "$root/gate.err"; then
    echo "expected blocked migration gate to fail" >&2
    return 1
  fi
  grep -q '"decision": "blocked"' "$root/gate.json"
  grep -q '"shadow_run"' "$root/gate.json"
  grep -q '"evidence_kind": "protocol_smoke"' "$root/gate.json"
  grep -q '"cutover_evidence"' "$root/gate.json"
  grep -q '"eligible": false' "$root/gate.json"
  grep -q '"shadow_ready"' "$root/gate.json"
  grep -q "nowledge migration gate is blocked" "$root/gate.err"
}

case "$smoke" in
  fuzz) run_fuzz_smokes ;;
  optimizer_summary) run_optimizer_summary_smoke ;;
  optimizer_group_1) run_optimizer_benchmark_group_smoke 1 "$optimizer_benchmark_group_size" ;;
  optimizer_group_2) run_optimizer_benchmark_group_smoke "$((1 + optimizer_benchmark_group_size))" "$optimizer_benchmark_group_size" ;;
  optimizer_group_3) run_final_optimizer_benchmark_group_smoke ;;
  relational_row_page_lending) run_optimizer_benchmark_group_smoke "$row_page_lending_benchmark_index" 1 ;;
  wal_group_commit) run_optimizer_benchmark_group_smoke "$wal_group_commit_benchmark_index" 1 ;;
  fixture_contract) run_fixture_contract_smoke ;;
  storage_recovery) run_storage_recovery_smoke ;;
  background_maintenance) run_background_maintenance_smoke ;;
  previous_wrapper_adapter) run_previous_wrapper_adapter_smoke ;;
  migration_gate) run_migration_gate_smoke ;;
  migration_gate_blocked) run_migration_gate_blocked_smoke ;;
  *)
    echo "unknown Linux CI smoke: $smoke" >&2
    exit 2
    ;;
esac
