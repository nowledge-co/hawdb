#!/usr/bin/env bash
# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
cd "$repo_root"

usage() {
  cat <<'EOF'
usage: scripts/nowledge-previous-wrapper-preflight.sh \
  --preflight-root <dir> \
  --nowledge-root <dir> \
  --wrapper-identity <id> \
  [--shadow-timeout-ms <ms>] \
  [--search-projection-evidence-json <path>] \
  [--search-projection-probe-json <path>] \
  [--search-projection-shadow-evidence-json <path>] \
  [--search-projection-shadow-primary-probe-json <path>] \
  [--search-projection-shadow-probe-json <path>] \
  [--search-candidate-shadow-evidence-json <path>] \
  [--search-candidate-shadow-probe-json <path>] \
  [--bounded-read-evidence-json <path>] \
  [--bounded-read-report-json <path>] \
  [--bounded-read-database <path>] \
  [--bounded-read-cypher <cypher>] \
  [--bounded-read-params-json <json-object>] \
  [--bounded-read-max-rows <n>] \
  [--bounded-read-max-estimated-payload-bytes <n>] \
  [--library-readiness-json <path>] \
  [--library-readiness-graph <path>] \
  [--library-readiness-search-projection <path>] \
  [--query-runtime-preflight-json <path>] \
  [--query-runtime-probe-json <path>] \
  [--query-runtime-database <path>] \
  [--require-integration-readiness] \
  [--graph-route-query-json <path>] \
  [--graph-route-database <path>] \
  [--graph-route-parity-json <path>] \
  [--graph-route-evidence-json <path>] \
  [--graph-route-readiness-json <path>] \
  [--integration-submodule-path <path>] \
  [--integration-submodule-commit <commit>] \
  [--integration-legacy-data-retained] \
  [--integration-legacy-data-deleted] \
  [--integration-coexistence-mode shadow|side_by_side] \
  [--integration-content-store-path <path>] \
  [--integration-content-store-present] \
  [--integration-content-store-engine <engine>] \
  [--integration-content-store-messages-available] \
  [--integration-content-store-source-chunks-available] \
  -- <wrapper-command> [args...]

Runs the HawDB-side Nowledge previous-wrapper production preflight bundle.
The wrapper command must own all Kuzu/Ladybug dependencies and must read from
an isolated Nowledge data copy, not from the live application database.
EOF
}

preflight_root=
nowledge_root=
wrapper_identity=
shadow_timeout_ms=
search_projection_evidence_json=
search_projection_probe_json=
search_projection_shadow_evidence_json=
search_projection_shadow_primary_probe_json=
search_projection_shadow_probe_json=
search_candidate_shadow_evidence_json=
search_candidate_shadow_probe_json=
bounded_read_evidence_json=
bounded_read_report_json=
bounded_read_database=
bounded_read_cypher=
bounded_read_params_json=
bounded_read_max_rows=
bounded_read_max_estimated_payload_bytes=
library_readiness_json=
library_readiness_graph=
library_readiness_search_projection=
query_runtime_preflight_json=
query_runtime_probe_json=
query_runtime_database=
require_integration_readiness=false
graph_route_query_json=
graph_route_database=
graph_route_parity_json=
graph_route_evidence_json=
graph_route_readiness_json=
integration_submodule_path=
integration_submodule_commit=
integration_legacy_data_retained=false
integration_legacy_data_deleted=false
integration_coexistence_mode=
integration_content_store_path=
integration_content_store_present=false
integration_content_store_engine=
integration_content_store_messages_available=false
integration_content_store_source_chunks_available=false

while (($# > 0)); do
  case "$1" in
    --preflight-root)
      preflight_root="${2:-}"
      shift 2
      ;;
    --nowledge-root)
      nowledge_root="${2:-}"
      shift 2
      ;;
    --wrapper-identity)
      wrapper_identity="${2:-}"
      shift 2
      ;;
    --shadow-timeout-ms)
      shadow_timeout_ms="${2:-}"
      shift 2
      ;;
    --search-projection-evidence-json)
      search_projection_evidence_json="${2:-}"
      shift 2
      ;;
    --search-projection-probe-json)
      search_projection_probe_json="${2:-}"
      shift 2
      ;;
    --search-projection-shadow-evidence-json)
      search_projection_shadow_evidence_json="${2:-}"
      shift 2
      ;;
    --search-projection-shadow-primary-probe-json)
      search_projection_shadow_primary_probe_json="${2:-}"
      shift 2
      ;;
    --search-projection-shadow-probe-json)
      search_projection_shadow_probe_json="${2:-}"
      shift 2
      ;;
    --search-candidate-shadow-evidence-json)
      search_candidate_shadow_evidence_json="${2:-}"
      shift 2
      ;;
    --search-candidate-shadow-probe-json)
      search_candidate_shadow_probe_json="${2:-}"
      shift 2
      ;;
    --bounded-read-evidence-json)
      bounded_read_evidence_json="${2:-}"
      shift 2
      ;;
    --bounded-read-report-json)
      bounded_read_report_json="${2:-}"
      shift 2
      ;;
    --bounded-read-database)
      bounded_read_database="${2:-}"
      shift 2
      ;;
    --bounded-read-cypher)
      bounded_read_cypher="${2:-}"
      shift 2
      ;;
    --bounded-read-params-json)
      bounded_read_params_json="${2:-}"
      shift 2
      ;;
    --bounded-read-max-rows)
      bounded_read_max_rows="${2:-}"
      shift 2
      ;;
    --bounded-read-max-estimated-payload-bytes)
      bounded_read_max_estimated_payload_bytes="${2:-}"
      shift 2
      ;;
    --library-readiness-json)
      library_readiness_json="${2:-}"
      shift 2
      ;;
    --library-readiness-graph)
      library_readiness_graph="${2:-}"
      shift 2
      ;;
    --library-readiness-search-projection)
      library_readiness_search_projection="${2:-}"
      shift 2
      ;;
    --query-runtime-preflight-json)
      query_runtime_preflight_json="${2:-}"
      shift 2
      ;;
    --query-runtime-probe-json)
      query_runtime_probe_json="${2:-}"
      shift 2
      ;;
    --query-runtime-database)
      query_runtime_database="${2:-}"
      shift 2
      ;;
    --require-integration-readiness)
      require_integration_readiness=true
      shift
      ;;
    --graph-route-query-json)
      graph_route_query_json="${2:-}"
      shift 2
      ;;
    --graph-route-database)
      graph_route_database="${2:-}"
      shift 2
      ;;
    --graph-route-parity-json)
      graph_route_parity_json="${2:-}"
      shift 2
      ;;
    --graph-route-readiness-json)
      graph_route_readiness_json="${2:-}"
      shift 2
      ;;
    --graph-route-evidence-json)
      graph_route_evidence_json="${2:-}"
      shift 2
      ;;
    --integration-submodule-path)
      integration_submodule_path="${2:-}"
      shift 2
      ;;
    --integration-submodule-commit)
      integration_submodule_commit="${2:-}"
      shift 2
      ;;
    --integration-legacy-data-retained)
      integration_legacy_data_retained=true
      shift
      ;;
    --integration-legacy-data-deleted)
      integration_legacy_data_deleted=true
      shift
      ;;
    --integration-coexistence-mode)
      integration_coexistence_mode="${2:-}"
      shift 2
      ;;
    --integration-content-store-path)
      integration_content_store_path="${2:-}"
      shift 2
      ;;
    --integration-content-store-present)
      integration_content_store_present=true
      shift
      ;;
    --integration-content-store-engine)
      integration_content_store_engine="${2:-}"
      shift 2
      ;;
    --integration-content-store-messages-available)
      integration_content_store_messages_available=true
      shift
      ;;
    --integration-content-store-source-chunks-available)
      integration_content_store_source_chunks_available=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$preflight_root" || -z "$nowledge_root" || -z "$wrapper_identity" || $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

if [[ "$preflight_root" == "/" || "$preflight_root" == "." ]]; then
  echo "refusing unsafe --preflight-root '$preflight_root'" >&2
  exit 2
fi

if [[ ! -d "$nowledge_root" ]]; then
  echo "--nowledge-root does not exist or is not a directory: $nowledge_root" >&2
  exit 2
fi

for evidence_path in \
  "$search_projection_evidence_json" \
  "$search_projection_probe_json" \
  "$search_projection_shadow_evidence_json" \
  "$search_projection_shadow_primary_probe_json" \
  "$search_projection_shadow_probe_json" \
  "$search_candidate_shadow_evidence_json" \
  "$search_candidate_shadow_probe_json" \
  "$bounded_read_evidence_json" \
  "$bounded_read_report_json" \
  "$library_readiness_json" \
  "$query_runtime_preflight_json" \
  "$query_runtime_probe_json" \
  "$graph_route_query_json" \
  "$graph_route_parity_json" \
  "$graph_route_evidence_json" \
  "$graph_route_readiness_json"; do
  if [[ -n "$evidence_path" && ! -f "$evidence_path" ]]; then
    echo "evidence JSON does not exist or is not a file: $evidence_path" >&2
    exit 2
  fi
done

if [[ -n "$bounded_read_evidence_json" && ( -n "$bounded_read_report_json" || -n "$bounded_read_cypher" ) ]]; then
  echo "--bounded-read-evidence-json cannot be combined with bounded read report or query generation" >&2
  exit 2
fi

if [[ -n "$bounded_read_report_json" && -n "$bounded_read_cypher" ]]; then
  echo "--bounded-read-report-json cannot be combined with --bounded-read-cypher" >&2
  exit 2
fi

if [[ -z "$bounded_read_cypher" && ( -n "$bounded_read_database" || -n "$bounded_read_params_json" || -n "$bounded_read_max_rows" || -n "$bounded_read_max_estimated_payload_bytes" ) ]]; then
  echo "bounded read query options require --bounded-read-cypher" >&2
  exit 2
fi

if [[ -n "$bounded_read_database" && ! -e "$bounded_read_database" ]]; then
  echo "--bounded-read-database does not exist: $bounded_read_database" >&2
  exit 2
fi

if [[ -n "$library_readiness_graph" && ! -e "$library_readiness_graph" ]]; then
  echo "--library-readiness-graph does not exist: $library_readiness_graph" >&2
  exit 2
fi

if [[ -n "$library_readiness_search_projection" && ! -e "$library_readiness_search_projection" ]]; then
  echo "--library-readiness-search-projection does not exist: $library_readiness_search_projection" >&2
  exit 2
fi

if [[ -n "$graph_route_database" && ! -e "$graph_route_database" ]]; then
  echo "--graph-route-database does not exist: $graph_route_database" >&2
  exit 2
fi

if [[ -n "$query_runtime_database" && ! -e "$query_runtime_database" ]]; then
  echo "--query-runtime-database does not exist: $query_runtime_database" >&2
  exit 2
fi

if [[ -n "$integration_content_store_path" && ! -f "$integration_content_store_path" ]]; then
  echo "--integration-content-store-path does not exist or is not a file" >&2
  exit 2
fi

if [[ -n "$query_runtime_preflight_json" && -n "$query_runtime_probe_json" ]]; then
  echo "--query-runtime-preflight-json cannot be combined with --query-runtime-probe-json" >&2
  exit 2
fi

if [[ -n "$graph_route_evidence_json" && -n "$graph_route_query_json" ]]; then
  echo "--graph-route-evidence-json cannot be combined with --graph-route-query-json" >&2
  exit 2
fi

if [[ -n "$search_projection_evidence_json" && -n "$search_projection_probe_json" ]]; then
  echo "--search-projection-evidence-json cannot be combined with --search-projection-probe-json" >&2
  exit 2
fi

if [[ -n "$search_projection_shadow_evidence_json" && ( -n "$search_projection_shadow_primary_probe_json" || -n "$search_projection_shadow_probe_json" ) ]]; then
  echo "--search-projection-shadow-evidence-json cannot be combined with search projection shadow probe inputs" >&2
  exit 2
fi

if [[ -n "$search_projection_shadow_primary_probe_json" && -z "$search_projection_shadow_probe_json" ]]; then
  echo "--search-projection-shadow-primary-probe-json requires --search-projection-shadow-probe-json" >&2
  exit 2
fi

if [[ -n "$search_projection_shadow_probe_json" && -z "$search_projection_shadow_primary_probe_json" ]]; then
  echo "--search-projection-shadow-probe-json requires --search-projection-shadow-primary-probe-json" >&2
  exit 2
fi

if [[ -n "$search_candidate_shadow_evidence_json" && -n "$search_candidate_shadow_probe_json" ]]; then
  echo "--search-candidate-shadow-evidence-json cannot be combined with --search-candidate-shadow-probe-json" >&2
  exit 2
fi

if [[ "$require_integration_readiness" == true ]]; then
  if [[ -z "$integration_content_store_path" ]]; then
    integration_content_store_path="$preflight_root/content.db"
  fi
  if [[ -f "$integration_content_store_path" ]]; then
    integration_content_store_present=true
    if [[ -z "$integration_content_store_engine" ]]; then
      integration_content_store_engine=sqlite
    fi
    if command -v sqlite3 >/dev/null 2>&1; then
      if [[ "$integration_content_store_messages_available" != true ]] \
        && [[ "$(sqlite3 -readonly "$integration_content_store_path" "SELECT 1 FROM sqlite_master WHERE type='table' AND name='thread_messages' LIMIT 1;" 2>/dev/null)" == "1" ]]; then
        integration_content_store_messages_available=true
      fi
      if [[ "$integration_content_store_source_chunks_available" != true ]] \
        && [[ "$(sqlite3 -readonly "$integration_content_store_path" "SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_chunks' LIMIT 1;" 2>/dev/null)" == "1" ]]; then
        integration_content_store_source_chunks_available=true
      fi
    fi
  fi
  if [[ -z "$graph_route_readiness_json" && -z "$graph_route_evidence_json" && -z "$graph_route_query_json" ]]; then
    echo "--require-integration-readiness requires --graph-route-readiness-json, --graph-route-evidence-json, or --graph-route-query-json" >&2
    exit 2
  fi
  if [[ -z "$graph_route_readiness_json" && -z "$graph_route_evidence_json" && -n "$graph_route_query_json" && -z "$graph_route_parity_json" ]]; then
    echo "--require-integration-readiness with --graph-route-query-json requires --graph-route-parity-json" >&2
    exit 2
  fi
  if [[ -z "$query_runtime_preflight_json" && -z "$query_runtime_probe_json" && -z "$graph_route_query_json" ]]; then
    echo "--require-integration-readiness requires --query-runtime-preflight-json, --query-runtime-probe-json, or --graph-route-query-json" >&2
    exit 2
  fi
  if [[ -z "$search_candidate_shadow_evidence_json" && -z "$search_candidate_shadow_probe_json" ]]; then
    echo "--require-integration-readiness requires --search-candidate-shadow-evidence-json or --search-candidate-shadow-probe-json" >&2
    exit 2
  fi
  if [[ -z "$search_projection_evidence_json" && -z "$search_projection_probe_json" && -z "$search_projection_shadow_probe_json" ]]; then
    echo "--require-integration-readiness requires --search-projection-evidence-json, --search-projection-probe-json, or --search-projection-shadow-probe-json" >&2
    exit 2
  fi
  if [[ -z "$search_projection_shadow_evidence_json" && ( -z "$search_projection_shadow_primary_probe_json" || -z "$search_projection_shadow_probe_json" ) ]]; then
    echo "--require-integration-readiness requires --search-projection-shadow-evidence-json or search projection shadow probe inputs" >&2
    exit 2
  fi
  if [[ -z "$integration_submodule_path" ]]; then
    echo "--require-integration-readiness requires --integration-submodule-path" >&2
    exit 2
  fi
  if [[ -z "$integration_submodule_commit" ]]; then
    if ! integration_submodule_commit="$(git -C "$integration_submodule_path" rev-parse --short HEAD 2>/dev/null)"; then
      echo "--require-integration-readiness could not derive --integration-submodule-commit from --integration-submodule-path" >&2
      exit 2
    fi
  fi
  if [[ -z "$integration_submodule_commit" ]]; then
    echo "--require-integration-readiness requires --integration-submodule-commit" >&2
    exit 2
  fi
  if [[ -z "$integration_coexistence_mode" ]]; then
    echo "--require-integration-readiness requires --integration-coexistence-mode" >&2
    exit 2
  fi
  if [[ -z "$integration_content_store_engine" ]]; then
    echo "--require-integration-readiness requires --integration-content-store-engine" >&2
    exit 2
  fi
fi

if [[ -z "$query_runtime_preflight_json" && -z "$query_runtime_probe_json" && -z "$graph_route_query_json" ]]; then
  echo "previous-wrapper preflight requires --query-runtime-preflight-json, --query-runtime-probe-json, or --graph-route-query-json" >&2
  exit 2
fi

mkdir -p "$preflight_root"

run_hawdb() {
  HAWDB_ENABLE_COMPATIBILITY_TOOLS=1 cargo run --quiet --bin hawdb -- "$@"
}

shadow_timeout_args=()
adapter_timeout_args=()
if [[ -n "$shadow_timeout_ms" ]]; then
  shadow_timeout_args=(--shadow-timeout-ms "$shadow_timeout_ms")
  adapter_timeout_args=(--command-timeout-ms "$shadow_timeout_ms")
fi

wrapper_command=("$@")
adapter_command=(
  cargo run --quiet --example nowledge_previous_wrapper_shadow_adapter --
  --wrapper-identity "$wrapper_identity"
  "${adapter_timeout_args[@]}"
  --persistent-command "${wrapper_command[@]}"
)

run_hawdb nowledge-fixture-contract nowledge-memory-core \
  > "$preflight_root/contract.json"

run_hawdb nowledge-fixture-contract-command-check \
  --require-full-contract \
  --wrapper-identity "$wrapper_identity" \
  --previous-wrapper-contract-evidence-output "$preflight_root/previous-wrapper-contract-evidence.json" \
  "$preflight_root/contract.json" \
  --persistent-command "${wrapper_command[@]}" \
  > "$preflight_root/contract-evidence.json"

run_hawdb external-shadow-adapter-smoke \
  --require-previous-wrapper \
  --shadow-trace "$preflight_root/adapter-shadow.jsonl" \
  "${shadow_timeout_args[@]}" \
  previous-wrapper \
  "${adapter_command[@]}" \
  > "$preflight_root/adapter-smoke.json"

TMPDIR="$preflight_root" run_hawdb > "$preflight_root/hawdb-demo.out"
hawdb_preflight_db="$preflight_root/hawdb-demo"

run_hawdb storage-recovery-report \
  --max-wal-replay-entries 100 \
  --require-durable \
  --require-checkpoint-boundary \
  --require-bounded-wal-replay \
  --require-clean-tail \
  "$hawdb_preflight_db" \
  > "$preflight_root/storage-recovery.json"

run_hawdb nowledge-storage-recovery-evidence \
  --require-ready \
  "$preflight_root/storage-recovery.json" \
  > "$preflight_root/storage-recovery-evidence.json"

run_hawdb background-maintenance-report \
  --require-cutover-ready \
  "$hawdb_preflight_db" \
  > "$preflight_root/background-maintenance.json"

run_hawdb nowledge-background-maintenance-evidence \
  --require-ready \
  "$preflight_root/background-maintenance.json" \
  > "$preflight_root/background-maintenance-evidence.json"

run_hawdb nowledge-cypher-migration-gate \
  --require-ready \
  --require-cutover-evidence \
  --shadow-ready \
  --shadow-trace "$preflight_root/migration-shadow.jsonl" \
  "${shadow_timeout_args[@]}" \
  --require-storage-recovery-evidence \
  --storage-recovery-report-json "$preflight_root/storage-recovery.json" \
  --require-background-maintenance-evidence \
  --background-maintenance-report-json "$preflight_root/background-maintenance.json" \
  --previous-wrapper-contract-evidence-json "$preflight_root/previous-wrapper-contract-evidence.json" \
  "$nowledge_root" \
  previous-wrapper \
  "${adapter_command[@]}" \
  > "$preflight_root/migration-gate.json"

run_hawdb nowledge-query-family-evidence \
  --require-ready \
  "$preflight_root/migration-gate.json" \
  > "$preflight_root/query-family-evidence.json"

if [[ -z "$graph_route_evidence_json" && -n "$graph_route_query_json" ]]; then
  graph_route_evidence_args=()
  if [[ -n "$graph_route_parity_json" ]]; then
    graph_route_evidence_args+=(
      --route-parity-json "$graph_route_parity_json"
    )
  fi
  run_hawdb nowledge-graph-route-evidence \
    "${graph_route_evidence_args[@]}" \
    "${graph_route_database:-$hawdb_preflight_db}" \
    "$graph_route_query_json" \
    > "$preflight_root/graph-route-evidence.json"
  graph_route_evidence_json="$preflight_root/graph-route-evidence.json"
fi

if [[ -z "$graph_route_readiness_json" && -n "$graph_route_evidence_json" ]]; then
  run_hawdb nowledge-graph-route-readiness \
    --require-ready \
    "$graph_route_evidence_json" \
    > "$preflight_root/graph-route-readiness.json"
  graph_route_readiness_json="$preflight_root/graph-route-readiness.json"
fi

if [[ -z "$bounded_read_evidence_json" ]]; then
  if [[ -n "$bounded_read_report_json" ]]; then
    bounded_read_evidence_args=(--require-ready)
    if [[ -n "$graph_route_readiness_json" ]]; then
      bounded_read_evidence_args+=(
        --graph-route-readiness-json "$graph_route_readiness_json"
      )
    fi
    run_hawdb nowledge-bounded-read-evidence \
      "${bounded_read_evidence_args[@]}" \
      "$bounded_read_report_json" \
      > "$preflight_root/bounded-read-evidence.json"
    bounded_read_evidence_json="$preflight_root/bounded-read-evidence.json"
  elif [[ -n "$bounded_read_cypher" ]]; then
    bounded_read_report_args=()
    if [[ -n "$bounded_read_params_json" ]]; then
      bounded_read_report_args+=(--params-json "$bounded_read_params_json")
    fi
    if [[ -n "$bounded_read_max_rows" ]]; then
      bounded_read_report_args+=(--max-rows "$bounded_read_max_rows")
    fi
    if [[ -n "$bounded_read_max_estimated_payload_bytes" ]]; then
      bounded_read_report_args+=(
        --max-estimated-payload-bytes "$bounded_read_max_estimated_payload_bytes"
      )
    fi
    run_hawdb nowledge-bounded-read-report \
      "${bounded_read_report_args[@]}" \
      "${bounded_read_database:-$hawdb_preflight_db}" \
      "$bounded_read_cypher" \
      > "$preflight_root/bounded-read-report.json"
    bounded_read_evidence_args=(--require-ready)
    if [[ -n "$graph_route_readiness_json" ]]; then
      bounded_read_evidence_args+=(
        --graph-route-readiness-json "$graph_route_readiness_json"
      )
    fi
    run_hawdb nowledge-bounded-read-evidence \
      "${bounded_read_evidence_args[@]}" \
      "$preflight_root/bounded-read-report.json" \
      > "$preflight_root/bounded-read-evidence.json"
    bounded_read_evidence_json="$preflight_root/bounded-read-evidence.json"
  fi
fi

if [[ -z "$search_projection_evidence_json" ]]; then
  if [[ -n "$search_projection_probe_json" ]]; then
    run_hawdb nowledge-search-projection-evidence \
      --require-ready \
      "$search_projection_probe_json" \
      > "$preflight_root/search-projection-evidence.json"
    search_projection_evidence_json="$preflight_root/search-projection-evidence.json"
  elif [[ -n "$search_projection_shadow_probe_json" ]]; then
    run_hawdb nowledge-search-projection-evidence \
      --require-ready \
      "$search_projection_shadow_probe_json" \
      > "$preflight_root/search-projection-evidence.json"
    search_projection_evidence_json="$preflight_root/search-projection-evidence.json"
  fi
fi

if [[ -z "$search_projection_shadow_evidence_json" && -n "$search_projection_shadow_primary_probe_json" ]]; then
  run_hawdb nowledge-search-projection-shadow-evidence \
    --require-ready \
    --primary-probe-json "$search_projection_shadow_primary_probe_json" \
    --shadow-probe-json "$search_projection_shadow_probe_json" \
    > "$preflight_root/search-projection-shadow-evidence.json"
  search_projection_shadow_evidence_json="$preflight_root/search-projection-shadow-evidence.json"
fi

if [[ -z "$search_candidate_shadow_evidence_json" && -n "$search_candidate_shadow_probe_json" ]]; then
  run_hawdb nowledge-search-candidate-shadow-evidence \
    --require-ready \
    "$search_candidate_shadow_probe_json" \
    > "$preflight_root/search-candidate-shadow-evidence.json"
  search_candidate_shadow_evidence_json="$preflight_root/search-candidate-shadow-evidence.json"
fi

if [[ -n "$query_runtime_preflight_json" ]]; then
  if [[ "$query_runtime_preflight_json" != "$preflight_root/query-runtime-preflight.json" ]]; then
    cp "$query_runtime_preflight_json" "$preflight_root/query-runtime-preflight.json"
  fi
else
  run_hawdb nowledge-query-runtime-preflight \
    --require-ready \
    --probe-json "${query_runtime_probe_json:-$graph_route_query_json}" \
    "${query_runtime_database:-$hawdb_preflight_db}" \
    > "$preflight_root/query-runtime-preflight.json"
fi

replacement_summary_evidence_args=(
  --query-family-evidence-json "$preflight_root/query-family-evidence.json"
  --query-runtime-preflight-json "$preflight_root/query-runtime-preflight.json"
)
if [[ -n "$search_projection_evidence_json" ]]; then
  replacement_summary_evidence_args+=(
    --search-projection-evidence-json "$search_projection_evidence_json"
  )
fi
if [[ -n "$search_projection_shadow_evidence_json" ]]; then
  replacement_summary_evidence_args+=(
    --search-projection-shadow-evidence-json "$search_projection_shadow_evidence_json"
  )
fi
if [[ -n "$search_candidate_shadow_evidence_json" ]]; then
  replacement_summary_evidence_args+=(
    --search-candidate-shadow-evidence-json "$search_candidate_shadow_evidence_json"
  )
fi
if [[ -n "$bounded_read_evidence_json" ]]; then
  replacement_summary_evidence_args+=(
    --bounded-read-evidence-json "$bounded_read_evidence_json"
  )
fi

run_hawdb nowledge-replacement-summary \
  --require-production-ready \
  "${replacement_summary_evidence_args[@]}" \
  "$preflight_root/migration-gate.json" \
  > "$preflight_root/replacement-summary.json"

if [[ -n "$library_readiness_json" ]]; then
  if [[ "$library_readiness_json" != "$preflight_root/library-readiness.json" ]]; then
    cp "$library_readiness_json" "$preflight_root/library-readiness.json"
  fi
else
  if [[ -z "$bounded_read_evidence_json" ]]; then
    echo "library readiness generation requires bounded read evidence; provide --bounded-read-evidence-json, --bounded-read-report-json, or --bounded-read-cypher" >&2
    exit 2
  fi
  if [[ -z "$search_projection_evidence_json" ]]; then
    echo "library readiness generation requires --search-projection-evidence-json" >&2
    exit 2
  fi
  if [[ -z "$search_projection_shadow_evidence_json" ]]; then
    echo "library readiness generation requires --search-projection-shadow-evidence-json" >&2
    exit 2
  fi
  library_readiness_args=(
    --bounded-read-evidence-json "$bounded_read_evidence_json"
    --query-family-evidence-json "$preflight_root/query-family-evidence.json"
    --search-projection-evidence-json "$search_projection_evidence_json"
    --search-projection-shadow-evidence-json "$search_projection_shadow_evidence_json"
  )
  if [[ -n "$library_readiness_search_projection" ]]; then
    library_readiness_args+=(
      --search-projection "$library_readiness_search_projection"
    )
  fi
  run_hawdb nowledge-mem-library-readiness \
    --require-ready \
    "${library_readiness_args[@]}" \
    "${library_readiness_graph:-$hawdb_preflight_db}" \
    > "$preflight_root/library-readiness.json"
fi

run_hawdb nowledge-previous-wrapper-preflight-check \
  --require-ready \
  --wrapper-identity "$wrapper_identity" \
  --bundle-dir "$preflight_root" \
  > "$preflight_root/preflight-check.json"

if [[ "$require_integration_readiness" == true ]]; then
  integration_bundle_args=(
    --require-ready
    --submodule-path "$integration_submodule_path"
    --submodule-commit "$integration_submodule_commit"
    --coexistence-mode "$integration_coexistence_mode"
    --content-store-engine "$integration_content_store_engine"
    --previous-wrapper-preflight-json "$preflight_root/preflight-check.json"
    --replacement-summary-json "$preflight_root/replacement-summary.json"
    --bounded-read-evidence-json "$bounded_read_evidence_json"
    --graph-route-readiness-json "$graph_route_readiness_json"
    --query-runtime-preflight-json "$preflight_root/query-runtime-preflight.json"
    --search-candidate-shadow-evidence-json "$search_candidate_shadow_evidence_json"
    --library-readiness-json "$preflight_root/library-readiness.json"
  )
  if [[ "$integration_legacy_data_retained" == true ]]; then
    integration_bundle_args+=(--legacy-data-retained)
  fi
  if [[ "$integration_legacy_data_deleted" == true ]]; then
    integration_bundle_args+=(--legacy-data-deleted)
  fi
  if [[ "$integration_content_store_present" == true ]]; then
    integration_bundle_args+=(--content-store-present)
  fi
  if [[ "$integration_content_store_messages_available" == true ]]; then
    integration_bundle_args+=(--content-store-messages-available)
  fi
  if [[ "$integration_content_store_source_chunks_available" == true ]]; then
    integration_bundle_args+=(--content-store-source-chunks-available)
  fi
  run_hawdb nowledge-mem-integration-bundle \
    "${integration_bundle_args[@]}" \
    > "$preflight_root/integration-bundle.json"
fi

if [[ "$require_integration_readiness" == true ]]; then
  cat "$preflight_root/integration-bundle.json"
else
  cat "$preflight_root/preflight-check.json"
fi
