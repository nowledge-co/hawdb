#!/usr/bin/env bash
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
  [--search-projection-shadow-evidence-json <path>] \
  [--bounded-read-evidence-json <path>] \
  [--bounded-read-report-json <path>] \
  [--bounded-read-database <path>] \
  [--bounded-read-cypher <cypher>] \
  [--bounded-read-params-json <json-object>] \
  [--bounded-read-max-rows <n>] \
  [--bounded-read-max-estimated-payload-bytes <n>] \
  [--query-runtime-preflight-json <path>] \
  [--query-runtime-probe-json <path>] \
  [--query-runtime-database <path>] \
  -- <wrapper-command> [args...]

Runs the Skein-side Nowledge previous-wrapper production preflight bundle.
The wrapper command must own all Kuzu/Ladybug dependencies and must read from
an isolated Nowledge data copy, not from the live application database.
EOF
}

preflight_root=
nowledge_root=
wrapper_identity=
shadow_timeout_ms=
search_projection_evidence_json=
search_projection_shadow_evidence_json=
bounded_read_evidence_json=
bounded_read_report_json=
bounded_read_database=
bounded_read_cypher=
bounded_read_params_json=
bounded_read_max_rows=
bounded_read_max_estimated_payload_bytes=
query_runtime_preflight_json=
query_runtime_probe_json=
query_runtime_database=

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
    --search-projection-shadow-evidence-json)
      search_projection_shadow_evidence_json="${2:-}"
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
  "$search_projection_shadow_evidence_json" \
  "$bounded_read_evidence_json" \
  "$bounded_read_report_json" \
  "$query_runtime_preflight_json" \
  "$query_runtime_probe_json"; do
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

if [[ -n "$query_runtime_database" && ! -e "$query_runtime_database" ]]; then
  echo "--query-runtime-database does not exist: $query_runtime_database" >&2
  exit 2
fi

if [[ -n "$query_runtime_preflight_json" && -n "$query_runtime_probe_json" ]]; then
  echo "--query-runtime-preflight-json cannot be combined with --query-runtime-probe-json" >&2
  exit 2
fi

if [[ -z "$query_runtime_preflight_json" && -z "$query_runtime_probe_json" ]]; then
  echo "previous-wrapper preflight requires --query-runtime-preflight-json or --query-runtime-probe-json" >&2
  exit 2
fi

mkdir -p "$preflight_root"

run_skein() {
  cargo run --quiet --bin skein -- "$@"
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

run_skein nowledge-fixture-contract nowledge-memory-core \
  > "$preflight_root/contract.json"

run_skein nowledge-fixture-contract-command-check \
  --require-full-contract \
  --wrapper-identity "$wrapper_identity" \
  --previous-wrapper-contract-evidence-output "$preflight_root/previous-wrapper-contract-evidence.json" \
  "$preflight_root/contract.json" \
  --persistent-command "${wrapper_command[@]}" \
  > "$preflight_root/contract-evidence.json"

run_skein external-shadow-adapter-smoke \
  --require-previous-wrapper \
  --shadow-trace "$preflight_root/adapter-shadow.jsonl" \
  "${shadow_timeout_args[@]}" \
  previous-wrapper \
  "${adapter_command[@]}" \
  > "$preflight_root/adapter-smoke.json"

TMPDIR="$preflight_root" run_skein > "$preflight_root/skein-demo.out"
skein_preflight_db="$preflight_root/skein-demo"

run_skein storage-recovery-report \
  --max-wal-replay-entries 100 \
  --require-durable \
  --require-checkpoint-boundary \
  --require-bounded-wal-replay \
  --require-clean-tail \
  "$skein_preflight_db" \
  > "$preflight_root/storage-recovery.json"

run_skein nowledge-storage-recovery-evidence \
  --require-ready \
  "$preflight_root/storage-recovery.json" \
  > "$preflight_root/storage-recovery-evidence.json"

run_skein background-maintenance-report \
  --require-cutover-ready \
  "$skein_preflight_db" \
  > "$preflight_root/background-maintenance.json"

run_skein nowledge-background-maintenance-evidence \
  --require-ready \
  "$preflight_root/background-maintenance.json" \
  > "$preflight_root/background-maintenance-evidence.json"

run_skein nowledge-cypher-migration-gate \
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

run_skein nowledge-query-family-evidence \
  --require-ready \
  "$preflight_root/migration-gate.json" \
  > "$preflight_root/query-family-evidence.json"

if [[ -z "$bounded_read_evidence_json" ]]; then
  if [[ -n "$bounded_read_report_json" ]]; then
    run_skein nowledge-bounded-read-evidence \
      --require-ready \
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
    run_skein nowledge-bounded-read-report \
      "${bounded_read_report_args[@]}" \
      "${bounded_read_database:-$skein_preflight_db}" \
      "$bounded_read_cypher" \
      > "$preflight_root/bounded-read-report.json"
    run_skein nowledge-bounded-read-evidence \
      --require-ready \
      "$preflight_root/bounded-read-report.json" \
      > "$preflight_root/bounded-read-evidence.json"
    bounded_read_evidence_json="$preflight_root/bounded-read-evidence.json"
  fi
fi

replacement_summary_evidence_args=(
  --query-family-evidence-json "$preflight_root/query-family-evidence.json"
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
if [[ -n "$bounded_read_evidence_json" ]]; then
  replacement_summary_evidence_args+=(
    --bounded-read-evidence-json "$bounded_read_evidence_json"
  )
fi

run_skein nowledge-replacement-summary \
  --require-production-ready \
  "${replacement_summary_evidence_args[@]}" \
  "$preflight_root/migration-gate.json" \
  > "$preflight_root/replacement-summary.json"

if [[ -n "$query_runtime_preflight_json" ]]; then
  if [[ "$query_runtime_preflight_json" != "$preflight_root/query-runtime-preflight.json" ]]; then
    cp "$query_runtime_preflight_json" "$preflight_root/query-runtime-preflight.json"
  fi
else
  run_skein nowledge-query-runtime-preflight \
    --require-ready \
    --probe-json "$query_runtime_probe_json" \
    "${query_runtime_database:-$skein_preflight_db}" \
    > "$preflight_root/query-runtime-preflight.json"
fi

run_skein nowledge-previous-wrapper-preflight-check \
  --require-ready \
  --wrapper-identity "$wrapper_identity" \
  --bundle-dir "$preflight_root" \
  > "$preflight_root/preflight-check.json"

cat "$preflight_root/preflight-check.json"
