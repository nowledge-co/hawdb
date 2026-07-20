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
  [--legacy-search-index <dir>] \
  [--search-active-model <model>] \
  [--search-active-dimension <dimension>] \
  [--search-source-graph-commit-epoch <epoch>] \
  [--search-projection-allow-unbounded | --search-projection-max-rows <n>] \
  [--search-projection-evidence-json <path>] \
  [--search-projection-shadow-evidence-json <path>] \
  [--bounded-read-evidence-json <path>] \
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
legacy_search_index=
search_active_model=
search_active_dimension=
search_source_graph_commit_epoch=
search_projection_allow_unbounded=
search_projection_max_rows=
search_projection_evidence_json=
search_projection_shadow_evidence_json=
bounded_read_evidence_json=

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
    --legacy-search-index)
      legacy_search_index="${2:-}"
      shift 2
      ;;
    --search-active-model)
      search_active_model="${2:-}"
      shift 2
      ;;
    --search-active-dimension)
      search_active_dimension="${2:-}"
      shift 2
      ;;
    --search-source-graph-commit-epoch)
      search_source_graph_commit_epoch="${2:-}"
      shift 2
      ;;
    --search-projection-allow-unbounded)
      search_projection_allow_unbounded=1
      shift
      ;;
    --search-projection-max-rows)
      search_projection_max_rows="${2:-}"
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

if [[ -n "$legacy_search_index" ]]; then
  if [[ -z "$search_active_model" || -z "$search_active_dimension" || -z "$search_source_graph_commit_epoch" ]]; then
    echo "--legacy-search-index requires --search-active-model, --search-active-dimension, and --search-source-graph-commit-epoch" >&2
    exit 2
  fi
  if [[ -n "$search_projection_allow_unbounded" && -n "$search_projection_max_rows" ]]; then
    echo "use only one of --search-projection-allow-unbounded or --search-projection-max-rows" >&2
    exit 2
  fi
  if [[ -z "$search_projection_allow_unbounded" && -z "$search_projection_max_rows" ]]; then
    echo "--legacy-search-index requires --search-projection-allow-unbounded or --search-projection-max-rows" >&2
    exit 2
  fi
  if [[ ! -d "$legacy_search_index" ]]; then
    echo "--legacy-search-index does not exist or is not a directory: $legacy_search_index" >&2
    exit 2
  fi
  if [[ ! -f "$nowledge_root/nmem-rs/Cargo.toml" ]]; then
    echo "--nowledge-root must contain nmem-rs/Cargo.toml to generate LanceDB search projection evidence" >&2
    exit 2
  fi
fi

mkdir -p "$preflight_root"

run_skein() {
  cargo run --quiet --bin skein -- "$@"
}

run_nmem_search() {
  cargo run --quiet \
    --manifest-path "$nowledge_root/nmem-rs/Cargo.toml" \
    -p nmem-search \
    --bin "$1" \
    -- "${@:2}"
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
  ${adapter_timeout_args[@]+"${adapter_timeout_args[@]}"}
  --persistent-command "${wrapper_command[@]}"
)

run_skein nowledge-fixture-contract nowledge-memory-core \
  > "$preflight_root/contract.json"

run_skein nowledge-fixture-contract-command-check \
  --require-full-contract \
  --wrapper-identity "$wrapper_identity" \
  "$preflight_root/contract.json" \
  --persistent-command "${wrapper_command[@]}" \
  > "$preflight_root/contract-evidence.json"

run_skein external-shadow-adapter-smoke \
  --require-previous-wrapper \
  --shadow-trace "$preflight_root/adapter-shadow.jsonl" \
  ${shadow_timeout_args[@]+"${shadow_timeout_args[@]}"} \
  previous-wrapper \
  "${adapter_command[@]}" \
  > "$preflight_root/adapter-smoke.json"

TMPDIR="$preflight_root" run_skein > "$preflight_root/skein-demo.out"
skein_preflight_db="$preflight_root/skein-demo"

search_projection_index="$preflight_root/search-projection-index"
search_projection_delta_json="$preflight_root/search-projection-delta.json"
search_projection_probe_json="$preflight_root/search-projection-probe.json"
python3 - "$search_projection_delta_json" <<'PY'
import json
import sys

rows = [
    ("Memory", "mem_1", True),
    ("Message", "msg_1", False),
    ("Community", "community_1", True),
    ("Entity", "entity_1", True),
    ("Source", "source_1", True),
    ("SourceChunk", "chunk_1", True),
]
payload = {
    "protocol": "nmem-lancedb-skein-search-projection-delta",
    "delta": {
        "upserts": [
            {
                "kind": kind,
                "external_id": external_id,
                "title": f"{external_id} title",
                "body": f"{external_id} body",
                "embedding": [1.0, 0.0] if include_embedding else None,
                "source_id": "source_1",
                "metadata": {
                    "space_id": "default",
                },
            }
            for kind, external_id, include_embedding in rows
        ],
        "deletes": [],
        "max_operations": 6,
        "source_graph_commit_epoch": 2,
    },
}
with open(sys.argv[1], "w", encoding="utf-8") as file:
    json.dump(payload, file, indent=2)
    file.write("\n")
PY
run_skein skein-search-projection-delta-probe \
  --active-model bge-m3 \
  --active-dimension 2 \
  "$search_projection_index" \
  "$search_projection_delta_json" \
  > "$search_projection_probe_json"

if [[ -z "$search_projection_shadow_evidence_json" && -n "$legacy_search_index" ]]; then
  lance_snapshot_versions_json="$preflight_root/lancedb-search-projection-table-versions.json"
  lancedb_primary_probe_json="$preflight_root/lancedb-search-projection-primary-probe.json"
  lancedb_primary_delta_json="$preflight_root/lancedb-search-projection-delta.json"
  skein_shadow_index="$preflight_root/search-projection-shadow-index"
  skein_shadow_probe_json="$preflight_root/search-projection-shadow-probe.json"
  search_projection_shadow_evidence_json="$preflight_root/search-projection-shadow-evidence.json"

  run_nmem_search nmem-search-projection-probe \
    --active-model "$search_active_model" \
    --active-dimension "$search_active_dimension" \
    --source-graph-commit-epoch "$search_source_graph_commit_epoch" \
    --snapshot-versions-path "$lance_snapshot_versions_json" \
    "$legacy_search_index" \
    > "$lancedb_primary_probe_json"

  delta_bound_args=()
  if [[ -n "$search_projection_allow_unbounded" ]]; then
    delta_bound_args+=(--allow-unbounded)
  else
    delta_bound_args+=(--max-rows "$search_projection_max_rows")
  fi
  run_nmem_search nmem-search-projection-delta \
    "${delta_bound_args[@]}" \
    --include-embeddings \
    --source-graph-commit-epoch "$search_source_graph_commit_epoch" \
    --snapshot-versions-path "$lance_snapshot_versions_json" \
    "$legacy_search_index" \
    > "$lancedb_primary_delta_json"

  run_skein skein-search-projection-delta-probe \
    --active-model "$search_active_model" \
    --active-dimension "$search_active_dimension" \
    "$skein_shadow_index" \
    "$lancedb_primary_delta_json" \
    > "$skein_shadow_probe_json"

  run_skein nowledge-search-projection-shadow-evidence \
    --require-ready \
    --primary-probe-json "$lancedb_primary_probe_json" \
    --shadow-probe-json "$skein_shadow_probe_json" \
    > "$search_projection_shadow_evidence_json"

  if [[ -z "$search_projection_evidence_json" ]]; then
    search_projection_evidence_json="$preflight_root/search-projection-evidence.json"
    run_skein nowledge-search-projection-evidence \
      --require-ready \
      "$lancedb_primary_probe_json" \
      > "$search_projection_evidence_json"
  fi
fi

if [[ -z "$search_projection_evidence_json" ]]; then
  search_projection_evidence_json="$preflight_root/search-projection-evidence.json"
  run_skein nowledge-search-projection-evidence \
    --require-ready \
    "$search_projection_probe_json" \
    > "$search_projection_evidence_json"
fi

if [[ -z "$bounded_read_evidence_json" ]]; then
  bounded_read_evidence_json="$preflight_root/bounded-read-evidence.json"
  run_skein nowledge-mem-bounded-read-evidence \
    --require-ready \
    "$skein_preflight_db" \
    "MATCH (m:Memory) RETURN m.title AS title" \
    > "$bounded_read_evidence_json"
fi

run_skein storage-recovery-report \
  --max-wal-replay-entries 100 \
  --require-durable \
  --require-checkpoint-boundary \
  --require-bounded-wal-replay \
  --require-clean-tail \
  "$skein_preflight_db" \
  > "$preflight_root/storage-recovery.json"

run_skein background-maintenance-report \
  --require-cutover-ready \
  --search-projection-index "$search_projection_index" \
  "$skein_preflight_db" \
  > "$preflight_root/background-maintenance.json"

python3 - "$preflight_root/contract-evidence.json" \
  "$preflight_root/previous-wrapper-contract-evidence.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as file:
    report = json.load(file)

with open(sys.argv[2], "w", encoding="utf-8") as file:
    json.dump(report["previous_wrapper_contract_evidence"], file, indent=2)
    file.write("\n")
PY

search_projection_evidence_args=()
if [[ -n "$search_projection_evidence_json" ]]; then
  search_projection_evidence_args+=(--search-projection-evidence-json "$search_projection_evidence_json")
fi
if [[ -n "$search_projection_shadow_evidence_json" ]]; then
  search_projection_evidence_args+=(--search-projection-shadow-evidence-json "$search_projection_shadow_evidence_json")
fi
if [[ -n "$bounded_read_evidence_json" ]]; then
  search_projection_evidence_args+=(--bounded-read-evidence-json "$bounded_read_evidence_json")
fi

run_skein nowledge-cypher-migration-gate \
  --require-ready \
  --require-cutover-evidence \
  --shadow-ready \
  --shadow-trace "$preflight_root/migration-shadow.jsonl" \
  ${shadow_timeout_args[@]+"${shadow_timeout_args[@]}"} \
  --require-storage-recovery-evidence \
  --storage-recovery-report-json "$preflight_root/storage-recovery.json" \
  --require-background-maintenance-evidence \
  --background-maintenance-report-json "$preflight_root/background-maintenance.json" \
  --previous-wrapper-contract-evidence-json "$preflight_root/previous-wrapper-contract-evidence.json" \
  ${search_projection_evidence_args[@]+"${search_projection_evidence_args[@]}"} \
  "$nowledge_root" \
  previous-wrapper \
  "${adapter_command[@]}" \
  > "$preflight_root/migration-gate.json"

python3 - "$preflight_root/contract-evidence.json" \
  "$preflight_root/migration-gate.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as file:
    contract = json.load(file)
with open(sys.argv[2], encoding="utf-8") as file:
    bundle = json.load(file)

bundle["contract_evidence"] = {
    key: contract.get(key)
    for key in [
        "required_contract_ready",
        "full_contract_checked",
        "full_contract_ready",
        "selected_checks",
    ]
}
bundle["contract_evidence"]["check_count"] = contract.get("check_count", contract.get("total_checks"))

with open(sys.argv[2], "w", encoding="utf-8") as file:
    json.dump(bundle, file, indent=2)
    file.write("\n")
PY

run_skein nowledge-replacement-summary \
  --require-production-ready \
  "$preflight_root/migration-gate.json" \
  > "$preflight_root/replacement-summary.json"

run_skein nowledge-previous-wrapper-preflight-check \
  --require-ready \
  --wrapper-identity "$wrapper_identity" \
  --bundle-dir "$preflight_root" \
  > "$preflight_root/preflight-check.json"

cat "$preflight_root/preflight-check.json"
