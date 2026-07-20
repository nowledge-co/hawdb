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
