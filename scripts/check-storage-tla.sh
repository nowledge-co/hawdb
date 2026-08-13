#!/usr/bin/env bash
set -euo pipefail

readonly tla_version="1.7.4"
readonly tla_sha256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repository_root
readonly tla_work_root="${TLA_WORK_ROOT:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/skein-tla}"
readonly downloaded_jar="$tla_work_root/tla2tools-$tla_version.jar"
readonly specifications=(
  SkeinStorageDurability
  SkeinWalGroupCommit
  SkeinWalDoctor
  SkeinGenerationReclamation
  SkeinConcurrentSnapshots
  SkeinTransactionConcurrency
  SkeinSourceSegmentPublication
  SkeinCrdtReplication
  SkeinGossipDelivery
  SkeinSystemSchemaUpgrade
  SkeinPropertyIndexPruning
  SkeinProjectionDurability
  SkeinCompactionVisibility
  SkeinColumnGroupManifest
  SkeinRuntimeAdmission
  SkeinColumnarShadowIntegration
)

manifest_json() {
  local revision="$1"
  if [[ ! "$revision" =~ ^[0-9a-fA-F]{7,64}$ ]]; then
    printf 'TLA+ source revision must be a hexadecimal commit identity\n' >&2
    return 1
  fi
  local separator=""
  local models=""
  local specification
  for specification in "${specifications[@]}"; do
    models+="$separator\"$specification\""
    separator=","
  done
  printf '{"schema_version":1,"source_revision":"%s","tla_tools_version":"%s","tla_tools_sha256":"%s","models":[%s]}' \
    "$revision" "$tla_version" "$tla_sha256" "$models"
}

verify_results() {
  local results_dir="$1"
  local source_revision="$2"
  local manifest="$results_dir/manifest.json"
  local expected_manifest
  expected_manifest="$(manifest_json "$source_revision")"
  if [[ ! -f "$manifest" ]] || [[ "$(tr -d '\n' < "$manifest")" != "$expected_manifest" ]]; then
    printf 'TLA+ result manifest does not match the expected revision and model set\n' >&2
    return 1
  fi
  [[ -s "$results_dir/java-version.txt" ]] || {
    printf 'TLA+ Java version evidence is missing\n' >&2
    return 1
  }

  local specification
  for specification in "${specifications[@]}"; do
    local result="$results_dir/$specification.txt"
    [[ -s "$result" ]] || {
      printf 'TLA+ result is missing for %s\n' "$specification" >&2
      return 1
    }
    grep -Fq 'Model checking completed. No error has been found.' "$result" || {
      printf 'TLA+ success marker is missing for %s\n' "$specification" >&2
      return 1
    }
    cmp "$repository_root/docs/tla/$specification.tla" \
      "$results_dir/models/$specification.tla"
    cmp "$repository_root/docs/tla/$specification.cfg" \
      "$results_dir/models/$specification.cfg"
  done
  if find "$results_dir" -name 'tla2tools*.jar' -print -quit | grep -q .; then
    printf 'TLA+ tool binaries must not be retained in release evidence\n' >&2
    return 1
  fi
}

if [[ "${1:-}" == "--manifest-json" ]]; then
  [[ "$#" -eq 2 ]] || {
    printf 'usage: %s --manifest-json SOURCE_REVISION\n' "$0" >&2
    exit 2
  }
  manifest_json "$2"
  printf '\n'
  exit
fi

if [[ "${1:-}" == "--verify-results" ]]; then
  [[ "$#" -eq 3 ]] || {
    printf 'usage: %s --verify-results RESULTS_DIR SOURCE_REVISION\n' "$0" >&2
    exit 2
  }
  verify_results "$2" "$3"
  exit
fi

if [[ "$#" -ne 0 ]]; then
  printf 'usage: %s [--manifest-json SOURCE_REVISION | --verify-results RESULTS_DIR SOURCE_REVISION]\n' "$0" >&2
  exit 2
fi

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

resolve_tla_jar() {
  if [[ -n "${TLA2TOOLS_JAR:-}" ]]; then
    printf '%s\n' "$TLA2TOOLS_JAR"
    return
  fi

  mkdir -p "$tla_work_root"
  if [[ ! -f "$downloaded_jar" ]] ||
    [[ "$(sha256_file "$downloaded_jar")" != "$tla_sha256" ]]; then
    local temporary_jar="$downloaded_jar.tmp"
    curl --fail --location --retry 3 --silent --show-error \
      "https://github.com/tlaplus/tlaplus/releases/download/v$tla_version/tla2tools.jar" \
      --output "$temporary_jar"
    if [[ "$(sha256_file "$temporary_jar")" != "$tla_sha256" ]]; then
      printf 'downloaded tla2tools.jar checksum mismatch\n' >&2
      return 1
    fi
    mv "$temporary_jar" "$downloaded_jar"
  fi
  printf '%s\n' "$downloaded_jar"
}

tla_jar="$(resolve_tla_jar)"
readonly tla_jar
readonly tla_java="${TLA_JAVA:-java}"
readonly tla_results_dir="${TLA_RESULTS_DIR:-}"
evidence_source_revision=""
if [[ -n "$tla_results_dir" ]]; then
  evidence_source_revision="${TLA_SOURCE_REVISION:-$(git -C "$repository_root" rev-parse HEAD)}"
  manifest_json "$evidence_source_revision" > /dev/null
  mkdir -p "$tla_results_dir"
  if find "$tla_results_dir" -mindepth 1 -print -quit | grep -q .; then
    printf 'TLA_RESULTS_DIR must be empty before model checking\n' >&2
    exit 1
  fi
  mkdir "$tla_results_dir/models"
  "$tla_java" -version > "$tla_results_dir/java-version.txt" 2>&1
fi
readonly evidence_source_revision

for specification in "${specifications[@]}"; do
  model_state_dir="$tla_work_root/states/$specification"
  mkdir -p "$model_state_dir"
  tla_command=(
    "$tla_java" -XX:+UseParallelGC -jar "$tla_jar"
    -cleanup
    -metadir "$model_state_dir"
    -workers auto
    -config "$repository_root/docs/tla/$specification.cfg"
    "$repository_root/docs/tla/$specification.tla"
  )
  if [[ -n "$tla_results_dir" ]]; then
    cp "$repository_root/docs/tla/$specification.tla" "$tla_results_dir/models/"
    cp "$repository_root/docs/tla/$specification.cfg" "$tla_results_dir/models/"
    "${tla_command[@]}" 2>&1 \
      | tee "$tla_results_dir/$specification.txt"
  else
    "${tla_command[@]}"
  fi
done

if [[ -n "$tla_results_dir" ]]; then
  manifest_json "$evidence_source_revision" > "$tla_results_dir/manifest.json"
  printf '\n' >> "$tla_results_dir/manifest.json"
fi
