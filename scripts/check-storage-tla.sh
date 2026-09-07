#!/usr/bin/env bash
set -euo pipefail

readonly tla_version="1.7.4"
readonly tla_sha256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repository_root
readonly storage_models_file="$repository_root/docs/tla/storage_models.bzl"
readonly tla_work_root="${TLA_WORK_ROOT:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/skein-tla}"
readonly downloaded_jar="$tla_work_root/tla2tools-$tla_version.jar"

specifications=()

load_specifications() {
  while IFS= read -r specification; do
    specifications+=("$specification")
  done < <(sed -n 's/^    "\([A-Za-z0-9_][A-Za-z0-9_]*\)",$/\1/p' "$storage_models_file")

  if [[ "${#specifications[@]}" -eq 0 ]]; then
    printf 'storage TLA+ model manifest is empty or invalid: %s\n' \
      "$storage_models_file" >&2
    return 1
  fi

  local declared
  local sorted_unique
  declared="$(printf '%s\n' "${specifications[@]}")"
  sorted_unique="$(printf '%s\n' "${specifications[@]}" | LC_ALL=C sort -u)"
  if [[ "$declared" != "$sorted_unique" ]]; then
    printf 'storage TLA+ model manifest must be sorted and contain no duplicates\n' >&2
    return 1
  fi

  local specification
  for specification in "${specifications[@]}"; do
    if [[ ! -f "$repository_root/docs/tla/$specification.tla" ]] ||
      [[ ! -f "$repository_root/docs/tla/$specification.cfg" ]]; then
      printf 'storage TLA+ model pair is missing for %s\n' "$specification" >&2
      return 1
    fi
  done

  local path
  local model
  for path in "$repository_root"/docs/tla/*.tla \
    "$repository_root"/docs/tla/*.cfg; do
    model="$(basename "$path")"
    model="${model%.*}"
    if ! grep -Fxq "$model" <<< "$declared"; then
      printf 'storage TLA+ model pair is not declared: %s\n' "$model" >&2
      return 1
    fi
  done
}

load_specifications
readonly -a specifications

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

verify_tlc_log() {
  local result="$1"
  [[ -s "$result" ]] &&
    grep -Fq 'Model checking completed. No error has been found.' "$result" &&
    grep -q '^Finished in ' "$result" &&
    ! grep -q '^Error:' "$result" || {
    printf 'TLA+ complete success evidence is missing or contains an error: %s\n' "$result" >&2
    return 1
  }
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
    verify_tlc_log "$result"
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

collect_bazel_results() {
  local -a bazel_tla_dirs=("$1" "${@:4}")
  local results_dir="$2"
  local source_revision="$3"
  local shard_count="${#bazel_tla_dirs[@]}"
  if [[ "$shard_count" -gt "${#specifications[@]}" ]]; then
    printf 'TLA+ shard count must not exceed the model count\n' >&2
    return 1
  fi
  manifest_json "$source_revision" > /dev/null
  mkdir -p "$results_dir"
  if find "$results_dir" -mindepth 1 -print -quit | grep -q .; then
    printf 'TLA+ results directory must be empty before collection\n' >&2
    return 1
  fi
  mkdir "$results_dir/models" "$results_dir/bazel"

  local specification
  local model_index=0
  for specification in "${specifications[@]}"; do
    local shard_index=$((model_index % shard_count))
    local evidence="${bazel_tla_dirs[$shard_index]}/${specification}_check.run.tlc-evidence"
    local receipt="$results_dir/bazel/$specification"
    local index
    for ((index = 0; index < shard_count; index++)); do
      if [[ "$index" -ne "$shard_index" ]] &&
        [[ -e "${bazel_tla_dirs[$index]}/${specification}_check.run.tlc-evidence" ]]; then
        printf 'TLA+ model appears in a duplicate or incorrect shard: %s\n' "$specification" >&2
        return 1
      fi
    done
    if [[ ! -f "$evidence/result.txt" ]] ||
      [[ "$(< "$evidence/result.txt")" != $'ok\n0' ]]; then
      printf 'TLA+ Bazel action did not complete successfully: %s\n' "$specification" >&2
      return 1
    fi
    if [[ ! -f "$evidence/tla2tools.sha256" ]] ||
      [[ "$(< "$evidence/tla2tools.sha256")" != "$tla_sha256" ]]; then
      printf 'TLA+ Bazel tool digest mismatch: %s\n' "$specification" >&2
      return 1
    fi
    if [[ ! -f "$evidence/tlc-args.txt" ]] ||
      [[ "$(< "$evidence/tlc-args.txt")" != $'-cleanup\n-workers\nauto' ]]; then
      printf 'TLA+ Bazel full-check arguments mismatch: %s\n' "$specification" >&2
      return 1
    fi
    verify_tlc_log "$evidence/tlc.log"
    cmp "$repository_root/docs/tla/$specification.tla" "$evidence/module.tla"
    cmp "$repository_root/docs/tla/$specification.cfg" "$evidence/model.cfg"
    [[ -s "$evidence/java-version.txt" ]] || {
      printf 'TLA+ Bazel Java version evidence is missing: %s\n' "$specification" >&2
      return 1
    }
    if [[ -f "$results_dir/java-version.txt" ]]; then
      cmp "$results_dir/java-version.txt" "$evidence/java-version.txt"
    else
      cp "$evidence/java-version.txt" "$results_dir/java-version.txt"
    fi
    cp "$evidence/tlc.log" "$results_dir/$specification.txt"
    cp "$evidence/module.tla" "$results_dir/models/$specification.tla"
    cp "$evidence/model.cfg" "$results_dir/models/$specification.cfg"
    mkdir "$receipt"
    cp "$evidence/result.txt" "$evidence/tla2tools.sha256" \
      "$evidence/tlc-args.txt" "$evidence/java-version.txt" "$receipt/"
    model_index=$((model_index + 1))
  done

  # Publish the commit-bound manifest only after the complete model set passes.
  manifest_json "$source_revision" > "$results_dir/manifest.json"
  printf '\n' >> "$results_dir/manifest.json"
  verify_results "$results_dir" "$source_revision"
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

if [[ "${1:-}" == "--collect-bazel-results" ]]; then
  [[ "$#" -eq 4 ]] || {
    printf 'usage: %s --collect-bazel-results BAZEL_TLA_DIR RESULTS_DIR SOURCE_REVISION\n' "$0" >&2
    exit 2
  }
  collect_bazel_results "$2" "$3" "$4"
  exit
fi

if [[ "${1:-}" == "--collect-bazel-shards" ]]; then
  [[ "$#" -ge 4 ]] || {
    printf 'usage: %s --collect-bazel-shards RESULTS_DIR SOURCE_REVISION BAZEL_TLA_DIR...\n' "$0" >&2
    exit 2
  }
  collect_bazel_results "$4" "$2" "$3" "${@:5}"
  exit
fi

if [[ "$#" -ne 0 ]] &&
  ! [[ "$#" -eq 1 && "${1:-}" == "--check-mutants" ]]; then
  printf 'usage: %s [--manifest-json SOURCE_REVISION | --verify-results RESULTS_DIR SOURCE_REVISION | --collect-bazel-results BAZEL_TLA_DIR RESULTS_DIR SOURCE_REVISION | --collect-bazel-shards RESULTS_DIR SOURCE_REVISION BAZEL_TLA_DIR... | --check-mutants]\n' "$0" >&2
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

check_mutants() {
  local mutants_file="$repository_root/docs/tla/mutants/mutants.txt"
  local mutant_state_root="$tla_work_root/mutants"
  local checked=0
  local module
  local config
  local invariant

  mkdir -p "$mutant_state_root"
  while read -r module config invariant; do
    [[ -n "$module" ]] || continue
    local module_path="$repository_root/docs/tla/$module.tla"
    local config_path="$repository_root/docs/tla/mutants/$config.cfg"
    local state_dir="$mutant_state_root/$config"
    local log="$mutant_state_root/$config.log"
    if [[ ! -f "$module_path" ]] || [[ ! -f "$config_path" ]]; then
      printf 'storage TLA+ mutant pair is missing: module=%s config=%s\n' \
        "$module" "$config" >&2
      return 1
    fi
    rm -rf "$state_dir"
    mkdir -p "$state_dir"
    if "$tla_java" -XX:+UseParallelGC -jar "$tla_jar" \
      -cleanup \
      -metadir "$state_dir" \
      -workers auto \
      -config "$config_path" \
      "$module_path" >"$log" 2>&1; then
      printf 'storage TLA+ mutant survived unexpectedly: %s\n' "$config" >&2
      return 1
    fi
    if ! grep -Fq "Invariant $invariant is violated." "$log"; then
      printf 'storage TLA+ mutant did not violate %s: %s\n' \
        "$invariant" "$config" >&2
      cat "$log" >&2
      return 1
    fi
    checked=$((checked + 1))
    printf 'storage TLA+ mutant rejected: %s -> %s\n' "$config" "$invariant"
  done < "$mutants_file"

  if [[ "$checked" -eq 0 ]]; then
    printf 'storage TLA+ mutant manifest is empty: %s\n' "$mutants_file" >&2
    return 1
  fi
}

if [[ "${1:-}" == "--check-mutants" ]]; then
  check_mutants
  exit
fi

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
