#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${TEST_SRCDIR:-}" && -n "${TEST_WORKSPACE:-}" ]]; then
  source_root="${TEST_SRCDIR}/${TEST_WORKSPACE}"
else
  source_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
fixture="$(mktemp -d "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/tla-evidence-test.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/repo/scripts" "$fixture/repo/docs/tla" "$fixture/valid"
cp "$source_root/scripts/check-storage-tla.sh" "$fixture/repo/scripts/"
checker="$fixture/repo/scripts/check-storage-tla.sh"
revision=0123456789abcdef0123456789abcdef01234567
digest=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
printf 'STORAGE_MODELS = [\n    "Alpha",\n    "SkeinCowPagePublication",\n]\n' \
  > "$fixture/repo/docs/tla/storage_models.bzl"
for model in Alpha SkeinCowPagePublication; do
  printf '%s\n' "---- MODULE $model ----" '====' > "$fixture/repo/docs/tla/$model.tla"
  printf 'SPECIFICATION Spec\n' > "$fixture/repo/docs/tla/$model.cfg"
  evidence="$fixture/valid/${model}_check.run.tlc-evidence"
  mkdir "$evidence"
  cp "$fixture/repo/docs/tla/$model.tla" "$evidence/module.tla"
  cp "$fixture/repo/docs/tla/$model.cfg" "$evidence/model.cfg"
  printf '%s\n' 'Model checking completed. No error has been found.' \
    'Finished in 01s at (2026-09-07 00:00:00)' > "$evidence/tlc.log"
  printf 'ok\n0\n' > "$evidence/result.txt"
  printf '%s\n' "$digest" > "$evidence/tla2tools.sha256"
  printf '%s\n' '-cleanup' '-workers' 'auto' > "$evidence/tlc-args.txt"
  if [[ "$model" == SkeinCowPagePublication ]]; then
    printf '%s\n' '-lncheck' 'final' >> "$evidence/tlc-args.txt"
    printf '%s\n' 'Checking temporal properties for the complete state space with 2 total distinct states' \
      'Model checking completed. No error has been found.' 'Finished in 01s' > "$evidence/tlc.log"
  fi
  printf 'openjdk version "21.0.9"\n' > "$evidence/java-version.txt"
done

expect_failure() {
  local output="$1"
  shift
  if "$@" > "$output" 2>&1; then
    printf 'expected rejection: %s\n' "$*" >&2
    exit 1
  fi
}

# Collection and verification must not resolve a JAR or execute Java.
export TLA_JAVA="$fixture/java-must-not-run"
export TLA2TOOLS_JAR="$fixture/jar-must-not-be-read"
export TLA_WORK_ROOT="$fixture/work-must-not-exist"
bash "$checker" --collect-bazel-results "$fixture/valid" "$fixture/collected" "$revision"
bash "$checker" --verify-results "$fixture/collected" "$revision"
test ! -e "$TLA_WORK_ROOT"
for model in Alpha SkeinCowPagePublication; do
  cmp "$fixture/valid/${model}_check.run.tlc-evidence/tlc.log" "$fixture/collected/$model.txt"
  cmp "$fixture/valid/${model}_check.run.tlc-evidence/module.tla" "$fixture/collected/models/$model.tla"
done

# Reusing cached action outputs produces identical retained evidence.
bash "$checker" --collect-bazel-results "$fixture/valid" "$fixture/cached" "$revision"
diff -r "$fixture/collected" "$fixture/cached"

# Ordered shards must cover each complete model exactly once. A shard cannot
# independently publish the complete manifest, even if its own models pass.
mkdir "$fixture/shard0" "$fixture/shard1"
cp -R "$fixture/valid/Alpha_check.run.tlc-evidence" "$fixture/shard0/"
cp -R "$fixture/valid/SkeinCowPagePublication_check.run.tlc-evidence" "$fixture/shard1/"
bash "$checker" --collect-bazel-shards "$fixture/merged" "$revision" "$fixture/shard0" "$fixture/shard1"
diff -r "$fixture/collected" "$fixture/merged"
expect_failure "$fixture/missing-shard.log" bash "$checker" --collect-bazel-shards \
  "$fixture/missing-shard-results" "$revision" "$fixture/shard0"
test ! -e "$fixture/missing-shard-results/manifest.json"
expect_failure "$fixture/swapped-shards.log" bash "$checker" --collect-bazel-shards \
  "$fixture/swapped-shards-results" "$revision" "$fixture/shard1" "$fixture/shard0"
test ! -e "$fixture/swapped-shards-results/manifest.json"
expect_failure "$fixture/duplicate-shard.log" bash "$checker" --collect-bazel-shards \
  "$fixture/duplicate-shard-results" "$revision" "$fixture/shard0" "$fixture/valid"
test ! -e "$fixture/duplicate-shard-results/manifest.json"
expect_failure "$fixture/excess-shards.log" bash "$checker" --collect-bazel-shards \
  "$fixture/excess-shards-results" "$revision" "$fixture/shard0" "$fixture/shard1" "$fixture/valid"
expect_failure "$fixture/no-shards.log" bash "$checker" --collect-bazel-shards "$fixture/no-shards-results" "$revision"

expect_failure "$fixture/reuse.log" bash "$checker" --collect-bazel-results \
  "$fixture/valid" "$fixture/collected" "$revision"
expect_failure "$fixture/revision.log" bash "$checker" --verify-results \
  "$fixture/collected" deadbeef
expect_failure "$fixture/args.log" bash "$checker" --collect-bazel-results "$fixture/valid"

for mutation in missing_model missing_log missing_verdict expected_counterexample nonzero_exit \
  wrong_jar missing_jar weakened_args missing_args no_success incomplete_log error_in_log \
  changed_module changed_config missing_java inconsistent_java \
  missing_final_check disabled_liveness default_liveness; do
  candidate="$fixture/$mutation"
  cp -R "$fixture/valid" "$candidate"
  evidence="$candidate/SkeinCowPagePublication_check.run.tlc-evidence"
  case "$mutation" in
    missing_model) mv "$evidence" "$candidate/undeclared-model" ;;
    missing_log) rm "$evidence/tlc.log" ;;
    missing_verdict) rm "$evidence/result.txt" ;;
    expected_counterexample) printf 'invariant_violation\n12\n' > "$evidence/result.txt" ;;
    nonzero_exit) printf 'ok\n137\n' > "$evidence/result.txt" ;;
    wrong_jar) printf 'wrong\n' > "$evidence/tla2tools.sha256" ;;
    missing_jar) rm "$evidence/tla2tools.sha256" ;;
    weakened_args) printf '%s\n' '-deadlock' > "$evidence/tlc-args.txt" ;;
    missing_args) rm "$evidence/tlc-args.txt" ;;
    no_success) printf 'Finished in 01s\n' > "$evidence/tlc.log" ;;
    incomplete_log) printf 'Model checking completed. No error has been found.\n' > "$evidence/tlc.log" ;;
    error_in_log) printf 'Error: Invariant Broken is violated.\n' >> "$evidence/tlc.log" ;;
    changed_module) printf '\\* changed\n' >> "$evidence/module.tla" ;;
    changed_config) printf 'CHECK_DEADLOCK FALSE\n' >> "$evidence/model.cfg" ;;
    missing_java) rm "$evidence/java-version.txt" ;;
    inconsistent_java) printf 'different runtime\n' > "$evidence/java-version.txt" ;;
    missing_final_check)
      printf '%s\n' 'Model checking completed. No error has been found.' 'Finished in 01s' > "$evidence/tlc.log" ;;
    disabled_liveness) printf '%s\n' '-cleanup' '-workers' 'auto' '-lncheck' 'none' > "$evidence/tlc-args.txt" ;;
    default_liveness) printf '%s\n' '-cleanup' '-workers' 'auto' > "$evidence/tlc-args.txt" ;;
  esac
  expect_failure "$fixture/$mutation.log" bash "$checker" --collect-bazel-results \
    "$candidate" "$fixture/$mutation-results" "$revision"
  test ! -e "$fixture/$mutation-results/manifest.json"
done

for mutation in duplicate unsorted undeclared missing_pair; do
  repo="$fixture/repo-$mutation"
  cp -R "$fixture/repo" "$repo"
  case "$mutation" in
    duplicate) printf '    "Alpha",\n' >> "$repo/docs/tla/storage_models.bzl" ;;
    unsorted) printf 'STORAGE_MODELS = [\n    "SkeinCowPagePublication",\n    "Alpha",\n]\n' > "$repo/docs/tla/storage_models.bzl" ;;
    undeclared) cp "$repo/docs/tla/Alpha.tla" "$repo/docs/tla/Gamma.tla" ;;
    missing_pair) rm "$repo/docs/tla/SkeinCowPagePublication.cfg" ;;
  esac
  expect_failure "$fixture/$mutation.log" bash "$repo/scripts/check-storage-tla.sh" --manifest-json "$revision"
done

cp -R "$fixture/collected" "$fixture/tampered-bundle"
printf 'Error: unexpected failure\n' >> "$fixture/tampered-bundle/Alpha.txt"
expect_failure "$fixture/tampered.log" bash "$checker" --verify-results "$fixture/tampered-bundle" "$revision"
printf 'fixture\n' > "$fixture/collected/tla2tools.jar"
expect_failure "$fixture/binary.log" bash "$checker" --verify-results "$fixture/collected" "$revision"

# Keep the standalone runner's invocation and pipeline-failure contract covered
# without launching another full model campaign. Real TLC and negative controls
# are exercised by the rule contract targets and the existing mutant runner.
cp "$source_root/docs/tla/tests/fake-java.sh" "$fixture/fake-java"
chmod +x "$fixture/fake-java"
export TLA_JAVA="$fixture/fake-java"
export TLA_WORK_ROOT="$fixture/standalone-work"
export TLA_RESULTS_DIR="$fixture/standalone-results"
export TLA_SOURCE_REVISION="$revision"
export TLA_TEST_INVOCATIONS="$fixture/invocations.txt"
bash "$checker"
bash "$checker" --verify-results "$TLA_RESULTS_DIR" "$revision"
test "$(wc -l < "$TLA_TEST_INVOCATIONS" | tr -d ' ')" = 2
grep -q '/Alpha.tla$' "$TLA_TEST_INVOCATIONS"
grep -q '/SkeinCowPagePublication.tla$' "$TLA_TEST_INVOCATIONS"
export TLA_TEST_FAIL=true
export TLA_RESULTS_DIR="$fixture/standalone-failure"
expect_failure "$fixture/standalone-failure.log" bash "$checker"
test ! -e "$TLA_RESULTS_DIR/manifest.json"

printf 'TLA evidence collection contract checks passed\n'
