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
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/testlogs"
export FAKE_NATIVE_ROOT="$fixture"
export BAZEL="$fixture/bazel"
export FAKE_NATIVE_SELECTOR="$root/scripts/vector_recovery_cases.py"
if command -v cygpath >/dev/null 2>&1; then
  FAKE_NATIVE_SELECTOR="$(cygpath -m "$FAKE_NATIVE_SELECTOR")"
fi
cat >"$BAZEL" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == info ]]; then echo "$FAKE_NATIVE_ROOT/testlogs"; exit 0; fi
printf '%q ' "$@" >>"$FAKE_NATIVE_ROOT/commands.txt"
printf '\n' >>"$FAKE_NATIVE_ROOT/commands.txt"
if [[ "${FAKE_NATIVE_MODE:-}" == failure ]]; then exit 23; fi
name=''
for argument in "$@"; do
  case "$argument" in
    --build_event_json_file=*)
      path="${argument#*=}"
      name="$(basename "$path" .bep.json)"
      if [[ "${FAKE_NATIVE_MODE:-}" != missing-bep ]]; then printf '{}\n' >"$path"; fi ;;
  esac
done
target="${!#}"
relative="${target#//}"
relative="${relative/:/\/}"
relative="${relative#/}"
mkdir -p "$FAKE_NATIVE_ROOT/testlogs/$relative"
printf '%s\n' "$name" >"$FAKE_NATIVE_ROOT/testlogs/$relative/test.xml"
printf '%s\n' "$name" >"$FAKE_NATIVE_ROOT/testlogs/$relative/test.log"
if [[ "$name" == go ]]; then echo '--- PASS: TestNative (0.00s)'; exit 0; fi
count=1
case "$name" in
  *branch_project_open) count=2 ;;
  *-views) count=3 ;;
  abandonment) count=8 ;;
esac
case "${FAKE_NATIVE_MODE:-}" in
  zero) count=0 ;;
  wrong-count) count=9 ;;
esac
printf 'test result: ok. %s passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n' "$count"
if [[ "$name" == recovery-* ]]; then
  python3 - "$FAKE_NATIVE_SELECTOR" "$name" <<'PY'
import importlib.util
import os
import sys
spec = importlib.util.spec_from_file_location("recovery_cases", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
for index, (origin, cut, durability, graph, updated) in enumerate(sorted(module.expected_combinations(sys.argv[2]))):
    if index == 0 and os.environ.get("FAKE_NATIVE_MODE") == "missing-recovery":
        continue
    print(f"canonical-vector-recovery-power-v1 origin={origin} cut={cut} "
          f"durability={durability} graph={graph} source_updated={updated} "
          "plans=7 distinct_reopens=3")
PY
fi
MOCK
chmod +x "$BAZEL"
for shard in transactions storage search consumers identities snapshots bindings abandonment \
  consumers-recovery-births consumers-recovery-unselected consumers-recovery-selected \
  consumers-recovery-lost-sql-link consumers-recovery-lost-sql-selector \
  consumers-recovery-lost-nodes-link consumers-recovery-lost-nodes-selector \
  consumers-publication-artifacts consumers-publication-selectors consumers-publication-barriers; do
  export WINDOWS_NATIVE_RESULTS="$fixture/$shard"
  bash "$root/scripts/run-windows-native-bazel.sh" "$shard" >"$fixture/$shard.log" 2>&1
done
# Repeated filtered invocations must retain their own XML before overwrite.
grep -qx instance-identity-default-views "$fixture/snapshots/testlogs/instance-identity-default-views/test.xml"
grep -qx instance-identity-minimal-views "$fixture/snapshots/testlogs/instance-identity-minimal-views/test.xml"
# Preserve every original selection, seven isolated vector recovery selections,
# three first-crash vector publication cases, and both default WAL recovery families.
[[ "$(wc -l <"$fixture/commands.txt" | tr -d ' ')" == 41 ]]
grep -q -- '--test_arg=recovery_995' "$fixture/commands.txt"
grep -q -- '--test_arg=api::tests::storage_recovery::wal_tail::' "$fixture/commands.txt"
grep -q -- '--test_arg=resident_diskann::tests::' "$fixture/commands.txt"
grep -q -- '--test_arg=vector_index_incremental --test_arg=vector_index_canonical_completion' "$fixture/commands.txt"
grep -q -- '--test_arg=--skip --test_arg=api::tests::power_loss::consumer_vector_incremental::faults:: --test_arg=--nocapture' "$fixture/commands.txt"
[[ "$(grep -c -- '--test_arg=api::tests::power_loss::consumer_vector_incremental::faults::recovery::vector_index_' "$fixture/commands.txt")" == 7 ]]
[[ "$(grep -c -- '--test_arg=api::tests::power_loss::consumer_vector_incremental::faults::vector_index_' "$fixture/commands.txt")" == 3 ]]
grep -q -- '--//:windows_native_profile=facade_all' "$fixture/commands.txt"
grep -q -- '--test_arg=--test-threads=4' "$fixture/commands.txt"
grep -q -- '--test_arg=-test.count=1' "$fixture/commands.txt"
for mode in zero failure missing-bep; do
  export WINDOWS_NATIVE_RESULTS="$fixture/refuse-$mode"
  if FAKE_NATIVE_MODE="$mode" bash "$root/scripts/run-windows-native-bazel.sh" transactions >"$fixture/refuse-$mode.log" 2>&1; then
    echo "native runner accepted $mode" >&2; exit 1
  fi
done
export WINDOWS_NATIVE_RESULTS="$fixture/refuse-count"
if FAKE_NATIVE_MODE=wrong-count bash "$root/scripts/run-windows-native-bazel.sh" snapshots >"$fixture/refuse-count.log" 2>&1; then
  echo 'native runner accepted wrong exact count' >&2; exit 1
fi
export WINDOWS_NATIVE_RESULTS="$fixture/refuse-publication-count"
if FAKE_NATIVE_MODE=wrong-count bash "$root/scripts/run-windows-native-bazel.sh" consumers-publication-artifacts >"$fixture/refuse-publication-count.log" 2>&1; then
  echo 'native runner accepted wrong publication count' >&2; exit 1
fi
export WINDOWS_NATIVE_RESULTS="$fixture/refuse-recovery"
if FAKE_NATIVE_MODE=missing-recovery bash "$root/scripts/run-windows-native-bazel.sh" consumers-recovery-selected >"$fixture/refuse-recovery.log" 2>&1; then
  echo 'native runner accepted missing recovery combination' >&2; exit 1
fi
export WINDOWS_NATIVE_RESULTS="$fixture/refuse-threads"
if POWER_LOSS_TEST_THREADS=3 bash "$root/scripts/run-windows-native-bazel.sh" abandonment >"$fixture/refuse-threads.log" 2>&1; then
  echo 'native runner accepted unsupported fault concurrency' >&2; exit 1
fi
printf 'PASS | native shard selections and negative controls\n'
