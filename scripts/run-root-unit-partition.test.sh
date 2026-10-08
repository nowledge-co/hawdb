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
if [[ -n "${TEST_SRCDIR:-}" && -n "${TEST_WORKSPACE:-}" ]]; then
  runner="${TEST_SRCDIR}/${TEST_WORKSPACE}/scripts/run-root-unit-partition.sh"
else
  runner="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-root-unit-partition.sh"
fi
fixture="$(mktemp -d "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/hawdb-unit-partition-test.XXXXXX")"
trap 'rm -rf "${fixture}"' EXIT
export PARTITION_PROBE_ARGS="${fixture}/args"
cat >"${fixture}/binary" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --list ]]; then
  if [[ "${PARTITION_PROBE_DISCOVERY_EXIT:-0}" != 0 ]]; then
    exit "${PARTITION_PROBE_DISCOVERY_EXIT}"
  fi
  if [[ "${PARTITION_PROBE_EMPTY:-0}" == 0 ]]; then
    echo 'api::tests::concurrent_transactions::owned: test'
  fi
  echo 'api::tests::graph_analytics::owned: test'
  echo 'api::tests::storage_recovery::owned: test'
  echo 'api::tests::query_execution::owned: test'
  echo '4 tests, 0 benchmarks'
  exit 0
fi
printf '%s\n' "$@" >"${PARTITION_PROBE_ARGS}"
exit "${PARTITION_PROBE_EXIT:-0}"
EOF
chmod +x "${fixture}/binary"

for partition in general concurrent analytics recovery; do
  "${runner}" "${fixture}/binary" "${partition}" custom_filter --exact --skip user_skip
  python3 - "${PARTITION_PROBE_ARGS}" "${partition}" <<'PY'
import pathlib
import sys

args = pathlib.Path(sys.argv[1]).read_text().splitlines()
names = {
    "concurrent": "api::tests::concurrent_transactions::owned",
    "analytics": "api::tests::graph_analytics::owned",
    "recovery": "api::tests::storage_recovery::owned",
    "general": "api::tests::query_execution::owned",
}
skips = [args[i + 1] for i, value in enumerate(args[:-1]) if value == "--skip"]
assert set(skips) == {name for owner, name in names.items() if owner != sys.argv[2]} | {"user_skip"}
assert args[-4:] == ["custom_filter", "--exact", "--skip", "user_skip"]
PY
done

expect_exit() {
  local expected="$1"
  shift
  local observed=0
  "$@" >"${fixture}/error" 2>&1 || observed=$?
  if [[ "${observed}" != "${expected}" ]]; then
    cat "${fixture}/error" >&2
    echo "expected exit ${expected}, got ${observed}" >&2
    exit 1
  fi
}
expect_exit 42 env PARTITION_PROBE_EXIT=42 "${runner}" "${fixture}/binary" general
expect_exit 17 env PARTITION_PROBE_DISCOVERY_EXIT=17 "${runner}" "${fixture}/binary" general
expect_exit 1 env PARTITION_PROBE_EMPTY=1 "${runner}" "${fixture}/binary" concurrent
expect_exit 64 "${runner}" "${fixture}/binary" unknown
echo "root unit partition ownership, caller arguments and failure propagation passed"
