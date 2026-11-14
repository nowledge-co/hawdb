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
  runner="${TEST_SRCDIR}/${TEST_WORKSPACE}/scripts/run-mvcc-joined-read-shard.sh"
else
  runner="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-mvcc-joined-read-shard.sh"
fi
fixture="$(mktemp -d "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/hawdb-joined-shard-test.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT
export JOINED_SHARD_RECORD="$fixture/selected"
cat >"$fixture/binary" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == --list ]]; then
  if [[ "${JOINED_SHARD_MODE:-}" != empty ]]; then
    for (( index=0; index<${JOINED_SHARD_CASES:-8}; index++ )); do
      echo "joined_case_$index: test"
    done
  fi
  exit 0
fi
name="$1"
shift
[[ "$1" == --exact && "$2" == --nocapture ]]
printf '%s\n' "$name" >>"$JOINED_SHARD_RECORD"
if [[ "${JOINED_SHARD_MODE:-}" == failure ]]; then exit 23; fi
passed=1
if [[ "${JOINED_SHARD_MODE:-}" == zero ]]; then passed=0; fi
printf 'test result: ok. %s passed; 0 failed; 0 ignored; 0 measured; %s filtered out;\n' \
  "$passed" "$(( ${JOINED_SHARD_CASES:-8} - 1 ))"
MOCK
chmod +x "$fixture/binary"
# Exercise both one case per shard and an uneven inventory after scenario splitting.
for count in 8 23; do
  export JOINED_SHARD_CASES="$count"
  : >"$JOINED_SHARD_RECORD"
  for index in {0..7}; do
    status="$fixture/status-$count-$index"
    TEST_TOTAL_SHARDS=8 TEST_SHARD_INDEX="$index" TEST_SHARD_STATUS_FILE="$status" \
      "$runner" "$fixture/binary" >"$fixture/output-$index"
    [[ -f "$status" ]]
    selected="$(grep -c '^test result: ok\. 1 passed;' "$fixture/output-$index")"
    (( selected == count / 8 || selected == (count + 7) / 8 ))
  done
  for (( index=0; index<count; index++ )); do
    echo "joined_case_$index"
  done >"$fixture/expected"
  # No omitted or duplicated case across the complete Bazel shard inventory.
  sort "$JOINED_SHARD_RECORD" >"$fixture/actual"
  sort "$fixture/expected" >"$fixture/expected-sorted"
  cmp "$fixture/expected-sorted" "$fixture/actual"
  : >"$JOINED_SHARD_RECORD"
  TEST_TOTAL_SHARDS=1 TEST_SHARD_INDEX=0 "$runner" "$fixture/binary" >"$fixture/local-output"
  cmp "$fixture/expected" "$JOINED_SHARD_RECORD"
done
expect_exit() {
  local expected="$1" actual=0
  shift
  "$@" >"$fixture/error" 2>&1 || actual=$?
  [[ "$actual" == "$expected" ]] || { cat "$fixture/error"; exit 1; }
}
expect_exit 23 env TEST_TOTAL_SHARDS=8 TEST_SHARD_INDEX=0 JOINED_SHARD_MODE=failure "$runner" "$fixture/binary"
expect_exit 1 env TEST_TOTAL_SHARDS=8 TEST_SHARD_INDEX=0 JOINED_SHARD_MODE=zero "$runner" "$fixture/binary"
expect_exit 1 env TEST_TOTAL_SHARDS=8 TEST_SHARD_INDEX=0 JOINED_SHARD_MODE=empty "$runner" "$fixture/binary"
expect_exit 1 env TEST_TOTAL_SHARDS=8 TEST_SHARD_INDEX=0 JOINED_SHARD_CASES=7 "$runner" "$fixture/binary"
for coordinates in '0 0' '8 8' '9 0' '8 -1' '8 01' '8 99999999999999999999'; do
  read -r total index <<<"$coordinates"
  expect_exit 64 env TEST_TOTAL_SHARDS="$total" TEST_SHARD_INDEX="$index" "$runner" "$fixture/binary"
done
echo 'PASS | joined-read shards cover each case once and preserve failures'
