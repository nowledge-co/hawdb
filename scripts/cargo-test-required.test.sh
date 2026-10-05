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
  runner="${TEST_SRCDIR}/${TEST_WORKSPACE}/scripts/cargo-test-required.sh"
else
  runner="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/cargo-test-required.sh"
fi
fixture="$(mktemp -d "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/cargo-test-required.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT
mkdir "$fixture/bin"
cat > "$fixture/bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" > "$HAWDB_CARGO_TEST_FIXTURE/arguments"
cat "$HAWDB_CARGO_TEST_FIXTURE/output"
exit "$HAWDB_CARGO_TEST_STATUS"
EOF
chmod +x "$fixture/bin/cargo"

check_result() {
  local expected="$1" cargo_status="$2" actual=0
  PATH="$fixture/bin:$PATH" HAWDB_CARGO_TEST_FIXTURE="$fixture" \
    HAWDB_CARGO_TEST_STATUS="$cargo_status" \
    bash "$runner" --locked -p hawdb-search --lib 'filter with spaces' -- --nocapture \
    > "$fixture/result" 2>&1 || actual=$?
  if [[ "$actual" -ne "$expected" ]]; then
    cat "$fixture/result" >&2
    echo "expected status $expected, got $actual" >&2
    exit 1
  fi
}

printf 'test result: ok. 3 passed; 0 failed; 0 ignored; 12 filtered out\n' > "$fixture/output"
check_result 0 0
printf '%s\n' test --locked -p hawdb-search --lib 'filter with spaces' -- --nocapture \
  > "$fixture/expected-arguments"
cmp "$fixture/expected-arguments" "$fixture/arguments"

printf 'test result: ok. 0 passed; 0 failed; 0 ignored; 100 filtered out\n' > "$fixture/output"
check_result 1 0
printf 'test result: ok. 0 passed; 0 failed; 3 ignored; 100 filtered out\n' > "$fixture/output"
check_result 1 0
printf 'Finished without a libtest result\n' > "$fixture/output"
check_result 1 0
printf 'error: compilation failed\n' > "$fixture/output"
check_result 42 42

# One passing binary must not conceal a later binary's failure.
printf '%s\n' 'test result: ok. 3 passed; 0 failed;' \
  'test result: FAILED. 0 passed; 1 failed;' > "$fixture/output"
check_result 42 42
printf '%s\n' 'test result: ok. 0 passed; 0 failed;' \
  'test result: ok. 12 passed; 0 failed;' > "$fixture/output"
check_result 0 0

echo "required Cargo test selection checks passed"
