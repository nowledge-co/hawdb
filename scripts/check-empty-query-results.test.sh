#!/usr/bin/env bash

set -euo pipefail

checker="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/check-empty-query-results.sh}"
fixture_root="$(mktemp -d "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/skein-query-results.XXXXXX")"
trap 'rm -r -- "$fixture_root"' EXIT

expect_failure() {
  local expected_status="$1"
  local expected_message="$2"
  shift 2
  local status=0
  bash "$checker" "$@" >"$fixture_root/output" 2>&1 || status=$?
  if [[ "$status" -ne "$expected_status" ]]; then
    echo "expected status $expected_status, got $status" >&2
    cat "$fixture_root/output" >&2
    exit 1
  fi
  if ! grep -Fq -- "$expected_message" "$fixture_root/output"; then
    echo "missing expected diagnostic: $expected_message" >&2
    cat "$fixture_root/output" >&2
    exit 1
  fi
}

touch "$fixture_root/empty result" "$fixture_root/second empty result"
printf '%s\n' '//:skein_minimal' '@crate_index//:simsimd' >"$fixture_root/nonempty result"
mkdir "$fixture_root/directory"

bash "$checker" "$fixture_root/empty result"
bash "$checker" "$fixture_root/empty result" "$fixture_root/second empty result"
expect_failure 2 'usage:'
expect_failure 1 'forbidden dependency path:' "$fixture_root/nonempty result"
expect_failure 1 'forbidden dependency path:' "$fixture_root/empty result" "$fixture_root/nonempty result"
expect_failure 1 '@crate_index//:simsimd' "$fixture_root/nonempty result" "$fixture_root/empty result"
expect_failure 1 'missing or unreadable query result:' "$fixture_root/missing result"
expect_failure 1 'missing or unreadable query result:' "$fixture_root/empty result" "$fixture_root/missing result"
expect_failure 1 'missing or unreadable query result:' "$fixture_root/directory"
