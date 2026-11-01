#!/usr/bin/env bash
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
if [[ -n "${TEST_SRCDIR:-}" && -n "${TEST_WORKSPACE:-}" ]]; then
  runner="$TEST_SRCDIR/$TEST_WORKSPACE/crates/executor/tests/run-endpoint-resolution.sh"
else
  runner="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-endpoint-resolution.sh"
fi
fixture="$(mktemp -d "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/hawdb-endpoint-script.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT
export ENDPOINT_PROBE_ARGS="$fixture/args"
export ENDPOINT_PROBE_CASE='mutation::staged::tests::resolved_endpoints_create_exactly_what_per_row_scans_create'
cat >"$fixture/unit binary" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
listing=0
for arg in "$@"; do
  if [[ "$arg" == --list ]]; then listing=1; fi
done
if [[ "$listing" == 1 ]]; then
  if [[ "${ENDPOINT_PROBE_DISCOVERY_EXIT:-0}" != 0 ]]; then
    exit "$ENDPOINT_PROBE_DISCOVERY_EXIT"
  fi
  cases=()
  if [[ "${ENDPOINT_PROBE_MISSING:-0}" == 0 ]]; then
    cases+=("$ENDPOINT_PROBE_CASE")
  fi
  if [[ "${ENDPOINT_PROBE_EMPTY_GENERAL:-0}" == 0 ]]; then
    cases+=('batch::tests::graph_match::ordinary' 'mutation::staged::tests::ordinary')
  fi
  if [[ "${ENDPOINT_PROBE_OVERLAP:-0}" == 1 ]]; then
    cases+=("nested::$ENDPOINT_PROBE_CASE")
  fi
  if [[ "${ENDPOINT_PROBE_DUPLICATE:-0}" == 1 ]]; then
    cases+=("$ENDPOINT_PROBE_CASE")
  fi
  if [[ ${#cases[@]} -gt 0 ]]; then
    for name in "${cases[@]}"; do
      selected=0
      skipped=0
      skip_next=0
      for arg in "$@"; do
        if [[ $skip_next == 1 ]]; then
          if [[ "$arg" == "$name" ]]; then skipped=1; fi
          skip_next=0
        elif [[ "$arg" == --skip ]]; then
          skip_next=1
        elif [[ "$arg" == "$name" ]]; then
          selected=1
        fi
      done
      if [[ "$1" == --list || ( $selected == 1 && $skipped == 0 ) ]]; then
        echo "$name: test"
      fi
    done
  fi
  exit 0
fi
printf '%s\n' "$@" >"$ENDPOINT_PROBE_ARGS"
exit "${ENDPOINT_PROBE_EXIT:-0}"
EOF
chmod +x "$fixture/unit binary"
"$runner" "$fixture/unit binary" "$ENDPOINT_PROBE_CASE" ordinary --nocapture --skip 'caller skip'
python3 - "$ENDPOINT_PROBE_ARGS" "$ENDPOINT_PROBE_CASE" <<'PY'
import pathlib
import sys

args = pathlib.Path(sys.argv[1]).read_text().splitlines()
isolated = sys.argv[2]
inventory = {isolated, 'batch::tests::graph_match::ordinary', 'mutation::staged::tests::ordinary'}
assert args[:3] == [isolated, '--exact', '--test-threads=1']
assert args[-4:] == ['ordinary', '--nocapture', '--skip', 'caller skip']
assert len(args) == 7
# The ordinary owner's native substring skip and the isolated owner's validated
# exact selection cover every discovered case once.
general = {name for name in inventory if isolated not in name}
selected = {args[0]}
assert general.isdisjoint(selected)
assert general | selected == inventory
PY

expect_status() {
  local expected="$1"
  shift
  local status=0
  "$@" >"$fixture/error" 2>&1 || status=$?
  if [[ "$status" != "$expected" ]]; then
    cat "$fixture/error" >&2
    echo "expected status $expected, got $status" >&2
    exit 1
  fi
}
expect_status 64 "$runner"
expect_status 1 env ENDPOINT_PROBE_MISSING=1 "$runner" "$fixture/unit binary" "$ENDPOINT_PROBE_CASE"
expect_status 1 env ENDPOINT_PROBE_EMPTY_GENERAL=1 "$runner" "$fixture/unit binary" "$ENDPOINT_PROBE_CASE"
expect_status 1 env ENDPOINT_PROBE_OVERLAP=1 "$runner" "$fixture/unit binary" "$ENDPOINT_PROBE_CASE"
expect_status 1 env ENDPOINT_PROBE_DUPLICATE=1 "$runner" "$fixture/unit binary" "$ENDPOINT_PROBE_CASE"
expect_status 1 "$runner" "$fixture/unit binary" "$ENDPOINT_PROBE_CASE" --skip "$ENDPOINT_PROBE_CASE"
expect_status 1 "$runner" "$fixture/unit binary" "$ENDPOINT_PROBE_CASE" batch::tests::graph_match::ordinary
expect_status 23 env ENDPOINT_PROBE_DISCOVERY_EXIT=23 "$runner" "$fixture/unit binary" "$ENDPOINT_PROBE_CASE"
expect_status 29 env ENDPOINT_PROBE_EXIT=29 "$runner" "$fixture/unit binary" "$ENDPOINT_PROBE_CASE"
