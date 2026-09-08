#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 ACL_TEST_BINARY" >&2
  exit 2
fi

readonly test_binary="$1"
readonly test_filter="api::tests::runtime_capabilities::"
readonly acl_test="${test_filter}enabled_access_control_filters_before_ranking_without_exposing_policy_inputs: test"

# A disabled ACL profile or a stale filter must not pass by executing no tests.
if ! "$test_binary" "$test_filter" --list | awk -v expected="$acl_test" '
  $0 == expected { found = 1 }
  END { exit !found }
'; then
  echo "ACL capability test is missing from the qualification binary" >&2
  exit 1
fi

exec "$test_binary" "$test_filter" --nocapture
