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
