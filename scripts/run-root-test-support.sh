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

# Runs the root unit cases that only exist with the `test-support` feature
# (#1965): the difference between the two inventories, so a new gated case is
# selected without a change here.

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <test-support-binary> <default-binary> [libtest arguments]" >&2
  exit 64
fi
test_support_binary="$1"
default_binary="$2"
shift 2

directory="$(mktemp -d "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/hawdb-test-support.XXXXXX")"
trap 'rm -rf "${directory}"' EXIT
"${test_support_binary}" --list >"${directory}/test-support.txt"
"${default_binary}" --list >"${directory}/default.txt"
names() { sed -n 's/: test$//p' "$1" | LC_ALL=C sort -u; }
names "${directory}/test-support.txt" >"${directory}/test-support.names"
names "${directory}/default.txt" >"${directory}/default.names"

selected=()
while IFS= read -r case_name; do
  case "${case_name}" in
    api::tests::power_loss::checkpoint_leftovers::*|api::tests::power_loss::checkpoint_temporaries::*) ;;
    # The remaining power-loss matrix keeps its weekly and Windows owners
    # (docs/BRANCH_POWER_LOSS_QUALIFICATION.md). This also leaves out the case
    # that fails on main (#1941).
    api::tests::power_loss::*) continue ;;
  esac
  selected+=("${case_name}")
done < <(LC_ALL=C comm -23 "${directory}/test-support.names" "${directory}/default.names")

# A renamed or dropped family must not silently leave the lane.
for required in \
  api::tests::immutable_retirement_reads:: \
  api::search_projection_consumer::tests::stationary::search_artifact_reads_survive_a_paused_immutable_retirement \
  api::tests::power_loss::checkpoint_leftovers::; do
  found=false
  for case_name in ${selected[@]+"${selected[@]}"}; do
    if [[ "${case_name}" == "${required}"* ]]; then found=true; break; fi
  done
  if [[ "${found}" == false ]]; then
    echo "test-support lane has no case of ${required}" >&2
    exit 1
  fi
done
rm -rf "${directory}"
trap - EXIT
echo "test-support lane: ${#selected[@]} cases"
exec "${test_support_binary}" "${selected[@]}" --exact "$@"
