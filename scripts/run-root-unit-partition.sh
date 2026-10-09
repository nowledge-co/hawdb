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

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <unit-test-binary> <general|concurrent|analytics|recovery> [libtest arguments]" >&2
  exit 64
fi
partition_binary="$1"
partition="$2"
shift 2
case "${partition}" in
  general|concurrent|analytics|recovery) ;;
  *) echo "unknown root unit partition: ${partition}" >&2; exit 64 ;;
esac

inventory="$(mktemp "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/hawdb-unit-partition.XXXXXX")"
trap 'rm -f "${inventory}"' EXIT
"${partition_binary}" --list >"${inventory}"
partition_args=("${partition_binary}")
owned_names=()
skipped_names=()
owned_cases=0
while IFS= read -r entry; do
  case "${entry}" in
    *": test") case_name="${entry%: test}" ;;
    *) continue ;;
  esac
  case "${case_name}" in
    api::tests::concurrent_transactions::*|api::concurrent::*|api::transaction_locks::*)
      case_owner=concurrent ;;
    analytics::*|api::graph_analytics::*|api::tests::graph_analytics::*|api::tests::pagerank::*|api::tests::projected_graph_artifacts::*|api::tests::projected_graph_execution::*|api::tests::projection_generations::*)
      case_owner=analytics ;;
    api::branch_lifecycle::*|api::search_projection_consumer::*|api::tests::storage_recovery::*|api::tests::mutation_persistence::*|api::tests::transaction_compaction::*|store_facade_tests::*)
      case_owner=recovery ;;
    *) case_owner=general ;;
  esac
  if [[ "${case_owner}" == "${partition}" ]]; then
    owned_cases=$((owned_cases + 1))
    owned_names+=("${case_name}")
  else
    # Multiple positive libtest filters are ORed. Complementary full-name skips
    # keep the ownership boundary when callers add filters or --exact.
    partition_args+=(--skip "${case_name}")
    skipped_names+=("${case_name}")
  fi
done <"${inventory}"
if [[ ${owned_cases} -eq 0 ]]; then
  echo "root unit partition has no discovered cases: ${partition}" >&2
  exit 1
fi
if [[ ${#skipped_names[@]} -gt 0 ]]; then
  # Without --exact, libtest treats even complete --skip names as substrings.
  # Reject inventories that would silently suppress a case owned here.
  for owned_name in "${owned_names[@]}"; do
    for skipped_name in "${skipped_names[@]}"; do
      if [[ "${owned_name}" == *"${skipped_name}"* ]]; then
        echo "root unit partition name overlap (${partition}): ${owned_name} contains skipped ${skipped_name}" >&2
        exit 1
      fi
    done
  done
fi
rm -f "${inventory}"
trap - EXIT
exec "${partition_args[@]}" "$@"
