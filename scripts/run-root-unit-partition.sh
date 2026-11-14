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
  echo "usage: $0 <unit-test-binary> <general|concurrent|checkpoint_headroom|checkpoint_headroom_growing_table|checkpoint_headroom_controls|checkpoint_lifecycle|checkpoint_large_commit|checkpoint_large_commit_interrupted|analytics|analytics_core|analytics_core_general|analytics_identity|analytics_rss|recovery> [libtest arguments]" >&2
  exit 64
fi
partition_binary="$1"
partition="$2"
# Identity admission shares the dense RSS fixture; keep the old selector usable.
if [[ "${partition}" == analytics_identity ]]; then
  partition=analytics_rss
fi
shift 2
case "${partition}" in
  general|concurrent|checkpoint_headroom|checkpoint_headroom_growing_table|checkpoint_headroom_controls|checkpoint_lifecycle|checkpoint_large_commit|checkpoint_large_commit_interrupted|analytics|analytics_core|analytics_core_general|analytics_identity|analytics_rss|recovery) ;;
  *) echo "unknown root unit partition: ${partition}" >&2; exit 64 ;;
esac

total_shards="${TEST_TOTAL_SHARDS:-1}"
shard_index="${TEST_SHARD_INDEX:-0}"
if [[ "${total_shards}" != 1 || "${shard_index}" != 0 ]]; then
  echo 'invalid root unit shard coordinates' >&2
  exit 64
fi

growing_table_case=api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay
partition_owner="${partition}"
case "${partition}" in
  checkpoint_headroom_growing_table|checkpoint_headroom_controls)
    partition_owner=checkpoint_headroom ;;
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
    api::automatic_checkpoint::tests::relational_live_overlay::large_commit::interrupted::*)
      case_owner=checkpoint_large_commit_interrupted ;;
    api::automatic_checkpoint::tests::relational_live_overlay::large_commit::*)
      case_owner=checkpoint_large_commit ;;
    api::automatic_checkpoint::tests::headroom::*|api::automatic_checkpoint::tests::relational_live_overlay::*|api::automatic_checkpoint::tests::delta_pressure_sustains_generations_and_preserves_all_values_after_reopen|api::automatic_checkpoint::tests::soft_wal_pressure_sustains_multiple_generations_without_host_checkpoints)
      case_owner=checkpoint_headroom ;;
    # Give the remaining lifecycle families their own runtime budget while
    # keeping them isolated; new families inherit this owner.
    api::automatic_checkpoint::tests::*)
      case_owner=checkpoint_lifecycle ;;
    api::tests::graph_analytics::persistent_edges_stream_with_bounded_rss_in_an_isolated_process|api::tests::graph_analytics::rss_child)
      case_owner=analytics_rss ;;
    api::tests::concurrent_transactions::*|api::concurrent::*|api::transaction_locks::*)
      case_owner=concurrent ;;
    analytics::*|api::graph_analytics::*|api::tests::graph_analytics::*|api::tests::pagerank::*|api::tests::projected_graph_artifacts::*|api::tests::projected_graph_execution::*|api::tests::projection_generations::*)
      case_owner=analytics_core_general ;;
    api::branch_lifecycle::*|api::search_projection_consumer::*|api::tests::storage_recovery::*|api::tests::mutation_persistence::*|api::tests::transaction_compaction::*|store_facade_tests::*)
      case_owner=recovery ;;
    *) case_owner=general ;;
  esac
  if [[ "${case_owner}" == "${partition_owner}" ]] ||
    [[ "${partition}" == analytics && "${case_owner}" == analytics_* ]] ||
    [[ "${partition}" == analytics_core && ( "${case_owner}" == analytics_core_general || "${case_owner}" == analytics_rss ) ]]; then
    selected=true
    if [[ "${partition}" == checkpoint_headroom_growing_table && "${case_name}" != "${growing_table_case}" ]]; then
      selected=false
    elif [[ "${partition}" == checkpoint_headroom_controls && "${case_name}" == "${growing_table_case}" ]]; then
      selected=false
    fi
    if [[ "${selected}" == true ]]; then
      owned_cases=$((owned_cases + 1))
      owned_names+=("${case_name}")
    else
      partition_args+=(--skip "${case_name}")
      skipped_names+=("${case_name}")
    fi
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
if [[ -n "${TEST_SHARD_STATUS_FILE:-}" ]]; then touch "${TEST_SHARD_STATUS_FILE}"; fi
rm -f "${inventory}"
trap - EXIT
exec "${partition_args[@]}" "$@"
