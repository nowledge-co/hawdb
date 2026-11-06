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
  if [[ "${PARTITION_PROBE_ONLY_GROWING_TABLE:-0}" == 1 ]]; then
    echo 'api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay: test'
    exit 0
  fi
  if [[ "${PARTITION_PROBE_EMPTY:-0}" == 0 ]]; then
    echo 'api::tests::concurrent_transactions::owned: test'
  fi
  if [[ "${PARTITION_PROBE_EMPTY_CORE:-0}" == 0 ]]; then
    echo 'api::tests::graph_analytics::owned: test'
  fi
  if [[ "${PARTITION_PROBE_EMPTY_RSS:-0}" == 0 ]]; then
    echo 'api::tests::graph_analytics::persistent_edges_stream_with_bounded_rss_in_an_isolated_process: test'
    echo 'api::tests::graph_analytics::rss_child: test'
  fi
  echo 'api::tests::storage_recovery::owned: test'
  echo 'api::tests::query_execution::owned: test'
  if [[ "${PARTITION_PROBE_EMPTY_LIFECYCLE:-0}" == 0 ]]; then
    echo 'api::automatic_checkpoint::tests::manual::ordinary_manual_during_automatic_preparation_resumes_automatic_progress: test'
    echo 'api::automatic_checkpoint::tests::idle_handoff::releasing_a_runtime_read_guard_wakes_unattended_handoff: test'
    echo 'api::automatic_checkpoint::tests::planning_recovery::actual_owner_reports_cached_memory_denial_and_recovers_without_frontend_work: test'
    echo 'api::automatic_checkpoint::tests::coalesced_capture::coalesced_commits_refresh_and_checkpoint_without_another_frontend_call: test'
    echo 'api::automatic_checkpoint::tests::new_family::future_lifecycle: test'
    echo 'api::automatic_checkpoint::tests::future_lifecycle: test'
  fi
  if [[ "${PARTITION_PROBE_EMPTY_HEADROOM:-0}" == 0 ]]; then
    echo 'api::automatic_checkpoint::tests::headroom::pending_idle: test'
    echo 'api::automatic_checkpoint::tests::headroom::capacity_wait::deadline: test'
    echo 'api::automatic_checkpoint::tests::delta_pressure_sustains_generations_and_preserves_all_values_after_reopen: test'
    echo 'api::automatic_checkpoint::tests::soft_wal_pressure_sustains_multiple_generations_without_host_checkpoints: test'
    echo 'api::automatic_checkpoint::tests::relational_live_overlay::steady_relational_updates_on_one_handle_never_exhaust_the_live_overlay: test'
    if [[ "${PARTITION_PROBE_EMPTY_GROWING_TABLE:-0}" == 0 ]]; then
      echo 'api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay: test'
    fi
  fi
  echo 'api::automatic_checkpoint::tests::relational_live_overlay::large_commit::owned: test'
  echo 'api::automatic_checkpoint::tests::relational_live_overlay::large_commit::interrupted::owned: test'
  if [[ "${PARTITION_PROBE_OVERLAP:-0}" == 1 ]]; then
    echo 'analytics::overlap: test'
    echo 'api::tests::query_execution::analytics::overlap: test'
  fi
  if [[ "${PARTITION_PROBE_SAME_OWNER:-0}" == 1 ]]; then
    echo 'query_control::owned: test'
    echo 'api::tests::query_execution::query_control::owned: test'
  fi
  if [[ "${PARTITION_PROBE_RSS_OVERLAP:-0}" == 1 ]]; then
    echo 'api::tests::graph_analytics::rss_child_extension: test'
  fi
  if [[ "${PARTITION_PROBE_GROWING_TABLE_OVERLAP:-0}" == 1 ]]; then
    echo 'api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay_extra: test'
  fi
  echo '18 tests, 0 benchmarks'
  exit 0
fi
printf '%s\n' "$@" >"${PARTITION_PROBE_ARGS}"
exit "${PARTITION_PROBE_EXIT:-0}"
EOF
chmod +x "${fixture}/binary"

for partition in general concurrent checkpoint_headroom checkpoint_headroom_growing_table checkpoint_headroom_controls checkpoint_lifecycle checkpoint_large_commit checkpoint_large_commit_interrupted analytics analytics_core analytics_core_general analytics_identity analytics_rss recovery; do
  "${runner}" "${fixture}/binary" "${partition}" custom_filter --exact --skip user_skip
  cp "${PARTITION_PROBE_ARGS}" "${fixture}/args.${partition}"
  python3 - "${PARTITION_PROBE_ARGS}" "${partition}" <<'PY'
import pathlib
import sys

args = pathlib.Path(sys.argv[1]).read_text().splitlines()
headroom = {
    "api::automatic_checkpoint::tests::headroom::pending_idle",
    "api::automatic_checkpoint::tests::headroom::capacity_wait::deadline",
    "api::automatic_checkpoint::tests::delta_pressure_sustains_generations_and_preserves_all_values_after_reopen",
    "api::automatic_checkpoint::tests::soft_wal_pressure_sustains_multiple_generations_without_host_checkpoints",
    "api::automatic_checkpoint::tests::relational_live_overlay::steady_relational_updates_on_one_handle_never_exhaust_the_live_overlay",
    "api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay",
}
growing_table = {
    "api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay",
}
owners = {
    "concurrent": {"api::tests::concurrent_transactions::owned"},
    "analytics_core_general": {"api::tests::graph_analytics::owned"},
    "analytics_rss": {
        "api::tests::graph_analytics::persistent_edges_stream_with_bounded_rss_in_an_isolated_process",
        "api::tests::graph_analytics::rss_child",
    },
    "recovery": {"api::tests::storage_recovery::owned"},
    "general": {"api::tests::query_execution::owned"},
    "checkpoint_lifecycle": {
        "api::automatic_checkpoint::tests::manual::ordinary_manual_during_automatic_preparation_resumes_automatic_progress",
        "api::automatic_checkpoint::tests::idle_handoff::releasing_a_runtime_read_guard_wakes_unattended_handoff",
        "api::automatic_checkpoint::tests::planning_recovery::actual_owner_reports_cached_memory_denial_and_recovers_without_frontend_work",
        "api::automatic_checkpoint::tests::coalesced_capture::coalesced_commits_refresh_and_checkpoint_without_another_frontend_call",
        "api::automatic_checkpoint::tests::new_family::future_lifecycle",
        "api::automatic_checkpoint::tests::future_lifecycle",
    },
    "checkpoint_headroom": headroom,
    "checkpoint_headroom_growing_table": growing_table,
    "checkpoint_headroom_controls": headroom - growing_table,
    "checkpoint_large_commit": {
        "api::automatic_checkpoint::tests::relational_live_overlay::large_commit::owned",
    },
    "checkpoint_large_commit_interrupted": {
        "api::automatic_checkpoint::tests::relational_live_overlay::large_commit::interrupted::owned",
    },
}
all_names = set().union(*owners.values())
core = owners["analytics_core_general"] | owners["analytics_rss"]
if sys.argv[2] == "analytics":
    owned = core | owners["analytics_rss"]
elif sys.argv[2] == "analytics_identity":
    owned = owners["analytics_rss"]
elif sys.argv[2] == "analytics_core":
    owned = core
else:
    owned = owners[sys.argv[2]]
skips = [args[i + 1] for i, value in enumerate(args[:-1]) if value == "--skip"]
assert set(skips) == (all_names - owned) | {"user_skip"}, (sys.argv[2], set(skips), owned)
assert args[-4:] == ["custom_filter", "--exact", "--skip", "user_skip"]
PY
done

env TEST_TOTAL_SHARDS=1 TEST_SHARD_INDEX=0 \
  TEST_SHARD_STATUS_FILE="${fixture}/status.headroom.controls" \
  "${runner}" "${fixture}/binary" checkpoint_headroom_controls custom_filter --exact --skip user_skip
[[ -f "${fixture}/status.headroom.controls" ]]

python3 - "${fixture}" <<'PY'
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
names = {
    "api::automatic_checkpoint::tests::headroom::pending_idle",
    "api::automatic_checkpoint::tests::headroom::capacity_wait::deadline",
    "api::automatic_checkpoint::tests::delta_pressure_sustains_generations_and_preserves_all_values_after_reopen",
    "api::automatic_checkpoint::tests::soft_wal_pressure_sustains_multiple_generations_without_host_checkpoints",
    "api::automatic_checkpoint::tests::relational_live_overlay::steady_relational_updates_on_one_handle_never_exhaust_the_live_overlay",
    "api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay",
}
selections = []
for partition in ("checkpoint_headroom_growing_table", "checkpoint_headroom_controls"):
    args = (root / f"args.{partition}").read_text().splitlines()
    skips = {args[i + 1] for i, value in enumerate(args[:-1]) if value == "--skip"}
    selections.append(names - skips)
    assert args[-4:] == ["custom_filter", "--exact", "--skip", "user_skip"]
assert selections[0] == {
    "api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay"
}
assert selections[1] == names - selections[0]
assert not selections[0] & selections[1]
assert selections[0] | selections[1] == names
PY

python3 - "${fixture}" <<'PY'
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
names = {
    "api::tests::concurrent_transactions::owned",
    "api::tests::graph_analytics::owned",
    "api::tests::graph_analytics::persistent_edges_stream_with_bounded_rss_in_an_isolated_process",
    "api::tests::graph_analytics::rss_child",
    "api::tests::storage_recovery::owned",
    "api::tests::query_execution::owned",
    "api::automatic_checkpoint::tests::manual::ordinary_manual_during_automatic_preparation_resumes_automatic_progress",
    "api::automatic_checkpoint::tests::idle_handoff::releasing_a_runtime_read_guard_wakes_unattended_handoff",
    "api::automatic_checkpoint::tests::planning_recovery::actual_owner_reports_cached_memory_denial_and_recovers_without_frontend_work",
    "api::automatic_checkpoint::tests::coalesced_capture::coalesced_commits_refresh_and_checkpoint_without_another_frontend_call",
    "api::automatic_checkpoint::tests::new_family::future_lifecycle",
    "api::automatic_checkpoint::tests::future_lifecycle",
    "api::automatic_checkpoint::tests::headroom::pending_idle",
    "api::automatic_checkpoint::tests::headroom::capacity_wait::deadline",
    "api::automatic_checkpoint::tests::delta_pressure_sustains_generations_and_preserves_all_values_after_reopen",
    "api::automatic_checkpoint::tests::soft_wal_pressure_sustains_multiple_generations_without_host_checkpoints",
    "api::automatic_checkpoint::tests::relational_live_overlay::steady_relational_updates_on_one_handle_never_exhaust_the_live_overlay",
    "api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay",
    "api::automatic_checkpoint::tests::relational_live_overlay::large_commit::owned",
    "api::automatic_checkpoint::tests::relational_live_overlay::large_commit::interrupted::owned",
}

def selected(partition):
    args = (root / f"args.{partition}").read_text().splitlines()
    skips = {args[i + 1] for i, arg in enumerate(args[:-1]) if arg == "--skip"}
    return names - skips

leaves = [selected(name) for name in [
    "general", "concurrent", "checkpoint_headroom_growing_table", "checkpoint_headroom_controls", "checkpoint_lifecycle", "checkpoint_large_commit", "checkpoint_large_commit_interrupted", "analytics_core_general", "analytics_rss", "recovery"
]]
assert set().union(*leaves) == names
assert sum(map(len, leaves)) == len(names)
assert selected("analytics_core") == selected("analytics_core_general") | selected("analytics_identity")
assert selected("analytics") == selected("analytics_core") | selected("analytics_rss")
PY

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
for partition in general checkpoint_headroom checkpoint_lifecycle; do
  expect_exit 42 env PARTITION_PROBE_EXIT=42 "${runner}" "${fixture}/binary" "${partition}"
done
expect_exit 1 env PARTITION_PROBE_EMPTY_GROWING_TABLE=1 "${runner}" "${fixture}/binary" checkpoint_headroom_growing_table
expect_exit 1 env PARTITION_PROBE_ONLY_GROWING_TABLE=1 "${runner}" "${fixture}/binary" checkpoint_headroom_controls
expect_exit 1 env PARTITION_PROBE_GROWING_TABLE_OVERLAP=1 "${runner}" "${fixture}/binary" checkpoint_headroom_controls
for coordinates in '0 0' '3 0' '2 0' '2 2' '2 -1' 'invalid 0' '2 invalid' '1 1'; do
  read -r total_shards shard_index <<<"${coordinates}"
  expect_exit 64 env TEST_TOTAL_SHARDS="${total_shards}" TEST_SHARD_INDEX="${shard_index}" "${runner}" "${fixture}/binary" checkpoint_headroom
done
expect_exit 64 env TEST_TOTAL_SHARDS=2 TEST_SHARD_INDEX=0 "${runner}" "${fixture}/binary" checkpoint_headroom_controls
expect_exit 1 env PARTITION_PROBE_EMPTY_HEADROOM=1 "${runner}" "${fixture}/binary" checkpoint_headroom
expect_exit 1 env PARTITION_PROBE_EMPTY_HEADROOM=1 "${runner}" "${fixture}/binary" checkpoint_headroom_controls
expect_exit 1 env PARTITION_PROBE_EMPTY_LIFECYCLE=1 "${runner}" "${fixture}/binary" checkpoint_lifecycle
expect_exit 17 env PARTITION_PROBE_DISCOVERY_EXIT=17 "${runner}" "${fixture}/binary" general
expect_exit 1 env PARTITION_PROBE_EMPTY=1 "${runner}" "${fixture}/binary" concurrent
expect_exit 1 env PARTITION_PROBE_EMPTY_CORE=1 "${runner}" "${fixture}/binary" analytics_core_general
expect_exit 1 env PARTITION_PROBE_EMPTY_RSS=1 "${runner}" "${fixture}/binary" analytics_identity
expect_exit 1 env PARTITION_PROBE_EMPTY_CORE=1 PARTITION_PROBE_EMPTY_RSS=1 "${runner}" "${fixture}/binary" analytics_core
expect_exit 1 env PARTITION_PROBE_EMPTY_RSS=1 "${runner}" "${fixture}/binary" analytics_rss
expect_exit 64 "${runner}" "${fixture}/binary" unknown
rm -f "${PARTITION_PROBE_ARGS}"
expect_exit 1 env PARTITION_PROBE_OVERLAP=1 "${runner}" "${fixture}/binary" general
[[ "$(<"${fixture}/error")" == *"root unit partition name overlap"* ]]
[[ ! -e "${PARTITION_PROBE_ARGS}" ]]
expect_exit 1 env PARTITION_PROBE_RSS_OVERLAP=1 "${runner}" "${fixture}/binary" analytics_core_general
[[ "$(<"${fixture}/error")" == *"root unit partition name overlap"* ]]
env PARTITION_PROBE_SAME_OWNER=1 "${runner}" "${fixture}/binary" general
[[ "$(<"${PARTITION_PROBE_ARGS}")" != *"query_control::owned"* ]]
echo "root unit partition ownership, caller arguments and failure propagation passed"
