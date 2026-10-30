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
shard="${1:?usage: run-windows-native-bazel.sh <shard>}"
BAZEL="${BAZEL:-bazel}"
results="${WINDOWS_NATIVE_RESULTS:-windows-native-results}"
mkdir -p "$results"
results="$(cd "$results" && pwd -P)"
# Git Bash must not turn //pkg:target into a Windows/UNC pathname.
export MSYS2_ARG_CONV_EXCL='*'

native_path() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s\n' "$1"; fi
}

preserve_test_outputs() {
  local status=$?
  trap - EXIT
  set +e
  local testlogs
  testlogs="$("$BAZEL" info bazel-testlogs 2>"$results/testlogs-location-error.txt")"
  if [[ -d "$testlogs" ]]; then
    while IFS= read -r -d '' file; do
      relative="${file#"$testlogs"/}"
      mkdir -p "$results/testlogs/$(dirname "$relative")"
      cp "$file" "$results/testlogs/$relative"
    done < <(find "$testlogs" -type f \( -name test.log -o -name test.xml \) -print0)
  fi
  exit "$status"
}
trap preserve_test_outputs EXIT

copy_selection_outputs() {
  local name="$1" target="$2" profile="$3" testlogs relative
  testlogs="$("$BAZEL" info "--//:windows_native_profile=$profile" bazel-testlogs 2>>"$results/testlogs-location-error.txt")" || return 0
  relative="${target#//}"
  relative="${relative/:/\/}"
  relative="${relative#/}"
  mkdir -p "$results/testlogs/$name"
  for artifact in test.log test.xml; do
    if [[ -f "$testlogs/$relative/$artifact" ]]; then
      cp "$testlogs/$relative/$artifact" "$results/testlogs/$name/$artifact"
    fi
  done
}

rust_case() {
  local name="$1" target="$2" profile="$3" expected="$4" threads="$5"
  shift 5
  local command=("$BAZEL" test --nocache_test_results --test_output=all --test_timeout=7200
    "--build_event_json_file=$(native_path "$results/$name.bep.json")")
  command+=("--//:windows_native_profile=$profile")
  for argument in "$@" --nocapture; do command+=("--test_arg=$argument"); done
  if [[ "$threads" != default ]]; then command+=("--test_arg=--test-threads=$threads"); fi
  command+=("$target")
  printf '%q ' "${command[@]}" >"$results/$name.command.txt"
  printf '\n' >>"$results/$name.command.txt"
  local status=0
  "${command[@]}" 2>&1 | tee "$results/$name.txt" || status=$?
  copy_selection_outputs "$name" "$target" "$profile"
  if [[ "$status" != 0 ]]; then return "$status"; fi
  test -s "$results/$name.bep.json"
  if [[ "$expected" == positive ]]; then
    grep -Eq '^test result: ok\. [1-9][0-9]* passed;' "$results/$name.txt"
  else
    grep -Eq "^test result: ok\\. $expected passed; 0 failed; 0 ignored;" "$results/$name.txt"
  fi
}

root=//:hawdb_unit_test_binary
storage=//crates/storage:hawdb_storage_tests
search=//crates/search:hawdb_search_tests
recovery_selector="$(native_path "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/vector_recovery_cases.py")"
case "$shard" in
  transactions)
    rust_case serializable-transactions "$root" facade_all positive 1 ::serializable::
    ;;
  storage)
    rust_case wal-default-recovery "$storage" storage_all positive 1 recovery_995
    rust_case wal-tail-facade "$root" facade_all positive 1 api::tests::storage_recovery::wal_tail::
    rust_case descriptor-capacity "$storage" storage_all positive 1 file_descriptors::tests::
    rust_case descriptor-facade "$root" facade_all positive 1 api::tests::descriptor_capacity::
    rust_case consumer-wal-admission "$storage" storage_all positive 1 captured_wal_append_prepays_post_write_descriptor_checks
    rust_case consumer-failure-boundaries "$storage" storage_all positive 1 consumer_allocation::bootstrap_tests file_io::source_directory::control_candidate::tests held_wal_rollback captured_wal_first_rollback captured_link native_directory_component
    ;;
  search)
    rust_case resident-diskann-cache "$search" search_all positive 1 resident_diskann::tests::
    rust_case held-generation-spool "$search" search_all positive 1 generation_spool
    rust_case captured-publication-capacity "$search" search_all positive 1 out_of_core::publish_lease::tests::captured_lease
    rust_case consumer-publication-refusal "$search" search_all 1 1 consumer::tests::windows_publication_refusal_preserves_receipt_and_native_lease --exact
    ;;
  consumers)
    publication_prefix="$(python3 "$recovery_selector" --publication-prefix)"
    rust_case consumer-reclamation "$root" facade_all positive 1 consumer_reclamation
    rust_case consumer-vector-incremental "$root" facade_all positive 1 vector_index_incremental vector_index_canonical_completion --skip "$publication_prefix"
    rust_case consumer-stationary-create "$root" facade_all 1 1 api::search_projection_consumer::tests::windows_registered_stationary_creation_catch_up_and_reopen --exact
    rust_case consumer-stationary-recovery "$root" facade_all positive 1 registered_stationary
    rust_case consumer-covered-catch-up //:hawdb_mvcc_staging_session_tests facade_all positive 1 registered_consumer
    ;;
  consumers-publication-*)
    publication_shard="${shard#consumers-}"
    publication_filter="$(python3 "$recovery_selector" "$publication_shard")"
    rust_case "$publication_shard" "$root" facade_all 1 1 "$publication_filter" --exact
    ;;
  consumers-recovery-*)
    recovery_shard="${shard#consumers-}"
    recovery_filter="$(python3 "$recovery_selector" "$recovery_shard")"
    rust_case "$recovery_shard" "$root" facade_all 1 1 "$recovery_filter" --exact
    python3 "$recovery_selector" "$recovery_shard" --validate-log "$(native_path "$results/$recovery_shard.txt")" >"$results/$recovery_shard.coverage.json"
    ;;
  identities)
    rust_case graph-identity-facade "$root" facade_default positive 1 identity_898_
    rust_case graph-identity-executor //crates/executor:hawdb_executor_unit_tests component_default positive 1 identity_898_
    rust_case graph-identity-cypher //crates/cypher:hawdb_cypher_tests component_default positive 1 identity_898_
    rust_case legacy-epoch-fences "$root" facade_default positive 1 _1043_
    ;;
  snapshots)
    rust_case instance-identity-default-in_memory_portable //:hawdb_in_memory_portable_default_tests facade_default 1 default database_instance_identity_
    rust_case instance-identity-minimal-in_memory_portable //:hawdb_in_memory_portable_tests facade_minimal 1 default database_instance_identity_
    rust_case instance-identity-default-branch_project_open //:hawdb_branch_project_open_tests facade_default 2 default database_instance_identity_
    rust_case instance-identity-minimal-branch_project_open //:hawdb_branch_project_open_minimal_tests facade_minimal 2 default database_instance_identity_
    rust_case instance-identity-default-views "$root" facade_default 3 default database_instance_identity_
    rust_case instance-identity-minimal-views //:hawdb_minimal_unit_test_binary facade_minimal 3 default database_instance_identity_
    ;;
  bindings)
    rust_case retained-ffi //bindings/ffi:hawdb_ffi_tests bindings positive default retained::
    rust_case retained-node "$root" facade_default positive default api::retained_query::node::tests
    rust_case retained-arrow "$root" facade_all positive default api::retained_query::tests::arrow
    "$BAZEL" test --nocache_test_results --test_output=all --test_timeout=600 \
      "--build_event_json_file=$(native_path "$results/go.bep.json")" \
      --//:windows_native_profile=bindings --test_arg=-test.v --test_arg=-test.count=1 //bindings/go:hawdb_go_tests 2>&1 | tee "$results/go.txt"
    test -s "$results/go.bep.json"
    grep -Eq '^--- PASS:' "$results/go.txt"
    ;;
  abandonment)
    threads="${POWER_LOSS_TEST_THREADS:-4}"
    case "$threads" in 1|2|4|8) ;; *) echo 'invalid power-loss test threads' >&2; exit 64 ;; esac
    printf 'power_loss_test_threads=%s\n' "$threads" | tee "$results/abandonment-config.txt"
    rust_case abandonment "$root" facade_all 8 "$threads" api::tests::power_loss::branch_abandonment::
    ;;
  *) echo "unknown Windows native shard: $shard" >&2; exit 64 ;;
esac
