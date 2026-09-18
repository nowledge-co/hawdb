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

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [workspace-root]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  workspace_root="$1"
elif [[ -n "${TEST_SRCDIR:-}" && -n "${TEST_WORKSPACE:-}" ]]; then
  workspace_root="${TEST_SRCDIR}/${TEST_WORKSPACE}"
else
  workspace_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
cd "${workspace_root}"

workspace_members="$(
  awk '
    /^\[workspace\]$/ { in_workspace = 1; next }
    in_workspace && /^\[/ { exit }
    in_workspace && /^[[:space:]]*members[[:space:]]*=/ { in_members = 1 }
    in_members {
      line = $0
      sub(/#.*/, "", line)
      while (match(line, /"[^"]+"/)) {
        print substr(line, RSTART + 1, RLENGTH - 2)
        line = substr(line, RSTART + RLENGTH)
      }
      if ($0 ~ /]/) { exit }
    }
  ' Cargo.toml
)"

if [[ -z "${workspace_members}" ]]; then
  echo "Cargo.toml must declare an explicit [workspace].members list" >&2
  exit 1
fi

has_named_test_suite() {
  local build_file="$1"
  local suite_name="$2"
  awk -v suite_name="${suite_name}" '
    /^[[:space:]]*test_suite\(/ {
      in_suite = 1
      found_name = 0
      next
    }
    in_suite && $0 ~ "^[[:space:]]*name[[:space:]]*=[[:space:]]*\"" suite_name "\"" {
      found_name = 1
    }
    in_suite && /^[[:space:]]*\)/ {
      if (found_name) {
        found = 1
        exit
      }
      in_suite = 0
    }
    END { exit(found ? 0 : 1) }
  ' "${build_file}"
}

printf '.\n%s\n' "${workspace_members}" | while IFS= read -r package_dir; do
  if [[ ! -f "${package_dir}/BUILD.bazel" ]]; then
    echo "missing BUILD.bazel for Cargo workspace package: ${package_dir}" >&2
    exit 1
  fi
  if ! grep -Eq '^[[:space:]]*rust_test\(' "${package_dir}/BUILD.bazel"; then
    echo "missing rust_test target for Cargo workspace package: ${package_dir}" >&2
    exit 1
  fi
done

if ! has_named_test_suite BUILD.bazel hawdb_presubmit_crate_tests; then
  echo "missing canonical crate presubmit suite: //:hawdb_presubmit_crate_tests" >&2
  exit 1
fi

printf '%s\n' "${workspace_members}" | while IFS= read -r package_dir; do
  if ! grep -Fqx "    \"${package_dir}\"," BUILD.bazel; then
    echo "Cargo workspace package is missing from _CARGO_WORKSPACE_PACKAGES: ${package_dir}" >&2
    exit 1
  fi

  exclusion_line="$(
    grep -E "^[[:space:]]*\"${package_dir}\":[[:space:]]*\(\"(local-only|manual|periodic)\",[[:space:]]*\"//${package_dir}:[^\"]+\"\),[[:space:]]*$" BUILD.bazel || true
  )"
  if [[ -n "${exclusion_line}" ]]; then
    if [[ "$(printf '%s\n' "${exclusion_line}" | wc -l | tr -d ' ')" != "1" ]]; then
      echo "duplicate crate presubmit exclusions: ${package_dir}" >&2
      exit 1
    fi
    alternate_target="$(printf '%s\n' "${exclusion_line}" | sed -E 's#^.*"//[^:]+:([^\"]+)"\),[[:space:]]*$#\1#')"
    if ! grep -Eq "^[[:space:]]*name[[:space:]]*=[[:space:]]*\"${alternate_target}\"" "${package_dir}/BUILD.bazel"; then
      echo "crate presubmit exclusion target does not exist: //${package_dir}:${alternate_target}" >&2
      exit 1
    fi
    continue
  fi

  if ! has_named_test_suite "${package_dir}/BUILD.bazel" presubmit_tests; then
    echo "missing crate presubmit suite: //${package_dir}:presubmit_tests" >&2
    exit 1
  fi
done
