#!/usr/bin/env bash

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

printf '.\n%s\n' "${workspace_members}" | while IFS= read -r package_dir; do
  if [[ ! -f "${package_dir}/Cargo.toml" ]]; then
    echo "missing Cargo.toml for Cargo workspace package: ${package_dir}" >&2
    exit 1
  fi
  if [[ ! -f "${package_dir}/BUILD.bazel" ]]; then
    echo "missing BUILD.bazel for Cargo workspace package: ${package_dir}" >&2
    exit 1
  fi
  if ! grep -Eq '^[[:space:]]*rust_test\(' "${package_dir}/BUILD.bazel"; then
    echo "missing rust_test target for Cargo workspace package: ${package_dir}" >&2
    exit 1
  fi
done
