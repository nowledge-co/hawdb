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
#
# HawDB is a workspace of ~35 crates.io packages joined by path
# dependencies (crates/*/Cargo.toml -> the root `hawdb` package). Publishing
# any one of them requires every path dependency it references to already
# exist on crates.io at the matching version, because `cargo publish`
# re-resolves the packaged manifest against the registry rather than the
# local workspace. `cargo publish` on the root package alone therefore
# always fails with "no matching package named `hawdb-<x>` found" the first
# time a version is released. This script publishes the workspace in a
# dependency-safe (leaf-first) order.
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: publish-crates.sh --print-order [workspace-root]
       publish-crates.sh [--from CRATE] [--dry-run] [-- <cargo publish args>...]

  --print-order    Print the crates.io publish order (leaf-first), one crate
                    per line, and exit. Reads the local workspace only via
                    `cargo metadata --offline`; makes no network calls.
  --from CRATE      Resume a previous run: skip every crate before CRATE in
                     the computed order.
  --dry-run         Pass --dry-run through to every `cargo publish` call.
  --                Remaining args are forwarded to every `cargo publish`
                    call verbatim (e.g. --token, --registry).

Crates whose Cargo.toml sets `publish = false` are skipped entirely. If
`cargo publish` reports a crate as already uploaded (e.g. because this run
is resuming after a partial failure), the script logs it and continues
instead of aborting.
EOF
}

resolve_workspace_root() {
  local override="${1:-}"
  if [[ -n "${override}" ]]; then
    echo "${override}"
  elif [[ -n "${TEST_SRCDIR:-}" && -n "${TEST_WORKSPACE:-}" ]]; then
    echo "${TEST_SRCDIR}/${TEST_WORKSPACE}"
  else
    (cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
  fi
}

# Reads `cargo metadata` for workspace_root and prints the leaf-first
# publish order of every workspace member that does not set `publish =
# false`. A crate only depends (for this purpose) on the workspace members
# reachable through any dependency table (normal, dev, or build), since
# `cargo package` resolves the full manifest, not just the library target.
publish_order() {
  local workspace_root="$1"
  (
    cd "${workspace_root}"
    cargo metadata --offline --format-version 1
  ) | jq -r '
    def toposort($graph):
      ($graph | keys) as $nodes
      | reduce range(0; ($nodes | length)) as $i (
          {order: [], remaining: $nodes, error: null};
          . as $state
          | if ($state.remaining | length) == 0 then $state
            else
              ($state.remaining
                | map(select(($graph[.] // []) - $state.order == []))
                | sort) as $ready
              | if ($ready | length) == 0 then
                  ($state + {error: ("cycle detected among: " + ($state.remaining | join(", ")))})
                else
                  {
                    order: ($state.order + $ready),
                    remaining: ($state.remaining - $ready),
                    error: $state.error,
                  }
                end
            end
        );

    . as $meta
    | ($meta.workspace_members) as $wsids
    | ($meta.packages | map(select(.id as $id | $wsids | index($id)))) as $wspkgs
    | ($wspkgs | map({key: .id, value: .name}) | from_entries) as $idToName
    | ($wspkgs | map({key: .name, value: ((.publish // null) != [])}) | from_entries) as $publishable
    | ($meta.resolve.nodes | map(select(.id as $id | $wsids | index($id)))) as $wsnodes
    | (reduce $wsnodes[] as $n ({}; . + {
          ($idToName[$n.id]): (
            [$n.deps[] | select(.pkg as $d | $wsids | index($d)) | $idToName[.pkg]] | unique
          )
        })) as $graph
    | toposort($graph) as $result
    | if ($result.error // null) != null then
        ($result.error | halt_error)
      else
        ($result.order | map(select($publishable[.])) | .[])
      end
  '
}

main() {
  local workspace_root
  local print_only=0
  local from_crate=""
  local -a cargo_extra_args=()
  local dry_run=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
    --print-order)
      print_only=1
      shift
      ;;
    --from)
      from_crate="${2:?--from requires a crate name}"
      shift 2
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    --)
      shift
      cargo_extra_args=("$@")
      break
      ;;
    *)
      if [[ "${print_only}" -eq 1 && -z "${workspace_root:-}" ]]; then
        workspace_root="$1"
        shift
      else
        echo "unrecognized argument: $1" >&2
        usage
        exit 2
      fi
      ;;
    esac
  done

  workspace_root="$(resolve_workspace_root "${workspace_root:-}")"

  if [[ "${print_only}" -eq 1 ]]; then
    publish_order "${workspace_root}"
    return 0
  fi

  if [[ "${dry_run}" -eq 1 ]]; then
    cargo_extra_args=("--dry-run" "${cargo_extra_args[@]}")
  fi

  local -a order=()
  while IFS= read -r crate; do
    order+=("${crate}")
  done < <(publish_order "${workspace_root}")

  local skipping=0
  if [[ -n "${from_crate}" ]]; then
    skipping=1
  fi

  local crate
  for crate in "${order[@]}"; do
    if [[ "${skipping}" -eq 1 ]]; then
      if [[ "${crate}" == "${from_crate}" ]]; then
        skipping=0
      else
        echo "skip (before --from ${from_crate}): ${crate}" >&2
        continue
      fi
    fi

    echo "== publishing ${crate} ==" >&2
    local publish_log
    publish_log="$(mktemp)"
    if (cd "${workspace_root}" && cargo publish -p "${crate}" "${cargo_extra_args[@]}") >"${publish_log}" 2>&1; then
      cat "${publish_log}" >&2
      rm -f "${publish_log}"
      continue
    fi

    cat "${publish_log}" >&2
    if grep -qiE "already (uploaded|exists)" "${publish_log}"; then
      echo "== ${crate} already published at this version, continuing ==" >&2
      rm -f "${publish_log}"
      continue
    fi

    rm -f "${publish_log}"
    echo "== publish failed for ${crate}; rerun with --from ${crate} after fixing the issue ==" >&2
    exit 1
  done
}

main "$@"
