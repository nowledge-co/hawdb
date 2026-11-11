#!/usr/bin/env bash
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

# Publish npm packages assembled by bindings/bun/scripts/assemble-npm.ts.
#
#   publish-assembled.sh <summary.json> [--publish] [--provenance] [--placeholders]
#
# Without --publish this is a dry run (`npm publish --dry-run`). Packages are
# published in the summary's order: platform packages before the main package
# that pins them. A version already on the registry is skipped when its
# tarball shasum equals the local pack, and refused otherwise, so a retried
# release never needs to republish (npm versions are immutable).
# --placeholders also publishes the name-reservation packages under the
# `placeholder` dist-tag; use it only for the one-time bootstrap.

set -euo pipefail

summary="${1:?usage: publish-assembled.sh <summary.json> [--publish] [--provenance] [--placeholders]}"
shift
publish=false
provenance=false
placeholders=false
for argument in "$@"; do
  case "$argument" in
    --publish) publish=true ;;
    --provenance) provenance=true ;;
    --placeholders) placeholders=true ;;
    *) echo "unknown argument: $argument" >&2; exit 64 ;;
  esac
done

entries() {
  local fields='.name + "\t" + .version + "\t" + .directory + "\t" + .kind'
  if [[ "$placeholders" == true ]]; then
    jq -r ".placeholders[], .publish[] | $fields" "$summary"
  else
    jq -r ".publish[] | $fields" "$summary"
  fi
}

while IFS=$'\t' read -r name version directory kind; do
  local_shasum="$(cd "$directory" && npm pack --dry-run --json | jq -r '.[0].shasum')"
  remote_shasum="$(npm view "$name@$version" dist.shasum 2>/dev/null || true)"
  if [[ -n "$remote_shasum" ]]; then
    if [[ "$remote_shasum" != "$local_shasum" ]]; then
      echo "::error::$name@$version is already published with shasum $remote_shasum, not $local_shasum" >&2
      exit 1
    fi
    echo "$name@$version is already published with identical contents"
    continue
  fi
  command=(npm publish "$directory" --access public)
  if [[ "$kind" == placeholder ]]; then
    command+=(--tag placeholder)
  fi
  if [[ "$provenance" == true ]]; then
    command+=(--provenance)
  fi
  if [[ "$publish" != true ]]; then
    command+=(--dry-run)
  fi
  echo "+ ${command[*]}"
  "${command[@]}"
done < <(entries)
