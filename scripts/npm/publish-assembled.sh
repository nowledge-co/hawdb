#!/usr/bin/env bash
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

# Publish npm packages assembled by bindings/bun/scripts/assemble-npm.ts.
#
#   publish-assembled.sh <summary.json> --tag <latest|dev|nightly> [--publish] [--provenance] [--placeholders]
#
# Every library and main package is published under the release's dist-tag:
# `latest` takes only a stable <x.y.z> version, and `dev` or `nightly` only
# that channel's prerelease version (<x.y.z>-dev.<n> or -nightly.<n>); any
# other tag or combination is refused. Without --publish this is a dry run
# (`npm publish --dry-run`). Packages are published in the summary's order:
# platform packages before the main package that pins them. A version already
# on the registry is skipped when its tarball shasum equals the local pack, and
# refused otherwise, so a retried release never needs to republish (npm
# versions are immutable).
# --placeholders also publishes the name-reservation packages under the
# `placeholder` dist-tag; use it only for the one-time bootstrap.

set -euo pipefail

usage="usage: publish-assembled.sh <summary.json> --tag <latest|dev|nightly> [--publish] [--provenance] [--placeholders]"
summary="${1:?$usage}"
shift
publish=false
provenance=false
placeholders=false
tag=""
while (( $# > 0 )); do
  case "$1" in
    --publish) publish=true ;;
    --provenance) provenance=true ;;
    --placeholders) placeholders=true ;;
    --tag)
      tag="${2:-}"
      shift
      ;;
    *) echo "unknown argument: $1" >&2; exit 64 ;;
  esac
  shift
done
case "$tag" in
  latest | dev | nightly) ;;
  *) echo "--tag must be latest, dev or nightly, not '$tag'" >&2; exit 64 ;;
esac

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
  elif [[ "$tag" == latest && ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::$name@$version is not a stable version; refusing to publish it under latest" >&2
    exit 1
  elif [[ "$tag" != latest && "$version" != *-"$tag".* ]]; then
    echo "::error::$name@$version is not a $tag prerelease; refusing to publish it" >&2
    exit 1
  else
    command+=(--tag "$tag")
  fi
  if [[ "$provenance" == true ]]; then
    command+=(--provenance)
  fi
  if [[ "$publish" != true ]]; then
    command+=(--dry-run)
  fi
  echo "+ ${command[*]}"
  if [[ "$publish" == true ]] && { : </dev/tty; } 2>/dev/null; then
    # This loop's stdin is the package list: npm must never read it. A local
    # publish gives npm the terminal, where it waits for the 2FA confirmation
    # (reading the list instead, it fails with EOTP); without a terminal (CI)
    # it gets no input at all.
    "${command[@]}" </dev/tty
  else
    "${command[@]}" </dev/null
  fi
done < <(entries)
