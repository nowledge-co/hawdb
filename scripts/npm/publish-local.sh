#!/usr/bin/env bash
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

# One-time npm bootstrap from a maintainer's macOS arm64 machine.
#
#   scripts/npm/publish-local.sh <version> <libhawdb_ffi.dylib> [--publish]
#
# npm trusted publishing can only be configured for packages that already
# exist, so the first publish is manual. This assembles and publishes:
#
#   - @nowledge/hawdb-ffi-darwin-arm64@<version> with the given library;
#   - @nowledge/hawdb-bun@<version>, whose optionalDependencies name only that
#     platform package (other platforms load an explicit path at this version);
#   - every other platform package at 0.0.0-placeholder.0 under the
#     `placeholder` dist-tag, containing no library, only to create the name.
#
# Build the library first with the release profile:
#
#   bazel build --config=release //bindings/ffi:hawdb_ffi
#
# Without --publish this is a dry run. With it, the caller must already be
# logged in (`npm login`) as an @nowledge member allowed to publish.

set -euo pipefail

version="${1:?usage: publish-local.sh <version> <libhawdb_ffi.dylib> [--publish]}"
library="${2:?usage: publish-local.sh <version> <libhawdb_ffi.dylib> [--publish]}"
mode="${3:-}"
case "$mode" in
  "" | --publish) ;;
  *) echo "unknown argument: $mode" >&2; exit 64 ;;
esac

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ "$(uname -s)-$(uname -m)" != Darwin-arm64 ]]; then
  echo "publish-local.sh publishes the darwin-arm64 package and must run on macOS arm64" >&2
  exit 1
fi
if [[ "$(otool -D "$library" | tail -n 1)" != "@rpath/libhawdb_ffi.dylib" ]]; then
  # Match the release archives: a relocatable install name and an ad-hoc
  # signature (rewriting the load command invalidates the linker's one).
  staged="$(mktemp -d)/libhawdb_ffi.dylib"
  cp "$library" "$staged"
  chmod u+w "$staged"
  install_name_tool -id @rpath/libhawdb_ffi.dylib "$staged"
  codesign --force --sign - "$staged"
  library="$staged"
fi

out="$root/.scratch/npm-bootstrap"
summary="$out/summary.json"
mkdir -p "$out"
bun "$root/bindings/bun/scripts/assemble-npm.ts" --version "$version" --out "$out" \
  --library "darwin-arm64=$library" --placeholders 0.0.0-placeholder.0 > "$summary"
jq -r '.placeholders[], .publish[] | "\(.name)@\(.version)  \(.directory)"' "$summary"

if [[ "$mode" == --publish ]]; then
  npm whoami > /dev/null
  bash "$root/scripts/npm/publish-assembled.sh" "$summary" --placeholders --publish
else
  bash "$root/scripts/npm/publish-assembled.sh" "$summary" --placeholders
  echo "Dry run complete. Re-run with --publish after npm login to publish."
fi
