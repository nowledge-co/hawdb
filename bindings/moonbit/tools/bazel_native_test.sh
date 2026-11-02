#!/usr/bin/env bash
# Copyright 2026 Nowledge contributors
# SPDX-License-Identifier: Apache-2.0
#
# Execute real MoonBit Native programs using the Bazel-built adapter.
set -euo pipefail

archive_runfile="${1:?expected Bazel staticlib runfile}"
runfiles="${TEST_SRCDIR:?}/${TEST_WORKSPACE:?}"
archive="${runfiles}/${archive_runfile}"
test -s "$archive"
command -v moon >/dev/null || { echo "MoonBit 'moon' is required on PATH" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required on PATH" >&2; exit 1; }

workspace="${TEST_TMPDIR:?}/hawdb-moonbit-bazel-conformance"
mkdir -p "$workspace/bindings"
# Dereference runfile symlinks so MoonBit may generate writable outputs without
# modifying the checkout or Bazel's immutable input tree.
cp -RL "$runfiles/bindings/moonbit" "$workspace/bindings/"
sdk="$workspace/bindings/moonbit"
mkdir -p "$sdk/native/target/release"

case "$(uname -s)" in
  Linux)
    cp "$archive" "$sdk/native/target/release/libhawdb_moonbit.a"
    ;;
  Darwin)
    cp "$archive" "$sdk/native/target/release/libhawdb_moonbit.a"
    ;;
  *)
    echo "Bazel MoonBit executable test requires Linux/macOS; Windows has a separate native lane" >&2
    exit 1
    ;;
esac

cd "$sdk"
moon check --target native

if [[ "$(uname -s)" == Darwin ]]; then
  python3 tools/configure_native_link.py --os macos
fi

rm -rf tests/conformance-db
moon run --target native tests/conformance 2>&1 | tee conformance.log
! grep -q '^CASE_FAIL ' conformance.log
grep -Fxq 'HAWDB_MOONBIT_BUSINESS_SUITE_PASS' conformance.log
