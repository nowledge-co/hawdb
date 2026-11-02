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
