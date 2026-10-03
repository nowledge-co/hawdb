#!/usr/bin/env bash
# Copyright 2026 Nowledge
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

bindgen_version="$(awk '/^name = "wasm-bindgen"$/ { found = 1; next } found && /^version = / { gsub(/"/, "", $3); print $3; exit }' Cargo.lock)"
if [[ "$(wasm-bindgen --version)" != "wasm-bindgen ${bindgen_version}" ]]; then
  echo "Install matching bindings: cargo install wasm-bindgen-cli --version ${bindgen_version} --locked" >&2
  exit 1
fi

export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-clang}"
if [[ -z "${AR_wasm32_unknown_unknown:-}" ]]; then
  rust_host="$(rustc -vV | sed -n 's/^host: //p')"
  export AR_wasm32_unknown_unknown="$(rustc --print sysroot)/lib/rustlib/${rust_host}/bin/llvm-ar"
fi
if ! command -v "${AR_wasm32_unknown_unknown}" > /dev/null; then
  echo "Install Rust's LLVM archiver with: rustup component add llvm-tools" >&2
  exit 1
fi

cargo build --locked -p hawdb --no-default-features \
  --target wasm32-unknown-unknown --example wasm_playground
target_dir="$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
wasm-bindgen --target web --out-dir examples/wasm-playground/pkg \
  "${target_dir}/wasm32-unknown-unknown/debug/examples/wasm_playground.wasm"
echo "Serve with: python3 -m http.server 8080 --bind 127.0.0.1 --directory examples/wasm-playground"
