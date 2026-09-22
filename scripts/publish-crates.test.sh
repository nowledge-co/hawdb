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

if [[ -n "${TEST_SRCDIR:-}" && -n "${TEST_WORKSPACE:-}" ]]; then
  script="${TEST_SRCDIR}/${TEST_WORKSPACE}/scripts/publish-crates.sh"
else
  script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/publish-crates.sh"
fi
fixture_root="${TEST_TMPDIR:-$(mktemp -d)}/publish-crates"

# Builds a small diamond-shaped workspace:
#   leaf <- mid <- top
#   leaf <- excluded (publish = false) <-(dev-dependency)- top
# so the ordering test exercises a transitive chain, a dev-dependency edge,
# and the publish = false exclusion in one fixture.
make_fixture() {
  local root="$1"
  mkdir -p "${root}/crates/leaf" "${root}/crates/mid" "${root}/crates/top" "${root}/crates/excluded"

  cat >"${root}/Cargo.toml" <<'EOF'
[workspace]
members = [
  "crates/leaf",
  "crates/mid",
  "crates/top",
  "crates/excluded",
]
resolver = "2"
EOF

  cat >"${root}/crates/leaf/Cargo.toml" <<'EOF'
[package]
name = "leaf"
version = "0.1.0"
edition = "2021"
EOF
  mkdir -p "${root}/crates/leaf/src"
  echo "" >"${root}/crates/leaf/src/lib.rs"

  cat >"${root}/crates/mid/Cargo.toml" <<'EOF'
[package]
name = "mid"
version = "0.1.0"
edition = "2021"

[dependencies]
leaf = { path = "../leaf", version = "0.1.0" }
EOF
  mkdir -p "${root}/crates/mid/src"
  echo "" >"${root}/crates/mid/src/lib.rs"

  cat >"${root}/crates/excluded/Cargo.toml" <<'EOF'
[package]
name = "excluded"
version = "0.1.0"
edition = "2021"
publish = false

[dependencies]
leaf = { path = "../leaf", version = "0.1.0" }
EOF
  mkdir -p "${root}/crates/excluded/src"
  echo "" >"${root}/crates/excluded/src/lib.rs"

  cat >"${root}/crates/top/Cargo.toml" <<'EOF'
[package]
name = "top"
version = "0.1.0"
edition = "2021"

[dependencies]
mid = { path = "../mid", version = "0.1.0" }

[dev-dependencies]
excluded = { path = "../excluded", version = "0.1.0" }
EOF
  mkdir -p "${root}/crates/top/src"
  echo "" >"${root}/crates/top/src/lib.rs"
}

fixture="${fixture_root}/diamond"
make_fixture "${fixture}"

order_out="${fixture_root}/order.out"
"${script}" --print-order "${fixture}" >"${order_out}"

# `excluded` sets publish = false and must never appear in the output.
if grep -qx "excluded" "${order_out}"; then
  echo "expected publish = false crate to be excluded from the order" >&2
  cat "${order_out}" >&2
  exit 1
fi

# Exactly the three publishable crates, each once.
line_count="$(wc -l <"${order_out}" | tr -d ' ')"
if [[ "${line_count}" != "3" ]]; then
  echo "expected 3 crates in the publish order, got ${line_count}" >&2
  cat "${order_out}" >&2
  exit 1
fi

leaf_pos="$(grep -nx "leaf" "${order_out}" | cut -d: -f1)"
mid_pos="$(grep -nx "mid" "${order_out}" | cut -d: -f1)"
top_pos="$(grep -nx "top" "${order_out}" | cut -d: -f1)"

if [[ -z "${leaf_pos}" || -z "${mid_pos}" || -z "${top_pos}" ]]; then
  echo "expected leaf, mid, and top to each appear exactly once" >&2
  cat "${order_out}" >&2
  exit 1
fi

if (( leaf_pos >= mid_pos )); then
  echo "expected leaf before mid (its dependent): got leaf=${leaf_pos} mid=${mid_pos}" >&2
  exit 1
fi

if (( mid_pos >= top_pos )); then
  echo "expected mid before top (its dependent): got mid=${mid_pos} top=${top_pos}" >&2
  exit 1
fi

echo "publish-crates.sh --print-order: leaf-first ordering verified" >&2
