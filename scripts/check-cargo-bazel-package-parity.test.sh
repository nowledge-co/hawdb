#!/usr/bin/env bash

set -euo pipefail

checker="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/check-cargo-bazel-package-parity.sh"
fixture_root="${TEST_TMPDIR:-$(mktemp -d)}/cargo-bazel-package-parity"

make_fixture() {
  local root="$1"
  mkdir -p "${root}/crates/member"
  cat >"${root}/Cargo.toml" <<'EOF'
[package]
name = "root"

[workspace]
members = [
  "crates/member",
]
EOF
  cat >"${root}/BUILD.bazel" <<'EOF'
rust_test(
    name = "root_tests",
)
EOF
  cat >"${root}/crates/member/Cargo.toml" <<'EOF'
[package]
name = "member"
EOF
  cat >"${root}/crates/member/BUILD.bazel" <<'EOF'
rust_test(
    name = "member_tests",
)
EOF
}

valid="${fixture_root}/valid"
make_fixture "${valid}"
"${checker}" "${valid}"

missing_build="${fixture_root}/missing-build"
make_fixture "${missing_build}"
rm "${missing_build}/crates/member/BUILD.bazel"
if "${checker}" "${missing_build}" >"${fixture_root}/missing-build.out" 2>&1; then
  echo "expected a workspace package without BUILD.bazel to fail" >&2
  exit 1
fi
grep -q 'missing BUILD.bazel for Cargo workspace package: crates/member' \
  "${fixture_root}/missing-build.out"

missing_test="${fixture_root}/missing-test"
make_fixture "${missing_test}"
printf 'filegroup(name = "member_sources")\n' >"${missing_test}/crates/member/BUILD.bazel"
if "${checker}" "${missing_test}" >"${fixture_root}/missing-test.out" 2>&1; then
  echo "expected a workspace package without rust_test to fail" >&2
  exit 1
fi
grep -q 'missing rust_test target for Cargo workspace package: crates/member' \
  "${fixture_root}/missing-test.out"
