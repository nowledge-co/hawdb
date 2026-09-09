#!/usr/bin/env bash

set -euo pipefail

if [[ -n "${TEST_SRCDIR:-}" && -n "${TEST_WORKSPACE:-}" ]]; then
  checker="${TEST_SRCDIR}/${TEST_WORKSPACE}/scripts/check-cargo-bazel-package-parity.sh"
else
  checker="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/check-cargo-bazel-package-parity.sh"
fi
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
_CARGO_WORKSPACE_PACKAGES = [
    "crates/member",
]

_SKEIN_PRESUBMIT_CRATE_EXCLUSIONS = {}

rust_test(
    name = "root_tests",
)

test_suite(
    name = "skein_presubmit_crate_tests",
    tests = [
        "//%s:presubmit_tests" % package
        for package in _CARGO_WORKSPACE_PACKAGES
        if package not in _SKEIN_PRESUBMIT_CRATE_EXCLUSIONS
    ],
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

test_suite(
    name = "presubmit_tests",
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

missing_suite="${fixture_root}/missing-suite"
make_fixture "${missing_suite}"
cat >"${missing_suite}/crates/member/BUILD.bazel" <<'EOF'
rust_test(
    name = "member_tests",
)
EOF
if "${checker}" "${missing_suite}" >"${fixture_root}/missing-suite.out" 2>&1; then
  echo "expected a workspace package without a presubmit suite to fail" >&2
  exit 1
fi
grep -q 'missing crate presubmit suite: //crates/member:presubmit_tests' \
  "${fixture_root}/missing-suite.out"

missing_registration="${fixture_root}/missing-registration"
make_fixture "${missing_registration}"
cat >"${missing_registration}/BUILD.bazel" <<'EOF'
_CARGO_WORKSPACE_PACKAGES = []
_SKEIN_PRESUBMIT_CRATE_EXCLUSIONS = {}

rust_test(
    name = "root_tests",
)

test_suite(
    name = "skein_presubmit_crate_tests",
)
EOF
if "${checker}" "${missing_registration}" >"${fixture_root}/missing-registration.out" 2>&1; then
  echo "expected an unregistered workspace package to fail" >&2
  exit 1
fi
grep -q 'Cargo workspace package is missing from _CARGO_WORKSPACE_PACKAGES: crates/member' \
  "${fixture_root}/missing-registration.out"

excluded="${fixture_root}/excluded"
make_fixture "${excluded}"
cat >"${excluded}/BUILD.bazel" <<'EOF'
_CARGO_WORKSPACE_PACKAGES = [
    "crates/member",
]

_SKEIN_PRESUBMIT_CRATE_EXCLUSIONS = {
    "crates/member": ("local-only", "//crates/member:member_manual_tests"),
}

rust_test(
    name = "root_tests",
)

test_suite(
    name = "skein_presubmit_crate_tests",
)
EOF
cat >"${excluded}/crates/member/BUILD.bazel" <<'EOF'
rust_test(
    name = "member_manual_tests",
    tags = ["manual"],
)
EOF
"${checker}" "${excluded}"

missing_exclusion_target="${fixture_root}/missing-exclusion-target"
make_fixture "${missing_exclusion_target}"
cat >"${missing_exclusion_target}/BUILD.bazel" <<'EOF'
_CARGO_WORKSPACE_PACKAGES = [
    "crates/member",
]

_SKEIN_PRESUBMIT_CRATE_EXCLUSIONS = {
    "crates/member": ("periodic", "//crates/member:member_periodic_tests"),
}

rust_test(
    name = "root_tests",
)

test_suite(
    name = "skein_presubmit_crate_tests",
)
EOF
if "${checker}" "${missing_exclusion_target}" >"${fixture_root}/missing-exclusion-target.out" 2>&1; then
  echo "expected a missing exclusion target to fail" >&2
  exit 1
fi
grep -q 'crate presubmit exclusion target does not exist: //crates/member:member_periodic_tests' \
  "${fixture_root}/missing-exclusion-target.out"
