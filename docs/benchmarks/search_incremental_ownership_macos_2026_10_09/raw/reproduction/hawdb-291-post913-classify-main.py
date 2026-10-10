"""Classify exact subsequent deltas without rebinding measured binaries."""
import hashlib
import json
import re
import subprocess
from pathlib import Path

QUALIFIED = '41e10dacf8b2589e416c01b9b047db98dfe01acd'
RELEASE = '1519433bb70900bf3f51f5a3ed93de0873072713'
ANNOUNCE_RC = 'be564913c50f8cb3b70e76a44994c14a1b4aed01'
PYTHON_PARAMETERS = 'fb1f835fedc450602c0364e1f2866de3bc190a18'
PYTHON_PARAMETER_PATHS = set('''bindings/python/README.md
bindings/python/python/hawdb/pydantic.py
bindings/python/src/database.rs
bindings/python/src/value.rs
bindings/python/tests/test_params.py
bindings/python/tests/test_pydantic.py'''.splitlines())
RELEASE_PATHS = set('''Cargo.lock
Cargo.toml
MODULE.bazel
README.md
bindings/ffi/BUILD.bazel
bindings/ffi/Cargo.lock
bindings/ffi/Cargo.toml
bindings/python/BUILD.bazel
bindings/python/Cargo.toml
crates/analytics/Cargo.toml
crates/artifact/Cargo.toml
crates/bootstrap/Cargo.toml
crates/compat/Cargo.toml
crates/cypher/Cargo.toml
crates/ddl/Cargo.toml
crates/evidence/Cargo.toml
crates/executor/Cargo.toml
crates/explain/Cargo.toml
crates/expression/Cargo.toml
crates/fuzz-contracts/Cargo.toml
crates/fuzz/Cargo.toml
crates/nowledge-contracts/Cargo.toml
crates/optimizer-graph/Cargo.toml
crates/optimizer-predicate/Cargo.toml
crates/optimizer-relational/Cargo.toml
crates/optimizer-vector/Cargo.toml
crates/optimizer/Cargo.toml
crates/plan-cache/Cargo.toml
crates/plan-cypher/Cargo.toml
crates/qos/Cargo.toml
crates/qualification/Cargo.toml
crates/query-policy/Cargo.toml
crates/readiness/Cargo.toml
crates/relational/BUILD.bazel
crates/relational/Cargo.toml
crates/resource-profile/Cargo.toml
crates/runtime-tokio/Cargo.toml
crates/search/Cargo.toml
crates/sql/Cargo.toml
crates/storage/Cargo.toml
crates/system-sql/Cargo.toml
crates/telemetry/Cargo.toml
crates/vector-projection/Cargo.toml'''.splitlines())
TOOLCHAIN_PATHS = {'.bazelversion', '.github/workflows/native-bindings.yml', '.github/workflows/python-bindings.yml'}


def classify_revision(work, qualified, revision):
    assert qualified == QUALIFIED

    def git(*args):
        return subprocess.check_output(['git', *args], cwd=work)

    def blob(ref, name):
        return git('show', ref + ':' + name)

    def digest(data):
        return hashlib.sha256(data).hexdigest()

    assert git('rev-parse', RELEASE + '^').decode().strip() == QUALIFIED
    assert set(git('diff', '--name-only', QUALIFIED, RELEASE).decode().splitlines()) == RELEASE_PATHS
    assert git('ls-tree', '-r', '--name-only', QUALIFIED) == git('ls-tree', '-r', '--name-only', RELEASE)
    lock_counts = {}
    release_rows = []
    for name in sorted(RELEASE_PATHS):
        before, after = blob(QUALIFIED, name), blob(RELEASE, name)
        if name.endswith('Cargo.lock'):
            pieces = before.decode().split('[[package]]')
            normalized = [pieces[0]]
            count = 0
            for piece in pieces[1:]:
                package = re.search(r'^name = "([^"]+)"$', piece, re.M).group(1)
                if package == 'hawdb' or package.startswith('hawdb-'):
                    if 'source = ' not in piece and '\nversion = "0.5.0"\n' in piece:
                        piece = piece.replace('\nversion = "0.5.0"\n', '\nversion = "0.6.0"\n', 1)
                        count += 1
                normalized.append(piece)
            assert '[[package]]'.join(normalized).encode() == after, name
            lock_counts[name] = count
            scope = 'local HawDB package versions only; every external block and dependency edge unchanged'
        elif name == 'README.md':
            assert before.replace(b'hawdb = "0.4"', b'hawdb = "0.6"', 1) == after
            scope = 'installation example'
        else:
            assert name.endswith('Cargo.toml') or name.endswith('BUILD.bazel') or name == 'MODULE.bazel'
            assert before.replace(b'"0.5.0"', b'"0.6.0"') == after, name
            for line in before.decode().splitlines():
                if '"0.5.0"' in line:
                    assert re.fullmatch(r'\s*version = "0\.5\.0",?', line) or re.match(r'hawdb[\w-]* = \{ path = ', line), (name, line)
            scope = 'package/module version and local HawDB dependency requirements only'
        release_rows.append({'path': name, 'before_sha256': digest(before), 'after_sha256': digest(after), 'scope': scope})
    assert lock_counts == {'Cargo.lock': 42, 'bindings/ffi/Cargo.lock': 41}
    version_hits = git('grep', '-n', '-E', 'CARGO_PKG_VERSION|CARGO_PKG_NAME|option_env!', QUALIFIED, '--', '*.rs').decode().splitlines()
    hit_paths = {line.split(':', 2)[1] for line in version_hits}
    assert hit_paths == {'bindings/ffi/src/lib.rs', 'bindings/python/src/lib.rs', 'crates/relational/src/query/expression.rs'}
    assert git('rev-parse', ANNOUNCE_RC + '^').decode().strip() == RELEASE
    assert set(git('diff', '--name-only', RELEASE, ANNOUNCE_RC).decode().splitlines()) == {'.bazelrc'}
    assert blob(RELEASE, '.bazelrc').replace(b'\n# Rust test binaries', b'\nbuild --announce_rc\n\n# Rust test binaries', 1) == blob(ANNOUNCE_RC, '.bazelrc')
    assert git('rev-parse', PYTHON_PARAMETERS + '^').decode().strip() == ANNOUNCE_RC
    assert set(git('diff', '--name-only', ANNOUNCE_RC, PYTHON_PARAMETERS).decode().splitlines()) == PYTHON_PARAMETER_PATHS
    assert blob(RELEASE, 'Cargo.toml') == blob(PYTHON_PARAMETERS, 'Cargo.toml')
    assert b'bindings/python' not in blob(PYTHON_PARAMETERS, 'Cargo.toml')
    assert blob(RELEASE, 'bindings/python/Cargo.toml') == blob(PYTHON_PARAMETERS, 'bindings/python/Cargo.toml')
    assert b'\n[workspace]\n' in blob(PYTHON_PARAMETERS, 'bindings/python/Cargo.toml')
    subprocess.run(['git', 'merge-base', '--is-ancestor', QUALIFIED, revision], cwd=work, check=True)
    release_present = subprocess.run(['git', 'merge-base', '--is-ancestor', RELEASE, revision], cwd=work).returncode == 0
    announce_present = subprocess.run(['git', 'merge-base', '--is-ancestor', ANNOUNCE_RC, revision], cwd=work, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    python_present = subprocess.run(['git', 'merge-base', '--is-ancestor', PYTHON_PARAMETERS, revision], cwd=work, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    changes = git('diff', '--name-only', QUALIFIED, revision).decode().splitlines()
    unknown = []
    classified = []
    for name in changes:
        after = blob(revision, name)
        if name.startswith('docs/'):
            scope = 'documentation'
        elif name == 'README.md' and release_present and after == blob(RELEASE, name):
            scope = 'verified #982 installation example'
        elif name in RELEASE_PATHS and release_present and after == blob(RELEASE, name):
            scope = 'verified #982 version-only metadata'
        elif name == '.bazelrc' and announce_present and after == blob(ANNOUNCE_RC, name):
            scope = 'verified #985 configuration logging only; no action or runtime options changed'
        elif name in PYTHON_PARAMETER_PATHS and python_present and after == blob(PYTHON_PARAMETERS, name):
            scope = 'verified #983 independent Python binding workspace; outside Rust library/search benchmark dependency graph'
        elif name == '.bazelversion' and after == b'9.3.0\n':
            scope = 'pin already-used Bazel 9.3.0'
        elif name in TOOLCHAIN_PATHS - {'.bazelversion'}:
            normalized = after.replace(b'      - ".bazelversion"\n', b'').replace(b'USE_BAZEL_VERSION: "9.3.0"', b'USE_BAZEL_VERSION: "9.2.0"')
            if normalized != blob(QUALIFIED, name):
                unknown.append(name)
            scope = 'binding CI Bazel pin and trigger; final CI remains required'
        else:
            unknown.append(name)
            scope = 'unqualified runtime, test, or build change'
        classified.append({'path': name, 'scope': scope, 'sha256': digest(after)})
    return {
        'qualified_commit': qualified, 'compared_revision': revision,
        'search_runtime_benchmark_tests_and_external_dependencies_unchanged': not unknown,
        'literal_build_inputs_unchanged': all(row['scope'] == 'documentation' for row in classified),
        'unqualified_changes': unknown, 'changes': classified,
        'later_changes': [
            {'commit': ANNOUNCE_RC, 'PR': 985, 'present_on_compared_revision': announce_present,
             'paths': ['.bazelrc'], 'scope': 'Adds only build --announce_rc diagnostic output.'},
            {'commit': PYTHON_PARAMETERS, 'PR': 983, 'present_on_compared_revision': python_present,
             'paths': sorted(PYTHON_PARAMETER_PATHS),
             'scope': 'Parameter conversion in the separate downstream Python binding workspace; root library manifests, search/storage/benchmark source and dependency graph remain unchanged.'},
        ],
        'release_metadata': {'commit': RELEASE, 'PR': 982, 'present_on_compared_revision': release_present,
                             'local_lock_package_counts': lock_counts, 'files': release_rows,
                             'version_macro_uses': version_hits,
                             'scope': 'Only package metadata and C/Python/SQL version strings advance. Measured binaries, source hashes, timings and RSS remain bound to 41e10dac; no new 0.6 binary measurement is claimed.'},
    }


if __name__ == '__main__':
    work = Path('/private/tmp/hawdb-291-incremental-recovery-followup')
    revision = subprocess.check_output(['git', 'rev-parse', 'origin/main'], cwd=work, text=True).strip()
    result = classify_revision(work, QUALIFIED, revision)
    Path('/private/tmp/hawdb-291-post913-main-change-classification.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k not in {'changes', 'release_metadata'}}, indent=2))
    assert not result['unqualified_changes'], result['unqualified_changes']
