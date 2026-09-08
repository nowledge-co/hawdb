#!/usr/bin/env bash
set -euo pipefail

python3 - "$@" <<'PY'
import os
from pathlib import Path
import subprocess
import sys
import tempfile

checker = str(Path(sys.argv[1]).resolve())
separator = sys.argv.index("--")
selected = sys.argv[2:separator]
manifest = sys.argv[separator + 1:]
expected = [
    "index_restart_cost",
    "integrity_checksum",
    "relational_index_access",
    "relational_index_page_cache",
    "relational_monotonic_append",
    "relational_oltp_mix",
]
assert selected == expected, (selected, expected)
assert manifest[7:13] == expected, "legacy group coverage must remain identical"
assert len(manifest) == len(set(manifest)), "benchmark manifest contains duplicates"

with tempfile.TemporaryDirectory(
    prefix="benchmark dispatch ", dir=os.environ.get("TEST_TMPDIR")
) as directory:
    root = Path(directory)
    calls = root / "calls.txt"
    runner = root / "runner"
    runner.write_text(
        '#!/usr/bin/env bash\n'
        'name="$(basename "$0")"\n'
        'echo "$name" >> "$SKEIN_SMOKE_TEST_CALLS"\n'
        'echo "output:$name"\n'
        'if [[ "$name" == "${SKEIN_SMOKE_TEST_FAIL:-}" ]]; then exit 17; fi\n',
        encoding="utf-8",
    )
    runner.chmod(0o755)
    executables = []
    for name in manifest:
        executable = root / f"skein_bench_{name}"
        executable.symlink_to(runner)
        executables.append(str(executable))

    def run(smoke, benchmarks=executables, fail=""):
        calls.write_text("", encoding="utf-8")
        env = dict(os.environ)
        env.update(
            TEST_TMPDIR=str(root / "tmp"),
            TEST_UNDECLARED_OUTPUTS_DIR=str(root / "outputs"),
            SKEIN_SMOKE_TEST_CALLS=str(calls),
            SKEIN_SMOKE_TEST_FAIL=fail,
        )
        result = subprocess.run(
            ["bash", checker, smoke, *([str(runner)] * 6), *benchmarks],
            env=env,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        return result, calls.read_text(encoding="utf-8").splitlines()

    result, legacy_calls = run("optimizer_group_2")
    assert result.returncode == 0, result.stderr
    assert legacy_calls == [f"skein_bench_{name}" for name in expected]
    individual_calls = []
    for name in selected:
        # Named dispatch must not depend on the positional group layout.
        result, observed = run(f"benchmark:{name}", list(reversed(executables)))
        assert result.returncode == 0, result.stderr
        assert observed == [f"skein_bench_{name}"], observed
        assert f"benchmark started: skein_bench_{name}" in result.stderr
        assert f"benchmark completed: skein_bench_{name}" in result.stderr
        outputs = list((root / "outputs").glob(f"*/skein_bench_{name}.txt"))
        assert outputs and all(
            path.read_text().strip() == f"output:skein_bench_{name}"
            for path in outputs
        )
        individual_calls.extend(observed)
    assert individual_calls == legacy_calls, "split must neither omit nor repeat benchmarks"

    result, observed = run("benchmark:not_in_manifest")
    assert result.returncode == 2 and not observed
    assert "unknown optimizer benchmark" in result.stderr
    result, observed = run(
        "benchmark:index_restart_cost", executables + [executables[7]]
    )
    assert result.returncode == 2 and not observed
    assert "duplicate optimizer benchmark" in result.stderr

    for name in selected:
        executable = f"skein_bench_{name}"
        result, observed = run(f"benchmark:{name}", fail=executable)
        assert result.returncode == 17, result.stderr
        assert observed == [executable]
        assert f"output:{executable}" in result.stderr
        assert f"benchmark failed: {executable} (exit 17)" in result.stderr
        assert "benchmark completed:" not in result.stderr
    result, observed = run("optimizer_group_2", fail="skein_bench_integrity_checksum")
    assert result.returncode == 17
    assert observed == [
        "skein_bench_index_restart_cost", "skein_bench_integrity_checksum"
    ]

print(
    "Benchmark dispatch: all six workloads preserved; "
    "selection, diagnostics and failures verified"
)
PY
