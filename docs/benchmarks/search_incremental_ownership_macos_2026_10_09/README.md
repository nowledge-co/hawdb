# Incremental search ownership qualification

The qualified source is `41e10dacf8b2589e416c01b9b047db98dfe01acd` (tree `6026a7fb47580a56a72677463077fee383431fdd`). Start from that revision
with the repository's pinned Rust 1.97.1, locked dependencies and default
features. The measured host is recorded in `provenance/scale-manifest.json`.
The report keeps fresh post-913 evidence separate from older measurements.

## Reproduce the matrix

Use an isolated checkout of the qualified revision. Build the same release
library and benchmark:

```console
cargo build --locked --release --lib
cargo bench --locked --bench search_mutation --no-run
```

Run the following from that checkout. This developer process explicitly sets
its own OS soft FD limit to 4,096; the embedded library does not change host
limits. The hard limit must already permit that setting. The three cells run
sequentially, with fresh fixtures, 32 seed segments and every 128 write/merge
rounds. All benchmark environment values match the measured manifest.

```python
import os
import resource
import subprocess
import tempfile

soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
assert hard == resource.RLIM_INFINITY or hard >= 4096
resource.setrlimit(resource.RLIMIT_NOFILE, (4096, hard))
fixed = {
    "HAWDB_SEARCH_MUTATION_BENCH_CONTENT_BYTES": "65536",
    "HAWDB_SEARCH_MUTATION_BENCH_ROUNDS": "128",
    "HAWDB_SEARCH_MUTATION_BENCH_MEMORY_BYTES": "268435456",
    "HAWDB_SEARCH_MUTATION_BENCH_SEGMENT_BYTES": "67108864",
    "HAWDB_SEARCH_MUTATION_BENCH_LEXICAL_BUILD_MEMORY_BYTES": "8388608",
    "HAWDB_SEARCH_MUTATION_BENCH_OPEN_FILES": "1024",
    "HAWDB_SEARCH_MUTATION_BENCH_VECTOR_DIMENSIONS": "384",
    "HAWDB_SEARCH_MUTATION_BENCH_COMPACTION_EVERY": "1",
    "HAWDB_SEARCH_MUTATION_BENCH_REUSE_VALIDATION": "1",
}
with tempfile.TemporaryDirectory(prefix="hawdb-291-reproduce-") as scratch:
    for documents, touches in [(81920, 10), (327680, 10), (327680, 100)]:
        env = dict(os.environ, **fixed)
        env["TMPDIR"] = scratch
        env["HAWDB_SEARCH_MUTATION_BENCH_DOCUMENTS"] = str(documents)
        env["HAWDB_SEARCH_MUTATION_BENCH_TOUCHES"] = str(touches)
        subprocess.run(
            ["cargo", "bench", "--locked", "--bench", "search_mutation"],
            env=env, check=True,
        )
```

Each cell emits one `search_mutation` JSON result. Inspect all peak and steady
RSS samples, the 128 actual compactions, changed and deleted document counts,
checkpoint bytes and merge bytes. The budget is 268,435,456 bytes. Artifact
write amplification sums checkpoint plus merge artifacts and divides by all
sustained changed body and embedding bytes. It excludes graph/WAL and device
writes. `raw/reproduction/hawdb-291-post913-scale-audit.py` records the complete
checks, while each `.result.json` preserves the actual measurement.

## Query, recovery and local fuzz commands

The complete feature profiles and recovery suite use:

```console
bazel test //crates/search:hawdb_search_tests //crates/search:hawdb_search_acl_tests //crates/search:hawdb_search_text_background_tests //crates/search:hawdb_search_text_only_tests //crates/search:hawdb_search_text_vector_tests --local_test_jobs=1 --test_output=errors
cargo test --locked -p hawdb --all-features --lib api::tests::power_loss::search_projection:: -- --test-threads=1 --nocapture
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test --local_test_jobs=1 --test_output=errors
```

The 294-comparison probe source is
`raw/reproduction/hawdb-291-post913-compressed-compaction-probe.rs`. Compile it
with the release library, then pass an unused synthetic fixture directory.
The original `compressed-probe.py` records all linker paths from this host;
the following selects paths from the new checkout instead:

```python
from pathlib import Path
import subprocess
import tempfile

release = Path("target/release").resolve()
probe = Path("/absolute/path/to/copied/evidence/raw/reproduction/hawdb-291-post913-compressed-compaction-probe.rs")
with tempfile.TemporaryDirectory(prefix="hawdb-291-reference-") as scratch:
    binary = Path(scratch) / "compressed-reference"
    command = [
        "rustc", "--edition=2021", str(probe), "-o", str(binary),
        "-C", "opt-level=3", "-C", "codegen-units=1", "-C", "lto=thin",
        "-L", "dependency=" + str(release / "deps"),
        "--extern", "hawdb=" + str(release / "libhawdb.rlib"),
    ]
    for native in sorted((release / "build").glob("*/out")):
        command += ["-L", "native=" + str(native)]
    subprocess.run(command, check=True)
    subprocess.run([str(binary), str(Path(scratch) / "fixture")], check=True)
```

Use the actual `CARGO_TARGET_DIR` when it differs from `target`. Expected
probe output is 294 comparisons, distinct/changed vectors, three real merges,
two initial owners, required RaBitQ artifacts and a retained old reader.

The full command and raw test logs distinguish actual executions from cached
passes. Any preserved timeout is resolved only by a recorded isolated retry
at its original deadline; no seed, case or resource cap changes. Formatting,
strict native and target Clippy commands are in the required-check receipt.

## Evidence interpretation

`report.json` binds every included raw file by SHA-256 and byte count. The
historical 20 GiB/K10 baseline includes all 32 seed rows at initialization.
Formats, dependencies and admission APIs differ, so compare emitted search
artifact bytes rather than claiming controlled throughput or RSS improvement.
The body corpus is compressible synthetic text. Host/cache state is
uncontrolled, and no debugger or memory-inspection process was attached.

Finite query/fault witnesses assume completed POSIX synchronization and atomic
same-directory rename. They do not prove universal ANN recall or every hardware
power-loss behavior. This report does not authorize stable Mem activation.
