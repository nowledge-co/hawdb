# IMV foreground work-limit resource diagnostic

This manual diagnostic supports a default of 250,000 row work units while
retaining the existing 64 MiB cumulative logical payload allowance. It uses
`Database::new()` and public parameterized SQL APIs, including ordinary facade
staging and commit. No internal limit override is used. The Rust source and
raw results are retained here; they are excluded from library archives and are
not registered in any UT, CI lane, scheduled job or test suite.

The motivating 60,000-row projection requires 120,000 work units. The row-ceiling
fixture has 125,000 BIGINT source rows and creates 125,000 projection rows,
exactly 250,000 work units. One extra source row is refused before publishing
the view. Source counts and view counts/absence are checked after each outcome.
Source rows are loaded in batches of 1,000, separately from the measured DDL.

The payload fixture uses deterministic high-entropy ASCII bodies of 128 KiB,
loaded in batches of eight, and projects each body twice. Body work alone is
five copies per source row: source hydration, two projection copies and two
output fields. At 102 rows this is 63.75 MiB before key/row charges; at 103 rows
it is 64.375 MiB. The former is admitted and the latter is refused by maintenance
payload admission. Neither fixture approaches the row-work ceiling.

Measurements use macOS 27.0.1/aarch64, pinned Rust 1.97.1 and the repository dev
profile (opt-level 1, checked arithmetic/debug assertions), with default Cargo
features and an in-memory database. Compilation is excluded. Three fresh
processes are retained for each admitted ceiling case; other controls use one.
`getrusage(RUSAGE_SELF).ru_maxrss` is read before and immediately after the DDL,
with macOS bytes retained in `results.json`. Both values are cumulative process
high-water marks, including fixture setup. They are not instantaneous RSS or
exclusive allocations made by the commit; subtracting them does not measure
live allocation cost. The source-only process provides an additional control.

| Case | Source rows | Payload bytes/row | Outcome | Peak before DDL (MiB) | Peak through DDL (MiB) | DDL time (ms) |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| motivating | 60000 | 0 | admitted | 39.766 | 73.750 | 442.151 |
| row_0 | 125000 | 0 | admitted | 62.672 | 133.500 | 1203.813 |
| row_1 | 125000 | 0 | admitted | 61.859 | 132.719 | 747.749 |
| row_2 | 125000 | 0 | admitted | 61.891 | 132.734 | 756.581 |
| row_refusal | 125001 | 0 | refused | 61.906 | 62.094 | 0.104 |
| payload_0 | 102 | 131072 | admitted | 34.031 | 34.344 | 19.378 |
| payload_1 | 102 | 131072 | admitted | 34.203 | 34.500 | 17.442 |
| payload_2 | 102 | 131072 | admitted | 34.047 | 34.359 | 18.021 |
| payload_refusal | 103 | 131072 | refused | 34.500 | 34.766 | 17.653 |
| source_only | 125000 | 0 | baseline | 61.875 | 61.875 | 0.000 |

The row-ceiling processes peaked at 132.719-133.500 MiB; the payload-ceiling
processes peaked at 34.344-34.500 MiB. These are observed values for these shapes,
not universal memory bounds. The logical 64 MiB allowance is not an RSS limit.
Foreground maintenance still lacks process-governor reservation for these
charges and a host-facing typed SQL override; this change does not add either.
Mobile devices/profiles, persistent I/O, all aggregate shapes, release-profile
performance and worst-case allocator behavior are not qualified here. No
production activation, throughput improvement or CI speedup is claimed.

`results.json` records the prior commit plus fingerprints of every changed
runtime/test source, root manifest/lock, profile/toolchain, diagnostic source and
measured binary. The final incremental diagnostic build duration is retained
there; initial dependency/build setup on an earlier diagnostic revision took
1m32s. A clean final-source build was not measured. Large resource fixtures
remain manual; ordinary UTs use integer charges and tiny fixtures.

Run from the repository root so the pinned toolchain and repository-wide Cargo
profiles are loaded. The temporary host does not join the library workspace or
change its manifest/lock. Offline setup assumes the repository dependencies are
already cached. After its initial lock reconciliation, all replays are locked.
The RSS diagnostic requires macOS or Linux.

```sh
python3 - <<'PY'
from pathlib import Path
import json, shutil
root = Path.cwd()
host = Path('/private/tmp/hawdb-imv-review-rss-harness')
host.mkdir(exist_ok=True)
source = root / 'docs/benchmarks/imv_work_limits_2026_10_30/imv_work_limits.rs'
manifest = '[package]\nname = "hawdb-imv-work-limits-diagnostic"\nversion = "0.0.0"\nedition = "2024"\npublish = false\n\n[[bin]]\nname = "hawdb-imv-work-limits-diagnostic"\npath = ' + json.dumps(str(source)) + '\n\n[dependencies]\nhawdb = { path = ' + json.dumps(str(root)) + ' }\nlibc = "0.2"\nserde_json = "1"\n\n[workspace]\n'
(host / 'Cargo.toml').write_text(manifest)
shutil.copyfile(root / 'Cargo.lock', host / 'Cargo.lock')
PY
CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=target cargo build --offline --manifest-path /private/tmp/hawdb-imv-review-rss-harness/Cargo.toml
cargo fmt --manifest-path /private/tmp/hawdb-imv-review-rss-harness/Cargo.toml -- --check
CARGO_BUILD_JOBS=4 CARGO_TARGET_DIR=target cargo clippy --locked --manifest-path /private/tmp/hawdb-imv-review-rss-harness/Cargo.toml --all-targets -- -D warnings
target/debug/hawdb-imv-work-limits-diagnostic 60000 0 admit
target/debug/hawdb-imv-work-limits-diagnostic 125000 0 baseline
target/debug/hawdb-imv-work-limits-diagnostic 125000 0 admit
target/debug/hawdb-imv-work-limits-diagnostic 125001 0 refuse
target/debug/hawdb-imv-work-limits-diagnostic 102 131072 admit
target/debug/hawdb-imv-work-limits-diagnostic 103 131072 refuse
```
