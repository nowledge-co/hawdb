# Bf-Tree key-comparison probe

This is a manual experiment within the isolated MVCC workspace. The default
adapter still uses unmodified, registry-pinned `bf-tree = 0.5.6`. No production
dependency, page layout, journal synchronization, or CI lane selects this probe.

The probe replaces four **remaining-key comparisons** in the real dependency:
two in leaf `key_cmp`/`lower_bound`, two in inner `key_compare`/`lower_bound`.
The existing two-byte leaf and four-byte inner previews and binary search stay
unchanged. It compares immutable key slices, never a vector load across
`LeafKVMeta` and its concurrently updated atomic reference bits.

| Variant | Remaining-key comparison |
| --- | --- |
| `upstream` | Original slice ordering, generally compiled to `memcmp` |
| `word` | Eight-byte big-endian unsigned integer comparisons, then slice tail |
| `neon` | Sixteen-byte NEON loads, two parallel 64-bit equality tests; compare the first unequal lane in byte order, then the same word tail |

The NEON kernel is compiled only for little-endian AArch64 with NEON enabled;
other targets retain slice ordering. The measurement driver refuses a host
that cannot execute the NEON branch. There is no global `target-cpu`, compiler
profile override, dispatch environment variable, or default algorithm change.
An original `memcmp` can itself use platform SIMD: `upstream` is not a claim
that the entire program executes only scalar instructions.

The two differential kernel tests cover lengths 0–160, all 16 pointer offsets,
every mismatch position, equal keys, proper prefixes, arbitrary binary bytes,
and 20,000 independent random length/content pairs. They are included in the
adapter's ordinary manual test target. Each patched adapter also runs the full
MVCC/recovery tests in both dev and release, plus all shape/layout smoke cases.

## Reproduce

Use Python 3.12 or later and the repository-pinned Rust toolchain. Run from the
repository root. Choose new scratch, measurement-output and journal directories.
Target directories may reuse existing build caches. Do not run measurements
while these builds or other local verification commands are still running.

```console
python3.14 experiments/bftree-mvcc/simd/prepare.py --scratch /tmp/bftree-simd-probe
python3.14 experiments/bftree-mvcc/simd/build.py --scratch /tmp/bftree-simd-probe --target-dir /tmp/bftree-simd-release --dev-target-dir /tmp/bftree-simd-dev
python3.14 experiments/bftree-mvcc/simd/measure.py --scratch /tmp/bftree-simd-probe --output /tmp/bftree-simd-results --journals /tmp/bftree-simd-journals
```

`prepare.py` verifies the cached registry archive against the existing lockfile's
checksum and upstream commit `3998ce8643dec4e98b07f2d11cad67bf2c2d28d1`. It
extracts three independent copies, preserves the upstream MIT notices and CRLF,
and checks the exact number of replaced call sites. It copies identical native
adapter/benchmark sources, the pinned toolchain and global Cargo profile.
Offline lock resolution may only replace this one registry dependency with a
local path; any unrelated lockfile change is refused. It never edits the Cargo
registry or the checked-in manifest/lockfile. The Apache-2.0 comparison kernel
is original probe code, not copied from RocksDB or Microsoft.

`build.py` records the exact commands, exit status and elapsed time of tests,
strict adapter Clippy and release builds. It saves fixed binaries with hashes.
The unchanged published dependency emits `unexpected cfg(spdk)` warnings when
compiled as a local path dependency; these are visible in all variants' logs.
No lint level is changed and no unsupported SPDK feature is enabled.

`measure.py` checks the prepared source and executable hashes, identical harness
sources and absence of compiler/profile environment overrides. Five repetitions
rotate/reverse executable, layout and case order. Each case uses a fresh child
process and journal; all three variants must produce byte-identical journals.
The native harness checks returned point values, every scan key/value/order and
reopen state. It retains current/seeded snapshots and unchanged `sync_all` per
transaction, with 100-byte values, 128-key seed and 64-key update batches.

The matrix covers all Versioned/Hybrid/COW layouts with numeric eight-byte keys
at 16/2, 512/24 and 2048/48 keys/overwrite rounds, plus 512/24 with two 64-byte
key shapes. `prefix64` shares 56 leading logical bytes before its numeric ID;
`spread64` begins with the numeric ID then has 56 deterministic varying bytes.
Both are synthetic distributions, not a captured Mem workload. The direct CLI
is `bench-shaped <new-journal> <layout> <keys> <rounds> <shape>`; old `bench` and
`bench-one` retain numeric keys. COW is an unchanged-algorithm control.

Each process reports 4096 current and 4096 old point reads, 32 current and 32 old
full scans, complete commit/prepare-sync/index-install percentiles, reopen time,
journal size and resident/peak memory. Scan p99 is a maximum. `results.jsonl.gz`
keeps all child outputs and compiler/load boundary observations; `summary.json`
contains medians of **within-repetition ratios**, all individual ratios and
absolute medians. Shared-host noise is not eliminated by rotating order.

See the [measured report](../measurements/simd-20261024/README.md) for the outcome
and the primary-source RocksDB comparison. These are component results, not
production HawDB performance, power-loss, x86 SIMD or total-RSS qualification.
