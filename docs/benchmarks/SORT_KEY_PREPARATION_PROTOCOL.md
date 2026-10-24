# Single-column sort-key preparation controls

Status: source qualification in progress; no performance acceptance is claimed.
This is an internal allocation change under `SortExec`, not a new ordering
operator. The original tuple allowance, row charge, cancellation points, stable
ordinal ties, typed `Value` ordering and spill format remain the contracts.
Top-N and computed/multiple-key preparation retain their existing paths.

The manual `concurrent_writers` control mode calls the embedded Rust API with
parameterized synthetic writes. It returns all 256 or 4,096 rows ordered by one
projected column. Cases use integer, float, 256-byte string, NULL and mixed
Int/Float keys. One retained snapshot supplies a bounded canonical input; a
stable reference sort and every complete typed result are checked outside timing.
The physical plan must retain `SortExec` over the requested projected column.
Each case labels one query warmup and retains eight query samples, plus fixture
setup duration. Reference copies are outside engine admission and are not a
whole-query memory comparison.

Use the repository's global release profile:

```sh
cargo bench --locked --bench concurrent_writers -- --sort-key-control
bazel run --config=release //:hawdb_bench_concurrent_writers -- --sort-key-control
```

`measure-sort-key-controls.py` consumes the two already built binaries and a
complete fixed writer-matrix receipt. It retains one process warmup pair and
40 measured pairs in alternating revision order. Each native process executes
all ten cases. Raw stdout, stderr, process status, checksums, source/binary
identity, load and every query sample are retained, including native errors.
Thirty predeclared latency endpoints use conservative simultaneous sign-test
bounds with ranks 10/31 and a candidate/baseline threshold of one. Inconclusive
endpoints do not pass; any refusal remains explicit, without retry or truncation.

```sh
python3 scripts/measure-sort-key-controls.py \
  --writer-evidence artifacts/strict-writer-gate \
  --output artifacts/sort-key-controls
```

These resident read-only controls do not establish physical exclusivity,
phase-specific heap peaks, concurrent publication acceptance or a universal
latency bound. They supplement the complete original/current-baseline writer
matrix and publication/resource qualification. They do not replace those gates
or discard earlier allocations. Ordinary executor units cover admission,
cancellation, typed order and spill behavior in the existing crate CI owner;
see [test ownership](../TEST_INDEX.md).
