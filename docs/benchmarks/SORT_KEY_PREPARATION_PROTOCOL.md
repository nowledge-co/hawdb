# Single-column sort-key preparation controls

Status: source qualification in progress; no performance acceptance is claimed.
This is an internal allocation change under `SortExec`, not a new ordering
operator. The original tuple allowance, row charge, cancellation points, stable
ordinal ties, typed `Value` ordering and spill format remain the contracts.
Top-N and computed/multiple-key preparation retain their existing paths.

In-memory runs select integer, general or mixed comparison once per run.
General comparisons use the original typed slice comparator; integer runs use
their common direction and stable ordinal ties. Only mixed runs inspect key
representation during comparison. Run keys keep the original Vec-header width,
including for general keys; the empty-key case retains its original charge.
Spill records still contain bindings and ordinals. Non-expression merges retain
encoded bindings and reconstruct owned ordering keys under the original key
allowance recorded in each spill run, including inline integer runs. Expression
merges retain the tagged-key fallback and its admission rules.
The homogeneous/mixed run and forced-spill guards cover both directions and
complete stable output under unchanged resource limits.

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

## Separately identified writer-phase diagnosis

The retained acceptance failures include in-memory, ungrouped 2W/2R commit-tail
regressions. Use `concurrent_writers --phase-diagnostic` to investigate that
bounded workload, rather than rerunning the failed acceptance sample. This
manual mode uses two optimistic writers with 0 or 2 readers and both concurrent
and host-serialized controls. Each release case performs all 256 disjoint-key
commits; reader cases retain all 128 reads, pinned-snapshot checks, typed output
and actual reader/writer overlap checks. It does not cover durable/grouped
writes, String sort controls or the complete acceptance matrix.

Five monotonic boundaries per transaction directly measure host-gate wait,
transaction creation, query execution and commit. Raw transaction identities and
boundaries remain available. The existing opt-in coordinator metrics are sampled
around the workload, before final verification. Their count/total deltas include
overlapping sequencer wait/hold, commit execution and read publication; never sum
these counters or turn their averages into per-commit p95. Instrumentation
perturbs timing and is disabled in the ordinary qualification path.

From a clean committed candidate, run the fixed paired diagnostic against an
explicit full baseline SHA:

```sh
python3 scripts/diagnose-sort-writer-phases.py \
  --baseline-revision d825c561781ccde0cc7f9f0e9bf42422709c378c \
  --work /tmp/hawdb-sort-phase-work \
  --output /tmp/hawdb-sort-phase-results
```

Both revisions use the identical overlaid benchmark, separate locks/target
directories and the candidate root's global bench profile. Both builds finish
before timing starts. One warmup pair and five measured pairs alternate process
order. The runner retains full source/overlay/binary hashes, toolchain, CPU/load
observations, every process result and raw phase sample, including failures.
Five-pair ratios are descriptive diagnostics: no confidence interval, zero-
increase acceptance, phase heap reduction or physical exclusivity is claimed.
The original 40-pair gates and all adverse historical results remain binding.
This mode has no CI or scheduled execution lane.

To finish builds while other verification runs, add `--prepare-only` to that
command. After verification finishes, use the same arguments with
`--measure-prepared` to run timing once. The latter checks the committed source
and both binary hashes and refuses previously measured evidence. It never
rebuilds during timing or replaces a failed sample.
