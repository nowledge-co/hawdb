# Spill-compaction target deadline

Task: [HawDB issue #877](https://github.com/nowledge-co/hawdb/issues/877).
Policy: [Bazel target timeouts](https://github.com/nowledge-co/mem/blob/main/docs/BAZEL_TIMEOUT.md).

## Observation

At `f716b242024f698a76d16bfa1bce069c338507a9`, based directly on main
`1a3e532da8da8d6874e3d950eebbcec377f055ce`, the unchanged manual target
`//crates/executor:hawdb_spill_compaction_fuzz_tests` expired its implicit
moderate/300-second deadline:

```text
TIMEOUT: //crates/executor:hawdb_spill_compaction_fuzz_tests (Summary)
-- Test timed out at 2026-10-06 18:55:10 UTC --
Executed 96 out of 96 tests: 95 tests pass and 1 fails locally.
```

The containing command exited 3 after 1347.154 seconds. Seed 7 passed in
59.5 seconds; graph-residency halves passed in 303.3 and 618.3 seconds.
The timeout is a test-target deadline, not a whole-job or build deadline.

```console
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test --nocache_test_results --test_output=errors
```

macOS arm64, M3 Max, 16 logical CPUs, 128 GiB RAM, pinned Rust 1.97.1,
default Bazel configuration. Fixtures use synthetic test-owned spill files.
Physical storage and complete concurrent host activity are unknown. No
assertion defect, hang, production regression or contention cause is proved.

The same production code and oracle passed at
`cc337b9716a51fc9e88dae6542fcc42641d68eb7` in 217.02 libtest seconds,
recording 512 successful and 1964 failing real-file fixtures. That different
containing revision is supporting timing, not a green receipt for this change.

## Immediate mitigation

Declare long/900 seconds for this one target, the next finite standard class.
Keep its medium resource class, label, exact filter, ignored/manual selection,
features, retries, all 128 PRNG inputs and all fixture assertions intact.
Global timeout/concurrency configuration and CI selection stay intact; fuzz
remains local-only. This grants observation time and claims no speedup.

## Measurement and decomposition

After the independent mitigation PR exists, measure the complete isolated
case and containing invocation, then fixture/phase costs. The case combines
independent deterministic inputs with final fan-ins 1/2, worker limits 1/4,
and success, injected error, run/byte budgets and cancellation. Preserve the
full successful/failing fixture union and PRNG advancement in any split.

Prefer independently compiled scoped harnesses that execute the real spill
writer, bounded executor, memory ledger, runtime checkpoints and compactor.
The current harness lives inside the executor library and exercises private
production types. Moving test files or selecting multiple filters in that
same large binary does not establish compile/cache isolation. Inspect that
dependency closure before choosing a boundary; record a concrete obstacle
and next step if a scoped harness is not feasible. Collect measurements
before rebalancing; do not reduce fixtures or repeat timeout escalation.

Owner: hawkingrei. Issue #877 remains open for final-head isolated and
containing receipts, phase costs and the decomposition disposition. No
complete over-300-second runtime has yet been observed.
