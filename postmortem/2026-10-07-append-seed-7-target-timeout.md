# Append seed-7 target timeout

Task: [HawDB issue #875](https://github.com/nowledge-co/hawdb/issues/875).
Policy: [Bazel target timeouts](https://github.com/nowledge-co/mem/blob/main/docs/BAZEL_TIMEOUT.md).

## Observation

The complete local 96-target fuzz invocation at
`cc337b9716a51fc9e88dae6542fcc42641d68eb7` reported:

```text
TIMEOUT: //crates/fuzz:hawdb_fuzz_append_seed_7_tests (Summary)
running 1 test
-- Test timed out at 2026-10-06 18:15:57 UTC --
```

The unchanged target has size small and an implicit short/60s deadline. It
selects exactly `append_state_machine_matches_reference_model_across_reopen`,
seed 7, 128 state-machine steps with checkpoint/WAL reopen and a complete
reference oracle. An earlier incomplete 89-target run at `0006585` completed
this target in 59.3s; the command itself failed on seven unrelated loading
errors and is not a complete baseline. The older 3-5s BUILD measurement does
not qualify this source or runner workload.

Reproduction:

```console
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test --nocache_test_results --test_output=errors
```

macOS arm64, M3 Max, 16 logical CPUs, 128 GiB RAM, pinned Rust 1.97.1, default
Bazel configuration. Fixtures use test-owned synthetic persistent data.
Physical storage and full concurrent host activity are unknown. Neither
contention, a production regression, an assertion failure nor a hang is proved.
[Historical issue #75](https://github.com/nowledge-co/hawdb/issues/75) isolated
these cases from an aggregate; this later failure concerns an already isolated
case's current deadline.

## Immediate mitigation

Only seed 7 receives moderate/300s, the next finite standard class. Sibling
replay and relational-rewrite deadlines remain short/60s. Names, size,
features, retries, all seeds/128 steps/assertions, `.bazelrc` and default CI
selection stay intact. Fuzz remains local-only. This is observation headroom,
not a measured speedup or a fixed hang.

## Timing and decomposition

After the independent mitigation PR exists, measure the complete isolated
case and qualify the full local aggregate on containing source. The selected
target is one cohesive state-machine oracle whose ordered transitions share
database/reference state; independent step fragments would change that oracle.
Preserve the sequence and collect phase/fixture costs before considering a
semantics-preserving decomposition. Do not reduce steps or repeat escalation.

Owner: hawkingrei. Issue #875 remains open for terminal containing-source
receipts, complete timing and the documented decomposition disposition. No
complete over-60s duration has been observed yet.
