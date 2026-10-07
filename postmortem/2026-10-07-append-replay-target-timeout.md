# Append replay target timeout

Task: [HawDB issue #888](https://github.com/nowledge-co/hawdb/issues/888).
Policy: [Bazel target timeouts](https://github.com/nowledge-co/mem/blob/main/docs/BAZEL_TIMEOUT.md).

## Observation

The literal local fuzz command on frozen clean
`9eef50e351a14fab95f657f2e28bf6c465bd0110` ended exit 3 in 938.735s:

```console
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

All 96 targets executed: 94 passed, and append replay plus seed 7 timed out.
All 1,847 source hashes remained unchanged. Replay reported:

```text
//crates/fuzz:hawdb_fuzz_append_replay_tests TIMEOUT in 60.1s
```

The preceding `84984115` run also expired replay at 61.0s. These are actual
test-target expirations, not build, runner or whole-job deadlines. No assertion
failure, production regression, hang, or causal explanation is established.

Environment: macOS 27.0.1/aarch64, M3 Max, 16 logical CPUs, 128 GiB, pinned
Rust 1.97.1 and default Bazel configuration. Persistent fixtures are synthetic
and test-owned. Physical storage and complete concurrent host activity are
unknown; the result does not establish cold physical I/O or normalized timing.

The target selects exactly
`append_oracle::tests::append_state_machine_is_replayable_for_another_seed`:
seed `0xfeed_beef`, all 128 ordered transitions, durable checkpoint/WAL reopen,
generated/default schemas and complete independent reference-model checks.
The independently owned [seed-7 issue](https://github.com/nowledge-co/hawdb/issues/875)
and its delivered timeout adjustment do not cover this sibling replay result.

## Immediate mitigation

Only replay changes from short/60s to moderate/300s, the next finite standard
class. Seed 7 retains its already-delivered 300s; relational rewrite retains
60s. Size, names, features, retries, both seeds, all 128 steps, fixtures,
assertions, `.bazelrc` and local-only campaign selection remain unchanged.
This is observation headroom, not a measured speedup or a hang fix.

## Timing and decomposition

After the independent mitigation PR exists, measure the complete uncached
replay and qualify the literal full campaign on containing source. Record
actual test runtime separately from compilation and command elapsed time.

This is one cohesive state-machine case. Its ordered transitions share one
database and two reference models; checkpoints and WAL reopens depend on prior
state. Splitting step ranges into independent tests would change the oracle.
Inspect fixture and transition costs before choosing a semantics-preserving
optimization or independent compile/fixture boundary. Preserve all transitions
and assertions; do not reduce steps or escalate again.

Owner: hawkingrei. The issue remains open for terminal containing-source
timing, full-campaign results and an evidence-backed decomposition disposition.
A complete over-60s duration remains unmeasured; no mitigation qualification or
performance improvement is claimed by this configuration change alone.
