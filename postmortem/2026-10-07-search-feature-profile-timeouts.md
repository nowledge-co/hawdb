# Search feature-profile target timeouts

Task: [HawDB issue #872](https://github.com/nowledge-co/hawdb/issues/872).
Governing policy: [Bazel test timeouts](https://github.com/nowledge-co/mem/blob/main/docs/BAZEL_TIMEOUT.md).

## Observation

At `0006585131c59e9e88ffbd0cd3ded16206de5db1`, the expanded optimizer,
Search feature and consumer verification selected 33 targets. It completed
29 and timed out four Search profiles under their existing moderate/300s
target deadlines:

| Target in `//crates/search` | Selected cases | Completed/ignored names | Timeout |
| --- | ---: | ---: | ---: |
| `hawdb_search_background_only_tests` | 629 | 621 | 301.5s |
| `hawdb_search_minimal_tests` | 613 | 606 | 305.6s |
| `hawdb_search_vector_background_tests` | 663 | 623 | 301.7s |
| `hawdb_search_vector_only_tests` | 647 | 604 | 304.1s |

These are incomplete receipts. The five other Search library profiles passed
in 477.2-488.0s under their existing long/900s deadlines. No assertion failure
was reported in the four expired logs. The feature matrix and original
timeout expression match immediate main base `1a3e532d`; no comparable
baseline execution has been measured.

Reproduction, preserving the complete matrix:

```console
bazel test //crates/search:presubmit_tests --nocache_test_results --test_output=errors
```

The recorded run also selected optimizer owners, Cascades, plan-core,
plan-cypher, plan-cache, portable and consumer targets. Total elapsed time was
538.860s; critical path was 535.77s. Tests use synthetic fixtures. Environment:
macOS arm64, M3 Max, 16 logical CPUs, 128 GiB RAM, Rust 1.97.1, default Bazel
configuration. Physical storage and complete concurrent host workload are
unknown; fixture durability/residency differ by case.

## Immediate mitigation

Only the four formerly moderate targets move to long/900s, the next finite
standard class. The five already-long targets retain their deadlines. This
provides observation headroom consistent with completed sibling-profile
times; it does not establish a speedup, a fixed hang, a production regression
or a proven host-contention cause.

Keep the full case inventory, assertions, features, public labels and retry
policy. No `.bazelrc`, global timeout, runner/job deadline or unrelated target
changes are part of this mitigation.

## Measurement and follow-up

After the independent mitigation PR exists, collect complete containing-source
profile and per-case durations, distinguish idle-host measurements from the
original combined invocation, and compare the same workload on supported
runners. Inspect persistence/recovery fixture costs and capability boundaries
before splitting into independently compiled targets with scoped sources and
dependencies. Preserve aggregate labels and CI coverage. Revisit the temporary
increase after decomposition; do not substitute another deadline escalation.

Owner: hawkingrei. Issue #872 remains open for qualification and decomposition,
or a documented concrete architectural obstacle with an owner and next step.
No repository-defect or performance-improvement claim follows from the current
unqualified local timing evidence.
