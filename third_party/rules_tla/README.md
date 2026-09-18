# Locally maintained TLA rules

This directory is based on [tomato-bazel/rules_tla v0.2.0](https://github.com/tomato-bazel/rules_tla/tree/b39eb6fc672a67a7d40f1cd258d2eb3abc13f0a3),
commit `b39eb6fc672a67a7d40f1cd258d2eb3abc13f0a3`. The MIT license and upstream
copyright are retained in `LICENSE`. HawDB owns the local changes below.

The public `tla_library` and `tla_check` contracts, pinned TLC 1.7.4 JAR, Java
runtime toolchain, isolated temporary directories, outcome classification, and
watchdog behavior are retained. No model or property is removed or weakened.

## Local changes

Each TLC build action also declares a `.tlc-evidence` directory containing its
complete log, root module/configuration snapshots, actual Java version, actual
JAR SHA-256, TLC arguments, and classified outcome/exit code. The `tla_evidence`
output group exposes these artifacts without running a second model checker.
Use a `filegroup(output_group = "tla_evidence")` over the generated `.run`
targets to materialize them, including after an action-cache hit.

Evidence and the success marker belong to the same action. Only an action
whose outcome matches `expect` can publish successful cached outputs. Callers
must still distinguish `ok` evidence from expected counterexamples; a passing
negative control does not prove a model valid. HawDB's collector additionally
requires exit code zero, complete success output, exact source/configuration,
the pinned JAR digest, and the unmodified full-check arguments.

Regression targets live in `//docs/tla/tests:rule_contract_tests`. The full
storage campaign remains `//docs/tla:storage_models`.

## Model-level sharding

`tla_test_suite(name, tests, shard_count = 1)` partitions an ordered list of
`tla_check` labels into nonempty, disjoint model shards. Model index `j` belongs
to shard `j % shard_count`. It exposes `<name>_shard_<i>` test suites and
`<name>_shard_<i>_evidence` filegroups; `<name>` and `<name>_evidence` always
cover the full list. Duplicate model labels, zero/negative counts, and more
shards than models are rejected during analysis.

This does not partition the state space of a single model. Every selected TLC
action still checks the original full configuration, including liveness. A
single `tla_check(shard_count > 1)` is rejected: native test sharding of its
trivial build-test wrapper cannot shard the model-checking build action.

Separate CI shards should upload only their declared evidence outputs, not a
shared `bazel-bin` directory containing other shards' stale outputs. HawDB's
`--collect-bazel-shards` accepts extracted TLA output directories in shard-index
order and publishes a complete manifest only if every declared model appears
exactly once in its assigned shard and all evidence validations pass. Missing,
duplicated, misplaced, or source/tool-mismatched evidence fails closed.
