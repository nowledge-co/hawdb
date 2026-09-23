<!--
Thank you for contributing to HawDB!
Target main directly and keep one coherent change per PR.
Title: type(scope): concise change, e.g. perf(storage): bound path statistics work.
See CONTRIBUTING.md for scope, evidence, and review expectations.
-->

### Issue

<!--
Every PR must link an existing, relevant issue, including documentation and
mechanical maintenance PRs. Search existing issues and create one first if needed.
Use close #123 only when this PR fully resolves the issue and completes all of
its acceptance criteria. Otherwise use ref #123 and explain the remaining work.
For multiple issues, choose each relationship explicitly: close #123, ref #456.
Replace the placeholder below. None, missing references, and placeholders are
not accepted. Reviewers must verify the issue and relationship before approval.
-->

Issue Number: ref #xxx

### Problem and context

<!-- Describe the concrete trigger, current behavior, and intended outcome.
Name the affected component and link the governing design/spec when relevant. -->

### Implementation and tradeoffs

<!-- Explain the approach, root cause for a fix, invariants preserved, and
alternatives that matter. Identify any bounded analogue paths affected by the
same cause. For optimizations, discuss small/bulk workloads and resource costs. -->

### Validation

<!-- Check only evidence obtained for this change. Give exact commands, tested
revision, features/profile, outcome, and relevant counts or artifact links.
Record not-run checks and their reasons. A planned test is not a passing test.
Review CONTRIBUTING.md for change-specific coverage and local fuzz commands. -->

- [ ] Unit or regression tests
- [ ] Embedded API / query integration tests
- [ ] Persistence / recovery / concurrency tests
- [ ] Differential or local fuzz tests
- [ ] Benchmark or resource measurements
- [ ] Documentation / template validation only (reason below)

Evidence and remaining gaps:

### Compatibility and risks

<!-- Keep applicable items and explain them; write None with a reason if none apply.
- Public Rust API, Cypher/SQL semantics, errors, or configuration/defaults.
- WAL/checkpoint/on-disk format, reopen/replay, or development database recreation.
- CPU, memory, I/O, lock duration, batch scaling, or budget exhaustion behavior.
- Cargo features, dependencies, crate ownership, or host integration.
For a Mem integration change, follow the release and full-verification contract
in AGENTS.md. Publishing HawDB crates does not authorize stable Mem activation.
-->

### Release note

<!-- State the externally observable change. Use None for internal-only or
documentation changes. Call out compatibility changes explicitly. -->

```release-note
None
```
