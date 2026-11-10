# Optimizer cost qualification development plan

Status: phase 1 candidate proof implemented, 2026-11-10. The shared workload
generator, frozen ordinary-plan baseline and fixed Sort/TopN proof are manual
tools; the four-family runner, diagnosis and fitting phases remain pending. The
[specification](specs/OPTIMIZER_COST_QUALIFICATION_SPEC.md) defines acceptance;
[TODO](../TODO.md) is the single actionable checklist. Related work:
[#1596](https://github.com/nowledge-co/hawdb/issues/1596).

## Approach and delivery boundaries

Extend the existing typed embedded qualification path with narrowly bounded
legal-candidate selection and reusable evidence. Reuse the unpublished
`tools/query-workloads` generator from Shiro and `tools/query-lab`; keep
execution/replay in the latter's explicit offline runner. This keeps
one parser/planner/executor path and avoids introducing a production hint
language or a second executor. Existing cost perturbation is a useful control,
but its limited choice coverage cannot stand in for candidate selection.

Land each stage directly against main after its prerequisite is available.
Keep later stages unimplemented until their entry gates hold. Each stage owns
its focused engine tests and source-bound evidence; schema, runtime admission
and physical-plan legality remain governed by the existing contracts.

## Available workload prerequisite

[Query workloads](../tools/query-workloads/README.md) preserves the existing
Cypher/SQL generator inventory and adds bounded type-directed SQLsmith
predicates. [Query lab](../tools/query-lab/README.md) freezes setup, queries,
parameters and budgets before execution; compares template/recursive validity,
syntax combinations, executed access shapes and failure replay; and collects
same-snapshot ordinary-plan cost baselines with three warmups/fifteen samples.

Selected relational access/join CE diagnostics also consume frozen validation
observations: directed distributions with independently counted query results,
positive-only Q-error and signed bias, explicit zero/partial/missing coverage,
and same-input paired reports. These do not provide complete predicate-level
CE traces, frozen-plan observed-cardinality re-costing or candidate regret.

The [compressed graph histogram](COMPRESSED_HISTOGRAMS.md) adapter adds 20 frozen
graph cases. It uses the existing qualification API for a selected tree
fingerprint, cost and operator actuals from the same execution. It bypasses
the physical cache and reports whole-call timing. It does not select legal
alternatives; these graph observations also do not qualify SQL candidate
identity, hardware weights or regret.

These tools are independent, unpublished workspaces, excluded from library
archives and ordinary CI. The fixed Cypher proof completes phase 1, not phase 2:
four-family SQL/Cypher coverage, paired timings and source-qualified regret remain
missing. No default cost coefficient changes in phase 1.

## Phase 1: Evidence and a minimal candidate-selection proof

Status: complete, 2026-11-10. The typed embedded API executes ordinary, TopN
and Sort-plus-Limit choices from the normal lowerer on one pinned snapshot. It
retains complete physical identity, raw costs, policy identity, properties,
cardinality estimates, execution failures and complete typed results. Candidate
requests are bounded to eight and reject stale fingerprints, missing ordinary
plans and duplicate inventories. Each candidate receives a fresh equal
deadline/work scope nested under the request's cumulative campaign admission,
so candidate order cannot consume another candidate's allowance and the
campaign total is not reset. `query-lab cost-proof` serializes the fixed
correctness record under the narrower
`hawdb-optimizer-cost-qualification-phase1-v1` protocol;
`cost-proof-replay` requires an exact replay. Both explicitly omit source/build
attestation, timing and regret claims. The full
`hawdb-optimizer-cost-qualification-v1` evidence model remains a phase-2 entry
requirement.

Dependencies: the specification, current cost/trace ownership and a frozen
synthetic read fixture.

Scope: define typed campaign/query/candidate/attempt records and prove that one
Cypher statement can execute two distinct legal Sort/TopN alternatives on one
pinned snapshot. Preserve the ordinary selected plan, typed parameters,
fingerprints, costs, complete results and execution failures. Report unsupported
selection rather than widening production optimizer controls.

Likely owners are `crates/cascades/src/qualification.rs`,
`src/api/optimizer_qualification.rs`, the existing optimizer lowerers and explain
records, with tool-only serialization/replay under `tools/query-lab/`. Final type
names and source boundaries must follow the current code during implementation.
Do not expose an arbitrary physical-plan execution API.

Entry: identify the legal alternative-generation seam, snapshot/resource
ownership and the normal execution path for the chosen fixture.

Exit: two executed fingerprints, complete equal results, unchanged raw costs
under ranking-only controls, and deterministic rejection of illegal, stale,
missing, duplicate and over-limit candidate requests. Missing/partial evidence
cannot be serialized as successful alternative-plan coverage.

Verification: existing owner/root tests for typed selection, parameter rebinding,
NULL/order/multiplicity, cancellation, budget refusal and plan-cache isolation.
The proof is a correctness milestone, not performance evidence.

## Phase 2: Four-family replay and measurement

Dependencies: phase 1 evidence and candidate selection.

Scope: implement the explicit offline runner and first-version family manifest:
scan/index, covering/non-covering, ordered-index/sort and Sort/TopN. Add the
same-snapshot SQL adapter and legal SQL selectors where absent. Freeze supported
frontend/family cells and retain unsupported cells explicitly.

Use existing persisted access and bounded-order fixtures as starting points;
validate their current operators and full outputs before reuse. Include selective
and broad predicates, residual work, skew, NULLs, duplicate keys, width/layout
controls, and Materialized/OutOfCore fresh-handle/warm cohorts. Keep bounded
fixture sizes in deterministic tests and larger measurement inputs manual.

Entry: every declared family has a concrete candidate-generation owner and a
result oracle; no proposed selector is mistaken for an existing API.

Exit: every manifest cell has a status and replay artifact, every claimed
comparison contains distinct executed plans, and phase timings/counters follow
the specification. Complete serial paired samples, seeded order, open/setup
partitions and failures remain available outside aggregates.

Verification: engine controls in current owner/root CI; runner replay and timing
aggregation controls in the existing local-only tool tests. A manual release
pilot proves collection on a fixed runner. It need not show that today's model
selects the fastest plan to establish a valid diagnostic report.

## Phase 3: Ranking and cardinality diagnosis

Dependencies: complete phase 2 raw records.

Scope: derive within-query rank agreement, ties/inconclusive comparisons,
regret distributions, selection failures and full manifest accounting. Add
zero-aware cardinality error and a frozen-plan observed-cardinality replay for
the access/ordering work that can be reconstructed. Separate input visits,
output rows, probe multiplicity, startup and planning metadata work.

Use the production cost-composition owner for re-costing, with an independent
reference in deterministic controls. Keep unobservable work marked unavailable.
Changing cardinalities for diagnosis must not regenerate a different plan or
update production statistics.

Entry: timing scope, operator identities and input/output/loop conventions are
known for each instrumented family.

Exit: reports distinguish CE, cost-formula/coefficient, search-space and
cache/layout problems with replayable counterexamples. Same-plan duplicates,
deliberately inverted synthetic rankings, zero rows, ties, partial cardinality
replay, timeouts and ordinary-plan failures have deterministic controls.

Verification: reference metric calculations, zero-aware arithmetic and failure
accounting in appropriate existing engine/tool owners; review real pilot
counterexamples separately from synthetic metric tests.

## Phase 4: Offline fitting and a separately reviewed policy change

Dependencies: phase 3 diagnosis showing a coefficient problem and sufficient
independent work features.

Scope: fit nonnegative relative weights offline; freeze template/distribution/
scale holdouts before fitting, preserve CPU normalization and saturation, and
publish scaling, objective, identifiability, environment and raw-record inputs.
Re-score frozen candidates before executing a candidate coefficient policy.
Add a missing cost term only when a scoped workload and observed work justify
it; do not compensate for missing work with an unexplained I/O multiplier.

Entry: agreed model/timing scope, reproducible baseline records, a frozen
holdout manifest and explicit promotion cohorts. If these are absent, retain a
diagnostic finding and improve collection instead of changing engine defaults.

Exit: offline fit and holdout report with all cases accounted for. Any proposed
default change is its own main-targeted implementation PR, with complete result
parity, reviewed plan goldens, non-worsening aggregate regret and the specified
paired regression gates. Uncertain cohorts limit the qualification claim.

Verification: engine arithmetic and scalar-conversion regressions, deterministic
fit/aggregation controls, and source-bound release measurements on the declared
host/residency/cache cohorts. A lower training loss is not this exit gate.

## Phase 5: Scoped join and vector extensions

Dependencies: the first-version runner and diagnosis protocol. These are separate
deliveries and do not block the four-family diagnostic milestone.

Join scope: freeze legal alternatives, then add actual build/probe cardinalities,
locator fetch, materialization and spill work. Enter only for an active Mem
workload; exit with complete equal results and independently checked work.

Vector scope: compare exact/Filtered DiskANN strategies against the same complete
eligible domain and source generation. Bind K/rank window, metric, dimensions,
candidate/beam settings, cold/warm state and a minimum recall requirement to an
exact oracle. Enter with qualified quality/latency collection; exit only when
each feasible candidate meets the quality contract before latency ranking.
Latency alone cannot qualify an approximate strategy.

## Ownership, execution and review

Production `hawdb` remains the library facade. Tool code stays private and
unpublished; no new crate is required by this plan. A manual CLI wraps typed
Rust entrypoints rather than driving the engine through shell commands.

Do not add Shiro/fuzz/tool tests or the measurement campaign to CI or schedules.
Use existing CI for compact engine correctness controls, retaining complete
feature matrices. Keep repository-wide compiler profiles, fixture assertions,
timeouts and isolated measurement gates. Update TEST_INDEX with actual owners,
scope and replay commands when each executable stage is added.

Retain campaign artifacts under the existing benchmark-evidence conventions:
exact revisions, manifests, raw samples, recomputed aggregates and limitations.
Bind each review to the tested source. A validated report, an accepted model,
passing CI and a merged policy change are separate outcomes.
