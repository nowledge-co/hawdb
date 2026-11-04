# Optimizer cost qualification and calibration

Status: proposed, 2026-11-04. This document defines prospective acceptance
criteria. The candidate runner, cardinality replay and coefficient fitter are
not implemented by this documentation change. Existing optimizer and admission
contracts remain authoritative.

Tracking: [#216](https://github.com/nowledge-co/hawdb/issues/216).
Delivery sequence: [development plan](../OPTIMIZER_COST_QUALIFICATION_PLAN.md).
Incomplete work is tracked only in [TODO](../../TODO.md).

## Purpose and existing foundation

Determine whether the optimizer selects a fast legal plan for an active Mem
query, and distinguish cardinality errors, cost-formula errors and missing
alternatives. Coefficient calibration follows this diagnosis.

The source inventory was checked at
`118dcdbc6ddabc8bf5307ba4b5cd6be86a0c487b`:

- [The cost contract](../OPTIMIZER_COST_MODEL.md) retains raw CPU, random access,
  sequential access and output components. Its weights are planning units,
  not hardware latency coefficients. Relational access/join costs do not cover
  every later SQL stage.
- [Optimizer qualification](OPTIMIZER_PROOFS_SPEC.md) executes one ordinary
  Cypher read on a pinned snapshot and retains the actual selected trace.
  Seeded cost perturbation covers selected ordered-access and Sort/TopN
  decisions, not every access or join alternative.
- Shiro provides typed result comparison and replay in an independent,
  unpublished workspace. Existing arithmetic campaigns prove cost composition
  and selection consistency; these are not measurements of plan quality.
- [Persisted access measurements](../RELATIONAL_ACCESS_COST_LINUX_CALIBRATION.md)
  demonstrate cache/locality-dependent winners. Their historical source and
  environment do not qualify current coefficients.

## Initial scope

The first version covers bounded, read-only SQL and Cypher cases derived from
active Mem access patterns. Each family must declare its supported frontend
and actual executable alternatives.

| Family | Required alternatives and controls |
| --- | --- |
| Access selection | Full scan versus secondary-index access; include a primary-key point control, residual predicates and selective/broad predicates |
| Coverage | Covering versus non-covering access for the same projected result; retain canonical-row fetch work |
| Ordering | Legal ordered-index access versus unordered access plus sorting; retain direction, NULL, continuation and ordering proofs |
| Bounded ordering | Sort plus limit versus TopN; include offset, duplicate keys, empty input and a deterministic final ordering key |

Absent frontend support is `unsupported`, not coverage. The initial executable
milestone may cover one family in Cypher; completion of the first version
requires every declared family and its supported SQL/Cypher adapters.

Join-order/algorithm calibration, spill-cost calibration and vector backend
selection are later extensions. This proposal adds no production hint language,
environment control plane, new storage format, general plan executor or new
production crate dependency. Initial coefficient experiments remain offline.

## Candidate and execution contract

1. A comparison MUST retain identical statement text, typed parameter values,
   logical schema, data snapshot, access policy and output contract. Candidate
   selection MUST NOT rewrite predicates, move limits unsafely or change NULL,
   duplicate or ordering semantics.
2. The ordinary parser, binder, legality/property checks, physical lowerer and
   admitted executor MUST produce and execute every candidate. A qualification
   selector may choose a named legal alternative; it cannot supply an arbitrary
   physical tree or bypass semantic checks.
3. Freeze a bounded candidate inventory before timing. Record generated,
   deduplicated, pruned, selected and refused alternatives with reasons and
   enumeration limits. Missing alternatives are a search-space limitation;
   the measured winner is only the best observed candidate. For a covered
   family, retain legal alternatives before cost-based pruning where available;
   the current cost winner alone cannot define the comparison inventory.
   Limit the initial protocol to eight distinct candidates per query, including
   the ordinary selection, and report any truncation explicitly.
4. Record the actual executed physical fingerprint, including access/index
   identity, ordering and relevant physical properties. Requested hints,
   equal scalar costs or operator-name lists do not prove plan identity.
   Deduplicate equal fingerprints and report same-plan comparisons separately.
5. A fixture cell claiming alternative-plan coverage MUST execute at least two
   distinct legal fingerprints. The ordinary selected plan MUST be present,
   or the report must explain why the cell is incomplete.
6. Validate complete typed results before accepting timing evidence. Preserve
   multiplicities, NULLs and declared ordering. A digest identifies a result
   artifact; it does not replace complete result comparison. Truncation, early
   consumer termination and matching arbitrary errors are not agreement.
7. Every attempt receives equivalent explicit admission and output limits.
   Attempts use fresh scoped task admission under a bounded campaign, without
   resetting a failed attempt's counters. Concurrent writes are excluded from
   the initial campaign. Reopened measurement handles must bind to the same
   immutable fixture identity; a different snapshot cannot substitute for it.

SQL needs an explicitly qualified same-snapshot adapter and legal candidate
selection where these are missing today. The existing Cypher qualification
entrypoint must not be described as that already implemented adapter.

## Evidence model

The proposed report protocol is `hawdb-optimizer-cost-qualification-v1`.
Its implementation MUST preserve raw observations, not only aggregates.

| Record | Required evidence |
| --- | --- |
| Campaign | Engine SHA, harness SHA, schema/fixture digest, toolchain, target/CPU, features, repository profile, residency/configuration, budgets, candidate limits, sampling seed and complete case manifest |
| Query | Original statement and typed parameters, result semantics, snapshot/checkpoint identity, ordinary selected fingerprint and expected family |
| Candidate | Actual fingerprint, physical properties, raw cost components, total, coefficient-policy identity, cost scope and cardinality provenance |
| Attempt | Schedule position, status, phase timings, full-result verdict, operator estimates and actual counters, logical accesses, requested bytes, cache/spill/admission counters and available memory measurements |
| Aggregate | Raw-record references, repetition counts, exclusions with reasons, tie policy, ranking/selection metrics, coverage and verdicts |

Parameters and fixtures must be synthetic or explicitly sanitized. Memory
budget, allocator peak and process RSS are separate fields. Logical accesses,
unique/requested pages and physical device I/O must not be conflated. Missing
measurements are unavailable, not zero.

An operator observation must identify whether a counter is per-loop or total,
and whether timing is inclusive or exclusive. Parent/child inclusive timings
must not be summed into a query latency. Input visits, output cardinality and
post-LIMIT rows are separate observations.

Report statuses include `completed`, `not_run`, `unsupported`,
`enumeration_limited`, `plan_unavailable`, `semantic_mismatch`, `cancelled`, `timed_out`,
`admission_refused` and `infrastructure_error`. Keep every manifest cell in the
denominator and retain its status. A timeout is censored evidence, not an
invented latency or a reason to delete a difficult query family.

## Measurement protocol

- First validate each candidate and freeze its fingerprint outside the timed
  repetitions. Qualification preparation must bypass stale physical-plan cache
  selection while normal warm execution/cache reuse is recorded explicitly.
- Separate fixture/build/open, parse/bind/plan, execution and complete result
  delivery/consumption times. Report end-to-end latency as well. Cost comparisons
  must name the modeled scope; missing sorting/hydration/setup costs must not
  be silently fitted into an access-only coefficient.
- Warm cells use three unmeasured warmups followed by at least fifteen paired
  repetitions. Rotate candidate order with a retained seed and run candidates
  serially. Retain all samples and the selected ordinary-plan control.
- Fresh-handle cells use at least fifteen independently reopened attempts per
  candidate with balanced order. Record open/validation separately. Fresh
  handles do not establish a cold OS cache or cold physical device.
- Distinguish Materialized and OutOfCore, warm and fresh-handle, clustered and
  dispersed keys, narrow and wide rows, uniform and skewed predicates, and
  small/bulk inputs. Select and freeze representative cells rather than
  silently sampling an incomplete Cartesian matrix.
- Record host load and runner identity. Use the repository-wide release profile
  for performance evidence, retain first-build cost, and keep fixture setup
  outside execution timing. Do not change target-local optimization, deadlines
  or compiler profiles for this campaign.

The manifest must bound query/candidate counts, result and artifact bytes,
per-attempt resources and total campaign work/deadline. Exceeding a limit records
the refusal and marks any unexecuted cells `not_run` with the cause; it must not
drop those cells or silently enlarge the budget.

Repetition counts, seeds, tie policy and cohort definitions are fixed before
measurement. Any amendment creates a new manifest identity; all old observations
remain attributable to their original policy.

## Selection and ranking metrics

For a cell with equivalent, completed, quality-eligible alternatives:

```text
regret = median_execution_time(ordinary_selected_plan)
         / min(median_execution_time(measured_eligible_candidate))
```

Use one declared timing scope throughout a metric. Report geometric-mean,
median, tail and worst regret by query family and environment, paired latency
ratios, and
tie-aware within-query rank agreement such as Kendall tau-b. Cross-query
scatter plots are diagnostic; they cannot establish that each query's winner
was selected correctly.

Predeclare a 5% practical tie band and a paired 95% confidence procedure with
its resampling seed. Differences below timer resolution or with inconclusive
intervals remain ties/inconclusive. Count a reliable pairwise inversion only
when its latency-ratio interval is outside the practical tie band and the
predicted order opposes that measured order. Do not manufacture a strict winner
from noise or claim stable p99 from fifteen samples. Preserve the ordinary selected
candidate as its own control so observed regret remains interpretable.

Refused or incorrect candidates cannot be latency winners. If the ordinary
selection fails while an equivalent candidate completes under the same budget,
report a selection failure separately from finite regret. Empty candidate sets,
zero/nonfinite timings and missing ordinary-plan evidence cannot yield a pass.

## Separate cardinality from the cost formula

Compute zero-aware Q-error and bias for aligned estimated/actual cardinalities.
Report empty-input cases separately and document smoothing; do not silently
apply the planner's minimum-one estimate to measured zero rows.

Add an offline diagnostic that re-costs the same frozen physical plan with
observed cardinalities for the portions whose work can be reconstructed. Retain
the original estimate-based cost, substituted inputs, startup charges and
unavailable components. Actual output rows alone do not reconstruct scanned
rows, probe multiplicity, early-stop work or metadata reads. Partial replay
must be marked partial rather than a complete true-cardinality cost.

| Observation | Investigation |
| --- | --- |
| Observed-cardinality replay improves ranking | Cardinality estimation or propagation |
| Both rankings remain poor | Coefficients, formula shape or unmodeled work |
| A known executable winner is absent | Enumeration, rule applicability or pruning |
| Winners change across cache/layout cohorts | Residency/locality assumptions |

Observed-cardinality replay MUST NOT alter the executed plan, feed future-query
statistics or change ordinary serving behavior. It is a diagnostic record.

## Offline coefficient experiments

Fit nonnegative coefficients to retained raw work and measured latency only
after the preceding diagnostics identify a coefficient problem. Freeze training
and holdout manifests by query template, data distribution and scale; parameter
variants of one template must not leak across them. Publish feature scaling,
fit objective, sample weights, coefficient uncertainty and identifiability
checks. Collinear components require more discriminating workloads or fewer
free parameters, not an unexplained fitted vector.

Preserve CPU normalization and opaque scalar conversion, weight raw components
once, and retain saturation/startup composition. A fitted time scale is distinct
from relative planning weights. Fit only comparable cost/timing scopes and
explicit host/cache cohorts; one host's coefficients are not portable defaults.
Re-score the frozen candidates offline before measuring any changed selection.

The initial campaign is diagnostic: a structurally valid report may expose a
poor current model. Promotion of new coefficients is a separate implementation
change. It requires complete holdout accounting, no semantic/admission failures,
non-worsening geometric-mean regret in each required holdout cohort and no newly
selected plan with a reproducible greater-than-5% latency regression against the previous policy in
any required cohort. Use paired 95% intervals; inconclusive cohorts prevent a
claim that the policy is qualified across those cohorts. Golden plan changes
must be reviewed deliberately. Weight queries equally within each cohort;
publish cohort aggregates separately and freeze any cross-cohort weights in
the manifest before collecting data.

## Extension boundaries and test ownership

Join work needs independent probe/build multiplicity, row-fetch, materialization
and spill observations before its coefficients can be calibrated. Vector work
needs the same eligible domain, generation, K/rank window, metric and a frozen
minimum recall requirement against exact ground truth. An ANN plan with lower
recall cannot win on latency alone. These extensions do not block the initial
four-family diagnostic runner and are not covered by its acceptance.

Engine qualification seams and compact deterministic arithmetic, serialization,
candidate-identity and failure controls belong to existing Cascades,
optimizer-graph, optimizer-relational, executor and root owners as applicable.
Keep complete feature selections and default/minimal contracts. Update
[TEST_INDEX](../TEST_INDEX.md) when executable ownership is added or changed.

Reuse Shiro's private fixture/replay/comparison support for the explicit offline
runner where suitable. Shiro, its tests, fuzz campaigns and the performance
campaign remain outside ordinary CI and scheduled jobs. Production HawDB must
not depend on the tool or shell out to it. A CLI is a thin wrapper over typed
Rust qualification; this proposal neither creates a new crate nor registers
new CI lanes.

## Reference methods

- [OptimizerTester evaluation](https://github.com/qw4990/OptimizerTester/blob/3225bf7795d6da899f784ba4131b375e18a7ff05/cost/costeval.go)
  uses operator workloads, true-cardinality cost observations, timings and
  Kendall correlation. Its [CE metrics](https://github.com/qw4990/OptimizerTester/blob/3225bf7795d6da899f784ba4131b375e18a7ff05/cetest/estimation.go)
  motivate separate cardinality diagnosis.
- [tidb-cost-calibrator evaluation](https://github.com/qw4990/tidb-cost-calibrator/blob/ab73549eafce026b884c9f999278ca3367201c66/costeval/eval.go)
  retains factor weights and repeated timings; its
  [regression](https://github.com/qw4990/tidb-cost-calibrator/blob/ab73549eafce026b884c9f999278ca3367201c66/costeval/regression.go)
  fits nonnegative factors with relative error.

These are methodology references, not HawDB dependencies or evidence that this
proposal is implemented. Their TiDB formats, engine-specific factors, global
correlations and timeout handling do not define this protocol.
