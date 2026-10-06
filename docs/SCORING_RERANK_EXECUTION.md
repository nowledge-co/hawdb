# Scoring rerank execution

This records the physical scoring operator's resource contract for
[issue #293](https://github.com/nowledge-co/hawdb/issues/293). It is not the
ordinary Cypher attachment, cache/template contract or Mem route migration;
those remain part of that issue's acceptance boundary.

`ScoringRerankExec` validates its `ScoringSpec`, captures time once for the
operator execution, and evaluates every candidate before selecting its result
window. A final K never becomes an upstream candidate limit. Equal combined
scores retain input order, including through spills. Signed zeros compare as
numerically equal for scoring: only its ordering expression normalizes zero;
the output column retains the original arithmetic sign bit. A zero result window
does not execute the candidate source. Invalid specifications and non-finite
combined arithmetic fail before any ranked output is emitted.

The scoring source checkpoints cancellation before and after evaluation,
accounts incoming batches, and reserves/checks the scored row before adding
its score column. `ScoringSpec::evaluate_score` and the full diagnostic
`evaluate` share one arithmetic implementation; the scalar path allocates no
per-row contribution, decay or missing-feature vectors. Missing features keep
the existing neutral arithmetic; callers needing provenance use `evaluate`.

Ranking uses the existing TopN heap, query/blocking accounts, spill admission
and merge implementation. In-memory Sort and TopN output transfer ownership
to `AccountedBindingBatch`, with the same row and payload caps as spilled
output. Both paths propagate consumer stop/error and release their accounts. Resident
rows beyond a parent output cap are destroyed and their retention charges
released before the final batch asks a parent transform to admit output.
Scoring memory reports use `ScoringRerankExec`; blocking inventory traverses
the shared plan-child contract, including scoring inputs.

The existing `ScoringSpec`, `Binding` and `QueryStreamOptions` fields and
physical operator signature remain intact. This resource correction does not
introduce product ranking weights or change SearchIndex RRF. The binding
feature source still reads its declared score column and returned numeric
property values; actual expansion-hop and graph-seed provenance are not yet
attached. A request-wide time anchor and coefficient/cache rebinding also
remain outstanding.

## Regression coverage

The resource guards exercise row admission, cancellation without upstream
checkpoints, zero K, complete candidate consumption, stable ties, late winners
from combined properties, invalid/overflow arithmetic, and actual resident
versus spill equivalence with released admissions. Separate Sort and TopN
guards exercise in-memory output payload limits, ownership transfer, all four
completion/stop/error/cancellation exits, and tight-budget parent admission
after a smaller output cap.
Blocking inventory coverage includes a scoring parent over nested Sort/TopN.
The former tuple-sort helper test now exercises the production scoring path
with the same ranking and zero-window assertions.

Use the registered executor/core targets and existing local sort/transform
differential campaigns. Browser-target Clippy is compilation/lint evidence;
it does not establish browser runtime acceptance or a Mem consumer migration.

## Required-test admission

The protected object is the physical scoring and Sort/TopN execution contract
above, together with the executor transfer caps in `ExecutionMemoryConfig`.
The oracle layer is actual production kernel output, complete ranking, source
consumption, independent stable expected rows, query-ledger ownership and actual
spill/run release. Public `ScoringSpec` diagnostic arithmetic remains a separate
core oracle.

| Guard group | Behavioral RED | Unique terminal evidence |
| --- | --- | --- |
| Scoring blocking admission, cancellation, zero window | Old main `1a3e532d` with unchanged new guards: three assertion failures | Over-budget success, cancelled source read, zero-window source read |
| Resident Sort/TopN output payload and ownership | Old shared production: both guard payload assertions fail | Changed resident output helper; expands the same guards with stop/error/cancel and ledger release rather than inferring them from spill |
| Blocking inventory and scoring report | Previous inventory/report production: missing nested operators and incorrect TopN attribution | Scoring parent traversal and operator attribution, beyond ranking-only tests |
| Invalid specification / arithmetic overflow | Separate precise validation and finite-result bypasses at `396a6c2`; unchanged guard fails on source-read count 1 vs 0 and unexpected success, respectively | Reject invalid specs before reading, and reject finite-input overflow before output |
| Complete stream / combined-property late winner | Precise candidate cap at two and SearchScore-only bypass at `396a6c2`; complete output becomes ordinals 1,0 instead of 1,2 and winner 0 instead of 3 | No seed-only early truncation or loss of graph property contribution; old-code positive PASS is not relabeled RED |
| Signed-zero ties and score bits | `396a6c2` production plus new guard chooses ordinal 1 instead of 0 | Numerical ties across batch boundaries and K=1/2; original reported score bits; expanded native oracle also checks zeros through actual spills |
| Capped resident parent admission | `396a6c2` production plus new guard rejects ProjectExec at 1,079 bytes against a 1,024-byte cap | Callback admission while discarded child rows must already be uncharged, rather than only checking eventual cleanup |

Focused replay/verification uses the registered executor unit owner:

```sh
cargo test --locked -p hawdb-executor --lib transform::tests::scoring -- --nocapture
cargo test --locked -p hawdb-executor --lib blocking::sort::tests -- --nocapture
bazel test //crates/executor:hawdb_executor_tests --nocache_test_results --test_output=errors
```

The complete-stream bypass keeps all inputs and terminal assertions; only the
assertion order was changed in the replay so the output oracle runs before the
source-request diagnostic. Bypass worktrees restore production files exactly.
RED and GREEN are behavioral outcomes, not compiler failures or assertion
mutations. The final exact head, commands and receipts belong to the delivery
packet and PR; the wider required fuzz result remains a separate gate.

CI owner is the existing hawdb-executor unit target in
`ci/skein-bazel-test-crates`; implementing owner is @hawkingrei, with the requested
human contract review recorded in the PR. The BUILD source glob and unit-suite
registration already discover these tests; no CI job, retry, feature or timeout
is added. Existing manual differential owners stay local-only. On the recorded
macOS focused run, scoring's eight tests took 1.66 seconds and sort's nine
ordinary tests 7.03 seconds (one manual test ignored). These are observations,
not CI p95 or an incremental before/after claim. The small guard additions fit
the existing unit target budget; CI duration still needs its own receipt.

Nonspill cases use isolated in-memory ledgers. Native spill qualification uses
one exclusively created synthetic temporary directory and removes only that
directory, with independent run-release checks. It covers the original ranking
case and signed-zero case under both resident and forced-spill budgets. A
one-entry TopN whose row fits its blocking cap cannot naturally spill; K=1 is
qualified on the resident path, while larger windows prove the spill comparator.
Minimal WASM compilation covers portable guards, but it does not prove browser
runtime, filesystem behavior, or a Mem query route.

An independently existing DISTINCT output analogue remains tracked by
[issue #880](https://github.com/nowledge-co/hawdb/issues/880). Its resident and
spill emitters do not enforce the transfer payload cap or use independent
output ownership. SQL directly consumes these kernels; Graph prepared dispatch
independently re-bounds transport payload. This correction does not claim the
DISTINCT kernel or all blocking operators are repaired. The owned follow-up
requires actual resident/spill RED/GREEN, complete DISTINCT semantics, parent
caps, stop/error/cancellation and downstream admission with ledger/run release.
