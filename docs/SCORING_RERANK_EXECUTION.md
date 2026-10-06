# Scoring rerank execution

This records the physical scoring operator's resource contract for
[issue #293](https://github.com/nowledge-co/hawdb/issues/293). It is not the
ordinary Cypher attachment, cache/template contract or Mem route migration;
those remain part of that issue's acceptance boundary.

`ScoringRerankExec` validates its `ScoringSpec`, captures time once for the
operator execution, and evaluates every candidate before selecting its result
window. A final K never becomes an upstream candidate limit. Equal combined
scores retain input order, including through spills. A zero result window
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
output. Both paths propagate consumer stop/error and release their accounts.
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
guards exercise in-memory output payload limits and ownership transfer.
Blocking inventory coverage includes a scoring parent over nested Sort/TopN.
The former tuple-sort helper test now exercises the production scoring path
with the same ranking and zero-window assertions.

Use the registered executor/core targets and existing local sort/transform
differential campaigns. Browser-target Clippy is compilation/lint evidence;
it does not establish browser runtime acceptance or a Mem consumer migration.
