# Relational prepared reads: argument and finite boundary

`PreparedRead::query_sql_with_params` adds canonical relational hydration to the
same pinned source and dependency budget as prepared Cypher reads. It reuses
existing `VersionKey::RelationalRow` writer intents; it does not introduce a
second relational version index or wait until a query subscribes to keys.

## Qualified native contract

Only canonical public single-table SELECTs are admitted. System/virtual catalogs,
projection-bound snapshots, append tables, joins, functions, locking SELECTs and
non-SELECTs fail closed. The executed statement is the same prepared template
used for dependency analysis, with the same parameters and pinned schema/store.
SQL WHERE always precedes grouping, ordering and LIMIT; an outer bound is never
inferred from a later phase or another binding.

A complete primary-key tuple is qualified when each primary-key column has a
column/value equality reachable only through AND from WHERE, for the same table
or alias. OR/NOT do not supply a necessary bound. A necessary primary-key equality
can coexist with an independent OR residual. Contradictory equalities may retain
one necessary tuple, as in the existing runtime access planner; the predicate
still rejects the row. Incomplete/null/oversized keys and other admitted single-
table reads record an explicit Database dependency. That fallback requires exact
source epoch equality and is observable through the dependency introspection.

The input scalar uses `hawdb_relational::query_value::value_to_relational_as`, the
same coercion used by access planning and writes; UUID text is normalized into
the canonical typed key. Incompatible scalar types fail rather than creating a
key with different equality semantics. The existing primary-key schema order
constructs the tuple, independently of predicate order or optimizer access cost.

## Conditional collector argument

Assume a canonical captured instance and schema, existing complete write-intent
classification, one WAL publication order and pins retaining newer conflict
stamps. For an admitted complete tuple K:

1. Every row satisfying WHERE has tuple K. This follows from the necessary
   conjunctions; disjunction and partial composite keys cannot establish it.
   It holds for an empty result as well as a present row.
2. All data-dependent output therefore comes from the presence and complete
   contents of row K. A local filter, projection, grouping, sort or LIMIT cannot
   introduce another base row. Function/other-source shapes are refused.
3. Qualified insert, payload update and deletion already stamp K, including
   absent destinations, false residuals and net-zero/ABA intents. A primary-key
   move, cascade, opaque write or other unqualified mutation keeps the existing
   Database barrier. Schema changes retain the Schema barrier. These summaries
   are not new shared writer intents that would serialize disjoint writers.
4. Those writer stamps exist before capture and before the first SQL statement.
   Validation compares the latest K/Schema/Database stamps with the pinned epoch.
   If no newer relevant stamp exists, the complete result remains valid. A
   disjoint key write is admissible without whole-database epoch equality.
5. Multiple statements share the same deduplicated bounded write-set structure.
   Ignoring an entry/byte/query/admission error sets sticky refusal, so no
   partially captured dependency set can be finished into a usable proof.

`HawDBPrimaryKeyPredicate` independently checks necessary-key selection and
absent-intent replay stability. The new model adds composite identities, pinned
source/query timing, publication read validation and multi-statement refusal;
it does not replace the transaction/publication/durability models.

## Admission before recursive work

A 600-term OR regression exposed stack exhaustion before post-parse expression
qualification could complete. The prepared-reader now gates input **before**
template lookup, upstream parsing, AST lowering or recursive drop. It uses the
same PostgreSQL dialect/tokenizer as execution through the SQL crate's hidden
`admit_postgres_sql_syntax` seam; no alternate semantic parser is selected.
Quoted/escaped/dollar strings and comments must have the execution frontend's
meaning, rather than a raw substring count or an experimental lexer interpretation.

The wrapper bounds SQL plus scalar parameter ownership to 64 KiB and at most
1024 parameters. SQL input bytes bound tokenizer scratch before tokenization;
4096 non-whitespace tokens and 32 conservative recursive tokens (keywords,
operators and opening delimiters) bound subsequent recursive work. Keyword leaf
positions can cause conservative refusal. Post-parse qualification additionally
caps 1024 expression nodes and depth 64. Primary-key metadata has at most 32
columns and 4096 estimated owned bytes before copying key values; larger keys
use Database fallback. The ordinary configured row/payload/runtime limits still
apply to execution. These finite bounds are not a numeric RSS qualification or a
claim that all general SQL entrypoints now have the same recursive-work gate.

## Finite model and controls

`HawDBRelationalProjectionReadValidation` has four composite row identities (two
tenants by two IDs), absent/present payloads, two commits, snapshot capture before
later query choice, point/residual/partial/OR/NULL queries, up to two statements
sharing a one-entry budget, narrow/broad writes, schema barriers and pruning.
An unpruned history oracle and direct captured/live query outputs are independent
of the optimized stamp index and classifier fault switches.

The main model checks **722,438 generated / 303,063 distinct states**, depth 9,
with `MaxEpoch = 2`. Nine controls narrow an incomplete composite key, extract
through OR, omit absent keys, delay stamps until first query, omit payload stamps,
ignore broad/schema barriers, prune a pinned stamp, or forget budget refusal.
Each exits with status 12 and the exact `NoStaleAcceptance` invariant violation.
The tenth `DisjointWitness` control intentionally violates
`NoDisjointAcceptance`, proving a successful point validation after another key
commits is reachable. Parser/runtime errors are not counted as counterexamples.

| Model seam | Native source |
| --- | --- |
| Capture / Read | `begin_prepared_read`; `dependency_read/relational.rs` template qualification and complete primary tuple extraction |
| Point stamps / broad writes | existing `store/graph_commit.rs::collect_relational_version_writes` and canonical mixed commit publication |
| IndexValid | `store/read_dependencies.rs::validate_read_dependencies` with Schema/Database barriers and the retained source pin |
| Refusal / union budget | shared `GraphReadDependencies::record` plus `PreparedRead.failed` and `finish` |
| Accept | shared source validator retained through catalog or closed Search activation under the exclusive Database borrow |

Native consumers cover the capture-to-first-query gap, disjoint same-table keys,
empty-key insertion phantoms, payload/delete/ABA/schema conflicts, composite/OR/
NULL fallback, necessary points with OR residuals, UUID coercion, shared SQL and
Cypher budgets, output limits, oversized-key fallback, lexical quote/comment
handling and the deep-OR/flat-IN admission boundaries.

## Remaining refinement

This is a conditional mathematical argument plus finite checks, not a mechanized
Rust refinement proof. The model abstracts scalar types, SQL parsing/coercion,
physical paged reads, byte ownership and WAL power-loss behavior. UUID and budget
regressions support those native mappings without proving them for every input.
Existing whole-database barriers remain required for unqualified mutation paths.

Relational IN/range/join and table-domain precision, supported function families,
projection-bound reads and host batch/receipt predicates remain unfinished. Mem
catch-up still needs its prepared hydrator interface, outer-owner handoff, exact
covered-batch watermark, target incarnation and actual source-validated Search
activation. The full #231/#232 retention, fault and 1/4/8-writer performance gates
remain open; these qualified point reads do not complete the integration.
