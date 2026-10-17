# Complete canonical SQL table read scopes

This component supplies a read-only invalidation identity needed by final-E
projection validation. It does not construct complete C..E coverage, authorize
an arbitrary mapper, or migrate the actual Mem publication owner.

## Native contract and source correspondence

A qualified canonical single-table SELECT keeps its existing necessary complete
primary-key or declared-index-prefix dependency when one is available. Otherwise
[the collector](../../src/api/dependency_read/relational.rs) records
`ReadRelationalTableDomain(schema.name)`. This covers all rows of the canonical
table, including an empty result, a LIMIT page, unsupported equality key types,
OR/residual predicates, and rows never returned by that query. Queries still use
the same pinned snapshot, SQL runtime, expression admission and row/payload
limits. Unsupported expressions/sources fail closed. JOIN qualification and its
existing conservative fallbacks are unchanged.

This identity is distinct from append-store `AppendTable`; that family is a
writer destination for the independent append storage API, not a SQL row scope.
`relational_table_domain_count` exposes the new read precision. It does not
certify complete source classification or output coverage.

[The native commit path](../../crates/storage/src/store/graph_commit.rs) first
collects and validates only writer destinations. After final mixed writer
validation, [the read-summary collector](../../crates/storage/src/store/read_dependencies/relational_indexes.rs)
uses complete staged row captures to stamp each changed table, before skipping
absent tombstone payloads. Payload-only updates, inserts and deletes therefore
all participate. It retains only the previous borrowed table name to avoid
repeated clones; unsorted/nonadjacent repetitions are also correct. No row
payload is copied for table identity.

The existing metadata work/byte admission applies before table-name ownership.
Every visited row charges work. Each newly encountered table name charges its
key estimate before cloning; the bounded VersionWriteSet then admits retained
key/write bytes. Incomplete index/row capture or metadata exhaustion installs
the existing ReadBarrier instead of claiming precise completeness. Domain
readers now check that barrier, as well as the existing schema/database barriers.
Opaque writer effects retain the existing Database protection. Conservative
fallback may still reject an unrelated write under capture/admission pressure;
this is observable as a typed barrier conflict, not an unbounded scan or silent
partial dependency. These are logical metadata estimates, not physical RSS/I/O
or query-work bounds.

The complete version-index candidate is admitted before WAL, then installed
with successful canonical publication. Read summaries are not fed back into
writer conflict validation, so disjoint same-table writer intents remain
independent. Failed or uncertain source application retains the existing
poison/fail-closed behavior. Read dependency registration starts before snapshot
capture escapes; summary collection is active even when the first query occurs
after a competing commit. Existing source identity, history pins and monotonic
schema/database barriers remain required.

## Conditional table-completeness argument

Let T(t,E) be all canonical rows of table t at captured epoch E. Consider any
successful later transaction that changes T(t). If its staged row captures are
complete and admitted, at least one row effect identifies t and its table stamp
is published at that transaction's epoch. A delete has an absent row payload
but still identifies t before the payload skip. If capture is incomplete or
metadata is refused, the shared read barrier is newer than E; opaque or schema
effects instead retain their existing barriers. Consequently accepted validation
implies no uncovered committed change to T(t) since E. This implication covers
new/unselected rows even if the original query returned zero or one row.

Unrelated-table or graph writes with complete admitted summaries do not change
the table's stamp or a global barrier. Their database epoch advance alone does
not invalidate this dependency. Precise point/prefix reads remain narrower:
they do not automatically acquire a whole-table dependency. Whole-source C
readers already holding Database remain conservative; this component does not
remove C's barrier or manufacture final-E authority by merging old dependencies.

This argument assumes correct complete staged captures and the existing atomic
canonical/version publication and pin discipline. It is not a mechanized Rust
refinement, a physical power-loss proof, or a proof of a host's mapping contract.

## Finite model and controls

[HawDBRelationalTableReadScope](HawDBRelationalTableReadScope.tla) explores two
same-table writers with disjoint destinations, two possible read scopes, and
insert/payload/delete/primary-key-move effects on an off-page row. Staged input,
successful WAL append, failed WAL, and canonical/summary publication are separate
transitions. Reads retain the source observation; scope validation considers
table, fallback and schema stamps. Mutation transitions stop at validation to
model the publication instant, not indefinite validity of an old observation
after subsequent commits. The publication authority/fence is a premise here.

TLC completed **5,816 generated / 3,392 distinct states**, depth 10. Ten fault
controls omit the domain, omit deletes, omit off-page effects, stamp another
table, ignore fallback, use a whole-epoch fallback, reuse summaries as writer
intents, stamp before WAL, read live data, or ignore schema. Each violates its
named safety invariant. Two negative witnesses reach valid publication after an
unrelated commit and both disjoint writers committed. All **13 targets passed**.
The initial model had an unparenthesized conditional assignment and failed TLC
evaluation; that run is retained and excluded from passing evidence. The source
argument applies at validation; no fairness, arbitrary cardinality, physical
flush ordering, durable receipt or Rust/TLA refinement claim follows.

## Native qualification

The complete default Cargo Storage suite passed **1,942 / 0 failed / 30
existing ignored**, 622.31s. The affected Bazel storage dependency selection
ran **26 passed / 0 failed / 1,946 filtered**, using
`bazel test //crates/storage:hawdb_storage_tests --test_arg=read_dependencies`.
The filtered Bazel run is not a second complete Storage run.

The final combined selected Bazel command passed **151/151 targets** (97
executed, 54 cached): 16 default/minimal facade targets, the evidence-script
test, all 98 targets expanded from the three required local fuzz suites, and
36 selected formal targets. The new table-scope targets ran **3/3 each** under
default/minimal profiles, including the stronger exact-table conflict
assertions. The complete publication consumers ran **64/64 each**, preserving
nullable-UNIQUE result completeness and the new dependency classification.
Default/minimal Cargo table-scope consumers also passed 3/3 each; the final
stronger default Cargo run passed 3/3. These are selected component checks,
not all workspace tests, all formal models or performance qualification.

The command used `--repo_env=CARGO_BAZEL_ISOLATED=false --local_test_jobs=1
--jobs=4 --test_output=errors`. It selected the seven preceding MVCC consumer
pairs plus the new table-scope pair, evidence script, the required
`//crates/fuzz:hawdb_fuzz_tests`, `//crates/fuzz:hawdb_fuzz_cli_tests` and
`//:hawdb_linux_ci_fuzz_smoke_test` suites, and the existing 23 selected
feed/capture models plus all 13 new table-scope targets. Runtime/test/model
inputs were frozen at tree `83aab8b641b5de43bb1cd131597d071cd09edb9e`; only
result/scope prose changed afterward. All 453 test log/XML/cache-status files,
36 model evidence directories, exact expanded command and source hashes were
retained. Wall time 2,622.517s and critical path 551.31s are qualification
observations, not performance or writer-scaling evidence.

Three storage regressions were extended for the table family: disjoint
same-table writers both commit, incomplete capture invalidates table scopes
through ReadBarrier while an unrelated point remains valid, and failed WAL
installs no early table stamp. Consumer cases cover hot/cold capture-to-query
gaps, unselected inserts/payloads/deletes/PK moves, empty-table phantoms,
unrelated SQL/graph writes, preserved point/prefix precision and sticky
entry/byte refusal.

Adverse evidence is retained. The initial complete facade run was **63 passed /
1 failed**: a nullable-UNIQUE test still expected the old whole-epoch fallback.
Its result-count assertions remain unchanged. Correct table/index classification
passed its isolated regression and both complete final facade profiles above.
The initial malformed model evaluation is excluded as described above.

Pinned `cargo fmt --all -- --check`, strict workspace/all-target/all-feature
Clippy with `-D warnings`, and both `prek run --all-files` hooks passed.
No assertions, timeouts, limits or durability defaults were weakened.

## Remaining integration gates

A native C..E session must bind complete source scope and its schema/mapping
contract to one private stage, native Ends, actual E and the held target/baseline.
Node domains alone do not guard relationship changes on unselected C owners;
final E scope must also cover the relationships actually used by the mapper.
A point query is not a complete table scope. A table scope is not a proof that a
host listed every canonical table, captured every parent/child effect, retained
independent vectors, used the right target incarnation, or produced all output.
New graph owners, binding/schema/instance ABA, history/capture retention,
fanout continuation, durable lost-response recovery, physical resource/power-loss
composition, short activation and actual Mem adoption remain required. The
original 1/4/8-writer and group-commit/latency/fairness gates remain open; Mem
Search ACK stays disabled.
