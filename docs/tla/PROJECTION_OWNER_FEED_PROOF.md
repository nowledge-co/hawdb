# Bounded owner input at one immutable end

This component exposes V1 owner facts through the embedded library. It does not
implement stage-bound C..E coverage, host mapping qualification, or activation.
The predecessor [source-fact contract](PROJECTION_OWNER_CAPTURE_PROOF.md) is
committed as `66d37efa`. The source-qualified cursor results are recorded below.

## Contract and shared implementation

`Database::begin_prepared_projection_owner_feed(C, reads, total_feed)` creates
one owned PreparedRead at E, then captures the owner's paged log from that same
snapshot. It accepts C only when

```
max(legacy_resume_floor, owner_v1_floor) <= C <= E.
```

The first inequality implies both floor requirements separately. In particular,
a legacy checkpoint's larger V1 fence cannot be replaced by its smaller legacy
document floor. The constructor charges the existing binary-seek probes before
walking events. Source identity, schema barriers and history pins are the
existing PreparedRead contracts; arbitrary caller-supplied resume ordinals are
not accepted.

[The cursor](../../crates/storage/src/store/search_projection_change_log/cursor/owner.rs)
wraps the same private state machine used by the legacy document cursor.
Owner mode selects each V1 owner vector in epoch order and skips legacy document
IDs; both modes retain the same relational table/key, admission, error, callback
and End transitions. Missing V1 data or relational RequiresRebuild refuses
before exposing that transaction. The public legacy enum is unchanged.
Repeated physical owners in separate transactions remain separate inputs.

`GraphOwner(epoch, node_id, before)` borrows the source record. `before` is
NotRecorded, Absent, or transaction-start labels with absent/string/unsupported
ID. It is **not** a post-state action: creation's Absent preimage must still be
hydrated at E, and a deleted node is resolved by its stable physical owner.
An update at B and deletion at E therefore both query the same absent E node.
Old parent identities remain available for later unfiltered child-key mapping;
this component does not perform that mapping.

Callbacks receive the same immutable ProjectionFeedRead interface as the legacy
feed. Parameterized Cypher/SQL and typed graph hydration stay in the existing
query runtime. `graph_label_id` resolves a known mapping label against that
same catalog without cloning catalog metadata; finished dependencies always
validate the schema barrier, even when the callback performed only metadata
lookups. An absent label also records the exact named node domain. Ordinary
node creation may introduce a label without an explicit DDL/schema stamp; the
absence domain catches that path. Present label IDs need no node-domain guard.
Unrelated property writes do not require a whole-epoch fallback. There is no
mutable PreparedRead/raw transaction escape.

## Admission and prefix argument

For an owner with n old labels and a string ID of m UTF-8 bytes, the logical
borrowed-input costs are:

| Preimage | Work | Bytes |
| --- | --- | --- |
| NotRecorded / Absent | 1 | 17 |
| Present, absent/unsupported ID | 1 + n | 18 + 4n |
| Present, string ID | 1 + n | 26 + 4n + m |

These are explicit feed units, not wire-format lengths, allocator reservations
or RSS. Relational key-field measurement and table/commit/End costs use the
existing kernel. A callback's own computation, key cloning, cross-statement
work and staged output require independent admission.

Let p be the private event/part ordinal and D the delivered sequence. Initially
D is empty and p denotes the first record after C. Inductively, every emitting
transition admits cumulative and step work, bytes and one item before invoking
the callback. It appends exactly the input at p and advances p only after
callback success. Nonemitting metadata transitions advance only the relevant
private part. Yield preserves p. Therefore D is always exactly a prefix of the
selected immutable input, including repeated owners.

Work charged before byte deferral remains charged. If the same input is
measured next step it is charged again. All costs above are nonnegative, and
counters use subtraction of remaining admission before addition; no successful
transition exceeds the per-step or cumulative limits. End has its own admitted
callback after the final transaction and is installed only after callback
success. A failed/ignored query, callback error, panic or shared-source poison
prevents finish. Repeated visits after successful End emit no second End.

This is a conditional inductive argument over the private state machine and
valid immutable records, not a verified Rust refinement.

## Finite owner-input model

[HawDBProjectionOwnerFeedCursor](HawDBProjectionOwnerFeedCursor.tla) specializes
the shared frame protocol to two transactions for the same physical owner,
followed by table/key/commit/End frames. It varies old-label count (0 or 2),
string byte count (0 or 6), relational key width and total admission. Owner
costs use the native string-preimage formulas above. An independent byte
oracle, in addition to the existing work oracle and immutable-prefix check,
detects uncharged labels/ID payload. Constructor rejection is a terminal failed
state when C=1 is below the independent owner floor (0 or 2); the legacy floor
is zero. This finite case supplements the general max-floor argument above.

TLC completed **6,560 generated / 4,384 distinct states**, depth 25. Twelve
fault controls reset totals, refund deferral, omit metadata work, skip input,
ignore errors, drop End, read mutable input, admit before bounds, trust only the
legacy floor, undercharge preimage work/bytes, or coalesce repeated owner events.
Each violates its registered safety invariant. Two negative progress witnesses
reach completion and completion after deferral. All 15 new targets and the two
existing feed/capture model targets passed (17/17).

The existing [generic feed model](PROJECTION_FEED_CURSOR_PROOF.md) and
[capture model](PROJECTION_OWNER_CAPTURE_PROOF.md) retain their separate scope.
The new model does not enumerate all lengths, live retention management, source
catalog semantics, Rust aliasing, physical query I/O, caller computation,
durable publication or the full host mapping. These finite results do not
prove a Rust/TLA refinement or complete C..E coverage authority.

## Label-absence dependency model

[HawDBProjectionLabelLookup](HawDBProjectionLabelLookup.tla) separately models
lookup of present Memory or absent Later at the source snapshot, followed by
implicit label creation, explicit DDL, or an unrelated Memory property write.
Accepted observations must still match the catalog; the unrelated property
write must remain publishable. Implicit creation changes the named node-domain
stamp; explicit DDL changes the schema barrier. Four negative controls omit the
absence guard, omit the schema barrier, read the wrong domain, or substitute a
whole-epoch barrier. Each produces its expected invariant violation. A separate
negative success witness establishes a reachable accepted publication.

All six targets passed; the positive model explored **14 generated / 14 distinct
states**, depth 3. An initial modeling error omitted parentheses around
the Boolean assigned to accepted, accidentally disabling refused transitions.
The whole-epoch negative control did not fail, detecting this vacuity. The
assignment was corrected without changing the invariants. This model covers
this metadata lookup contract, not complete C/E catalog binding or arbitrary
catalog mutations, and does not establish a mechanized Rust refinement.

## Native evidence so far

Five owner storage tests and seven existing cursor/page tests passed together.
They exercise owner-only deletion, borrowed string/label/key pointer identity
after live trim/append, repeated owners, the larger upgrade floor, a 300-owner
plus 300-key transaction split, preimage deferral, cumulative exhaustion,
callback errors/panic and rebuild refusal. The generic budget state machine's
existing legacy pointer/deferral/error tests remain unchanged.

Six embedded consumer tests passed under both default and minimal features
in Cargo and Bazel. The cold consumer covers checkpoint/reopen and
original Database handle closure. The consumers check original Thread
identities plus fixed-E graph/SQL hydration after later writes, private
single-item steps and End, ignored query failure, unfinished or failed End, a
graph DDL gap, and a synchronized competing writer through the same actual
DatabasePublicationOwner while hydration is paused. The metadata regression
executes actual prepared-search publication: implicit creation of a previously
absent label refuses while an unrelated property write publishes.

Four private native tests passed for source poisoning before traversal/after
End, pinned scalar label lookup with subsequent schema invalidation, absent
label domain invalidation, and avoiding a whole-epoch fallback for unrelated
writes. An independent pre-fix public probe reproduced the incorrect accepted
publication after implicit label creation; this was an actual dependency bug
in the new uncommitted lookup API. The first combined matrix was interrupted
after discovering that defect (114 passed / 17 skipped, exit 8); it is not a
complete passing qualification. Its full storage action passed 1,942 tests
with 30 existing ignored tests, before the lookup fix. Final source-qualified
Bazel/fuzz and lint results are recorded below.

## Final component qualification

The combined selected Bazel command passed **137/137 targets** (27 executed,
110 cached): the default Storage suite, 14 default/minimal embedded consumer
targets, the evidence-script test, all 98 targets from the three required local
fuzz suites, and 23 selected model targets. The Storage action reports **1,942
passed / 0 failed / 30 existing ignored**. The new default/minimal owner-feed
actions each ran all six tests, including cold reopen and actual missing-label
publication rejection. Four private facade tests also passed. This is selected
component qualification, not all workspace tests or all formal models.

The command used the pinned Rust toolchain and unchanged Bazel configuration
with `--repo_env=CARGO_BAZEL_ISOLATED=false --local_test_jobs=1 --jobs=4
--test_output=errors`. Its target inputs were the Storage suite, six preceding
MVCC consumer pairs plus the new owner-feed pair, evidence script, the required
`//crates/fuzz:hawdb_fuzz_tests`, `//crates/fuzz:hawdb_fuzz_cli_tests` and
`//:hawdb_linux_ci_fuzz_smoke_test` suites, the existing generic-feed/capture
models, and the owner-feed/label models with all their controls. Runtime, tests
and models were frozen at tree `3a5bf17b5bc4ed36564cc41fceeafcce88ec07ec`;
only result/scope prose changed afterward. Logs, XML and cache-status files for
all 137 targets and all 23 model evidence directories were retained separately
from the initial interrupted run. Wall time was 1,283.760s, critical path
355.84s; cached action timings are not new executions or performance evidence.

Pinned `cargo fmt --all -- --check`, strict workspace/all-target/all-feature
Clippy with `-D warnings`, and both `prek run --all-files` hooks passed.
The independent public probe that previously accepted stale label absence now
returns a typed dependency conflict on the exact Later node domain. No test
assertions, admission limits, timeouts or durability defaults were weakened.

## Remaining authority boundary

An empty SQL table creation need not advance the owner floor or produce a row
key. End therefore does not prove complete table classification. A native C..E
session must independently bind catalog/schema and mapping versions and prove
complete source-owner effects, including old-parent fanout and independent
vectors. It must also bind all mapped output to the stage and pinned E.

Do not union C's whole-epoch dependency into final validation and call that an
incremental solution: it would reject the legitimate C..E changes. Conversely,
validating only touched E owners misses new/previously-unselected owners after
E. Final source-scope summaries, capture retention/leases, baseline/target
incarnation, output receipts and final visibility remain separate obligations.
A future delta must not delete the union of every intermediate old document ID.
For example, C has A→a and unchanged B→b, while A moves a→b→c before E. Deleting
a and b then inserting c loses B. A qualified stable-owner baseline or a proven
first-C-identity derivation is required; mapper namespaces, eligibility and
uniqueness constraints determine whether derivation is valid. Those premises
must be checked against actual Mem mapping before choosing an incremental
storage format.

The same-owner consumer fixture is library composition, not the actual Mem
outer-owner migration or a bounded activation-time measurement. Actual Mem
adoption, short atomic publication, lost-response recovery, physical power-loss
composition, RSS/I/O and original 1/4/8-writer gates remain open. Mem Search ACK
stays disabled.
