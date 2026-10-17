# Endpoint-complete owner capture, contract V2

This strengthens the native source-input primitive for a future complete C..E
session. It does not qualify a mapper, parent/child fanout, business document
identity, independent vectors, stage coverage, target incarnation or activation.

## Native source correspondence

The [pre-WAL collector](../../crates/storage/src/store/search_projection_capture.rs)
now records both physical endpoints of every relationship create, property
update and delete, independent of relationship type or built-in Search-label
eligibility. Node effects and typed relational-key capture retain their previous
contracts. Existing physical IDs use NotRecorded; same-transaction-created
endpoints use Absent. A prior node rename/delete preimage retains precedence.
Repeated endpoints deduplicate within a transaction, while every effect charges
work. Nested creates followed by property updates/deletes borrow the same WAL
identities. Existing relationships use hot identity fields or the cold canonical
identity header. Endpoint capture does not clone endpoint/relationship property
bodies. Unknown/tombstoned relationship identities refuse capture with a gap;
unknown catalog type or storage integrity errors fail closed and poison.

The existing cumulative work/byte/depth admission and gap publication apply.
The prepared complete record or gap still installs only after WAL/canonical
commit through the existing transaction path. A failed append does not install
prepared owner inputs. No new WAL framing or synchronization policy is added.
Header reads and metadata estimates are not whole-query I/O/RSS guarantees.

Legacy Search effects remain separate. Creates/deletes of HAS_LABEL retain the
existing built-in invalidations. Relationship property updates add native owner
facts but no legacy upserts/deletes: the built-in mapper ignores those fields.
This avoids needless document rewrites, including replacing external vector
state with a lexical reconstruction. Other relationship types add endpoint facts
without inventing legacy Search semantics. Extra retained owner events can
consume history budgets and cause an earlier conservative rebuild boundary.

## Persistent qualification boundary

[The checkpoint writer](../../crates/storage/src/checkpoint/owner_capture.rs)
emits `projection_owner_capture\t2\t<floor>`. The row/preimage encoding remains
V1 (`projection_owner_change_v1` / `projection_owner_effect_v1`), because its
shape did not change; the header's capture contract is stronger. Record counts,
ordering, declared owners, floors and the existing outer integrity envelope
remain validated. Readers restricted to capture header 1 reject header 2.

New readers parse and validate old V1 facts, then discard their owner authority
when [recovering the log](../../crates/storage/src/store/search_projection_change_log.rs).
Canonical transactions and legacy change records are retained. The V2-exclusive
floor becomes the old checkpoint head, just as when a checkpoint has no owner
extension. Later WAL effects are captured under the current implementation.
A successful new checkpoint preserves this fence and only writes qualified
facts above it; writing header 2 does not upgrade the earlier interval. This is
proof-version classification, not a source-schema compatibility migration.

## Conditional interval argument

For each successfully committed graph relationship effect, the immutable source
identity is either known (including a same-transaction create) and both endpoint
owners enter the transaction record, or admission/identity uncertainty publishes
a gap at that epoch. Thus any accepted interval above the maximum capture,
retention and legacy floors contains every such endpoint effect. A checkpoint
preserves complete rows above its floor or fails integrity validation; reopening
an older contract moves the floor to its head. No partial prefix can claim the
new contract. Retaining both endpoints is necessary but does not by itself prove
which projection documents depend on them.

## Finite model and qualification

[HawDBProjectionEndpointCapture](HawDBProjectionEndpointCapture.tla) abstracts
three source epochs, both endpoints, create/property/delete, built-in versus
other types, owner-only records, admitted capture versus gaps, failed WAL,
retention, immutable captured intervals, V1/V2 checkpoints, dropped rows and
recovery. Its positive direct TLC run checked 105,468 generated / 28,224 distinct
states, complete depth 15. Nine fault configurations and an upgrade witness
produce their expected invariant counterexamples. The pinned tool jar SHA-256 is
936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88.
This is not mechanized Rust refinement or physical power-loss modeling.

Initial native selection passed 16 capture tests and four checkpoint tests,
including arbitrary-type endpoint create/property/delete after checkpoint and
both residency modes, same-transaction birth facts, unknown identity and V1
upgrade refusal. The strengthened capture selection passed 18/18 and checkpoint selection 4/4.
It adds HAS_LABEL property-only legacy preservation and repeated endpoint work
exhaustion. After removing two redundant Copy-type clones from the test fixture,
the same selections passed again (9.48 seconds / 0.01 seconds). The seven-test
owner-feed integration passed (9.04 seconds), including a new one-item-step
property-event test in hot/cold storage: both endpoints are delivered, native E
End is acknowledged exactly once, and reads retain E's value after a later
property write. The scoped-query integration passed 7/7 (27.07 seconds). Strict
locked workspace/all-target/all-feature Clippy passed after the Copy fix. These
are component checks, not full final-source Storage/Bazel/fuzz qualification.
The minimal profile passed owner-feed 7/7 (7.04 seconds) and scoped-query
7/7 (28.97 seconds); strict Clippy passed again after the newest facade test.
These timings are observations, not benchmarks. Full final-source Cargo Storage
passed 1,953 tests with zero failures and 30 existing ignored tests (537.48
seconds), plus one compile-fail doctest. Runtime/test/model tree
`64feed46277ba1bd2124147d291a47a2387b625d` stayed fixed for the final Bazel
matrix; only result prose changed afterward. All 189 selected targets passed:
121 executed and 68 cached, 2,633.308 seconds wall and 359.54 seconds critical
path. This includes 18 default/minimal facade targets, the evidence-script test,
98 required local fuzz targets and 72 selected formal targets. Final Bazel
owner-feed profiles passed 7/7 each (5.57 / 7.13 seconds), and scope-query profiles
passed 7/7 each (18.55 / 16.97 seconds).

Separate Bazel Storage selections passed capture 18/18 (1,965 filtered), checkpoint
owner capture 4/4 (1,979 filtered), and read dependencies 32/32 (1,951 filtered).
These filtered runs are not full Bazel Storage coverage. The local archive keeps
567 matrix log/XML/cache-status files and all 72 model evidence directories.
The evidence output group was explicitly materialized for cached actions;
module/configuration bytes and the pinned tool digest were checked against the
current source. Final formatting, strict locked workspace/all-target/all-feature
Clippy and all-file prek passed before commit. No assertions, limits, checks or
durability defaults were weakened. The separate in-progress Search cleanup
component is not part of this qualification. The scope-query work has separate
[model and native evidence](SCOPED_GRAPH_READ_PROOF.md).

## Remaining native C..E requirements

A native session must bind C-ready custody and source identity/schema to E,
consume all native inputs and mapper fanout before closing its private stage,
and validate complete final-E dependencies. Declared parent keys other than the
captured graph `id` still need adequate preimages or another qualified closure.
Even with every endpoint, arbitrary metadata changes require a qualified reverse
fanout plan. Semantic producer selection across graph/SQL mirrors must precede
strict final document-ID uniqueness; the old in-memory delta's call-order
behavior is not that contract. Independent vectors, binding/target ABA,
durable lost-response receipts, actual Mem adoption, physical resource/power-loss
composition and original 1/4/8-writer latency/fairness/group-fsync gates remain.
