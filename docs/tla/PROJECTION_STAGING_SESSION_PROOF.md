# Joint projection intake, mapper continuation and stage custody

This implements the native intake/mapping/stage lifecycle needed by #1043 and
#231/#232. It does **not** implement verified baseline authority, complete
stage-bound coverage activation, host binding/incarnation or external-vector
receipts. The full acceptance ledger remains open.

## Ownership and source map

`src/api/dependency_read/staging_session.rs` composes the existing pinned
`PreparedProjectionFeed` with the resumable Search unordered mutation writer.
One `ProjectionStagingSession` owns the fixed C..E feed, a fixed mapping function,
one admitted pending input, admitted opaque continuation bytes, cumulative
mapping reservations, and the private stage. No caller-provided count or resume
ordinal advances its source position. The underlying cursor may acknowledge
capture into the pending slot, but the session cannot capture another item
until its mapper returns Done. End must itself be mapped successfully, including
an empty C=E interval.

Constructor admission consumes `SearchGenerationAdmission`; numeric task memory
values alone cannot substitute for a live permit. Its unordered-update factory
binds the admitted CPU, memory and I/O ceilings, intersecting a caller's smaller
memory ceiling, and retains the permit in build memory through the private
candidate and immediate cleanup. Source reads use that exact bound context.
The ordinary
Search reader's watermark must equal C and the feed must admit C against E and
its retained floor. This numeric check is necessary but is **not** baseline
verification: an unknown head can enter private staging but obtains no activation
authority. Build options cannot supply a source epoch; the session sets it to E.
Existing Search expected-base checks still apply. The task context is shared by
source queries and sorting/cancellation. A failed step retains admission while
the failed session still owns private resources; consuming/dropping it releases
admission after cleanup. Constructor refusal also releases admission. The
external Search reader and retained source memory have their own ownership and
are not a measured whole-operation RSS bound.

Pending graph/delete/relational inputs are measured before cloning. PK width is
capped at 32 fields; total input metadata has a separate byte cap. The Search
stage's own retained-memory ledger reserves pending/continuation allocations;
a second ledger with the same nominal limit would not establish shared admission.
Continuation replacement admits the new bytes before dropping the old bytes,
so overlap remains charged. Pending data and continuation fields drop before
the stage and its cleanup/admission owners. Source snapshot/query allocation
is not included in this retained-stage ledger.

## Cooperative mapping and backpressure

Each advance performs one feed frame (internally at most one delivered item),
one mapper invocation, or one ordering quantum. A feed frame can consume only
metadata work; the Input progress variant does not promise a newly delivered
item. If ordering is busy, it advances before another mapper invocation.

The immutable mapping function receives the current borrowed input, its previous
bounded continuation bytes, and a context with only tracked queries and stage
operations. Its function pointer is retained for the session; there is no
per-step mapper substitution or arbitrary mutable read/stage escape. Business
mapping correctness and cooperative return remain trusted host premises; HawDB
cannot preempt arbitrary Rust code or prove that Done has the intended meaning.

One invocation can emit at most one upsert/delete and replace continuation at
most once. More retains the input; Done retires it. An invocation yielding More
without any query, output or continuation update refuses. Repeated meaningful
but nonterminating callbacks still exhaust cumulative call/query admission.
Native code cannot infer semantic progress merely from different opaque bytes.
The real Mem mapper still requires migration to this continuation protocol.

Before each query, the context reserves one cumulative invocation and the full
configured statement row/payload limits. These charges never refund across
empty results, failed preflights, ordering pauses or source inputs. The query
counter counts reserved invocations, not necessarily dispatched queries.
Per-call query limits force a fanout mapper to yield instead of running all
pages in one callback. Statements execute through the original parameterized
Cypher/SQL runtime and retain their own statement admission/dependencies.
Counts apply to outer tracked statement invocations. Internal guard lookups,
including the qualified JOIN collector's one-row / 4 KiB lookup, retain their
separate limits and are not additional charges in these counters.

These are logical invocation/output reservations, not cumulative physical
query fuel, scan/encoded-block I/O, arbitrary mapper CPU, transient host document
construction, complete retained-source memory or RSS bounds. Caller-created
documents precede the existing Search queue's retained ownership admission.
Sort work/read/spill limits retain their separate existing contract. Final
artifact construction and activation I/O remain blocking and unqualified for
size-independent critical sections.

## Failure and opaque completion

The session and tracked read helper mark themselves failed before entering
fallible work or host code. A mapper error, panic, ignored query/output/state
error, admission refusal or cancellation makes the session unusable. The mapper
context checks sticky failures even when the function returns Done. Cancellation
is checked before each advance, after mapping and before finish. Earlier private
outputs can survive a failed step only inside the failed session until cleanup;
they cannot become a successful token.

After mapped End, the native owner closes stage intake and drives ordering.
Finish requires acknowledged End, no pending input, successful ordering and a
usable source; then the existing blocking artifact builder creates an owned
candidate. `ProjectionStagedInputs` keeps that candidate and its dependencies
private. It implements no sealed Search publication trait and exposes no
candidate conversion. A compile-fail doctest verifies that passing it directly
to `Database::publish_prepared_search` is rejected.

The ordinary staging exit consumes the query context and retains read
version/history dependencies, not a queryable E snapshot. The complete protocol
must perform all E-dependent ownership/capture checks while the session still
owns that context, before closing it. In particular, inspect the closed reduced
mutation stream under native custody, record/prove each logical owner scope
(including delete/absence cases), and bind that verification to the same stage.
Only then may finalization reduce the source to dependency metadata. Later
activation revalidates sealed results and versions; it must not hydrate a newer
live database and treat it as E. The [native output-check phase](PROJECTION_OUTPUT_VERIFICATION_PROOF.md) now supplies this traversal with a fixed trusted verifier and shared logical admission; it still does not certify the complete host contracts or authorize publication.
A baseline certificate for C is also a prerequisite, rather than an assumption
that an ordinary Search watermark certifies its complete contents.

This is joint custody of the exact mapper outputs, **not** a certificate that
the base was complete, host source-owner IDs were unique, every business effect
was captured, or independent vectors/bindings were current. Future verification
must consume this exact stage rather than attach unrelated public vectors or
counts. Full-source enumeration remains a separate prerequisite for baseline
comparison; this C..E mutation intake is not a full replacement builder.

## Finite model and controls

`HawDBProjectionStagingSession.tla` abstracts a graph input with two mapping
continuations, a relational input and End. The source is pinned before a live
mutation; mapping, admission, stage backpressure, closure, ordering, error and
cancellation are explicit. Expected mapper semantics and the existing sort/
artifact kernel are assumptions. No visible publication action exists in the
positive configuration.

The positive model checks **478 generated / 312 distinct states**, depth **13**.
Eleven faults each return exact TLC invariant-violation exit 12 on their named
invariant: SkipPending, LoseContinuation, ResetUsage, IgnoreMapperError,
IgnoreCancel, EndFromFeed, ReadLive, PublishOpaque, SkipMemoryAdmission and
MissingClose and ReleasePermitEarly. One ProgressWitness control also returns 12, proving that
successful joint staging is reachable. The lost-continuation check detects
discarded native continuation even when cumulative admission would eventually
stop repetition. It is not a proof that arbitrary host code is correct or live.
`NoUnleasedStage` retains the permit through successful opaque completion and
failed ownership, until explicit retirement releases resources. It models the
declared admission lifetime, not total source/allocator memory accounting.

TLC 2.19 / TLA+ tools 1.7.4, Java 21.0.9; jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
The registered action is `//docs/tla:HawDBProjectionStagingSession_check`;
twelve expected-invariant configurations are in `mutants/mutants.txt`.
Initial development runs with a missing file path and an underspecified boolean
assignment are excluded; they are not successful controls or safety evidence.

The model is finite and conditional, not Rust refinement, arbitrary graph/SQL
mapping completeness, durable receipts, byte-level power-loss composition,
physical cost, external versions or fairness qualification.

## Native evidence and remaining delivery

Eight all-feature Cargo tests pass (8/8, 49.13s), plus the nonpublication
compile-fail doctest (1/1, 0.23s). They cover hot/cold graph plus relational
input pinned across later
mutations/checkpoint; three-step fanout with pending input retained across
ordering; cumulative calls/query/row/payload/input admission; ignored invalid
queries, ignored second queries/outputs and oversized continuation; no-progress
yield, cancellation, panic and premature finish; required task memory and native
watermark ownership/base matching; repeated row changes plus update/delete at
fixed E; and mapped End on an empty interval. Old Search generation/watermark
and document bodies remain unchanged through successful staging and drop.
These tests prove private-intake seams, not visible repair of the legacy
partial-prefix bug.

The admission lifecycle test observes the actual governor: a competing
background admission refuses while the session or opaque staged artifact lives.
After successful artifact drop, constructor refusal, or cleanup of an ignored
output error/panic, both active-background and admitted-memory counters return
to zero. A failed session continues holding the permit until consumed/dropped.
This checks lease ownership, not allocator/RSS usage or completed production
governance of all source/query allocations.

Final frozen-runtime Bazel qualification passes **136/136 targets** (40
executed, 96 cached; 1,621.422s elapsed, 510.34s critical path), invocation
`71dc7b32-4fc6-4f5c-b54f-fd40b935115f`. The command includes both new staging
session actions (8/8 each; default 28.0s, minimal 42.1s), all eighteen projection
models, relevant crate suites, the evidence-collector contract and the three
required local fuzz labels. It uses `--local_test_jobs=1 --jobs=4` and unchanged
action deadlines. The input tree was
`e658fc42dd986f43df8105a1a140d424b4ffdbc0`; subsequent edits only clarify this
proof and record results. Strict pinned-toolchain format, workspace/all-target/
all-feature Clippy and `prek run --all-files` pass. Separate allocation, scan and
fast-fuzz actions pass (3/3; allocation cached, scan and fast-fuzz executed).

An earlier pre-governor prototype matrix at tree
`8ed0ee8405aa5cc6ccc3fb00b1f110517b57c06b` passed 133/136, with default joined
reads, default publication and append seed 7 exceeding their unchanged 300s
limits under `--local_test_jobs=2 --jobs=6`. Those failures remain adverse
evidence. An isolated final-runtime recheck passes all three at the same 300s
limits (158.8s, 186.7s and 20.4s), and those entries are cached in the final
matrix. Reduced local concurrency is qualification configuration, not a proven
engine speedup, removed timeout cause or supported-platform performance result.

Still required: complete verified baseline comparison, capture/mapping and
unique source-owner contracts, external-vector versions and host binding/
incarnation, durable stage/operation receipts and idempotent activation; complete
query/fanout/physical and source-retention governance; incremental final artifact
construction; actual Mem outer-owner adoption; composed power-loss/cancellation/
lost-response recovery; and original 1/4/8-writer throughput, p50/p95/p99, fairness
and lock wait/hold qualification. The legacy catch-up path remains unchanged.
