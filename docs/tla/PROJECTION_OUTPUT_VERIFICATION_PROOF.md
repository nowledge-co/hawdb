# Final-output checks while the source snapshot is queryable

This adds a native closed-run verification phase for #231/#232/#1043. It is a
prerequisite for complete source-coherent publication, not that publication
protocol. Baseline certificates, complete capture/mapping contracts, unique
source ownership, binding/incarnation and independent-vector authority, durable
operation receipts, actual Mem migration and the original performance/recovery
qualification remain required.

## Custody and sequencing

`ProjectionStagingSession::verify_outputs` consumes successfully closed/ordered
intake into `ProjectionOutputVerificationSession`. The fixed verification
function is installed once. The wrapper owns the original session, queryable E,
private stage, cumulative mapping reservations and retained governor permit.
There is no ordinary-session escape or mutable stage/cursor access.

Each `advance` visits at most one final mutation or explicit End. The native
Search reader owns its position across quanta and validates its complete run
extent, record count, frame checksums, ordering and aggregate digest. The callback
cannot supply an ordinal, skip a mutation or claim a processed count. Native
counters advance only after callback success. All final upserts and deletes are
visited after duplicate reduction; the callback also acknowledges End, including
an empty output run. Repeated advance after completion does not replay End.

`ProjectionOutputVerificationContext` exposes only tracked Cypher/SQL from the
same E snapshot. It shares calls, statement invocations, reserved rows and
payload bytes with mapping; switching phase does not reset admission. Per-call
query caps still apply. The wrapper header is reserved from the stage's retained
memory ledger before ownership. Unlike the mapper, this context has no stage
output or continuation methods. A verifier needing more than the admitted
queries for one final mutation refuses; this API does not suspend arbitrary Rust
callbacks or prove host termination.

Search moves its reduced run into a private inspection reader. Artifact
construction refuses while inspection is incomplete. After successful explicit
End, it returns that same run to finalization; it does not create a caller-owned
replacement list. A failed callback, ignored tracked-query/comparison error,
panic, cancellation, impossible quantum or exhausted cumulative budget keeps
the attempt unusable. Resources/permits remain held while the failed owner
exists and release through its normal cleanup. Cancellation is checked before
and after host verification and again before artifact construction.

Only after these checks does `finish` close the source query context and build
the ordinary invisible candidate. The resulting `ProjectionStagedInputs` keeps
its candidate and dependency metadata private and reports the native inspected
mutation count as `Some(n)`. Ordinary `finish_staging` reports `None`. Neither
form implements the sealed Search publication trait. The count is observation,
not coverage or baseline authority. Later activation must revalidate recorded
versions and sealed contract identities; it must not replace E with a live read.

## Exact document comparison and limits

A borrowed `SearchMutationInspectionInput` exposes the native document ID and
delete kind; `id() == None` is End. For an upsert, `require_document` streams a
host-derived expected document's complete canonical encoding against the held
spool record. Body, title, metadata and embedding all participate. Equal lengths
and CRC32C values do not replace byte equality. The comparison reads the complete
record and verifies its checksum, using admitted fixed buffers rather than
materializing the staged body. A second comparison, comparison against delete
or End, and ignored comparison failures poison the callback.

The existing ordering ledger charges inspection reads, record visits and work
cumulatively. A record's conservative decode-plus-optional-comparison cost must
fit the quantum before callback dispatch; prefix inspection is retained across
yields. Charges reflect actual reserved logical reads, not the conservative
preflight ceiling. Filesystem operations, buffered reads, expected-document
construction/measurement, host CPU, source snapshots and total RSS remain
outside a complete physical-cost or latency bound. Query reservations cover
outer statement invocations; internal JOIN guard queries retain separate limits.
Artifact construction and activation remain blocking.

The host's fixed verifier remains a trusted semantic contract. It must derive
expected payload/owner scopes correctly and check unique ownership and logical
absence for deletes. An arbitrary callback returning Ok does not prove those
facts. This phase provides custody and tracked checks, not a way to bless an
unknown baseline or omitted source effects. Full-source baseline comparison and
contract qualification must precede authoritative coverage activation.

## Conditional argument and finite model

Assuming immutable private runs, correct existing decoding/order reduction and
tracked-read collectors, and a semantically correct fixed verifier:

1. Closed intake prevents later mapper output. Native forward traversal and
   explicit validated End establish that every final reduced mutation received
   the installed verifier, in order, without a caller-controlled prefix cursor.
2. The original prepared source remains owned through that traversal. Every
   verifier query reads E and contributes dependencies to the same source token.
   In particular a logical owner lookup returning no rows captures absence;
   validating only a deleted physical node ID would not cover replacement owners.
3. A failure or cancellation cannot produce successful opaque completion. The
   same run returns to artifact construction only after successful End. Its
   existing frame/digest validation is rerun when constructing the artifact.
4. Phase changes retain cumulative admission and the actual permit. This bounds
   the stated logical reservations and retained-stage ownership, not all memory.
5. Completion still cannot invoke visible publication. Baseline, effect coverage,
   external authorities and durability need additional composition evidence.

`HawDBProjectionOutputVerification.tla` abstracts two final mutations and End,
preexisting mapping reservations, a pinned source and competing live mutation,
fixed verifier identity, failure/cancellation, source close, opaque completion
and retirement. The positive model checks **183 generated / 106 distinct states**,
depth **8**. Eleven exact-invariant fault controls and one completion witness all
return expected TLC exit 12: SkipMutation, ReadLive, DropPinEarly, ChangeChecker,
ResetAdmission, IgnoreError, IgnoreCancel, PrematureFinish, PublishInspected,
ReleasePermitEarly, BeforeOrdering, and ProgressWitness.

TLC 2.19 / TLA+ tools 1.7.4, Java 21.0.9; jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
The registered action is `//docs/tla:HawDBProjectionOutputVerification_check`;
all twelve controls are registered in `mutants/mutants.txt`. The first development
run had a sequence-token parse error and is excluded. This finite conditional
model does not establish Rust refinement, semantic host correctness, complete
mapping/ownership, byte-level power-loss behavior, physical cost or liveness.

## Native evidence

The all-feature facade staging suite passes **12/12** (30.10s), including the
original eight intake cases and four new verification cases. They cover hot/cold
E queries after later writes/checkpoint; four final reduced upserts plus explicit
End; final delete/absence versus a newly created live owner; shared cumulative
query admission; premature ordering/verification finish; ignored query/comparison
errors, panic and cancellation; and permit retention/refund through failure.
The empty C=E path also requires native verification End, reports zero inspected mutations and charges its separate End callback. Old visible Search documents, generation and watermark remain unchanged.

The all-feature Search inspection selection passes **5/5** (1.61s), covering
one-work quanta without callback replay, exact final artifact preservation,
partial walk refusal, callback failure/panic and ignored/second comparisons,
premature ordering and cumulative I/O refusal, explicit empty End, a 64 KiB body,
metadata/embedding mismatch and a known equal-length/equal-CRC collision.

A source-private library regression passes **1/1** (0.45s), checking recorded
logical-owner absence dependencies: a new physical node with the same business
ID rejects validation, while insertion into another indexed ID remains valid.
It does not activate the opaque token. These checks prove the new traversal and
read-dependency seams, not repair of the legacy visible partial-prefix path.

## Final source qualification

The frozen-runtime Bazel matrix passes **137/137 targets** (45 executed / 92
cached; 1,712.947s elapsed, 325.00s critical path), invocation
`95d0670f-b8ba-413c-87a7-23b4a5490768`. Both final staging profiles pass **12/12**
(default 34.55s / minimal 38.76s native), including empty verification End and
successful-traversal counters. Search passes **852 / 20 existing ignored**,
publication **64/64 each**, JOIN **8/8 each**, owner **5/5 each**, full-source
**6/6 each**. Both graph-residency campaigns, all nineteen registered projection
models, the evidence-script contract and the three required local fuzz labels
pass. The command uses `--local_test_jobs=1 --jobs=4` and unchanged deadlines,
assertions and test selection. Debug timings are qualification evidence, not
performance or global RSS measurements.

Input tree `6f182fab8018ad122340991e7160c5b08ed55e8a` remained frozen throughout.
After the matrix, the exact final-source logical-owner regression was rebuilt
and rerun through `//:hawdb_unit_general_tests` with
`--test_arg=logical_owner_verification`: **1/1 native** passes; other partition
cases are intentionally filtered, not claimed as tested. Only this proof's
result prose changes after runtime verification. The pinned-toolchain
`cargo fmt --all -- --check`, workspace/all-target/all-feature strict Clippy,
and `prek run --all-files` pass. Changed-document relative links pass 28/28.
The earlier prototype's timeout evidence remains recorded in
[the staging proof](PROJECTION_STAGING_SESSION_PROOF.md); no deadline increase,
removed coverage, proven engine speedup or complete native activation follows.
