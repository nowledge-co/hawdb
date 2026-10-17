# Complete pinned-source Search replacement

This implements a genuine complete replacement for a readable Search head with
unknown or lagged source watermark. It contributes to #231/#232/#1043; it does
not close their [full acceptance ledger](../specs/MVCC_VERSIONED_PUBLICATION_SPEC.md).
The current fallback requires the canonical Database to remain at captured C
through activation. Complete C..E composition is still required to allow ongoing
unrelated writes without repeatedly restarting full enumeration. This delivery
must not be reported as completing the outer-owner concurrency/performance gate.

## Native construction and complete set

`SearchGenerationAdmission::prepare_unordered_replacement` consumes actual shared
admission and creates a generation writer with the held reader's target root,
lexical/embedding policies, import provenance and expected active generation.
All reader policies, target identity and ID limits are copied at construction.
The full wrapper checks and consumes a pristine full sink, removes its unused
reader reference, and owns its state without a reader/Database/owner lifetime.
Ordinary deltas retain their necessary reader borrow and cannot enter this
conversion. The internal constructor explicitly omits old mutation/retraction state and its
disk reservation. The replacement wrapper refuses an incremental sink, has no
delete/delta API or mutable inner escape, and irreversibly enables unique upserts.
Its header and sorter share the admitted build ledger and permit. There is no
old-document body lookup or implicit old-content union during this construction.

The native source epoch is bound exactly once before input. A failed, unbound or
rebound writer cannot be reused. The facade rejects caller-provided build epochs,
captures full source under the stage's bound task context, then binds the actual
C; known future base watermarks refuse rather than regress. No arbitrary historic
source is opened from a numeric cursor. Damaged/unreadable heads remain integrity
errors: this API requires an existing validated reader and does not recreate an
empty database or certify uncertain evidence.

`ProjectionFullRebuildSession` owns the complete source frontier, fixed mapper,
cumulative calls/queries/reserved rows/payload, private unique sort and resource
leases. It shares the exact full-mapping helper with baseline comparison: each
native graph candidate or relational PK produces zero/one document, all canonical
tables including empty/engine tables must be classified, and table boundaries/End
cannot own documents. Ignored query failures, callback errors/panic and cumulative
refusal are sticky. No caller ordinal, vector or processed count certifies End.
At most one borrowed source item is offered before sorting backpressure; repeated
ordering steps preserve their cumulative work/read/spill budgets. Identical
payloads with duplicate document IDs from different source owners still refuse.

Only acknowledged source End plus closed ordering allow `finish`. The full source
snapshot then closes and retains its instance/history dependency proof; the
ordered complete run streams directly into a complete generation spool. Old
targets are never visited to create retractions. Existing single/empty generation
and private partitioned generation builders construct the full replacement.
`active_manifest_update` and mutation runs remain absent. Private partition
selectors remain under the private stage; the public final selector selects one
complete closure. Therefore IDs absent from the mapped source disappear,
including all old IDs when canonical output is empty.

Set argument: given the source cursor's complete native enumeration, a correct
fixed host mapper, unique output ownership and acknowledged End, let X be its
complete canonical document set at C. External sorting retains exactly one
record per unique ID. Direct full-spool emission produces X; complete generation
construction and selector installation do not union any old set Y. Hence the
result is exactly X even when Y contains extras, unknown provenance or stale
bodies/vectors. This is conditional on the existing codecs, private artifact
closure and source cursor invariants; it is not a formal Rust refinement.

## Fenced activation and recovery boundary

`ProjectionPreparedFullRebuild` privately owns candidate, source dependencies,
usage reports and an admitted metadata lease. It has no generic publication
trait or raw candidate conversion. `Database::publish_projection_full_rebuild`
consumes it through the existing sealed source-validation path. Under the same
exclusive Database borrow, the path checks instance identity, full-C Database
dependency, exact captured/staged watermark, nonregression and known durable
source prefix or explicit typed synchronization. The candidate independently
rechecks the expected active Search generation under publication exclusion before
installing any real-root artifacts, and retains exclusion through manifest-last
publication. This backend comparison assumes monotonic generations within the
same target incarnation; it does not independently fence a filesystem target
replacement/ABA that reuses the same generation. Host binding/incarnation remains
an explicit separate gate. Relevant and unrelated canonical commits both reject this initial
whole-C fallback. A changed Search head rejects without overwriting the newer
head, even if the source has not changed.

Cancellation before mapping, after ordering or after preparation refuses before
visibility. Failed sessions retain actual governor admission until cleanup;
successful opaque candidates retain it until activation/drop. Fallible setup
reserves before source escape; shared metadata and history/reader lifetimes must
still be included in the future complete retention/RSS qualification. Source
synchronization covers the complete captured WAL prefix and does not weaken
future `SyncOnEveryWrite` defaults or silently change relaxed write policy.
A recovered prefix is not evidence of a completed durability barrier.

The existing source/selector durability code supplies the current ordering; this
finite model and process reopen evidence do not prove byte-level power-loss
safety or atomic Graph/Search rollback. A response lost after a durable selector
commit can leave the replacement committed. This result is a consumed candidate,
not an idempotent durable operation receipt. No automatic rollback/retry is
invented. Composed lost/torn/reordered-write evidence and durable retry receipts
remain explicit acceptance work.

## Host authority and physical bounds

A successful generic full replacement is not automatically a versioned Mem
baseline. The fixed mapper's schema/capture/mapping contract, document ownership,
namespace and target binding/incarnation still need authoritative sealing.
Independent external-vector versions cannot be inferred from this database C.
The synthetic facade mapper deliberately emits no vectors; its vector fixture
checks removal of a stale vector-bearing old document under that synthetic
contract, not preservation/qualification of actual Mem external vectors. Native
replacement separately checks complete canonical bytes including original
embedding and metadata for an emitted vector document. Actual Mem must adopt its
complete contract before this path authorizes acknowledgment or cutover.

No parent fanout is needed for this full zero/one map: child PKs are independently
enumerated. Incremental parent effects still require complete captured/unfiltered
child membership and stable old identities. An ongoing C..E overlay needs explicit
phase/source-owner identities: ordinary same-E reduction cannot mix different C/E
bodies. Historical C matches cannot bypass final-E dependency checks.

Mapping and sort reservations are bounded logical admission, not measured total
physical CPU/I/O, numeric RSS or wall time. Canonical query physical work, pinned
source/reader retention and host document construction remain additional work.
Final artifact construction, installation, verification and synchronization are
still blocking and can scale with total content size. C..E composition, incremental
artifact construction, short publication, actual Mem outer-owner migration and
1/4/8 writer throughput/fairness/p50/p95/p99/lock wait/hold gates remain open.

## Finite model

`HawDBProjectionFullRebuild.tla` models empty/two-owner/duplicate-owner full input,
pinned C, strict cumulative map admission including End, unique sort, private
partition progress, complete preparation, independent canonical/instance/Search
head changes, source durability/synchronization, cancellation and resource
ownership including failed attempts, and early closure of the old Search reader
independently of the retained canonical source proof pin. Positive checking passes
**11,247 generated / 4,850 distinct states**, depth **14**. Fifteen faults and five witnesses all
return exact expected TLC exit 12 and named invariant: SkipSource, SkipEnd,
DedupOwners, ReadLive, IgnoreMapperError, ResetCredits, RetainOld, PublishPrefix,
IgnoreSource, IgnoreInstance, IgnoreBase, IgnoreDurability, IgnoreCancel,
ReleasePinEarly, ReleasePermitEarly; SuccessWitness, EmptyWitness,
PartitionWitness, WriteWitness and ClosedReaderWitness. The latter proves
publication can follow old Search reader closure while the canonical source
proof remains retained. WriteWitness proves that preparation permits
canonical progress; it does not assert that this conservative C candidate can
then activate. The model is conditional on source/mapping and backend closure,
and abstracts durable source as a barrier flag; it is not a physical power-loss,
host-contract, fairness or performance proof.

TLC 2.19 / TLA+ tools 1.7.4, Java 21.0.9; jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
Registered action: `//docs/tla:HawDBProjectionFullRebuild_check`.
All twenty controls are registered in `mutants/mutants.txt`. Initial local
controls had one Java temporary standard-module extraction race (TLC exit 150,
not an invariant counterexample). The corrected runner uses one Java temporary
directory per process; all twenty expected controls pass. Initial TLA parse
errors and a test-only metadata type error are excluded from passing receipts.

## Native evidence

Initial facade qualification passes all **15/15** tests (112.33s): seven existing
full baseline regressions plus eight full-rebuild cases. Full replacement covers
unknown/lagged/extra/vector-bearing old heads, hot/cold storage, forced one-document
partitions, independent complete-source comparison afterward, empty replacement
and reopen, source/instance/Search-head conflicts, duplicate owners, unclassified
tables, cumulative call/query refusal, ignored read error, boundary output,
panic, incomplete finish and cancellation before/after ordering and after
preparation. Actual governor counters remain admitted until cleanup and return
to zero afterward. Relaxed and recovered prefixes refuse implicit activation;
explicit synchronization succeeds, and later relaxed writes still require their
own barrier. Broad final qualification remains separate from these focused runs.

The exact final native replacement module passes **3/3** (1.35s), including a
previously mutated base closure, empty replacement, absence of retraction state,
exact emitted metadata/original embedding comparison, duplicate ownership,
unbound/rebound intake and rejection of an incremental sink. The opaque-result
nonpublication compile-fail doctest passes **1/1** (0.20s). Changed-document
relative links pass **32/32**; final broad qualification is recorded separately.

The initial borrowed-reader prototype also passes **141/141** targets (45
executed / 96 cached), 2,096.240s elapsed / 602.00s critical path; invocation
`38fea508-5b5c-4042-8bde-b18f1295a53a`, frozen tree
`6a8c8d4076741704bead31bc0031c490bb1c82e9`. Its Search suite passes 861/20
existing ignored. These results precede removal of the unnecessary reader
borrow and do not qualify that later source. They are retained independently.

The owned-session revision's synchronized consumer test passes **1/1** (5.20s).
A session escapes `DatabasePublicationOwner::with_read`, moves to a worker, and
waits while another worker acquires the same owner's writer lock, closes the old
Search reader and optionally commits a canonical write. Completion is received
before preparation resumes. Without a write the complete replacement publishes
and independently matches the full source; with a write the conservative full-C
fence rejects with a typed dependency outcome and preserves the old head.
Both session and prepared result cross thread boundaries. This proves owned
preparation composition, not actual Mem migration or unrelated-write activation.
The final owned native module passes **3/3** (0.92s), including ordinary delta
exclusion from the owned full-sink conversion. Final broad owned-revision
qualification remains separate from the borrowed prototype above.

The final owned-session facade qualification passes **16/16** (111.70s): the
seven baseline cases, eight replacement cases and synchronized owner/reader
release case all pass on the lean owned state. Build options move into the
admitted stage rather than being cloned and retained again by the session;
only the read/mapping limits and bound task remain there. The final opaque-result
compile-fail doctest passes **1/1** (0.28s). Format, workspace/all-target/all-feature
strict Clippy and `prek run --all-files` pass. The strengthened model including
reader closure passes 11,247 generated / 4,850 distinct states at depth 14, and
all twenty isolated controls pass. These focused receipts precede the final
owned-input broad matrix and do not substitute for it.

The final owned-runtime matrix passes **141/141 targets** (46 executed / 95
cached), 1,893.954s elapsed / 342.48s critical path; invocation
`44494fe9-495c-417f-b8d4-110d5f287744`, frozen input tree
`0b06c2abda2d55b3014922a4c16f5891a3eac4fb`. Both default/minimal baseline/rebuild
profiles pass **16/16** (75.99s / 80.54s native). Search passes **861 / 20 existing
ignored**, Storage **1,168 / 29 existing ignored**, publication **64/64 each**,
staging **12/12 each**, JOIN **8/8 each**, owner **5/5 each** and full source
**6/6 each**. Both graph-residency campaigns, twenty-one registered positive
models, evidence checks and required local fuzz pass. No timeout, assertion or
coverage was reduced. The subsequent owner-overlay design/model supplement
changes no Rust runtime/test/build inputs; its qualification is recorded below.
The design-only owner-overlay supplement passes **142/142 targets**, with
**1 executed / 141 cached**, 3.571s elapsed / 3.32s critical path; invocation
`301e6a3d-b6b6-4801-b5ec-2f1befbcc61d`, input tree
`d05d187ffd2407cd1eddfc53b34b90c7b0f68346`. The new owner-overlay model is the
executed action. This supplements the 141-target owned-runtime qualification;
it does not report a fresh execution of all 142 targets. Changed-document
relative links pass **37/37**.

These receipts precede integration with main's automatic checkpoint owner.
That integration starts at `1ee0d49844852412879a2d6e8667487e55cca990`, retaining
its checkpoint replay, canonical reader, source-durability fence and formal
controls. The replacement and owner-overlay additions were applied together;
the historical matrices above do not qualify the new combined source. Native
and Bazel requalification is required before publication of that source. The
merge's small-delta checkpoint pressure failures remain adverse evidence; no
full concurrency or latency gate is inferred from a focused rebuild test.

See [the separate conditional owner argument](PROJECTION_OWNER_OVERLAY_PROOF.md),
which is not a native C..E activation implementation.

## Combined main and cleanup-fence qualification

The combined runtime/model input tree is
`abd422eea144ed9d9d264f977532ec8d22dedfca`, based on `1ee0d49`. Subsequent changes
add proof-result prose, the refreshed Mem source audit, and retained conditional
oracles/probes; Rust runtime, registered test, BUILD, TLA and CFG bytes are
unchanged. The checkpoint cleanup capacity fix has its own source-linked
[conditional proof and native regressions](CHECKPOINT_CLEANUP_HEADROOM_PROOF.md).

The first 120-target run finished **118 passed / 2 TIMEOUT**, 40 executed / 80
cached; invocation `0c7d1ff0-b585-466e-b0ad-98dbb7b68054`, elapsed 21,984.699s /
critical path 1,014.79s. Default baseline/rebuild and fast fuzz timed out. System
power logs record repeated sleep/wake intervals: the action XMLs report
14,431.2s and 4,174.2s, respectively. Fast fuzz printed a native 44/44 result in
455.21s but remained an action failure. These observations do not establish a
code-performance cause, and the initial failures remain part of the record.

An exact-command retry, after the first process terminated and frozen source
hashes were checked, passes **120/120**, **2 executed / 118 cached**, 221.423s /
critical path 218.86s; invocation `6ba616b8-8285-48db-9986-f61a9fbacac9`. Default
baseline/rebuild passes **16/16**, 90.46s native / 90.6s action; fast fuzz passes
**44/44**, 127.95s native / 128.1s action. Original 300s/900s deadlines and all
assertions remain unchanged. Task-scoped prevention of idle sleep was used;
this is not a controlled uncontended performance experiment.

This matrix covers both profiles of publication, JOIN, full source, baseline/
rebuild, staging and owner integration; Search; all three required local fuzz
suites; eight selected positive models (full rebuild, owner overlay, cleanup
headroom, change capture, capture closure, source durability, prepared Search
publication and automatic checkpoint); and the evidence-script check. It is
not a fresh all-model or whole-workspace runtime suite. Search passes **861 /
20 existing ignored**, 507.83s native. The two graph-residency halves each pass
one native campaign with **16 seeds / 992 state checks**, 295.31s and 312.79s.

Additional combined-runtime checks pass: the selected automatic-checkpoint
general partition **84/84** (491.11s native, two threads), the separate completed
group wake regression **1/1** (15.56s), and the opaque-result compile-fail doctest
**1/1** (0.27s). Strict workspace/all-target/all-feature Clippy, format and prek
pass. These scoped receipts do not close the actual Mem migration, native C/E,
short activation, durable operation receipt, physical fault/resource or writer
scaling/fairness/latency gates.
