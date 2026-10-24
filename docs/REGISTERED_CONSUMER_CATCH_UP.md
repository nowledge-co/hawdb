# Source-coherent registered Consumer catch-up

Issue Number: ref #1138, ref #1043, ref #1106

The embedded `Database` facade connects fixed-source C..E preparation and full-E
coverage to stationary registered Consumer publication. It retains the same
logical ID, registration UUID and target lease across catch-up and verified
reopen. Use a versioned logical ID for the host's trusted projection definition.

## Versioned projection definition

Prefer `SearchProjectionDefinition::new(name, version, scope, initialize, delta,
full)` for new hosts. The name uses `SearchProjectionConsumerId`, and version is
`NonZeroU64`. Domain names must be nonempty; sorting and deduplication make order
and duplicate names irrelevant. Database scope, an explicitly empty domain set,
graph labels and relational table names remain distinct identities.

`Database::open_or_create_defined_search_projection_consumer` uses that
initializer. `begin_defined_search_projection_consumer_catch_up` accepts the
same definition and refuses a consumer with another derived ID. Its staging
session's `verify_complete_source(config)` fixes the complete mapper and scope;
callers cannot independently substitute either. `NowledgeMemEmbeddedStoreHandle`
exposes the corresponding defined acquisition and returns
`NowledgeMemDefinedSearchProjectionConsumer`, which retains the definition and
delegates catch-up, reservation, resolution, renewal and cleanup to the original
embedded consumer/owner.

A version/scope change needs a fresh target and full bootstrap. Reopening the
old target using its old definition continues to work after a refused new
binding. Mapper addresses are excluded from durable identity: hosts must bump
the version whenever initializer/delta/full semantics change. The definition is
host configuration, not a mapping certificate or a global process-memory bound.
Execution admission, exact full comparison and native owner/recovery fences
remain required. Raw APIs below remain available for explicitly managed hosts.

## Library flow

1. Acquire the projection with `open_or_create_search_projection_consumer`.
   Only an absent healthy registration invokes the trusted full initializer.
2. Supply `ProjectionConsumerCatchUpConfig` with explicit read/feed/mapping/sort
   limits and a task without resource controllers. The facade derives C from the
   completed receipt, captures E and supplies its original governor admission.
3. Call `begin_search_projection_consumer_catch_up`, then `advance` with explicit
   feed and ordering quanta. Input, mapping, continuation and sorting share the
   original cumulative limits and one fixed source E. Partial positions remain
   private, including positions inside a single source commit.
4. After staging completes, call `verify_complete_source` with explicit full
   enumeration, mapping and comparison limits and a fixed full-source mapper.
   Advance until `ProjectionCoverageStep::Complete`, then call `finish`.
5. Pass the opaque `ProjectionCoveredConsumerUpdate` to
   `publish_search_projection_consumer_covered_update`. Choose
   `RequireKnownDurable` for an already observed source barrier, or explicitly
   choose `Synchronize` after WAL replay or for an uncovered relaxed prefix.
6. Close and reacquire the same logical ID. Reopen verifies the original durable
   native selection and metadata; it does not run the initializer again.

The staging and coverage sessions borrow the immutable Consumer owner. They do
not borrow the database, so source writes can proceed between bounded steps.
The finished token owns its original native read pin and captured temporary
files, and releases the Consumer borrow before activation. It exposes no mutable
records, candidate substitution, arbitrary activation callback or partial ACK.

Staging and coverage use one background admission. Finishing coverage releases
execution and retains only the original native files, pins and governed memory.
An idle token occupies no CPU, background-task or I/O slots: a single-slot host
can create or reopen another Consumer, or prepare another update, before
publishing it. Publication obtains fresh admission from the same governor and
rebinds read-back and cleanup to that operation. Each I/O wave still requires
fresh admission. Dropping an idle token may admit bounded cleanup; refusal keeps
the existing funded native cleanup ticket for retry.

## Content and publication handoff

The existing complete-E comparison verifies the reduced delta against an
independently enumerated full-source stream and the exact selected base. Missing
deletes, unexpected old documents, duplicate full-source IDs, phantom upserts
and differing content fail the comparison. A poisoned or incomplete session
cannot produce a publication token.

Registered generation construction consumes the **same compared full-E stream**.
It rewinds the held immutable run, validates its extent, checksums, ordering and
count again, and populates the private checkpoint builder under a retained
resident-memory envelope. It does not invoke either mapper again. Temporary
sorting files are created through original captured native directories; records
keep their actual producer handles rather than reopening a diagnostic path.
The existing private checkpoint producer retains the namespace assumptions in
[stationary publication](CONSUMER_STATIONARY_PUBLICATION.md).

The facade checks the exact previous receipt, including database, projection,
registration, checkpoint, source epoch and metadata identity. It validates the
source instance, dependencies and observed durability before allocation and
again immediately before native selection. The storage reservation supplies the
original source branch, incarnation and expected previous selection. The native
view and durable receipt become complete together; only then is the disposable
registry cache acknowledged and published. An empty interval retains the exact
existing receipt and creates no successor.

Resident-size and retained-memory admission, and opening the held comparison
run, happen before reserving an allocation or creating a successor directory.
Repeated deterministic budget refusals leave the allocation ledger unchanged;
resident refusals retain typed `ResourceBudgetExceeded` diagnostics.

`SearchProjectionConsumerCatchUpReport.applied_batch_count` counts completed
whole-interval publications (zero for an empty interval). Its operation count is
the number of admitted delta emissions before repeated-key reduction; private
feed/ordering steps are not published batches.

## Refusal and recovery

`ProjectionConsumerCatchUpError` preserves lifecycle, feed and publication errors
as separate typed variants. `Stopped` preserves cancellation/deadline reasons.
If retaining a failed publication owner also fails, `RecoveryRetention` retains
the original typed failure as its error source and attaches the retention error.
Cancellation, incomplete intake, invalid mapping,
budget refusal, changed registration/base, or changed source cannot acknowledge
an intermediate watermark. Before selection the previous completed receipt
remains authoritative. A prepared publication failure can leave a retained native
operation; resolve it before starting new work.

After an uncertain selector/barrier or registry response, call
`resolve_search_projection_consumer_publication` with the live owner, or close
and reopen it through `open_or_create_search_projection_consumer`. Recovery
resolves the original operation, including after subsequent source commits.
It can recover a fully committed E even when the caller received an error.
Unattempted selections recover the old completed version. Runtime pins acquired
before the attempt keep that old immutable version readable.
If in-process pins survive handle reopen, configure that reopened database with
the same host governor; retained native ownership cannot migrate to a different
governor while the original pins remain live.

Private preparation is not persisted as resumable source progress. Cancellation
or restart discards private work and a new session starts from the selected
durable base. This flow does not renew, unregister, or silently rebuild a failed
registration.

## Costs and qualification boundary

Independent verification still compares every mapped document with the exact
selected base and complete E source. `ProjectionSourceScope::Database` retains
its conservative Database dependency and rejects every post-E canonical commit.
`Domains` records the declared label/table dependencies, including absence, and
can publish E after unrelated writes when those dependencies remain valid.
Mapping reads outside the declared domains are refused by the existing source
scope checks. See [source-scoped publication](CONSUMER_PROGRESS_AND_RETENTION.md).

Label-domain enumeration traverses actual pinned resident/delta records and
canonical record metadata instead of the historical allocated-ID range.
Canonical segments carry a label Bloom synopsis; unrelated segments can be
pruned without decoding properties. Work includes false positives, unrelated
resident rows, label checks and descriptor decisions. Each descriptor/segment load acquires a fresh I/O wave from the original
task; retained segment bytes and metadata reserve working memory before the
fetch. The underlying reader retains its own physical bounds. `feed_usage()` exposes
cumulative logical enumeration work; it is not an RSS or elapsed-time bound.

Registered publication continues to build a complete physical generation and
resident compatibility index. The split `prepare_covered_update` flow performs
that construction outside the original embedded write owner, then validates
and publishes through that same owner. The blocking compatibility publication
method retains its existing costs. Unchanged Search segment reuse was evaluated
in [the architecture qualification](benchmarks/architecture_2026_10_24/README.md);
no cross-generation sharing is enabled by this change.

The graph canonical descriptor value changes to version 2 for label summaries;
old development databases require explicit recreation and fail closed with their
files retained. Search format and native Consumer binding formats are unchanged.
This delivery does not authorize Mem Search ACK, stable artifacts, full Mem
activation, or waive the outstanding performance/platform acceptance gates.

Default/minimal `hawdb_mvcc_staging_session` tests own public graph/SQL, hot/cold,
one-item, full-rebuild equality, cancellation, budget, competing-base and reopen
regressions. Root Consumer tests own selector/cache lost-response injection and
original-operation recovery after later source writes. The covered family also
checks native generation refusal retires its birth, and strict foreground FD
pressure at seal/intent does not reject necessary internal admission. Injected
pre-selection failures retain the same prepared operation with zero idle
execution slots. The rename-observer cases require `test-support`. Native Windows runs both families
explicitly. Process reopen and deterministic failure injection are distinct from
power-loss qualification.

The covered path uses the recovery-owner handoff from PR #1156. It reserves
recovery metadata before allocation, passes an uncreated birth into native
preparation, and retains the returned original owner after preparation or
completion failure. The existing resolver retries or retires that same native
operation with fresh admission; idle recovery owners retain no construction
execution permit.
