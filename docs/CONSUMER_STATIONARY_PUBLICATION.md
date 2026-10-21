# Registered Consumer stationary publication

Issue Number: ref #1092, ref #1106

## Scope

Registered Consumers implement immutable generations on Unix and Windows. Exact
revision qualification depends on the native workflow evidence below. Creation
writes directly into a newly born final target; catch-up writes a fresh sibling
generation. Publication changes a selector in the source allocation ledger. It
does not move a directory containing a live reader or publication lock.

The embedded `Database` facade owns this lifecycle. Internal Storage and Search
APIs support that facade and are not additional application integration points.
The legacy unregistered directory relocation helper still refuses Windows with
`CapabilityUnavailable(SearchProjectionPublication)`. Hosts may explicitly
disable the registered capability on every platform. WASM persistent publication
remains unavailable.

## Ownership and publication

1. Capture the original source project/branch authority and target parent in the
   source's file descriptor domain. Reserve fresh monotonic incarnation,
   generation and operation numbers plus registration/projection/checkpoint UUIDs.
2. Durably record native directory birth and its complete binding. An occupied
   target is rejected before initialization. A pathname alone cannot grant custody.
3. Run the application's initializer against one pinned source snapshot and a
   private in-memory builder. The initializer must propagate hydration failures,
   retain the builder and default analyzer, and return a complete mapping.
4. Build a private checkpoint, copy the complete referenced artifact closure
   through the original native directories, seal metadata with full length/SHA256
   references, and prepare the immutable serving view and compatibility snapshot.
   Preparation and reader construction precede the final source fence.
5. Persist the publication intent, including the previous root. Synchronize the
   covered source recovery prefix, validate the original source and epoch E, and
   attempt the precomputed native selector while source publication is excluded.
6. Release the source runtime borrow before selector completion barriers. Publish
   the completed immutable view and receipt together. Then mirror that receipt
   into the disposable registration cache.

The first target's logical publication lease remains held across sibling
selections. The current generation also retains native publication custody.
Neither lock is released to permit a namespace move. Queries retain immutable
artifact handles and their resource owners. Dropping a Consumer releases runtime
exclusion; it does not unregister the durable registration.

The private checkpoint producer still uses the existing synchronous path-based
checkpoint writer inside its validated, exclusively owned scratch directory.
Original-directory checks run before and after construction, and its IO is charged
to the original source FD domain. The host must exclude concurrent raw filesystem
mutation of this private namespace. The final generation, selector, immutable
serving view and reopen verification use captured native authority. This delivery
does not claim to migrate every Search checkpoint writer to relative native IO.

## Host API

- `open_or_create_search_projection_consumer`: recover/reopen the exact durable
  registration or initialize a genuinely absent registration. Existing UUIDs,
  lease policy and cursor survive reopen; new options do not renew an old lease.
- `catch_up_search_projection_consumer`: hydrate against one pinned E, prepare a
  complete successor privately, and publish its native receipt. A partial batch
  that cannot prove the complete pinned E is refused before hydration (#1043).
- `resolve_search_projection_consumer_publication`: resolve a retained uncertain
  attempt with fresh admission. It resolves the original operation even after
  later source writes; it does not pretend that a lost response means rollback.
- `search_projection_consumer_read_permit`: obtain query admission from the same
  database governor used by retained reads and Consumer publication.
- `SearchProjectionConsumer::pin`: obtain `SearchProjectionConsumerReadPin` for
  the completed immutable version. Its governed `search_with_options` returns
  `SearchRetainedOutput`; both query scratch and retained results remain charged.
- `SearchProjectionConsumer::search_index`: fallible access to the immutable
  resident compatibility view. It returns Busy while publication is unresolved.
  Uncoordinated mutation/checkpoint entry points remain refused.
- `renew_search_projection_consumer`: explicitly persist the lease extension.
- `unregister_search_projection_consumer`: durably revoke the registration,
  release its logical registry capacity, and preserve physical files and old pins.
  Reusing the logical ID requires a fresh UUID and an unoccupied target.

An old pin remains readable across successor publication and owner closure.
Holding a pin does not retain an execution slot. New pins, compatibility reads,
catch-up and readiness refuse `HawDBError::ProjectionBusy` while the owner has an
unresolved selection. Ordinary queries against the source database can continue.

## Recovery and registration cache

`consumer-allocations.hawdb` is authoritative. It records original source/birth
identity, candidate seals, previous/next roots, completed outcomes, lease policy
and explicit registration revocation. The allocator high water never decreases.
Corrupt or missing initialized native evidence fails closed; it is not replaced
with an empty domain.

`projection_consumers.meta` is a bounded, checksummed **v2 cache**. Reconstruct it
from the native ledger after a lost cache response, missing file, corruption or a
foreign copied cache. Cache bytes cannot create a registration or advance a
cursor. Reopen verifies the original birth, complete binding, metadata reference,
manifest and every declared artifact before becoming Active. A missing artifact
requires rebuilding and is not silently repaired. Transient IO and resource
refusals propagate without making a registration permanently RebuildRequired;
verified integrity mismatches remain fail-closed. A source-only backup restores
source identity/data but does not import external Consumer custody; copying a
cache into that restore does not authorize adopting a newer external generation.

After an uncertain selector attempt, recovery compares only the recorded
previous/next operation and verifies the complete candidate closure before
completing the native outcome. It returns the selected E or the previous completed
root. A later source epoch cannot change the historical outcome. Failed/unselected
initial creation with retained evidence may require explicit rebuilding at a new
target; existing unpublished directories are not silently adopted or deleted.

Ordinary initializer errors attempt cleanup through the known-created native
scratch and retire an unsealed birth only after all producer owners close and
only the original birth marker remains. Cleanup resource refusal preserves
unpublished evidence. Allocation/debt storage is bounded (256 current or unresolved
rows, 1 MiB ledger). Completion keeps each registration's initial birth and current
selected root, compacts other completed predecessors, and unregister releases
completed logical rows. The monotonic allocator high water never moves
backward. Unresolved debt is retained and exhaustion returns a typed
`ResourceBudgetExceeded(MetadataEntries)` refusal. Successful catch-up is not
limited to 256 lifetime operations. Outcome flags are encoded before selection,
so the later completion/revocation flags cannot grow an admitted ledger image.
Compaction does not delete physical files: automated old-generation reclamation
remains a separate qualification/integration item from the 64-registration limit.

## Limits and remaining integration

Current defaults bound complete generation verification to 256 MiB, metadata to
1 MiB/4096 artifacts, and the conservative resident compatibility envelope to
64 MiB. The envelope is derived from the snapshot's declared decoded length,
encoded bytes and retained overhead; it is not an RSS measurement. Publication
uses an admitted 64-MiB working request and one IO wave slot in the database's
existing governor. Hosts still own initializer work and process memory policy.

Catch-up currently clones the completed resident documents and constructs a
complete generation. This is not the bounded native C..E producer required by
#1043, nor the throughput/fairness/lock-duration qualification tracked in #232.
Large-corpus construction cost and completed physical debt reclamation remain
explicit gaps; this document does not declare the full stationary specification
or all MVCC work complete.

## Verification ownership

- Root unit Consumer family: native create/catch-up/reopen, text/vector/filter
  reads, old pins, logical revocation, cache recovery, source-epoch refusal,
  occupied targets, namespace replacement and lost replies at selector boundaries.
- Search Consumer family: native target lease, complete metadata/candidate/read
  slot/reopen contracts, original descriptor budgets and cleanup refusal.
- Storage native-directory family: native rename/barrier custody, lost responses,
  torn/reordered power-loss models and original-handle cleanup.
- Native Windows workflow: positive registered creation and recovery stay enabled;
  the separate legacy relocation refusal remains an executable check. A competing
  FD owner exhausts unreserved capacity after opening the captured WAL writer;
  post-write native checks must use the capacity admitted before record writes.

Local regression fixtures include 260 actual catch-up publications while retaining
an old read pin, typed refusal for 256 genuinely unresolved allocation rows,
transient-reopen retry, and mid-operation cold branch refusal at a 32-FD limit
followed by a successful higher-budget retry. The long ledger fixture suspends
periodic automatic checkpoint publication to isolate allocation capacity; separate
checkpoint fixtures retain concurrency coverage.

Local GNU cross-checking is compile/lint evidence only. Native Windows execution
is required before positive Windows qualification is claimed. The owner's Windows
machine power-loss campaign is a post-merge follow-up; process termination and
modeled storage faults do not establish that machine/storage guarantee.
