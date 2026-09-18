# Embedded generation context contract

The embedded `hawdb::SearchOutOfCoreGenerationWriter` exposes two additive methods:

```rust,ignore
SearchOutOfCoreGenerationWriter::create_with_context(root, options, task_context)
SearchOutOfCoreGenerationWriter::prepare_delta_with_context(reader, delta, options, task_context)
```

They use the existing `hawdb::RuntimeTaskContext` type. `create` and `prepare_delta`
retain their default contexts; existing options, reports and query entrypoints
retain their type identity and behavior. Configure lexical terms and manifest
bytes through the existing writer methods or reader configuration. No separate
runtime, helper process or query route is introduced.

## Operation ownership

The task follows create/push/finish, or prepare/finish, until the operation and
its private stage drop. Its `memory_bytes` reservation bounds admitted Rust
capacities and named native allowances across input/options, spool, segments,
descriptors, vector and lexical artifacts, analyzer worker/TLS, retained terms,
spill/merge progress, discovery/publication and cleanup. Delta conversion and
ordered hydration share that same operation ledger. Capacity remains charged
until the payload's final owner releases it, including replacement overlap.

The host must acquire shared admission before constructing the task and retain
that host permit through finish/drop. A copied numeric reservation does not
acquire process capacity or coordinate another operation. With no reservation,
the ledger has no additional finite operation ceiling; component limits still
apply. This synchronous build does not acquire the task's storage I/O-wave
controller. Serial work and the one joined analyzer worker do not introduce an
independent background scheduler.

The ledger is not a process RSS cap: fixed administrative account metadata,
allocator/OS overhead and the shared default analyzer dictionary are outside its
payload accounting. Pinned native workspaces have named qualified allowances;
dependency upgrades require requalification. Errors and the existing owned
reports become caller-owned at the return boundary. `result_bytes` does not
create a separate lease for these reports, and readers opened later own their
own limits and resources.

## Cancellation, failure and publication

Cancellation/deadline checks are cooperative around native analyzer, vector,
compression, filesystem and serde calls, with additional checkpoints within
controlled loops. A syscall or opaque native call already in progress is not
preempted. One analyzer worker is joined before releasing its admitted workspace,
including unwind and native TLS destruction.

Failed input poisons a writer; finishing it cannot publish partial input. A
cancelled or failed prepare returns no update. Unfinished writer/update drop and
caller unwind discard the private stage, with cleanup capacity retained before
stage creation. Filesystem cleanup failures remain best effort.

Successful active-manifest replacement is the commit fence. Cancellation before
it fails the operation; cancellation afterward cannot undo publication and may
set the existing cleanup-retry report. Optional old-generation cleanup denial
does not fail a successful commit. Existing filesystem durability errors after
rename can be ambiguous; this interface does not redefine that recovery contract.

Preparation captures the reader's term policy, manifest budget and generation
identity. It validates analyzer, embedding and import/source provenance. The
returned update does not borrow the reader; later reader changes cannot alter
the snapshot. If another writer publishes first, the stale update fails before
publishing. Complete range, compressed and inflated integrity is mandatory for
base hydration, even when a consumer fails after staging only a prefix.

Registered consumer directories remain exclusive to their consumer owner. A
generation writer checks the binding under publication exclusion using the same
operation task and budget. Rejection releases exclusion so the registered
consumer can continue checkpointing.

## Evidence and scope

`tests/search_generation_context.rs` imports only the embedded facade and std.
Default and minimal-feature callers compare all retained files, complete
hydration, exact query results and reports against the original default
entrypoints through repeated updates and deletes. The minimal feature set must
return the explicit unavailable-full-text capability error for text queries;
build/update/delete and complete hydration remain verified in that profile.
Failure cases cover stopped
and unadmitted tasks, spare input capacity, poisoning, caller unwind, abandoned
updates, stale generations, incompatible identity, reader policy snapshots and
consumer publication exclusion followed by a successful consumer checkpoint.
Owner tests retain exact/one-short admission, allocation/native lifetime,
commit-fence and negative-control evidence for each integrated stage.

See the [resource foundation](SEARCH_BUILD_RESOURCE_OWNERSHIP.md),
[analyzer](SEARCH_ANALYZER_WORKSPACE.md), [terms](SEARCH_TOKEN_OWNERSHIP.md),
[spill](SEARCH_SPILL_OWNERSHIP.md), [publication](SEARCH_PUBLICATION_OWNERSHIP.md),
[cleanup](SEARCH_CLEANUP_OWNERSHIP.md) and [delta](SEARCH_DELTA_OWNERSHIP.md) contracts.

This interface does not complete #392's adaptive large-source lifecycle. The
4 MiB source guard, finite term and 256 MiB manifest defaults remain. Shared host
feedback belongs to #186/#385; #206 still requires its hash-verified original
full-corpus comparison. Local synthetic fixtures do not satisfy that gate.
