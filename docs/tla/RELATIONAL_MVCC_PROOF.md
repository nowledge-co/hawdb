# Explicit relational write intents and MVCC

The supported narrow path consists of Insert (Error or Replace) and
DeleteByPrimaryKey over tables without non-primary unique constraints,
declared unique indexes, outgoing foreign keys, or incoming foreign-key edges.
Non-unique secondary indexes are allowed. Predicate writes, UPSERT, DDL,
unknown/malformed input shapes and opaque relational WAL keep a Database barrier.
This does not claim general per-key SQL UPDATE/DELETE or serializable isolation.

## Why net changes are not write intents

The existing RelationalPrimaryKeyChangeCapture is a changefeed contract: it
omits keys whose before and after rows are equal. It cannot establish a complete
MVCC write set. A replacement followed by restoration and a deletion of an absent
key still carry explicit write intent. Likewise, deriving keys from a predicate
replayed at commit can change the set of affected rows relative to the private
snapshot. No predicate path is narrowed by this change.

For an admitted table t and its primary-key projection P, define:

- F(Insert(t, rows)) = {(t, P(row)) : row in rows};
- F(DeleteByPrimaryKey(t, keys)) = {(t, k) : k in keys};
- F(transaction) = union of its operation footprints in statement order.

Every explicit mutation can alter only a row in F. Unique secondary constraints
and foreign-key dependencies are excluded by eligibility, including references
*to* the table because deletion/replacement may cascade into other tables.
Non-unique postings may share an index value, but their entries include primary
keys and are recomputed from current canonical state under the commit sequencer;
they are not published from a stale private index snapshot. Thus writes to
disjoint explicit primary keys preserve each other's rows and postings.
Primary-key extraction clones the same schema-ordered column values used by
`relational::row_key`, without an alternate string encoding or equality rule.

The collector visits every explicit intent, including intermediate changes and
missing keys. A later insertion replaces a tombstone disposition with live;
a later deletion replaces live with tombstone. Either disposition carries the
latest commit epoch and participates equally in first-committer-wins validation.
If a prior commit touched any (t,k) after a transaction's base epoch, its stamp
rejects the stale writer. If keys are disjoint, no broad barrier intervened and
other admissions succeed, both can commit. This is snapshot isolation for these
write footprints, not validation of arbitrary earlier read predicates.

Classification completes before adding row identities. Any unsupported operation
makes the whole relational transaction broad. Mixing broad and narrow domains
is safe because the existing Database/Schema rule checks both barrier directions.
A new schema or foreign-key declaration cannot silently change eligibility for a
stale transaction: DDL publishes a broad barrier. Pessimistic and conflict-noop
rebasing retain their existing rules; their successful commits still publish
appropriate stamps for concurrent optimistic transactions.

## Admission, validation and publication

The existing relational row/payload admission runs before footprint construction.
One borrowed-name map deduplicates touched table metadata; incoming references
are checked in one catalog pass rather than once per table. With T touched
tables, S schemas and F foreign-key edges, dependency checking takes
O(S + F log(T + 1)) work after classification, and retains only the touched-table
map and primary-key positions. This does not bound the resident catalog itself.
The shared VersionWriteSet entry and estimated-byte caps apply while adding row
keys. A failure discards local preparation before WAL or canonical mutation.

Optimistic validation runs before relational staging so a stale duplicate INSERT
produces a typed retryable conflict instead of being masked by the canonical
primary-key constraint check. The complete mixed graph/relational/append set is
validated again after private preparation and before WAL append. This adds a
validation pass; no latency improvement is claimed without measurement.

Preparation uses current canonical state. WAL encoding, constraint validation,
row/index publication, shared fsync and poison/reopen semantics remain unchanged.
The changefeed's net-change capture is still used only for its original purpose.
Version stamps remain process-local: reopening starts a new conflict baseline,
and every new explicit commit records its intents again. Pin-based reclamation
and the existing MVCC watermark theorem apply to the new identity variant.

## Formal and executable evidence

`HawDBRelationalWriteIntent.tla` has two transactions and two keys, with absent,
original and replacement row states. The stamp validator is compared against
independent complete *intent* history, not a net-change list. TLC checks
4,212 generated / 3,312 distinct states, depth 5, with an empty queue. The
DropNoopStamp and DropDeleteStamp controls must violate
ValidationMatchesIntentHistory. NoNoopCommitWitness must produce a reachable
unchanged-data commit. This model does not include constraints or decoding;
the eligibility/footprint argument above is its conditional source mapping.

The existing `HawDBMvccValidation` supplies the separate multi-key, broad-barrier,
pinning/pruning and restart checks. `HawDBOptimisticCommitAdmission` covers
serialized validation within a durability group. These models are not asserted
to be one complete machine-checked Rust refinement.

Executable coverage includes materialized and forced OutOfCore storage,
composite primary keys, disjoint and same-key inserts, a shared non-unique index
value, pinned readers and writers across checkpoint, byte-identical WAL on
rejection, reopen and new post-restart conflicts. Storage tests cover replacement
then restoration, absent deletion, disjoint explicit deletes, disposition
replacement, write-set count limits and relational mutation admission.
Additional cases retain Database barriers for unique constraints, declared
unique indexes, outgoing/incoming foreign keys and predicate replay. The older
Database-barrier regression now uses an explicit predicate operation because
ordinary unconstrained INSERT is no longer broad. Restoring the old implementation
fails the disjoint-key integration regression.

Still open: narrower predicate/UPSERT/constraint footprints, full version-memory
bounds, complete recovery/refinement composition, retry fairness and workload
latency qualification. Primary-key and table-name payloads use existing page
weight accounting; the per-write-set estimate excludes allocator overhead and
is not a global memory guarantee.
