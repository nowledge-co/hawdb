# Governed immutable Search reader construction

Status: implementation in progress. This is a construction-resource contract,
not evidence of C-ready/E activation or completed MVCC qualification. The full
acceptance scope remains in [MVCC versioned publication](MVCC_VERSIONED_PUBLICATION_SPEC.md).

## Lifetime and admission contract

A constructor borrows one admitted operation's `RuntimePermit`,
`RuntimeTaskContext` and `BuildMemory`. It must admit encoded buffers, decoded
metadata, path construction, native validation and I/O before the corresponding
work or allocation. The query account and host governor must describe the same
operation. Creating an independent default task or budget inside a read phase
would break this contract.

Encoded buffers and decode scratch belong to construction execution. They must
be destroyed before the working permit can be returned. Owned descriptors and
layout vectors belong to the resulting shared segment. Their query-account
leases and host retained-memory leases must remain until their backing storage
is destroyed. A retained guard cannot own a construction permit or a
`BuildMemory` whose `host_admission` retains that permit.

| State transition | Required ownership effect |
| --- | --- |
| Admit encoded input | Reserve before path conversion, open and buffer allocation |
| Admit decoded output | Inspect bounded input without owned token tables; reserve before serde or map/vector growth |
| Decode succeeds | Transfer the owned result and retained guard together; release encoded input and scratch |
| Decode fails or operation stops | Destroy partial output and buffers before refunding their capacity |
| Construction completes | Release CPU/task/I/O admission while the shared reader retains only memory and source custody |
| Reader clone | Share the original owner and its guards; do not detach or duplicate payload capacity |
| Final concurrent drop | Destroy the shared control block and payload in the order required by their retained guards |

These imply three separate obligations: physical backing is covered while live,
reader clones cannot outlive capacity ownership, and a live immutable reader does
not retain execution admission. The existing
[shared reader capacity argument](../tla/SHARED_READER_CAPACITY_PROOF.md) covers
the shared lifetime protocol and its explicit abstraction assumptions. It does
not prove schema admission arithmetic, Rust allocation behavior, or this whole
constructor. Native allocation observations and schema-specific regression tests
are additional evidence, not replacements for those obligations.

## Implemented construction boundaries

`ReadAdmission` is borrowed; `ReaderMemory` owns only query/host memory leases.
`ReadBytes` borrows the construction permit's lifetime and owns its transient
buffer before its scratch reservation. `DecodedDescriptor` owns the descriptor
before its guard. Segment contents own layout metadata before their layout guard.

Descriptor preflight counts fixed grammar records and owned strings, including
repeated/replaced values. The decoder uses a nine-field stack table rather than
a vector proportional to excess tabs. Layout preflight accounts for both map
and compact sequence encodings and vector growth overlap. A checksum-valid file
that fails later semantic validation still requires admission before decoding.
These are conservative bounds for the pinned implementation; they are not an
allocator- or RSS-independent theorem.

JSON checksum, sizing, retained output and streaming output share a writer
boundary that rechecks the task before converting a serde I/O error. A known
cancellation remains an execution error and must not be treated as evidence of
corrupt storage. Ordinary noncancelled malformed input remains a storage error.
A decode boundary also checks cancellation before propagating a parse failure.
This does not yet establish bounded cancellation latency inside every parser.

`File::with_admitted_read` now provides the scoped Storage boundary. Its
private native/cache lease spans validation and consumption; `AdmittedFileRead`
exposes metadata and a nonclonable sequential read view with an independent
positioned cursor. Cold identity validation admits the complete object before
reading, checks the control between at most 64 KiB chunks, and publishes a cache
entry only after complete identity verification. Selected reads check control
between at most 8 KiB calls, including short-read/EINTR retries. Native trace and
descriptor ownership remain on the managed file. The fallback positioned-read
lock is shared with existing portable positioned reads.

The Search bound-file helper uses this scope and holds one operation I/O slot
until it returns. It reserves validation scratch when a cold object requires it
and preserves a typed admission error across the generic I/O callback. The
physical identity length must equal the selected full-artifact length before
validation payload I/O. Tests exercise cold denial/stop/retry, warm reuse,
replacement and concurrent cursor/eviction custody at the Storage boundary.
Search tests currently exercise the connected native-file path; they do not yet
establish a complete Search reader over a cold managed artifact closure.

The admitted segment entry remains private. The current next-stage implementation
also pre-admits lexical JSON metadata and its file/control storage, streams its
artifact checksum/header through the same held file, and retains the lexical
payload with the segment. Descriptor, layout, lexical manifest and payload paths
use admitted construction. A per-artifact size/read limit returns an execution
refusal before opening; this is not a cumulative physical-I/O budget.

Vector-bearing segments use the same `ReadAdmission` bridge and retained owners.
The logical input is explicitly pinned under read control while vector metadata
is decoded, verified and mapped; the mapping reader retains no input File. This
closes uncontrolled cold revalidation inside the vector constructor. The native
FD argument, finite model and remaining assumptions are recorded in
[admitted native input custody](../tla/ADMITTED_NATIVE_LEASE_PROOF.md).

Targeted positive tests currently cover native-file segment construction and
producer/clone/final-drop lifetimes, including a fresh vector query. Storage
managed-file fixtures qualify the scoped/pinned primitives separately. They do
not yet establish a complete cold managed Search root under a real governor.
There is no complete public governed reader factory or actual Mem adoption yet.
The ordinary reader entry continues to use the existing ungoverned path.

## Remaining required construction work

The visibility implementation includes an admitted mutation-run decoder. Its
preflight and decoding use the same borrowed construction admission; each shared
term-file payload owns its retained query/host receipts. Stored terms require a
fresh serving context that borrows execution, including after their parent run
and producer close. Term-range CRCs are recorded during streaming decode and
checked against independent raw-file traversal in canonical, pretty and escaped
JSON fixtures with multiple arrays. Canonical body checksum serialization keeps
the original typed admission error instead of reporting storage corruption.

The execution protocol is described in
[borrowed query cursor safety](../tla/BORROWED_QUERY_CURSOR_PROOF.md). Its finite
model and three negative controls do not establish a complete governed closure.
The private `validate_targets_with_admission` branch now uses the constructor's
task and ledger for document probes, captured body decoding, CJK reanalysis and
stored-term comparison. The document probe admits its range and decode buffers;
capture retains a full-file read scope. Later body and term reads acquire I/O
per bounded call, and an admitted temporary `Source` borrows execution until its
worker joins and its consumers finish. The one-slot protocol and its limits are
recorded in [target reanalysis I/O safety](../tla/TARGET_REANALYSIS_IO_PROOF.md).
The private root constructor now connects that branch to root/configuration/
analyzer capacity, head and manifest decoding, exact segment/run collections and
closure-validation indexes. It borrows an existing project, charges incoming
host buffers by actual capacity, and retains no producer execution. Its sealed
wrapper refuses conversion to an ordinary public reader; admitted reuse is
likewise refused until source-root custody exists. The ordinary public entry
continues through its existing legacy branch. The conditional argument and
unproven obligations are in
[private root construction](../tla/ROOT_READER_CONSTRUCTION_PROOF.md).
These tests exercise native files, with project owners
explicitly closed for final heap reclamation; managed cache/registry ownership
remains a separate unresolved requirement.

1. **Complete cold-path qualification.** The scoped primitive and Search
   bound-file integration exist, but must be qualified together over an actual
   complete managed Search artifact closure, with governor refusal and
   cancellation during cold validation. Quantify physical work across all
   artifacts, including retained cache/registry metadata and the opening-lock
   wait. Warm-cache tests alone do not prove this. Preserve the original
   fairness and writer-latency gates when one I/O slot spans a whole artifact.
2. **Complete metadata closure.** The private root path now connects visibility,
   analyzer/configuration and collection admission to the shared children.
   Qualify cold managed cache/registry owners, then connect source-root custody,
   governed serving and refresh/reuse before exposing a production entrypoint.
3. **Bounded cancellation and work.** Check long scalar/line parsing, schema
   validation loops and physical checksum work, not only record boundaries.
   Account for full physical objects when selected metadata is smaller than its
   containing immutable object. Do not use a separate default task for probes.
4. **Source custody and GC.** A live view needs an incarnation-bound source root
   registration linearized with GC selection/unlink, including deferred child
   reads. A held file descriptor alone does not preserve paths needed later.
   A permanent busy-operation reference would also block future publication and
   is not a reader registration. Registry metadata and removal need admission.
5. **Full native qualification.** Positive complete-reader construction, refusal
   at each boundary, cancellation during physical reads and decoding, source
   replacement, clone/reuse lifetimes, producer closure and concurrent final
   drop must use actual governor/account ownership and physical observations.
   Add cold managed-file fixtures, then run the required feature, lint, Bazel
   and unfiltered fuzz checks on the final integrated source.

Completing this construction contract still leaves closed C-ready mapping and
owner/parent/fanout indexes, independent source vectors, C..E validation,
incarnation/ABA checks, atomic activation/receipts and bounded durable cleanup.
Their proofs, power-loss qualification, actual Mem integration and the original
writer/fairness/fsync gates remain required by the parent specification.
