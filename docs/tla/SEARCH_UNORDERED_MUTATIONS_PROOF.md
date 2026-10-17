# Fixed-snapshot external mutation ordering

This is the private output-ordering component of the
[complete C..E protocol](../specs/MVCC_SEARCH_BATCH_COVERAGE_SPEC.md).
It does not implement the artifact-owning coverage session, durable operation
receipts, or actual Mem outer-owner migration. Neither an ordinary prepared
Search update nor successful arbitrary callbacks constitute coverage authority.

## Native contract and source mapping

`SearchOutOfCoreGenerationWriter::prepare_unordered_delta` retains the original
Search reader/base selector and creates a private generation stage. Inputs are
fixed-source-snapshot intents, rather than chronological commands. For each ID:

- A final-E upsert dominates retract intents from earlier source changes.
- Repeated deletes coalesce; identical complete encoded upserts coalesce.
- Different encoded upserts refuse the entire attempt, regardless of input
  order. Equal lengths and CRCs are insufficient evidence of equality.

The caller must actually read every upsert from the same E and provide every
necessary old-ID retract. The sorter cannot establish those premises from a
`SearchDocument` alone. A missing or incorrectly mapped input is not repaired by
sorting. This API therefore cannot mint a complete-through receipt.

`generation_writer/unordered.rs` admits input operations, document/header
ownership, buffer capacity and payload; sorts each bounded buffer; and spills
through `unordered/run.rs`. Incoming document ownership remains admitted while
a full buffer is flushed. The shared build ledger also charges paths, file
handles, decoder headers and I/O buffers. With a task memory reservation it
enforces that reservation across the shared accounts. Default task contexts
have no finite shared memory reservation; explicit payload/record and cumulative
I/O/work caps still apply. Caller-side document construction precedes admission.

Runs are carried through a fixed `usize::BITS` array. Each merge consumes two
sorted runs, creates one complete run, and removes both consumed files before
returning. This bounds live run metadata by the input counter's bit width;
intermediate output/read costs remain cumulative, including duplicates. Removing
a file does not refund cumulative spill admission. The private mixed-record
format preserves each document frame's length and CRC, plus delete checksums,
analyzer flags and expected run count/extent/digest. Readers check complete
drain and nondecreasing IDs. CRC evidence detects covered accidental corruption;
it is not a cryptographic integrity guarantee against deliberate file mutation.

The final pass groups equal IDs. `SpoolRecord::encoded_eq` first compares frame
length/CRC, then compares the complete payload using two admitted 8 KiB buffers.
Bodies stream between run files and into the ordered writer without a full-body
decode. The ordinary streamed delta writer creates an invisible
`SearchOutOfCorePreparedUpdate`; activation retains its expected-base CAS.
Input/IO/admission failures and panics leave the unordered writer unusable.
Drop/cancellation use the existing stage cleanup owner.

The conditional argument has two parts. First, each successful input adds one
record to the private buffer/run partition. Sorting permutes that membership;
a successful merge preserves the union and ordering of its two inputs. Both
inputs must reach validated EOF before the replacement run is accepted. An
induction over flushes/merges therefore preserves every accepted record until
the final complete drain. Second, per-ID reduction has values `Delete`,
`Upsert(d)` and absorbing `Conflict`: Delete is the identity, equal upserts
combine to that upsert, and unequal upserts combine to Conflict. This operator
is associative and commutative when Conflict propagates. Thus all permutations
and spill/merge groupings yield the same result or refusal, provided exact
encoded equality and complete membership hold. Native code checks both
premises at the stated seams; the finite model checks selected interleavings
and the deliberately colliding-body case rather than proving arbitrary Rust.

Ordering now also has [resumable per-quantum admission](SEARCH_SORT_QUANTUM_PROOF.md)
for heap comparisons/transitions and logical run reads/writes. Queueing retains
one admitted pending input instead of driving a whole merge. A record or exact
comparison that exceeds an entire quantum refuses instead of yielding forever.
The blocking convenience methods drive the same states; final base retraction
reads, indexing and artifact sealing remain blocking. A source feed quantum
does not cover those separate phases. Encoded record maxima, shared ledger
estimates and run bounds do not prove total RSS, page-cache residency, syscall
latency, or size-independent activation cost.

## Finite refinement

`HawDBSearchUnorderedMutations` uses five indexed input intents, including
duplicate deletes, an upsert/delete overlap, and two same-ID upserts whose
digests and lengths deliberately collide. The duplicate upserts are either
byte-identical or conflicting. All input permutations, two-slot buffers and
all pairwise run merge schedules are explored. Accepted indices form a disjoint
partition across buffer/runs; reduction cannot start early, and no prepared
candidate can omit an accepted input or suppress a conflicting upsert. A
separate cumulative-cost oracle detects merge refunds. Base replacement and
sticky failures constrain publication.

The model abstracts byte encoding, CRC validation, the concrete comparison and
merge loops, binary carry scheduling, exact resource sizes, Rust ownership,
physical I/O, cleanup retries and crashes. `Ordered` is mathematical ordering,
not a proof of Rust's sorting implementation. Native source-linked regressions
cover the concrete seams below. This model does not prove arbitrary host mapping,
complete DDL/change capture, power-loss composition or cross-store atomicity.

TLC 2.19 / TLA+ 1.7.4, Java 21.0.9, pinned jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`:
**36,006 generated / 13,921 distinct states**, depth 23, no invariant failure.
Seven registered fault controls drop an input, equate digest collisions, ignore
a conflict, prepare early, refund merge cost, revive a failed attempt or publish
against a replaced base. Each exits 12 for its named invariant. The separate
CompletionWitness also exits 12, proving a successful publish is reachable.
Parser failures, timeouts and generic nonzero exits do not count as controls.

## Native evidence

`generation_writer/unordered/tests.rs` covers:

- Baseline preservation before activation and final-E reduction across buffer
  sizes 1/2/10; 36 deterministic permutation/buffer cases use an independent
  final-document map, including empty/CJK bodies and deleted-ID absence.
- Complete-wire CRC32C collision (`487a6ddf463ff161` versus
  `301516450b94bbce`, same ID/title, CRC `cf846bec`, length 45): the attempt must
  refuse. A digest-only implementation fails this regression.
- Truncated/mutated runs; conflicting duplicates; exhausted input, payload,
  read, spill and visit admission; ignored errors; a real create-new filesystem
  refusal; task-memory denial despite ample payload-buffer admission.
- Cancellation while buffered, spilled and prepared; stage/ledger release; a
  competing publisher winning the original selector before activation.

`tests/mvcc_publication_owner.rs` composes the real public owner, one pinned
graph/SQL feed, one-operation source steps, private external sort, and the
source-validated prepared-update publication API. Materialized and cold reopened
fixtures include an intermediate graph update followed by delete, and repeated
SQL updates. A foreground writer actually commits before hydration/sorting
resume. Disjoint writes allow the exact E document set; relevant SQL changes
reject before generation/watermark visibility. Business mapping is a fixture
premise; this composition test does not provide a stage-bound coverage token.

The legacy partial-prefix catch-up path is unchanged. #1043, the full #231/#232
acceptance ledger, actual host migration, receipts/recovery, retained-source RSS
and enabled/disabled catch-up writer measurements remain outstanding.
