# Shared reader capacity protocol

## Scope

`HawDBSharedReaderCapacity.tla` checks the retained-resource protocol for one
immutable vector projection with three single-use client slots. It abstracts
allocation sizes as one control-block unit, two payload units and one fixed
ledger-metadata unit. These are symbolic units, not the implementation's byte
estimates. The model is not a refinement proof of Rust, `Arc`, serde, the
allocator, source immutability, a complete governed reader, or MVCC activation.

Construction first reserves query capacity, then host retained capacity, then
allocates the shareable backing. Host refusal rolls back without constructing
backing. Early failure after both grants uses `Abort`; failures after backing
allocation use the same drop ordering as successful objects. This collapsing of
partial construction requires the separate Rust RAII/source argument. Sharing changes only reference ownership. Beginning a
drop moves a client from usable to dropping; its strong reference still counts.
Each atomic decrement removes one reference. The last decrement extracts the
payload once and frees the control block. Payload destruction precedes query
refund; dropping the account reference precedes host refund. The producer also
holds the ledger: physical destruction occurs only after both references are
gone. Closing the producer releases execution capacity independently of retained
backing.

## Invariants and induction argument

- `BackingCovered`: every live backing allocation has both query and host
  admission. Only `Allocate` creates it, after both grants. Nonfinal decrements
  preserve both grants. The final decrement frees the block but retains the
  grants through payload disposal. Neither refund action precedes payload free.
- `LedgerLifetime` states that the ledger lives exactly while the producer or
  the projection account reference survives. Query refund does not itself drop
  the account reference.
- `LedgerCovered`: ledger metadata has host admission or the still-open producer
  working grant. Query reservation alone does not account for ledger metadata.
  Query refund may precede physical ledger destruction; host retained admission
  must remain through that destruction if the producer is already closed; an
  open producer otherwise supplies the remaining working grant.
- `CapacityCovered`: once retained host admission is present, live symbolic
  backing plus ledger metadata fits its four-unit charge. Cloning adds no backing.
- `ReferencesCovered`: both usable and dropping references retain live backing.
  The last decrement is the only shared-state action removing backing.
- `SingleExtraction`: after a constructed projection begins final disposal, its
  payload was extracted exactly once. No weak/raw outer-Arc escape is allowed.
  Keeping child backing and guards together is an obligation of the concrete
  private `ProjectionContents` type; the generic helper does not prohibit
  interior mutability or independently owned children in arbitrary payloads.
- `NoExecutionAfterClose`: retained memory alone never retains a CPU/task/I/O
  permit. Later queries require their own admission.
- `NoTerminalLeak`: every terminal path has released query and host charges and
  freed backing; ledger metadata is also freed once the producer closes.

`EventuallyDisposed` is checked under weak fairness of `Next`. Every client slot
is used at most once and every nonstuttering action advances a finite lifecycle,
so fair execution eventually disposes and closes the producer. This assumes that
clients eventually drop: it is not a host scheduling/fairness guarantee and does
not require reclaiming an intentionally retained reader.

## Rust correspondence and limits

| Model | Current implementation obligation/evidence |
| --- | --- |
| Query then host reserve | `ProjectionReadAdmission` in `generation_writer/governed/reader.rs` delegates to `ReadAdmission::retained` in `build_memory/read.rs`; query guard rolls back if host reservation fails. |
| Admit before backing | `FileProjection::open_admitted_file`: path, encoded manifest and decoded metadata grants precede their allocations; scratch is separate. |
| No backing on clone | `FileProjection<Memory>::clone` clones `SharedAllocation<ProjectionContents<Memory>>`; the generic retained owner needs no `Clone`. |
| Last decrement/extraction | `SharedAllocation::drop` uses `Arc::into_inner` for every clone. The atomic/exactly-once property is a standard-library contract assumed by this model; concurrent native final-drop regression tests cover its use here. |
| Control block before payload | No weak/raw Arc escapes; `into_inner` returns the value after freeing its allocation. Then `drop(Option<T>)` destroys the payload. |
| Payload before grants | `ProjectionContents` declares path/manifest/mmap before its two retained guards; field order preserves this on drop and unwind. |
| Ledger before host refund | `ProjectionReaderMemory` aliases the common `ReaderMemory`, which declares `QueryMemoryLease` before `RuntimeRetainedMemory`. The former refunds its bytes and drops its account reference before the latter refunds host capacity. Ledger metadata dies only after both producer and retained account references are gone. Shared ledger metadata remains covered conservatively by each surviving receipt. |
| Execution closes independently | Returned receipts contain no `RuntimePermit` or task context. Native tests close the producer, check CPU/task/I/O zero, admit a fresh query, retain memory through all clones, and finally check zero live allocation/host charge. |

The model collapses successful construction's multiple leases into one charge
and assumes the source file is immutable and already admitted. It does not prove
the schema-specific byte bounds or malformed-input parser behavior. Those need
source review and actual-allocation denial/clone/drop tests. Returned diagnostic
objects use the existing error API and are not retained resource owners; scratch admission covers construction, not an error retained by
a caller after admission ends. Error lifetime/accounting also remains outside
this component. It does not model mmap residency, physical I/O budgets, GC roots, publication authority, durable
receipts, source/binding incarnation or power-loss recovery. Ordinary Search
reader construction still uses the unaccounted vector entry; this component must
be composed with all descriptor/layout/lexical/visibility/root admission before
it can establish a governed immutable read view.

## Checks and negative controls

The normal configuration checks all listed invariants and the conditional
termination property. Mutant configurations must produce invariant violations:

| Mutation | Required failure |
| --- | --- |
| `RefundBeforeBlock` | `BackingCovered`: return host capacity while shared block/payload remain. |
| `DetachClone` | `CapacityCovered`: a deep-cloned child allocates extra payload without new admission. |
| `KeepExecutionPermit` | `NoExecutionAfterClose`: reader custody retains active execution after producer closure. |
| `RefundBeforeLedger` | `LedgerCovered`: return host capacity before the final ledger metadata is freed. |

Run the normal and four manual negative-control Bazel targets:

```console
bazel test //docs/tla:HawDBSharedReaderCapacity_check \
  //docs/tla:HawDBSharedReaderCapacityRefundBeforeBlock_check \
  //docs/tla:HawDBSharedReaderCapacityDetachClone_check \
  //docs/tla:HawDBSharedReaderCapacityKeepExecutionPermit_check \
  //docs/tla:HawDBSharedReaderCapacityRefundBeforeLedger_check
```

Inspect retained
TLC logs for the named invariant failures; a parser/tool failure is not a passing
negative control. Finite exploration and these counterexamples do not establish
an unbounded implementation theorem or complete the MVCC qualification gates.

The in-progress [governed reader construction design](../specs/MVCC_GOVERNED_READER_DESIGN.md)
records the common receipt's use by descriptor/layout metadata and the scoped
Storage read primitive. The model and its state space are unchanged by that
extraction. Its proof scope does not expand to the full constructor, cold-file
validation, parser work, source custody or activation merely because those paths
now share a receipt type.
