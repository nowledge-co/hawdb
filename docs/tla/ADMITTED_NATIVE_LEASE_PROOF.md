# Admitted native input lease custody

Status: finite safety model and native regression evidence for reader
construction. This does not establish a complete governed Search read view,
C-ready/E, source/GC custody, or the parent MVCC acceptance gates.

## Argument and implementation mapping

A logical immutable `File` identifies one object and normally acquires native
cache leases lazily. `pin_admitted` acquires a validated native lease under the
caller's read control and stores a managed `Arc<File>` in that logical file.
Pinned copies share this native file and its existing descriptor permit; they
do not clone an uncounted OS descriptor. No raw native handle is exposed.

| Protocol step | Native obligation |
| --- | --- |
| Cold open | `ImmutableFileHandles::get_controlled` coalesces cache misses under the opening lock. The native file acquires its descriptor permit before opening. |
| Validation admission | The read control admits complete identity-validation length before any identity payload read. |
| Partial validation / stop | Bounded chunks check the control. Error returns drop the incomplete native file without inserting it into the cache. |
| Publish validated lease | Complete length/digest verification precedes cache insertion and returning its Arc. A stop after valid insertion may leave a fully validated idle cache entry. |
| Pin / clone | `ImmutableFile::pinned` retains the returned Arc; logical `try_clone` shares it. Ordinary unpinned readers keep their existing lazy behavior. |
| Scoped read | `with_admitted_read` takes an independent Arc to the pinned native file and uses an independent positioned cursor. |
| Cache pressure | Eviction selects only an idle cache Arc. The cache lock serializes lookup/Arc cloning with selection; active scope/pin references prevent selection. |
| Return mapped reader | Search's vector loader pins the selected file only while `FileProjection::open_admitted_file` parses, verifies and maps it. Projection contents contain the mmap and metadata, no input File. Input locals drop before the call returns. |
| Last native reference | `NativeFile` closes its native handle before dropping its descriptor permit. An idle cache may retain the counted descriptor until eviction. |

The source argument relies on standard Arc strong-reference semantics, managed
file access remaining private, the descriptor admission implementation, and
immutable object identity. It does not derive those assumptions from TLC. In
particular, the one-object model does not prove the global descriptor budget
across all objects; existing native budget/eviction tests are additional evidence.

## Executed finite model

`HawDBAdmittedNativeLease` explores two single-use client slots, one immutable
object, one cache-opening owner and two symbolic validation chunks. It separates
pin references from active read references and allows dropping a pin while a
scope still retains the native lease. Cancellation can occur at each state.
There is no liveness/fairness claim or byte-to-wall-time correspondence.

TLC completed the normal graph with **965 states generated, 385 distinct states,
zero states left on queue**. Four separate controls exited 12 with their named
invariant failures:

| Control | Detected invariant | Counterexample states generated / distinct |
| --- | --- | --- |
| EvictBusy | LeaseCovered | 201 / 117 |
| PublishEarly | CachedValidated | 164 / 99 |
| SkipCharge | DescriptorCovered | 2 / 2 |
| KeepReturnedPin | NoPinAfterReturn | 251 / 132 |

The positive graph also checks descriptor coverage, complete validation before
consumption, a native owner for every open descriptor, and no construction pin
in a returned mapped reader. This is finite exploration, not a parametric proof
for arbitrary clients or a proof of Rust/OS behavior.

Commands:

```sh
bazel test //docs/tla:HawDBAdmittedNativeLease_check \
  //docs/tla:HawDBAdmittedNativeLeaseEvictBusy_check \
  //docs/tla:HawDBAdmittedNativeLeasePublishEarly_check \
  //docs/tla:HawDBAdmittedNativeLeaseSkipCharge_check \
  //docs/tla:HawDBAdmittedNativeLeaseKeepReturnedPin_check
bazel build --output_groups=tla_evidence \
  //docs/tla:HawDBAdmittedNativeLease_check.run \
  //docs/tla:HawDBAdmittedNativeLeaseEvictBusy_check.run \
  //docs/tla:HawDBAdmittedNativeLeasePublishEarly_check.run \
  //docs/tla:HawDBAdmittedNativeLeaseSkipCharge_check.run \
  //docs/tla:HawDBAdmittedNativeLeaseKeepReturnedPin_check.run
```

Actual module/config hashes, TLC arguments, logs, result/exit classifications,
Java version and tool jar hash were inspected and retained for all five runs.
The first local draft had a conjunction/disjunction precedence error; that run
failed semantic parsing and is not counted as invariant evidence.

## Native coverage and remaining composition

Native Storage regression tests cover cold refusal and mid-validation stop
without partial cache publication, retry, warm reuse, replacement, concurrent
positioned readers, cache pressure, pinned copies sharing one counted descriptor,
and eviction after all pins disappear. The selected `pinned_` campaign executed
49 native cases, including the two new pin cases; this is filtered coverage.

Native Search tests exercise both vector-free and vector-bearing segment
construction. They check real allocation peaks against query admission, producer
CPU/task/I/O release, fresh-operation vector queries, zero added allocation on
cloning, concurrent final destruction, and final zero host/live allocation.
A 3 MiB artifact streams with fixed scratch under a 2 MiB working budget. A
checksum-valid compact lexical directory expanding beyond 2 MiB is refused
before owned records allocate. These are targeted tests, not full qualification.

The existing [shared capacity model](SHARED_READER_CAPACITY_PROOF.md) addresses a
different lifetime: retained memory and allocation destruction. Composing the
two models does not automatically prove the full reader. Cumulative physical
work (including repeated cold validations), parser/cleanup latency, cache and
registry heap ownership, root/config/analyzer/visibility metadata and incarnation-
bound GC roots still require implementation and qualification. Cached native
file metadata can outlive the query/view that initialized it; its actual owner
must retain capacity rather than being charged only to a temporary read guard.
The construction I/O scope must still pass the original fairness and writer
latency gates. Actual Mem adoption, independent vectors, closed C-ready mappings,
C..E validation, atomic activation/receipts and power-loss/cleanup gates remain
required by [the parent specification](../specs/MVCC_VERSIONED_PUBLICATION_SPEC.md).
