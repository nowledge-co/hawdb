# QoS hardening contracts

This records the contracts enforced by [issue #197](https://github.com/nowledge-co/skein/issues/197).

## Class and admission policy

`WorkClass` has one declaration generating the enum, exhaustive `ALL` array,
index, string encoding, and parser. `WORK_CLASS_COUNT` and class snapshots are
derived from it. A constant assertion checks every index against array order;
there is no separately maintained snapshot list. Existing indices and names
remain unchanged.

`LocalQosPolicy::foreground_admission()` is the shared, explicit foreground
probe. Snapshots no longer construct an artificial maximum-size request.
Foreground work bypasses this background policy, not runtime governor limits.

`BackgroundWorkHint::expected_value_score(estimated_operations)` uses the same
estimate as the background decision. A tenant budget strictly below that
estimate zeroes the score; equality does not. A nonzero score is a ranking
signal, not permission to run: background disablement and other admission
limits still apply. Callers must now provide the actual plan estimate instead
of relying on an implicit zero.

## Resource lifetime and platform safety

`BackgroundWorkPermit` requires explicit implementation. Implementers must own
the reservation until drop; an arbitrary `Debug + Send` value is not a permit.
The embedded facade wraps `RuntimePermit` without changing its CPU, memory,
I/O, or task release semantics, including unwind. The trait stays open for
host-provided admission implementations; this does not introduce a storage
engine API or a dependency on a particular governor in the storage crate.

The QoS crate denies unsafe code by default. Only the existing process-memory
and Apple filesystem FFI functions opt out, with local safety explanations.
The resource and runtime modules forbid unsafe code. `libc` is a Unix-only
direct dependency; Windows uses `windows-sys`. No I/O backend is changed.

## Defer means reject this attempt, not queue it

`StoragePressureState::DeferMutation` rejects live writes before WAL append or
data mutation. It neither sleeps under a writer lock nor schedules a retry.
The caller performs maintenance and explicitly retries. Recovery still replays
already committed operations up to the hard delta budget instead of applying
live backpressure to them.

The former `DelayMutation`, `WalDelayThreshold`, `DeltaDelayThreshold`, and
`STORAGE_PRESSURE_DELAY_RATIO_PER_MILLION` names now use `Defer`/`DEFER`.
Their diagnostic strings likewise use `defer`. This is a source/diagnostic API
rename, not a persisted format migration. The 70% soft, 90% defer, and 100% hard
pressure thresholds and existing admission/replay decisions are unchanged.

## Ledger poisoning

Reserve, transfer, release, account creation, and snapshots consistently recover
a poisoned mutex. Ledger state never escapes its module. Caller-supplied owner
conversion happens before locking, and all budget/overflow checks and map
allocation happen before counter updates. Counter updates contain no caller
callbacks or fallible allocation. This preserves valid accounting through an
unwind; it does not claim to reconstruct arbitrarily corrupted counters.

Regression tests exercise poisoned-state reserve/grow/shrink/reset/drop,
same-class and cross-class ownership transfer, rejected-operation atomicity,
concurrent root-budget admission, and a panicking/reentrant owner conversion.
Facade tests cover real governor permit retention, pressure rejection without
WAL/epoch changes, explicit retry after checkpoint, and committed WAL replay.
