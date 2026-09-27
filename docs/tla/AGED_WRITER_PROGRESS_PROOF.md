# Aged background writer completion under foreground arrivals

This extends [governed writer progress](GOVERNED_WRITER_PROGRESS_PROOF.md) for
#232. The existing governor prefers queued foreground work until a background
waiter reaches `BACKGROUND_ADMISSION_AGING` (currently 100 ms). A full-capacity
background transaction must not remain blocked by recurring foreground requests
once that deadline passes. No runtime policy or public API changes here.

## Source argument and assumptions

Retain the original proof's stable capacity, finite valid work, retiring owners,
shared governor, full-transaction admission lease, successful durability and
fair host retry assumptions. There is one background large waiter L and any
finite number of simultaneously active/queued one-slot foreground writers;
foreground actors may return indefinitely. Time progresses beyond L's fixed
enqueue deadline, and the host eventually retries after that deadline. This is
not a claim about abandoned async tasks or an automatic timer inside HawDB:
`RuntimeAdmissionWaiter::next_priority_change_at` exposes the deadline to the host.

Before aging, foreground admission may preempt L, including unqueued foreground
requests. Thus same-priority FIFO reasoning alone is insufficient. Once L ages,
`selected_admission_waiter` checks aged background entries before foreground
entries. L is the only background entry, so it stays selected until removal.
`admission_queue_error` rejects every other queued caller; its unqueued
foreground exception requires `!aged_background`, so direct foreground admission
cannot bypass L either. Selection, admission and waiter removal occur under the
same governor mutex. The enqueue time is retained across unsuccessful retries.

The number of existing holders is finite and no new holder can enter while L
is selected. Their eventual retirement therefore drains occupied capacity to
zero. With the other gates sufficient and stable, L becomes continuously
admissible. Fair host retry admits it. The transaction lease holds all CPU slots
through private execution, canonical commit, durability and return. The original
finite-work ranking argument then proves whole-transaction completion, and the
subsequent foreground snapshots contain the complete published large write.

The argument generalizes over any finite number of older holders and any finite
valid statement sequence. It does not establish retry fairness for transactions
that reserve only part of capacity, unrestricted priority mixes, changing limits,
ungoverned writers, failed storage, or a production wall-clock latency bound.
Those remain separate #232 obligations.

## Shared temporal model and controls

`HawDBAgedWriterProgress` extends the original model rather than duplicating its
work/commit/retirement transitions. Its cfg sets `BackgroundLarge = TRUE`.
Before `aged`, selection takes the earliest foreground waiter, if present;
after `aged`, the older large waiter is selected. `Age` is one monotone transition
abstracting passage of the deadline. Weak fairness of `Age` expresses eventual
time progression; weak fairness of service and admission retains the original
conditional scheduling assumptions. Small actors can complete and rejoin forever.
`NoBypass` forbids foreground grants while L waits only after L becomes protected.

TLC checks capacity, queue identity, exclusive large ownership, atomic large
publication and eventual large retirement over **551 generated / 232 distinct
states**, depth 21, an empty queue, and a completed full-state-space temporal
check. The original foreground instance still has 140 generated / 77 distinct
states, depth 20. This is a finite temporal model with a source argument, not a
machine-checked Rust refinement or proof of crash behavior.

- The registered bypass control violates `NoBypass` after aging.
- Removing that safety assertion while allowing bypass produces a temporal
  starvation counterexample despite fair owner service.
- `DisableAging = TRUE` produces a temporal starvation counterexample even with
  ordinary foreground selection: each small actor can keep returning before
  full capacity becomes continuously available to L.
- `NoUnagedForegroundWitness` reaches a repeated foreground admission while L
  waits before aging. `NoAgedWaitingWitness` reaches aged L with younger waiters.
  These exclude a model that silently forbids priority preemption or arrivals.

The evidence verifier requires complete-graph temporal checking for both model
instances and rejects missing/partial logs. Bazel declares the shared module as
an explicit `tla_library` dependency, so the sandbox uses the same source.

## Executable boundary

`admitted_large_transaction_completes_under_recurring_small_transactions` now
runs four fixtures: foreground/background L crossed with memory/durable-grouped
storage. For background L, both old transactions keep the two slots while the
test waits until `next_priority_change_at` returns None. It does not assume that
setup finishes within 100 ms or mutate the governor's clock. One old owner then
retires, exposing a free slot. A direct, unqueued foreground request must fail
with `QueuedAhead`; four actual foreground threads must then all observe
`QueuedAhead`; L retains its waiter until the other owner retires and admits the
64-statement transaction. Each thread subsequently commits 32 transactions and
checks all 64 large rows in its snapshot. Each fixture checks 194 exact rows,
131 epoch increments, no remaining CPU/foreground/background/memory reservations,
and exact durable reopen where applicable.

The existing governor unit test separately checks both sides of the priority
selection boundary with a controlled enqueue timestamp. The integration fixture
focuses on aged priority plus transaction lifetime and persistence. Temporarily
removing aged-background selection makes the direct admission unexpectedly
succeed, failing the regression before worker startup. Production source is
restored before positive verification. A test deadline detects a hung test and
is not a latency guarantee.
