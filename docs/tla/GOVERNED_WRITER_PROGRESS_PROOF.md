# Conditional completion of a governed large writer

This supplements the per-lock-request progress theorem and the transaction
permit-lifetime proof. It covers one whole, finite transaction under recurring
smaller arrivals, subject to an explicit host policy. It does not establish
unconditional fairness for every HawDB transaction or close #232.

## Contract and assumptions

The original instance has all competing mutation callers use the same
RuntimeGovernor, foreground priority, and `begin_admitted_transaction`. The
[aged-background refinement](AGED_WRITER_PROGRESS_PROOF.md) also covers one
background large waiter competing with foreground small writers. The large
request needs all C CPU slots, where C > 1; each small request needs one. The large waiter is
retained across resource refusals and preserves its original queue position.
Capacity and other admission limits stay sufficient for the request. Previously
admitted owners eventually complete or otherwise retire, and the host eventually
retries a continuously admissible request. Once admitted, the large transaction
runs a finite, valid sequence of statements, without cancellation, deadline,
resource exhaustion, storage failure or conflicting ungoverned writers. Its
commit, durability operation and result delivery eventually complete.

The full-capacity request is a host-selected policy, not an automatic database
lock or a new default. A transaction requesting fewer slots may overlap smaller
writers and can still conflict. Admission limit changes, arbitrary priority
mixes, abandoned owners, unscheduled callers and retry policies require separate
arguments; the refinement covers only its stated mix. The plain transaction and
autocommit entrypoints are not implicitly governed by this contract.

## Source-level progress argument

Let Q be the foreground admission queue, H the admitted owners, and L the large
request. With one priority, `selected_admission_waiter` selects the oldest
entry. `admission_queue_error` rejects younger queued and unqueued requests
before resource admission; checking selection, reserving capacity and removing
a successful waiter occur under the same governor-state mutex.

Once L is at the head, later one-slot requests cannot consume a newly freed
slot while L waits for all C slots. There are finitely many existing owners.
Their eventual retirement and the no-bypass rule make the available capacity
monotonically reach C while L waits. The other stable admission gates then make
L continuously admissible. Fair host retry grants its reservation. More
abstractly, finitely many older queued requests can be handled first by the same
argument, provided their own work and required resources meet the assumptions.

`begin_admitted_transaction` captures the workspace only after receiving the
permit. The [lease invariant](TRANSACTION_ADMISSION_LEASE_PROOF.md) keeps that
reservation through all statements, queued execution, shared sync and result
retirement. Since L holds C slots and admission never overbooks, no other
participating writer can acquire a workspace while it runs. Existing governed
owners have already retired. Therefore this workload does not cause a newer
competing write to invalidate L's captured epoch. This step relies on all
mutators participating and on the permit covering the complete transaction;
it is not a theorem about ungoverned writes or read-set serializability.

Each successful statement decreases a finite remaining-work measure. Under
service of enabled steps, the measure reaches zero, then the valid commit and
result retirement complete. Only then can younger writers receive capacity and
capture snapshots. Canonical publication is atomic, so those snapshots include
all of L's statements rather than a prefix. The unchanged storage and durability
protocol supplies atomicity; the progress model does not re-prove WAL decoding,
failed-sync recovery or byte-level publication.

## Finite temporal model

`HawDBGovernedWriterProgress.tla` starts with two admitted small owners and the
large request at the head of the queue. Small actors can retire, enqueue, admit,
work, commit and return indefinitely. The large writer requires both slots and
two abstract work steps; a small writer needs one slot and one step. Admission
removes one queue entry, but the actor remains charged through its durable
state until retirement.

Invariants check types/queue membership, slot capacity, no younger admission
while the large writer waits, exclusive capacity ownership by the large writer,
and whole large publication. Under weak fairness of admission and each owner's
work, commit and retirement, `LargeCompletes` requires eventual retirement of
the large transaction. This checks an infinite-behavior property over the finite
state graph, not merely a bounded number of arrivals. The original cfg sets
`BackgroundLarge = FALSE` and `DisableAging = FALSE`; `aged` is initially true
and the added aging transition is disabled, preserving this instance’s graph.

The configured instance completed with **140 generated / 77 distinct states**,
depth 20, an empty queue, and completed temporal checking over the whole graph.
The Bazel TLC action and its test actually executed. The evidence collector now
rejects missing or partial-graph temporal logs for this model; its contract test
checks both refusal cases as well as complete-graph acceptance.

Controls distinguish the obligations:

- The registered `AllowBypass = TRUE` mutant must violate `NoBypass`.
- With that flag and `INVARIANT NoBypass` removed, the unchanged temporal
  property has a starvation counterexample even though owner service remains
  fair: recurring small arrivals can prevent capacity from accumulating for L.
- With `FairService = FALSE`, an owner can retain capacity indefinitely and
  `LargeCompletes` again has a temporal counterexample.
- Separate false invariants `NoYoungWaiterWitness` and
  `NoRepeatedSmallWitness` reach a younger queued arrival and a repeated small
  admission after large publication. Recurring work is not silently excluded.

Reproduce the positive model and registered safety control with:

```sh
bazel test //docs/tla:HawDBGovernedWriterProgress_check
scripts/check-storage-tla.sh --check-mutants
bash scripts/check-storage-tla.test.sh
```

For each temporal control, copy the positive cfg to a temporary file and apply
only the flag/invariant changes above. Run TLC with `-lncheck final` and require
`Temporal properties were violated`, rather than accepting an arbitrary
nonzero exit. For each reachability witness, append `INVARIANT <witness name>`
to a separate positive cfg and require that named invariant violation. Both
temporal controls and both witnesses were checked. Finite checking and the
source argument are not a machine-checked Rust refinement or a general
priority/constraint/recovery composition proof.

## Executable workload

`admitted_large_transaction_completes_under_recurring_small_transactions` runs
in memory and durable grouped mode, for both foreground and aged-background
large requests. The latter waits for the public aging deadline while both slots
remain held, before running the same workload and assertions. Two older one-slot transactions stage
disjoint SQL inserts. A two-slot large waiter queues, then one older transaction
commits. Four worker threads submit younger one-slot requests while one slot is
free; each must observe `QueuedAhead`. After the other older writer commits,
the large request obtains capacity despite continuing younger retry attempts.

The large transaction executes 64 inserts and commits once. Each small thread
then performs 32 real transactions, rejoining the queue for each admission.
Every one of those 128 transactions checks that its captured SQL snapshot
contains all 64 large rows, then inserts its own distinct key and commits. The
fixture checks the exact 194 rows, 131 commit-epoch increments, all worker
completions and zero remaining CPU/task/memory reservations. Durable reopen
must reproduce the exact rows and epoch. A bounded retry deadline detects a
hung test; its value is not a production latency guarantee.

Temporarily ignoring `admission_queue_error` causes this regression to fail:
younger workers consume the available slot and the large request sees
`CpuSaturated`; younger snapshots can also miss the large transaction's rows.
The original guard is restored before positive validation. The tests keep the
existing 16 MiB mutation reservation and check the row set without an unnecessary
blocking sort inside that reservation.

This provides a concrete sustained-arrival qualification for the stated host
policy. Arbitrary-size concurrent transactions, whole-transaction retry fairness
without full-capacity admission, other priority mixes and uncontrolled callers
remain outside these results and remain #232 acceptance work.

The [conflict-retry policy](GOVERNED_CONFLICT_RETRY_PROOF.md) now connects an
ordinary shared first attempt, pre-publication conflict rejection, persistent
retry admission and a fresh full-capacity attempt. It establishes conditional
completion for that explicit host policy, not for arbitrary fixed-weight retries.
