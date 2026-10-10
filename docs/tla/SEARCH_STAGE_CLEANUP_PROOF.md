# Search private-stage cleanup ownership

This safety repair is part of [#867](https://github.com/nowledge-co/hawdb/issues/867).
The model is [HawDBSearchStageCleanup](HawDBSearchStageCleanup.tla). It covers
initial writer cleanup and explicit/automatic retry of a retained stage.

## Defect and source refinement

Previously, retry moved a pending ticket out of the registry and marked its
slot `Active`. Initial cleanup also moved its ticket into a local variable.
A panic inside removal dropped that ticket and then `Registration`. Its
destructor removed the active registry entry and released retained admission,
even though the private stage could still exist. This was safe allocation
destruction order, but it lost the obligation to remove or account for the stage.

`CleanupAttempt` now holds both values. Its destructor returns every unfinished
ticket to the existing, pre-admitted registry entry. Restoration moves the
ticket; it allocates no replacement entry, path, workspace, or reservation.
Only `complete`, reached after `remove_batches` returns `Ok(true)`, drops the
ticket and then the registration. `Ok(false)` retains the obligation even in
the full-pass path. A panic after cleanup begins records an unknown failure;
caller cancellation or workspace refusal before cleanup preserves the prior
disposition.

| Model transition | Rust boundary in `spool/stage.rs` |
| --- | --- |
| `Reserve` | Admit metadata/paths and retained host memory; register the owner before creating the directory. |
| `Create` | `StageDirectory::create` returns its private stage and ticket. |
| `SetupFailure` | Failed native creation drops the active registration with no newly owned directory. |
| `Begin` | Initial cleanup or retry moves the registration and ticket into `CleanupAttempt`. |
| `Defer` | Guard destruction after error, preflight refusal, cancellation, or `Ok(false)` restores `Pending`. |
| `Remove` | `Ticket::remove_batches` confirms deletion or an already absent stage. |
| `Unwind` | `CleanupAttempt::drop` restores the ticket while retained admission is still live. |
| `FreeTicket`, `Release` | `CleanupAttempt::complete` drops the ticket before its registration. |

Taking a retry ticket and constructing its guard are non-allocating moves under
one exclusive registry selection. Other callers cannot take that owner while
its slot is active. Checked, non-reused registration identities are an existing
assumption; the model abstracts identity allocation and root-scoped selection.

## Inductive safety argument

Let `E` be stages whose native removal has not been confirmed, `T` the live
ticket allocations, `L` the retained reservations, and `c(s) > 0` each owner's
pre-admitted allowance. The tracked host allowance is
`U = sum(c(s) for s in L)`. Prove the stronger `OwnershipInvariant`, including
the phase/ownership relations, rather than just the observable subset checks:

- `E` is a subset of `L`: unresolved disk evidence always has an owner.
- `T` is a subset of `L`: ticket allocations outlive neither their admission
  nor their owner.
- `U` equals the sum of retained reservations and `0 <= U <= Budget`.
- Reserved/live/pending/attempt/removed/freed phases have a reservation;
  live/pending/attempt/removed phases have a ticket. Evidence is confined to
  live/pending/attempt phases.

The argument applies to any finite owner set, positive allowance function and
nonnegative fixed budget with `RestoreOnUnwind = TRUE` and
`ReleaseBeforeTicketFree = FALSE`:

1. `Init` has empty sets and zero charge, establishing the invariant.
2. `Reserve` adds one fresh reservation and its exact allowance only when the
   sum remains within budget. `Create` can add evidence and a ticket only in
   the already reserved phase. `SetupFailure` removes a reservation only in
   that phase, where neither evidence nor a ticket exists.
3. `Begin`, `Defer`, and correct `Unwind` change phase while preserving all
   ownership sets and charges. In particular, an interrupted attempt cannot
   refund its retained allowance. These steps cover both cleanup entry paths.
4. `Remove` removes only evidence. `FreeTicket` removes only the ticket.
   Neither step refunds admission, so coverage is preserved.
5. `Release` requires the freed phase with no ticket. The phase relation also
   gives no evidence. Removing that reservation subtracts exactly its positive
   allowance, keeping the sum nonnegative and within budget.
6. Other owners are unchanged by every transition. Stuttering also preserves
   the invariant. Induction over transitions therefore gives
   `Spec => []OwnershipInvariant` under the stated assumptions.

This is a written inductive proof of the abstract protocol, not a TLAPS-checked
theorem or a mechanized Rust refinement. The fixed-budget model represents
owners sharing one admission budget and their retained allowances, not all
active work, allocator overhead, native memory or
process RSS. Shared active permits retain their existing last-owner semantics.

## Executable checks and negative controls

The positive TLC configuration has two owners with distinct allowances, a
three-unit budget, all interleavings, caller deferrals, and unwind before or
after native removal. It checks the stronger invariant over the complete
configured state graph. No fairness or cleanup-liveness assertion is assumed.

Two independently runnable controls demonstrate that the properties detect
the ownership mistakes:

- `ForgetOnUnwind` disables guard restoration. The trace
  `Reserve -> Create -> Begin -> Unwind` violates `EvidenceHasOwner`.
- `ReleaseBeforeTicketFree` refunds admission while the ticket is still live.
  `Reserve -> Create -> Begin -> Remove -> FreeTicket` violates
  `TicketMemoryCovered`.

```console
bazel test //docs/tla:HawDBSearchStageCleanup_check //docs/tla:HawDBSearchStageCleanupForgetOnUnwind_check //docs/tla:HawDBSearchStageCleanupReleaseBeforeTicketFree_check
```

The positive model is in the authoritative model manifest and the negative
controls are in `mutants/mutants.txt`. TLC success is finite model checking,
not a proof for arbitrary owner counts or the compiled program.

The Rust regression `cleanup_unwind_preserves_evidence_and_retained_admission`
uses the existing test-only, one-shot unlink hook: disconnecting its resume
channel produces a catchable panic after cleanup begins. A subprocess isolates
the hook and registry. It covers writer Drop and public retry, intact evidence,
exact retained host charging, release of active work, changed-limit FD reopen,
and successful later cleanup with zero debt. The original implementation fails
the same test with zero pending stages instead of one. This injects a Rust
unwind; it does not demonstrate a naturally occurring filesystem panic.

The guarantee assumes unwinding runs destructors, registry restoration and
lease destruction do not themselves panic, and owned paths remain valid under
the existing filesystem contract. Panic-abort builds, double-panic termination,
OOM abort, process crashes, power loss, external path replacement, unknown
recursive trees, and native Windows handle qualification are outside this
proof. Permanent failures may retain bounded charged debt indefinitely; operator
remediation and maintenance/reopen coordination remain open in #867.
