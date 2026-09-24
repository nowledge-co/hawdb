# Query memory and concurrent spill compaction

This proof accompanies #745 and `HawDBQueryMemoryLedger.tla`. It concerns
**accounted bytes**, not total process RSS, allocator overhead, untracked I/O
buffers, or arbitrary allocations by caller-provided codecs. The proof below
is mathematical induction with an explicit implementation mapping; TLC checks
a finite instance. Neither is a machine-checked refinement proof of Rust.

## Budget invariant and induction

Let the query budget be Q, parent account budgets be B[a], direct live charges
be D[a], and each live child c have allowance C[c] and charged payload L[c].
Let P(c) be the child's parent. Define

```
K[a] = D[a] + sum(C[c] for live children c with P(c) = a)
R    = sum(K[a] for all root accounts a)
```

The invariant I is the conjunction of:

1. All charges and allowances are nonnegative; live children have positive
   allowances, and `0 <= L[c] <= C[c]`.
2. `K[a] <= B[a]` for every parent and `R <= Q`.
3. A child's backing reservation remains live while any child account clone,
   sibling account, nested child, or lease retains it, including a zero-byte
   lease. Once the last reference drops, both its charge and payload vanish.
4. Peak charge is monotone, at least R, and no greater than Q.

Initially all sums are zero, so I holds. Assume I holds before an action:

- **Direct reserve or child creation:** admission checks the proposed increment
  b against both `K[a] + b <= B[a]` and `R + b <= Q` under the parent ledger's
  mutex. Only successful admission increments these counters; the peak becomes
  `max(peak, R + b)`. Child creation has `C[c] = b, L[c] = 0`. This preserves I.
- **Child grow:** admission checks `L[c] + b <= C[c]` in the child ledger. The
  backing allowance is already part of R, so no parent/root increment occurs.
  This preserves I without charging the same payload twice.
- **Shrink/reset:** decrease a live payload or direct charge without releasing
  capacity still referenced by a child. Nonnegativity and upper bounds remain.
- **Clone or intermediate drop:** the backing is shared, so charges do not
  change. **Final drop** first removes live payload ownership and then releases
  the backing; all affected sums decrease. A dropped account with a surviving
  lease is an intermediate drop, even if the lease has zero bytes.
- **Transfer:** only same-ledger ownership can transfer. The implementation
  checks the target and post-transfer root totals before mutation; the model
  represents equal-sized direct transfers. A rejected cross-ledger transfer
  cannot bypass child admission. Different-sized transfers remain covered by
  implementation tests and the same post-total inequality argument.
- **Failure/cancellation:** after dispatched tasks join and owners unwind, each
  remaining charge follows the drop rules. The model's terminal actions
  abstract this completed cleanup, not instantaneous cleanup on cancellation
  request. Caller-retained results remain a separate returned state.

All actions preserve I, hence I holds for every finite execution. Summing
`L[c] <= C[c]` proves that aggregate child payload plus direct charges is no
larger than R. For nested children, apply the same argument at each finite
ledger-tree level. The model checks one child level; nested ownership and
sibling retention have separate Rust regressions. The mathematical induction
is not restricted to the model's two merge identities or small integer budgets.
Rust checked arithmetic rejects overflow before a counter change; saturating
size bounds may conservatively reduce concurrency but cannot wrap an allowance.

## Allowance derivation and scheduler

For a known binding codec, let m[r] be the maximum of decoded binding size and
mapped merge-row size observed while writing run r, and s[r] its maximum encoded
payload length. Every merge retains at most one head from each input. Reading
one head replaces its decoded binding charge with its mapped-row charge; its
maximum is already included in m[r]. Thus a pair's charged blocking working set
is bounded by `m[left] + m[right]`. In particular, partial aggregation records
must include the full merge-row structure, not only hash-group payload bytes.

Sort and DISTINCT output a selected input binding. Raw aggregate output
re-encodes a selected compact input. Partial aggregate reduction selects each
Min/Max component from one of the two inputs, with fixed-width Count/Avg state;
its encoded output is bounded by the sum of the two encoded input bounds.
Reading/writing payloads is sequential within a pair. Reserving
`s[left] + s[right]` therefore covers its charged staging buffer. Every produced
run records new bounds, so the argument applies inductively across levels.
Opaque external-order codecs have no decoded-size contract and use the full
serial allowances instead. These are codec-specific premises, not properties
proved by the ledger model.

The scheduler caps conservative allowances at the original serial capacity.
It admits whole child allowances, never divides the single-item limit by the
worker count. Each successful admission preserves I. If another pair cannot
be admitted, the already admitted wave finishes and releases its reservations
before retrying. If no pair can be admitted, the pair runs alone against the
original accounts, whose actual allocation checks preserve I. This matters when
run maxima occur at different positions: their sum may exceed a budget even
though the serial working set fits. Blocking admission followed by failed
staging admission releases the temporary blocking reservation without running
the merge or consuming a spill run/write budget.

Pairing and level boundaries are unchanged. By induction on levels, each pair
receives the same ordered input runs as serial compaction and applies the same
codec/reduction; ordered collection yields identical output runs regardless of
completion order. No additional floating-point reassociation is introduced.
On any error or unwind, inputs and all completed sibling outputs remain owned
by the failing level and drop before returning an error. Later waves do not
start after an observed failure. With finite runs, no cancellation, successful
merges, and eventual worker completion, each iteration advances at least one
pair and each level reduces run count, so compaction terminates. This is a
conditional progress argument; the TLA+ model asserts safety, not scheduler
fairness or codec termination.

## Implementation and verification mapping

| Model/proof transition | Rust implementation or regression |
| --- | --- |
| Atomic direct/root admission | `QueryMemoryLedger::reserve` |
| Child allowance and root backing | `QueryMemoryAccount::sub_account` |
| Child payload growth | `QueryMemoryLease::grow` in the private child ledger |
| Surviving clones, siblings, and leases | `QueryMemoryAccount::backing`, `sibling`, RAII lease drop |
| Two-stage admission and rollback | `compact_runs_with_memory`, `SpillBudgetTracker::for_merge` |
| Bounds propagated across levels | `SpillWriter::note_merge_record_bytes`, `write`, `write_record_payload` |
| Shared cumulative disk/run limits | Shared atomic counters in `SpillBudgetTracker` |
| Failed admission/lifetime | `sub_accounts_reserve_parent_capacity_without_double_charging`, `nested_accounts_and_siblings_keep_backing_until_last_owner_drops`, `failed_sub_account_admission_does_not_leak_root_capacity` |
| Concurrent waves, tight budgets | `independent_merge_allowances_admit_real_concurrency_and_preserve_large_items`, `conservative_root_bounds_fall_back_without_rejecting_a_serial_working_set`, `unequal_run_bounds_survive_multiple_levels_and_shared_staging_limits` |
| Output equivalence and cleanup | `concurrent_compaction_matches_serial_byte_for_byte`, `every_merge_failure_and_unwind_releases_partial_outputs_and_pending_inputs`, `compaction_differential_campaign` |

The memory model does not model spill file contents, codec allocation internals,
OS failures, or a whole compaction level. The proof premises for those paths
are checked against the above implementation and real-file tests. Updating
row codecs, fan-in, live-row retention, or account lifetime requires revisiting
these premises as well as the tests and model.
