// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Differential check of the ledger against an ordered-map reference model of
//! its charging rules (#1615): every admission, refusal, transfer, release and
//! snapshot must match, including the typed refusal fields.

use super::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Ok,
    Budget {
        scope: ResourceBudgetScope,
        class: Option<&'static str>,
        requested: u64,
        used: u64,
        budget: u64,
    },
    Execution,
}

fn outcome(result: Result<()>) -> Outcome {
    match result {
        Ok(()) => Outcome::Ok,
        Err(HawDBError::ResourceBudgetExceeded(error)) => {
            assert_eq!(error.resource, ResourceBudgetKind::MemoryBytes);
            Outcome::Budget {
                scope: error.scope,
                class: error.memory_class,
                requested: error.requested,
                used: error.used,
                budget: error.budget,
            }
        }
        Err(HawDBError::Execution(_)) => Outcome::Execution,
        Err(other) => panic!("unexpected ledger error type: {other:?}"),
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Counter {
    used: usize,
    peak: usize,
}

#[derive(Debug)]
struct ModelAccount {
    class: QueryMemoryClass,
    budget: usize,
    counter: Counter,
}

/// The pre-#1615 rules, written against ordered maps.
#[derive(Debug)]
struct Model {
    budget: usize,
    root: Counter,
    accounts: Vec<ModelAccount>,
    classes: BTreeMap<QueryMemoryClass, Counter>,
}

impl Model {
    fn budget_outcome(
        &self,
        scope: ResourceBudgetScope,
        account: usize,
        requested: usize,
        used: usize,
        budget: usize,
    ) -> Outcome {
        Outcome::Budget {
            scope,
            class: Some(self.accounts[account].class.as_str()),
            requested: requested as u64,
            used: used as u64,
            budget: budget as u64,
        }
    }

    fn reserve(&mut self, account: usize, bytes: usize) -> Outcome {
        if bytes == 0 {
            return Outcome::Ok;
        }
        let entry = &self.accounts[account];
        let Some(account_next) = entry.counter.used.checked_add(bytes) else {
            return self.budget_outcome(
                ResourceBudgetScope::Account,
                account,
                bytes,
                entry.counter.used,
                entry.budget,
            );
        };
        if account_next > entry.budget {
            return self.budget_outcome(
                ResourceBudgetScope::Account,
                account,
                bytes,
                entry.counter.used,
                entry.budget,
            );
        }
        match self.root.used.checked_add(bytes) {
            Some(root_next) if root_next <= self.budget => {
                let class = entry.class;
                let entry = &mut self.accounts[account].counter;
                entry.used = account_next;
                entry.peak = entry.peak.max(account_next);
                self.root.used = root_next;
                self.root.peak = self.root.peak.max(root_next);
                let class = self.classes.entry(class).or_default();
                class.used += bytes;
                class.peak = class.peak.max(class.used);
                Outcome::Ok
            }
            _ => self.budget_outcome(
                ResourceBudgetScope::Query,
                account,
                bytes,
                self.root.used,
                self.budget,
            ),
        }
    }

    fn release(&mut self, account: usize, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let entry = &mut self.accounts[account];
        let released = bytes.min(entry.counter.used);
        entry.counter.used -= released;
        self.root.used = self.root.used.saturating_sub(released);
        if let Some(class) = self.classes.get_mut(&entry.class) {
            class.used = class.used.saturating_sub(released);
        }
    }

    fn can_reserve(&self, account: usize, bytes: usize) -> bool {
        let entry = &self.accounts[account];
        entry
            .counter
            .used
            .checked_add(bytes)
            .is_some_and(|next| next <= entry.budget)
            && self
                .root
                .used
                .checked_add(bytes)
                .is_some_and(|next| next <= self.budget)
    }

    fn available(&self, account: usize) -> usize {
        let entry = &self.accounts[account];
        entry
            .budget
            .saturating_sub(entry.counter.used)
            .min(self.budget.saturating_sub(self.root.used))
    }

    fn transfer(
        &mut self,
        source: usize,
        source_bytes: usize,
        target: usize,
        target_bytes: usize,
    ) -> Outcome {
        if source == target || source_bytes > self.accounts[source].counter.used {
            return Outcome::Execution;
        }
        let entry = &self.accounts[target];
        let target_next = match entry.counter.used.checked_add(target_bytes) {
            Some(next) if next <= entry.budget => next,
            _ => {
                return self.budget_outcome(
                    ResourceBudgetScope::Account,
                    target,
                    target_bytes,
                    entry.counter.used,
                    entry.budget,
                )
            }
        };
        let Some(after_release) = self.root.used.checked_sub(source_bytes) else {
            return Outcome::Execution;
        };
        let root_next = match after_release.checked_add(target_bytes) {
            Some(next) if next <= self.budget => next,
            _ => {
                return self.budget_outcome(
                    ResourceBudgetScope::Query,
                    target,
                    target_bytes.saturating_sub(source_bytes),
                    self.root.used,
                    self.budget,
                )
            }
        };
        let source_class = self.accounts[source].class;
        let target_class = self.accounts[target].class;
        self.accounts[source].counter.used -= source_bytes;
        let entry = &mut self.accounts[target].counter;
        entry.used = target_next;
        entry.peak = entry.peak.max(target_next);
        self.root.used = root_next;
        self.root.peak = self.root.peak.max(root_next);
        if let Some(class) = self.classes.get_mut(&source_class) {
            class.used = class.used.saturating_sub(source_bytes);
        }
        let class = self.classes.entry(target_class).or_default();
        class.used = class.used.saturating_add(target_bytes);
        class.peak = class.peak.max(class.used);
        Outcome::Ok
    }

    fn snapshot(&self) -> QueryMemoryLedgerSnapshot {
        QueryMemoryLedgerSnapshot {
            budget_bytes: self.budget,
            used_bytes: self.root.used,
            peak_bytes: self.root.peak,
            account_count: self.accounts.len(),
            classes: self
                .classes
                .iter()
                .map(|(class, counter)| QueryMemoryClassSnapshot {
                    class: *class,
                    used_bytes: counter.used,
                    peak_bytes: counter.peak,
                })
                .collect(),
        }
    }
}

/// Counts (admitted, account refusals, query refusals, execution errors).
fn tally(counts: &mut [usize; 4], outcome: &Outcome) {
    let slot = match outcome {
        Outcome::Ok => 0,
        Outcome::Budget {
            scope: ResourceBudgetScope::Account,
            ..
        } => 1,
        Outcome::Budget { .. } => 2,
        Outcome::Execution => 3,
    };
    counts[slot] += 1;
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // SplitMix64: deterministic and dependency-free.
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn bytes(&mut self) -> usize {
        match self.below(16) {
            0 => 0,
            1 => usize::MAX,
            2 => usize::MAX - self.below(4),
            _ => self.below(48),
        }
    }
}

#[test]
fn ledger_matches_ordered_map_reference_across_random_operations() {
    const CLASSES: [QueryMemoryClass; 4] = [
        QueryMemoryClass::BlockingState,
        QueryMemoryClass::PipelineBatch,
        QueryMemoryClass::BlockingState,
        QueryMemoryClass::ResultMaterialization,
    ];
    let mut counts = [0usize; 4];
    for seed in 0..64 {
        let mut rng = Rng(seed);
        let root_budget = 64 + rng.below(64);
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(root_budget).unwrap());
        let mut model = Model {
            budget: root_budget,
            root: Counter::default(),
            accounts: Vec::new(),
            classes: BTreeMap::new(),
        };
        let mut accounts = Vec::new();
        for class in CLASSES {
            let budget = 16 + rng.below(root_budget);
            accounts.push(ledger.account(class, "reference", NonZeroUsize::new(budget).unwrap()));
            model.accounts.push(ModelAccount {
                class,
                budget,
                counter: Counter::default(),
            });
        }
        // Each real lease is mirrored by (account index, charged bytes).
        let mut leases: Vec<(QueryMemoryLease, usize, usize)> = Vec::new();
        for step in 0..400 {
            let context = format!("seed {seed} step {step}");
            match rng.below(9) {
                0 | 1 => {
                    let account = rng.below(accounts.len());
                    let bytes = rng.bytes();
                    let expected = model.reserve(account, bytes);
                    let actual = accounts[account].reserve(bytes).map(|lease| {
                        leases.push((lease, account, bytes));
                    });
                    assert_eq!(outcome(actual), expected, "{context}: reserve");
                    tally(&mut counts, &expected);
                }
                2 if !leases.is_empty() => {
                    let index = rng.below(leases.len());
                    let bytes = rng.bytes();
                    let (lease, account, charged) = &mut leases[index];
                    let expected = model.reserve(*account, bytes);
                    let actual = lease.grow(bytes);
                    if actual.is_ok() {
                        *charged = charged.saturating_add(bytes);
                    }
                    assert_eq!(outcome(actual), expected, "{context}: grow");
                    tally(&mut counts, &expected);
                }
                3 if !leases.is_empty() => {
                    let index = rng.below(leases.len());
                    let bytes = rng.bytes();
                    let (lease, account, charged) = &mut leases[index];
                    let released = bytes.min(*charged);
                    model.release(*account, released);
                    *charged -= released;
                    lease.shrink(bytes);
                }
                4 if !leases.is_empty() => {
                    let (lease, account, charged) = leases.swap_remove(rng.below(leases.len()));
                    model.release(account, charged);
                    drop(lease);
                }
                5 if !leases.is_empty() => {
                    let index = rng.below(leases.len());
                    let bytes = rng.below(48);
                    let (lease, account, charged) = &mut leases[index];
                    let account = *account;
                    match lease.split_off(bytes) {
                        Ok(split) => {
                            assert!(bytes <= *charged, "{context}: split");
                            *charged -= bytes;
                            leases.push((split, account, bytes));
                        }
                        Err(error) => {
                            assert!(bytes > *charged, "{context}: split");
                            assert_eq!(outcome(Err(error)), Outcome::Execution);
                        }
                    }
                }
                6 if leases.len() >= 2 => {
                    let source = leases.swap_remove(rng.below(leases.len()));
                    let index = rng.below(leases.len());
                    let (target, account, charged) = &mut leases[index];
                    match target.absorb(source.0) {
                        Ok(()) => {
                            assert_eq!(*account, source.1, "{context}: absorb");
                            *charged += source.2;
                        }
                        Err(returned) => {
                            assert_ne!(*account, source.1, "{context}: absorb");
                            leases.push((returned, source.1, source.2));
                        }
                    }
                }
                7 if leases.len() >= 2 => {
                    let source = rng.below(leases.len());
                    let target = rng.below(leases.len());
                    if source == target {
                        continue;
                    }
                    let source_bytes = rng.below(48);
                    let target_bytes = rng.bytes();
                    let (left, right) = if source < target {
                        let (left, right) = leases.split_at_mut(target);
                        (&mut left[source], &mut right[0])
                    } else {
                        let (left, right) = leases.split_at_mut(source);
                        (&mut right[0], &mut left[target])
                    };
                    let expected = if source_bytes > left.2 {
                        Outcome::Execution
                    } else {
                        model.transfer(left.1, source_bytes, right.1, target_bytes)
                    };
                    let actual = left.0.transfer_to(source_bytes, &mut right.0, target_bytes);
                    if actual.is_ok() {
                        left.2 -= source_bytes;
                        right.2 = right.2.saturating_add(target_bytes);
                    }
                    assert_eq!(outcome(actual), expected, "{context}: transfer");
                    tally(&mut counts, &expected);
                }
                _ => {
                    let account = rng.below(accounts.len());
                    let bytes = rng.bytes();
                    assert_eq!(
                        accounts[account].can_reserve(bytes),
                        model.can_reserve(account, bytes),
                        "{context}: can_reserve"
                    );
                    assert_eq!(
                        accounts[account].available_bytes(),
                        model.available(account),
                        "{context}: available_bytes"
                    );
                }
            }
            for (lease, _, charged) in &leases {
                assert_eq!(lease.bytes(), *charged, "{context}: lease bytes");
            }
            assert_eq!(ledger.snapshot(), model.snapshot(), "{context}: snapshot");
            for (index, account) in accounts.iter().enumerate() {
                assert_eq!(
                    account.peak_bytes(),
                    model.accounts[index].counter.peak,
                    "{context}: account peak"
                );
            }
        }
        // Releases match charges exactly: settling every lease returns to zero.
        drop(leases);
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.used_bytes, 0, "seed {seed}");
        assert!(snapshot.classes.iter().all(|class| class.used_bytes == 0));
    }
    // The sequence must reach every refusal path, not only admissions.
    assert!(counts.iter().all(|&count| count > 100), "{counts:?}");
}

#[test]
fn leases_keep_their_ledger_and_child_backing_after_every_account_handle_drops() {
    let budget = NonZeroUsize::new(64).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let retained = ledger.account(QueryMemoryClass::BlockingState, "retained", budget);
    let source = ledger.source_account_retaining("source", budget, retained);
    let child = source.sub_account(NonZeroUsize::new(32).unwrap()).unwrap();
    let mut lease = child.reserve(8).unwrap();
    let split = lease.split_off(3).unwrap();
    let source_lease = source.reserve(5).unwrap();
    drop((source, child));
    // The child's 32-byte grant and the 5-byte source charge stay on the root.
    assert_eq!(ledger.snapshot().used_bytes, 37);
    drop(lease);
    assert_eq!(ledger.snapshot().used_bytes, 37);
    drop(split);
    // The last child lease releases the grant backing the child ledger.
    assert_eq!(ledger.snapshot().used_bytes, 5);
    drop(ledger);
    drop(source_lease);
}
