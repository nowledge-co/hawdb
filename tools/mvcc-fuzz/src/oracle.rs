// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::case::{apply, Case, Operation, Rows};
use std::collections::BTreeSet;

#[derive(Clone, Default)]
pub struct Footprint {
    pub keys: BTreeSet<i64>,
    pub broad: bool,
}

impl Footprint {
    pub fn record(&mut self, op: &Operation) {
        match *op {
            Operation::Read
            | Operation::LockingRead { .. }
            | Operation::GraphRead
            | Operation::GraphUpdate { .. } => {}
            Operation::Update { id, .. }
            | Operation::Delete { id }
            | Operation::Insert { id, .. } => {
                // Absent and net-zero writes remain intents.
                self.keys.insert(id);
            }
            Operation::BroadUpdate { .. } => self.broad = true,
        }
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.broad || other.broad || !self.keys.is_disjoint(&other.keys)
    }

    fn is_empty(&self) -> bool {
        !self.broad && self.keys.is_empty()
    }
}

pub struct Oracle {
    pub visible: Rows,
    pub private: [Rows; 2],
    pub intents: [Footprint; 2],
    history: Vec<Footprint>,
}

impl Oracle {
    pub fn new(case: &Case) -> Self {
        Self {
            visible: case.initial.clone(),
            private: [case.initial.clone(), case.initial.clone()],
            intents: Default::default(),
            history: Vec::new(),
        }
    }

    pub fn execute(&mut self, tx: usize, op: &Operation) -> Result<(), String> {
        apply(&mut self.private[tx], op)?;
        self.intents[tx].record(op);
        Ok(())
    }

    pub fn conflicts(&self, tx: usize) -> bool {
        !self.intents[tx].is_empty()
            && self
                .history
                .iter()
                .any(|prior| self.intents[tx].overlaps(prior))
    }

    pub fn commit(&mut self, tx: usize) {
        let intent = &self.intents[tx];
        if intent.broad {
            self.visible = self.private[tx].clone();
        } else {
            for key in &intent.keys {
                match self.private[tx].get(key) {
                    Some(value) => {
                        self.visible.insert(*key, *value);
                    }
                    None => {
                        self.visible.remove(key);
                    }
                }
            }
        }
        if !intent.is_empty() {
            self.history.push(intent.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::case::generate;

    #[test]
    fn absent_and_restored_writes_still_conflict() {
        for id in [0, 9] {
            let case = generate(1, 0);
            let mut oracle = Oracle::new(&case);
            let before = oracle.private[0].clone();
            let original = before.get(&id).copied();
            oracle.execute(0, &Operation::Delete { id }).unwrap();
            if let Some(value) = original {
                oracle.execute(0, &Operation::Insert { id, value }).unwrap();
            }
            assert_eq!(oracle.private[0], before);
            oracle.commit(0);
            oracle.execute(1, &Operation::Delete { id }).unwrap();
            assert!(
                oracle.conflicts(1),
                "a net-change-only mutant misses this conflict"
            );
        }
    }

    #[test]
    fn broad_barriers_protect_both_orders_but_disjoint_keys_do_not_conflict() {
        let case = generate(1, 0);
        let narrow = Operation::Update { id: 0, value: 7 };
        let broad = Operation::BroadUpdate {
            minimum: 0,
            value: 8,
        };
        for ops in [[narrow.clone(), broad.clone()], [broad, narrow.clone()]] {
            let mut oracle = Oracle::new(&case);
            for (tx, op) in ops.iter().enumerate() {
                oracle.execute(tx, op).unwrap();
            }
            oracle.commit(0);
            assert!(
                oracle.conflicts(1),
                "a one-sided barrier mutant misses one order"
            );
        }
        let mut oracle = Oracle::new(&case);
        oracle.execute(0, &narrow).unwrap();
        oracle
            .execute(1, &Operation::Update { id: 1, value: 8 })
            .unwrap();
        oracle.commit(0);
        assert!(
            !oracle.conflicts(1),
            "a whole-epoch mutant rejects disjoint writers"
        );
        oracle.commit(1);
        assert_eq!(oracle.visible[&0], 7);
        assert_eq!(oracle.visible[&1], 8);
    }
}
