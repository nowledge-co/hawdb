// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const FORMAT_VERSION: u32 = 1;
pub type Rows = BTreeMap<i64, i64>;

/// Concurrency control used by both transactions of a case.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Optimistic,
    Pessimistic,
}

impl Mode {
    pub fn is_optimistic(&self) -> bool {
        *self == Self::Optimistic
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub format_version: u32,
    pub seed: u64,
    pub index: u64,
    /// Omitted for optimistic cases so earlier artifacts stay byte-identical.
    #[serde(default, skip_serializing_if = "Mode::is_optimistic")]
    pub mode: Mode,
    pub initial: Rows,
    pub transactions: [Program; 2],
    /// Each entry executes the named transaction's next operation or terminal.
    pub schedule: Vec<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Program {
    pub operations: Vec<Operation>,
    pub rollback: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Read,
    Update {
        id: i64,
        value: i64,
    },
    Delete {
        id: i64,
    },
    Insert {
        id: i64,
        value: i64,
    },
    BroadUpdate {
        minimum: i64,
        value: i64,
    },
    /// `SELECT ... FOR SHARE` or `FOR UPDATE`; pessimistic cases only.
    LockingRead {
        exclusive: bool,
    },
    /// Cypher read of every `Record` node; pessimistic cases only.
    GraphRead,
    /// Cypher property write of one `Record` node; pessimistic cases only.
    GraphUpdate {
        id: i64,
        value: i64,
    },
}

impl Operation {
    pub fn is_pessimistic_only(&self) -> bool {
        matches!(
            self,
            Self::LockingRead { .. } | Self::GraphRead | Self::GraphUpdate { .. }
        )
    }

    pub fn is_graph(&self) -> bool {
        matches!(self, Self::GraphRead | Self::GraphUpdate { .. })
    }

    /// Statement text and positional values; graph statements bind `$value`
    /// then `$id`.
    pub fn sql(&self) -> (&'static str, Vec<i64>) {
        match *self {
            Self::Read => ("SELECT id, value FROM records ORDER BY id", vec![]),
            Self::Update { id, value } => (
                "UPDATE records SET value = $1 WHERE id = $2",
                vec![value, id],
            ),
            Self::Delete { id } => ("DELETE FROM records WHERE id = $1", vec![id]),
            Self::Insert { id, value } => (
                "INSERT INTO records (id, value) VALUES ($1, $2)",
                vec![id, value],
            ),
            Self::BroadUpdate { minimum, value } => (
                "UPDATE records SET value = $1 WHERE value >= $2",
                vec![value, minimum],
            ),
            Self::LockingRead { exclusive: false } => (
                "SELECT id, value FROM records ORDER BY id FOR SHARE",
                vec![],
            ),
            Self::LockingRead { exclusive: true } => (
                "SELECT id, value FROM records ORDER BY id FOR UPDATE",
                vec![],
            ),
            Self::GraphRead => (
                "MATCH (n:Record) RETURN n.id AS id, n.value AS value ORDER BY n.id",
                vec![],
            ),
            Self::GraphUpdate { id, value } => (
                "MATCH (n:Record) WHERE n.id = $id SET n.value = $value",
                vec![value, id],
            ),
        }
    }
}

impl Case {
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != FORMAT_VERSION {
            return Err("unsupported case format_version".into());
        }
        if self.initial.len() > 64 || self.schedule.len() > 128 {
            return Err("case exceeds row or schedule limits".into());
        }
        let mut counts = [0; 2];
        for &tx in &self.schedule {
            let Some(count) = counts.get_mut(tx) else {
                return Err("schedule transaction must be 0 or 1".into());
            };
            *count += 1;
        }
        for (tx, program) in self.transactions.iter().enumerate() {
            if program.operations.len() > 63 || counts[tx] != program.operations.len() + 1 {
                return Err(
                    "schedule must execute every operation and one terminal per transaction".into(),
                );
            }
            let mut rows = self.initial.clone();
            for op in &program.operations {
                if op.is_pessimistic_only() && self.mode.is_optimistic() {
                    return Err(
                        "locking reads and graph operations require a pessimistic case".into(),
                    );
                }
                apply(&mut rows, op)?;
            }
        }
        Ok(())
    }
}

/// This interpreter uses generated operations, never production SQL/footprint code.
pub fn apply(rows: &mut Rows, op: &Operation) -> Result<(), String> {
    match *op {
        Operation::Read
        | Operation::LockingRead { .. }
        | Operation::GraphRead
        | Operation::GraphUpdate { .. } => {}
        Operation::Update { id, value } => {
            if let Some(row) = rows.get_mut(&id) {
                *row = value;
            }
        }
        Operation::Delete { id } => {
            rows.remove(&id);
        }
        Operation::Insert { id, value } => {
            if rows.contains_key(&id) {
                return Err("case inserts an existing primary key in its private snapshot".into());
            }
            rows.insert(id, value);
        }
        Operation::BroadUpdate { minimum, value } => {
            for row in rows.values_mut() {
                if *row >= minimum {
                    *row = value;
                }
            }
        }
    }
    Ok(())
}

pub(crate) struct Random(u64);

impl Random {
    pub(crate) fn new(seed: u64, index: u64) -> Self {
        Self(seed.wrapping_add(index.wrapping_mul(0x9e3779b97f4a7c15)))
    }

    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
}

pub fn generate(seed: u64, index: u64) -> Case {
    let mut random = Random::new(seed, index);
    let initial: Rows = (0..4).map(|id| (id, (random.next() % 20) as i64)).collect();
    let key = (random.next() % 4) as i64;
    let value = (random.next() % 100) as i64;
    let update = Operation::Update { id: key, value };
    // Cycle every conflict family; randomness varies keys, values and interleavings.
    let (left, right) = match index % 8 {
        0 => (
            vec![update.clone()],
            vec![Operation::Update {
                id: key,
                value: value + 1,
            }],
        ),
        1 => (
            vec![update.clone()],
            vec![Operation::Update {
                id: (key + 1) % 4,
                value,
            }],
        ),
        2 => (
            vec![
                update.clone(),
                Operation::Update {
                    id: key,
                    value: initial[&key],
                },
            ],
            vec![update],
        ),
        3 => (
            vec![Operation::Delete { id: 4 }],
            vec![Operation::Insert { id: 4, value }],
        ),
        4 => (
            vec![
                Operation::Delete { id: key },
                Operation::Insert { id: key, value },
            ],
            vec![update],
        ),
        5 => (
            vec![Operation::BroadUpdate {
                minimum: (random.next() % 20) as i64,
                value,
            }],
            vec![update],
        ),
        6 => (
            vec![Operation::Insert { id: 4, value }],
            vec![Operation::Insert {
                id: 4,
                value: value + 1,
            }],
        ),
        _ => (vec![update.clone()], vec![update]),
    };
    let transactions = [left, right].map(|operations| {
        let mut sequence = vec![Operation::Read];
        for op in operations {
            sequence.push(op);
            sequence.push(Operation::Read);
        }
        Program {
            operations: sequence,
            rollback: false,
        }
    });
    let mut case = Case {
        format_version: FORMAT_VERSION,
        seed,
        index,
        mode: Mode::Optimistic,
        initial,
        transactions,
        schedule: Vec::new(),
    };
    if index % 8 == 7 {
        case.transactions[(random.next() % 2) as usize].rollback = true;
    }
    interleave(&mut case, &mut random);
    case
}

/// Appends a random interleaving that preserves each transaction's program order.
pub(crate) fn interleave(case: &mut Case, random: &mut Random) {
    let mut remaining = case
        .transactions
        .each_ref()
        .map(|tx| tx.operations.len() + 1);
    while remaining != [0, 0] {
        let mut tx = (random.next() % 2) as usize;
        if remaining[tx] == 0 {
            tx = 1 - tx;
        }
        case.schedule.push(tx);
        remaining[tx] -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_cases_are_valid_and_replayable() {
        for seed in [0, 1, 42, u64::MAX] {
            for index in 0..256 {
                let case = generate(seed, index);
                case.validate().unwrap();
                let encoded = serde_json::to_string(&case).unwrap();
                let decoded: Case = serde_json::from_str(&encoded).unwrap();
                assert_eq!(encoded, serde_json::to_string(&decoded).unwrap());
                assert_eq!(
                    encoded,
                    serde_json::to_string(&generate(seed, index)).unwrap()
                );
            }
        }
    }

    #[test]
    fn malformed_schedule_and_duplicate_insert_are_rejected() {
        let mut case = generate(1, 0);
        case.schedule[0] = 2;
        assert!(case.validate().is_err());
        let mut case = generate(1, 0);
        case.transactions[0].operations[0] = Operation::Insert { id: 0, value: 0 };
        assert!(case.validate().is_err());
        let mut case = generate(1, 0);
        case.schedule.pop();
        assert!(case.validate().is_err());
    }
}
