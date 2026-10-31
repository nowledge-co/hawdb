// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use super::Memo;

pub(super) struct MemoTable<'a> {
    rules: Vec<RuleMemo<'a>>,
    len: usize,
}

#[derive(Default)]
struct RuleMemo<'a> {
    // Forward parsing and probes at the current token avoid a map lookup.
    latest: Option<(usize, Memo<'a>)>,
    older: Older<'a>,
}

#[derive(Default)]
enum Older<'a> {
    #[default]
    Empty,
    // Small rules avoid allocating a hash table. The scan has a fixed bound.
    Small(Vec<(usize, Memo<'a>)>),
    // Dense token visits use direct indexing. At most four slots per memo.
    Dense {
        start: usize,
        slots: Vec<Option<Memo<'a>>>,
        count: usize,
    },
    // A sparse rule stays sparse: repeated density changes cannot cause
    // repeated linear conversions or adversarial quadratic copying.
    Sparse(HashMap<usize, Memo<'a>>),
}

impl<'a> MemoTable<'a> {
    pub fn new(rules: usize) -> Self {
        Self {
            rules: std::iter::repeat_with(RuleMemo::default)
                .take(rules)
                .collect(),
            len: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn get(&self, rule: usize, position: usize) -> Option<&Memo<'a>> {
        let entries = &self.rules[rule];
        match &entries.latest {
            Some((latest, memo)) if position == *latest => Some(memo),
            None => None,
            Some((latest, _)) if position > *latest => None,
            Some(_) => entries.older.get(position),
        }
    }
    pub fn insert(&mut self, rule: usize, position: usize, memo: Memo<'a>) {
        let entries = &mut self.rules[rule];
        match &mut entries.latest {
            slot @ None => {
                *slot = Some((position, memo));
                self.len += 1;
            }
            Some((latest, value)) if position == *latest => *value = memo,
            Some((latest, value)) if position > *latest => {
                let old_position = std::mem::replace(latest, position);
                let old_value = std::mem::replace(value, memo);
                entries.older.insert(old_position, old_value);
                self.len += 1;
            }
            Some(_) => self.len += usize::from(entries.older.insert(position, memo)),
        }
    }
}

impl<'a> Older<'a> {
    fn get(&self, position: usize) -> Option<&Memo<'a>> {
        match self {
            Self::Empty => None,
            Self::Small(entries) => entries
                .iter()
                .find(|(key, _)| *key == position)
                .map(|(_, memo)| memo),
            Self::Dense { start, slots, .. } => position
                .checked_sub(*start)
                .and_then(|index| slots.get(index))
                .and_then(Option::as_ref),
            Self::Sparse(entries) => entries.get(&position),
        }
    }
    // Returns true only for a new logical memo entry, including dense holes.
    fn insert(&mut self, position: usize, memo: Memo<'a>) -> bool {
        match self {
            Self::Empty => {
                *self = Self::Small(vec![(position, memo)]);
                true
            }
            Self::Small(entries) => {
                if let Some((_, value)) = entries.iter_mut().find(|(key, _)| *key == position) {
                    *value = memo;
                    return false;
                }
                entries.push((position, memo));
                if entries.len() == 9 {
                    let start = entries
                        .iter()
                        .map(|(key, _)| *key)
                        .min()
                        .expect("nine entries");
                    let end = entries
                        .iter()
                        .map(|(key, _)| *key)
                        .max()
                        .expect("nine entries");
                    if end - start < 4 * entries.len() {
                        let count = entries.len();
                        let mut slots: Vec<_> = std::iter::repeat_with(|| None)
                            .take(end - start + 1)
                            .collect();
                        for (key, value) in entries.drain(..) {
                            slots[key - start] = Some(value);
                        }
                        *self = Self::Dense {
                            start,
                            slots,
                            count,
                        };
                    } else {
                        *self = Self::Sparse(entries.drain(..).collect());
                    }
                }
                true
            }
            Self::Dense {
                start,
                slots,
                count,
            } => {
                if let Some(index) = position
                    .checked_sub(*start)
                    .filter(|index| *index < slots.len())
                {
                    let fresh = slots[index].replace(memo).is_none();
                    *count += usize::from(fresh);
                    return fresh;
                }
                // Only append cheap, dense forward visits. An earlier visit
                // or large gap converts once to the permanent sparse form.
                if let Some(index) = position
                    .checked_sub(*start)
                    .filter(|index| *index < 4 * (*count + 1))
                {
                    slots.resize_with(index + 1, || None);
                    slots[index] = Some(memo);
                    *count += 1;
                } else {
                    let mut values: HashMap<_, _> = slots
                        .drain(..)
                        .enumerate()
                        .filter_map(|(index, value)| value.map(|value| (index + *start, value)))
                        .collect();
                    values.insert(position, memo);
                    *self = Self::Sparse(values);
                }
                true
            }
            Self::Sparse(entries) => entries.insert(position, memo).is_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_cache_preserves_pending_backwards_and_replaced_entries() {
        let mut table = MemoTable::new(2);
        table.insert(0, 1, Memo::Pending);
        table.insert(0, 3, Memo::Pending);
        table.insert(1, 2, Memo::Failed(None));
        assert!(matches!(table.get(0, 1), Some(Memo::Pending)));
        assert!(matches!(table.get(0, 3), Some(Memo::Pending)));
        assert!(table.get(0, 4).is_none());
        assert!(table.get(0, 2).is_none());
        table.insert(0, 1, Memo::Matched(2, 10, None));
        table.insert(0, 3, Memo::Failed(None));
        assert_eq!(table.len(), 3);
        assert!(matches!(table.get(0, 1), Some(Memo::Matched(2, 10, None))));
        assert!(matches!(table.get(0, 3), Some(Memo::Failed(None))));
        table.insert(0, 2, Memo::Failed(None));
        table.insert(0, 5, Memo::Matched(6, 11, None));
        assert_eq!(table.len(), 5);
        assert!(matches!(table.get(0, 2), Some(Memo::Failed(None))));
        assert!(matches!(table.get(0, 3), Some(Memo::Failed(None))));
        assert!(matches!(table.get(0, 5), Some(Memo::Matched(6, 11, None))));
        assert!(matches!(table.get(1, 2), Some(Memo::Failed(None))));
    }

    #[test]
    fn adaptive_storage_matches_sparse_oracle_through_density_changes() {
        let dense: Vec<_> = (0..48).map(|i| i * 2).collect();
        let sparse: Vec<_> = (0..48).map(|i| i * 1000).collect();
        let mut gap = dense.clone();
        gap.extend([10000, 10002, 1, 3, 10001]);
        let mut earlier: Vec<_> = (100..148).collect();
        earlier.extend([1, 2, 3]);
        for positions in [dense, sparse, gap, earlier] {
            let mut table = MemoTable::new(2);
            let mut oracle = HashMap::new();
            for (index, position) in positions
                .iter()
                .chain(positions.iter().rev())
                .copied()
                .enumerate()
            {
                let rule = index % 2;
                table.insert(rule, position, Memo::Matched(position + 1, index, None));
                oracle.insert((rule, position), index);
                assert_eq!(table.len(), oracle.len());
                for (&(rule, key), &value) in &oracle {
                    assert!(
                        matches!(table.get(rule, key), Some(Memo::Matched(end, node, None)) if *end == key + 1 && *node == value)
                    );
                }
                assert!(table.get(rule, 1000000).is_none());
            }
        }
    }
}
