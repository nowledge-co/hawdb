// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `node IN column` as a hash semi-join (#1485).
//!
//! A WHERE conjunct such as `friend IN friends`, where `friends` is a row
//! column holding a list of collected nodes, compares the candidate's node
//! value with every element. A node value carries its `_id`, so an element
//! can only equal it when it has the same `_id`. The first time a row's
//! candidate reaches such a conjunct, the list's element ids are collected
//! into a set; a candidate whose id is absent is then rejected before its
//! record is read, exactly as the conjunct (false or NULL) would reject it.
//! A candidate whose id is present is read and evaluated as before, so the
//! set never changes a result. A column that is not a list keeps the
//! ordinary evaluation and its error.

use super::*;
use hawdb_plan_cypher::{membership_conjunct, predicate_cannot_fail};
use std::cell::RefCell;
use std::collections::HashSet;

/// Element ids of one list column for the current input row.
pub(super) struct MembershipSet {
    /// `None` when the column is not a list or NULL.
    ids: Option<HashSet<i64>>,
    /// Whether a candidate outside `ids` makes the conjunct NULL rather than
    /// false: the column is NULL or the list has a NULL element.
    unknown: bool,
    _lease: Option<crate::QueryMemoryLease>,
}

/// Per-input-row cache of [`MembershipSet`]s, keyed by column.
#[derive(Default)]
pub(super) struct MembershipSets {
    sets: RefCell<BTreeMap<String, MembershipSet>>,
}

impl MembershipSets {
    /// Drops the sets built for the previous input row.
    pub(super) fn reset(&self) {
        self.sets.borrow_mut().clear();
    }
}

/// Bytes one id occupies in the set, including hash-table overhead.
const SET_ENTRY_BYTES: usize = 32;

impl MatchRuntime<'_> {
    /// Whether the conjuncts that run after step `index` certainly reject
    /// `neighbor` as `target`, before its record is read, without skipping
    /// an error the ordinary evaluation would raise. Only a membership
    /// conjunct on `target` is checked, only when every conjunct before it
    /// in that group cannot fail and is not a membership over a non-list,
    /// and, where a NULL result would not stop the evaluation, only when
    /// every later conjunct of the group cannot fail.
    pub(super) fn membership_rejects(
        &self,
        index: usize,
        target: &str,
        neighbor: NodeId,
        row: &Binding,
    ) -> Result<bool> {
        if self.deferred.get() || self.program.id_set_conjuncts.is_empty() {
            return Ok(false);
        }
        let all = self.program.predicate_conjuncts();
        let planned = |conjunct: &Predicate| {
            self.program.id_set_conjuncts.iter().any(|index| {
                all.get(*index)
                    .is_some_and(|planned| std::ptr::eq(planned, conjunct))
            })
        };
        let group: &[&Predicate] = if index + 1 < self.program.steps.len() {
            self.placed
                .early
                .get(index + 1)
                .map(Vec::as_slice)
                .unwrap_or_default()
        } else {
            &self.placed.last
        };
        // Groups before the last step reject at the first false or NULL
        // conjunct; the last one is a three-valued conjunction that goes on
        // past a NULL, so a later conjunct there can still fail.
        let last = index + 1 >= self.program.steps.len();
        let mut conjuncts = Vec::new();
        for conjunct in group {
            flatten(conjunct, &mut conjuncts);
        }
        for (position, conjunct) in conjuncts.iter().enumerate() {
            if let Some((variable, column)) = membership_conjunct(conjunct)
                && variable == target
                && planned(conjunct)
            {
                match self.membership(column, neighbor, row)? {
                    Membership::NotList => return Ok(false),
                    Membership::Present => continue,
                    Membership::Absent { unknown } => {
                        return Ok(!last
                            || !unknown
                            || conjuncts[position + 1..]
                                .iter()
                                .all(|later| predicate_cannot_fail(later)));
                    }
                }
            }
            if !predicate_cannot_fail(conjunct) {
                return Ok(false);
            }
        }
        Ok(false)
    }

    fn membership(&self, column: &str, neighbor: NodeId, row: &Binding) -> Result<Membership> {
        let mut sets = self.memberships.sets.borrow_mut();
        if !sets.contains_key(column) {
            let set = match row.values.get(column) {
                // `x IN NULL` is NULL: no candidate passes.
                Some(Value::Null) => MembershipSet {
                    ids: Some(HashSet::new()),
                    unknown: true,
                    _lease: None,
                },
                Some(Value::List(values)) => {
                    let lease = self
                        .account
                        .reserve(values.len().saturating_mul(SET_ENTRY_BYTES))?;
                    let ids = values
                        .iter()
                        .filter_map(|value| match value {
                            Value::Map(entries) => match entries.get("_id") {
                                Some(Value::Int(id)) => Some(*id),
                                _ => None,
                            },
                            _ => None,
                        })
                        .collect();
                    MembershipSet {
                        ids: Some(ids),
                        unknown: values.contains(&Value::Null),
                        _lease: Some(lease),
                    }
                }
                _ => MembershipSet {
                    ids: None,
                    unknown: false,
                    _lease: None,
                },
            };
            sets.insert(column.to_string(), set);
        }
        let set = &sets[column];
        Ok(match &set.ids {
            None => Membership::NotList,
            Some(ids) if ids.contains(&(neighbor.0 as i64)) => Membership::Present,
            Some(_) => Membership::Absent {
                unknown: set.unknown,
            },
        })
    }
}

/// What a candidate's id says about one membership conjunct.
enum Membership {
    /// The column is not a list: the ordinary evaluation decides, and fails.
    NotList,
    /// Some element has the id: the ordinary evaluation decides.
    Present,
    /// No element has it: the conjunct is false, or NULL when `unknown`.
    Absent { unknown: bool },
}

fn flatten<'p>(predicate: &'p Predicate, output: &mut Vec<&'p Predicate>) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                flatten(child, output);
            }
        }
        predicate => output.push(predicate),
    }
}
