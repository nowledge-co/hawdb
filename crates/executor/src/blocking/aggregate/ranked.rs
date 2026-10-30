// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded evidence for every observed group, finalized after input exhaustion.

use super::*;
use crate::QueryMemoryLease;
use hawdb_plan_cypher::ProjectionExpression;
use std::collections::BTreeSet;

// Admit independent ID/rank trees, including sparse B-tree nodes and splits.
// Payloads and replacement overlap are additional. Allocation probes qualify
// this conservative envelope for the repository's pinned native toolchain.
const EVIDENCE_ENTRY_BYTES: usize = 4096;
const GROUP_ENTRY_BYTES: usize = 2048;

#[derive(Clone, Copy, Debug)]
struct Score(f64);

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

struct Evidence {
    by_id: BTreeMap<String, Score>,
    by_score: BTreeMap<Score, BTreeSet<String>>,
    limit: usize,
    memory: QueryMemoryLease,
}

impl Evidence {
    fn new(limit: usize, account: &QueryMemoryAccount) -> Result<Self> {
        if limit == 0 {
            return Err(invalid("evidence limit must be positive"));
        }
        Ok(Self {
            by_id: BTreeMap::new(),
            by_score: BTreeMap::new(),
            limit,
            memory: account.reserve(0)?,
        })
    }

    fn entry_bytes(id: &str) -> Result<usize> {
        id.len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(EVIDENCE_ENTRY_BYTES))
            .ok_or_else(|| invalid("evidence admission size overflow"))
    }

    fn remove_rank(&mut self, id: &str, score: Score) {
        let bucket = self.by_score.get_mut(&score).expect("ID has a rank");
        assert!(bucket.remove(id));
        if bucket.is_empty() {
            self.by_score.remove(&score);
        }
    }

    fn push(&mut self, id: &str, score: f64) -> Result<()> {
        if !score.is_finite() {
            return Err(invalid("score must be a finite Float"));
        }
        let score = Score(score);
        let previous = self.by_id.get(id).copied();
        if previous.is_some_and(|old| old >= score) {
            return Ok(());
        }
        if previous.is_none() && self.by_id.len() == self.limit {
            let (worst_score, ids) = self.by_score.first_key_value().expect("full evidence");
            let worst_id = ids.last().expect("nonempty score bucket");
            if score < *worst_score || (score == *worst_score && id >= worst_id.as_str()) {
                return Ok(());
            }
        }
        let bytes = Self::entry_bytes(id)?;
        self.memory.grow(bytes)?;
        let id_key = id.to_owned();
        let rank_key = id.to_owned();
        if let Some(old) = previous {
            self.remove_rank(id, old);
        }
        self.by_id.insert(id_key, score);
        self.by_score.entry(score).or_default().insert(rank_key);
        if previous.is_some() {
            self.memory.shrink(bytes);
        } else if self.by_id.len() > self.limit {
            let (worst_score, ids) = self.by_score.first_key_value().expect("full evidence");
            let worst_score = *worst_score;
            let worst_id = ids.last().expect("nonempty score bucket");
            let (id, _) = self
                .by_id
                .remove_entry(worst_id.as_str())
                .expect("rank has an ID");
            self.remove_rank(&id, worst_score);
            let released = Self::entry_bytes(&id)?;
            drop(id);
            self.memory.shrink(released);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(Value, QueryMemoryLease)> {
        self.memory.grow(
            self.by_id
                .len()
                .checked_mul(std::mem::size_of::<Value>())
                .ok_or_else(|| invalid("evidence output size overflow"))?,
        )?;
        let mut values = Vec::with_capacity(self.by_id.len());
        for (_, ids) in self.by_score.into_iter().rev() {
            values.extend(ids.into_iter().map(Value::String));
        }
        Ok((Value::List(values), self.memory))
    }
}

enum State {
    Maximum(Option<Value>),
    Evidence(Evidence),
}

struct Group {
    states: Vec<State>,
    memory: QueryMemoryLease,
}

fn invalid(message: &str) -> HawDBError {
    HawDBError::Semantic(format!("collect_top: {message}"))
}

fn scalar_payload_bytes(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::Uuid(_) => 0,
        Value::String(text) => text.len(),
        Value::Binary(bytes) => bytes.len(),
        Value::Vector(vector) => vector.encoded().len(),
        Value::List(_) | Value::Map(_) => value_memory_bytes(value).saturating_mul(16),
    }
}

fn maximum_input<'a>(target: &AggregateTarget, binding: &'a Binding) -> Option<&'a Value> {
    match target {
        AggregateTarget::Column(column) => binding.values.get(column),
        AggregateTarget::ColumnProperty { column, property } => match binding.values.get(column)? {
            Value::Map(values) => values.get(property),
            _ => None,
        },
        AggregateTarget::Property { variable, property } => {
            binding_property(binding, variable, property)
        }
        _ => None,
    }
    .filter(|value| **value != Value::Null)
}

fn new_group(
    key_memory: &mut QueryMemoryLease,
    items: &[Aggregation],
    account: &QueryMemoryAccount,
) -> Result<Group> {
    let bytes = items
        .len()
        .checked_mul(std::mem::size_of::<State>())
        .and_then(|n| n.checked_add(GROUP_ENTRY_BYTES))
        .ok_or_else(|| invalid("group state size overflow"))?;
    let mut memory = account.reserve(bytes)?;
    let mut states = Vec::with_capacity(items.len());
    for item in items {
        states.push(match item.target {
            AggregateTarget::Ranked { limit, .. } => {
                State::Evidence(Evidence::new(limit, account)?)
            }
            _ => State::Maximum(None),
        });
    }
    // Keep the caller's key admission until every fallible construction step
    // succeeds. The caller also owns the key if this transfer is refused.
    let key_bytes = key_memory.bytes();
    key_memory.transfer_to(key_bytes, &mut memory, key_bytes)?;
    Ok(Group { states, memory })
}

pub(super) fn stream(
    input: &PhysicalPlan,
    group_keys: &[Projection],
    items: &[Aggregation],
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    for item in items {
        let valid = !item.distinct
            && match (&item.function, &item.target) {
                (AggregateFunction::Collect, AggregateTarget::Ranked { limit, .. }) => *limit > 0,
                (
                    AggregateFunction::Max,
                    AggregateTarget::Column(_)
                    | AggregateTarget::ColumnProperty { .. }
                    | AggregateTarget::Property { .. },
                ) => true,
                _ => false,
            };
        if !valid {
            return Err(invalid(
                "ranked evidence supports collect_top and MAX aggregates",
            ));
        }
    }
    let account = context.memory_ledger.account_with_budget_option(
        QueryMemoryClass::BlockingState,
        "AggregateExec collect_top",
        context.memory.blocking_operator_bytes,
        "ExecutionMemoryConfig.blocking_operator_bytes",
    );
    let key_memory = KeyMemory::new(
        &account,
        context.memory.query_memory_bytes,
        "ExecutionMemoryConfig.query_memory_bytes",
        "collect_top expressions",
        "collect_top group keys",
    );
    let mut groups = BTreeMap::<Vec<Value>, Group>::new();
    let mut input_rows = 0usize;
    let mut borrowed_group_key_rows = 0usize;
    runtime_checkpoint(context.task_context)?;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            runtime_checkpoint(context.task_context)?;
            let borrowed_key: Option<&[Value]> = match group_keys {
                [] => Some(&[]),
                [Projection {
                    expression: ProjectionExpression::Column(column),
                    ..
                }] => binding.values.get(column).map(std::slice::from_ref),
                _ => None,
            };
            let existing_key = borrowed_key.filter(|key| groups.contains_key(*key));
            let group = if let Some(key) = existing_key {
                borrowed_group_key_rows += 1;
                groups.get_mut(key).expect("existing group")
            } else {
                let PreparedKey {
                    values: key,
                    mut allocation,
                } = key_memory.group(
                    group_keys,
                    context.catalog,
                    &binding,
                    context.task_context,
                )?;
                if key.iter().any(|value| {
                    !matches!(
                        value,
                        Value::Null
                            | Value::Bool(_)
                            | Value::Int(_)
                            | Value::String(_)
                            | Value::Uuid(_)
                    )
                }) {
                    return Err(invalid(
                        "group keys support NULL, Bool, Int, String and Uuid",
                    ));
                }
                match groups.entry(key) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(new_group(&mut allocation, items, &account)?)
                    }
                }
            };
            for (state, item) in group.states.iter_mut().zip(items) {
                match state {
                    State::Evidence(evidence) => {
                        let AggregateTarget::Ranked {
                            document_id, score, ..
                        } = &item.target
                        else {
                            unreachable!()
                        };
                        let (Some(Value::String(id)), Some(Value::Float(score))) =
                            (binding.values.get(document_id), binding.values.get(score))
                        else {
                            return Err(invalid(
                                "evidence requires a non-null String ID and finite Float score",
                            ));
                        };
                        evidence.push(id, *score)?;
                    }
                    State::Maximum(current) => {
                        if let Some(value) = maximum_input(&item.target, &binding)
                            && current.as_ref().is_none_or(|previous| value > previous)
                        {
                            let incoming = scalar_payload_bytes(value);
                            group.memory.grow(incoming)?;
                            let old = current.replace(value.clone());
                            let released = old.as_ref().map_or(0, scalar_payload_bytes);
                            drop(old);
                            group.memory.shrink(released);
                        }
                    }
                }
            }
            input_rows = input_rows.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;
    runtime_checkpoint(context.task_context)?;
    if groups.is_empty() && group_keys.is_empty() {
        let PreparedKey {
            values,
            mut allocation,
        } = key_memory.group(
            group_keys,
            context.catalog,
            &Binding::values(BTreeMap::new()),
            context.task_context,
        )?;
        groups.insert(values, new_group(&mut allocation, items, &account)?);
    }
    let groups_considered = groups.len();
    let retained_evidence = groups
        .values()
        .flat_map(|group| &group.states)
        .map(|state| match state {
            State::Evidence(evidence) => evidence.by_id.len(),
            State::Maximum(_) => 0,
        })
        .sum();
    let mut output_groups = 0;
    let result = finish_groups(
        groups,
        group_keys,
        items,
        &account,
        context,
        execution_limit,
        &mut |batch| {
            output_groups += batch.len();
            emit(batch)
        },
    );
    context
        .observer
        .record_blocking_memory_report(BlockingOperatorMemoryReport {
            operator: "AggregateExec collect_top".into(),
            budget_bytes: context.memory.blocking_operator_bytes.get(),
            peak_tracked_bytes: account.peak_bytes(),
            input_rows,
            candidate_rows: 0,
            replay_rows: 0,
            repartitions: 0,
            max_spill_bytes: context.memory.max_spill_bytes.get(),
            max_spill_runs: context.memory.max_spill_runs.get(),
            spilled_bytes: 0,
            spill_run_count: 0,
            spilled_rows: 0,
        });
    context.observer.record_grouped_evidence_report(
        input,
        crate::GroupedEvidenceExecutionReport {
            operator_id: None,
            executed: true,
            input_exhausted: true,
            input_rows,
            borrowed_group_key_rows,
            groups_considered,
            retained_evidence,
            output_groups,
            peak_state_bytes: account.peak_bytes(),
            spilled_bytes: 0,
        },
    );
    result
}

fn emit_checked(
    rows: BindingBatch,
    task: Option<&RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(task)?;
    let control = emit(rows)?;
    runtime_checkpoint(task)?;
    Ok(control)
}

fn finish_groups(
    groups: BTreeMap<Vec<Value>, Group>,
    group_keys: &[Projection],
    items: &[Aggregation],
    account: &QueryMemoryAccount,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let output_account = account.sibling(
        QueryMemoryClass::PipelineBatch,
        "collect_top output",
        context.memory.batch_payload_bytes,
    );
    let mut rows = Vec::new();
    let mut owners = Vec::new();
    let mut output_memory = output_account.reserve(0)?;
    let mut emitted = 0usize;
    for (key, group) in groups {
        runtime_checkpoint(context.task_context)?;
        if execution_limit.is_reached(emitted) {
            break;
        }
        let output_bytes = group_keys
            .iter()
            .map(|item| item.name.len())
            .chain(items.iter().map(|item| item.name.len()))
            .try_fold(4096usize, |total, len| {
                total
                    .checked_add(len.saturating_mul(2))
                    .and_then(|n| n.checked_add(1024))
            })
            .ok_or_else(|| invalid("group output admission size overflow"))?
            .checked_add((items.len() + 1).saturating_mul(std::mem::size_of::<QueryMemoryLease>()))
            .ok_or_else(|| invalid("group output owner size overflow"))?;
        if !rows.is_empty() && !output_account.can_reserve(output_bytes) {
            let control = emit_checked(std::mem::take(&mut rows), context.task_context, emit)?;
            owners.clear();
            output_memory.shrink(output_memory.bytes());
            if control == BatchControl::Stop {
                return Ok(control);
            }
        }
        output_memory.grow(output_bytes)?;
        let mut retained = Vec::with_capacity(items.len() + 1);
        retained.push(group.memory);
        let mut values = BTreeMap::new();
        for (projection, value) in group_keys.iter().zip(key) {
            insert_projected_value(&mut values, &projection.name, value);
        }
        for (item, state) in items.iter().zip(group.states) {
            let value = match state {
                State::Maximum(value) => value.unwrap_or(Value::Null),
                State::Evidence(evidence) => {
                    let (value, memory) = evidence.finish()?;
                    retained.push(memory);
                    value
                }
            };
            insert_projected_value(&mut values, &item.name, value);
        }
        rows.push(Binding {
            values,
            nodes: BTreeMap::new().into(),
            relationships: BTreeMap::new().into(),
        });
        owners.push(retained);
        emitted = emitted.saturating_add(1);
        if rows.len() == context.memory.batch_rows.get() {
            let control = emit_checked(std::mem::take(&mut rows), context.task_context, emit)?;
            owners.clear();
            output_memory.shrink(output_memory.bytes());
            if control == BatchControl::Stop {
                return Ok(control);
            }
        }
    }
    runtime_checkpoint(context.task_context)?;
    if rows.is_empty() {
        Ok(BatchControl::Continue)
    } else {
        emit_checked(rows, context.task_context, emit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_group_handoff_keeps_the_callers_key_admission() {
        let budget = std::num::NonZeroUsize::new(64 * 1024).unwrap();
        let ledger = crate::QueryMemoryLedger::new(budget);
        let keys = ledger.account(QueryMemoryClass::BlockingState, "prepared key", budget);
        let state_bytes = GROUP_ENTRY_BYTES + std::mem::size_of::<State>();
        let groups = ledger.account(
            QueryMemoryClass::BlockingState,
            "group state",
            std::num::NonZeroUsize::new(state_bytes).unwrap(),
        );
        let mut allocation = keys.reserve(4096).unwrap();
        let key = Value::String("x".repeat(4096));
        let items = [Aggregation {
            function: AggregateFunction::Max,
            target: AggregateTarget::Column("score".into()),
            distinct: false,
            name: "best".into(),
        }];
        assert!(matches!(
            new_group(&mut allocation, &items, &groups),
            Err(HawDBError::ResourceBudgetExceeded(_))
        ));
        assert_eq!(allocation.bytes(), 4096);
        assert_eq!(ledger.snapshot().used_bytes, 4096);
        drop(key);
        drop(allocation);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    fn oracle(rows: &[(String, f64)], limit: usize) -> Vec<Value> {
        let mut maxima = BTreeMap::<String, f64>::new();
        for (id, score) in rows {
            maxima
                .entry(id.clone())
                .and_modify(|old| {
                    if score.total_cmp(old).is_gt() {
                        *old = *score;
                    }
                })
                .or_insert(*score);
        }
        let mut ranked = maxima.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|(left_id, left), (right_id, right)| {
            right.total_cmp(left).then_with(|| left_id.cmp(right_id))
        });
        ranked.truncate(limit);
        ranked
            .into_iter()
            .map(|(id, _)| Value::String(id))
            .collect()
    }

    #[test]
    fn ranked_evidence_matches_exhaustive_deduplication_under_permutations() {
        let bytes = NonZeroUsize::new(1 << 20).unwrap();
        let mut seed = 1119u64;
        for case in 0..240 {
            let limit = 1 + case % 17;
            let mut rows = Vec::new();
            for _ in 0..180 {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                rows.push((
                    format!("doc-{:03}", (seed >> 21) % 41),
                    ((seed >> 37) % 19) as f64 - 9.0,
                ));
            }
            rows.extend([
                ("negative-zero".into(), -0.0),
                ("positive-zero".into(), 0.0),
            ]);
            let expected = oracle(&rows, limit);
            for reverse in [false, true] {
                let ledger = QueryMemoryLedger::new(bytes);
                let account =
                    ledger.account(QueryMemoryClass::BlockingState, "ranked oracle", bytes);
                let mut summary = Evidence::new(limit, &account).unwrap();
                if reverse {
                    rows.reverse();
                }
                for (id, score) in &rows {
                    summary.push(id, *score).unwrap();
                }
                assert!(summary.by_id.len() <= limit);
                assert_eq!(
                    summary.by_score.values().map(BTreeSet::len).sum::<usize>(),
                    summary.by_id.len()
                );
                let (actual, memory) = summary.finish().unwrap();
                assert_eq!(
                    actual,
                    Value::List(expected.clone()),
                    "case={case} reverse={reverse}"
                );
                drop(actual);
                drop(memory);
                assert_eq!(ledger.snapshot().used_bytes, 0);
            }
        }
    }

    #[test]
    fn ranked_evidence_replacement_is_admitted_before_mutation() {
        let cost = Evidence::entry_bytes("a").unwrap();
        let bytes = NonZeroUsize::new(2 * cost - 1).unwrap();
        let ledger = QueryMemoryLedger::new(bytes);
        let account = ledger.account(QueryMemoryClass::BlockingState, "ranked refusal", bytes);
        let mut summary = Evidence::new(1, &account).unwrap();
        summary.push("a", 1.0).unwrap();
        let before = ledger.snapshot().used_bytes;
        assert!(matches!(
            summary.push("b", 2.0),
            Err(HawDBError::ResourceBudgetExceeded(_))
        ));
        assert_eq!(ledger.snapshot().used_bytes, before);
        assert_eq!(
            summary.by_id.keys().map(String::as_str).collect::<Vec<_>>(),
            ["a"]
        );
        let (value, memory) = summary.finish().unwrap();
        assert_eq!(value, Value::List(vec![Value::String("a".into())]));
        drop(value);
        drop(memory);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn ranked_evidence_large_limit_is_lazy_and_rejects_nonfinite_scores() {
        let bytes = NonZeroUsize::new(16 << 10).unwrap();
        let ledger = QueryMemoryLedger::new(bytes);
        let account = ledger.account(QueryMemoryClass::BlockingState, "lazy evidence", bytes);
        let mut summary = Evidence::new(usize::MAX, &account).unwrap();
        assert_eq!(ledger.snapshot().used_bytes, 0);
        for score in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(summary.push("invalid", score).is_err());
            assert!(summary.by_id.is_empty());
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
        summary.push("a", -0.0).unwrap();
        summary.push("a", 0.0).unwrap();
        assert_eq!(summary.by_id["a"].0.to_bits(), 0.0f64.to_bits());
        assert_eq!(summary.by_id.len(), 1);
    }
}
