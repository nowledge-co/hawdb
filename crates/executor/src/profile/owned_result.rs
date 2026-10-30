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

//! Capacity admission and ownership transfer for compact owned query results.

use super::{
    QueryRowStorage, QueryRowsBuilder, QuerySchema, Row, RowRef, OWNED_RESULT_SEGMENT_ROWS,
};
use crate::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::{HawDBError, Result, Value, ValueRef};
use std::sync::Arc;

/// Source ordinals mapped to the bound schema, charged only during collection.
pub(super) struct InputOrder {
    ordinals: Vec<usize>,
    _memory: QueryMemoryLease,
}

impl QueryRowsBuilder {
    pub(crate) fn owner_memory_bytes() -> usize {
        // Two Arc reference-count pairs cover the result storage and schema columns.
        std::mem::size_of::<QueryRowStorage>() + 4 * std::mem::size_of::<usize>()
    }

    pub(crate) fn push_budgeted_named_row(
        &mut self,
        row: Row,
        account: &QueryMemoryAccount,
        retained: &mut QueryMemoryLease,
    ) -> Result<()> {
        let heap_bytes = row.values().try_fold(0usize, |total, value| {
            checked_result_add(total, retained_value_heap_bytes(value)?)
        })?;
        // Keep the incoming tree, keys and payload charged until ownership
        // transfer destroys the tree. Payload accounting is shared with the
        // retained result instead of traversing it again as a logical estimate.
        let key_bytes = row.keys().try_fold(0usize, |total, name| {
            checked_result_add(total, name.capacity())
        })?;
        let transient = account.reserve(checked_result_add(
            checked_result_add(
                std::mem::size_of::<Row>(),
                retained_map_node_bound(row.len())?,
            )?,
            checked_result_add(heap_bytes, key_bytes)?,
        )?)?;
        let (next_count, reorder) = self.prepare_budgeted_row(
            row.keys().map(String::as_str),
            row.len(),
            heap_bytes,
            account,
            retained,
        )?;
        self.append_budgeted_values(row.into_values(), reorder);
        self.row_count = next_count;
        drop(transient);
        Ok(())
    }

    pub(crate) fn push_budgeted_borrowed_row(
        &mut self,
        row: RowRef<'_>,
        account: &QueryMemoryAccount,
        retained: &mut QueryMemoryLease,
    ) -> Result<()> {
        let heap_bytes = row.iter().try_fold(0usize, |total, (_, value)| {
            checked_result_add(total, cloned_value_heap_bytes(value)?)
        })?;
        let (next_count, reorder) = self.prepare_budgeted_row(
            row.iter().map(|(name, _)| name),
            row.len(),
            heap_bytes,
            account,
            retained,
        )?;
        // Source buffers stay producer-owned. Only values that outlive this
        // call are copied, after their complete retained footprint is admitted.
        self.append_budgeted_values(row.iter().map(|(_, value)| value.to_owned_value()), reorder);
        self.row_count = next_count;
        Ok(())
    }

    pub(crate) fn bind_budgeted_schema<'name>(
        &mut self,
        names: impl Iterator<Item = &'name str> + Clone,
        account: &QueryMemoryAccount,
        retained: &mut QueryMemoryLease,
    ) -> Result<()> {
        if self.schema.is_some() {
            return Ok(());
        }
        // Validate before allocating, including when execution produces no rows.
        for (index, name) in names.clone().enumerate() {
            if names.clone().take(index).any(|previous| previous == name) {
                return Err(HawDBError::Semantic(format!(
                    "query result schema contains duplicate column {name}"
                )));
            }
        }
        let slots = checked_result_mul(names.clone().count(), std::mem::size_of::<String>())?;
        let name_bytes = names
            .clone()
            .try_fold(0usize, |total, name| checked_result_add(total, name.len()))?;
        // Admit both the retained Arc and the overlapping conversion buffer.
        let staging = account.reserve(slots)?;
        retained.grow(checked_result_add(slots, name_bytes)?)?;
        self.schema = Some(QuerySchema {
            columns: Arc::from(names.map(str::to_owned).collect::<Vec<_>>()),
        });
        drop(staging);
        Ok(())
    }

    fn prepare_input_order<'row>(
        &mut self,
        names: impl Iterator<Item = &'row str> + Clone,
        width: usize,
        account: &QueryMemoryAccount,
    ) -> Result<bool> {
        let schema = self.schema.as_ref().expect("result schema is bound");
        let mismatch =
            || HawDBError::Execution("query result row does not match its bound schema".into());
        if width != schema.len() {
            return Err(mismatch());
        }
        if names
            .clone()
            .eq(schema.columns().iter().map(String::as_str))
        {
            return Ok(false);
        }
        if self.input_order.as_ref().is_some_and(|order| {
            names.clone().eq(order
                .ordinals
                .iter()
                .map(|&ordinal| schema.columns()[ordinal].as_str()))
        }) {
            return Ok(true);
        }
        // Only a new input order needs a full permutation check. Do not mutate
        // the cache until validation succeeds, including duplicate-name checks.
        if schema
            .columns()
            .iter()
            .any(|name| names.clone().filter(|candidate| *candidate == name).count() != 1)
        {
            return Err(mismatch());
        }
        if self.input_order.is_none() {
            let mut memory = account.reserve(0)?;
            let mut ordinals = Vec::new();
            reserve_result_capacity(&mut ordinals, width, &mut memory)?;
            self.input_order = Some(InputOrder {
                ordinals,
                _memory: memory,
            });
        }
        let order = self.input_order.as_mut().expect("input order is admitted");
        order.ordinals.clear();
        order.ordinals.extend(names.map(|name| {
            schema
                .columns()
                .iter()
                .position(|column| column == name)
                .expect("input columns were validated")
        }));
        Ok(true)
    }

    fn append_budgeted_values(&mut self, values: impl Iterator<Item = Value>, reorder: bool) {
        if !reorder {
            self.values.extend(values);
            return;
        }
        let order = self.input_order.as_ref().expect("input order is admitted");
        let start = self.values.len();
        // prepare_budgeted_row has already admitted and reserved every slot.
        self.values
            .resize(start + order.ordinals.len(), Value::Null);
        for (&ordinal, value) in order.ordinals.iter().zip(values) {
            self.values[start + ordinal] = value;
        }
    }

    fn prepare_budgeted_row<'row>(
        &mut self,
        names: impl Iterator<Item = &'row str> + Clone,
        width: usize,
        heap_bytes: usize,
        account: &QueryMemoryAccount,
        retained: &mut QueryMemoryLease,
    ) -> Result<(usize, bool)> {
        self.bind_budgeted_schema(names.clone(), account, retained)?;
        let reorder = self.prepare_input_order(names, width, account)?;
        let next_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(result_capacity_overflow)?;
        let rows_in_segment = self.row_count % OWNED_RESULT_SEGMENT_ROWS;
        let starts_segment = width != 0 && rows_in_segment == 0 && self.row_count != 0;
        let mut next_segment = Vec::new();
        if width != 0 {
            let segments = self
                .segments
                .as_mut()
                .expect("budgeted results use segmented storage");
            let needed = checked_result_add(self.row_count / OWNED_RESULT_SEGMENT_ROWS, 1)?;
            // A single active segment needs no directory in the final result.
            if (starts_segment || !segments.is_empty()) && needed > segments.capacity() {
                let target = needed
                    .checked_next_power_of_two()
                    .ok_or_else(result_capacity_overflow)?;
                reserve_result_capacity(segments, target, retained)?;
            }
            if starts_segment {
                reserve_result_capacity(&mut next_segment, width, retained)?;
            } else {
                let needed_values = checked_result_mul(rows_in_segment + 1, width)?;
                if needed_values > self.values.capacity() {
                    let target =
                        checked_result_mul((rows_in_segment + 1).next_power_of_two(), width)?;
                    reserve_result_capacity(&mut self.values, target, retained)?;
                }
            }
        }
        if let Err(error) = retained.grow(heap_bytes) {
            let next_bytes =
                checked_result_mul(next_segment.capacity(), std::mem::size_of::<Value>())?;
            drop(next_segment);
            retained.shrink(next_bytes);
            return Err(error);
        }
        if starts_segment {
            self.segments
                .as_mut()
                .expect("budgeted results use segmented storage")
                .push(std::mem::replace(&mut self.values, next_segment));
        }
        Ok((next_count, reorder))
    }
}

fn result_capacity_overflow() -> HawDBError {
    HawDBError::Execution("query result retained capacity overflow".into())
}

fn checked_result_add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).ok_or_else(result_capacity_overflow)
}

fn checked_result_mul(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right).ok_or_else(result_capacity_overflow)
}

fn retained_map_node_bound(entries: usize) -> Result<usize> {
    // The pinned std B-tree has 11 slots, 12 child pointers, and at least
    // five entries per non-root node. Include an allocated empty root and
    // bound every node as internal. See OWNED_RESULT_MEMORY_ACCOUNTING.md;
    // toolchain upgrades must requalify this implementation-dependent bound.
    let nodes = checked_result_add(1, entries.saturating_sub(1) / 5)?;
    let slots = checked_result_mul(11, std::mem::size_of::<(String, Value)>())?;
    let pointers = checked_result_mul(14, std::mem::size_of::<usize>())?;
    checked_result_mul(nodes, checked_result_add(slots, pointers)?)
}

fn retained_value_heap_bytes(value: &Value) -> Result<usize> {
    match value {
        Value::String(value) => Ok(value.capacity()),
        Value::Binary(value) => Ok(value.capacity()),
        Value::Vector(value) => Ok(value.retained_capacity()),
        Value::List(values) => values.iter().try_fold(
            checked_result_mul(values.capacity(), std::mem::size_of::<Value>())?,
            |total, value| checked_result_add(total, retained_value_heap_bytes(value)?),
        ),
        Value::Map(values) => values.iter().try_fold(
            retained_map_node_bound(values.len())?,
            |total, (name, value)| {
                checked_result_add(
                    total,
                    checked_result_add(name.capacity(), retained_value_heap_bytes(value)?)?,
                )
            },
        ),
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::Uuid(_) => Ok(0),
    }
}

/// Footprint of the owned copy, independent of spare capacity in its source.
/// The pinned Vec/String clone implementations allocate their logical length;
/// nested maps use the same conservative native-node bound as moved results.
fn cloned_value_heap_bytes(value: ValueRef<'_>) -> Result<usize> {
    match value {
        ValueRef::String(value) => Ok(value.len()),
        ValueRef::Binary(value) => Ok(value.len()),
        ValueRef::Vector(value) => Ok(value.encoded().len()),
        ValueRef::List(values) => values.iter().try_fold(
            checked_result_mul(values.len(), std::mem::size_of::<Value>())?,
            |total, value| checked_result_add(total, cloned_value_heap_bytes(value.as_ref())?),
        ),
        ValueRef::Map(values) => values.iter().try_fold(
            retained_map_node_bound(values.len())?,
            |total, (name, value)| {
                checked_result_add(
                    total,
                    checked_result_add(name.len(), cloned_value_heap_bytes(value.as_ref())?)?,
                )
            },
        ),
        ValueRef::Null
        | ValueRef::Bool(_)
        | ValueRef::Int(_)
        | ValueRef::Float(_)
        | ValueRef::Uuid(_) => Ok(0),
    }
}

pub(crate) fn cloned_row_memory_bytes(row: RowRef<'_>) -> Result<usize> {
    row.iter().try_fold(
        checked_result_add(
            std::mem::size_of::<Row>(),
            retained_map_node_bound(row.len())?,
        )?,
        |total, (name, value)| {
            checked_result_add(
                total,
                checked_result_add(name.len(), cloned_value_heap_bytes(value)?)?,
            )
        },
    )
}

fn reserve_result_capacity<T>(
    values: &mut Vec<T>,
    target: usize,
    retained: &mut QueryMemoryLease,
) -> Result<()> {
    let old_bytes = checked_result_mul(values.capacity(), std::mem::size_of::<T>())?;
    let new_bytes = checked_result_mul(target, std::mem::size_of::<T>())?;
    // Admit the full replacement while the old buffer is still retained.
    // This explicitly accounts for growth overlap rather than relying on realloc.
    retained.grow(new_bytes)?;
    let mut replacement = Vec::new();
    if let Err(error) = replacement.try_reserve_exact(target) {
        retained.shrink(new_bytes);
        return Err(HawDBError::Execution(format!(
            "query result allocation failed: {error}"
        )));
    }
    let actual_bytes = checked_result_mul(replacement.capacity(), std::mem::size_of::<T>())?;
    if actual_bytes > new_bytes {
        if let Err(error) = retained.grow(actual_bytes - new_bytes) {
            drop(replacement);
            retained.shrink(new_bytes);
            return Err(error);
        }
    } else {
        retained.shrink(new_bytes - actual_bytes);
    }
    let old = std::mem::replace(values, replacement);
    values.extend(old);
    retained.shrink(old_bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryMemoryClass, QueryMemoryLedger};
    use std::cell::Cell;
    use std::num::NonZeroUsize;

    #[test]
    fn repeated_input_order_is_linear_and_its_cache_is_transient() {
        let budget = NonZeroUsize::new(1024 * 1024).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::ResultMaterialization, "result", budget);
        let columns: Vec<_> = (0..64).map(|index| format!("column{index:02}")).collect();
        let mut builder = QueryRowsBuilder::new();
        let mut retained = account.reserve(0).unwrap();
        builder
            .bind_budgeted_schema(columns.iter().map(String::as_str), &account, &mut retained)
            .unwrap();
        let before = ledger.snapshot().used_bytes;
        let names = || columns.iter().rev().map(String::as_str);
        assert!(builder
            .prepare_input_order(names(), columns.len(), &account)
            .unwrap());
        let cache_bytes = builder.input_order.as_ref().unwrap().ordinals.capacity()
            * std::mem::size_of::<usize>();
        assert_eq!(ledger.snapshot().used_bytes, before + cache_bytes);
        let visits = Cell::new(0);
        for _ in 0..4 {
            visits.set(0);
            let names = names().inspect(|_| visits.set(visits.get() + 1));
            assert!(builder
                .prepare_input_order(names, columns.len(), &account)
                .unwrap());
            assert!(
                visits.get() <= 2 * columns.len(),
                "visited {} names",
                visits.get()
            );
        }
        let wrong = std::iter::repeat_n(columns[0].as_str(), columns.len());
        assert!(builder
            .prepare_input_order(wrong, columns.len(), &account)
            .is_err());
        assert!(builder
            .prepare_input_order(names(), columns.len(), &account)
            .unwrap());
        let rows = builder.finish();
        assert_eq!(ledger.snapshot().used_bytes, before);
        assert_eq!(rows.schema().columns(), columns);
        drop(retained);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn input_order_cache_is_admitted_before_allocation() {
        let budget = NonZeroUsize::new(1).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::ResultMaterialization, "result", budget);
        let mut builder = QueryRowsBuilder::with_schema(
            QuerySchema::try_new(["z".into(), "a".into()]).unwrap(),
            0,
        );
        assert!(matches!(
            builder.prepare_input_order(["a", "z"].into_iter(), 2, &account),
            Err(HawDBError::ResourceBudgetExceeded(_)),
        ));
        assert!(builder.input_order.is_none());
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
