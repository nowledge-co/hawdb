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

use super::{schema_value_mismatch, NumericFragment};
use crate::columnar::{ValidityBuilder, ValidityView};
use hawdb_core::Result;
use hawdb_core::Value;
use hawdb_storage::NodeRecord;

pub(super) fn admitted_numeric_batch_rows(
    configured_rows: usize,
    memory_budget_bytes: usize,
    needs_node_ids: bool,
    needs_validity: bool,
) -> Option<usize> {
    let value_bytes = std::mem::size_of::<f64>();
    let node_id_bytes = usize::from(needs_node_ids) * std::mem::size_of::<u64>();
    let selection_bytes = std::mem::size_of::<u32>();
    let bytes_per_row = value_bytes
        .saturating_add(node_id_bytes)
        .saturating_add(selection_bytes);
    let mut lower = 0usize;
    let mut upper = configured_rows;
    while lower < upper {
        let rows = lower + (upper - lower).div_ceil(2);
        let required_bytes = rows.saturating_mul(bytes_per_row).saturating_add(
            usize::from(needs_validity)
                .saturating_mul(rows.div_ceil(u64::BITS as usize))
                .saturating_mul(std::mem::size_of::<u64>()),
        );
        if required_bytes <= memory_budget_bytes {
            lower = rows;
        } else {
            upper = rows.saturating_sub(1);
        }
    }
    (lower > 0).then_some(lower)
}

/// A cursor whose batch view is valid only until the next mutable cursor step.
///
/// Keeping this internal preserves the object-safe row executor facade while
/// allowing statically dispatched producers to reuse their backing buffers.
pub(super) trait LendingBatchCursor {
    type Batch<'batch>
    where
        Self: 'batch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch<'_>>>;
}

#[derive(Debug, Clone, Copy)]
pub(super) enum NumericBatchValues<'batch> {
    Int(&'batch [i64]),
    Float(&'batch [f64]),
}

impl NumericBatchValues<'_> {
    #[cfg(test)]
    pub(super) fn len(self) -> usize {
        match self {
            Self::Int(values) => values.len(),
            Self::Float(values) => values.len(),
        }
    }

    pub(super) fn value(self, row: usize) -> Value {
        match self {
            Self::Int(values) => Value::Int(values[row]),
            Self::Float(values) => Value::Float(values[row]),
        }
    }
}

enum NumericValueBuffer {
    Int(Vec<i64>),
    Float(Vec<f64>),
}

impl NumericValueBuffer {
    fn with_capacity(property_type: hawdb_core::PropertyType, rows: usize) -> Self {
        match property_type {
            hawdb_core::PropertyType::Int => Self::Int(Vec::with_capacity(rows)),
            hawdb_core::PropertyType::Float => Self::Float(Vec::with_capacity(rows)),
            _ => unreachable!("numeric fragment eligibility checks the property type"),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Int(values) => values.clear(),
            Self::Float(values) => values.clear(),
        }
    }

    fn view(&self) -> NumericBatchValues<'_> {
        match self {
            Self::Int(values) => NumericBatchValues::Int(values),
            Self::Float(values) => NumericBatchValues::Float(values),
        }
    }

    fn push_node_value(
        &mut self,
        fragment: NumericFragment<'_>,
        node: &NodeRecord,
    ) -> Result<bool> {
        match (self, node.properties.get(fragment.property)) {
            (Self::Int(values), Some(Value::Int(value))) => {
                values.push(*value);
                Ok(true)
            }
            (Self::Float(values), Some(Value::Float(value))) => {
                values.push(*value);
                Ok(true)
            }
            (Self::Int(values), Some(Value::Null) | None) => {
                values.push(0);
                Ok(false)
            }
            (Self::Float(values), Some(Value::Null) | None) => {
                values.push(0.0);
                Ok(false)
            }
            (_, Some(value)) => Err(schema_value_mismatch(fragment, value)),
        }
    }
}

pub(super) struct NumericNodeBatch<'batch> {
    pub(super) input_rows: usize,
    pub(super) node_ids: Option<&'batch [u64]>,
    pub(super) values: NumericBatchValues<'batch>,
    pub(super) validity: ValidityView<'batch>,
}

pub(super) struct OwnedNumericBatchBuffer<'plan> {
    fragment: NumericFragment<'plan>,
    batch_rows: usize,
    input_rows: usize,
    node_ids: Option<Vec<u64>>,
    values: NumericValueBuffer,
    validity: ValidityBuilder,
}

impl<'plan> OwnedNumericBatchBuffer<'plan> {
    pub(super) fn new(
        fragment: NumericFragment<'plan>,
        batch_rows: usize,
        needs_node_ids: bool,
    ) -> Self {
        Self {
            fragment,
            batch_rows,
            input_rows: 0,
            node_ids: needs_node_ids.then(|| Vec::with_capacity(batch_rows)),
            values: NumericValueBuffer::with_capacity(fragment.property_type, batch_rows),
            validity: ValidityBuilder::with_capacity(batch_rows),
        }
    }

    pub(super) fn push_owned(&mut self, node: NodeRecord) -> Result<()> {
        self.input_rows = self.input_rows.saturating_add(1);
        if let Some(node_ids) = &mut self.node_ids {
            node_ids.push(node.id.0);
        }
        let valid = self.values.push_node_value(self.fragment, &node)?;
        self.validity.push(valid);
        Ok(())
    }

    pub(super) fn is_full(&self) -> bool {
        self.input_rows == self.batch_rows
    }

    pub(super) fn is_empty(&self) -> bool {
        self.input_rows == 0
    }

    pub(super) fn take_batch(&self) -> NumericNodeBatch<'_> {
        NumericNodeBatch {
            input_rows: self.input_rows,
            node_ids: self.node_ids.as_deref(),
            values: self.values.view(),
            validity: self.validity.view(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.input_rows = 0;
        if let Some(node_ids) = &mut self.node_ids {
            node_ids.clear();
        }
        self.values.clear();
        self.validity.clear();
    }
}

pub(super) struct NumericNodeBatchCursor<'store, 'plan, I>
where
    I: Iterator<Item = &'store NodeRecord>,
{
    nodes: I,
    fragment: NumericFragment<'plan>,
    batch_rows: usize,
    node_ids: Option<Vec<u64>>,
    values: NumericValueBuffer,
    validity: ValidityBuilder,
}

impl<'store, 'plan, I> NumericNodeBatchCursor<'store, 'plan, I>
where
    I: Iterator<Item = &'store NodeRecord>,
{
    pub(super) fn new(
        nodes: I,
        fragment: NumericFragment<'plan>,
        batch_rows: usize,
        needs_node_ids: bool,
    ) -> Self {
        Self {
            nodes,
            fragment,
            batch_rows,
            node_ids: needs_node_ids.then(|| Vec::with_capacity(batch_rows)),
            values: NumericValueBuffer::with_capacity(fragment.property_type, batch_rows),
            validity: ValidityBuilder::with_capacity(batch_rows),
        }
    }

    fn push_node_value(&mut self, node: &NodeRecord) -> Result<()> {
        if let Some(node_ids) = &mut self.node_ids {
            node_ids.push(node.id.0);
        }
        let valid = self.values.push_node_value(self.fragment, node)?;
        self.validity.push(valid);
        Ok(())
    }
}

impl<'store, 'plan, I> LendingBatchCursor for NumericNodeBatchCursor<'store, 'plan, I>
where
    I: Iterator<Item = &'store NodeRecord>,
{
    type Batch<'batch>
        = NumericNodeBatch<'batch>
    where
        Self: 'batch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch<'_>>> {
        if let Some(node_ids) = &mut self.node_ids {
            node_ids.clear();
        }
        self.values.clear();
        self.validity.clear();
        let mut input_rows = 0usize;
        for _ in 0..self.batch_rows {
            let Some(node) = self.nodes.next() else {
                break;
            };
            input_rows = input_rows.saturating_add(1);
            self.push_node_value(node)?;
        }
        if input_rows == 0 {
            return Ok(None);
        }
        Ok(Some(NumericNodeBatch {
            input_rows,
            node_ids: self.node_ids.as_deref(),
            values: self.values.view(),
            validity: self.validity.view(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::NumericPredicate;
    use super::*;
    use hawdb_storage::NodeId;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn numeric_batch_admission_accounts_for_required_slots() {
        assert_eq!(
            admitted_numeric_batch_rows(128, 120, false, false),
            Some(10)
        );
        assert_eq!(admitted_numeric_batch_rows(128, 120, true, false), Some(6));
        assert_eq!(admitted_numeric_batch_rows(128, 11, false, false), None);
        assert_eq!(admitted_numeric_batch_rows(128, 120, false, true), Some(9));
    }

    #[test]
    fn lending_numeric_cursor_reuses_value_storage() {
        let nodes = (0..5)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::new(),
                properties: BTreeMap::from([("score".to_string(), Value::Int(id as i64))]),
            })
            .collect::<Vec<_>>();
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: hawdb_core::PropertyType::Int,
            predicate: NumericPredicate::Compare(hawdb_plan::ComparisonOp::Gte),
            expected: crate::columnar::NumericLiteral::Int(0),
            fused_operators: None,
        };
        let mut cursor = NumericNodeBatchCursor::new(nodes.iter(), fragment, 2, true);

        let first_values_ptr = {
            let first = cursor.next_batch().unwrap().unwrap();
            let first_values = match first.values {
                NumericBatchValues::Int(values) => values,
                NumericBatchValues::Float(_) => panic!("expected integer batch"),
            };
            assert_eq!(first.node_ids, Some(&[0, 1][..]));
            assert_eq!(first_values, &[0, 1]);
            first_values.as_ptr()
        };
        {
            let second = cursor.next_batch().unwrap().unwrap();
            let second_values = match second.values {
                NumericBatchValues::Int(values) => values,
                NumericBatchValues::Float(_) => panic!("expected integer batch"),
            };
            assert_eq!(second.node_ids, Some(&[2, 3][..]));
            assert_eq!(second_values, &[2, 3]);
            assert_eq!(second_values.as_ptr(), first_values_ptr);
        }
        {
            let final_batch = cursor.next_batch().unwrap().unwrap();
            assert_eq!(final_batch.node_ids, Some(&[4][..]));
        }
        assert!(cursor.next_batch().unwrap().is_none());
    }

    #[test]
    fn lending_numeric_cursor_tracks_nulls_without_retaining_records() {
        let nodes = [
            NodeRecord {
                id: NodeId(1),
                labels: BTreeSet::new(),
                properties: BTreeMap::new(),
            },
            NodeRecord {
                id: NodeId(2),
                labels: BTreeSet::new(),
                properties: BTreeMap::from([("score".to_string(), Value::Float(2.5))]),
            },
        ];
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: hawdb_core::PropertyType::Float,
            predicate: NumericPredicate::Compare(hawdb_plan::ComparisonOp::Gt),
            expected: crate::columnar::NumericLiteral::Float(1.0),
            fused_operators: None,
        };
        let mut cursor = NumericNodeBatchCursor::new(nodes.iter(), fragment, 8, false);

        let batch = cursor.next_batch().unwrap().unwrap();
        assert_eq!(batch.node_ids, None);
        assert_eq!(batch.input_rows, 2);
        assert_eq!(batch.values.len(), 2);
        assert_eq!(batch.values.value(1), Value::Float(2.5));
        assert_eq!(batch.validity.valid_count(), 1);
        assert!(!batch.validity.is_valid(0));
    }

    #[test]
    fn owned_numeric_buffer_retains_only_typed_columns() {
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: hawdb_core::PropertyType::Int,
            predicate: NumericPredicate::Compare(hawdb_plan::ComparisonOp::Gte),
            expected: crate::columnar::NumericLiteral::Int(2),
            fused_operators: None,
        };
        let mut buffer = OwnedNumericBatchBuffer::new(fragment, 3, true);
        for id in 0..3 {
            let score = if id == 1 {
                Value::Null
            } else {
                Value::Int(id as i64)
            };
            buffer
                .push_owned(NodeRecord {
                    id: NodeId(id),
                    labels: BTreeSet::new(),
                    properties: BTreeMap::from([
                        ("score".to_string(), score),
                        ("payload".to_string(), Value::String("x".repeat(4096))),
                    ]),
                })
                .unwrap();
        }

        assert!(buffer.is_full());
        let batch = buffer.take_batch();
        assert_eq!(batch.input_rows, 3);
        assert_eq!(batch.node_ids, Some(&[0, 1, 2][..]));
        assert_eq!(batch.values.len(), 3);
        assert_eq!(batch.values.value(2), Value::Int(2));
        assert_eq!(batch.validity.valid_count(), 2);
        assert!(!batch.validity.is_valid(1));
        buffer.clear();
        assert!(buffer.is_empty());
    }
}
