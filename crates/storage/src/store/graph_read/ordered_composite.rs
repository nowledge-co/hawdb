// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::property_projection::CompositeOrder;
use hawdb_core::access::CompositeOrderedSeek;
use hawdb_core::ids::projected_value_allocation_bytes;
use std::cmp::Ordering as KeyOrdering;

#[cfg(test)]
#[path = "ordered_composite/tests.rs"]
mod tests;

struct Directed<I> {
    input: I,
    descending: bool,
}
impl<I: DoubleEndedIterator> Iterator for Directed<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        if self.descending {
            self.input.next_back()
        } else {
            self.input.next()
        }
    }
}

fn key_matches(key: &[(String, Value)], seek: &CompositeOrderedSeek) -> bool {
    key.len() == seek.index_properties.len()
        && key.iter().all(|(_, value)| !matches!(value, Value::Null))
        && key
            .iter()
            .zip(&seek.index_properties)
            .all(|((name, _), expected)| name == expected)
        && key
            .iter()
            .zip(&seek.equality_prefix)
            .all(|((_, value), (_, expected))| value == expected)
        && seek.after.as_ref().is_none_or(|after| {
            let order = key[seek.equality_prefix.len()..]
                .iter()
                .map(|(_, value)| value)
                .cmp(after.iter());
            if seek.descending {
                order.is_lt()
            } else {
                order.is_gt()
            }
        })
}

fn key_bytes(seek: &CompositeOrderedSeek) -> usize {
    let names = seek
        .index_properties
        .iter()
        .fold(0usize, |total, name| total.saturating_add(name.len()));
    let prefix = seek
        .equality_prefix
        .iter()
        .fold(0usize, |total, (_, value)| {
            total.saturating_add(projected_value_allocation_bytes(value))
        });
    let after = seek.after.iter().flatten().fold(0usize, |total, value| {
        total.saturating_add(projected_value_allocation_bytes(value))
    });
    seek.index_properties
        .len()
        .saturating_mul(2 * std::mem::size_of::<(String, Value)>())
        .saturating_add(names.saturating_mul(2))
        .saturating_add(1)
        .saturating_add(prefix.saturating_mul(2))
        .saturating_add(after)
}

type CompositeBound = (LabelId, Vec<(String, Value)>);

fn seek_bounds(label: LabelId, seek: &CompositeOrderedSeek) -> (CompositeBound, CompositeBound) {
    let prefix = &seek.equality_prefix;
    let mut first = Vec::with_capacity(seek.index_properties.len());
    first.extend(prefix.iter().cloned());
    let mut last = Vec::with_capacity(seek.index_properties.len());
    last.extend(prefix.iter().cloned());
    let property = &seek.index_properties[prefix.len()];
    first.push((property.clone(), Value::Null));
    // The property name sorts before its own NUL extension, irrespective of value.
    let mut successor = String::with_capacity(property.len() + 1);
    successor.push_str(property);
    successor.push('\0');
    last.push((successor, Value::Null));
    if let Some(after) = &seek.after {
        let target = if seek.descending {
            &mut last
        } else {
            &mut first
        };
        target.truncate(prefix.len());
        target.extend(
            seek.index_properties[prefix.len()..]
                .iter()
                .cloned()
                .zip(after.iter().cloned()),
        );
    }
    ((label, first), (label, last))
}

fn compare_live_key(
    key: &[(String, Value)],
    id: NodeId,
    canonical: &Value,
    canonical_id: NodeId,
) -> std::result::Result<KeyOrdering, PersistentPropertyProjectionError> {
    let Value::List(values) = canonical else {
        return Err(PersistentPropertyProjectionError::Corrupt(
            "ordered composite projection did not return a tuple".into(),
        ));
    };
    Ok(key
        .iter()
        .map(|(_, value)| value)
        .cmp(values.iter())
        .then_with(|| id.cmp(&canonical_id)))
}

fn validate_node<'a>(
    node: &ProjectedNodeRecord,
    label: LabelId,
    seek: &CompositeOrderedSeek,
    mut values: impl Iterator<Item = &'a Value>,
) -> std::result::Result<(), PersistentPropertyProjectionError> {
    let valid = node.labels.contains(&label)
        && seek.index_properties.iter().all(|property| {
            values
                .next()
                .is_some_and(|value| node.properties.get(property) == Some(value))
        })
        && values.next().is_none();
    if !valid {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "ordered composite key disagrees with canonical node {}",
            node.id.0
        )));
    }
    Ok(())
}

impl GraphStore {
    pub(super) fn visit_projected_nodes_by_composite_order_with_admission(
        &self,
        label: LabelId,
        seek: &CompositeOrderedSeek,
        properties: Option<&BTreeSet<String>>,
        admission: &ProjectionAdmission<'_>,
        mut consumer: impl FnMut(AdmittedProjection) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let prefix = seek.equality_prefix.len();
        if prefix == 0
            || prefix >= seek.index_properties.len()
            || seek
                .equality_prefix
                .iter()
                .zip(&seek.index_properties)
                .any(|((name, _), expected)| name != expected)
            || seek
                .after
                .as_ref()
                .is_some_and(|after| after.len() != seek.index_properties.len() - prefix)
            || properties.is_some_and(|properties| {
                seek.index_properties
                    .iter()
                    .any(|key| !properties.contains(key))
            })
        {
            return Err(HawDBError::Execution("ordered composite access requires a complete key projection and valid prefix/continuation".into()));
        }
        if admission.has_work_limit() {
            return Err(HawDBError::Execution(
                "ordered composite access is unavailable in a bounded-work scope".into(),
            ));
        }
        let _key_memory = admission
            .scratch(key_bytes(seek))
            .map_err(canonical_segment_error)?;
        let (first, last) = seek_bounds(label, seek);
        let entries = self.composite_property_index.range(&first, &last).flat_map(
            |((candidate_label, key), ids)| ids.iter().map(move |id| (*candidate_label, key, *id)),
        );
        let mut live = Directed {
            input: entries,
            descending: seek.descending,
        }
        .take_while(|_| admission.checkpoint().is_ok())
        .filter(|(candidate_label, key, _)| *candidate_label == label && key_matches(key, seek))
        .filter(|(_, _, id)| !self.node_tombstones.contains(id))
        .filter_map(|(_, key, id)| self.nodes.get(&id).map(|node| (key, node)))
        .peekable();
        let emit_live = |key: &[(String, Value)],
                         node: &NodeRecord,
                         consumer: &mut dyn FnMut(AdmittedProjection) -> GraphScanControl|
         -> std::result::Result<
            GraphScanControl,
            PersistentPropertyProjectionError,
        > {
            let selected = admission.live_selection(node, properties)?;
            validate_node(
                &selected.node,
                label,
                seek,
                key.iter().map(|(_, value)| value),
            )?;
            Ok(consumer(selected))
        };
        if let Some(reader) = &self.canonical_base {
            let projection = self
                .persistent_property_projection
                .as_ref()
                .filter(|projection| {
                    projection
                        .manifest()
                        .supports_composite_equality(label, &seek.index_properties)
                })
                .ok_or_else(|| {
                    HawDBError::Execution(
                        "ordered composite projection is unavailable for this read view".into(),
                    )
                })?;
            let order = CompositeOrder {
                label,
                properties: &seek.index_properties,
                equality: &seek.equality_prefix,
                after: seek.after.as_deref(),
                descending: seek.descending,
            };
            let mut result = GraphScanControl::Continue;
            let (report, _) = projection
                .scan_composite_ordered_entries(
                    order,
                    |bytes| {
                        admission
                            .scratch(bytes)
                            .map_err(PersistentPropertyProjectionError::Canonical)
                    },
                    |key, id| {
                        admission.checkpoint()?;
                        if self.nodes.contains_key(&id) || self.node_tombstones.contains(&id) {
                            return Ok(CanonicalScanControl::Continue);
                        }
                        while let Some((live_key, node)) = live.peek() {
                            let ordering = compare_live_key(live_key, node.id, key, id)?;
                            if if seek.descending {
                                ordering.is_lt()
                            } else {
                                ordering.is_gt()
                            } {
                                break;
                            }
                            let (live_key, node) = live.next().expect("peeked ordered delta");
                            if emit_live(live_key, node, &mut consumer)? == GraphScanControl::Stop {
                                result = GraphScanControl::Stop;
                                return Ok(CanonicalScanControl::Stop);
                            }
                        }
                        let selected = admission
                            .canonical_selection(reader, id, properties)?
                            .ok_or_else(|| {
                                PersistentPropertyProjectionError::Corrupt(format!(
                                    "ordered composite projection references missing node {}",
                                    id.0
                                ))
                            })?;
                        let Value::List(values) = key else {
                            unreachable!("ordered reader validates tuple keys")
                        };
                        validate_node(&selected.node, label, seek, values.iter())?;
                        result = consumer(selected);
                        Ok(if result == GraphScanControl::Stop {
                            CanonicalScanControl::Stop
                        } else {
                            CanonicalScanControl::Continue
                        })
                    },
                )
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
            self.graph_index_read_metrics
                .record_property(PersistentGraphIndexClass::NodeCompositeEquality, report);
            if result == GraphScanControl::Stop {
                return Ok(result);
            }
        }
        for (key, node) in live {
            if emit_live(key, node, &mut consumer)
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?
                == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }
}
