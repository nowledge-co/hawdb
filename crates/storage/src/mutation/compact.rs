//! Compaction of the facade's already-staged graph transaction journal.

use crate::wal::WalOp;
use crate::{NodeId, RelId};
use std::collections::BTreeMap;

/// Folds SETs into same-transaction creates and elides those creates on DELETE.
///
/// This is not a replay or admission API. It only processes this vector's top
/// level; other operations, including nested batches, pass through unchanged.
/// Validation, ID allocation, commit epochs, and durable publication remain
/// the caller's responsibility.
pub fn compact_transaction_graph_ops(ops: Vec<WalOp>) -> Vec<WalOp> {
    let mut compacted = Vec::<Option<WalOp>>::with_capacity(ops.len());
    let mut created_nodes = BTreeMap::<NodeId, usize>::new();
    let mut created_relationships = BTreeMap::<RelId, usize>::new();

    for op in ops {
        match op {
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => {
                let index = compacted.len();
                compacted.push(Some(WalOp::CreateNode {
                    id,
                    label,
                    properties,
                }));
                created_nodes.insert(id, index);
            }
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                let index = compacted.len();
                compacted.push(Some(WalOp::CreateRelationship {
                    id,
                    source,
                    target,
                    rel_type,
                    properties,
                }));
                created_relationships.insert(id, index);
            }
            WalOp::SetNodeProperty {
                id,
                property,
                value,
            } => {
                let folded = created_nodes.get(&id).is_some_and(|index| {
                    let Some(WalOp::CreateNode { properties, .. }) = compacted[*index].as_mut()
                    else {
                        return false;
                    };
                    properties.insert(property.clone(), value.clone());
                    true
                });
                if !folded {
                    compacted.push(Some(WalOp::SetNodeProperty {
                        id,
                        property,
                        value,
                    }));
                }
            }
            WalOp::SetRelationshipProperty {
                id,
                property,
                value,
            } => {
                let folded = created_relationships.get(&id).is_some_and(|index| {
                    let Some(WalOp::CreateRelationship { properties, .. }) =
                        compacted[*index].as_mut()
                    else {
                        return false;
                    };
                    properties.insert(property.clone(), value.clone());
                    true
                });
                if !folded {
                    compacted.push(Some(WalOp::SetRelationshipProperty {
                        id,
                        property,
                        value,
                    }));
                }
            }
            WalOp::DeleteRelationship { id } => {
                if let Some(index) = created_relationships.remove(&id) {
                    compacted[index] = None;
                } else {
                    compacted.push(Some(WalOp::DeleteRelationship { id }));
                }
            }
            WalOp::DeleteNode { id } => {
                if let Some(index) = created_nodes.remove(&id) {
                    compacted[index] = None;
                } else {
                    compacted.push(Some(WalOp::DeleteNode { id }));
                }
            }
            other => compacted.push(Some(other)),
        }
    }

    compacted.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests;
