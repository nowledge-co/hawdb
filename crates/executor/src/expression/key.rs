// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Admit scalar working memory and retain owned keys until operator handoff.

use super::{
    binding_id, binding_property, evaluate_projection_in_graph, Binding, Catalog, ExpressionGraph,
    ProjectedValue, Projection, Result, RuntimeTaskContext, SortDirection, SortItem, SortKey,
    Value,
};
use crate::binding::{node_memory_bytes, relationship_memory_bytes, value_memory_bytes};
use crate::pipeline::runtime_checkpoint;
use crate::{QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease};
use hawdb_plan_cypher::{GraphGroupKeys, ProjectionExpression};
use hawdb_storage::{NodeRecord, RelRecord};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// One element of a grouping key (#1615). A group key that is a bound node or
/// relationship variable groups by entity identity: it hashes and compares
/// as `(kind, id)`, the same identity `count(DISTINCT n)` uses
/// (`binding_identity_key`), and keeps the first row's shared record, which
/// the grouping emits. Every other key is its value. NULL stays a value.
#[derive(Debug, Clone)]
pub(crate) enum GroupKeyValue {
    Value(Value),
    Node(Arc<NodeRecord>),
    Relationship(Arc<RelRecord>),
}

// Grouping charges and spill sizes count a key element as one `Value` slot.
const _: () = assert!(std::mem::size_of::<GroupKeyValue>() == std::mem::size_of::<Value>());

/// Reference-count header of a retained shared record.
const SHARED_RECORD_HEADER_BYTES: usize = 2 * std::mem::size_of::<usize>();

impl GroupKeyValue {
    /// `(kind, id)` as `binding_identity_key` reports it: 0 for a node and 1
    /// for a relationship.
    pub(crate) fn identity(&self) -> Option<(u8, u64)> {
        match self {
            Self::Value(_) => None,
            Self::Node(node) => Some((0, node.id.0)),
            Self::Relationship(relationship) => Some((1, relationship.id.0)),
        }
    }

    /// The map a projected entity has always produced.
    pub(crate) fn into_value(self, catalog: &Catalog) -> Value {
        match self {
            Self::Value(value) => value,
            Self::Node(node) => super::node_value(&node, catalog),
            Self::Relationship(relationship) => super::relationship_value(&relationship, catalog),
        }
    }
}

impl PartialEq for GroupKeyValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Value(left), Self::Value(right)) => left == right,
            _ => self.identity().is_some() && self.identity() == other.identity(),
        }
    }
}

impl Eq for GroupKeyValue {}

impl Hash for GroupKeyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match (self, self.identity()) {
            (Self::Value(value), _) => {
                state.write_u8(u8::MAX);
                value.hash(state);
            }
            (_, Some((kind, id))) => {
                state.write_u8(kind);
                state.write_u64(id);
            }
            (_, None) => unreachable!("entities have an identity"),
        }
    }
}

impl Ord for GroupKeyValue {
    /// Values, then nodes, then relationships, entities by id. A key position
    /// holds either NULL or one entity kind, so groups keep NULL first.
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Value(left), Self::Value(right)) => left.cmp(right),
            (Self::Value(_), _) => Ordering::Less,
            (_, Self::Value(_)) => Ordering::Greater,
            _ => self.identity().cmp(&other.identity()),
        }
    }
}

impl PartialOrd for GroupKeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Retained bytes of one key element. A record shared with input rows is
/// charged to each group that keeps it, once.
fn group_key_value_memory_bytes(value: &GroupKeyValue) -> usize {
    match value {
        GroupKeyValue::Value(value) => value_memory_bytes(value),
        GroupKeyValue::Node(node) => std::mem::size_of::<GroupKeyValue>()
            .saturating_add(SHARED_RECORD_HEADER_BYTES)
            .saturating_add(node_memory_bytes(node)),
        GroupKeyValue::Relationship(relationship) => std::mem::size_of::<GroupKeyValue>()
            .saturating_add(SHARED_RECORD_HEADER_BYTES)
            .saturating_add(relationship_memory_bytes(relationship)),
    }
}

pub(crate) fn group_key_memory_bytes(key: &[GroupKeyValue]) -> usize {
    key.iter().fold(0usize, |total, value| {
        total.saturating_add(group_key_value_memory_bytes(value))
    })
}

/// Builds the output row of one group: each key under its projection name,
/// then `values` (the aggregates). With slot output, an entity key leaves in
/// the graph slot of its name; otherwise it leaves as its map.
pub(crate) fn group_output_binding(
    group_keys: &[Projection],
    key: Vec<GroupKeyValue>,
    graph_keys: GraphGroupKeys,
    catalog: &Catalog,
) -> Binding {
    let mut binding = Binding::values(BTreeMap::new());
    for (item, value) in group_keys.iter().zip(key) {
        match value {
            GroupKeyValue::Node(node) if graph_keys.retains_slots() => {
                binding.nodes.insert_arc(item.name.clone(), node);
            }
            GroupKeyValue::Relationship(relationship) if graph_keys.retains_slots() => {
                binding
                    .relationships
                    .insert_arc(item.name.clone(), relationship);
            }
            value => super::insert_projected_value(
                &mut binding.values,
                &item.name,
                value.into_value(catalog),
            ),
        }
    }
    binding
}

#[cfg(test)]
thread_local! {
    static LEGACY_MAP_KEYS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Groups by materialized entity maps, as before #1615, on this thread while
/// `f` runs. The differential tests compare it with identity grouping.
#[cfg(test)]
pub(crate) fn with_legacy_map_keys<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            LEGACY_MAP_KEYS.with(|legacy| legacy.set(self.0));
        }
    }
    let _restore = Restore(LEGACY_MAP_KEYS.with(|legacy| legacy.replace(true)));
    f()
}

/// The identity key of a bound node or relationship variable.
fn entity_group_key(expression: &ProjectionExpression, binding: &Binding) -> Option<GroupKeyValue> {
    let ProjectionExpression::Variable { variable } = expression else {
        return None;
    };
    #[cfg(test)]
    if LEGACY_MAP_KEYS.with(std::cell::Cell::get) {
        return None;
    }
    binding
        .nodes
        .get_shared(variable)
        .map(|node| GroupKeyValue::Node(Arc::clone(node)))
        .or_else(|| {
            binding
                .relationships
                .get_shared(variable)
                .map(|relationship| GroupKeyValue::Relationship(Arc::clone(relationship)))
        })
}

pub(crate) struct PreparedKey<T> {
    pub(crate) values: T,
    pub(crate) allocation: QueryMemoryLease,
}

pub(crate) struct KeyMemory {
    evaluation: QueryMemoryAccount,
    preparation: QueryMemoryAccount,
}

impl KeyMemory {
    pub(crate) fn new(
        parent: &QueryMemoryAccount,
        budget: NonZeroUsize,
        budget_option: &'static str,
        evaluation_owner: &'static str,
        preparation_owner: &'static str,
    ) -> Self {
        Self {
            evaluation: parent.sibling_with_budget_option(
                QueryMemoryClass::BlockingState,
                evaluation_owner,
                budget,
                budget_option,
            ),
            preparation: parent.sibling_with_budget_option(
                QueryMemoryClass::BlockingState,
                preparation_owner,
                budget,
                budget_option,
            ),
        }
    }

    pub(crate) fn group(
        &self,
        items: &[Projection],
        catalog: &Catalog,
        binding: &Binding,
        task_context: Option<&RuntimeTaskContext>,
        graph: Option<&ExpressionGraph<'_>>,
    ) -> Result<PreparedKey<Vec<Value>>> {
        runtime_checkpoint(task_context)?;
        let mut allocation = self
            .preparation
            .reserve(items.len().saturating_mul(std::mem::size_of::<Value>()))?;
        let mut values = Vec::with_capacity(items.len());
        for item in items {
            runtime_checkpoint(task_context)?;
            let value = evaluate_projection_in_graph(
                &item.expression,
                catalog,
                binding,
                Some(&self.evaluation),
                graph,
            )?;
            values.push(value.into_owned_accounted(&mut allocation)?);
        }
        runtime_checkpoint(task_context)?;
        Ok(PreparedKey { values, allocation })
    }

    /// Grouping keys with entity identity. Shares a bound record instead of
    /// building its map; the record is charged when a group retains it.
    pub(crate) fn group_by_identity(
        &self,
        items: &[Projection],
        catalog: &Catalog,
        binding: &Binding,
        task_context: Option<&RuntimeTaskContext>,
        graph: Option<&ExpressionGraph<'_>>,
    ) -> Result<PreparedKey<Vec<GroupKeyValue>>> {
        runtime_checkpoint(task_context)?;
        let mut allocation = self.preparation.reserve(
            items
                .len()
                .saturating_mul(std::mem::size_of::<GroupKeyValue>()),
        )?;
        let mut values = Vec::with_capacity(items.len());
        for item in items {
            runtime_checkpoint(task_context)?;
            if let Some(entity) = entity_group_key(&item.expression, binding) {
                values.push(entity);
                continue;
            }
            let value = evaluate_projection_in_graph(
                &item.expression,
                catalog,
                binding,
                Some(&self.evaluation),
                graph,
            )?;
            values.push(GroupKeyValue::Value(
                value.into_owned_accounted(&mut allocation)?,
            ));
        }
        runtime_checkpoint(task_context)?;
        Ok(PreparedKey { values, allocation })
    }

    pub(crate) fn sort(
        &self,
        items: &[SortItem],
        catalog: &Catalog,
        binding: &Binding,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<PreparedKey<Vec<(Value, SortDirection)>>> {
        runtime_checkpoint(task_context)?;
        let mut allocation = self.preparation.reserve(
            items
                .len()
                .saturating_mul(std::mem::size_of::<(Value, SortDirection)>()),
        )?;
        let mut values = Vec::with_capacity(items.len());
        for item in items {
            runtime_checkpoint(task_context)?;
            let value = match &item.key {
                SortKey::Property { variable, property } => {
                    binding_property(binding, variable, property)
                        .map(ProjectedValue::borrowed)
                        .unwrap_or_else(|| ProjectedValue::scalar(Value::Null))
                }
                SortKey::Id { variable } => {
                    ProjectedValue::scalar(binding_id(binding, variable).unwrap_or(Value::Null))
                }
                // Planning projects pattern expressions in ORDER BY keys to
                // columns first, so a sort key never reads the graph.
                SortKey::Expression(expression) => evaluate_projection_in_graph(
                    expression,
                    catalog,
                    binding,
                    Some(&self.evaluation),
                    None,
                )?,
                SortKey::Column(name) => binding
                    .values
                    .get(name)
                    .map(ProjectedValue::borrowed)
                    .unwrap_or_else(|| ProjectedValue::scalar(Value::Null)),
            };
            values.push((value.into_owned_accounted(&mut allocation)?, item.direction));
        }
        runtime_checkpoint(task_context)?;
        Ok(PreparedKey { values, allocation })
    }
}

#[cfg(test)]
mod tests {
    use super::super::evaluate_projection_borrowed;
    use super::*;
    use crate::QueryMemoryLedger;
    use hawdb_plan_cypher::ProjectionExpression;

    #[test]
    fn computed_payload_moves_without_copy_or_duplicate_admission() {
        let payload_bytes = 512;
        let slot_bytes = std::mem::size_of::<Value>();
        let budget = NonZeroUsize::new(2 * slot_bytes + payload_bytes).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let retained = ledger.account(QueryMemoryClass::BlockingState, "retained", budget);
        let memory = KeyMemory::new(&retained, budget, "test", "evaluation", "keys");
        let expression = ProjectionExpression::Lower(Box::new(ProjectionExpression::Literal(
            Value::String("X".repeat(payload_bytes)),
        )));
        let catalog = Catalog::default();
        let binding = Binding::values(Default::default());
        let mut allocation = memory.preparation.reserve(slot_bytes).unwrap();
        let value =
            evaluate_projection_borrowed(&expression, &catalog, &binding, Some(&memory.evaluation))
                .unwrap();
        let Value::String(text) = value.as_ref() else {
            panic!("expected a string");
        };
        let pointer = text.as_ptr();
        assert_eq!(ledger.snapshot().used_bytes, budget.get());
        let owned = value.into_owned_accounted(&mut allocation).unwrap();
        let Value::String(text) = &owned else {
            panic!("expected a string");
        };
        assert_eq!(text.as_ptr(), pointer);
        assert_eq!(text, &"x".repeat(payload_bytes));
        assert_eq!(ledger.snapshot().used_bytes, slot_bytes + payload_bytes);
        let mut destination = retained.reserve(0).unwrap();
        allocation
            .transfer_to(
                allocation.bytes(),
                &mut destination,
                slot_bytes + payload_bytes,
            )
            .unwrap();
        drop(owned);
        drop(destination);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(ledger.snapshot().peak_bytes, budget.get());
    }

    #[test]
    fn failed_or_cancelled_keys_release_prepared_values() {
        let budget = NonZeroUsize::new(4096).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let retained = ledger.account(QueryMemoryClass::BlockingState, "retained", budget);
        let memory = KeyMemory::new(&retained, budget, "test", "evaluation", "keys");
        let binding = Binding::values(Default::default());
        let catalog = Catalog::default();
        for document in ["{invalid".to_string(), "x".repeat(256)] {
            let items = [
                Projection {
                    expression: ProjectionExpression::Literal(Value::String("first".into())),
                    name: "first".into(),
                },
                Projection {
                    expression: ProjectionExpression::JsonValue {
                        expression: Box::new(ProjectionExpression::Literal(Value::String(
                            document,
                        ))),
                        path: "$.source".into(),
                        null_on_non_scalar: false,
                    },
                    name: "second".into(),
                },
            ];
            assert!(memory
                .group(&items, &catalog, &binding, None, None)
                .is_err());
            assert_eq!(ledger.snapshot().used_bytes, 0);
            let context = RuntimeTaskContext::default();
            context.cancellation().cancel();
            let error = memory
                .group(&items, &catalog, &binding, Some(&context), None)
                .err()
                .unwrap();
            assert!(
                error.to_string().contains("runtime task stopped"),
                "{error}"
            );
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }

    #[test]
    fn borrowed_sort_keys_keep_tuple_storage_and_child_backing_admitted() {
        let payload = "x".repeat(256);
        let key_bytes = std::mem::size_of::<(Value, SortDirection)>() + payload.len();
        let budget = NonZeroUsize::new(key_bytes).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let parent = ledger.account(QueryMemoryClass::BlockingState, "parent", budget);
        let child = parent.sub_account(budget).unwrap();
        let memory = KeyMemory::new(&child, budget, "test", "evaluation", "keys");
        let binding = Binding::scalar("value", Value::String(payload.clone()));
        let prepared = memory
            .sort(
                &[SortItem {
                    key: SortKey::Column("value".into()),
                    direction: SortDirection::Asc,
                }],
                &Catalog::default(),
                &binding,
                None,
            )
            .unwrap();
        assert_eq!(prepared.allocation.bytes(), key_bytes);
        assert_eq!(
            prepared.values,
            vec![(Value::String(payload), SortDirection::Asc)]
        );
        let error = memory
            .sort(
                &[SortItem {
                    key: SortKey::Column("value".into()),
                    direction: SortDirection::Asc,
                }],
                &Catalog::default(),
                &binding,
                None,
            )
            .err()
            .expect("the retained key already uses the complete allowance");
        let hawdb_core::HawDBError::ResourceBudgetExceeded(cause) = error else {
            panic!("expected a typed key-preparation refusal, got {error}");
        };
        assert_eq!(cause.scope, hawdb_core::ResourceBudgetScope::Account);
        assert_eq!(cause.memory_class, Some("blocking_state"));
        assert_eq!(cause.owner, "keys");
        assert_eq!(cause.used, key_bytes as u64);
        assert_eq!(cause.budget, key_bytes as u64);
        assert_eq!(prepared.allocation.bytes(), key_bytes);
        drop((memory, child, parent));
        assert_eq!(ledger.snapshot().used_bytes, key_bytes);
        drop(prepared);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
