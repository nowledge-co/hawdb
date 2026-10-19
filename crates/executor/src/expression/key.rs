// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Admit scalar working memory and retain owned keys until operator handoff.

use super::{
    binding_id, binding_property, evaluate_projection_borrowed, Binding, Catalog, ProjectedValue,
    Projection, Result, RuntimeTaskContext, SortDirection, SortItem, SortKey, Value,
};
use crate::pipeline::runtime_checkpoint;
use crate::{QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease};
use std::num::NonZeroUsize;

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
    ) -> Result<PreparedKey<Vec<Value>>> {
        runtime_checkpoint(task_context)?;
        let mut allocation = self
            .preparation
            .reserve(items.len().saturating_mul(std::mem::size_of::<Value>()))?;
        let mut values = Vec::with_capacity(items.len());
        for item in items {
            runtime_checkpoint(task_context)?;
            let value = evaluate_projection_borrowed(
                &item.expression,
                catalog,
                binding,
                Some(&self.evaluation),
            )?;
            values.push(value.into_owned_accounted(&mut allocation)?);
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
                SortKey::Expression(expression) => evaluate_projection_borrowed(
                    expression,
                    catalog,
                    binding,
                    Some(&self.evaluation),
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
                    },
                    name: "second".into(),
                },
            ];
            assert!(memory.group(&items, &catalog, &binding, None).is_err());
            assert_eq!(ledger.snapshot().used_bytes, 0);
            let context = RuntimeTaskContext::default();
            context.cancellation().cancel();
            let error = memory
                .group(&items, &catalog, &binding, Some(&context))
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
