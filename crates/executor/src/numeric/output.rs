// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! One admitted projection schema shared by all native output rows.

use crate::pipeline::{BatchControl, BindingBatch};
use crate::{QueryMemoryAccount, QueryMemoryLease, RowRef};
use hawdb_core::Result;
use hawdb_plan_cypher::Projection;
use std::fmt::Write;

pub(super) enum NumericOutput<'a> {
    Bindings(&'a mut dyn FnMut(BindingBatch) -> Result<BatchControl>),
    Borrowed(&'a mut dyn FnMut(RowRef<'_>) -> Result<BatchControl>),
}

pub(super) struct NativeProjection {
    pub columns: Vec<(String, usize)>,
    _allocation: QueryMemoryLease,
}

impl NativeProjection {
    pub fn new(items: &[Projection], account: &QueryMemoryAccount) -> Result<Self> {
        let suffix_bytes = items.len().saturating_add(2).ilog10() as usize + 2;
        let bytes = items.iter().fold(
            items
                .len()
                .saturating_mul(std::mem::size_of::<(String, usize)>()),
            |bytes, item| {
                bytes
                    .saturating_add(item.name.len())
                    .saturating_add(suffix_bytes)
            },
        );
        let allocation = account.reserve(bytes)?;
        let mut columns: Vec<(String, usize)> = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            let mut name = String::with_capacity(item.name.len().saturating_add(suffix_bytes));
            name.push_str(&item.name);
            let mut suffix = 2;
            while columns.iter().any(|(column, _)| column == &name) {
                name.clear();
                name.push_str(&item.name);
                write!(name, "#{suffix}").expect("writing into a String cannot fail");
                suffix += 1;
            }
            columns.push((name, index));
        }
        // Match the public map schema, including the existing #2 alias rule.
        // Unstable sorting is in-place and does not need another allocation.
        columns.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(Self {
            columns,
            _allocation: allocation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::{NumericLiteral, NumericPredicate};
    use crate::numeric::{NumericBatchEmitter, NumericFragment};
    use crate::observer::QueryExecutionObserver;
    use crate::{ExecutionLimit, ExecutionMemoryConfig, QueryMemoryLedger};
    use hawdb_core::{HawDBError, PropertyType, RuntimeTaskContext, Value};
    use hawdb_plan_cypher::ProjectionExpression;
    use hawdb_storage::{NodeId, NodeRecord};
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;

    #[test]
    fn native_projection_borrows_payloads_and_preserves_aliases_on_every_exit() {
        let items = [
            Projection {
                name: "value".into(),
                expression: ProjectionExpression::Property {
                    variable: "n".into(),
                    property: "body".into(),
                },
            },
            Projection {
                name: "value".into(),
                expression: ProjectionExpression::Literal(Value::String("literal".into())),
            },
            Projection {
                name: "value#2".into(),
                expression: ProjectionExpression::Literal(Value::Null),
            },
        ];
        let node = NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([
                ("score".into(), Value::Int(1)),
                ("body".into(), Value::String("x".repeat(256 * 1024))),
            ]),
        };
        let fragment = NumericFragment {
            label: "Item",
            property: "score",
            property_type: PropertyType::Int,
            predicate: NumericPredicate::Eq,
            expected: NumericLiteral::Int(1),
            fused_operators: None,
        };
        // Borrowing the body must not charge a second 256-KiB owned payload.
        let memory = ExecutionMemoryConfig {
            query_memory_bytes: NonZeroUsize::new(4096).unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        for exit in ["complete", "stop", "error", "cancel", "panic", "refuse"] {
            let ledger = QueryMemoryLedger::new(if exit == "refuse" {
                NonZeroUsize::MIN
            } else {
                memory.query_memory_bytes
            });
            let observer = QueryExecutionObserver::default();
            let task = RuntimeTaskContext::default();
            let mut calls = 0;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut consumer = |row: RowRef<'_>| {
                    assert!(matches!(row, RowRef::Projected(_)));
                    calls += 1;
                    let body = row.get("value").unwrap().as_str().unwrap();
                    assert_eq!(
                        body.as_ptr(),
                        node.properties["body"].as_ref().as_str().unwrap().as_ptr()
                    );
                    let ProjectionExpression::Literal(literal) = &items[1].expression else {
                        unreachable!()
                    };
                    assert_eq!(
                        row.get("value#2").unwrap().as_str().unwrap().as_ptr(),
                        literal.as_ref().as_str().unwrap().as_ptr()
                    );
                    assert!(matches!(
                        row.get("value#2#2"),
                        Some(hawdb_core::ValueRef::Null)
                    ));
                    assert_eq!(
                        row.iter().map(|(name, _)| name).collect::<Vec<_>>(),
                        ["value", "value#2", "value#2#2"]
                    );
                    assert_eq!(row.len(), 3);
                    assert!(row.get("missing").is_none());
                    assert!(ledger.snapshot().used_bytes > 0);
                    match exit {
                        "error" => Err(HawDBError::Execution("native consumer failed".into())),
                        "panic" => panic!("native consumer panic"),
                        "cancel" => {
                            task.cancellation().cancel();
                            Ok(BatchControl::Continue)
                        }
                        "stop" => Ok(BatchControl::Stop),
                        _ => Ok(BatchControl::Continue),
                    }
                };
                let mut emitter = NumericBatchEmitter::new(
                    fragment,
                    &items,
                    ExecutionLimit::unlimited(),
                    (&memory, &ledger),
                    Some(&task),
                    &observer,
                    NumericOutput::Borrowed(&mut consumer),
                );
                emitter.emit_nodes(std::slice::from_ref(&node))
            }));
            assert_eq!(calls, usize::from(exit != "refuse"));
            match exit {
                "panic" => assert!(result.is_err()),
                "error" | "cancel" | "refuse" => assert!(result.unwrap().is_err()),
                "stop" => assert_eq!(result.unwrap().unwrap(), BatchControl::Stop),
                _ => assert_eq!(result.unwrap().unwrap(), BatchControl::Continue),
            }
            assert_eq!(ledger.snapshot().used_bytes, 0, "{exit}");
        }
    }

    #[test]
    fn projected_row_views_preserve_send_and_sync() {
        fn require_send_sync<T: Send + Sync>() {}
        require_send_sync::<RowRef<'static>>();
    }
}
