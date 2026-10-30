// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Query-owned dispatch for simple CASE with constant string keys. Values and
//! result expressions remain borrowed from the bound physical plan.

use super::{
    evaluate_projection_borrowed, Binding, Catalog, ProjectedValue, Projection,
    ProjectionExpression,
};
use crate::observer::ExecutionObserver;
use crate::pipeline::runtime_checkpoint;
use crate::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::{HawDBError, Result, RuntimeTaskContext, Value};

const MIN_DISPATCH_BRANCHES: usize = 8;

struct SimpleCaseDispatch<'plan> {
    expression: &'plan ProjectionExpression,
    operand: &'plan ProjectionExpression,
    results: Vec<(&'plan str, usize, &'plan ProjectionExpression)>,
    otherwise: Option<&'plan ProjectionExpression>,
}

pub(crate) struct PreparedProjectionExpressions<'plan> {
    cases: Vec<SimpleCaseDispatch<'plan>>,
    _allocation: Option<QueryMemoryLease>,
}

fn eligible(expression: &ProjectionExpression) -> bool {
    matches!(expression, ProjectionExpression::Case { operand: Some(operand), branches, .. }
        if matches!(operand.as_ref(), ProjectionExpression::Property { .. } | ProjectionExpression::Column(_))
        && branches.len() >= MIN_DISPATCH_BRANCHES && branches.iter().all(|(key, _)|
            matches!(key, ProjectionExpression::Literal(Value::String(_) | Value::Null))))
}

impl<'plan> PreparedProjectionExpressions<'plan> {
    pub(crate) fn new(
        items: &'plan [Projection],
        account: &QueryMemoryAccount,
        task: Option<&RuntimeTaskContext>,
        observer: &dyn ExecutionObserver,
    ) -> Result<Self> {
        let empty = || Self {
            cases: Vec::new(),
            _allocation: None,
        };
        let mut bytes = 0usize;
        let mut count = 0usize;
        for item in items.iter().filter(|item| eligible(&item.expression)) {
            let ProjectionExpression::Case { branches, .. } = &item.expression else {
                unreachable!()
            };
            count += 1;
            // Reserve vector slots and layouts before allocating.
            bytes = bytes
                .saturating_add(std::mem::size_of::<SimpleCaseDispatch<'_>>())
                .saturating_add(branches.len().saturating_mul(std::mem::size_of::<(
                    &str,
                    usize,
                    &ProjectionExpression,
                )>()));
        }
        if count == 0 {
            return Ok(empty());
        }
        let allocation = match account.reserve(bytes) {
            Ok(allocation) => allocation,
            // Dispatch is optional: the generic expression owns no table and
            // remains a complete execution path under a small workspace.
            Err(HawDBError::ResourceBudgetExceeded(_)) => return Ok(empty()),
            Err(error) => return Err(error),
        };
        let mut cases = Vec::with_capacity(count);
        for item in items.iter().filter(|item| eligible(&item.expression)) {
            runtime_checkpoint(task)?;
            let ProjectionExpression::Case {
                operand: Some(operand),
                branches,
                otherwise,
            } = &item.expression
            else {
                unreachable!()
            };
            let mut results = Vec::with_capacity(branches.len());
            for (index, (key, result)) in branches.iter().enumerate() {
                if index % 64 == 0 {
                    runtime_checkpoint(task)?;
                }
                if let ProjectionExpression::Literal(Value::String(key)) = key {
                    // CASE chooses the first duplicate, even if its result is NULL.
                    results.push((key.as_str(), index, result));
                }
            }
            // The ordinal makes duplicate ordering explicit with an in-place
            // sort; dedup keeps the original first branch, including NULL results.
            results.sort_unstable_by(|left, right| left.0.cmp(right.0).then(left.1.cmp(&right.1)));
            results.dedup_by(|later, earlier| later.0 == earlier.0);
            cases.push(SimpleCaseDispatch {
                expression: &item.expression,
                operand,
                results,
                otherwise: otherwise.as_deref(),
            });
        }
        observer.record_simple_case_dispatch(cases.len(), 0);
        Ok(Self {
            cases,
            _allocation: Some(allocation),
        })
    }

    pub(crate) fn evaluate<'a>(
        &'a self,
        expression: &'a ProjectionExpression,
        catalog: &'a Catalog,
        binding: &'a Binding,
        account: Option<&QueryMemoryAccount>,
        observer: &dyn ExecutionObserver,
    ) -> Result<ProjectedValue<'a>> {
        let evaluate =
            |expression| evaluate_projection_borrowed(expression, catalog, binding, account);
        let Some(dispatch) = self
            .cases
            .iter()
            .find(|case| std::ptr::eq(case.expression, expression))
        else {
            return evaluate(expression);
        };
        let operand = evaluate(dispatch.operand)?;
        let result = match operand.as_ref() {
            Value::String(key) => dispatch
                .results
                .binary_search_by(|entry| entry.0.cmp(key.as_str()))
                .ok()
                .map(|index| dispatch.results[index].2)
                .or(dispatch.otherwise),
            Value::Null => dispatch.otherwise,
            _ => return evaluate(expression),
        };
        observer.record_simple_case_dispatch(0, 1);
        match result {
            Some(result) => evaluate(result),
            None => Ok(ProjectedValue::scalar(Value::Null)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::QueryExecutionObserver;
    use crate::{QueryMemoryClass, QueryMemoryLedger};
    use std::num::NonZeroUsize;

    fn projection(count: usize) -> Projection {
        Projection {
            name: "score".into(),
            expression: ProjectionExpression::Case {
                operand: Some(Box::new(ProjectionExpression::Column("key".into()))),
                branches: (0..count)
                    .map(|index| {
                        (
                            ProjectionExpression::Literal(Value::String(format!("key-{index}"))),
                            ProjectionExpression::Literal(Value::Int(index as i64)),
                        )
                    })
                    .collect(),
                otherwise: Some(Box::new(ProjectionExpression::Literal(Value::Int(-1)))),
            },
        }
    }

    #[test]
    fn prepared_cases_match_generic_evaluation_and_report_actual_lookups() {
        for count in [20, 100, 512] {
            let items = [projection(count)];
            let budget = NonZeroUsize::new(1024 * 1024).unwrap();
            let ledger = QueryMemoryLedger::new(budget);
            let account = ledger.account(QueryMemoryClass::PipelineBatch, "CASE test", budget);
            let observer = QueryExecutionObserver::default();
            let prepared =
                PreparedProjectionExpressions::new(&items, &account, None, &observer).unwrap();
            let catalog = Catalog::default();
            for index in 0..count {
                let binding = Binding::scalar("key", Value::String(format!("key-{index}")));
                let expected = evaluate_projection_borrowed(
                    &items[0].expression,
                    &catalog,
                    &binding,
                    Some(&account),
                )
                .unwrap();
                let actual = prepared
                    .evaluate(
                        &items[0].expression,
                        &catalog,
                        &binding,
                        Some(&account),
                        &observer,
                    )
                    .unwrap();
                assert_eq!(actual.as_ref(), expected.as_ref());
            }
            for key in [Value::Null, Value::String("missing".into()), Value::Int(0)] {
                let binding = Binding::scalar("key", key);
                let expected = evaluate_projection_borrowed(
                    &items[0].expression,
                    &catalog,
                    &binding,
                    Some(&account),
                )
                .unwrap();
                let actual = prepared
                    .evaluate(
                        &items[0].expression,
                        &catalog,
                        &binding,
                        Some(&account),
                        &observer,
                    )
                    .unwrap();
                assert_eq!(actual.as_ref(), expected.as_ref());
            }
            let report = observer.into_reports().pipeline_memory;
            assert_eq!(report.simple_case_dispatch_tables, 1);
            assert_eq!(report.simple_case_dispatch_lookups, count + 2);
            assert!(ledger.snapshot().used_bytes > 0);
            drop(prepared);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }

    #[test]
    fn dispatch_preserves_first_match_null_keys_and_lazy_errors() {
        let mut item = projection(20);
        let ProjectionExpression::Case { branches, .. } = &mut item.expression else {
            unreachable!()
        };
        branches[0].1 = ProjectionExpression::Literal(Value::Null);
        branches.push((
            ProjectionExpression::Literal(Value::String("key-0".into())),
            ProjectionExpression::Literal(Value::Int(99)),
        ));
        // A NULL key is not selected by a NULL operand; its failing result is lazy.
        branches.push((
            ProjectionExpression::Literal(Value::Null),
            ProjectionExpression::Lower(Box::new(ProjectionExpression::Literal(Value::Int(1)))),
        ));
        let items = [item];
        let budget = NonZeroUsize::new(64 * 1024).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::PipelineBatch, "CASE test", budget);
        let observer = QueryExecutionObserver::default();
        let prepared =
            PreparedProjectionExpressions::new(&items, &account, None, &observer).unwrap();
        let catalog = Catalog::default();
        for (key, expected) in [
            (Value::String("key-0".into()), Value::Null),
            (Value::Null, Value::Int(-1)),
        ] {
            let binding = Binding::scalar("key", key);
            assert_eq!(
                prepared
                    .evaluate(
                        &items[0].expression,
                        &catalog,
                        &binding,
                        Some(&account),
                        &observer
                    )
                    .unwrap()
                    .as_ref(),
                &expected
            );
        }
    }

    #[test]
    fn small_workspace_retains_complete_generic_case() {
        let items = [projection(20)];
        let budget = NonZeroUsize::new(256).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::PipelineBatch, "CASE test", budget);
        let observer = QueryExecutionObserver::default();
        let prepared =
            PreparedProjectionExpressions::new(&items, &account, None, &observer).unwrap();
        let catalog = Catalog::default();
        let binding = Binding::scalar("key", Value::String("key-19".into()));
        assert_eq!(
            prepared
                .evaluate(
                    &items[0].expression,
                    &catalog,
                    &binding,
                    Some(&account),
                    &observer
                )
                .unwrap()
                .as_ref(),
            &Value::Int(19)
        );
        assert_eq!(
            observer
                .into_reports()
                .pipeline_memory
                .simple_case_dispatch_tables,
            0
        );
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
