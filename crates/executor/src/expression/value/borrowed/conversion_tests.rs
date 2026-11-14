// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Conversion functions charge their string result to the query memory
//! account before it is allocated (#1682 review): Kùzu `string(x)` output can
//! be much larger than its input.

use super::*;
use crate::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_core::{ConversionFunction, Vector};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

fn kuzu_string(column: &str) -> ProjectionExpression {
    ProjectionExpression::Conversion {
        function: ConversionFunction::KuzuString,
        argument: Box::new(ProjectionExpression::Column(column.to_string())),
    }
}

fn binding() -> Binding {
    Binding::values(BTreeMap::from([
        (
            "floats".to_string(),
            Value::List(vec![Value::Float(1e308); 1_000]),
        ),
        (
            "vector".to_string(),
            Value::Vector(Vector::new(&[3.0e38; 4_096]).unwrap()),
        ),
        (
            "lists".to_string(),
            Value::List(vec![Value::List(vec![Value::Int(i64::MIN); 64]); 256]),
        ),
        (
            "small".to_string(),
            Value::List(vec![Value::Int(1), Value::Null]),
        ),
    ]))
}

fn account(ledger: &QueryMemoryLedger, budget: usize) -> QueryMemoryAccount {
    ledger.account(
        QueryMemoryClass::PipelineBatch,
        "conversion test",
        NonZeroUsize::new(budget).unwrap(),
    )
}

#[test]
fn kuzu_string_output_beyond_the_account_is_refused_before_allocation() {
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1 << 20).unwrap());
    let small = account(&ledger, 64 * 1024);
    // The account's own refusal for a reservation of this kind.
    let own_error = small.reserve(1 << 20).unwrap_err();
    let catalog = Catalog::default();
    let binding = binding();
    let available = small.available_bytes();
    for column in ["floats", "vector", "lists"] {
        let expression = kuzu_string(column);
        let error = evaluate_projection_borrowed(&expression, &catalog, &binding, Some(&small))
            .err()
            .unwrap_or_else(|| panic!("string({column}) beyond the account must fail"));
        assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&own_error),
            "{column}: {error}"
        );
        assert!(error.to_string().contains("memory"), "{column}: {error}");
        assert_eq!(small.available_bytes(), available, "{column}");
    }
}

#[test]
fn kuzu_string_output_is_charged_at_least_its_length_and_released() {
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(4 << 20).unwrap());
    let large = account(&ledger, 2 << 20);
    let catalog = Catalog::default();
    let binding = binding();
    for column in ["small", "floats", "vector", "lists"] {
        let available = large.available_bytes();
        let expression = kuzu_string(column);
        let value =
            evaluate_projection_borrowed(&expression, &catalog, &binding, Some(&large)).unwrap();
        let Value::String(text) = value.as_ref() else {
            panic!("{column}: {value:?}", value = value.as_ref());
        };
        assert!(
            available - large.available_bytes() >= text.len(),
            "{column}: charged {} for {} bytes",
            available - large.available_bytes(),
            text.len()
        );
        if column == "small" {
            assert_eq!(text, "[1,]");
        }
        drop(value);
        assert_eq!(large.available_bytes(), available, "{column}");
    }
}

#[test]
fn unaccounted_conversion_output_is_bounded_before_allocation() {
    // Without an account the result counts toward the per-evaluation
    // unaccounted byte limit; output above it is refused.
    let catalog = Catalog::default();
    let huge = Binding::values(BTreeMap::from([(
        "floats".to_string(),
        Value::List(vec![Value::Float(1e308); 40_000]),
    )]));
    let error = evaluate_projection_borrowed(&kuzu_string("floats"), &catalog, &huge, None)
        .err()
        .expect("output beyond the unaccounted limit must fail");
    assert!(
        error.to_string().contains("work limit")
            || error.to_string().contains("without a query memory account"),
        "{error}"
    );
}
