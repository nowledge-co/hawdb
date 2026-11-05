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

//! Numeric functions, `reduce` and relationship endpoints (#1487).

use super::*;
use crate::binding::value_memory_bytes;
use crate::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_core::{
    ids::{NodeId, RelId},
    ListFunction, NumericFunction, RelTypeId, RelationshipEndpoint,
};
use hawdb_plan_cypher::{ReduceProjection, ScalarBinaryOp};
use std::num::NonZeroUsize;

fn literal(value: Value) -> ProjectionExpression {
    ProjectionExpression::Literal(value)
}

fn local(name: &str) -> ProjectionExpression {
    ProjectionExpression::LocalVariable {
        name: name.to_string(),
        property: None,
    }
}

fn ints(values: &[i64]) -> Value {
    Value::List(values.iter().copied().map(Value::Int).collect())
}

fn add(left: ProjectionExpression, right: ProjectionExpression) -> ProjectionExpression {
    ProjectionExpression::Binary {
        left: Box::new(left),
        op: ScalarBinaryOp::Add,
        right: Box::new(right),
    }
}

fn reduce(
    initial: ProjectionExpression,
    list: ProjectionExpression,
    expression: ProjectionExpression,
) -> ProjectionExpression {
    ProjectionExpression::Reduce(Box::new(ReduceProjection {
        accumulator: "acc".to_string(),
        initial,
        variable: "x".to_string(),
        list,
        expression,
    }))
}

fn evaluate(expression: &ProjectionExpression, binding: &Binding) -> Result<Value> {
    evaluate_projection_expression(expression, &Catalog::default(), binding)
}

fn values() -> Binding {
    Binding::values(BTreeMap::from([
        ("xs".to_string(), ints(&[1, 2, 3])),
        ("missing".to_string(), Value::Null),
    ]))
}

#[test]
fn numeric_functions_evaluate_their_argument_with_null_propagation() {
    let call = |function, argument| ProjectionExpression::NumericFunction {
        function,
        argument: Box::new(argument),
    };
    let binding = values();
    for (expression, expected) in [
        (
            call(NumericFunction::Abs, literal(Value::Int(-4))),
            Value::Int(4),
        ),
        (
            call(NumericFunction::Floor, literal(Value::Float(-0.5))),
            Value::Float(-1.0),
        ),
        (
            call(NumericFunction::Round, literal(Value::Float(-0.5))),
            Value::Float(0.0),
        ),
        (
            call(
                NumericFunction::Ceil,
                ProjectionExpression::Column("missing".to_string()),
            ),
            Value::Null,
        ),
    ] {
        assert_eq!(evaluate(&expression, &binding).unwrap(), expected);
    }
    let error = evaluate(
        &call(
            NumericFunction::Sqrt,
            ProjectionExpression::Column("xs".into()),
        ),
        &binding,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("sqrt() requires an INTEGER or FLOAT argument, got LIST"));
}

#[test]
fn reduce_folds_in_order_and_propagates_a_null_list() {
    let binding = values();
    let sum = reduce(
        literal(Value::Int(10)),
        ProjectionExpression::Column("xs".to_string()),
        add(local("acc"), local("x")),
    );
    assert_eq!(evaluate(&sum, &binding).unwrap(), Value::Int(16));
    // Order matters: append each element to the accumulated list.
    let appended = reduce(
        literal(ints(&[])),
        ProjectionExpression::Column("xs".to_string()),
        ProjectionExpression::ListFunction {
            function: ListFunction::Append,
            arguments: vec![local("acc"), local("x")],
        },
    );
    assert_eq!(evaluate(&appended, &binding).unwrap(), ints(&[1, 2, 3]));
    let null = reduce(
        literal(Value::Int(0)),
        ProjectionExpression::Column("missing".to_string()),
        add(local("acc"), local("x")),
    );
    assert_eq!(evaluate(&null, &binding).unwrap(), Value::Null);
    let error = evaluate(
        &reduce(literal(Value::Int(0)), literal(Value::Int(3)), local("acc")),
        &binding,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("reduce() requires a list"), "{error}");
}

#[test]
fn reduce_accumulators_are_charged_and_bounded() {
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1 << 20).unwrap());
    let account = ledger.account(
        QueryMemoryClass::PipelineBatch,
        "reduce test",
        NonZeroUsize::new(64 * 1024).unwrap(),
    );
    let catalog = Catalog::default();
    let binding = values();
    let available = account.available_bytes();
    let range = |end| ProjectionExpression::ListFunction {
        function: ListFunction::Range,
        arguments: vec![literal(Value::Int(1)), literal(Value::Int(end))],
    };
    let appended = reduce(
        literal(ints(&[])),
        range(100),
        ProjectionExpression::ListFunction {
            function: ListFunction::Append,
            arguments: vec![local("acc"), local("x")],
        },
    );
    let value =
        evaluate_projection_borrowed(&appended, &catalog, &binding, Some(&account)).unwrap();
    assert_eq!(
        value.as_ref(),
        &Value::List((1..=100).map(Value::Int).collect())
    );
    assert!(available - account.available_bytes() >= value_memory_bytes(value.as_ref()));
    drop(value);
    assert_eq!(account.available_bytes(), available);

    // A fold whose accumulator outgrows the budget fails instead of growing.
    let error = evaluate_projection_borrowed(
        &reduce(
            literal(ints(&[])),
            range(100_000),
            ProjectionExpression::ListFunction {
                function: ListFunction::Append,
                arguments: vec![local("acc"), local("x")],
            },
        ),
        &catalog,
        &binding,
        Some(&account),
    )
    .err()
    .expect("an accumulator beyond the budget must fail")
    .to_string();
    assert!(error.contains("memory"), "{error}");
    assert_eq!(account.available_bytes(), available);

    // Each element is a list work unit, as for comprehensions, while a long
    // numeric fold copies no payload.
    let twenty = Binding::values(BTreeMap::from([(
        "twenty".to_string(),
        Value::List((0..20).map(Value::Int).collect()),
    )]));
    let inner = |list| reduce(literal(Value::Int(0)), list, add(local("acc"), local("x")));
    assert_eq!(
        evaluate(&inner(range(1_000_000)), &binding).unwrap(),
        Value::Int(500_000_500_000)
    );
    let error = evaluate(
        &ProjectionExpression::ListComprehension(Box::new(
            hawdb_plan_cypher::ListComprehensionProjection {
                variable: "y".to_string(),
                list: range(1_000_000),
                predicate: None,
                projection: Some(inner(ProjectionExpression::Column("twenty".to_string()))),
            },
        )),
        &twenty,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("work limit"), "{error}");
}

#[test]
fn relationship_endpoints_select_the_stored_source_and_target() {
    let node = |id| NodeRecord {
        id: NodeId(id),
        labels: Default::default(),
        properties: BTreeMap::from([("id".to_string(), Value::Int(id as i64 * 10))]),
    };
    let mut binding = Binding::values(BTreeMap::from([("gone".to_string(), Value::Null)]));
    // An undirected match may bind the target as the first pattern node.
    binding.nodes.insert("a".to_string(), node(8));
    binding.nodes.insert("b".to_string(), node(7));
    binding.relationships.insert(
        "r".to_string(),
        RelRecord {
            id: RelId(11),
            source: NodeId(7),
            target: NodeId(8),
            rel_type: RelTypeId(0),
            properties: BTreeMap::new(),
        },
    );
    let endpoint = |endpoint, variable: &str| ProjectionExpression::PropertyAccess {
        expression: Box::new(ProjectionExpression::RelationshipEndpoint {
            endpoint,
            variable: variable.to_string(),
            endpoints: Some(["a".to_string(), "b".to_string()]),
        }),
        key: "id".to_string(),
    };
    assert_eq!(
        evaluate(&endpoint(RelationshipEndpoint::Start, "r"), &binding).unwrap(),
        Value::Int(70)
    );
    assert_eq!(
        evaluate(&endpoint(RelationshipEndpoint::End, "r"), &binding).unwrap(),
        Value::Int(80)
    );
    assert_eq!(
        evaluate(&endpoint(RelationshipEndpoint::End, "gone"), &binding).unwrap(),
        Value::Null
    );
    let error = evaluate(&endpoint(RelationshipEndpoint::Start, "a"), &binding)
        .unwrap_err()
        .to_string();
    assert!(error.contains("startNode() requires a relationship, got node 'a'"));
    // Another variable binding the same node is never consulted.
    binding.nodes = Default::default();
    binding.nodes.insert("a".to_string(), node(8));
    binding.nodes.insert("c".to_string(), node(7));
    let error = evaluate(&endpoint(RelationshipEndpoint::Start, "r"), &binding)
        .unwrap_err()
        .to_string();
    assert!(error.contains("endpoint nodes to be bound"), "{error}");
    let unresolved = ProjectionExpression::RelationshipEndpoint {
        endpoint: RelationshipEndpoint::End,
        variable: "r".to_string(),
        endpoints: None,
    };
    let error = evaluate(&unresolved, &binding).unwrap_err().to_string();
    assert!(error.contains("endpoint nodes to be bound"), "{error}");
    binding.nodes = Default::default();
    let error = evaluate(&endpoint(RelationshipEndpoint::End, "r"), &binding)
        .unwrap_err()
        .to_string();
    assert!(error.contains("endpoint nodes to be bound"), "{error}");
}
