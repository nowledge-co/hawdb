// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `GraphMatchExec` runs the branches of a seeded union (#1625) in turn
//! instead of its own steps. The branches here are written by hand in the
//! form the optimizer emits (`graph::lowering::seeded_union`).

use super::*;
use hawdb_core::RelationshipDirection;
use hawdb_plan_cypher::{GraphMatchNode, GraphMatchProgram, Predicate};

const QUERY: &str = "MATCH (n:Memory)-[:MENTIONS]->(m:Memory) WHERE n.id = 1 OR m.id = 2 \
                     RETURN n.id AS n, m.id AS m";

fn plan() -> PhysicalPlan {
    lower(hawdb_plan_cypher::plan_pipeline_query(QUERY, &BTreeMap::new()).unwrap())
}

fn program(plan: &mut PhysicalPlan) -> &mut GraphMatchProgram {
    let PhysicalPlan::ProjectExec { input, .. } = plan else {
        panic!("expected projection");
    };
    let PhysicalPlan::GraphMatchExec { program, .. } = input.as_mut() else {
        panic!("expected generic MATCH");
    };
    program
}

fn node(variable: &str, id: Option<Value>) -> GraphMatchNode {
    GraphMatchNode {
        variable: variable.into(),
        label: "Memory".into(),
        properties: id.map(|id| ("id".to_string(), id)).into_iter().collect(),
    }
}

/// The optimizer's guard: `variable {id: value}` does not match.
fn not_seeded(variable: &str, value: Value, null_value_term: bool) -> Predicate {
    let mut terms = vec![Predicate::PropertyIsNull {
        variable: variable.into(),
        property: "id".into(),
    }];
    if null_value_term {
        terms.push(Predicate::ExpressionEq {
            expression: ProjectionExpression::IsNull {
                expression: Box::new(ProjectionExpression::Literal(value.clone())),
                negated: false,
            },
            value: ProjectionExpression::Literal(Value::Bool(true)),
        });
    }
    terms.push(Predicate::PropertyNotEq {
        variable: variable.into(),
        property: "id".into(),
        value,
    });
    Predicate::Or(terms)
}

/// Branches seeded at `n {id: n_seed}` and at `m {id: m_seed}`, the second
/// with `guard`.
fn branches(
    program: &GraphMatchProgram,
    n_seed: Value,
    m_seed: Value,
    guard: Option<Predicate>,
) -> Vec<GraphMatchProgram> {
    let predicate = program.predicate.clone().unwrap();
    let first = GraphMatchProgram {
        steps: vec![
            GraphMatchStep::Node(node("n", Some(n_seed))),
            GraphMatchStep::Expand {
                source: "n".into(),
                relationship: None,
                rel_type: "MENTIONS".into(),
                rel_types: Vec::new(),
                properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                min_hops: 1,
                max_hops: 1,
                target: node("m", None),
            },
        ],
        ..program.clone()
    };
    let second = GraphMatchProgram {
        steps: vec![
            GraphMatchStep::Node(node("m", Some(m_seed))),
            GraphMatchStep::Expand {
                source: "m".into(),
                relationship: None,
                rel_type: "MENTIONS".into(),
                rel_types: Vec::new(),
                properties: BTreeMap::new(),
                direction: RelationshipDirection::Incoming,
                min_hops: 1,
                max_hops: 1,
                target: node("n", None),
            },
        ],
        predicate: Some(match guard {
            Some(guard) => Predicate::And(vec![predicate, guard]),
            None => predicate,
        }),
        ..program.clone()
    };
    vec![first, second]
}

fn run(plan: &PhysicalPlan, limit: ExecutionLimit) -> Vec<BTreeMap<String, Value>> {
    with_context(None, |context| {
        let mut rows = Vec::new();
        execute_binding_batches(plan, context, limit, &mut |batch| {
            rows.extend(batch.into_iter().map(|row| row.values));
            Ok(BatchControl::Continue)
        })
        .unwrap();
        rows
    })
}

fn with_union(n_seed: Value, m_seed: Value, guard: Option<Predicate>) -> PhysicalPlan {
    let mut plan = plan();
    let program = program(&mut plan);
    program.seeded_union = branches(program, n_seed, m_seed, guard);
    plan
}

#[test]
fn the_branches_run_instead_of_the_steps() {
    let expected = BTreeMap::from([
        ("n".to_string(), Value::Int(1)),
        ("m".to_string(), Value::Int(2)),
    ]);
    assert_eq!(
        run(&plan(), ExecutionLimit::unlimited()),
        std::slice::from_ref(&expected)
    );
    // The one match satisfies both seeds; the guard keeps it in the first
    // branch only.
    let guarded = with_union(
        Value::Int(1),
        Value::Int(2),
        Some(not_seeded("n", Value::Int(1), true)),
    );
    assert_eq!(
        run(&guarded, ExecutionLimit::unlimited()),
        std::slice::from_ref(&expected)
    );
    let unguarded = with_union(Value::Int(1), Value::Int(2), None);
    assert_eq!(
        run(&unguarded, ExecutionLimit::unlimited()),
        [expected.clone(), expected.clone()]
    );
    // The limit spans the branches.
    let limit = ExecutionLimit {
        output_rows: Some(1),
    };
    assert_eq!(run(&unguarded, limit), [expected]);
}

#[test]
fn a_null_seed_value_matches_nothing_and_guards_nothing() {
    // The first seed's value is NULL, as a cached plan's parameter can be:
    // its branch is empty, so the second branch must keep the match.
    let guarded = with_union(
        Value::Null,
        Value::Int(2),
        Some(not_seeded("n", Value::Null, true)),
    );
    assert_eq!(run(&guarded, ExecutionLimit::unlimited()).len(), 1);
    // Without the NULL-value term the guard is UNKNOWN and drops it.
    let unknown = with_union(
        Value::Null,
        Value::Int(2),
        Some(not_seeded("n", Value::Null, false)),
    );
    assert!(run(&unknown, ExecutionLimit::unlimited()).is_empty());
}
