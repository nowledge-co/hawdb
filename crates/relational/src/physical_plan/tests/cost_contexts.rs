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

use super::*;
use hawdb_optimizer::RelationalAccessCostContext;
use std::num::NonZeroU64;

fn snapshot(rows: u64, pages: u64) -> RelationalAccessCostContext {
    RelationalAccessCostContext::for_snapshot_rows(
        NonZeroU64::new(rows).unwrap(),
        NonZeroU64::new(pages).unwrap(),
    )
}

fn contexts() -> RelationalJoinCostContexts {
    RelationalJoinCostContexts::default()
        .with_relation(BindingId::new(0), snapshot(3, 2))
        .with_relation(BindingId::new(1), snapshot(2, 1))
}

#[test]
fn syntax_hash_merge_and_probe_costs_match_context_aware_prepared_profiles() {
    let state = state();
    let reader = Reader::default();
    let contexts = contexts();
    for (mode_index, mode) in modes(&reader).into_iter().enumerate() {
        for indexed in [false, true] {
            let mut plan = access_plan(indexed);
            plan.finalize_physical_join_plan_with_cost_contexts(
                &statement(),
                &state,
                mode,
                &contexts,
            )
            .unwrap();
            // Snapshot scan CPU: L7+6, R6+3. Point bounds take two steps
            // for two pages and one for one page, plus two artifact opens:
            // noncovering index CPU L6+12, R4+6, random L4+18/R3+6.
            // Scans add three ordered metadata accesses per page. Hash's one
            // estimated match also re-fetches L(CPU5/random7) and R(4/4)
            // from their locators. A probe pays every raw right-input
            // component three times; merge retains its projected rows.
            let (algorithm, expected) = if !indexed {
                (
                    RelationalPhysicalJoinAlgorithm::Hash,
                    PlanCostBreakdown::new(1, 39, 11, 14, 5),
                )
            } else if mode_index < 2 {
                (
                    RelationalPhysicalJoinAlgorithm::Merge,
                    PlanCostBreakdown::new(1, 34, 31, 5, 5),
                )
            } else {
                (
                    RelationalPhysicalJoinAlgorithm::BatchedIndex,
                    PlanCostBreakdown::new(6, 48, 49, 9, 9),
                )
            };
            let tree = plan.physical_join_plan().unwrap();
            let RelationalPhysicalJoinNode::Join {
                algorithm: actual, ..
            } = &tree.root
            else {
                panic!("expected join");
            };
            assert_eq!(*actual, algorithm);
            assert_eq!(tree.cost_breakdown, expected);
            let prepared = prepared(statement(), plan);
            prepared.validate_with_cost_contexts(&contexts).unwrap();
            assert!(error(prepared.validate()).contains("estimates diverge"));
            let profiles =
                planned_operator_cardinality_profiles_with_cost_contexts(&prepared, &contexts)
                    .unwrap();
            assert_eq!(profiles.len(), 2);
            assert_eq!(profiles[0].estimated_rows, 3);
            assert_eq!(profiles[1].estimated_rows, expected.estimated_rows as usize);
        }
    }
}

#[test]
fn coverage_recomposition_keeps_context_and_removes_only_covering_fetch_work() {
    let state = state();
    let contexts = contexts();
    for (sql, covering, expected) in [
        (
            "SELECT k FROM l",
            true,
            PlanCostBreakdown::new(3, 3, 1, 3, 3),
        ),
        (
            "SELECT n FROM l",
            false,
            PlanCostBreakdown::new(3, 18, 22, 3, 3),
        ),
    ] {
        let statement = select(sql);
        let fields = crate::field_plan::plan_relational_field_plan(&statement, &state).unwrap();
        let mut plan = PreparedRelationalAccessPlan {
            join_accesses: Vec::new(),
            ..access_plan(true)
        };
        plan.finalize_physical_join_plan_with_cost_contexts(
            &statement,
            &state,
            Mode::Materialized,
            &contexts,
        )
        .unwrap();
        assert_eq!(
            plan.physical_join_plan().unwrap().cost_breakdown,
            PlanCostBreakdown::new(3, 18, 22, 3, 3)
        );
        plan.apply_physical_index_coverage_with_cost_contexts(&state, &fields, &contexts)
            .unwrap();
        let tree = plan.physical_join_plan().unwrap();
        assert_eq!(
            tree.root.first_relation().access.descriptor().covering,
            covering
        );
        assert_eq!(tree.cost_breakdown, expected);
        let prepared = prepared(statement, plan);
        prepared.validate_with_cost_contexts(&contexts).unwrap();
        let profiles =
            planned_operator_cardinality_profiles_with_cost_contexts(&prepared, &contexts).unwrap();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].estimated_rows, 3);
    }
}

#[test]
fn recursive_physical_cost_walk_retains_each_binding_context() {
    let relation = |binding, table: &str, rows, is_probe| {
        RelationalPhysicalJoinNode::relation(
            BindingId::new(binding),
            table.into(),
            table.into(),
            if is_probe {
                RelationalPhysicalAccess::Probe(probe(rows, false))
            } else {
                RelationalPhysicalAccess::Base(base(rows, false))
            },
        )
    };
    let right = RelationalPhysicalJoinNode::join(
        RelationalOperatorId::from_plan_index(1),
        SqlJoinKind::Inner,
        Vec::new(),
        relation(1, "r", 2, false),
        relation(2, "c", 4, true),
    )
    .unwrap();
    let root = RelationalPhysicalJoinNode::join(
        RelationalOperatorId::from_plan_index(2),
        SqlJoinKind::Inner,
        Vec::new(),
        relation(0, "l", 3, false),
        right,
    )
    .unwrap();
    let contexts = contexts().with_relation(BindingId::new(2), snapshot(4, 2));
    // Right scan probe CPU9+2*14=37/sequential5+2*10=25. The root pays
    // its sequential9 once and materializes the complete right subtree once,
    // plus 3*8 pair operations. Cardinality ceil(3*8/10) differs from output13.
    let tree = RelationalPhysicalJoinPlan::new(root, PlanCostBreakdown::new(3, 74, 0, 34, 13));
    tree.validate().unwrap();
    let profiles =
        planned_tree_operator_cardinality_profiles_with_cost_contexts(&tree, &contexts).unwrap();
    assert_eq!(profiles.len(), 3);
    assert_eq!(profiles[1].estimated_rows, 8);
    assert_eq!(profiles[2].estimated_rows, 3);
    assert!(error(
        planned_tree_operator_cardinality_profiles_with_cost_contexts(
            &tree,
            &contexts.with_relation(BindingId::new(2), RelationalAccessCostContext::default())
        )
    )
    .contains("estimates diverge"));
}

#[test]
fn empty_physical_contexts_preserve_legacy_costs_profiles_and_prepared_validation() {
    let state = state();
    let reader = Reader::default();
    let empty = RelationalJoinCostContexts::default();
    for mode in modes(&reader) {
        for indexed in [false, true] {
            let mut legacy = access_plan(indexed);
            legacy
                .finalize_physical_join_plan(&statement(), &state, mode)
                .unwrap();
            let mut explicit = access_plan(indexed);
            explicit
                .finalize_physical_join_plan_with_cost_contexts(&statement(), &state, mode, &empty)
                .unwrap();
            let fields =
                crate::field_plan::plan_relational_field_plan(&statement(), &state).unwrap();
            legacy
                .apply_physical_index_coverage(&state, &fields)
                .unwrap();
            explicit
                .apply_physical_index_coverage_with_cost_contexts(&state, &fields, &empty)
                .unwrap();
            assert_eq!(
                format!("{:?}", legacy.physical_join_plan().unwrap()),
                format!("{:?}", explicit.physical_join_plan().unwrap())
            );
            let prepared = prepared(statement(), explicit);
            prepared.validate().unwrap();
            prepared.validate_with_cost_contexts(&empty).unwrap();
            assert_eq!(
                planned_operator_cardinality_profiles(&prepared).unwrap(),
                planned_operator_cardinality_profiles_with_cost_contexts(&prepared, &empty)
                    .unwrap()
            );
        }
    }
}
