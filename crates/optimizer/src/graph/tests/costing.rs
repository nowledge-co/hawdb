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

use super::super::{
    costing::{estimate_physical_plan_cost, estimate_physical_plan_cost_breakdown},
    OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics,
};
use hawdb_plan::{PhysicalPlan, Predicate, VectorExecutionResourceProfile, VectorPhysicalPlan};
use std::collections::BTreeMap;

fn assert_scalar_cost_matches_breakdown(plan: &PhysicalPlan, catalog: &OptimizerCatalog) {
    assert_eq!(
        estimate_physical_plan_cost(plan, catalog),
        estimate_physical_plan_cost_breakdown(plan, catalog).as_plan_cost()
    );
}

#[test]
fn source_segment_scan_preserves_scan_startup_cost_in_breakdown() {
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::default(),
        OptimizerCatalogStatistics {
            label_counts: BTreeMap::from([("Source".to_string(), 12)]),
            ..OptimizerCatalogStatistics::default()
        },
    );
    let plan = PhysicalPlan::SourceSegmentScan {
        variable: "source".to_string(),
        predicate: Predicate::ConstantBool(true),
    };

    let breakdown = estimate_physical_plan_cost_breakdown(&plan, &catalog);

    assert_eq!(breakdown.estimated_rows, 12);
    assert_eq!(breakdown.random_io, 4);
    assert_eq!(breakdown.sequential_io, 12);
    assert_eq!(breakdown.cost, 20);
    assert_scalar_cost_matches_breakdown(&plan, &catalog);
}

#[test]
fn vector_seed_scan_preserves_total_cost_in_breakdown() {
    let catalog = OptimizerCatalog::default();
    let plan = PhysicalPlan::VectorSeedScan {
        embedding_parameter: "embedding".to_string(),
        output_external_id: true,
        metadata_filters: BTreeMap::new(),
        resource_profile: VectorExecutionResourceProfile {
            priority: 128,
            max_parallelism: 1,
            max_working_memory_bytes: None,
        },
        vector_plan: VectorPhysicalPlan::TopK {
            limit: 8,
            input: Box::new(VectorPhysicalPlan::Filter { fields: Vec::new() }),
        },
    };

    let breakdown = estimate_physical_plan_cost_breakdown(&plan, &catalog);

    assert_eq!(breakdown.estimated_rows, 8);
    assert_eq!(breakdown.cpu, 72);
    assert_eq!(breakdown.output_rows, 8);
    assert_eq!(breakdown.cost, 80);
    assert_scalar_cost_matches_breakdown(&plan, &catalog);
}

#[test]
fn recursive_plan_scalar_cost_is_the_breakdown_projection() {
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::default(),
        OptimizerCatalogStatistics {
            label_counts: BTreeMap::from([("Memory".to_string(), 32)]),
            ..OptimizerCatalogStatistics::default()
        },
    );
    let plan = PhysicalPlan::LimitExec {
        offset: 3,
        limit: Some(5),
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::ConstantBool(true),
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "memory".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };

    assert_scalar_cost_matches_breakdown(&plan, &catalog);
}
