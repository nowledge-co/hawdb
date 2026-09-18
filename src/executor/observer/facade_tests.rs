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

use crate::executor::execute_with_row_limit_profile;
use crate::optimizer::PhysicalPlan;
use crate::schema::Catalog;
use crate::store::GraphStore;
use std::collections::BTreeMap;

#[test]
fn root_profile_constructor_preserves_public_types_and_defaults() {
    let profile: crate::executor::ReadExecutionProfile =
        crate::executor::read_execution_profile(&PhysicalPlan::EmptyExec, Some(3)).unwrap();
    let owner: hawdb_executor::ReadExecutionProfile<crate::store::ScanPruningReport> = profile;
    assert_eq!(owner.max_rows, Some(3));
    assert_eq!(owner.detection_row_cap, Some(4));
    assert_eq!(
        owner.pipeline_memory_report,
        hawdb_executor::PipelineMemoryReport::default()
    );
}

#[test]
fn root_profiled_execution_uses_query_local_owner_reports() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for _ in 0..3 {
        store
            .create_node(&mut catalog, "Item", BTreeMap::new())
            .unwrap();
    }
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: "Item".into(),
    };
    for (plan, expected) in [(&scan, 3), (&PhysicalPlan::EmptyExec, 0), (&scan, 3)] {
        let result = execute_with_row_limit_profile(plan, &mut catalog, &mut store, None).unwrap();
        assert_eq!(result.rows.len(), expected);
        let profile = result.profile;
        assert_eq!(profile.pipeline_memory_report.output_rows, expected);
        assert_eq!(profile.operator_cardinality_profiles.len(), 1);
        let operator = &profile.operator_cardinality_profiles[0];
        assert_eq!(operator.operator_id.ordinal(), 0);
        assert_eq!(operator.operator, plan.kind());
        assert_eq!(operator.actual_rows, Some(expected));
    }
}
