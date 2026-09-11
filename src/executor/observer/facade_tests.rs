use crate::executor::execute_with_row_limit_profile;
use crate::optimizer::PhysicalPlan;
use crate::schema::Catalog;
use crate::store::GraphStore;
use std::collections::BTreeMap;

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
