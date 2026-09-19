use hawdb::executor::{
    execute_with_request, execute_with_request_consumer, ExecutionMemoryConfig, ExecutionRequest,
    ExecutionResources, NoExternalReadOperator,
};
use hawdb::optimizer::PhysicalPlan;
use hawdb::schema::Catalog;
use hawdb::store::GraphStore;
use hawdb::value::Value;
use std::collections::BTreeMap;

#[test]
fn public_execution_request_materializes_a_profiled_result() {
    let plan = PhysicalPlan::EmptyExec;
    let parameters = BTreeMap::new();
    let memory = ExecutionMemoryConfig::default();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let mut external = NoExternalReadOperator;

    let output = execute_with_request(
        ExecutionRequest::new(&plan, &parameters, &memory).with_output_limits(Some(1), None),
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
    )
    .unwrap();

    assert!(output.rows.is_empty());
    assert_eq!(output.profile.max_rows, Some(1));
}

#[test]
fn public_consumer_does_not_receive_rows_before_limit_validation() {
    let plan = PhysicalPlan::SeqNodeScan {
        variable: "node".to_string(),
        label: "Item".to_string(),
    };
    let parameters = BTreeMap::new();
    let memory = ExecutionMemoryConfig::default();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([("rank".to_string(), Value::Int(1))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([("rank".to_string(), Value::Int(2))]),
        )
        .unwrap();
    let mut external = NoExternalReadOperator;
    let mut delivered = 0;

    let error = execute_with_request_consumer(
        ExecutionRequest::new(&plan, &parameters, &memory).with_output_limits(Some(1), None),
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
        &mut |_| {
            delivered += 1;
            Ok(())
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("exceeding max_read_result_rows"));
    assert_eq!(delivered, 0);
}
