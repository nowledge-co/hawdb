use super::preparation::prepare_syntax_access_plan;
use super::*;
use crate::relational_sql::{
    compile_relational_statement_sql, RelationalJoinPlanningAttempt, RelationalJoinPlanningStrategy,
};
use crate::Value;
use skein_storage::{RelationalMutationLimits, RelationalOverflowConfig};
use std::num::{NonZeroU64, NonZeroUsize};

mod connected_enumeration;
mod costed_algorithms;
mod cross_join;
mod having;

#[test]
fn candidate_work_has_an_independent_budget_and_checkpoint() {
    let limits = RelationalQueryLimits {
        max_output_rows: 1,
        max_output_payload_bytes: 1,
        max_intermediate_rows: 1,
        max_candidate_work: 1,
        hydration: RelationalHydrationBudget::default(),
        index_read: skein_storage::RelationalIndexReadLimits::default(),
        row_read: skein_storage::RelationalRowPageSnapshotReadLimits::default(),
    };
    let cancellation = skein_core::RuntimeCancellationToken::new();
    let task_context = skein_core::RuntimeTaskContext::without_deadline(cancellation.clone());
    let mut pipeline =
        RelationalPipelineState::new(Some(&task_context), limits, NonZeroUsize::MIN, Vec::new());

    pipeline
        .account_candidate_work()
        .expect("first candidate fits");
    let error = pipeline
        .account_candidate_work()
        .expect_err("second candidate exceeds its separate budget");
    assert!(error.to_string().contains("max_candidate_work 1"));

    let mut cancellable = RelationalPipelineState::new(
        Some(&task_context),
        RelationalQueryLimits {
            max_candidate_work: 2,
            ..limits
        },
        NonZeroUsize::MIN,
        Vec::new(),
    );
    cancellation.cancel();
    let error = cancellable
        .account_candidate_work()
        .expect_err("candidate work must reach a runtime checkpoint");
    assert!(error
        .to_string()
        .contains("runtime task stopped: cancelled"));
}

fn batched_index_join_state() -> RelationalState {
    let mut state = RelationalState::default();
    for sql in [
            "CREATE TABLE batch_outer (id TEXT PRIMARY KEY, join_key TEXT)",
            "CREATE TABLE batch_inner (id TEXT PRIMARY KEY, join_key TEXT NOT NULL, value TEXT NOT NULL)",
            "CREATE INDEX idx_batch_inner_join_key ON batch_inner (join_key)",
            "INSERT INTO batch_outer (id, join_key) VALUES ('outer-1', 'shared'), ('outer-2', 'shared'), ('outer-3', 'solo'), ('outer-4', NULL)",
            "INSERT INTO batch_inner (id, join_key, value) VALUES ('inner-1', 'shared', 'first'), ('inner-2', 'shared', 'second'), ('inner-3', 'solo', 'only')",
        ] {
            let transaction = compile_relational_statement_sql(sql, &[], &state)
                .unwrap_or_else(|error| panic!("failed to compile SQL '{sql}': {error}"));
            state = state
                .stage_transaction(
                    transaction,
                    RelationalMutationLimits::default(),
                    RelationalOverflowConfig::default(),
                )
                .unwrap_or_else(|error| panic!("failed to apply SQL '{sql}': {error}"));
        }
    state
}

fn batched_index_join_limits() -> RelationalQueryLimits {
    RelationalQueryLimits {
        max_output_rows: 16,
        max_output_payload_bytes: 64 * 1024,
        max_intermediate_rows: 128,
        max_candidate_work: 128,
        hydration: RelationalHydrationBudget::default(),
        index_read: skein_storage::RelationalIndexReadLimits::default(),
        row_read: skein_storage::RelationalRowPageSnapshotReadLimits::default(),
    }
}

fn prepare_batched_index_join(state: &RelationalState) -> PreparedRelationalSelect {
    let prepared_sql = skein_sql::prepare_postgres_sql(
        "SELECT o.id AS outer_id, i.id AS inner_id \
             FROM batch_outer AS o \
             LEFT JOIN batch_inner AS i \
             ON i.join_key = o.join_key AND o.id <> 'outer-2'",
    )
    .expect("valid batched index join SELECT");
    let SqlStatement::Select(select) = prepared_sql.statement else {
        panic!("expected SELECT statement");
    };
    prepare_relational_select(
        select,
        &[],
        state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::Materialized,
            RelationalRowReadMode::CanonicalMemory,
        ),
        batched_index_join_limits(),
        RelationalJoinPlanningContext::default(),
        RelationalSqlStageTimings::default(),
    )
    .expect("prepare batched index join")
}

fn merge_join_state() -> RelationalState {
    let mut state = RelationalState::default();
    for sql in [
            "CREATE TABLE merge_left (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, join_key TEXT NOT NULL)",
            "CREATE TABLE merge_right (id TEXT PRIMARY KEY, join_key TEXT NOT NULL, value TEXT NOT NULL)",
            "CREATE INDEX idx_merge_left_tenant_key ON merge_left (tenant, join_key)",
            "CREATE INDEX idx_merge_right_key ON merge_right (join_key)",
            "INSERT INTO merge_left (id, tenant, join_key) VALUES ('left-b', 'tenant-1', 'b'), ('left-a', 'tenant-1', 'a'), ('left-other', 'tenant-2', 'a')",
            "INSERT INTO merge_right (id, join_key, value) VALUES ('right-a', 'a', 'first'), ('right-b-1', 'b', 'second'), ('right-b-2', 'b', 'third')",
        ] {
            let transaction = compile_relational_statement_sql(sql, &[], &state)
                .unwrap_or_else(|error| panic!("failed to compile SQL '{sql}': {error}"));
            state = state
                .stage_transaction(
                    transaction,
                    RelationalMutationLimits::default(),
                    RelationalOverflowConfig::default(),
                )
                .unwrap_or_else(|error| panic!("failed to apply SQL '{sql}': {error}"));
        }
    state
}

fn prepare_merge_join(state: &RelationalState) -> PreparedRelationalSelect {
    prepare_merge_join_with_index_read_mode(state, RelationalIndexReadMode::Materialized)
}

fn prepare_merge_join_with_index_read_mode(
    state: &RelationalState,
    index_read_mode: RelationalIndexReadMode<'_>,
) -> PreparedRelationalSelect {
    let prepared_sql = skein_sql::prepare_postgres_sql(
        "SELECT l.id AS left_id, r.id AS right_id \
             FROM merge_left AS l \
             INNER JOIN merge_right AS r ON r.join_key = l.join_key \
             WHERE l.tenant = 'tenant-1'",
    )
    .expect("valid merge join SELECT");
    let SqlStatement::Select(select) = prepared_sql.statement else {
        panic!("expected SELECT statement");
    };
    prepare_relational_select(
        select,
        &[],
        state,
        RelationalQueryReadModes::new(index_read_mode, RelationalRowReadMode::CanonicalMemory),
        batched_index_join_limits(),
        RelationalJoinPlanningContext::new(
            RelationalJoinEnumerationConfig::default(),
            RelationalJoinPlanningDirective::SyntaxOrder,
        ),
        RelationalSqlStageTimings::default(),
    )
    .expect("prepare merge join")
}

fn hash_join_state() -> RelationalState {
    let mut state = RelationalState::default();
    for sql in [
            "CREATE TABLE hash_left (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, join_key TEXT, tag TEXT NOT NULL)",
            "CREATE TABLE hash_right (id TEXT PRIMARY KEY, join_key TEXT, tag TEXT NOT NULL, value TEXT NOT NULL)",
            "CREATE INDEX idx_hash_left_tenant ON hash_left (tenant)",
            "INSERT INTO hash_left (id, tenant, join_key, tag) VALUES ('left-b', 'tenant-1', 'b', 'gold'), ('left-a', 'tenant-1', 'a', 'silver'), ('left-null', 'tenant-1', NULL, 'silver'), ('left-other', 'tenant-2', 'a', 'silver')",
            "INSERT INTO hash_right (id, join_key, tag, value) VALUES ('right-a', 'a', 'silver', 'keep'), ('right-b-1', 'b', 'gold', 'keep'), ('right-b-2', 'b', 'gold', 'keep'), ('right-b-skip', 'b', 'gold', 'skip'), ('right-null', NULL, 'silver', 'keep')",
        ] {
            let transaction = compile_relational_statement_sql(sql, &[], &state)
                .unwrap_or_else(|error| panic!("failed to compile SQL '{sql}': {error}"));
            state = state
                .stage_transaction(
                    transaction,
                    RelationalMutationLimits::default(),
                    RelationalOverflowConfig::default(),
                )
                .unwrap_or_else(|error| panic!("failed to apply SQL '{sql}': {error}"));
        }
    state
}

fn prepare_hash_join(state: &RelationalState) -> PreparedRelationalSelect {
    let prepared_sql = skein_sql::prepare_postgres_sql(
        "SELECT l.id AS left_id, r.id AS right_id \
             FROM hash_left AS l \
             INNER JOIN hash_right AS r \
             ON r.join_key = l.join_key AND r.tag = l.tag AND r.value = 'keep' \
             WHERE l.tenant = 'tenant-1'",
    )
    .expect("valid hash join SELECT");
    let SqlStatement::Select(select) = prepared_sql.statement else {
        panic!("expected SELECT statement");
    };
    prepare_relational_select(
        select,
        &[],
        state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::Materialized,
            RelationalRowReadMode::CanonicalMemory,
        ),
        batched_index_join_limits(),
        RelationalJoinPlanningContext::default(),
        RelationalSqlStageTimings::default(),
    )
    .expect("prepare hash join")
}

fn prepare_hash_left_join(state: &RelationalState) -> PreparedRelationalSelect {
    let prepared_sql = skein_sql::prepare_postgres_sql(
        "SELECT l.id AS left_id, r.id AS right_id \
             FROM hash_left AS l \
             LEFT JOIN hash_right AS r \
             ON r.join_key = l.join_key AND r.tag = l.tag AND r.value = 'keep' \
             WHERE l.tenant = 'tenant-1'",
    )
    .expect("valid hash left join SELECT");
    let SqlStatement::Select(select) = prepared_sql.statement else {
        panic!("expected SELECT statement");
    };
    prepare_relational_select(
        select,
        &[],
        state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::Materialized,
            RelationalRowReadMode::CanonicalMemory,
        ),
        batched_index_join_limits(),
        RelationalJoinPlanningContext::default(),
        RelationalSqlStageTimings::default(),
    )
    .expect("prepare hash left join")
}

fn constrained_hash_join_memory() -> skein_executor::ExecutionMemoryConfig {
    skein_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(512).expect("non-zero blocking budget"),
        max_spill_bytes: NonZeroU64::new(64 * 1024).expect("non-zero spill budget"),
        max_spill_runs: NonZeroUsize::new(4).expect("non-zero spill run budget"),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: std::env::temp_dir().join(format!(
            "skein-hash-join-spill-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        )),
        ..skein_executor::ExecutionMemoryConfig::default()
    }
}

#[test]
fn explain_estimated_rows_never_render_zero() {
    assert_eq!(
        optional_estimated_rows_explain_value(Some(0)),
        Value::Int(1)
    );
    assert_eq!(optional_estimated_rows_explain_value(None), Value::Null);
    assert_eq!(optional_usize_explain_value(Some(0)), Value::Int(0));
}

#[test]
fn relational_ledger_uses_admitted_memory_with_configured_fallback() {
    let state = RelationalState::default();
    let memory = skein_executor::ExecutionMemoryConfig::default();
    let descriptor = PreparedRelationalExecutionDescriptor {
        mode: PreparedRelationalExecutionMode::StreamingProjection,
        memory_shape: RelationalExecutionMemoryShape {
            pipeline_batch_count: 1,
            blocking_operator_count: 0,
        },
    };
    let admitted_bytes = 32 * 1024 * 1024;
    let task_context = skein_core::RuntimeTaskContext::default().with_memory_reservation(
        skein_core::RuntimeMemoryReservation::new(admitted_bytes, 1024),
    );
    let read_modes = RelationalQueryReadModes::new(
        RelationalIndexReadMode::Materialized,
        RelationalRowReadMode::CanonicalMemory,
    );

    let governed = descriptor
        .admit(
            &state,
            read_modes,
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                Some(&task_context),
            ),
        )
        .unwrap();
    let ungoverned = descriptor
        .admit(
            &state,
            read_modes,
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                None,
            ),
        )
        .unwrap();

    assert_eq!(
        governed.memory_ledger.snapshot().budget_bytes,
        usize::try_from(admitted_bytes).unwrap()
    );
    assert_eq!(
        ungoverned.memory_ledger.snapshot().budget_bytes,
        memory.query_memory_bytes.get()
    );

    let undersized_context = skein_core::RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(1, 1));
    let error = descriptor
        .admit(
            &state,
            read_modes,
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                Some(&undersized_context),
            ),
        )
        .err()
        .expect("undersized runtime admission must fail closed");
    assert!(
        error.to_string().contains("exceeding query_memory_bytes 1"),
        "{error}"
    );
}

#[test]
fn physical_join_plan_rejects_output_schema_drift() {
    let full_scan = || RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::FullScan,
        name: "__full_scan".to_string(),
        index_columns: Vec::new(),
        access_columns: BTreeSet::new(),
        equality_prefix_len: 0,
        order_prefix_len: 0,
        exclusive_range: false,
        reverse_order: false,
        unique_point: false,
        covering: false,
        requires_row_fetch: false,
        estimated_rows: 1,
    };
    let join_predicate = match skein_sql::prepare_postgres_sql(
            "SELECT left_table.id FROM left_table INNER JOIN right_table ON left_table.id = right_table.id",
        )
        .expect("parse join predicate")
        .statement
        {
            SqlStatement::Select(select) => select
                .joins
                .into_iter()
                .next()
                .expect("join")
                .on,
            _ => unreachable!("join test must parse as a SELECT"),
        };
    let left = RelationalPhysicalJoinNode::relation(
        BindingId::new(0),
        "left_table".to_string(),
        "left_table".to_string(),
        RelationalPhysicalAccess::Base(RelationalAccessCandidate {
            descriptor: full_scan(),
            access: RelationalBaseAccess::FullScan,
        }),
    );
    let right = RelationalPhysicalJoinNode::relation(
        BindingId::new(1),
        "right_table".to_string(),
        "right_table".to_string(),
        RelationalPhysicalAccess::Probe(RelationalJoinAccessCandidate {
            descriptor: full_scan(),
            access: RelationalJoinAccess::FullScan,
        }),
    );
    let mut plan = RelationalPhysicalJoinPlan::new(
        RelationalPhysicalJoinNode::join(
            RelationalOperatorId::from_plan_index(1),
            SqlJoinKind::Inner,
            vec![join_predicate],
            left,
            right,
        )
        .expect("build physical join"),
        estimate_relational_access_cost(1),
    );
    let RelationalPhysicalJoinNode::Join { output_schema, .. } = &mut plan.root else {
        panic!("expected physical join root");
    };
    *output_schema =
        RelationalPhysicalOutputSchema::relation(BindingId::new(0), "left_table", "left_table");

    let error = plan
        .validate()
        .expect_err("schema drift must fail closed before execution");
    assert!(error.to_string().contains("inconsistent output schema"));
}

#[test]
fn prepared_index_join_uses_batched_physical_operator_and_profile() {
    let state = batched_index_join_state();
    let prepared = prepare_batched_index_join(&state);
    let RelationalPhysicalJoinNode::Join { algorithm, .. } = &prepared
        .access_plan
        .physical_join_plan()
        .expect("physical join plan")
        .root
    else {
        panic!("expected physical join root");
    };
    assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::BatchedIndex);
    let profiles =
        planned_operator_cardinality_profiles(&prepared).expect("physical operator profiles");
    assert_eq!(
        profiles[1].operator,
        RelationalOperatorKind::BatchedIndexNestedLoopLeftJoin
    );
}

#[test]
fn batched_index_join_preserves_duplicate_probe_keys_and_left_join_nulls() {
    let state = batched_index_join_state();
    let prepared = prepare_batched_index_join(&state);
    let memory = skein_executor::ExecutionMemoryConfig::default();
    let execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                None,
            ),
        )
        .expect("admit batched index join");
    let output = execute_select(&prepared, &[], execution).expect("execute batched index join");

    assert_eq!(output.rows.len(), 5);
    assert_eq!(
        output.rows[0]["outer_id"],
        Value::String("outer-1".to_string())
    );
    assert_eq!(
        output.rows[0]["inner_id"],
        Value::String("inner-1".to_string())
    );
    assert_eq!(
        output.rows[1]["outer_id"],
        Value::String("outer-1".to_string())
    );
    assert_eq!(
        output.rows[1]["inner_id"],
        Value::String("inner-2".to_string())
    );
    assert_eq!(
        output.rows[2]["outer_id"],
        Value::String("outer-2".to_string())
    );
    assert_eq!(output.rows[2]["inner_id"], Value::Null);
    assert_eq!(
        output.rows[3]["outer_id"],
        Value::String("outer-3".to_string())
    );
    assert_eq!(
        output.rows[3]["inner_id"],
        Value::String("inner-3".to_string())
    );
    assert_eq!(
        output.rows[4]["outer_id"],
        Value::String("outer-4".to_string())
    );
    assert_eq!(output.rows[4]["inner_id"], Value::Null);
    assert_eq!(
        output.operator_cardinality_profiles[1].operator,
        RelationalOperatorKind::BatchedIndexNestedLoopLeftJoin
    );
    assert_eq!(output.operator_cardinality_profiles[1].actual_rows, Some(5));
}

#[test]
fn batched_index_join_rejects_an_input_row_larger_than_its_batch_budget() {
    let state = batched_index_join_state();
    let prepared = prepare_batched_index_join(&state);
    let memory = skein_executor::ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(1).expect("non-zero batch budget"),
        ..skein_executor::ExecutionMemoryConfig::default()
    };
    let execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                None,
            ),
        )
        .expect("admit constrained batched index join");
    let error = execute_select(&prepared, &[], execution)
        .expect_err("batched index join must enforce its batch budget");
    assert!(error
        .to_string()
        .contains("RelationalBatchedIndexJoin input row exceeds batch_payload_bytes 1"));
}

#[test]
fn prepared_index_ordered_join_uses_merge_operator_and_profile() {
    let state = merge_join_state();
    let prepared = prepare_merge_join(&state);
    let RelationalPhysicalJoinNode::Join {
        algorithm,
        equi_join_keys,
        ..
    } = &prepared
        .access_plan
        .physical_join_plan()
        .expect("physical join plan")
        .root
    else {
        panic!("expected physical join root");
    };
    assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::Merge);
    assert_eq!(
        equi_join_keys
            .as_ref()
            .expect("merge key contract")
            .columns
            .iter()
            .map(|(right, left)| (right.as_str(), left.name.as_str()))
            .collect::<Vec<_>>(),
        [("join_key", "join_key")]
    );
    let profiles =
        planned_operator_cardinality_profiles(&prepared).expect("physical operator profiles");
    assert_eq!(profiles[1].operator, RelationalOperatorKind::MergeJoin);
}

#[test]
fn transaction_workspace_join_keeps_the_batched_index_probe_plan() {
    let state = merge_join_state();
    let prepared = prepare_merge_join_with_index_read_mode(
        &state,
        RelationalIndexReadMode::TransactionWorkspace,
    );
    let RelationalPhysicalJoinNode::Join { algorithm, .. } = &prepared
        .access_plan
        .physical_join_plan()
        .expect("physical join plan")
        .root
    else {
        panic!("expected physical join root");
    };
    assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::BatchedIndex);
}

#[test]
fn merge_join_reuses_right_key_groups_and_preserves_left_index_order() {
    let state = merge_join_state();
    let prepared = prepare_merge_join(&state);
    let memory = skein_executor::ExecutionMemoryConfig::default();
    let execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                None,
            ),
        )
        .expect("admit merge join");
    let output = execute_select(&prepared, &[], execution).expect("execute merge join");

    assert_eq!(output.rows.len(), 3);
    assert_eq!(
        output.rows[0]["left_id"],
        Value::String("left-a".to_string())
    );
    assert_eq!(
        output.rows[0]["right_id"],
        Value::String("right-a".to_string())
    );
    assert_eq!(
        output.rows[1]["left_id"],
        Value::String("left-b".to_string())
    );
    assert_eq!(
        output.rows[1]["right_id"],
        Value::String("right-b-1".to_string())
    );
    assert_eq!(
        output.rows[2]["left_id"],
        Value::String("left-b".to_string())
    );
    assert_eq!(
        output.rows[2]["right_id"],
        Value::String("right-b-2".to_string())
    );
    assert_eq!(
        output.operator_cardinality_profiles[1].operator,
        RelationalOperatorKind::MergeJoin
    );
    assert_eq!(output.operator_cardinality_profiles[1].actual_rows, Some(3));
    assert!(output
        .blocking_operator_memory_reports
        .iter()
        .any(|report| report.operator == "RelationalMergeJoinRightInput"));
}

#[test]
fn merge_join_rejects_right_input_that_exceeds_its_blocking_budget() {
    let state = merge_join_state();
    let prepared = prepare_merge_join(&state);
    let memory = skein_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(1).expect("non-zero blocking budget"),
        ..skein_executor::ExecutionMemoryConfig::default()
    };
    let execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                None,
            ),
        )
        .expect("admit constrained merge join");
    let error = execute_select(&prepared, &[], execution)
        .expect_err("merge join must enforce its blocking budget");
    assert!(error
        .to_string()
        .contains("RelationalMergeJoinRightInput state exceeds blocking_operator_bytes 1"));
}

#[test]
fn prepared_full_scan_equi_join_uses_hash_operator_and_profile() {
    let state = hash_join_state();
    let prepared = prepare_hash_join(&state);
    let RelationalPhysicalJoinNode::Join {
        algorithm,
        equi_join_keys,
        ..
    } = &prepared
        .access_plan
        .physical_join_plan()
        .expect("physical join plan")
        .root
    else {
        panic!("expected physical join root");
    };
    assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::Hash);
    assert_eq!(
        equi_join_keys
            .as_ref()
            .expect("hash key contract")
            .columns
            .iter()
            .map(|(right, left)| (right.as_str(), left.name.as_str()))
            .collect::<Vec<_>>(),
        [("join_key", "join_key"), ("tag", "tag")]
    );
    let profiles =
        planned_operator_cardinality_profiles(&prepared).expect("physical operator profiles");
    assert_eq!(profiles[1].operator, RelationalOperatorKind::HashJoin);
    assert_eq!(
        profiles[1].access_path.kind,
        RelationalAccessPathKind::FullScan
    );
}

#[test]
fn hash_join_preserves_duplicate_build_rows_and_evaluates_full_on_predicates() {
    let state = hash_join_state();
    let prepared = prepare_hash_join(&state);
    let memory = skein_executor::ExecutionMemoryConfig::default();
    let execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                None,
            ),
        )
        .expect("admit hash join");
    let output = execute_select(&prepared, &[], execution).expect("execute hash join");

    assert_eq!(output.rows.len(), 3);
    assert_eq!(
        output.rows[0]["left_id"],
        Value::String("left-a".to_string())
    );
    assert_eq!(
        output.rows[0]["right_id"],
        Value::String("right-a".to_string())
    );
    assert_eq!(
        output.rows[1]["left_id"],
        Value::String("left-b".to_string())
    );
    assert_eq!(
        output.rows[1]["right_id"],
        Value::String("right-b-1".to_string())
    );
    assert_eq!(
        output.rows[2]["right_id"],
        Value::String("right-b-2".to_string())
    );
    assert_eq!(
        output.operator_cardinality_profiles[1].operator,
        RelationalOperatorKind::HashJoin
    );
    assert_eq!(output.operator_cardinality_profiles[1].actual_rows, Some(3));
    assert!(output
        .blocking_operator_memory_reports
        .iter()
        .any(|report| report.operator == "RelationalHashJoinBuild"));
}

#[test]
fn hash_join_spills_and_falls_back_for_a_hot_partition() {
    let state = hash_join_state();
    let prepared = prepare_hash_join(&state);
    let memory = constrained_hash_join_memory();
    let execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                None,
            ),
        )
        .expect("admit constrained hash join");
    let output = execute_select(&prepared, &[], execution).expect("spill-backed hash join");
    let mut rows = output
        .rows
        .iter()
        .map(|row| (row["left_id"].clone(), row["right_id"].clone()))
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (
                Value::String("left-a".to_string()),
                Value::String("right-a".to_string()),
            ),
            (
                Value::String("left-b".to_string()),
                Value::String("right-b-1".to_string()),
            ),
            (
                Value::String("left-b".to_string()),
                Value::String("right-b-2".to_string()),
            ),
        ]
    );
    assert!(output
        .blocking_operator_memory_reports
        .iter()
        .any(|report| {
            report.operator == "RelationalHashJoinGrace"
                && report.spilled_rows > 0
                && report.spill_run_count > 0
        }));
    assert!(output
        .blocking_operator_memory_reports
        .iter()
        .any(|report| report.operator == "RelationalHashJoinGraceHotPartition"));
    std::fs::remove_dir_all(&memory.spill_directory).expect("remove hash join spill fixture");
}

#[test]
fn hash_left_join_null_extends_unmatched_and_null_keys() {
    let state = hash_join_state();
    let prepared = prepare_hash_left_join(&state);
    let RelationalPhysicalJoinNode::Join {
        algorithm, kind, ..
    } = &prepared
        .access_plan
        .physical_join_plan()
        .expect("physical join plan")
        .root
    else {
        panic!("expected physical join root");
    };
    assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::Hash);
    assert_eq!(*kind, SqlJoinKind::Left);

    let memory = skein_executor::ExecutionMemoryConfig::default();
    let execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                None,
            ),
        )
        .expect("admit hash left join");
    let output = execute_select(&prepared, &[], execution).expect("execute hash left join");
    let mut rows = output
        .rows
        .iter()
        .map(|row| (row["left_id"].clone(), row["right_id"].clone()))
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (
                Value::String("left-a".to_string()),
                Value::String("right-a".to_string()),
            ),
            (
                Value::String("left-b".to_string()),
                Value::String("right-b-1".to_string()),
            ),
            (
                Value::String("left-b".to_string()),
                Value::String("right-b-2".to_string()),
            ),
            (Value::String("left-null".to_string()), Value::Null),
        ]
    );
    assert_eq!(
        output.operator_cardinality_profiles[1].operator,
        RelationalOperatorKind::HashJoin
    );
}

#[test]
fn hash_join_observes_cancellation_after_admission() {
    let state = hash_join_state();
    let prepared = prepare_hash_join(&state);
    let memory = skein_executor::ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::MIN,
        ..skein_executor::ExecutionMemoryConfig::default()
    };
    let cancellation = skein_core::RuntimeCancellationToken::new();
    let task_context = skein_core::RuntimeTaskContext::without_deadline(cancellation.clone());
    let execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                batched_index_join_limits(),
                &memory,
                Some(&task_context),
            ),
        )
        .expect("admit hash join before cancellation");
    assert!(cancellation.cancel());
    let error = execute_select(&prepared, &[], execution)
        .expect_err("cancelled hash join must stop at a bounded checkpoint");
    assert!(error
        .to_string()
        .contains("runtime task stopped: cancelled"));
}

#[test]
fn prepared_bushy_physical_join_plan_materializes_the_composite_right_input_once() {
    const SQL: &str = "SELECT a.id AS a_id, d.id AS d_id \
            FROM bushy_a AS a \
            INNER JOIN bushy_b AS b ON b.a_id = a.id \
            INNER JOIN bushy_c AS c ON c.bridge = b.bridge \
            INNER JOIN bushy_d AS d ON d.c_id = c.id \
            ORDER BY a.id ASC, d.id ASC";

    let mut state = RelationalState::default();
    for sql in [
        "CREATE TABLE bushy_a (id TEXT PRIMARY KEY)",
        "CREATE TABLE bushy_b (id TEXT PRIMARY KEY, a_id TEXT NOT NULL, bridge TEXT NOT NULL)",
        "CREATE TABLE bushy_c (id TEXT PRIMARY KEY, bridge TEXT NOT NULL)",
        "CREATE TABLE bushy_d (id TEXT PRIMARY KEY, c_id TEXT NOT NULL)",
        "INSERT INTO bushy_a (id) VALUES ('a-1'), ('a-2')",
        "INSERT INTO bushy_b (id, a_id, bridge) VALUES ('b-1', 'a-1', 'x'), ('b-2', 'a-2', 'y')",
        "INSERT INTO bushy_c (id, bridge) VALUES ('c-1', 'x'), ('c-2', 'y')",
        "INSERT INTO bushy_d (id, c_id) VALUES ('d-1', 'c-1'), ('d-2', 'c-2')",
    ] {
        let transaction = compile_relational_statement_sql(sql, &[], &state)
            .unwrap_or_else(|error| panic!("failed to compile SQL '{sql}': {error}"));
        state = state
            .stage_transaction(
                transaction,
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
            )
            .unwrap_or_else(|error| panic!("failed to apply SQL '{sql}': {error}"));
    }

    let prepared_sql = skein_sql::prepare_postgres_sql(SQL).expect("valid bushy SELECT");
    let SqlStatement::Select(select) = prepared_sql.statement else {
        panic!("expected SELECT statement");
    };
    let limits = RelationalQueryLimits {
        max_output_rows: 8,
        max_output_payload_bytes: 64 * 1024,
        max_intermediate_rows: 128,
        max_candidate_work: 128,
        hydration: RelationalHydrationBudget::default(),
        index_read: skein_storage::RelationalIndexReadLimits::default(),
        row_read: skein_storage::RelationalRowPageSnapshotReadLimits::default(),
    };
    let mut binding_nanos = 0;
    let planned = join_order::plan_select_join_order(
        select.clone(),
        &[],
        &state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::Materialized,
            RelationalRowReadMode::CanonicalMemory,
        ),
        limits,
        RelationalJoinPlanningContext::default(),
        &mut binding_nanos,
    )
    .expect("plan bushy candidate with bounded materialization enabled");
    let access_plan = planned
        .access_plan
        .expect("eligible bushy candidate retains an access plan");
    assert_eq!(
        access_plan
            .physical_join_plan()
            .expect("eligible bushy candidate has a physical join plan")
            .root
            .materialized_right_count(),
        1
    );

    let mut syntax_plan = prepare_syntax_access_plan(
        &select,
        &[],
        &state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::Materialized,
            RelationalRowReadMode::CanonicalMemory,
        ),
        limits,
    )
    .expect("prepare syntax access plan");
    syntax_plan
        .finalize_physical_join_plan(&select, &state, RelationalIndexReadMode::Materialized)
        .expect("finalize syntax physical join plan");
    let syntax_physical_plan = syntax_plan
        .physical_join_plan()
        .expect("syntax physical join plan");
    assert_eq!(syntax_physical_plan.root.materialized_right_count(), 0);
    assert!(matches!(
        syntax_physical_plan.root,
        RelationalPhysicalJoinNode::Join {
            algorithm: RelationalPhysicalJoinAlgorithm::Probe,
            ..
        }
    ));
    assert_eq!(
        syntax_physical_plan
            .output_schema
            .bindings
            .iter()
            .map(|binding| (
                binding.binding.get(),
                binding.table.as_str(),
                binding.qualifier.as_str()
            ))
            .collect::<Vec<_>>(),
        [
            (0, "bushy_a", "a"),
            (1, "bushy_b", "b"),
            (2, "bushy_c", "c"),
            (3, "bushy_d", "d"),
        ]
    );
    let c_schema = state.table_schema("bushy_c").expect("bushy_c schema");
    let c_base = choose_base_access(RelationalBaseAccessPlanning {
        predicate: None,
        order_by: &[],
        prefer_ordered_access: false,
        parameters: &[],
        state: &state,
        schema: c_schema,
        table: "bushy_c",
        qualifier: "c",
        cardinality_limit: limits.max_intermediate_rows.saturating_add(1),
        projection: RelationalProjectionAccessPlanning::default(),
    })
    .expect("prepare bushy_c materialized base access");

    let relation =
        |binding: u32, table: &str, qualifier: &str, access: RelationalPhysicalAccess| {
            RelationalPhysicalJoinNode::relation(
                BindingId::new(binding),
                table.to_string(),
                qualifier.to_string(),
                access,
            )
        };
    let left = RelationalPhysicalJoinNode::join(
        RelationalOperatorId::from_plan_index(1),
        SqlJoinKind::Inner,
        vec![select.joins[0].on.clone()],
        relation(
            0,
            "bushy_a",
            "a",
            RelationalPhysicalAccess::Base(syntax_plan.base_access.clone()),
        ),
        relation(
            1,
            "bushy_b",
            "b",
            RelationalPhysicalAccess::Probe(syntax_plan.join_accesses[0].clone()),
        ),
    )
    .expect("build left physical join");
    let right = RelationalPhysicalJoinNode::join(
        RelationalOperatorId::from_plan_index(2),
        SqlJoinKind::Inner,
        vec![select.joins[2].on.clone()],
        relation(2, "bushy_c", "c", RelationalPhysicalAccess::Base(c_base)),
        relation(
            3,
            "bushy_d",
            "d",
            RelationalPhysicalAccess::Probe(syntax_plan.join_accesses[2].clone()),
        ),
    )
    .expect("build right physical join");
    let left_cost = estimate_relational_probe_join_cost(
        estimate_relational_access_cost(syntax_plan.base_access.descriptor.estimated_rows),
        syntax_plan.join_accesses[0].descriptor.estimated_rows,
        RelationalJoinCardinality::Inner,
    );
    let right_cost = estimate_relational_probe_join_cost(
        estimate_relational_access_cost(right.first_relation().access.descriptor().estimated_rows),
        syntax_plan.join_accesses[2].descriptor.estimated_rows,
        RelationalJoinCardinality::Inner,
    );
    let cost = estimate_relational_join_cost(
        left_cost,
        right_cost,
        RelationalJoinCardinality::Inner,
        RelationalJoinRightInput::Materialized,
        RelationalJoinSelectivity::Unknown,
    );
    let root = RelationalPhysicalJoinNode::join(
        RelationalOperatorId::from_plan_index(3),
        SqlJoinKind::Inner,
        vec![select.joins[1].on.clone()],
        left,
        right,
    )
    .expect("build materialized physical join");
    let mut access_plan = syntax_plan;
    access_plan.join_selection = None;
    access_plan.physical_join_plan = Some(RelationalPhysicalJoinPlan::new(root, cost));
    let execution = PreparedRelationalExecutionDescriptor::prepare(&select, &access_plan)
        .expect("prepare relational execution descriptor");
    let prepared = PreparedRelationalSelect {
        statement: select,
        access_plan,
        join_planning: RelationalJoinPlanningOutcome::selected(
            RelationalJoinPlanningAttempt::selected(
                RelationalJoinPlanningStrategy::CsgCmpMemo,
                true,
                7,
                8,
                cost,
            ),
            vec!["a".into(), "b".into(), "c".into(), "d".into()],
            RelationalJoinEnumerationConfig::default(),
            Vec::new(),
        ),
        execution,
        stage_timings: RelationalSqlStageTimings::default(),
    };
    prepared.validate().expect("validate prepared bushy plan");
    let physical_plan = prepared
        .access_plan
        .physical_join_plan()
        .expect("materialized physical join plan");
    assert_eq!(physical_plan.root.materialized_right_count(), 1);
    assert!(matches!(
        physical_plan.root,
        RelationalPhysicalJoinNode::Join {
            algorithm: RelationalPhysicalJoinAlgorithm::Materialized,
            ..
        }
    ));
    assert_eq!(
        physical_plan
            .output_schema
            .bindings
            .iter()
            .map(|binding| (binding.binding.get(), binding.qualifier.as_str()))
            .collect::<Vec<_>>(),
        [(0, "a"), (1, "b"), (2, "c"), (3, "d")]
    );

    let memory = skein_executor::ExecutionMemoryConfig::default();
    let resources = RelationalQueryResourceContext::new(
        RelationalJoinEnumerationConfig::default(),
        limits,
        &memory,
        None,
    );
    let admitted = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            resources,
        )
        .expect("admit prepared bushy plan");
    let output = execute_select(&prepared, &[], admitted).expect("execute prepared bushy plan");

    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0]["a_id"], Value::String("a-1".to_string()));
    assert_eq!(output.rows[1]["d_id"], Value::String("d-2".to_string()));
    assert_eq!(
        output.join_planning.strategy,
        RelationalJoinPlanningStrategy::CsgCmpMemo
    );
    assert!(output
        .blocking_operator_memory_reports
        .iter()
        .any(|report| {
            report.operator == "RelationalBushyJoinMaterialize"
                && report.input_rows == 2
                && report.peak_tracked_bytes > 0
        }));

    let constrained_memory = skein_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(64).expect("non-zero memory budget"),
        ..skein_executor::ExecutionMemoryConfig::default()
    };
    let constrained_resources = RelationalQueryResourceContext::new(
        RelationalJoinEnumerationConfig::default(),
        limits,
        &constrained_memory,
        None,
    );
    let constrained_execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            constrained_resources,
        )
        .expect("admit constrained bushy plan");
    let error = execute_select(&prepared, &[], constrained_execution)
        .expect_err("bushy materialization must honor its memory budget");
    assert!(error
        .to_string()
        .contains("RelationalBushyJoinMaterialize state exceeds blocking_operator_bytes"));
}
