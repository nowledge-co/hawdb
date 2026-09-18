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

use super::{
    bound_join_key, bound_relation_join_key, bound_row_resident_bytes, predicate_truth,
    relational_key_resident_bytes, visit_prepared_physical_join_plan_node, BindingId, BoundRow,
    HawDBError, OperatorMemoryTracker, QueryMemoryClass, RefCell, RelationalEquiJoinKeys,
    RelationalIndexRuntime, RelationalKey, RelationalOperatorId, RelationalPhysicalJoinExecution,
    RelationalPhysicalJoinNode, RelationalPhysicalOutputSchema, RelationalPipelineState,
    RelationalRowRuntime, RelationalState, Result, SqlPredicate, Value,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn visit_index_merge_join<'a>(
    operator_id: RelationalOperatorId,
    predicates: &[SqlPredicate],
    equi_join_keys: &RelationalEquiJoinKeys,
    left: &'a RelationalPhysicalJoinNode,
    right: &'a RelationalPhysicalJoinNode,
    output_schema: &RelationalPhysicalOutputSchema,
    outer: Option<&BoundRow<'a>>,
    parameters: &[Value],
    state: &'a RelationalState,
    profiled_base_binding: BindingId,
    execution: &RelationalPhysicalJoinExecution<'a>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    index_runtime: &RelationalIndexRuntime<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    if outer.is_some() {
        return Err(HawDBError::Execution(
            "merge join cannot run below a probe input".to_string(),
        ));
    }
    let (
        RelationalPhysicalJoinNode::Relation(_left_relation),
        RelationalPhysicalJoinNode::Relation(right_relation),
    ) = (left, right)
    else {
        return Err(HawDBError::Execution(
            "merge join requires two relation inputs".to_string(),
        ));
    };
    let right_schema = state.table_schema(&right_relation.table).ok_or_else(|| {
        HawDBError::Semantic(format!("unknown relational table {}", right_relation.table))
    })?;
    let mut right_rows = Vec::new();
    let mut right_tracker = OperatorMemoryTracker::with_account(
        execution.memory.blocking_operator_bytes,
        execution.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "RelationalMergeJoinRightInput",
            execution.memory.blocking_operator_bytes,
        ),
    );
    let mut previous_right_key = None;
    visit_prepared_physical_join_plan_node(
        right,
        None,
        parameters,
        state,
        profiled_base_binding,
        execution,
        pipeline,
        index_runtime,
        row_runtime,
        &mut |row| {
            let Some(key) = bound_relation_join_key(&row, right_relation, equi_join_keys)? else {
                return Ok(true);
            };
            if previous_right_key
                .as_ref()
                .is_some_and(|previous| key < *previous)
            {
                return Err(HawDBError::Execution(
                    "merge join right input violates its declared key order".to_string(),
                ));
            }
            previous_right_key = Some(key.clone());
            let bytes = bound_row_resident_bytes(&row)
                .saturating_add(relational_key_resident_bytes(&key))
                .saturating_add(std::mem::size_of::<(RelationalKey, BoundRow<'_>)>());
            if right_tracker.would_exceed(bytes) {
                return Err(HawDBError::Execution(format!(
                    "RelationalMergeJoinRightInput state exceeds blocking_operator_bytes {}",
                    execution.memory.blocking_operator_bytes
                )));
            }
            right_tracker.try_charge(bytes)?;
            right_rows.push((key, row));
            Ok(true)
        },
    )?;
    execution
        .reports
        .borrow_mut()
        .push(hawdb_executor::blocking::in_memory_report(
            "RelationalMergeJoinRightInput",
            &right_tracker,
            right_tracker.peak_bytes,
            right_rows.len(),
            execution.memory,
        ));

    let mut previous_left_key = None;
    let mut active_right_key = None;
    let mut active_right_range = 0..0;
    let mut right_cursor = 0usize;
    visit_prepared_physical_join_plan_node(
        left,
        None,
        parameters,
        state,
        profiled_base_binding,
        execution,
        pipeline,
        index_runtime,
        row_runtime,
        &mut |left_row| {
            pipeline.borrow_mut().account_candidate_work()?;
            let Some(left_key) = bound_join_key(&left_row, right_schema, &equi_join_keys.columns)?
            else {
                return Ok(true);
            };
            if previous_left_key
                .as_ref()
                .is_some_and(|previous| left_key < *previous)
            {
                return Err(HawDBError::Execution(
                    "merge join left input violates its declared key order".to_string(),
                ));
            }
            previous_left_key = Some(left_key.clone());
            if active_right_key.as_ref() != Some(&left_key) {
                while right_cursor < right_rows.len() && right_rows[right_cursor].0 < left_key {
                    right_cursor = right_cursor.saturating_add(1);
                }
                let start = right_cursor;
                while right_cursor < right_rows.len() && right_rows[right_cursor].0 == left_key {
                    right_cursor = right_cursor.saturating_add(1);
                }
                active_right_key = Some(left_key);
                active_right_range = start..right_cursor;
            }
            for (_, right_row) in &right_rows[active_right_range.clone()] {
                pipeline.borrow_mut().account_candidate_work()?;
                let mut combined = left_row.clone();
                combined.bindings.extend(right_row.bindings.clone());
                let mut predicates_match = true;
                for predicate in predicates {
                    if predicate_truth(predicate, &combined, parameters)? != Some(true) {
                        predicates_match = false;
                        break;
                    }
                }
                if !predicates_match {
                    continue;
                }
                output_schema.ensure_matches(combined.schema_bindings())?;
                pipeline.borrow_mut().account_operator_row(operator_id)?;
                if !visit(combined)? {
                    return Ok(false);
                }
            }
            Ok(true)
        },
    )
}
