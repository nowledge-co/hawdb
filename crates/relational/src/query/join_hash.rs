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
    bound_join_key, bound_relation_join_key, null_extended_tree_row, predicate_truth,
    typed_row_set_locator, visit_prepared_physical_join_plan_node,
    with_typed_locator_bound_row_for_scan, BindingId, BoundRow, DefaultHasher, ExecutorBinding,
    Hash, Hasher, HawDBError, RefCell, RelationalEquiJoinKeys, RelationalIndexRuntime,
    RelationalKey, RelationalLocatorLayout, RelationalOperatorId, RelationalPhysicalJoinExecution,
    RelationalPhysicalJoinNode, RelationalPhysicalOutputSchema, RelationalPhysicalRelation,
    RelationalPipelineState, RelationalRowRuntime, RelationalRowSetLocator, RelationalState,
    Result, SqlJoinKind, SqlPredicate, Value,
};
use hawdb_executor::blocking::{
    AdmittedHashJoin, AdmittedHashJoinAdapter, AdmittedHashJoinCandidate, AdmittedHashJoinControl,
    AdmittedHashJoinRecord, AdmittedHashJoinSide,
};

pub(super) const HASH_JOIN_SPILL_BINDING_NAME: &str = "__hawdb_relational_hash_locator";

pub(super) fn hash_join_spill_locator(binding: ExecutorBinding) -> Result<RelationalRowSetLocator> {
    if !binding.nodes.is_empty() || !binding.relationships.is_empty() || binding.values.len() != 1 {
        return Err(HawDBError::StorageIntegrity(
            "hash join spill record has an invalid binding shape".to_string(),
        ));
    }
    let Some(Value::Binary(payload)) = binding.values.get(HASH_JOIN_SPILL_BINDING_NAME) else {
        return Err(HawDBError::StorageIntegrity(
            "hash join spill record has no typed relational locator".to_string(),
        ));
    };
    RelationalRowSetLocator::decode_hash_spill_record(payload)
}

pub(super) fn visit_hash_join_candidate<'a>(
    context: &HashJoinCandidateContext<'_>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    left_row: &BoundRow<'a>,
    right_row: &BoundRow<'a>,
    matched: &mut bool,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    pipeline.borrow_mut().account_candidate_work()?;
    let mut combined = left_row.clone();
    combined.bindings.extend(right_row.bindings.clone());
    for predicate in context.predicates {
        if predicate_truth(predicate, &combined, context.parameters)? != Some(true) {
            return Ok(true);
        }
    }
    *matched = true;
    context
        .output_schema
        .ensure_matches(combined.schema_bindings())?;
    pipeline
        .borrow_mut()
        .account_operator_row(context.operator_id)?;
    visit(combined)
}

pub(super) struct HashJoinCandidateContext<'a> {
    pub(super) operator_id: RelationalOperatorId,
    pub(super) predicates: &'a [SqlPredicate],
    pub(super) output_schema: &'a RelationalPhysicalOutputSchema,
    pub(super) parameters: &'a [Value],
}

pub(super) fn visit_hash_join_unmatched<'a>(
    operator_id: RelationalOperatorId,
    output_schema: &RelationalPhysicalOutputSchema,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    left_row: &BoundRow<'a>,
    null_right: &BoundRow<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    let mut combined = left_row.clone();
    combined.bindings.extend(null_right.bindings.clone());
    output_schema.ensure_matches(combined.schema_bindings())?;
    pipeline.borrow_mut().account_operator_row(operator_id)?;
    visit(combined)
}

pub(super) fn relational_physical_relation_locator_layout<'a>(
    state: &'a RelationalState,
    relation: &'a RelationalPhysicalRelation,
) -> Result<RelationalLocatorLayout<'a>> {
    let schema = state.table_schema(&relation.table).ok_or_else(|| {
        HawDBError::Semantic(format!("unknown relational table {}", relation.table))
    })?;
    RelationalLocatorLayout::from_bindings([(
        relation.binding,
        relation.table.as_str(),
        relation.qualifier.as_str(),
        schema,
    )])
}

fn relational_hash_join_hash(key: &RelationalKey) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

struct RelationalHashJoinAdapter<'state, 'context, 'runtime, 'pipeline> {
    operator_id: RelationalOperatorId,
    kind: SqlJoinKind,
    predicates: &'context [SqlPredicate],
    equi_join_keys: &'context RelationalEquiJoinKeys,
    right_relation: &'context RelationalPhysicalRelation,
    left_locator_layout: &'context RelationalLocatorLayout<'state>,
    right_locator_layout: &'context RelationalLocatorLayout<'state>,
    output_schema: &'context RelationalPhysicalOutputSchema,
    parameters: &'context [Value],
    state: &'state RelationalState,
    row_runtime: &'context RelationalRowRuntime<'state>,
    pipeline: &'context RefCell<&'pipeline mut RelationalPipelineState<'runtime>>,
    visit: &'context mut dyn FnMut(BoundRow<'state>) -> Result<bool>,
    null_right: Option<BoundRow<'state>>,
}

impl<'state, 'context, 'runtime, 'pipeline>
    RelationalHashJoinAdapter<'state, 'context, 'runtime, 'pipeline>
{
    fn record_for_build(&self, row: &BoundRow<'state>) -> Result<Option<AdmittedHashJoinRecord>> {
        let Some(key) = bound_relation_join_key(row, self.right_relation, self.equi_join_keys)?
        else {
            return Ok(None);
        };
        self.record(key, typed_row_set_locator(row)?)
    }

    fn record_for_probe(&self, row: &BoundRow<'state>) -> Result<Option<AdmittedHashJoinRecord>> {
        self.pipeline.borrow_mut().account_candidate_work()?;
        let right_schema = self
            .state
            .table_schema(&self.right_relation.table)
            .ok_or_else(|| {
                HawDBError::Semantic(format!(
                    "unknown relational table {}",
                    self.right_relation.table
                ))
            })?;
        let Some(key) = bound_join_key(row, right_schema, &self.equi_join_keys.columns)? else {
            return Ok(None);
        };
        self.record(key, typed_row_set_locator(row)?)
    }

    fn record(
        &self,
        key: RelationalKey,
        locator: RelationalRowSetLocator,
    ) -> Result<Option<AdmittedHashJoinRecord>> {
        Ok(Some(AdmittedHashJoinRecord {
            hash: relational_hash_join_hash(&key),
            binding: ExecutorBinding::scalar(
                HASH_JOIN_SPILL_BINDING_NAME,
                Value::Binary(locator.encode_hash_spill_record()?),
            ),
        }))
    }

    fn locator(&self, binding: &ExecutorBinding) -> Result<RelationalRowSetLocator> {
        hash_join_spill_locator(binding.clone())
    }

    fn key_for_locator(
        &self,
        side: AdmittedHashJoinSide,
        locator: &RelationalRowSetLocator,
    ) -> Result<RelationalKey> {
        match side {
            AdmittedHashJoinSide::Build => with_typed_locator_bound_row_for_scan(
                locator,
                self.right_locator_layout,
                self.row_runtime,
                |row| {
                    bound_relation_join_key(row, self.right_relation, self.equi_join_keys)?
                        .ok_or_else(|| {
                            HawDBError::StorageIntegrity(
                                "hash join spill build row has a null join key".to_string(),
                            )
                        })
                },
            ),
            AdmittedHashJoinSide::Probe => with_typed_locator_bound_row_for_scan(
                locator,
                self.left_locator_layout,
                self.row_runtime,
                |row| {
                    let right_schema = self
                        .state
                        .table_schema(&self.right_relation.table)
                        .ok_or_else(|| {
                            HawDBError::Semantic(format!(
                                "unknown relational table {}",
                                self.right_relation.table
                            ))
                        })?;
                    bound_join_key(row, right_schema, &self.equi_join_keys.columns)?.ok_or_else(
                        || {
                            HawDBError::StorageIntegrity(
                                "hash join spill probe row has a null join key".to_string(),
                            )
                        },
                    )
                },
            ),
        }
    }

    fn visit_unmatched_row(&mut self, left_row: &BoundRow<'state>) -> Result<bool> {
        let Some(null_right) = &self.null_right else {
            return Ok(true);
        };
        visit_hash_join_unmatched(
            self.operator_id,
            self.output_schema,
            self.pipeline,
            left_row,
            null_right,
            self.visit,
        )
    }
}

impl AdmittedHashJoinAdapter for RelationalHashJoinAdapter<'_, '_, '_, '_> {
    fn validate_spill_record(
        &mut self,
        side: AdmittedHashJoinSide,
        hash: u64,
        binding: &ExecutorBinding,
    ) -> Result<()> {
        let locator = self.locator(binding)?;
        if relational_hash_join_hash(&self.key_for_locator(side, &locator)?) != hash {
            return Err(HawDBError::StorageIntegrity(
                "hash join spill key does not match its recorded hash".to_string(),
            ));
        }
        Ok(())
    }

    fn visit_candidate(
        &mut self,
        probe: &ExecutorBinding,
        build: &ExecutorBinding,
    ) -> Result<AdmittedHashJoinCandidate> {
        let probe_locator = self.locator(probe)?;
        let build_locator = self.locator(build)?;
        let right_schema = self
            .state
            .table_schema(&self.right_relation.table)
            .ok_or_else(|| {
                HawDBError::Semantic(format!(
                    "unknown relational table {}",
                    self.right_relation.table
                ))
            })?;
        with_typed_locator_bound_row_for_scan(
            &probe_locator,
            self.left_locator_layout,
            self.row_runtime,
            |left_row| {
                let left_key =
                    bound_join_key(left_row, right_schema, &self.equi_join_keys.columns)?
                        .ok_or_else(|| {
                            HawDBError::StorageIntegrity(
                                "hash join spill probe row has a null join key".to_string(),
                            )
                        })?;
                with_typed_locator_bound_row_for_scan(
                    &build_locator,
                    self.right_locator_layout,
                    self.row_runtime,
                    |right_row| {
                        let right_key = bound_relation_join_key(
                            right_row,
                            self.right_relation,
                            self.equi_join_keys,
                        )?
                        .ok_or_else(|| {
                            HawDBError::StorageIntegrity(
                                "hash join spill build row has a null join key".to_string(),
                            )
                        })?;
                        if right_key != left_key {
                            return Ok(AdmittedHashJoinCandidate::rejected());
                        }
                        let mut matched = false;
                        let keep_going = visit_hash_join_candidate(
                            &HashJoinCandidateContext {
                                operator_id: self.operator_id,
                                predicates: self.predicates,
                                output_schema: self.output_schema,
                                parameters: self.parameters,
                            },
                            self.pipeline,
                            left_row,
                            right_row,
                            &mut matched,
                            self.visit,
                        )?;
                        Ok(AdmittedHashJoinCandidate {
                            matched,
                            control: if keep_going {
                                AdmittedHashJoinControl::Continue
                            } else {
                                AdmittedHashJoinControl::Stop
                            },
                        })
                    },
                )
            },
        )
    }

    fn visit_unmatched(&mut self, probe: &ExecutorBinding) -> Result<AdmittedHashJoinControl> {
        let locator = self.locator(probe)?;
        let keep_going = with_typed_locator_bound_row_for_scan(
            &locator,
            self.left_locator_layout,
            self.row_runtime,
            |left_row| self.visit_unmatched_row(left_row),
        )?;
        Ok(if keep_going {
            AdmittedHashJoinControl::Continue
        } else {
            AdmittedHashJoinControl::Stop
        })
    }

    fn requires_complete_probe_match(&self) -> bool {
        self.kind == SqlJoinKind::Left
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn visit_hash_join<'a>(
    operator_id: RelationalOperatorId,
    kind: SqlJoinKind,
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
            "hash join cannot run below a probe input".to_string(),
        ));
    }
    let (
        RelationalPhysicalJoinNode::Relation(left_relation),
        RelationalPhysicalJoinNode::Relation(right_relation),
    ) = (left, right)
    else {
        return Err(HawDBError::Execution(
            "hash join requires two relation inputs".to_string(),
        ));
    };
    let left_locator_layout = relational_physical_relation_locator_layout(state, left_relation)?;
    let right_locator_layout = relational_physical_relation_locator_layout(state, right_relation)?;
    let task_context = pipeline.borrow().task_context;
    let null_right = (kind == SqlJoinKind::Left)
        .then(|| null_extended_tree_row(right, state))
        .transpose()?;
    let mut join = AdmittedHashJoin::new(
        "RelationalHashJoin",
        execution.memory,
        execution.memory_ledger,
        task_context,
    );
    let mut adapter = RelationalHashJoinAdapter {
        operator_id,
        kind,
        predicates,
        equi_join_keys,
        right_relation,
        left_locator_layout: &left_locator_layout,
        right_locator_layout: &right_locator_layout,
        output_schema,
        parameters,
        state,
        row_runtime,
        pipeline,
        visit,
        null_right,
    };
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
            if let Some(record) = adapter.record_for_build(&row)? {
                join.push_build(record)?;
            }
            Ok(true)
        },
    )?;
    join.finish_build()?;
    let fully_consumed = visit_prepared_physical_join_plan_node(
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
            let Some(record) = adapter.record_for_probe(&left_row)? else {
                return adapter.visit_unmatched_row(&left_row);
            };
            Ok(join.push_probe(record, &mut adapter)? == AdmittedHashJoinControl::Continue)
        },
    )?;
    let completed = if fully_consumed {
        join.finish(&mut adapter)? == AdmittedHashJoinControl::Continue
    } else {
        false
    };
    drop(adapter);
    execution.reports.borrow_mut().push(join.report());
    Ok(completed)
}
