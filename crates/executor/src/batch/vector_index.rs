// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Shared eligible-domain collection and canonical hydration for explicit
//! vector modes. Upstream limits remain inside the eligible input boundary.

use super::*;
use crate::{ExternalReadResourceContract, ExternalReadResultBudget, VectorIndexExecutionRequest};
use hawdb_storage::read_view::{
    AdmittedNodeRead, AdmittedVec, GraphReadAdmission, GraphReadAllocation,
};

pub(super) fn stream(
    scan: &hawdb_plan_cypher::VectorIndexScan<PhysicalPlan>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let Some(Value::Vector(embedding)) = context.parameters.get(&scan.embedding_parameter) else {
        return Err(HawDBError::Semantic(
            "vector index requires a typed vector parameter".into(),
        ));
    };
    let resources = ExternalReadResourceContract {
        priority: 0,
        max_parallelism: NonZeroUsize::MIN,
        max_working_memory_bytes: context.memory.blocking_operator_bytes,
        result: ExternalReadResultBudget {
            max_rows: scan.top_k,
            max_memory_bytes: context.memory.blocking_operator_bytes,
        },
        task_context: context.task_context,
    };
    let eligibility_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "VectorIndexScan eligibility",
        context.memory.blocking_operator_bytes,
    );
    let definition_memory;
    let definition = if scan.mode.requires_eligible_domain() {
        definition_memory = Some(
            eligibility_account
                .reserve(hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES)?,
        );
        Some(context.external.vector_index_definition(&scan.index)?)
    } else {
        definition_memory = None;
        None
    };
    let mut eligible = definition
        .as_ref()
        .map(|(definition, identity)| {
            crate::vector_eligibility::VectorEligibleDomain::new(
                definition,
                *identity,
                embedding.as_ref(),
                scan.mode,
                scan.max_work,
                &eligibility_account,
                context.task_context,
            )
        })
        .transpose()?;
    if let (Some(domain), Some((definition, _))) = (&mut eligible, &definition) {
        let hawdb_core::VectorIndexTarget::NodeProperty {
            table, property, ..
        } = definition.target()
        else {
            return Err(HawDBError::Semantic(
                "Cypher eligibility requires a node-property index".into(),
            ));
        };
        let label = context.catalog.label_id(table).ok_or_else(|| {
            HawDBError::StorageIntegrity(
                "vector index label is absent from the canonical catalog".into(),
            )
        })?;
        let variable = scan
            .eligible_variable
            .as_ref()
            .ok_or_else(|| HawDBError::Semantic("missing eligible node variable".into()))?;
        let input = scan
            .input
            .as_deref()
            .ok_or_else(|| HawDBError::Semantic("missing vector eligible input plan".into()))?;
        validate_eligible_input(input)?;
        let eligible_task = domain.task().clone();
        let eligible_context = BatchReadContext {
            task_context: Some(&eligible_task),
            ..context
        };
        let collected = execute_prepared_binding_batches(
            BatchPlanRef::descendant(input),
            eligible_context,
            ExecutionLimit::unlimited(),
            &mut |batch| {
                for mut binding in batch {
                    domain.observe_input()?;
                    // OPTIONAL MATCH nulls contribute input work but no vector identity.
                    let Some(node) = binding.nodes.remove(variable) else {
                        continue;
                    };
                    if !node.labels.contains(&label) {
                        continue;
                    }
                    binding.nodes.insert(scan.variable.clone(), node);
                    if let Some(predicate) = &scan.node_visibility_predicate
                        && !evaluate_predicate_observed(
                            predicate,
                            context.catalog,
                            context.store,
                            &binding,
                            context.observer,
                            crate::store::AdjacencyReadMemory {
                                budget_bytes: context.memory.query_memory_bytes.get(),
                                account: Some(&eligibility_account),
                            },
                            Some(&eligible_task),
                        )?
                    {
                        continue;
                    }
                    let node = &binding.nodes[&scan.variable];
                    match node.properties.get(property) {
                        None | Some(Value::Null) => {}
                        Some(Value::Vector(vector)) => {
                            // Cover the temporary formatted identity until insert
                            // has admitted and copied its own retained key.
                            let _identity_memory = eligibility_account.reserve(64)?;
                            domain.insert(&format!("n:{:016x}", node.id.0), vector.as_ref())?;
                        }
                        _ => {
                            return Err(HawDBError::StorageIntegrity(
                                "eligible property is not a canonical vector".into(),
                            ))
                        }
                    }
                }
                Ok(BatchControl::Continue)
            },
        );
        eligible_task.check_work_limit()?;
        collected?;
        domain.finish()?;
    }
    let candidates = context
        .external
        .execute_vector_index(VectorIndexExecutionRequest {
            index: &scan.index,
            embedding: embedding.as_ref(),
            candidate_limit: scan.candidate_limit,
            top_k: scan.top_k,
            mode: scan.mode,
            eligible: eligible.as_ref(),
            resources,
        })?;
    drop(eligible);
    drop(definition);
    drop(definition_memory);
    resources.checkpoint()?;
    if candidates.row_count() > scan.top_k {
        return Err(HawDBError::StorageIntegrity(
            "vector index exceeded its declared topK".into(),
        ));
    }
    let account = context
        .kernel_context()
        .source_account("VectorIndexScan canonical nodes");
    let mut allocate =
        |bytes| crate::store::admit_graph_read(&account, context.task_context, bytes);
    let mut allocations =
        AdmittedVec::<Box<dyn GraphReadAllocation>>::new(&GraphReadAdmission::new(&mut allocate))?;
    let mut output = AccountedBindingBatch::with_ledger(
        "VectorIndexScan",
        context.memory.batch_rows.get(),
        context.memory.batch_payload_bytes,
        context.memory_ledger,
    );
    let mut emitted = 0usize;
    for ordinal in 0..candidates.row_count() {
        resources.checkpoint()?;
        if execution_limit.is_reached(emitted) {
            break;
        }
        let candidate = candidates.row(ordinal)?;
        if !candidate.distance.is_finite() || !(0.0..=2.0).contains(&candidate.distance) {
            return Err(HawDBError::StorageIntegrity(
                "vector index returned an invalid cosine distance".into(),
            ));
        }
        let input = context.store.node_with_allocation(
            candidate.node_id,
            None,
            &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                &mut |bytes| {
                    let bytes = bytes
                        .saturating_add(4096)
                        .saturating_add(scan.variable.len())
                        .saturating_add(scan.distance_column.len());
                    if !output.is_empty()
                        && (!account.can_reserve(bytes) || output.would_exceed_payload(bytes))
                    {
                        let control = output.emit(emit)?;
                        allocations.clear();
                        if control == BatchControl::Stop {
                            return Ok(None);
                        }
                    }
                    crate::store::admit_graph_read(&account, context.task_context, bytes).map(Some)
                },
                context.task_context,
            ),
        )?;
        let node = match input {
            AdmittedNodeRead::Node(node) => node,
            AdmittedNodeRead::Stopped => return Ok(BatchControl::Stop),
            _ => {
                return Err(HawDBError::StorageIntegrity(
                    "vector index candidate is absent from the bound canonical snapshot".into(),
                ))
            }
        };
        let (node, allocation) = node.into_parts();
        let binding = Binding {
            nodes: BTreeMap::from([(scan.variable.clone(), node)]).into(),
            values: BTreeMap::from([(
                scan.distance_column.clone(),
                Value::Float(candidate.distance),
            )]),
            relationships: BTreeMap::new().into(),
        };
        if let Some(predicate) = &scan.node_visibility_predicate
            && !evaluate_predicate_observed(
                predicate,
                context.catalog,
                context.store,
                &binding,
                context.observer,
                crate::store::AdjacencyReadMemory {
                    budget_bytes: context.memory.query_memory_bytes.get(),
                    account: Some(&account),
                },
                context.task_context,
            )?
        {
            continue;
        }
        allocations.try_push(allocation)?;
        if output.push(binding, emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        emitted = emitted.saturating_add(1);
        if output.is_full() {
            let control = output.emit(emit)?;
            allocations.clear();
            if control == BatchControl::Stop {
                return Ok(control);
            }
        }
    }
    resources.checkpoint()?;
    output.emit(emit)
}

fn validate_eligible_input(plan: &PhysicalPlan) -> Result<()> {
    if matches!(
        plan,
        PhysicalPlan::GraphAlgorithm { .. }
            | PhysicalPlan::GraphAlgorithmOutput(_)
            | PhysicalPlan::TextSeedScan { .. }
            | PhysicalPlan::VectorSeedScan { .. }
            | PhysicalPlan::GraphSeedScan { .. }
            | PhysicalPlan::VectorIndexScan(_)
    ) {
        return Err(HawDBError::Semantic(
            "vector eligibility requires canonical graph input; algorithm and external seed work is not admitted".into(),
        ));
    }
    match plan.children() {
        PlanChildren::None => Ok(()),
        PlanChildren::Unary(input) => validate_eligible_input(input),
        PlanChildren::Binary(left, right) => {
            validate_eligible_input(left)?;
            validate_eligible_input(right)
        }
    }
}

#[cfg(test)]
mod eligibility_tests {
    use super::*;

    #[test]
    fn eligibility_refuses_unmetered_algorithm_even_below_a_limit() {
        let algorithm = PhysicalPlan::GraphAlgorithm {
            algorithm: hawdb_plan_cypher::GraphAlgorithmKind::PageRank,
            graph_name: "input".into(),
            options: hawdb_plan_cypher::GraphAlgorithmOptions {
                max_iterations: Some(usize::MAX),
                tolerance: Some(0.0),
                ..Default::default()
            },
            score_column: "rank".into(),
            return_node_identity: true,
            node_visibility_predicate: None,
        };
        let plan = PhysicalPlan::LimitExec {
            offset: 0,
            limit: Some(1),
            input: Box::new(algorithm),
        };
        assert!(matches!(
            validate_eligible_input(&plan),
            Err(HawDBError::Semantic(_))
        ));
    }
}
