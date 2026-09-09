//! Private procedure and external-read batch handlers.

use super::*;

pub(super) struct ShortestPathSpec<'a> {
    pub(super) source_label: &'a str,
    pub(super) source_id: &'a Value,
    pub(super) source_visibility_predicate: &'a Option<Predicate>,
    pub(super) rel_type: &'a str,
    pub(super) direction: &'a RelationshipDirection,
    pub(super) target_label: &'a str,
    pub(super) target_id: &'a Value,
    pub(super) target_visibility_predicate: &'a Option<Predicate>,
    pub(super) min_hops: &'a usize,
    pub(super) max_hops: &'a usize,
    pub(super) returns: &'a [crate::planner::ShortestPathProjection],
}

impl ShortestPathSpec<'_> {
    pub(super) fn stream(
        self,
        context: BatchReadContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            source_label,
            source_id,
            source_visibility_predicate,
            rel_type,
            direction,
            target_label,
            target_id,
            target_visibility_predicate,
            min_hops,
            max_hops,
            returns,
        } = self;
        let source_visibility_filter = source_visibility_predicate
            .as_ref()
            .map(property_filter_from_predicate)
            .transpose()?;
        let target_visibility_filter = target_visibility_predicate
            .as_ref()
            .map(property_filter_from_predicate)
            .transpose()?;
        let bindings = execute_shortest_path(
            context.catalog,
            context.store,
            ShortestPathExecInput {
                source_label,
                source_id,
                source_visibility_filter: source_visibility_filter.as_ref(),
                path_node_visibility_filter: source_visibility_filter.as_ref(),
                rel_type,
                direction: *direction,
                target_label,
                target_id,
                target_visibility_filter: target_visibility_filter.as_ref(),
                min_hops: *min_hops,
                max_hops: *max_hops,
                returns,
            },
            execution_limit,
            TraversalExecutionContext {
                memory: context.memory,
                memory_ledger: context.memory_ledger,
                task_context: context.task_context,
                observer: context.observer,
            },
        )?;
        bindings.emit_batches(context.memory.batch_rows.get(), emit)
    }
}

pub(super) struct ThreadRepairStatsSpec<'a> {
    pub(super) label: &'a str,
    pub(super) identity_label: &'a str,
    pub(super) identity_ref_property: &'a str,
    pub(super) thread_id_property: &'a str,
    pub(super) message_rel_type: &'a str,
    pub(super) message_label: &'a str,
    pub(super) memory_rel_type: &'a str,
    pub(super) memory_label: &'a str,
}

impl ThreadRepairStatsSpec<'_> {
    pub(super) fn stream(
        self,
        context: BatchReadContext<'_>,
        _execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } = self;
        let bindings = thread_repair_stats_rows(
            context.catalog,
            context.store,
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
            context.memory.blocking_operator_bytes,
            context.memory_ledger,
            context.observer,
            context.task_context,
        )?;
        bindings.emit_batches(context.memory.batch_rows.get(), emit)
    }
}

pub(super) struct VectorSeedScanSpec<'a> {
    pub(super) embedding_parameter: &'a str,
    pub(super) output_external_id: &'a bool,
    pub(super) metadata_filters: &'a BTreeMap<String, String>,
    pub(super) resource_profile: &'a crate::planner::VectorExecutionResourceProfile,
    pub(super) vector_plan: &'a crate::planner::VectorPhysicalPlan,
}

impl VectorSeedScanSpec<'_> {
    pub(super) fn stream(
        self,
        context: BatchReadContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            embedding_parameter,
            output_external_id,
            metadata_filters,
            resource_profile,
            vector_plan,
        } = self;
        let max_rows = vector_plan_top_k(vector_plan)
            .ok_or_else(|| {
                SkeinError::Execution("vector seed physical plan is missing TopK".to_string())
            })?
            .min(execution_limit.output_rows.unwrap_or(usize::MAX));
        if max_rows == 0 {
            return emit_owned_binding_batches(Vec::new(), context.memory.batch_rows.get(), emit);
        }
        let embedding =
            vector_embedding_parameter(context.parameters, embedding_parameter, vector_plan)?;
        let external_memory = external_read_memory_budget(*resource_profile, context.memory);
        let admitted_parallelism = context
            .task_context
            .map_or(1, |task_context| task_context.admitted_parallelism().get());
        let resources = ExternalReadResourceContract {
            priority: resource_profile.priority,
            max_parallelism: NonZeroUsize::new(
                resource_profile
                    .max_parallelism
                    .max(1)
                    .min(admitted_parallelism),
            )
            .expect("resolved external read parallelism is non-zero"),
            max_working_memory_bytes: external_memory.max_working_bytes,
            result: ExternalReadResultBudget {
                max_rows,
                max_memory_bytes: external_memory.max_result_bytes,
            },
            task_context: context.task_context,
        };
        let external_account = context.memory_ledger.account(
            QueryMemoryClass::ExternalRead,
            "VectorSeedScan external read",
            NonZeroUsize::new(resources.reserved_memory_bytes())
                .expect("external read reservation is non-zero"),
        );
        let _external_lease = external_account.reserve(resources.reserved_memory_bytes())?;
        resources.checkpoint()?;
        let output = context
            .external
            .execute_vector_seed(VectorSeedExecutionRequest {
                embedding: &embedding,
                metadata_filters,
                vector_plan,
                resources,
            })?;
        resources.checkpoint()?;
        output.validate_result_budget(resources.result)?;
        context.observer.record_vector_execution(output.report);
        let mut bindings = collect_bounded_operator_bindings_with_account(
            "VectorSeedScan",
            output.rows.into_iter().map(|row| {
                let mut values = BTreeMap::from([
                    ("id".to_string(), Value::String(row.id)),
                    ("score".to_string(), Value::Float(row.score)),
                ]);
                if *output_external_id && let Some(external_id) = row.external_id {
                    values.insert("external_id".to_string(), Value::String(external_id));
                }
                Binding {
                    values,
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                }
            }),
            context.memory.blocking_operator_bytes,
            context.memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "VectorSeedScan",
                context.memory.blocking_operator_bytes,
            ),
        )?;
        bindings.truncate(execution_limit.output_rows.unwrap_or(usize::MAX));
        emit_owned_binding_batches(bindings, context.memory.batch_rows.get(), emit)
    }
}
