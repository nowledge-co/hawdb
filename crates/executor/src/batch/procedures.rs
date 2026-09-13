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
    pub(super) returns: &'a [skein_plan::ShortestPathProjection],
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
