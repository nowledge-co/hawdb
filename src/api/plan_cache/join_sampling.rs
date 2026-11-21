// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_optimizer::graph::join_sampling::{
    JoinSamplingShape, SampledMatchStatistics, SampledPath,
};
use hawdb_storage::join_sampling::{
    JoinSamplingHop, JoinSamplingLimits, JoinSamplingNode, JoinSamplingRequest,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct SamplingReadKey {
    epoch: u64,
    checkpoint_epoch: Option<u64>,
    generation: Option<u64>,
    limits: JoinSamplingLimits,
    template_values: Vec<Value>,
    values: Vec<Value>,
    parameterized: bool,
}

#[cfg(test)]
pub(super) fn retention_fixture() -> SamplingReadKey {
    let mut values = Vec::with_capacity(16);
    values.push(Value::String("v".repeat(256)));
    SamplingReadKey {
        epoch: 7,
        checkpoint_epoch: Some(3),
        generation: Some(2),
        limits: JoinSamplingLimits::default(),
        template_values: values.clone(),
        values,
        parameterized: false,
    }
}

impl SamplingReadKey {
    pub(super) fn matches_snapshot<R: hawdb_storage::graph_engine::GraphReadEngine>(
        &self,
        store: &R,
    ) -> bool {
        let view = store.published_read_view();
        self.epoch == view.visible_commit_epoch()
            && self.checkpoint_epoch == view.checkpoint_commit_epoch()
            && self.generation == view.physical_generation().map(|generation| generation.0)
    }

    pub(super) fn matches_parameters(&self, parameters: &BTreeMap<String, Value>) -> bool {
        self.template_values
            .iter()
            .zip(&self.values)
            .all(|(template, expected)| {
                resolve_plan_template_value(template, parameters, self.parameterized)
                    .is_ok_and(|value| value == expected)
            })
    }
}

pub(super) struct BoundSampling {
    pub(super) key: SamplingReadKey,
    shape: JoinSamplingShape,
    request: JoinSamplingRequest,
}

pub(super) fn bind<S: crate::executor::ExecutionStore>(
    context: &PlanCacheContext<'_, S>,
    logical: &LogicalPlan,
    parameters: &BTreeMap<String, Value>,
    parameterized: bool,
) -> Result<Option<BoundSampling>> {
    let Some(mut limits) = context.config.join_sampling else {
        return Ok(None);
    };
    limits.max_retained_bytes = limits
        .max_retained_bytes
        .min(context.config.execution_memory.query_memory_bytes.get());
    // Restricted populations need their own scoped sampling contract. Preserve
    // the ordinary access-control/scoring paths until that exists.
    if context.access_control.is_some() || context.scoring.is_some() {
        return Ok(None);
    }
    let value_ok = |value: &Value| {
        bounded_template(value)
            && resolve_plan_template_value(value, parameters, parameterized).is_ok_and(scalar_ok)
    };
    let Some(shape) = JoinSamplingShape::from_logical_with_value_check(logical, &value_ok) else {
        return Ok(None);
    };
    let mut nodes = Vec::new();
    for node in &shape.nodes {
        let Some(label) = context.catalog.label_id(&node.label) else {
            return Ok(None);
        };
        if context
            .catalog
            .property_index_id_with_kind(label, &node.property, IndexKind::Equality)
            .is_none()
        {
            return Ok(None);
        }
        let value = resolve_plan_template_value(&node.value, parameters, parameterized)?;
        if !scalar_ok(value) {
            return Ok(None);
        }
        nodes.push(JoinSamplingNode {
            label,
            property: node.property.clone(),
            value: value.clone(),
        });
    }
    let mut hops = Vec::new();
    for hop in &shape.hops {
        let Some(rel_type) = context.catalog.rel_type_id(&hop.rel_type) else {
            return Ok(None);
        };
        let direction = match hop.direction {
            hawdb_core::RelationshipDirection::Outgoing => {
                hawdb_storage::store::AdjacencyDirection::Outgoing
            }
            hawdb_core::RelationshipDirection::Incoming => {
                hawdb_storage::store::AdjacencyDirection::Incoming
            }
            hawdb_core::RelationshipDirection::Undirected => return Ok(None),
        };
        hops.push(JoinSamplingHop {
            rel_type,
            direction,
        });
    }
    let view = context.store.published_read_view();
    let key = SamplingReadKey {
        epoch: view.visible_commit_epoch(),
        checkpoint_epoch: view.checkpoint_commit_epoch(),
        generation: view.physical_generation().map(|generation| generation.0),
        limits,
        template_values: shape.nodes.iter().map(|node| node.value.clone()).collect(),
        values: nodes.iter().map(|node| node.value.clone()).collect(),
        parameterized,
    };
    Ok(Some(BoundSampling {
        key,
        shape,
        request: JoinSamplingRequest { nodes, hops },
    }))
}

fn scalar_ok(value: &Value) -> bool {
    matches!(value, Value::Int(_) | Value::Bool(_))
        || matches!(value, Value::String(text) if text.len() <= 256)
}

// Bound the copy of an opaque slot, without interpreting its private keys.
// Parameter resolution remains owned by hawdb-plan-cache.
fn bounded_template(value: &Value) -> bool {
    scalar_ok(value)
        || matches!(value, Value::Map(fields) if fields.len() <= 2
        && fields.iter().all(|(key, value)| key.len() <= 128 && (scalar_ok(value)
            || matches!(value, Value::List(path) if path.len() <= 8
                && path.iter().all(|part| matches!(part, Value::Int(_)))))))
}

impl BoundSampling {
    pub(super) fn collect<S: crate::executor::ExecutionStore>(
        &self,
        context: &PlanCacheContext<'_, S>,
    ) -> Result<(Option<SampledMatchStatistics>, String)> {
        let report =
            context
                .store
                .sample_join_path(&self.request, self.key.limits, context.task_context)?;
        #[cfg(feature = "qualification")]
        crate::api::query_preparation_metrics::record_join_sampling(report.work.elapsed_nanos);
        if report.snapshot_epoch != self.key.epoch {
            return Err(HawDBError::StorageIntegrity(
                "join sampling returned another read snapshot".into(),
            ));
        }
        let estimates = report.estimates.map(|estimates| {
            let convert = |path: hawdb_storage::join_sampling::JoinPathSample| SampledPath {
                seed_rows: path.seed_rows,
                work_rows: path.work_rows,
                hops: path
                    .hops
                    .into_iter()
                    .map(|hop| (hop.candidate_rows, hop.output_rows))
                    .collect(),
            };
            SampledMatchStatistics {
                shape: self.shape.clone(),
                forward: convert(estimates.forward),
                reverse: convert(estimates.reverse),
            }
        });
        if estimates
            .as_ref()
            .is_some_and(|statistics| !statistics.is_valid())
        {
            return Err(HawDBError::StorageIntegrity(
                "join sampling returned invalid path statistics".into(),
            ));
        }
        let selected = estimates.as_ref().map(|statistics| {
            if statistics.prefers_reverse() {
                "reverse"
            } else {
                "forward"
            }
        });
        let decision = format!(
            "join sampling: epoch={} selected={selected:?} refusal={:?} forward_work_rows={:?} reverse_work_rows={:?} probes={} prepared_entries={} read_bytes={} read_operations={} retained_bytes={} elapsed_ns={}",
            report.snapshot_epoch, report.refusal,
            estimates.as_ref().map(|statistics| statistics.forward.work_rows),
            estimates.as_ref().map(|statistics| statistics.reverse.work_rows),
            report.work.probes, report.work.prepared_entries, report.work.read_bytes,
            report.work.read_operations, report.work.retained_bytes, report.work.elapsed_nanos,
        );
        Ok((estimates, decision))
    }
}

hawdb_core::impl_retained_heap_for_struct!(SamplingReadKey {
    epoch,
    checkpoint_epoch,
    generation,
    limits,
    template_values,
    values,
    parameterized
});
