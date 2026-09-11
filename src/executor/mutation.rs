//! Concrete-store entrypoint and graph projection adapter for mutation execution.

use super::*;
use skein_executor::store::{GraphExecutionRead, ScanControl};
use skein_storage::RelRecord;

use skein_executor::mutation::execute_mutation_with_store;
pub use skein_executor::mutation::project_staged_mutation_return_rows;
pub use skein_executor::mutation::{is_mutation_plan, mutation_command};
pub(super) use skein_executor::mutation::{
    node_set_assignment, relationship_on_create_property_value,
};

pub fn execute_mutation_with_limits(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    execute_mutation_with_store(plan, catalog, store, limits, task_context)
}

pub(super) fn try_projected_graph_with_node_filter(
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    node_labels: &[String],
    rel_types: &[String],
    include_node: impl Fn(&NodeRecord) -> bool,
    layout: ProjectionLayout,
    budget: ProjectionMemoryBudget,
) -> Result<ProjectedGraph> {
    let source = GraphExecutionProjectionSource(store);
    if node_labels.is_empty() && rel_types.is_empty() {
        return ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &source,
            None,
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    let label_ids = node_labels
        .iter()
        .filter_map(|label| catalog.label_id(label))
        .collect::<Vec<_>>();
    if !node_labels.is_empty() && label_ids.is_empty() {
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            &source,
            &[],
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    let rel_type_ids = rel_types
        .iter()
        .filter_map(|rel_type| catalog.rel_type_id(rel_type))
        .collect::<Vec<_>>();
    if !rel_types.is_empty() && rel_type_ids.is_empty() {
        if label_ids.is_empty() {
            return ProjectedGraph::try_from_store_without_edges_with_node_filter_and_layout(
                &source,
                include_node,
                layout,
                budget,
            )
            .map_err(|error| SkeinError::Execution(error.to_string()));
        }
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            &source,
            &label_ids,
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    ProjectedGraph::try_from_store_labels_and_rel_types_with_node_filter_and_layout(
        &source,
        &label_ids,
        &rel_type_ids,
        include_node,
        layout,
        budget,
    )
    .map_err(|error| SkeinError::Execution(error.to_string()))
}

struct GraphExecutionProjectionSource<'a>(&'a dyn GraphExecutionRead);

impl skein_analytics::ProjectionSource for GraphExecutionProjectionSource<'_> {
    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> skein_analytics::ProjectionScanControl,
    ) -> std::result::Result<skein_analytics::ProjectionScanControl, String> {
        self.0
            .visit_nodes_owned(None, &mut |node| {
                Ok(match visitor(node) {
                    skein_analytics::ProjectionScanControl::Continue => ScanControl::Continue,
                    skein_analytics::ProjectionScanControl::Stop => ScanControl::Stop,
                })
            })
            .map(|control| match control {
                ScanControl::Continue => skein_analytics::ProjectionScanControl::Continue,
                ScanControl::Stop => skein_analytics::ProjectionScanControl::Stop,
            })
            .map_err(|error| error.to_string())
    }

    fn visit_projection_relationships(
        &self,
        visitor: &mut dyn FnMut(RelRecord) -> skein_analytics::ProjectionScanControl,
    ) -> std::result::Result<skein_analytics::ProjectionScanControl, String> {
        let scan = self
            .0
            .scan_relationships_with_filter_pruning(None, None)
            .map_err(|error| error.to_string())?;
        for relationship in scan.relationships {
            if visitor(relationship) == skein_analytics::ProjectionScanControl::Stop {
                return Ok(skein_analytics::ProjectionScanControl::Stop);
            }
        }
        Ok(skein_analytics::ProjectionScanControl::Continue)
    }
}
