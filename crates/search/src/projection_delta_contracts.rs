use crate::SearchProjectionDelta;
use skein_qos::{BackgroundWorkHint, BackgroundWorkPlan, WorkClass, WorkRequest};
use skein_storage::RelationalTablePrimaryKeyChanges;

/// A bounded graph-derived delta to apply to a search projection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchProjectionGraphDeltaRequest {
    /// Live graph nodes changed in the selected commits. Nodes without a
    /// direct search-projection kind remain present so a host batch hydrator
    /// can resolve application-owned projection dependencies.
    pub upsert_node_ids: Vec<u64>,
    pub delete_document_ids: Vec<String>,
    pub max_operations: Option<usize>,
    pub complete_through_graph_commit_epoch: Option<u64>,
}

impl SearchProjectionGraphDeltaRequest {
    pub fn operation_count(&self) -> usize {
        self.upsert_node_ids.len() + self.delete_document_ids.len()
    }

    #[doc(hidden)]
    pub fn background_work_request(&self) -> WorkRequest {
        WorkRequest::background(WorkClass::Projection, self.operation_count())
    }

    pub fn background_work_plan(&self, hint: BackgroundWorkHint) -> Option<BackgroundWorkPlan> {
        let operation_count = self.operation_count();
        if operation_count == 0 {
            return None;
        }
        if self
            .max_operations
            .is_some_and(|limit| operation_count > limit)
        {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            operation_count,
            hint,
        ))
    }
}

/// A coherent graph and relational changefeed batch for projection catch-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionChangeBatch {
    graph_delta: SearchProjectionGraphDeltaRequest,
    relational_primary_key_changes: Vec<RelationalTablePrimaryKeyChanges>,
}

impl SearchProjectionChangeBatch {
    #[doc(hidden)]
    pub fn new(
        graph_delta: SearchProjectionGraphDeltaRequest,
        relational_primary_key_changes: Vec<RelationalTablePrimaryKeyChanges>,
    ) -> Self {
        Self {
            graph_delta,
            relational_primary_key_changes,
        }
    }

    pub fn operation_count(&self) -> usize {
        self.graph_delta.operation_count().saturating_add(
            self.relational_primary_key_changes
                .iter()
                .map(|table| table.primary_keys.len())
                .fold(0usize, usize::saturating_add),
        )
    }

    pub const fn complete_through_commit_epoch(&self) -> Option<u64> {
        self.graph_delta.complete_through_graph_commit_epoch
    }

    pub fn graph_delta(&self) -> &SearchProjectionGraphDeltaRequest {
        &self.graph_delta
    }

    #[doc(hidden)]
    pub fn graph_delta_mut(&mut self) -> &mut SearchProjectionGraphDeltaRequest {
        &mut self.graph_delta
    }

    #[doc(hidden)]
    pub fn into_graph_delta(self) -> SearchProjectionGraphDeltaRequest {
        self.graph_delta
    }

    pub fn relational_primary_key_changes(&self) -> &[RelationalTablePrimaryKeyChanges] {
        &self.relational_primary_key_changes
    }

    pub fn has_relational_changes(&self) -> bool {
        !self.relational_primary_key_changes.is_empty()
    }
}

/// Application-owned relational projection work paired with a search delta.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchProjectionRelationalDelta {
    pub delta: SearchProjectionDelta,
    pub processed_primary_key_count: usize,
}

#[cfg(test)]
mod tests {
    use super::{SearchProjectionChangeBatch, SearchProjectionGraphDeltaRequest};
    use skein_qos::{BackgroundWorkHint, WorkClass};

    #[test]
    fn graph_delta_request_preserves_projection_admission_boundaries() {
        assert!(SearchProjectionGraphDeltaRequest::default()
            .background_work_plan(BackgroundWorkHint::default())
            .is_none());

        let request = SearchProjectionGraphDeltaRequest {
            upsert_node_ids: vec![1, 2],
            delete_document_ids: vec!["removed".to_string()],
            max_operations: Some(3),
            complete_through_graph_commit_epoch: Some(7),
        };
        let plan = request
            .background_work_plan(BackgroundWorkHint::default())
            .expect("bounded request must be schedulable");
        assert_eq!(request.operation_count(), 3);
        assert_eq!(request.background_work_request().estimated_operations, 3);
        assert_eq!(plan.request.class, WorkClass::Projection);
        assert_eq!(plan.request.estimated_operations, 3);

        let over_limit = SearchProjectionGraphDeltaRequest {
            max_operations: Some(2),
            ..request.clone()
        };
        assert!(over_limit
            .background_work_plan(BackgroundWorkHint::default())
            .is_none());
    }

    #[test]
    fn change_batch_retains_graph_delta_without_relational_changes() {
        let request = SearchProjectionGraphDeltaRequest {
            upsert_node_ids: vec![4],
            complete_through_graph_commit_epoch: Some(9),
            ..SearchProjectionGraphDeltaRequest::default()
        };
        let batch = SearchProjectionChangeBatch::new(request, Vec::new());

        assert_eq!(batch.operation_count(), 1);
        assert_eq!(batch.complete_through_commit_epoch(), Some(9));
        assert!(!batch.has_relational_changes());
        assert_eq!(batch.graph_delta().upsert_node_ids, [4]);
        assert!(batch.relational_primary_key_changes().is_empty());
    }
}
