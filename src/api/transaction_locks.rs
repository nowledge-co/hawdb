//! Crate-private facade for storage-owned transaction lock metadata.

pub(crate) use skein_storage::transaction_locks::{
    GraphAdjacencyDirection, GraphAllocationKind, LockMode, LockRequest, LockTable, LockTarget,
    WaitForGraph, DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE,
};

#[cfg(test)]
mod tests {
    use super::*;
    use skein_storage::transaction_locks as owner;

    #[test]
    fn facade_preserves_storage_lock_type_identity() {
        let mut table: owner::LockTable = LockTable::default();
        let request: owner::LockRequest = LockRequest::graph_node(LockMode::Shared, 7);
        let _: &owner::LockTarget = &request.target;
        let _: owner::LockMode = request.mode;
        let _: owner::GraphAllocationKind = GraphAllocationKind::Node;
        let _: owner::GraphAdjacencyDirection = GraphAdjacencyDirection::Outgoing;
        let result: crate::error::Result<()> = table.grant(1, request.clone());
        result.unwrap();
        assert!(table.covers_all(1, &[request]));

        let mut waiting: owner::WaitForGraph = WaitForGraph::default();
        waiting
            .register(
                2,
                &table.blockers(2, &LockRequest::graph_node(LockMode::Exclusive, 7)),
            )
            .unwrap();
        assert_eq!(
            DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE,
            owner::DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE
        );
    }
}
