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

//! Crate-private facade for storage-owned transaction lock metadata.

pub(crate) use hawdb_storage::transaction_locks::{
    GraphAdjacencyDirection, GraphAllocationKind, LockMode, LockRequest, LockTable, LockTarget,
    WaitForGraph, DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE,
};

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_storage::transaction_locks as owner;

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
