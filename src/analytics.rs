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

//! Compatibility facade and embedded-store adapter for `hawdb-analytics`.

pub use hawdb_analytics::{
    CommunityAssignment, GraphAlgorithmMemoryEstimate, HierarchicalCommunityAssignment,
    LouvainOptions, PageRankOptions, PageRankScore, ProjectedGraph, ProjectionLayout,
    ProjectionMemoryAdmissionError, ProjectionMemoryBudget, ProjectionMemoryEstimate,
    ProjectionScanControl, ProjectionSource,
};

use crate::store::{GraphScanControl, GraphStore};

impl ProjectionSource for GraphStore {
    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(hawdb_storage::NodeRecord) -> ProjectionScanControl,
    ) -> std::result::Result<ProjectionScanControl, String> {
        self.visit_nodes_owned(None, |node| match visitor(node) {
            ProjectionScanControl::Continue => GraphScanControl::Continue,
            ProjectionScanControl::Stop => GraphScanControl::Stop,
        })
        .map(|control| match control {
            GraphScanControl::Continue => ProjectionScanControl::Continue,
            GraphScanControl::Stop => ProjectionScanControl::Stop,
        })
        .map_err(|error| error.to_string())
    }

    fn visit_projection_relationships(
        &self,
        visitor: &mut dyn FnMut(hawdb_storage::RelRecord) -> ProjectionScanControl,
    ) -> std::result::Result<ProjectionScanControl, String> {
        for relationship in self.relationship_records_owned() {
            let relationship = relationship.map_err(|error| error.to_string())?;
            if visitor(relationship) == ProjectionScanControl::Stop {
                return Ok(ProjectionScanControl::Stop);
            }
        }
        Ok(ProjectionScanControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::{PageRankOptions, ProjectedGraph};
    use crate::schema::Catalog;
    use crate::store::GraphStore;
    use crate::Value;
    use std::collections::BTreeMap;

    #[test]
    fn embedded_store_projects_through_the_storage_neutral_analytics_source() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();

        let graph = ProjectedGraph::from_store(&store, catalog.rel_type_id("MENTIONS"));

        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
        assert_eq!(
            graph.outgoing_targets(source).unwrap().collect::<Vec<_>>(),
            vec![target]
        );
        assert_eq!(graph.page_rank(PageRankOptions::default()).len(), 2);
    }

    fn properties(values: &[(&str, i64)]) -> BTreeMap<String, Value> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), Value::Int(*value)))
            .collect()
    }
}
