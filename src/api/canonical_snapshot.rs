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

use crate::schema::Catalog;
use crate::Result;

pub use hawdb_bootstrap::*;

pub(super) fn export_canonical_graph_snapshot_for(
    catalog: &Catalog,
    store: &impl hawdb_storage::graph_engine::GraphReadEngine,
) -> CanonicalGraphSnapshotExport {
    try_export_canonical_graph_snapshot_for(catalog, store)
        .expect("unchecked canonical snapshot export encountered a storage read error")
}

pub(super) fn try_export_canonical_graph_snapshot_for(
    catalog: &Catalog,
    store: &impl hawdb_storage::graph_engine::GraphReadEngine,
) -> Result<CanonicalGraphSnapshotExport> {
    let nodes = store
        .node_records_owned()
        .map(|node| {
            let node = node?;
            Ok(CanonicalSnapshotNode {
                node_id: node.id.0,
                stable_id: node.properties.get("id").cloned(),
                labels: node
                    .labels
                    .iter()
                    .filter_map(|label_id| catalog.label_name(*label_id))
                    .map(str::to_string)
                    .collect(),
                properties: node.properties.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let relationships = store
        .relationship_records_owned()
        .map(|relationship| {
            let relationship = relationship?;
            Ok(CanonicalSnapshotRelationship {
                relationship_id: relationship.id.0,
                stable_id: relationship.properties.get("id").cloned(),
                source_node_id: relationship.source.0,
                target_node_id: relationship.target.0,
                rel_type: catalog
                    .rel_type_name(relationship.rel_type)
                    .unwrap_or_default()
                    .to_string(),
                properties: relationship.properties.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CanonicalGraphSnapshotExport::from_rows(
        store.commit_epoch(),
        nodes,
        relationships,
    ))
}
