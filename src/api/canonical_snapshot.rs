use crate::schema::Catalog;
use crate::store::GraphStore;
use crate::Result;

pub use skein_bootstrap::*;

pub(super) fn export_canonical_graph_snapshot_for(
    catalog: &Catalog,
    store: &GraphStore,
) -> CanonicalGraphSnapshotExport {
    try_export_canonical_graph_snapshot_for(catalog, store)
        .expect("unchecked canonical snapshot export encountered a storage read error")
}

pub(super) fn try_export_canonical_graph_snapshot_for(
    catalog: &Catalog,
    store: &GraphStore,
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
