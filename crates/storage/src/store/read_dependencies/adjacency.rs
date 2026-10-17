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

use super::*;

impl GraphStore {
    /// Collects complete one-hop identities before predicates filter endpoints.
    /// Does not construct relationship/node property values. Canonical scans
    /// still read and validate encoded blocks. Work
    /// includes overridden/tombstoned canonical entries and live group visits.
    #[doc(hidden)]
    pub fn collect_prepared_adjacency_dependencies(
        &self,
        captured: &mut GraphReadDependencies,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        remaining_work: &mut usize,
    ) -> Result<()> {
        let result = self.collect_adjacency_dependencies_inner(
            captured,
            node_id,
            rel_type,
            direction,
            remaining_work,
        );
        captured.refused |= result.is_err();
        self.poison_on_storage_error(&result);
        result
    }

    fn collect_adjacency_dependencies_inner(
        &self,
        captured: &mut GraphReadDependencies,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        remaining_work: &mut usize,
    ) -> Result<()> {
        self.ensure_usable()?;
        if !Arc::ptr_eq(&self.read_dependency_readers, &captured.readers)
            || self.commit_epoch != captured.source_epoch
        {
            return Err(HawDBError::Execution(
                "adjacency dependency scan requires the captured source snapshot".into(),
            ));
        }
        if self.canonical_base.is_some() && self.canonical_adjacency.is_none() {
            return Err(HawDBError::Execution(
                "prepared adjacency requires a qualified canonical adjacency descriptor".into(),
            ));
        }
        record(captured, VersionKey::GraphNode(node_id))?;
        record(captured, VersionKey::GraphAdjacency { node_id, direction })?;
        let adjacency = match direction {
            AdjacencyDirection::Outgoing => &self.outgoing,
            AdjacencyDirection::Incoming => &self.incoming,
        };
        for ((_, kind), entries) in
            adjacency.range(&(node_id, RelTypeId(0)), &(node_id, RelTypeId(u32::MAX)))
        {
            charge(remaining_work)?;
            if rel_type.is_some_and(|expected| expected != *kind) {
                continue;
            }
            for entry in entries.iter_copied() {
                charge(remaining_work)?;
                identity(captured, entry.relationship_id, entry.neighbor_id)?;
            }
        }
        if let Some(adjacency) = &self.canonical_adjacency {
            let mut callback_error = None;
            let result = adjacency
                .scan_endpoint_inputs_control(node_id, direction, rel_type, |entry| {
                    let result = charge(remaining_work).and_then(|()| {
                        if self
                            .relationship_tombstones
                            .contains(&entry.relationship_id)
                            || self.relationships.contains_key(&entry.relationship_id)
                        {
                            return Ok(());
                        }
                        identity(captured, entry.relationship_id, entry.neighbor_id)
                    });
                    if let Err(error) = result {
                        callback_error = Some(error);
                        Ok(CanonicalScanControl::Stop)
                    } else {
                        Ok(CanonicalScanControl::Continue)
                    }
                })
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()));
            self.poison_on_storage_error(&result);
            result?;
            if let Some(error) = callback_error {
                return Err(error);
            }
        }
        Ok(())
    }
}

fn charge(remaining: &mut usize) -> Result<()> {
    *remaining = remaining.checked_sub(1).ok_or_else(|| {
        HawDBError::Execution("prepared adjacency identity work budget exceeded".into())
    })?;
    Ok(())
}
fn record(captured: &mut GraphReadDependencies, key: VersionKey) -> Result<()> {
    captured
        .record(key)
        .map_err(|error| HawDBError::Execution(error.to_string()))
}
fn identity(
    captured: &mut GraphReadDependencies,
    relationship: RelId,
    neighbor: NodeId,
) -> Result<()> {
    record(captured, VersionKey::GraphRelationship(relationship))?;
    record(captured, VersionKey::GraphNode(neighbor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_metadata_work_refusal_cannot_validate_a_partial_footprint() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::default();
        let root = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let neighbor = store
            .create_node(&mut catalog, "Label", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, root, neighbor, "HAS_LABEL", BTreeMap::new())
            .unwrap();
        let kind = catalog.rel_type_id("HAS_LABEL");
        let mut captured = store.capture_read_dependencies(32, 8192).unwrap();
        assert!(store
            .collect_prepared_adjacency_dependencies(
                &mut captured,
                root,
                kind,
                AdjacencyDirection::Outgoing,
                &mut 0
            )
            .is_err());
        assert!(matches!(
            store.validate_read_dependencies(&captured),
            Err(GraphReadDependencyError::CaptureRefused)
        ));
        store.ensure_usable().unwrap();
    }

    #[test]
    fn metadata_enumeration_collects_identities_with_large_relationship_properties() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::default();
        let root = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let neighbor = store
            .create_node(&mut catalog, "Label", BTreeMap::new())
            .unwrap();
        let relationship = store
            .create_relationship(
                &mut catalog,
                root,
                neighbor,
                "HAS_LABEL",
                BTreeMap::from([("body".into(), Value::String("x".repeat(1024 * 1024)))]),
            )
            .unwrap();
        let mut captured = store.capture_read_dependencies(32, 8192).unwrap();
        store
            .collect_prepared_adjacency_dependencies(
                &mut captured,
                root,
                catalog.rel_type_id("HAS_LABEL"),
                AdjacencyDirection::Outgoing,
                &mut 2,
            )
            .unwrap();
        assert!(captured
            .keys
            .contains_key(&VersionKey::GraphRelationship(relationship)));
        assert!(captured.keys.contains_key(&VersionKey::GraphNode(neighbor)));
        store.validate_read_dependencies(&captured).unwrap();
    }
}
