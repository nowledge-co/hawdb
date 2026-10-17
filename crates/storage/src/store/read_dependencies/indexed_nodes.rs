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

//! Bounded old/new scalar-index footprints for prepared node reads.

use super::*;
use crate::version::node_property_read_key;

const MAX_INDEX_DESCRIPTORS: usize = 1024;
const MAX_METADATA_BYTES: usize = 64 * 1024;

pub(super) struct IndexedNodeReadInvalidations<'a> {
    catalog: &'a Catalog,
    indexes: Vec<&'a hawdb_core::IndexDescriptor>,
    required_properties: BTreeSet<String>,
    qualified: bool,
}

pub(super) fn record_stamp(stamps: &mut VersionWriteSet, key: VersionKey) -> Result<()> {
    stamps
        .record_live(key)
        .map_err(|error| HawDBError::Execution(error.to_string()))
}

impl<'a> IndexedNodeReadInvalidations<'a> {
    pub(super) fn new(catalog: &'a Catalog) -> Self {
        let mut result = Self {
            catalog,
            indexes: Vec::new(),
            required_properties: BTreeSet::new(),
            qualified: true,
        };
        let mut bytes = 0usize;
        for (position, index) in catalog.property_indexes().enumerate() {
            if position >= MAX_INDEX_DESCRIPTORS {
                result.qualified = false;
                break;
            }
            if index.kind == IndexKind::FullText {
                continue;
            }
            if !result.required_properties.contains(&index.property) {
                bytes = bytes.saturating_add(index.property.len()).saturating_add(
                    std::mem::size_of::<String>() + 4 * std::mem::size_of::<usize>(),
                );
                if bytes > MAX_METADATA_BYTES {
                    result.qualified = false;
                    break;
                }
                result.required_properties.insert(index.property.clone());
            }
            result.indexes.push(index);
        }
        result
    }

    pub(super) fn record_label(
        &self,
        label: &str,
        properties: Option<&BTreeMap<String, Value>>,
        changed: Option<(&str, &Value)>,
        stamps: &mut VersionWriteSet,
    ) -> Result<()> {
        record_stamp(stamps, VersionKey::ReadNodeDomain(Some(label.into())))?;
        if !self.qualified {
            return record_stamp(stamps, VersionKey::ReadBarrier);
        }
        let Some(label_id) = self.catalog.label_id(label) else {
            return record_stamp(stamps, VersionKey::ReadBarrier);
        };
        for index in self
            .indexes
            .iter()
            .filter(|index| index.label_id == label_id)
        {
            if let Some(value) = properties.and_then(|properties| properties.get(&index.property)) {
                self.record_key(label, &index.property, value, stamps)?;
            }
            if let Some((property, value)) =
                changed.filter(|(property, _)| *property == index.property)
            {
                self.record_key(label, property, value, stamps)?;
            }
        }
        Ok(())
    }

    pub(super) fn record_node(
        &self,
        labels: &BTreeSet<LabelId>,
        properties: &BTreeMap<String, Value>,
        changed: Option<(&str, &Value)>,
        stamps: &mut VersionWriteSet,
    ) -> Result<()> {
        for label in labels {
            if let Some(name) = self.catalog.label_name(*label) {
                self.record_label(name, Some(properties), changed, stamps)?;
            } else {
                record_stamp(stamps, VersionKey::ReadBarrier)?;
            }
        }
        Ok(())
    }

    fn record_key(
        &self,
        label: &str,
        property: &str,
        value: &Value,
        stamps: &mut VersionWriteSet,
    ) -> Result<()> {
        if let Some(key) = node_property_read_key(label, property, value) {
            record_stamp(stamps, key)?;
        }
        // The same unsupported values use the always-stamped domain at read.
        Ok(())
    }

    /// Cold writes read only declared indexed properties. Refuse oversized
    /// ownership before decoding/copying, then conservatively invalidate reads.
    /// Physical failures still fail closed rather than becoming a fallback.
    pub(super) fn projected_before(
        &self,
        store: &GraphStore,
        id: NodeId,
    ) -> Result<Option<ProjectedNodeRecord>> {
        if !self.qualified {
            return Ok(None);
        }
        let mut refused = false;
        let result =
            store.projected_node_owned_admitted(id, &self.required_properties, &mut |bytes| {
                if bytes > MAX_METADATA_BYTES {
                    refused = true;
                    return Err(HawDBError::Execution(
                        "read invalidation metadata exceeds its ownership limit".into(),
                    ));
                }
                Ok(())
            });
        if refused {
            return Ok(None);
        }
        store.poison_on_storage_error(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::{GraphMutation, MutationLimits, PropertyFilter};
    use crate::version::{node_property_read_key, MAX_NODE_PROPERTY_READ_KEY_BYTES};

    fn fixture() -> (Catalog, GraphStore) {
        let mut catalog = Catalog::default();
        let label = catalog.get_or_create_label("Memory");
        catalog.get_or_create_property_index(label, "id");
        let mut store = GraphStore::default();
        store
            .commit_mutations(
                &mut catalog,
                ["a", "b"]
                    .into_iter()
                    .map(|id| GraphMutation::CreateNode {
                        label: "Memory".into(),
                        properties: BTreeMap::from([("id".into(), Value::String(id.into()))]),
                    })
                    .collect(),
            )
            .unwrap();
        (catalog, store)
    }

    fn capture(store: &GraphStore, value: &str) -> GraphReadDependencies {
        let mut captured = store.capture_read_dependencies(32, 8192).unwrap();
        captured
            .record(node_property_read_key("Memory", "id", &Value::String(value.into())).unwrap())
            .unwrap();
        captured
    }

    fn update(id: u64, property: &str, value: Value) -> GraphMutation {
        GraphMutation::SetNodeProperty {
            label: "Memory".into(),
            filter: Some(PropertyFilter::IdEq {
                value: Value::Int(id as i64),
            }),
            property: property.into(),
            value,
        }
    }

    #[test]
    fn exact_reads_retain_disjoint_stale_writer_and_reader_independence() {
        let (mut catalog, mut store) = fixture();
        let matching = capture(&store, "a");
        let absent = capture(&store, "absent");
        let mut first = store.begin_mutation_transaction(&catalog);
        let mut second = store.begin_mutation_transaction(&catalog);
        first
            .stage_mutation_with_limits(
                update(0, "value", Value::Int(1)),
                MutationLimits::default(),
            )
            .unwrap();
        second
            .stage_mutation_with_limits(
                update(1, "value", Value::Int(2)),
                MutationLimits::default(),
            )
            .unwrap();
        for transaction in [first, second] {
            store
                .commit_mutation_transaction_and_relational(
                    &mut catalog,
                    transaction,
                    RelationalTransaction::default(),
                    MutationLimits::default(),
                )
                .unwrap();
        }
        assert!(store.validate_read_dependencies(&matching).is_err());
        store.validate_read_dependencies(&absent).unwrap();
    }

    #[test]
    fn indexed_assignment_batch_covers_old_and_final_keys_and_survives_pruning() {
        let (mut catalog, mut store) = fixture();
        let old = capture(&store, "a");
        let final_key = capture(&store, "final");
        store
            .commit_mutations(
                &mut catalog,
                vec![
                    update(0, "id", Value::String("intermediate".into())),
                    update(0, "id", Value::String("final".into())),
                    update(0, "value", Value::Int(9)),
                ],
            )
            .unwrap();
        store.reclaim_version_history();
        assert!(store.validate_read_dependencies(&old).is_err());
        assert!(store.validate_read_dependencies(&final_key).is_err());
        assert_eq!(
            store.nodes.get(&NodeId(0)).unwrap().properties["id"],
            Value::String("final".into())
        );
    }

    #[test]
    fn index_value_aba_still_conflicts_after_reclaim() {
        let (mut catalog, mut store) = fixture();
        let captured = capture(&store, "a");
        for value in ["moved", "a"] {
            store
                .commit_mutations(
                    &mut catalog,
                    vec![update(0, "id", Value::String(value.into()))],
                )
                .unwrap();
        }
        store.reclaim_version_history();
        assert!(store.validate_read_dependencies(&captured).is_err());
    }

    #[test]
    fn descriptor_work_refusal_uses_barrier_for_exact_property_reads() {
        let (mut catalog, mut store) = fixture();
        let label = catalog.label_id("Memory").unwrap();
        for number in 0..MAX_INDEX_DESCRIPTORS {
            catalog.get_or_create_property_index(label, &format!("extra_{number}"));
        }
        let captured = capture(&store, "a");
        store
            .commit_mutations(&mut catalog, vec![update(1, "value", Value::Int(2))])
            .unwrap();
        assert!(matches!(
            store.validate_read_dependencies(&captured),
            Err(GraphReadDependencyError::Conflict(
                crate::version::VersionConflict {
                    key: VersionKey::ReadBarrier,
                    ..
                }
            ))
        ));
    }

    #[test]
    fn unsupported_large_index_values_keep_domain_protection_without_global_barrier() {
        let (mut catalog, mut store) = fixture();
        let label = catalog.label_id("Memory").unwrap();
        catalog.get_or_create_property_index(label, "large");
        store
            .commit_mutations(
                &mut catalog,
                vec![update(
                    1,
                    "large",
                    Value::String("x".repeat(MAX_NODE_PROPERTY_READ_KEY_BYTES * 2)),
                )],
            )
            .unwrap();
        let exact = capture(&store, "a");
        let mut domain = store.capture_read_dependencies(32, 8192).unwrap();
        domain
            .record(VersionKey::ReadNodeDomain(Some("Memory".into())))
            .unwrap();
        store
            .commit_mutations(&mut catalog, vec![update(1, "value", Value::Int(2))])
            .unwrap();
        store.validate_read_dependencies(&exact).unwrap();
        assert!(store.validate_read_dependencies(&domain).is_err());
        assert!(store
            .version_index
            .stamp(&VersionKey::ReadBarrier)
            .is_none());
        assert!(
            node_property_read_key("Memory", "large", &Value::List(vec![Value::Int(1)])).is_none()
        );
    }
}
