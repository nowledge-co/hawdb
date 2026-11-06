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
use crate::graph_index::RelIdPropertyPostingList;
use crate::read_view::{AdmittedKeySet, GraphReadAllocation};

pub(super) struct AdmittedRelationshipCandidates {
    pub(super) ids: AdmittedKeySet<RelId>,
    pub(super) strategy: ScanPruningStrategy,
    _descriptor: Box<dyn GraphReadAllocation>,
}

impl GraphStore {
    /// Resident indexed candidates are admitted incrementally, before tree
    /// insertion. Stop building when adjacency is the cheaper access path.
    pub(super) fn prune_resident_relationships_admitted(
        &self,
        rel_type: RelTypeId,
        filter: &PropertyFilter,
        max_keys: usize,
        admit: &mut dyn FnMut(usize) -> Result<Option<Box<dyn GraphReadAllocation>>>,
    ) -> Result<Option<AdmittedRelationshipCandidates>> {
        if let PropertyFilter::And(filters) = filter {
            let mut best: Option<AdmittedRelationshipCandidates> = None;
            for child in filters {
                // A child no smaller than the best candidate cannot replace it,
                // so it may stop building at that size.
                let cap = best
                    .as_ref()
                    .map_or(max_keys, |best| best.ids.len().min(max_keys));
                if let Some(candidate) =
                    self.prune_resident_relationships_admitted(rel_type, child, cap, admit)?
                {
                    if candidate.ids.is_empty() {
                        return Ok(Some(candidate));
                    }
                    if best
                        .as_ref()
                        .is_none_or(|best| candidate.ids.len() < best.ids.len())
                    {
                        best = Some(candidate);
                    }
                }
            }
            return Ok(best);
        }
        if let PropertyFilter::Or(filters) = filter {
            let Some(descriptor) = admit(128)? else {
                return Ok(None);
            };
            if descriptor.bytes() < 128 {
                return Err(HawDBError::Execution(
                    "relationship probe admission returned an insufficient allocation permit"
                        .into(),
                ));
            }
            let Some(allocation) = admit(0)? else {
                return Ok(None);
            };
            let mut ids = AdmittedKeySet::with_allocation(allocation);
            for child in filters {
                let Some(candidate) =
                    self.prune_resident_relationships_admitted(rel_type, child, max_keys, admit)?
                else {
                    return Ok(None);
                };
                for id in candidate.ids.iter() {
                    if !ids.contains(id) && ids.len() == max_keys {
                        return Ok(None);
                    }
                    ids.try_insert(*id)?;
                }
            }
            return Ok(Some(AdmittedRelationshipCandidates {
                ids,
                strategy: if filters.is_empty() {
                    ScanPruningStrategy::Empty
                } else {
                    ScanPruningStrategy::OrUnion
                },
                _descriptor: descriptor,
            }));
        }
        let property = match filter {
            PropertyFilter::Eq { property, .. }
            | PropertyFilter::NotEq { property, .. }
            | PropertyFilter::In { property, .. }
            | PropertyFilter::IsNull { property }
            | PropertyFilter::IsNotNull { property }
            | PropertyFilter::DefaultIfNullOrEq { property, .. } => Some(property),
            PropertyFilter::Range {
                property,
                lower,
                upper,
            } if lower.is_some() || upper.is_some() => Some(property),
            PropertyFilter::IdEq { .. } | PropertyFilter::IdIn { .. } => None,
            PropertyFilter::IdRange { lower, upper } if lower.is_some() || upper.is_some() => None,
            _ => return Ok(None),
        };
        let descriptor_bytes =
            128usize.saturating_add(property.map_or(0, |property| property.len()));
        let Some(mut descriptor) = admit(descriptor_bytes)? else {
            return Ok(None);
        };
        if descriptor.bytes() < descriptor_bytes {
            return Err(HawDBError::Execution(
                "relationship probe admission returned an insufficient allocation permit".into(),
            ));
        }
        let Some(allocation) = admit(0)? else {
            return Ok(None);
        };
        let mut ids = AdmittedKeySet::with_allocation(allocation);
        let mut insert = |id| {
            if !ids.contains(&id) && ids.len() == max_keys {
                return Ok(false);
            }
            ids.try_insert(id)?;
            Ok::<_, HawDBError>(true)
        };
        if property.is_none()
            || matches!(
                filter,
                PropertyFilter::IsNull { .. } | PropertyFilter::DefaultIfNullOrEq { .. }
            )
        {
            for record in self.relationships.values() {
                descriptor.grow(0)?;
                if record.rel_type == rel_type
                    && match filter {
                        PropertyFilter::IdEq { value } => matches!(value, Value::Int(value) if u64::try_from(*value).ok() == Some(record.id.0)),
                        PropertyFilter::IdIn { values } => values.iter().any(|value| matches!(value, Value::Int(value) if u64::try_from(*value).ok() == Some(record.id.0))),
                        _ => property_filter_matches(filter, record.id.0, &record.properties),
                    }
                    && !insert(record.id)?
                {
                    return Ok(None);
                }
            }
        } else if let Some(property) = property {
            // Equality and membership read their exact keys; other filters read
            // only this type's postings for the property. Scanning the whole
            // index here made every source node pay for every relationship
            // property entry (#1546).
            let exact = match filter {
                PropertyFilter::Eq { value, .. } => Some(std::slice::from_ref(value)),
                PropertyFilter::In { values, .. } => Some(values.as_slice()),
                _ => None,
            };
            if let Some(values) = exact {
                for value in values {
                    descriptor.grow(0)?;
                    let Some(_key) = admit_key_copy(admit, property, value)? else {
                        return Ok(None);
                    };
                    record_index_entry_visit();
                    let Some(posting) = self.relationship_property_index.get(&(
                        rel_type,
                        property.clone(),
                        value.clone(),
                    )) else {
                        continue;
                    };
                    // A posting larger than the cap cannot prune; reject it
                    // before copying any identifier.
                    if posting.len() > max_keys {
                        return Ok(None);
                    }
                    for id in posting.iter() {
                        if !insert(*id)? {
                            return Ok(None);
                        }
                    }
                }
            } else {
                let Some(_key) = admit_key_copy(admit, property, &Value::Null)? else {
                    return Ok(None);
                };
                // `property` followed by NUL is the least greater string, so the
                // interval covers exactly this property's keys (and possibly a
                // NULL key of that successor, which the property check skips).
                let first = (rel_type, property.clone(), Value::Null);
                let last = (rel_type, format!("{property}\u{0}"), Value::Null);
                for ((candidate_type, candidate_property, value), posting) in
                    self.relationship_property_index.range(&first, &last)
                {
                    descriptor.grow(0)?;
                    record_index_entry_visit();
                    if *candidate_type != rel_type || candidate_property != property {
                        continue;
                    }
                    let matches = match filter {
                        PropertyFilter::NotEq {
                            value: expected, ..
                        } => value != expected,
                        PropertyFilter::IsNotNull { .. } => value != &Value::Null,
                        PropertyFilter::Range { lower, upper, .. } => {
                            range_bounds_match(value, lower.as_ref(), upper.as_ref())
                        }
                        _ => unreachable!(),
                    };
                    if matches {
                        for id in posting.iter() {
                            if !insert(*id)? {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }
        let strategy = match filter {
            PropertyFilter::Eq { property, .. } => ScanPruningStrategy::PropertyEq {
                property: property.clone(),
            },
            PropertyFilter::NotEq { property, .. } => ScanPruningStrategy::PropertyNotEq {
                property: property.clone(),
            },
            PropertyFilter::In { values, .. } if values.is_empty() => ScanPruningStrategy::Empty,
            PropertyFilter::In { property, .. } => ScanPruningStrategy::PropertyIn {
                property: property.clone(),
            },
            PropertyFilter::IsNull { property } => ScanPruningStrategy::PropertyMissingOrNull {
                property: property.clone(),
            },
            PropertyFilter::IsNotNull { property } => ScanPruningStrategy::PropertyExists {
                property: property.clone(),
            },
            PropertyFilter::Range { property, .. } => ScanPruningStrategy::PropertyRange {
                property: property.clone(),
            },
            PropertyFilter::DefaultIfNullOrEq {
                property, negated, ..
            } => {
                if *negated {
                    ScanPruningStrategy::PropertyDefaultIfNullNotEq {
                        property: property.clone(),
                    }
                } else {
                    ScanPruningStrategy::PropertyDefaultIfNullEq {
                        property: property.clone(),
                    }
                }
            }
            PropertyFilter::IdEq { .. } => ScanPruningStrategy::IdEq,
            PropertyFilter::IdIn { values } => {
                if values.is_empty() {
                    ScanPruningStrategy::Empty
                } else {
                    ScanPruningStrategy::IdIn
                }
            }
            PropertyFilter::IdRange { .. } => ScanPruningStrategy::IdRange,
            _ => unreachable!(),
        };
        Ok(Some(AdmittedRelationshipCandidates {
            ids,
            strategy,
            _descriptor: descriptor,
        }))
    }
}

impl GraphStore {
    /// Endpoint node ids of `rel_type` relationships whose properties equal
    /// every entry of `properties`: the sources, the targets, or both. The
    /// ids are sorted and distinct. A resident store reads the resident
    /// relationship property index; an out-of-core store reads the persisted
    /// relationship equality projection of its canonical base and merges the
    /// resident delta. `None` means neither can answer within
    /// `max_relationships` candidate relationships (no property is given, an
    /// out-of-core store has no complete projection for any of them, the
    /// smallest posting is larger, or admission refused), so the caller keeps
    /// its node-first path (#1548).
    pub fn relationship_property_endpoint_candidates(
        &self,
        rel_type: RelTypeId,
        properties: &BTreeMap<String, Value>,
        endpoints: RelationshipEndpoints,
        max_relationships: usize,
        admit: &mut dyn FnMut(usize) -> Result<Option<Box<dyn GraphReadAllocation>>>,
    ) -> Result<Option<EndpointCandidates>> {
        if properties.is_empty() {
            return Ok(None);
        }
        if self.canonical_base.is_some() {
            return self.canonical_relationship_property_endpoint_candidates(
                rel_type,
                properties,
                endpoints,
                max_relationships,
                admit,
            );
        }
        let mut smallest: Option<(&String, &RelIdPropertyPostingList)> = None;
        for (property, value) in properties {
            let Some(_key) = admit_key_copy(admit, property, value)? else {
                return Ok(None);
            };
            record_index_entry_visit();
            let Some(posting) =
                self.relationship_property_index
                    .get(&(rel_type, property.clone(), value.clone()))
            else {
                // No relationship carries this value: no endpoint can match.
                let Some(allocation) = admit(0)? else {
                    return Ok(None);
                };
                return Ok(Some(EndpointCandidates {
                    ids: Vec::new(),
                    probed_property: property.clone(),
                    allocation,
                }));
            };
            if smallest.is_none_or(|(_, smallest)| posting.len() < smallest.len()) {
                smallest = Some((property, posting));
            }
        }
        let (probed_property, smallest) = smallest.expect("at least one property was probed");
        if smallest.len() > max_relationships {
            return Ok(None);
        }
        let Some(allocation) = admit(endpoint_bytes(smallest.len(), endpoints))? else {
            return Ok(None);
        };
        let mut ids =
            Vec::with_capacity(smallest.len().saturating_mul(endpoints.per_relationship()));
        for id in smallest.iter() {
            let Some(record) = self.relationships.get(id) else {
                return Err(HawDBError::StorageIntegrity(format!(
                    "relationship property index references missing relationship {}",
                    id.0
                )));
            };
            push_matching_endpoints(&mut ids, record, rel_type, properties, endpoints);
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(Some(EndpointCandidates {
            ids,
            probed_property: probed_property.clone(),
            allocation,
        }))
    }

    /// The out-of-core form: the canonical base answers from its persisted
    /// relationship equality projection, and the resident delta, which holds
    /// every created or rewritten relationship, from its own index.
    fn canonical_relationship_property_endpoint_candidates(
        &self,
        rel_type: RelTypeId,
        properties: &BTreeMap<String, Value>,
        endpoints: RelationshipEndpoints,
        max_relationships: usize,
        admit: &mut dyn FnMut(usize) -> Result<Option<Box<dyn GraphReadAllocation>>>,
    ) -> Result<Option<EndpointCandidates>> {
        let Some(projection) = self.persistent_property_projection.as_ref() else {
            return Ok(None);
        };
        if projection.is_poisoned() {
            return Ok(None);
        }
        // Rank the complete projections by their block-granular estimate.
        // Equal estimates (one block) are tried in turn: the first posting
        // within the cap answers.
        let mut probes = Vec::new();
        for (property, value) in properties {
            if !projection.manifest().supports_relationship(
                rel_type,
                property,
                PersistentPropertyProjectionKind::RelationshipEquality,
            ) {
                continue;
            }
            let (estimate, report) = projection
                .estimate_relationship_equality_entries(rel_type, property, value)
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
            self.graph_index_read_metrics
                .record_property(PersistentGraphIndexClass::RelationshipEquality, report);
            probes.push((estimate, property, value));
        }
        probes.sort_by_key(|(estimate, _, _)| *estimate);
        for (estimate, property, value) in probes {
            let delta =
                self.relationship_property_index
                    .get(&(rel_type, property.clone(), value.clone()));
            let delta_len = delta.as_ref().map_or(0, |posting| posting.len());
            if delta_len > max_relationships {
                continue;
            }
            let base_cap = max_relationships - delta_len;
            let (base, _base_allocation) = match self.canonical_relationship_posting(
                (rel_type, property, value),
                estimate,
                base_cap,
                admit,
            )? {
                BaseRelationshipPosting::Within(base, allocation) => (base, allocation),
                BaseRelationshipPosting::Overflow => continue,
                BaseRelationshipPosting::Refused => return Ok(None),
            };
            let relationships = base.len().saturating_add(delta_len);
            let Some(allocation) = admit(endpoint_bytes(relationships, endpoints))? else {
                return Ok(None);
            };
            let mut ids =
                Vec::with_capacity(relationships.saturating_mul(endpoints.per_relationship()));
            let mut reader = crate::read_view::GraphReadAllocator::new(&mut *admit, None);
            for id in base {
                let row = match self.relationship_with_allocation(id, &mut reader)? {
                    AdmittedRelationshipRead::Relationship(row) => row,
                    AdmittedRelationshipRead::Stopped => return Ok(None),
                    AdmittedRelationshipRead::Missing => {
                        return Err(HawDBError::StorageIntegrity(format!(
                            "relationship property projection references missing canonical relationship {}",
                            id.0
                        )));
                    }
                };
                let (record, _permit) = row.into_parts();
                if record.rel_type != rel_type || record.properties.get(property) != Some(value) {
                    return Err(HawDBError::StorageIntegrity(format!(
                        "relationship property projection candidate {} fails its canonical seek predicate",
                        id.0
                    )));
                }
                push_matching_endpoints(&mut ids, &record, rel_type, properties, endpoints);
            }
            if let Some(delta) = delta {
                for id in delta.iter() {
                    let Some(record) = self.relationships.get(id) else {
                        return Err(HawDBError::StorageIntegrity(format!(
                            "relationship property index references missing relationship {}",
                            id.0
                        )));
                    };
                    push_matching_endpoints(&mut ids, record, rel_type, properties, endpoints);
                }
            }
            ids.sort_unstable();
            ids.dedup();
            return Ok(Some(EndpointCandidates {
                ids,
                probed_property: property.clone(),
                allocation,
            }));
        }
        Ok(None)
    }

    /// Canonical relationship ids carrying `property = value`, excluding
    /// tombstoned relationships and those the delta rewrites.
    fn canonical_relationship_posting(
        &self,
        (rel_type, property, value): (RelTypeId, &str, &Value),
        estimate: u64,
        cap: usize,
        admit: &mut dyn FnMut(usize) -> Result<Option<Box<dyn GraphReadAllocation>>>,
    ) -> Result<BaseRelationshipPosting> {
        let projection = self
            .persistent_property_projection
            .as_ref()
            .expect("the caller checked the projection");
        // The estimate counts every entry of the overlapping blocks, so it
        // bounds the matches; one more than the cap detects an overflow.
        let slots = usize::try_from(estimate)
            .unwrap_or(usize::MAX)
            .min(cap.saturating_add(1));
        let Some(allocation) = admit(slots.saturating_mul(std::mem::size_of::<RelId>()))? else {
            return Ok(BaseRelationshipPosting::Refused);
        };
        let mut ids = Vec::with_capacity(slots);
        let mut overflow = false;
        let (report, _control) = projection
            .scan_relationship_equality_candidates(rel_type, property, value, |id| {
                if self.relationship_tombstones.contains(&id)
                    || self.relationships.contains_key(&id)
                {
                    return Ok(CanonicalScanControl::Continue);
                }
                if ids.len() == slots {
                    overflow = true;
                    return Ok(CanonicalScanControl::Stop);
                }
                ids.push(id);
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        self.graph_index_read_metrics
            .record_property(PersistentGraphIndexClass::RelationshipEquality, report);
        if overflow || ids.len() > cap {
            return Ok(BaseRelationshipPosting::Overflow);
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(BaseRelationshipPosting::Within(ids, allocation))
    }
}

/// One canonical posting: within the cap with its admitted ids, larger than
/// the cap, or refused by admission.
enum BaseRelationshipPosting {
    Within(Vec<RelId>, Box<dyn GraphReadAllocation>),
    Overflow,
    Refused,
}

/// Bytes of the endpoint ids seeded from `relationships` relationships.
fn endpoint_bytes(relationships: usize, endpoints: RelationshipEndpoints) -> usize {
    relationships
        .saturating_mul(endpoints.per_relationship())
        .saturating_mul(std::mem::size_of::<NodeId>())
}

/// Pushes the requested endpoints of `record` when it is a `rel_type`
/// relationship carrying every entry of `properties`.
fn push_matching_endpoints(
    ids: &mut Vec<NodeId>,
    record: &RelRecord,
    rel_type: RelTypeId,
    properties: &BTreeMap<String, Value>,
    endpoints: RelationshipEndpoints,
) {
    if record.rel_type != rel_type
        || properties
            .iter()
            .any(|(property, value)| record.properties.get(property) != Some(value))
    {
        return;
    }
    if matches!(
        endpoints,
        RelationshipEndpoints::Sources | RelationshipEndpoints::Both
    ) {
        ids.push(record.source);
    }
    if matches!(
        endpoints,
        RelationshipEndpoints::Targets | RelationshipEndpoints::Both
    ) {
        ids.push(record.target);
    }
}

/// Sorted distinct endpoint ids, the relationship property whose posting
/// supplied them, and the permit that admitted them.
pub struct EndpointCandidates {
    pub ids: Vec<NodeId>,
    pub probed_property: String,
    pub allocation: Box<dyn GraphReadAllocation>,
}

/// Which endpoints of the matching relationships a caller seeds from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipEndpoints {
    Sources,
    Targets,
    Both,
}

impl RelationshipEndpoints {
    fn per_relationship(self) -> usize {
        match self {
            Self::Both => 2,
            Self::Sources | Self::Targets => 1,
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Relationship property index entries visited by pruning on this thread.
    static INDEX_ENTRIES_VISITED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn record_index_entry_visit() {
    #[cfg(test)]
    INDEX_ENTRIES_VISITED.with(|visited| visited.set(visited.get() + 1));
}

/// Admits the owned copy of one probe key (property name and value).
fn admit_key_copy(
    admit: &mut dyn FnMut(usize) -> Result<Option<Box<dyn GraphReadAllocation>>>,
    property: &str,
    value: &Value,
) -> Result<Option<Box<dyn GraphReadAllocation>>> {
    let bytes = std::mem::size_of::<(RelTypeId, String, Value)>()
        .saturating_add(property.len() + 1)
        .saturating_add(
            usize::try_from(crate::mutation::estimated_value_bytes(value)).unwrap_or(usize::MAX),
        );
    admit(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_resident_filter_families_preserve_rows_reports_and_stop() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Source", BTreeMap::new())
            .unwrap();
        let other = store
            .create_node(&mut catalog, "Source", BTreeMap::new())
            .unwrap();
        for (ordinal, state) in [
            Some("active"),
            Some("inactive"),
            Some("active"),
            None,
            Some(""),
            Some("active"),
        ]
        .into_iter()
        .enumerate()
        {
            let target = store
                .create_node(&mut catalog, "Target", BTreeMap::new())
                .unwrap();
            let mut properties =
                BTreeMap::from([("timestamp".into(), Value::Int(ordinal as i64 * 10))]);
            if let Some(state) = state {
                properties.insert("state".into(), Value::String(state.into()));
            }
            store
                .create_relationship(&mut catalog, source, target, "LINK", properties)
                .unwrap();
        }
        store
            .create_relationship(
                &mut catalog,
                other,
                source,
                "LINK",
                BTreeMap::from([
                    ("state".into(), Value::String("active".into())),
                    ("timestamp".into(), Value::Int(70)),
                ]),
            )
            .unwrap();
        let rel_type = catalog.rel_type_id("LINK").unwrap();
        let active = PropertyFilter::Eq {
            property: "state".into(),
            value: Value::String("active".into()),
        };
        let range = PropertyFilter::Range {
            property: "timestamp".into(),
            lower: Some((Value::Int(10), true)),
            upper: Some((Value::Int(40), true)),
        };
        let filters = vec![
            active.clone(),
            range.clone(),
            PropertyFilter::And(vec![active.clone(), range]),
            PropertyFilter::Or(vec![
                active.clone(),
                PropertyFilter::Eq {
                    property: "state".into(),
                    value: Value::String("inactive".into()),
                },
            ]),
            PropertyFilter::In {
                property: "state".into(),
                values: vec![
                    Value::String("active".into()),
                    Value::String("active".into()),
                ],
            },
            PropertyFilter::In {
                property: "state".into(),
                values: vec![],
            },
            PropertyFilter::NotEq {
                property: "state".into(),
                value: Value::String("inactive".into()),
            },
            PropertyFilter::IsNull {
                property: "state".into(),
            },
            PropertyFilter::IsNotNull {
                property: "state".into(),
            },
            PropertyFilter::DefaultIfNullOrEq {
                property: "state".into(),
                empty: Value::String("".into()),
                default: Value::String("default".into()),
                value: Value::String("default".into()),
                negated: false,
            },
            PropertyFilter::DefaultIfNullOrEq {
                property: "state".into(),
                empty: Value::String("".into()),
                default: Value::String("default".into()),
                value: Value::String("active".into()),
                negated: true,
            },
            PropertyFilter::IdEq {
                value: Value::Int(-1),
            },
            PropertyFilter::IdEq {
                value: Value::Int(2),
            },
            PropertyFilter::IdIn {
                values: vec![Value::Int(-1), Value::Int(2), Value::Int(2)],
            },
            PropertyFilter::IdRange {
                lower: Some((Value::Int(1), true)),
                upper: Some((Value::Int(3), true)),
            },
            PropertyFilter::Or(vec![
                active.clone(),
                PropertyFilter::IdNotEq {
                    value: Value::Int(2),
                },
            ]),
        ];
        for filter in filters {
            let mut expected = Vec::new();
            store
                .visit_adjacent_relationships_with_filter_owned(
                    source,
                    Some(rel_type),
                    AdjacencyDirection::Outgoing,
                    &filter,
                    |record| {
                        expected.push(record.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            expected.sort_unstable();
            let mut actual = Vec::new();
            let mut admit = |bytes| {
                Ok(Some(
                    Box::new(UnaccountedReadAllocation(bytes)) as Box<dyn GraphReadAllocation>
                ))
            };
            let (control, report) = store
                .visit_filtered_ordered_relationships_with_allocation(
                    (source, Some(rel_type), AdjacencyDirection::Outgoing),
                    &filter,
                    64 * 1024,
                    &mut crate::read_view::ControlledGraphReadAllocator::new(&mut admit, None),
                    |record| {
                        actual.push(record.into_parts().0.id);
                        Ok(GraphScanControl::Continue)
                    },
                )
                .unwrap();
            actual.sort_unstable();
            assert_eq!(control, GraphScanControl::Continue);
            assert_eq!(actual, expected, "{filter:?}");
            if let Some(report) = report {
                assert!(report.pruned);
                assert_eq!(report.candidate_count_before_pruning, 7);
                assert_eq!(
                    report.pruned_candidate_count + report.candidate_count_before_filter,
                    7
                );
                assert_eq!(
                    report.filtered_out_count + report.output_count,
                    report.candidate_count_before_filter
                );
                assert!(report.output_count >= actual.len());
            }
        }
        let mut calls = 0;
        let mut admit = |_| {
            calls += 1;
            Ok(None)
        };
        let (control, report) = store
            .visit_filtered_ordered_relationships_with_allocation(
                (source, Some(rel_type), AdjacencyDirection::Outgoing),
                &active,
                64 * 1024,
                &mut crate::read_view::ControlledGraphReadAllocator::new(&mut admit, None),
                |_| panic!("stopped probe must not own or emit a source record"),
            )
            .unwrap();
        assert_eq!(control, GraphScanControl::Stop);
        assert!(report.is_none());
        assert_eq!(calls, 1, "normal Stop must not invoke fallback admission");
    }

    /// Pruning reads only the probed keys, or this type's postings for the
    /// property, and an AND stops building a child larger than its best
    /// candidate (#1546).
    #[test]
    fn indexed_relationship_pruning_reads_only_the_probed_keys() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let hub = store
            .create_node(&mut catalog, "Entity", BTreeMap::new())
            .unwrap();
        let mut sources = Vec::new();
        for offset in 0..300_i64 {
            let source = store
                .create_node(&mut catalog, "Entity", BTreeMap::new())
                .unwrap();
            sources.push(source);
            for (rel_type, state) in [("LINK", "active"), ("OTHER", "active")] {
                store
                    .create_relationship(
                        &mut catalog,
                        source,
                        hub,
                        rel_type,
                        BTreeMap::from([
                            ("table".into(), Value::Int(7)),
                            ("offset".into(), Value::Int(offset)),
                            ("state".into(), Value::String(state.into())),
                            ("stat".into(), Value::Int(offset % 3)),
                            ("statex".into(), Value::Int(offset)),
                        ]),
                    )
                    .unwrap();
            }
        }
        let rel_type = catalog.rel_type_id("LINK").unwrap();
        let eq = |property: &str, value: Value| PropertyFilter::Eq {
            property: property.into(),
            value,
        };
        let mut admit = |bytes| {
            Ok(Some(
                Box::new(UnaccountedReadAllocation(bytes)) as Box<dyn GraphReadAllocation>
            ))
        };
        let mut prune = |filter: &PropertyFilter, max_keys| {
            INDEX_ENTRIES_VISITED.with(|visited| visited.set(0));
            let candidate = store
                .prune_resident_relationships_admitted(rel_type, filter, max_keys, &mut admit)
                .unwrap()
                .map(|candidate| candidate.ids.iter().copied().collect::<Vec<_>>());
            (candidate, INDEX_ENTRIES_VISITED.with(std::cell::Cell::get))
        };
        // Mem's occurrence lookup: a common table id and a unique offset.
        let occurrence = PropertyFilter::And(vec![
            eq("table", Value::Int(7)),
            eq("offset", Value::Int(42)),
        ]);
        let (candidate, visited) = prune(&occurrence, 1);
        assert_eq!(candidate.map(|ids| ids.len()), Some(1));
        assert!(visited <= 2, "visited {visited} index entries");
        // The common posting alone exceeds a one-relationship cap at once.
        let (candidate, visited) = prune(&eq("table", Value::Int(7)), 1);
        assert_eq!(candidate, None);
        assert_eq!(visited, 1);
        // A range reads only this property's 300 keys, not `stat`/`statex`.
        let range = PropertyFilter::Range {
            property: "offset".into(),
            lower: Some((Value::Int(10), true)),
            upper: Some((Value::Int(19), true)),
        };
        let (candidate, visited) = prune(&range, usize::MAX);
        assert_eq!(candidate.map(|ids| ids.len()), Some(10));
        assert_eq!(visited, 300);
        let (candidate, visited) = prune(
            &PropertyFilter::IsNotNull {
                property: "stat".into(),
            },
            usize::MAX,
        );
        assert_eq!(candidate.map(|ids| ids.len()), Some(300));
        assert_eq!(visited, 3);
        // Results still match the unpruned adjacency for every source.
        let filters = [
            occurrence.clone(),
            eq("state", Value::String("active".into())),
            PropertyFilter::In {
                property: "offset".into(),
                values: vec![
                    Value::Int(3),
                    Value::Int(3),
                    Value::Int(299),
                    Value::Int(900),
                ],
            },
            PropertyFilter::NotEq {
                property: "stat".into(),
                value: Value::Int(1),
            },
            PropertyFilter::IsNotNull {
                property: "stat".into(),
            },
            range,
        ];
        for source in [sources[3], sources[42], sources[299]] {
            for filter in &filters {
                let mut expected = Vec::new();
                store
                    .visit_adjacent_relationships_with_filter_owned(
                        source,
                        Some(rel_type),
                        AdjacencyDirection::Outgoing,
                        filter,
                        |record| {
                            expected.push(record.id);
                            GraphScanControl::Continue
                        },
                    )
                    .unwrap();
                let mut actual = Vec::new();
                let mut admit = |bytes| {
                    Ok(Some(
                        Box::new(UnaccountedReadAllocation(bytes)) as Box<dyn GraphReadAllocation>
                    ))
                };
                store
                    .visit_filtered_ordered_relationships_with_allocation(
                        (source, Some(rel_type), AdjacencyDirection::Outgoing),
                        filter,
                        64 * 1024,
                        &mut crate::read_view::ControlledGraphReadAllocator::new(&mut admit, None),
                        |record| {
                            actual.push(record.into_parts().0.id);
                            Ok(GraphScanControl::Continue)
                        },
                    )
                    .unwrap();
                expected.sort_unstable();
                actual.sort_unstable();
                assert_eq!(actual, expected, "{filter:?}");
            }
        }
    }
}
