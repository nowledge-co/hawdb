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
        admit: &mut ControlledGraphReadAllocator<'_>,
    ) -> Result<Option<AdmittedRelationshipCandidates>> {
        if let PropertyFilter::And(filters) = filter {
            let mut best: Option<AdmittedRelationshipCandidates> = None;
            for child in filters {
                if let Some(candidate) =
                    self.prune_resident_relationships_admitted(rel_type, child, max_keys, admit)?
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
        } else {
            for ((candidate_type, candidate_property, value), posting) in
                self.relationship_property_index.iter()
            {
                descriptor.grow(0)?;
                if *candidate_type != rel_type || Some(candidate_property) != property {
                    continue;
                }
                let matches = match filter {
                    PropertyFilter::Eq {
                        value: expected, ..
                    } => value == expected,
                    PropertyFilter::NotEq {
                        value: expected, ..
                    } => value != expected,
                    PropertyFilter::In { values, .. } => values.contains(value),
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
                    &mut admit,
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
                &mut admit,
                |_| panic!("stopped probe must not own or emit a source record"),
            )
            .unwrap();
        assert_eq!(control, GraphScanControl::Stop);
        assert!(report.is_none());
        assert_eq!(calls, 1, "normal Stop must not invoke fallback admission");
    }
}
