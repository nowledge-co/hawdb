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

//! Read, scan, seek, and scan-pruning methods for [`GraphStore`].

use super::*;
use hawdb_storage::ids::project_node_record;

impl GraphStore {
    pub fn canonical_node_from_segments(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        self.durable
            .as_ref()
            .and_then(|durable| durable.canonical_segments.as_ref())
            .map(|reader| reader.get_node(id).map_err(canonical_segment_error))
            .transpose()
            .map(Option::flatten)
    }

    pub fn canonical_relationship_from_segments(&self, id: RelId) -> Result<Option<RelRecord>> {
        self.durable
            .as_ref()
            .and_then(|durable| durable.canonical_segments.as_ref())
            .map(|reader| reader.get_relationship(id).map_err(canonical_segment_error))
            .transpose()
            .map(Option::flatten)
    }

    pub fn node_records_owned(&self) -> GraphNodeIterator {
        hawdb_storage::graph_overlay::node_records(
            self.canonical_base
                .as_ref()
                .map(CanonicalSegmentReader::node_records),
            self.nodes.values().cloned().collect::<Vec<_>>(),
            self.node_tombstones.clone(),
        )
    }

    pub fn relationship_records_owned(&self) -> GraphRelationshipIterator {
        hawdb_storage::graph_overlay::relationship_records(
            self.canonical_base
                .as_ref()
                .map(CanonicalSegmentReader::relationship_records),
            self.relationships.values().cloned().collect::<Vec<_>>(),
            self.relationship_tombstones.clone(),
        )
    }

    pub fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        if self.node_tombstones.contains(&id) {
            return Ok(None);
        }
        if let Some(node) = self.nodes.get(&id) {
            return Ok(Some(node.clone()));
        }
        self.canonical_base
            .as_ref()
            .map(|reader| reader.get_node(id).map_err(canonical_segment_error))
            .transpose()
            .map(Option::flatten)
    }

    pub fn relationship_owned(&self, id: RelId) -> Result<Option<RelRecord>> {
        if self.relationship_tombstones.contains(&id) {
            return Ok(None);
        }
        if let Some(relationship) = self.relationships.get(&id) {
            return Ok(Some(relationship.clone()));
        }
        self.canonical_base
            .as_ref()
            .map(|reader| reader.get_relationship(id).map_err(canonical_segment_error))
            .transpose()
            .map(Option::flatten)
    }

    pub fn visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        self.visit_selected_nodes_owned(label_id, None, |node| {
            consumer(NodeRecord {
                id: node.id,
                labels: node.labels,
                properties: node.properties,
            })
        })
    }

    pub fn visit_projected_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        required_properties: &BTreeSet<String>,
        consumer: impl FnMut(ProjectedNodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        self.visit_selected_nodes_owned(label_id, Some(required_properties), consumer)
    }

    pub fn visit_projected_nodes_by_access_owned(
        &self,
        label_id: LabelId,
        access: &hawdb_plan::NodeProjectionAccess,
        required_properties: &BTreeSet<String>,
        consumer: impl FnMut(ProjectedNodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        match access {
            hawdb_plan::NodeProjectionAccess::LabelScan => {
                self.visit_projected_nodes_owned(Some(label_id), required_properties, consumer)
            }
            hawdb_plan::NodeProjectionAccess::PropertyValues { property, values } => self
                .visit_projected_nodes_by_property_owned(
                    label_id,
                    property,
                    values,
                    required_properties,
                    consumer,
                ),
            hawdb_plan::NodeProjectionAccess::PropertyUnion { .. } => Err(HawDBError::Execution(
                "property-union projection access requires executor-owned deduplication admission"
                    .to_string(),
            )),
            hawdb_plan::NodeProjectionAccess::CompositeEquality { predicates } => self
                .visit_projected_nodes_by_composite_property_owned(
                    label_id,
                    predicates,
                    required_properties,
                    consumer,
                ),
            hawdb_plan::NodeProjectionAccess::CompositeRange { seek } => self
                .visit_projected_nodes_by_composite_range_owned(
                    label_id,
                    seek,
                    required_properties,
                    consumer,
                ),
            hawdb_plan::NodeProjectionAccess::PropertyRange {
                property,
                lower,
                upper,
            } => self.visit_projected_nodes_by_property_range_owned(
                label_id,
                property,
                lower.as_ref(),
                upper.as_ref(),
                required_properties,
                consumer,
            ),
            hawdb_plan::NodeProjectionAccess::FullText { property, query } => self
                .visit_projected_nodes_by_full_text_property_owned(
                    label_id,
                    property,
                    query,
                    required_properties,
                    consumer,
                ),
        }
    }

    fn visit_selected_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        required_properties: Option<&BTreeSet<String>>,
        mut consumer: impl FnMut(ProjectedNodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let project_delta = |node: &NodeRecord| match required_properties {
            Some(required) => project_node_record(node.clone(), required),
            None => ProjectedNodeRecord {
                id: node.id,
                labels: node.labels.clone(),
                properties: node.properties.clone(),
            },
        };
        let Some(reader) = &self.canonical_base else {
            for node in self.nodes.values() {
                if self.node_matches_label(node, label_id)
                    && consumer(project_delta(node)) == GraphScanControl::Stop
                {
                    return Ok(GraphScanControl::Stop);
                }
            }
            return Ok(GraphScanControl::Continue);
        };

        let mut delta = self.nodes.iter().peekable();
        let mut graph_control = GraphScanControl::Continue;
        let mut consume_base = |base: ProjectedNodeRecord| {
            while delta.peek().is_some_and(|(id, _)| **id < base.id) {
                let (id, node) = delta.next().expect("peeked delta node exists");
                if !self.node_tombstones.contains(id)
                    && self.node_matches_label(node, label_id)
                    && consumer(project_delta(node)) == GraphScanControl::Stop
                {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
            }
            if delta.peek().is_some_and(|(id, _)| **id == base.id) {
                let (id, node) = delta.next().expect("matching delta node exists");
                if !self.node_tombstones.contains(id)
                    && self.node_matches_label(node, label_id)
                    && consumer(project_delta(node)) == GraphScanControl::Stop
                {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                return Ok(CanonicalScanControl::Continue);
            }
            if !self.node_tombstones.contains(&base.id)
                && label_id.is_none_or(|label_id| base.labels.contains(&label_id))
                && consumer(base) == GraphScanControl::Stop
            {
                graph_control = GraphScanControl::Stop;
                return Ok(CanonicalScanControl::Stop);
            }
            Ok(CanonicalScanControl::Continue)
        };
        let (_, canonical_control) = match required_properties {
            Some(required_properties) => {
                reader.scan_projected_nodes_control(required_properties, &mut consume_base)
            }
            None => reader.scan_nodes_control(|node| {
                consume_base(ProjectedNodeRecord {
                    id: node.id,
                    labels: node.labels,
                    properties: node.properties,
                })
            }),
        }
        .map_err(canonical_segment_error)?;
        if canonical_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for (id, node) in delta {
            if !self.node_tombstones.contains(id)
                && self.node_matches_label(node, label_id)
                && consumer(project_delta(node)) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn try_visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        mut consumer: impl FnMut(NodeRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let mut consumer_error = None;
        let control = self.visit_nodes_owned(label_id, |node| match consumer(node) {
            Ok(control) => control,
            Err(error) => {
                consumer_error = Some(error);
                GraphScanControl::Stop
            }
        })?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(control),
        }
    }

    pub fn visit_relationships_owned(
        &self,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            for relationship in self.relationships.values() {
                if self.relationship_matches_type(relationship, rel_type)
                    && consumer(relationship.clone()) == GraphScanControl::Stop
                {
                    return Ok(GraphScanControl::Stop);
                }
            }
            return Ok(GraphScanControl::Continue);
        };

        let mut delta = self.relationships.iter().peekable();
        let mut graph_control = GraphScanControl::Continue;
        let (_, canonical_control) = reader
            .scan_relationships_control(|base| {
                while delta.peek().is_some_and(|(id, _)| **id < base.id) {
                    let (id, relationship) =
                        delta.next().expect("peeked delta relationship exists");
                    if !self.relationship_tombstones.contains(id)
                        && self.relationship_matches_type(relationship, rel_type)
                        && consumer(relationship.clone()) == GraphScanControl::Stop
                    {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                }
                if delta.peek().is_some_and(|(id, _)| **id == base.id) {
                    let (id, relationship) =
                        delta.next().expect("matching delta relationship exists");
                    if !self.relationship_tombstones.contains(id)
                        && self.relationship_matches_type(relationship, rel_type)
                        && consumer(relationship.clone()) == GraphScanControl::Stop
                    {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                    return Ok(CanonicalScanControl::Continue);
                }
                if !self.relationship_tombstones.contains(&base.id)
                    && self.relationship_matches_type(&base, rel_type)
                    && consumer(base) == GraphScanControl::Stop
                {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(canonical_segment_error)?;
        if canonical_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for (id, relationship) in delta {
            if !self.relationship_tombstones.contains(id)
                && self.relationship_matches_type(relationship, rel_type)
                && consumer(relationship.clone()) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn try_visit_relationships_owned(
        &self,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(RelRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let mut consumer_error = None;
        let control = self.visit_relationships_owned(rel_type, |relationship| {
            match consumer(relationship) {
                Ok(control) => control,
                Err(error) => {
                    consumer_error = Some(error);
                    GraphScanControl::Stop
                }
            }
        })?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(control),
        }
    }

    pub fn statistics(&self, catalog: &Catalog) -> GraphStatistics {
        if !self.canonical_base_out_of_core {
            let mut statistics = compute_statistics_with_basic(
                &self.nodes,
                &self.relationships,
                Some(catalog),
                self.basic_statistics(),
            );
            statistics.index_samples = compute_index_statistics_samples(
                catalog,
                &self.property_index,
                &self.composite_property_index,
            );
            return statistics;
        }
        let mut statistics = self.checkpoint_statistics.clone();
        retain_supported_property_statistics(&mut statistics, Some(catalog));
        retain_valid_index_statistics_samples(&mut statistics, catalog);
        let basic = self.basic_statistics();
        statistics.node_count = basic.node_count;
        statistics.relationship_count = basic.relationship_count;
        statistics.label_counts = basic.label_counts;
        statistics.rel_type_counts = basic.rel_type_counts;
        statistics
    }

    pub fn basic_statistics(&self) -> BasicGraphStatistics {
        let mut statistics = self.basic_statistics.clone();
        statistics.computed_at_commit_epoch = self.commit_epoch;
        statistics
    }

    pub fn basic_statistics_consistency_report(&self) -> BasicStatisticsConsistencyReport {
        BasicStatisticsConsistencyReport::new(
            self.basic_statistics(),
            compute_basic_statistics(&self.nodes, &self.relationships, self.commit_epoch),
        )
    }

    pub fn adjacency_consistency_report(&self) -> AdjacencyConsistencyReport {
        AdjacencyConsistencyReport::new(
            self.commit_epoch,
            self.relationships.len(),
            maintained_adjacency_groups(&self.outgoing, &self.incoming),
            recompute_adjacency_groups(&self.relationships),
            &self.relationships,
        )
    }

    pub fn degree_statistics_consistency_report(&self) -> DegreeStatisticsConsistencyReport {
        DegreeStatisticsConsistencyReport::new(
            self.commit_epoch,
            compute_degree_statistics_from_adjacency(&self.nodes, &self.outgoing, &self.incoming),
            compute_degree_statistics_from_relationships(&self.nodes, &self.relationships),
        )
    }

    pub fn distinct_value_statistics_consistency_report(
        &self,
        catalog: &Catalog,
    ) -> DistinctValueStatisticsConsistencyReport {
        let recomputed = compute_statistics_for_catalog(
            &self.nodes,
            &self.relationships,
            catalog,
            self.basic_statistics(),
        );
        // The index-derived side can only speak for declared properties, so
        // the recomputed side is narrowed to the same keys. Comparing against
        // every property would flag the undeclared ones forever.
        let recomputed_property_distinct_counts = recomputed
            .property_distinct_counts
            .into_iter()
            .filter(|((label_id, property), _)| {
                catalog.has_scalar_property_index(*label_id, property)
            })
            .collect();
        DistinctValueStatisticsConsistencyReport::new(
            self.commit_epoch,
            compute_node_property_distinct_counts_from_index(&self.property_index, catalog),
            recomputed_property_distinct_counts,
            compute_relationship_property_distinct_counts_from_index(
                &self.relationship_property_index,
                catalog,
            ),
            recomputed.rel_property_distinct_counts,
        )
    }

    pub fn scan_nodes<'a>(
        &'a self,
        label_id: Option<LabelId>,
    ) -> impl Iterator<Item = &'a NodeRecord> + 'a {
        self.nodes
            .values()
            .filter(move |node| label_id.map(|id| node.labels.contains(&id)).unwrap_or(true))
    }

    pub fn scan_nodes_with_filter_pruning<'a>(
        &'a self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
    ) -> ScanPrunedNodeScan<'a> {
        let candidate =
            filter.and_then(|filter| self.prune_node_candidates(catalog, label_id, filter));
        let Some(candidate) = candidate else {
            let candidate_count_before_filter = self.node_count_for_label(label_id);
            let nodes = self
                .scan_nodes(label_id)
                .filter(|node| {
                    filter
                        .map(|filter| property_filter_matches(filter, node.id.0, &node.properties))
                        .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            let output_count = nodes.len();
            return ScanPrunedNodeScan {
                nodes,
                report: ScanPruningReport {
                    target_kind: ScanPruningTargetKind::Node,
                    label_id,
                    rel_type_id: None,
                    strategy: ScanPruningStrategy::FullLabelScan,
                    pruned: false,
                    exact_empty: false,
                    candidate_count_before_pruning: candidate_count_before_filter,
                    pruned_candidate_count: 0,
                    candidate_count_before_filter,
                    output_count,
                    filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
                },
            };
        };

        let candidate_count_before_pruning = self.node_count_for_label(label_id);
        let candidate_count_before_filter = candidate.node_ids.len();
        let nodes = candidate
            .node_ids
            .iter()
            .filter_map(|node_id| self.nodes.get(node_id))
            .filter(|node| self.node_matches_label(node, label_id))
            .filter(|node| {
                filter
                    .map(|filter| property_filter_matches(filter, node.id.0, &node.properties))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        let output_count = nodes.len();
        ScanPrunedNodeScan {
            nodes,
            report: ScanPruningReport {
                target_kind: ScanPruningTargetKind::Node,
                label_id,
                rel_type_id: None,
                strategy: candidate.strategy,
                pruned: true,
                exact_empty: candidate.exact_empty,
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning
                    .saturating_sub(candidate_count_before_filter),
                candidate_count_before_filter,
                output_count,
                filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
            },
        }
    }

    pub fn node_count_for_label(&self, label_id: Option<LabelId>) -> usize {
        let count = label_id.map_or(self.basic_statistics.node_count, |label_id| {
            self.basic_statistics
                .label_counts
                .get(&label_id)
                .copied()
                .unwrap_or_default()
        });
        usize::try_from(count).unwrap_or(usize::MAX)
    }

    fn node_matches_label(&self, node: &NodeRecord, label_id: Option<LabelId>) -> bool {
        label_id
            .map(|label_id| node.labels.contains(&label_id))
            .unwrap_or(true)
    }

    fn prune_node_candidates(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filter: &PropertyFilter,
    ) -> Option<ScanPruningCandidate> {
        // Every branch below that reads `property_index` first passes through
        // `indexes_property`. The index only holds declared properties, so a
        // candidate set built from an undeclared one would be empty rather
        // than complete, and the caller treats candidates as exact.
        match filter {
            PropertyFilter::And(filters) => {
                self.prune_and_node_candidates(catalog, label_id, filters)
            }
            PropertyFilter::Or(filters) => {
                self.prune_or_node_candidates(catalog, label_id, filters)
            }
            PropertyFilter::Not(_) => None,
            PropertyFilter::IdEq { value } => Some(ScanPruningCandidate::exact(
                ScanPruningStrategy::IdEq,
                self.node_ids_for_id_values(label_id, std::slice::from_ref(value)),
            )),
            PropertyFilter::IdNotEq { .. } => None,
            PropertyFilter::IdRange { lower, upper } => {
                if lower.is_none() && upper.is_none() {
                    return None;
                }
                Some(ScanPruningCandidate::exact(
                    ScanPruningStrategy::IdRange,
                    self.node_ids_for_id_range(label_id, lower.as_ref(), upper.as_ref()),
                ))
            }
            PropertyFilter::IdIn { values } => Some(ScanPruningCandidate::exact(
                if values.is_empty() {
                    ScanPruningStrategy::Empty
                } else {
                    ScanPruningStrategy::IdIn
                },
                self.node_ids_for_id_values(label_id, values),
            )),
            PropertyFilter::Eq { property, value } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        ScanPruningStrategy::PropertyEq {
                            property: property.clone(),
                        },
                        self.node_ids_for_property_values(
                            label_id,
                            property,
                            std::slice::from_ref(value),
                        ),
                    )
                })
            }
            PropertyFilter::NotEq { property, value } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        ScanPruningStrategy::PropertyNotEq {
                            property: property.clone(),
                        },
                        self.node_ids_for_property_not_in_values(
                            label_id,
                            property,
                            std::slice::from_ref(value),
                        ),
                    )
                })
            }
            PropertyFilter::IsNull { property } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        ScanPruningStrategy::PropertyMissingOrNull {
                            property: property.clone(),
                        },
                        self.node_ids_for_property_missing_or_null(label_id, property),
                    )
                })
            }
            PropertyFilter::IsNotNull { property } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        ScanPruningStrategy::PropertyExists {
                            property: property.clone(),
                        },
                        self.node_ids_for_property_exists(label_id, property),
                    )
                })
            }
            PropertyFilter::ListContains { .. }
            | PropertyFilter::ListContainsLower { .. }
            | PropertyFilter::Contains { .. }
            | PropertyFilter::StartsWith { .. }
            | PropertyFilter::EndsWith { .. }
            | PropertyFilter::RegexMatch { .. } => None,
            PropertyFilter::DefaultIfNullOrEq {
                property,
                empty,
                default,
                value,
                negated,
            } => {
                if !self.indexes_property(catalog, label_id, property) {
                    return None;
                }
                let strategy = if *negated {
                    ScanPruningStrategy::PropertyDefaultIfNullNotEq {
                        property: property.clone(),
                    }
                } else {
                    ScanPruningStrategy::PropertyDefaultIfNullEq {
                        property: property.clone(),
                    }
                };
                let node_ids = if *negated {
                    self.node_ids_for_default_if_null_not_eq(
                        label_id, property, empty, default, value,
                    )
                } else {
                    self.node_ids_for_default_if_null_eq(label_id, property, empty, default, value)
                };
                Some(ScanPruningCandidate::exact(strategy, node_ids))
            }
            PropertyFilter::In { property, values } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        if values.is_empty() {
                            ScanPruningStrategy::Empty
                        } else {
                            ScanPruningStrategy::PropertyIn {
                                property: property.clone(),
                            }
                        },
                        self.node_ids_for_property_values(label_id, property, values),
                    )
                })
            }
            PropertyFilter::Range {
                property,
                lower,
                upper,
            } => {
                if lower.is_none() && upper.is_none() {
                    return None;
                }
                if !self.indexes_property(catalog, label_id, property) {
                    return None;
                }
                Some(ScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyRange {
                        property: property.clone(),
                    },
                    self.node_ids_for_property_range(
                        label_id,
                        property,
                        lower.as_ref(),
                        upper.as_ref(),
                    ),
                ))
            }
        }
    }

    fn prune_and_node_candidates(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filters: &[PropertyFilter],
    ) -> Option<ScanPruningCandidate> {
        let mut best: Option<ScanPruningCandidate> = None;
        for filter in filters {
            let Some(candidate) = self.prune_node_candidates(catalog, label_id, filter) else {
                continue;
            };
            if candidate.exact_empty {
                return Some(candidate);
            }
            if best
                .as_ref()
                .map(|best| candidate.node_ids.len() < best.node_ids.len())
                .unwrap_or(true)
            {
                best = Some(candidate);
            }
        }
        best
    }

    fn prune_or_node_candidates(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filters: &[PropertyFilter],
    ) -> Option<ScanPruningCandidate> {
        if filters.is_empty() {
            return Some(ScanPruningCandidate {
                strategy: ScanPruningStrategy::Empty,
                node_ids: BTreeSet::new(),
                exact_empty: true,
            });
        }

        let mut node_ids = BTreeSet::new();
        for filter in filters {
            let candidate = self.prune_node_candidates(catalog, label_id, filter)?;
            node_ids.extend(candidate.node_ids);
        }
        Some(ScanPruningCandidate::exact(
            ScanPruningStrategy::OrUnion,
            node_ids,
        ))
    }

    fn node_ids_for_id_values(
        &self,
        label_id: Option<LabelId>,
        values: &[Value],
    ) -> BTreeSet<NodeId> {
        values
            .iter()
            .filter_map(|value| match value {
                Value::Int(value) => u64::try_from(*value).ok().map(NodeId),
                _ => None,
            })
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| self.node_matches_label(node, label_id))
                    .unwrap_or(false)
            })
            .collect()
    }

    fn node_ids_for_label(&self, label_id: Option<LabelId>) -> BTreeSet<NodeId> {
        self.nodes
            .values()
            .filter(|node| self.node_matches_label(node, label_id))
            .map(|node| node.id)
            .collect()
    }

    fn node_ids_for_id_range(
        &self,
        label_id: Option<LabelId>,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> BTreeSet<NodeId> {
        self.nodes
            .keys()
            .copied()
            .filter(|node_id| range_bounds_match(&Value::Int(node_id.0 as i64), lower, upper))
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| self.node_matches_label(node, label_id))
                    .unwrap_or(false)
            })
            .collect()
    }

    fn node_ids_for_property_values(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        values: &[Value],
    ) -> BTreeSet<NodeId> {
        if values.is_empty() {
            return BTreeSet::new();
        }
        let values = values.iter().collect::<BTreeSet<_>>();
        self.property_index
            .iter()
            .filter(|((candidate_label_id, candidate_property, value), _)| {
                label_id
                    .map(|label_id| *candidate_label_id == label_id)
                    .unwrap_or(true)
                    && candidate_property == property
                    && values.contains(value)
            })
            .flat_map(|(_, node_ids)| node_ids.iter().copied())
            .collect()
    }

    fn node_ids_for_property_not_in_values(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        values: &[Value],
    ) -> BTreeSet<NodeId> {
        let values = values.iter().collect::<BTreeSet<_>>();
        self.property_index
            .iter()
            .filter(|((candidate_label_id, candidate_property, value), _)| {
                label_id
                    .map(|label_id| *candidate_label_id == label_id)
                    .unwrap_or(true)
                    && candidate_property == property
                    && !values.contains(value)
            })
            .flat_map(|(_, node_ids)| node_ids.iter().copied())
            .collect()
    }

    fn node_ids_for_property_exists(
        &self,
        label_id: Option<LabelId>,
        property: &str,
    ) -> BTreeSet<NodeId> {
        self.property_index
            .iter()
            .filter(|((candidate_label_id, candidate_property, value), _)| {
                label_id
                    .map(|label_id| *candidate_label_id == label_id)
                    .unwrap_or(true)
                    && candidate_property == property
                    && value != &Value::Null
            })
            .flat_map(|(_, node_ids)| node_ids.iter().copied())
            .collect()
    }

    fn node_ids_for_property_missing_or_null(
        &self,
        label_id: Option<LabelId>,
        property: &str,
    ) -> BTreeSet<NodeId> {
        let non_null = self.node_ids_for_property_exists(label_id, property);
        self.nodes
            .values()
            .filter(|node| self.node_matches_label(node, label_id))
            .filter(|node| !non_null.contains(&node.id))
            .map(|node| node.id)
            .collect()
    }

    fn node_ids_for_default_if_null_eq(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        empty: &Value,
        default: &Value,
        value: &Value,
    ) -> BTreeSet<NodeId> {
        if value == default {
            let mut node_ids = self.node_ids_for_property_missing_or_null(label_id, property);
            let mut values = vec![empty.clone()];
            if value != empty {
                values.push(value.clone());
            }
            node_ids.extend(self.node_ids_for_property_values(label_id, property, &values));
            return node_ids;
        }

        if value == empty || value == &Value::Null {
            return BTreeSet::new();
        }
        self.node_ids_for_property_values(label_id, property, std::slice::from_ref(value))
    }

    fn node_ids_for_default_if_null_not_eq(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        empty: &Value,
        default: &Value,
        value: &Value,
    ) -> BTreeSet<NodeId> {
        let equal_node_ids =
            self.node_ids_for_default_if_null_eq(label_id, property, empty, default, value);
        self.node_ids_for_label(label_id)
            .difference(&equal_node_ids)
            .copied()
            .collect()
    }

    fn node_ids_for_property_range(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> BTreeSet<NodeId> {
        self.property_index
            .iter()
            .filter(|((candidate_label_id, candidate_property, value), _)| {
                label_id
                    .map(|label_id| *candidate_label_id == label_id)
                    .unwrap_or(true)
                    && candidate_property == property
                    && range_bounds_match(value, lower, upper)
            })
            .flat_map(|(_, node_ids)| node_ids.iter().copied())
            .collect()
    }

    pub fn seek_nodes_by_property<'a>(
        &'a self,
        label_id: LabelId,
        property: &str,
        value: &Value,
    ) -> impl Iterator<Item = &'a NodeRecord> + 'a {
        self.property_index
            .get(&(label_id, property.to_string(), value.clone()))
            .into_iter()
            .flat_map(|node_ids| node_ids.iter())
            .filter_map(|node_id| self.nodes.get(node_id))
    }

    pub fn visit_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[Value],
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if node
                    .properties
                    .get(property)
                    .is_some_and(|candidate| values.iter().any(|value| candidate == value))
                {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let mut seen = BTreeSet::new();
        let equality_projection =
            self.persistent_property_projection
                .as_ref()
                .filter(|projection| {
                    projection.manifest().supports(
                        label_id,
                        property,
                        PersistentPropertyProjectionKind::Equality,
                    )
                });
        for value in values {
            let mut graph_control = GraphScanControl::Continue;
            let canonical_control = if let Some(projection) = equality_projection {
                let (report, control) = projection
                    .scan_equality_candidates(label_id, property, value, |node_id| {
                        if self.node_tombstones.contains(&node_id)
                            || self.nodes.contains_key(&node_id)
                            || !seen.insert(node_id)
                        {
                            return Ok(CanonicalScanControl::Continue);
                        }
                        let node = reader.get_node(node_id)?.ok_or_else(|| {
                            PersistentPropertyProjectionError::Corrupt(format!(
                                "property projection references missing canonical node {}",
                                node_id.0
                            ))
                        })?;
                        if !node.labels.contains(&label_id)
                            || node.properties.get(property) != Some(value)
                        {
                            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                                "property projection candidate {} fails its canonical equality predicate",
                                node_id.0
                            )));
                        }
                        if consumer(node) == GraphScanControl::Stop {
                            graph_control = GraphScanControl::Stop;
                            return Ok(CanonicalScanControl::Stop);
                        }
                        Ok(CanonicalScanControl::Continue)
                    })
                    .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
                self.graph_index_read_metrics
                    .record_property(PersistentGraphIndexClass::NodeEquality, report);
                control
            } else {
                let (_, control) = reader
                    .scan_nodes_by_property_control(label_id, property, value, |node| {
                        if self.node_tombstones.contains(&node.id)
                            || self.nodes.contains_key(&node.id)
                            || !seen.insert(node.id)
                        {
                            return Ok(CanonicalScanControl::Continue);
                        }
                        if consumer(node) == GraphScanControl::Stop {
                            graph_control = GraphScanControl::Stop;
                            return Ok(CanonicalScanControl::Stop);
                        }
                        Ok(CanonicalScanControl::Continue)
                    })
                    .map_err(canonical_segment_error)?;
                control
            };
            if canonical_control == CanonicalScanControl::Stop {
                return Ok(graph_control);
            }
        }
        for node in self.nodes.values() {
            if node.labels.contains(&label_id)
                && node
                    .properties
                    .get(property)
                    .is_some_and(|candidate| values.iter().any(|value| candidate == value))
                && consumer(node.clone()) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub(crate) fn visit_projected_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[Value],
        required_properties: &BTreeSet<String>,
        mut consumer: impl FnMut(ProjectedNodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            return self.visit_nodes_by_property_owned(label_id, property, values, |node| {
                consumer(project_node_record(node, required_properties))
            });
        };
        let Some(projection) = self
            .persistent_property_projection
            .as_ref()
            .filter(|projection| {
                projection.manifest().supports(
                    label_id,
                    property,
                    PersistentPropertyProjectionKind::Equality,
                )
            })
        else {
            return self.visit_nodes_by_property_owned(label_id, property, values, |node| {
                consumer(project_node_record(node, required_properties))
            });
        };

        let mut decode_properties = required_properties.clone();
        decode_properties.insert(property.to_string());
        let mut seen = BTreeSet::new();
        for value in values {
            let mut graph_control = GraphScanControl::Continue;
            let (report, projection_control) = projection
                .scan_equality_candidates(label_id, property, value, |node_id| {
                    if self.node_tombstones.contains(&node_id)
                        || self.nodes.contains_key(&node_id)
                        || !seen.insert(node_id)
                    {
                        return Ok(CanonicalScanControl::Continue);
                    }
                    let node = reader
                        .get_projected_node(node_id, &decode_properties)?
                        .ok_or_else(|| {
                            PersistentPropertyProjectionError::Corrupt(format!(
                                "property projection references missing canonical node {}",
                                node_id.0
                            ))
                        })?;
                    if !node.labels.contains(&label_id)
                        || node.properties.get(property) != Some(value)
                    {
                        return Err(PersistentPropertyProjectionError::Corrupt(format!(
                            "property projection candidate {} fails its canonical equality predicate",
                            node_id.0
                        )));
                    }
                    if consumer(node) == GraphScanControl::Stop {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                    Ok(CanonicalScanControl::Continue)
                })
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
            self.graph_index_read_metrics
                .record_property(PersistentGraphIndexClass::NodeEquality, report);
            if projection_control == CanonicalScanControl::Stop {
                return Ok(graph_control);
            }
        }
        for node in self.nodes.values() {
            if node.labels.contains(&label_id)
                && node
                    .properties
                    .get(property)
                    .is_some_and(|candidate| values.iter().any(|value| candidate == value))
                && consumer(project_node_record(node.clone(), &decode_properties))
                    == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn seek_nodes_by_composite_property<'a>(
        &'a self,
        label_id: LabelId,
        predicates: &[(String, Value)],
    ) -> Vec<&'a NodeRecord> {
        self.composite_property_index
            .get(&(label_id, predicates.to_vec()))
            .into_iter()
            .flat_map(|node_ids| node_ids.iter())
            .filter_map(|node_id| self.nodes.get(node_id))
            .collect()
    }

    pub fn visit_nodes_by_composite_property_owned(
        &self,
        label_id: LabelId,
        predicates: &[(String, Value)],
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some((first_property, first_value)) = predicates.first() else {
            return self.visit_nodes_owned(Some(label_id), consumer);
        };
        if let (Some(reader), Some(projection)) = (
            self.canonical_base.as_ref(),
            self.persistent_property_projection.as_ref(),
        ) {
            let properties = predicates
                .iter()
                .map(|(property, _)| property.clone())
                .collect::<Vec<_>>();
            if projection
                .manifest()
                .supports_composite_equality(label_id, &properties)
            {
                let values = predicates
                    .iter()
                    .map(|(_, value)| value)
                    .collect::<Vec<_>>();
                let mut graph_control = GraphScanControl::Continue;
                let (report, projection_control) = projection
                    .scan_composite_equality_candidates(
                        label_id,
                        &properties,
                        &values,
                        |node_id| {
                            if self.node_tombstones.contains(&node_id)
                                || self.nodes.contains_key(&node_id)
                            {
                                return Ok(CanonicalScanControl::Continue);
                            }
                            let node = reader.get_node(node_id)?.ok_or_else(|| {
                                PersistentPropertyProjectionError::Corrupt(format!(
                                    "composite property projection references missing canonical node {}",
                                    node_id.0
                                ))
                            })?;
                            if !node.labels.contains(&label_id)
                                || !predicates.iter().all(|(property, value)| {
                                    node.properties.get(property) == Some(value)
                                })
                            {
                                return Err(PersistentPropertyProjectionError::Corrupt(format!(
                                    "composite property projection candidate {} fails its canonical predicate",
                                    node_id.0
                                )));
                            }
                            if consumer(node) == GraphScanControl::Stop {
                                graph_control = GraphScanControl::Stop;
                                return Ok(CanonicalScanControl::Stop);
                            }
                            Ok(CanonicalScanControl::Continue)
                        },
                    )
                    .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
                self.graph_index_read_metrics
                    .record_property(PersistentGraphIndexClass::NodeCompositeEquality, report);
                if projection_control == CanonicalScanControl::Stop {
                    return Ok(graph_control);
                }
                for node in self.nodes.values() {
                    if node.labels.contains(&label_id)
                        && predicates
                            .iter()
                            .all(|(property, value)| node.properties.get(property) == Some(value))
                        && consumer(node.clone()) == GraphScanControl::Stop
                    {
                        return Ok(GraphScanControl::Stop);
                    }
                }
                return Ok(GraphScanControl::Continue);
            }
        }
        self.visit_nodes_by_property_owned(
            label_id,
            first_property,
            std::slice::from_ref(first_value),
            |node| {
                if predicates
                    .iter()
                    .all(|(property, value)| node.properties.get(property) == Some(value))
                {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            },
        )
    }

    fn visit_projected_nodes_by_composite_property_owned(
        &self,
        label_id: LabelId,
        predicates: &[(String, Value)],
        required_properties: &BTreeSet<String>,
        mut consumer: impl FnMut(ProjectedNodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let properties = predicates
            .iter()
            .map(|(property, _)| property.clone())
            .collect::<Vec<_>>();
        let Some((reader, projection)) = self
            .canonical_base
            .as_ref()
            .zip(self.persistent_property_projection.as_ref())
            .filter(|(_, projection)| {
                projection
                    .manifest()
                    .supports_composite_equality(label_id, &properties)
            })
        else {
            return self.visit_nodes_by_composite_property_owned(label_id, predicates, |node| {
                consumer(project_node_record(node, required_properties))
            });
        };

        let mut decode_properties = required_properties.clone();
        decode_properties.extend(properties.iter().cloned());
        let values = predicates
            .iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        let mut graph_control = GraphScanControl::Continue;
        let (report, projection_control) = projection
            .scan_composite_equality_candidates(label_id, &properties, &values, |node_id| {
                if self.node_tombstones.contains(&node_id) || self.nodes.contains_key(&node_id) {
                    return Ok(CanonicalScanControl::Continue);
                }
                let node = reader
                    .get_projected_node(node_id, &decode_properties)?
                    .ok_or_else(|| {
                        PersistentPropertyProjectionError::Corrupt(format!(
                            "composite property projection references missing canonical node {}",
                            node_id.0
                        ))
                    })?;
                if !node.labels.contains(&label_id)
                    || !predicates
                        .iter()
                        .all(|(property, value)| node.properties.get(property) == Some(value))
                {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "composite property projection candidate {} fails its canonical predicate",
                        node_id.0
                    )));
                }
                if consumer(node) == GraphScanControl::Stop {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        self.graph_index_read_metrics
            .record_property(PersistentGraphIndexClass::NodeCompositeEquality, report);
        if projection_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for node in self.nodes.values() {
            if node.labels.contains(&label_id)
                && predicates
                    .iter()
                    .all(|(property, value)| node.properties.get(property) == Some(value))
                && consumer(project_node_record(node.clone(), &decode_properties))
                    == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn visit_nodes_by_composite_range_owned(
        &self,
        label_id: LabelId,
        seek: &hawdb_plan::CompositeRangeSeek,
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        validate_composite_range_seek(seek)?;
        if let (Some(reader), Some(projection)) = (
            self.canonical_base.as_ref(),
            self.persistent_property_projection.as_ref(),
        ) && projection
            .manifest()
            .supports_composite_equality(label_id, &seek.index_properties)
        {
            let equality_values = seek
                .equality_prefix
                .iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            let mut graph_control = GraphScanControl::Continue;
            let (report, projection_control) = projection
                .scan_composite_range_candidates(
                    label_id,
                    &seek.index_properties,
                    &equality_values,
                    seek.lower.as_ref(),
                    seek.upper.as_ref(),
                    |node_id| {
                        if self.node_tombstones.contains(&node_id)
                            || self.nodes.contains_key(&node_id)
                        {
                            return Ok(CanonicalScanControl::Continue);
                        }
                        let node = reader.get_node(node_id)?.ok_or_else(|| {
                            PersistentPropertyProjectionError::Corrupt(format!(
                                "composite range projection references missing canonical node {}",
                                node_id.0
                            ))
                        })?;
                        if !node.labels.contains(&label_id)
                            || !node_matches_composite_range(&node, seek)
                        {
                            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                                "composite range projection candidate {} fails its canonical predicate",
                                node_id.0
                            )));
                        }
                        if consumer(node) == GraphScanControl::Stop {
                            graph_control = GraphScanControl::Stop;
                            return Ok(CanonicalScanControl::Stop);
                        }
                        Ok(CanonicalScanControl::Continue)
                    },
                )
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
            self.graph_index_read_metrics
                .record_property(PersistentGraphIndexClass::NodeCompositeEquality, report);
            if projection_control == CanonicalScanControl::Stop {
                return Ok(graph_control);
            }
            for node in self.nodes.values() {
                if node.labels.contains(&label_id)
                    && node_matches_composite_range(node, seek)
                    && consumer(node.clone()) == GraphScanControl::Stop
                {
                    return Ok(GraphScanControl::Stop);
                }
            }
            return Ok(GraphScanControl::Continue);
        }

        let (first_property, first_value) = seek
            .equality_prefix
            .first()
            .expect("validated composite range equality prefix");
        self.visit_nodes_by_property_owned(
            label_id,
            first_property,
            std::slice::from_ref(first_value),
            |node| {
                if node_matches_composite_range(&node, seek) {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            },
        )
    }

    fn visit_projected_nodes_by_composite_range_owned(
        &self,
        label_id: LabelId,
        seek: &hawdb_plan::CompositeRangeSeek,
        required_properties: &BTreeSet<String>,
        mut consumer: impl FnMut(ProjectedNodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        validate_composite_range_seek(seek)?;
        let Some((reader, projection)) = self
            .canonical_base
            .as_ref()
            .zip(self.persistent_property_projection.as_ref())
            .filter(|(_, projection)| {
                projection
                    .manifest()
                    .supports_composite_equality(label_id, &seek.index_properties)
            })
        else {
            return self.visit_nodes_by_composite_range_owned(label_id, seek, |node| {
                consumer(project_node_record(node, required_properties))
            });
        };

        let mut decode_properties = required_properties.clone();
        decode_properties.extend(
            seek.equality_prefix
                .iter()
                .map(|(property, _)| property.clone()),
        );
        decode_properties.insert(seek.range_property.clone());
        let equality_values = seek
            .equality_prefix
            .iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        let mut graph_control = GraphScanControl::Continue;
        let (report, projection_control) = projection
            .scan_composite_range_candidates(
                label_id,
                &seek.index_properties,
                &equality_values,
                seek.lower.as_ref(),
                seek.upper.as_ref(),
                |node_id| {
                    if self.node_tombstones.contains(&node_id) || self.nodes.contains_key(&node_id)
                    {
                        return Ok(CanonicalScanControl::Continue);
                    }
                    let node = reader
                        .get_projected_node(node_id, &decode_properties)?
                        .ok_or_else(|| {
                            PersistentPropertyProjectionError::Corrupt(format!(
                                "composite range projection references missing canonical node {}",
                                node_id.0
                            ))
                        })?;
                    if !node.labels.contains(&label_id)
                        || !projected_node_matches_composite_range(&node, seek)
                    {
                        return Err(PersistentPropertyProjectionError::Corrupt(format!(
                            "composite range projection candidate {} fails its canonical predicate",
                            node_id.0
                        )));
                    }
                    if consumer(node) == GraphScanControl::Stop {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        self.graph_index_read_metrics
            .record_property(PersistentGraphIndexClass::NodeCompositeEquality, report);
        if projection_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for node in self.nodes.values() {
            if node.labels.contains(&label_id) && node_matches_composite_range(node, seek) {
                let node = project_node_record(node.clone(), &decode_properties);
                if consumer(node) == GraphScanControl::Stop {
                    return Ok(GraphScanControl::Stop);
                }
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn seek_nodes_by_property_range<'a>(
        &'a self,
        label_id: LabelId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> Vec<&'a NodeRecord> {
        self.property_index
            .iter()
            .filter_map(
                |((candidate_label_id, candidate_property, value), node_ids)| {
                    if *candidate_label_id != label_id || candidate_property != property {
                        return None;
                    }
                    if range_bounds_match(value, lower, upper) {
                        Some(node_ids)
                    } else {
                        None
                    }
                },
            )
            .flat_map(|node_ids| node_ids.iter())
            .filter_map(|node_id| self.nodes.get(node_id))
            .collect()
    }

    pub fn visit_nodes_by_property_range_owned(
        &self,
        label_id: LabelId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if node
                    .properties
                    .get(property)
                    .is_some_and(|value| range_bounds_match(value, lower, upper))
                {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let Some(projection) = self
            .persistent_property_projection
            .as_ref()
            .filter(|projection| {
                projection.manifest().supports(
                    label_id,
                    property,
                    PersistentPropertyProjectionKind::Range,
                )
            })
        else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if node
                    .properties
                    .get(property)
                    .is_some_and(|value| range_bounds_match(value, lower, upper))
                {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };

        let mut graph_control = GraphScanControl::Continue;
        let (report, projection_control) = projection
            .scan_range_candidates(label_id, property, lower, upper, |node_id| {
                if self.node_tombstones.contains(&node_id) || self.nodes.contains_key(&node_id) {
                    return Ok(CanonicalScanControl::Continue);
                }
                let node = reader.get_node(node_id)?.ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection references missing canonical node {}",
                        node_id.0
                    ))
                })?;
                if !node.labels.contains(&label_id)
                    || !node
                        .properties
                        .get(property)
                        .is_some_and(|value| range_bounds_match(value, lower, upper))
                {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection candidate {} fails its canonical range predicate",
                        node_id.0
                    )));
                }
                if consumer(node) == GraphScanControl::Stop {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        self.graph_index_read_metrics
            .record_property(PersistentGraphIndexClass::NodeRange, report);
        if projection_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for node in self.nodes.values() {
            if self.node_tombstones.contains(&node.id) {
                continue;
            }
            if node.labels.contains(&label_id)
                && node
                    .properties
                    .get(property)
                    .is_some_and(|value| range_bounds_match(value, lower, upper))
                && consumer(node.clone()) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    fn visit_projected_nodes_by_property_range_owned(
        &self,
        label_id: LabelId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
        required_properties: &BTreeSet<String>,
        mut consumer: impl FnMut(ProjectedNodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some((reader, projection)) = self
            .canonical_base
            .as_ref()
            .zip(self.persistent_property_projection.as_ref())
            .filter(|(_, projection)| {
                projection.manifest().supports(
                    label_id,
                    property,
                    PersistentPropertyProjectionKind::Range,
                )
            })
        else {
            return self.visit_nodes_by_property_range_owned(
                label_id,
                property,
                lower,
                upper,
                |node| consumer(project_node_record(node, required_properties)),
            );
        };

        let mut decode_properties = required_properties.clone();
        decode_properties.insert(property.to_string());
        let mut graph_control = GraphScanControl::Continue;
        let (report, projection_control) = projection
            .scan_range_candidates(label_id, property, lower, upper, |node_id| {
                if self.node_tombstones.contains(&node_id) || self.nodes.contains_key(&node_id) {
                    return Ok(CanonicalScanControl::Continue);
                }
                let node = reader
                    .get_projected_node(node_id, &decode_properties)?
                    .ok_or_else(|| {
                        PersistentPropertyProjectionError::Corrupt(format!(
                            "property projection references missing canonical node {}",
                            node_id.0
                        ))
                    })?;
                if !node.labels.contains(&label_id)
                    || !node
                        .properties
                        .get(property)
                        .is_some_and(|value| range_bounds_match(value, lower, upper))
                {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection candidate {} fails its canonical range predicate",
                        node_id.0
                    )));
                }
                if consumer(node) == GraphScanControl::Stop {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        self.graph_index_read_metrics
            .record_property(PersistentGraphIndexClass::NodeRange, report);
        if projection_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for node in self.nodes.values() {
            if self.node_tombstones.contains(&node.id) {
                continue;
            }
            if node.labels.contains(&label_id)
                && node
                    .properties
                    .get(property)
                    .is_some_and(|value| range_bounds_match(value, lower, upper))
                && consumer(project_node_record(node.clone(), &decode_properties))
                    == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn seek_nodes_by_full_text_property<'a>(
        &'a self,
        label_id: LabelId,
        property: &str,
        query: &str,
    ) -> Vec<&'a NodeRecord> {
        let tokens = full_text_query_tokens(query);
        let Some((first, rest)) = tokens.split_first() else {
            return Vec::new();
        };
        let mut candidates = self
            .full_text_property_index
            .get(&(label_id, property.to_string(), first.clone()))
            .map(|node_ids| node_ids.iter().copied().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        for token in rest {
            let Some(ids) =
                self.full_text_property_index
                    .get(&(label_id, property.to_string(), token.clone()))
            else {
                return Vec::new();
            };
            candidates = candidates.intersection(ids).copied().collect();
            if candidates.is_empty() {
                return Vec::new();
            }
        }
        candidates
            .into_iter()
            .filter_map(|node_id| self.nodes.get(&node_id))
            .collect()
    }

    pub fn visit_nodes_by_full_text_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        query: &str,
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let query_tokens = full_text_query_tokens(query);
        if query_tokens.is_empty() {
            return Ok(GraphScanControl::Continue);
        }
        let matches_query = |node: &NodeRecord| match node.properties.get(property) {
            Some(Value::String(value)) => {
                let tokens = full_text_index_tokens(value);
                query_tokens.iter().all(|token| tokens.contains(token))
            }
            _ => false,
        };
        let Some(reader) = &self.canonical_base else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if matches_query(&node) {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let Some(projection) = self
            .persistent_property_projection
            .as_ref()
            .filter(|projection| {
                projection.manifest().supports(
                    label_id,
                    property,
                    PersistentPropertyProjectionKind::FullText,
                )
            })
        else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if matches_query(&node) {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let mut seed_token = None;
        let mut estimate_report = hawdb_storage::PersistentPropertyProjectionReadReport::default();
        for token in &query_tokens {
            let (estimated_entries, report) = projection
                .estimate_full_text_token_entries(label_id, property, token)
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
            accumulate_property_projection_report(&mut estimate_report, report);
            if seed_token
                .as_ref()
                .is_none_or(|(_, current_entries)| estimated_entries < *current_entries)
            {
                seed_token = Some((token, estimated_entries));
            }
        }
        let seed_token = seed_token
            .map(|(token, _)| token)
            .expect("non-empty full-text query has a seed token");
        let mut graph_control = GraphScanControl::Continue;
        let (report, projection_control) = projection
            .scan_full_text_token_candidates(label_id, property, seed_token, |node_id| {
                if self.node_tombstones.contains(&node_id) || self.nodes.contains_key(&node_id) {
                    return Ok(CanonicalScanControl::Continue);
                }
                let node = reader.get_node(node_id)?.ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection references missing canonical node {}",
                        node_id.0
                    ))
                })?;
                if !node.labels.contains(&label_id) || !matches_query(&node) {
                    return Ok(CanonicalScanControl::Continue);
                }
                if consumer(node) == GraphScanControl::Stop {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        accumulate_property_projection_report(&mut estimate_report, report);
        self.graph_index_read_metrics
            .record_property(PersistentGraphIndexClass::NodeFullText, estimate_report);
        if projection_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for node in self.nodes.values() {
            if self.node_tombstones.contains(&node.id) {
                continue;
            }
            if node.labels.contains(&label_id)
                && matches_query(node)
                && consumer(node.clone()) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    fn visit_projected_nodes_by_full_text_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        query: &str,
        required_properties: &BTreeSet<String>,
        mut consumer: impl FnMut(ProjectedNodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let query_tokens = full_text_query_tokens(query);
        if query_tokens.is_empty() {
            return Ok(GraphScanControl::Continue);
        }
        let Some((reader, projection)) = self
            .canonical_base
            .as_ref()
            .zip(self.persistent_property_projection.as_ref())
            .filter(|(_, projection)| {
                projection.manifest().supports(
                    label_id,
                    property,
                    PersistentPropertyProjectionKind::FullText,
                )
            })
        else {
            return self.visit_nodes_by_full_text_property_owned(
                label_id,
                property,
                query,
                |node| consumer(project_node_record(node, required_properties)),
            );
        };

        let mut decode_properties = required_properties.clone();
        decode_properties.insert(property.to_string());
        let matches_query = |properties: &BTreeMap<String, Value>| match properties.get(property) {
            Some(Value::String(value)) => {
                let tokens = full_text_index_tokens(value);
                query_tokens.iter().all(|token| tokens.contains(token))
            }
            _ => false,
        };
        let mut seed_token = None;
        let mut estimate_report = hawdb_storage::PersistentPropertyProjectionReadReport::default();
        for token in &query_tokens {
            let (estimated_entries, report) = projection
                .estimate_full_text_token_entries(label_id, property, token)
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
            accumulate_property_projection_report(&mut estimate_report, report);
            if seed_token
                .as_ref()
                .is_none_or(|(_, current_entries)| estimated_entries < *current_entries)
            {
                seed_token = Some((token, estimated_entries));
            }
        }
        let seed_token = seed_token
            .map(|(token, _)| token)
            .expect("non-empty full-text query has a seed token");
        let mut graph_control = GraphScanControl::Continue;
        let (report, projection_control) = projection
            .scan_full_text_token_candidates(label_id, property, seed_token, |node_id| {
                if self.node_tombstones.contains(&node_id) || self.nodes.contains_key(&node_id) {
                    return Ok(CanonicalScanControl::Continue);
                }
                let node = reader
                    .get_projected_node(node_id, &decode_properties)?
                    .ok_or_else(|| {
                        PersistentPropertyProjectionError::Corrupt(format!(
                            "property projection references missing canonical node {}",
                            node_id.0
                        ))
                    })?;
                if !node.labels.contains(&label_id) || !matches_query(&node.properties) {
                    return Ok(CanonicalScanControl::Continue);
                }
                if consumer(node) == GraphScanControl::Stop {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        accumulate_property_projection_report(&mut estimate_report, report);
        self.graph_index_read_metrics
            .record_property(PersistentGraphIndexClass::NodeFullText, estimate_report);
        if projection_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for node in self.nodes.values() {
            if self.node_tombstones.contains(&node.id) {
                continue;
            }
            if node.labels.contains(&label_id)
                && matches_query(&node.properties)
                && consumer(project_node_record(node.clone(), &decode_properties))
                    == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    #[inline]
    pub fn outgoing_relationships<'a>(
        &'a self,
        source: NodeId,
        rel_type: RelTypeId,
    ) -> impl Iterator<Item = &'a RelRecord> + 'a {
        self.outgoing
            .get(&(source, rel_type))
            .into_iter()
            .flat_map(AdjacencyPostingList::iter_copied)
            .filter_map(|entry| self.relationships.get(&entry.relationship_id))
    }

    #[inline]
    pub fn incoming_relationships<'a>(
        &'a self,
        target: NodeId,
        rel_type: RelTypeId,
    ) -> impl Iterator<Item = &'a RelRecord> + 'a {
        self.incoming
            .get(&(target, rel_type))
            .into_iter()
            .flat_map(AdjacencyPostingList::iter_copied)
            .filter_map(|entry| self.relationships.get(&entry.relationship_id))
    }

    pub fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        mut consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            if let Some(rel_type) = rel_type {
                return self.visit_live_adjacent_relationships_owned(
                    node_id, rel_type, direction, consumer,
                );
            }
            return self.visit_relationships_owned(rel_type, |relationship| {
                let adjacent = match direction {
                    AdjacencyDirection::Outgoing => relationship.source == node_id,
                    AdjacencyDirection::Incoming => relationship.target == node_id,
                };
                if adjacent {
                    consumer(relationship)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let mut graph_control = GraphScanControl::Continue;
        let canonical_control = {
            let mut consume_canonical = |relationship: RelRecord| {
                if self.relationship_tombstones.contains(&relationship.id)
                    || self.relationships.contains_key(&relationship.id)
                {
                    return CanonicalScanControl::Continue;
                }
                if consumer(relationship) == GraphScanControl::Stop {
                    graph_control = GraphScanControl::Stop;
                    CanonicalScanControl::Stop
                } else {
                    CanonicalScanControl::Continue
                }
            };
            if let Some(adjacency) = &self.canonical_adjacency {
                let (report, control) = adjacency
                    .scan_endpoint_entries_control(node_id, direction, rel_type, |entry| {
                        let relationship = match entry {
                            CanonicalAdjacencyEntry::Inline(relationship) => relationship,
                            CanonicalAdjacencyEntry::CanonicalReference { relationship_id } => {
                                reader
                                    .get_relationship(relationship_id)
                                    .map_err(|error| {
                                        hawdb_storage::CanonicalAdjacencyError::Source(
                                            error.to_string(),
                                        )
                                    })?
                                    .ok_or_else(|| {
                                        hawdb_storage::CanonicalAdjacencyError::Corrupt(format!(
                                            "canonical adjacency references missing relationship {}",
                                            relationship_id.0
                                        ))
                                    })?
                            }
                        };
                        Ok(consume_canonical(relationship))
                    })
                    .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
                let class = match direction {
                    AdjacencyDirection::Outgoing => PersistentGraphIndexClass::ForwardAdjacency,
                    AdjacencyDirection::Incoming => PersistentGraphIndexClass::ReverseAdjacency,
                };
                self.graph_index_read_metrics
                    .record_adjacency(class, report);
                control
            } else {
                let endpoint_direction = match direction {
                    AdjacencyDirection::Outgoing => CanonicalEndpointDirection::Source,
                    AdjacencyDirection::Incoming => CanonicalEndpointDirection::Target,
                };
                reader
                    .scan_relationships_for_endpoint_control(
                        node_id,
                        endpoint_direction,
                        rel_type,
                        |relationship| Ok(consume_canonical(relationship)),
                    )
                    .map_err(canonical_segment_error)?
                    .1
            }
        };
        if canonical_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        if let Some(rel_type) = rel_type {
            return self
                .visit_live_adjacent_relationships_owned(node_id, rel_type, direction, consumer);
        }
        for relationship in self.relationships.values() {
            if rel_type.is_some_and(|rel_type| relationship.rel_type != rel_type) {
                continue;
            }
            let adjacent = match direction {
                AdjacencyDirection::Outgoing => relationship.source == node_id,
                AdjacencyDirection::Incoming => relationship.target == node_id,
            };
            if adjacent && consumer(relationship.clone()) == GraphScanControl::Stop {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    fn visit_live_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
        mut consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(entries) = self.adjacency_relationship_ids(node_id, rel_type, direction) else {
            return Ok(GraphScanControl::Continue);
        };
        for entry in entries.iter_copied() {
            let Some(relationship) = self.relationships.get(&entry.relationship_id) else {
                return Err(HawDBError::StorageIntegrity(format!(
                    "live adjacency references missing relationship {}",
                    entry.relationship_id.0
                )));
            };
            if consumer(relationship.clone()) == GraphScanControl::Stop {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn try_visit_ordered_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        memory_budget_bytes: usize,
        consumer: impl FnMut(RelRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        self.try_visit_ordered_adjacent_relationships_accounted(
            node_id,
            rel_type,
            direction,
            memory_budget_bytes,
            |_| Ok(()),
            consumer,
        )
    }

    pub(crate) fn try_visit_ordered_adjacent_relationships_accounted(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        memory_budget_bytes: usize,
        charge_key_bytes: impl FnMut(usize) -> Result<()>,
        mut consumer: impl FnMut(RelRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let Some(rel_type) = rel_type else {
            return self.try_visit_compact_sorted_adjacency(
                node_id,
                None,
                direction,
                memory_budget_bytes,
                charge_key_bytes,
                consumer,
            );
        };
        let mut live_entries = self
            .adjacency_relationship_ids(node_id, rel_type, direction)
            .into_iter()
            .flat_map(AdjacencyPostingList::iter_copied)
            .peekable();
        let (Some(reader), Some(adjacency)) = (
            self.canonical_base.as_ref(),
            self.canonical_adjacency.as_ref(),
        ) else {
            return emit_live_adjacency_before(self, &mut live_entries, None, &mut consumer);
        };

        let mut graph_control = GraphScanControl::Continue;
        let mut consumer_error = None;
        let (report, canonical_control) = adjacency
            .scan_endpoint_entries_control(node_id, direction, Some(rel_type), |entry| {
                let relationship = match entry {
                    CanonicalAdjacencyEntry::Inline(relationship) => relationship,
                    CanonicalAdjacencyEntry::CanonicalReference { relationship_id } => reader
                        .get_relationship(relationship_id)
                        .map_err(|error| {
                            hawdb_storage::CanonicalAdjacencyError::Source(error.to_string())
                        })?
                        .ok_or_else(|| {
                            hawdb_storage::CanonicalAdjacencyError::Corrupt(format!(
                                "canonical adjacency references missing relationship {}",
                                relationship_id.0
                            ))
                        })?,
                };
                if self.relationship_tombstones.contains(&relationship.id)
                    || self.relationships.contains_key(&relationship.id)
                {
                    return Ok(CanonicalScanControl::Continue);
                }
                let canonical_key = ordered_relationship_key(&relationship, direction);
                match emit_live_adjacency_before(
                    self,
                    &mut live_entries,
                    Some(canonical_key),
                    &mut consumer,
                ) {
                    Ok(GraphScanControl::Continue) => {}
                    Ok(GraphScanControl::Stop) => {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                    Err(error) => {
                        consumer_error = Some(error);
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                }
                match consumer(relationship) {
                    Ok(GraphScanControl::Continue) => Ok(CanonicalScanControl::Continue),
                    Ok(GraphScanControl::Stop) => {
                        graph_control = GraphScanControl::Stop;
                        Ok(CanonicalScanControl::Stop)
                    }
                    Err(error) => {
                        consumer_error = Some(error);
                        graph_control = GraphScanControl::Stop;
                        Ok(CanonicalScanControl::Stop)
                    }
                }
            })
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        self.graph_index_read_metrics.record_adjacency(
            match direction {
                AdjacencyDirection::Outgoing => PersistentGraphIndexClass::ForwardAdjacency,
                AdjacencyDirection::Incoming => PersistentGraphIndexClass::ReverseAdjacency,
            },
            report,
        );
        if let Some(error) = consumer_error {
            return Err(error);
        }
        if canonical_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        emit_live_adjacency_before(self, &mut live_entries, None, &mut consumer)
    }

    fn try_visit_compact_sorted_adjacency(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        memory_budget_bytes: usize,
        mut charge_key_bytes: impl FnMut(usize) -> Result<()>,
        mut consumer: impl FnMut(RelRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let mut entries = Vec::new();
        self.try_visit_adjacent_relationships_owned(
            node_id,
            rel_type,
            direction,
            |relationship| {
                push_compact_adjacency_key(
                    &mut entries,
                    ordered_relationship_key(&relationship, direction),
                    memory_budget_bytes,
                    &mut charge_key_bytes,
                )?;
                Ok(GraphScanControl::Continue)
            },
        )?;
        entries.sort_unstable();
        let mut index = 0usize;
        emit_compact_adjacency_before(self, &entries, &mut index, None, &mut consumer)
    }

    pub fn visit_adjacent_relationships_with_filter_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        filter: &PropertyFilter,
        mut consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<(GraphScanControl, Option<ScanPruningReport>)> {
        let Some(rel_type) = rel_type else {
            return self.visit_adjacent_relationships_filter_fallback(
                node_id, None, direction, filter, consumer,
            );
        };
        let (Some(reader), Some(adjacency), Some(projection)) = (
            self.canonical_base.as_ref(),
            self.canonical_adjacency.as_ref(),
            self.persistent_property_projection.as_ref(),
        ) else {
            return self.visit_adjacent_relationships_filter_fallback(
                node_id,
                Some(rel_type),
                direction,
                filter,
                consumer,
            );
        };
        let Some(probe) = relationship_projection_probe(projection, rel_type, filter)
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?
        else {
            return self.visit_adjacent_relationships_filter_fallback(
                node_id,
                Some(rel_type),
                direction,
                filter,
                consumer,
            );
        };
        let adjacency_entries = adjacency
            .estimate_endpoint_entries(node_id, direction, Some(rel_type))
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        if probe.estimated_entries() > adjacency_entries {
            self.graph_index_read_metrics
                .record_property(probe.index_class(), probe.estimate_report());
            return self.visit_adjacent_relationships_filter_fallback(
                node_id,
                Some(rel_type),
                direction,
                filter,
                consumer,
            );
        }

        let mut graph_control = GraphScanControl::Continue;
        let mut candidate_count = 0usize;
        let mut output_count = 0usize;
        let (projection_read_report, projection_control) = probe
            .scan(projection, |relationship_id| {
                if self.relationship_tombstones.contains(&relationship_id)
                    || self.relationships.contains_key(&relationship_id)
                {
                    return Ok(CanonicalScanControl::Continue);
                }
                candidate_count = candidate_count.saturating_add(1);
                let relationship = reader
                    .get_relationship(relationship_id)?
                    .ok_or_else(|| {
                        PersistentPropertyProjectionError::Corrupt(format!(
                            "relationship property projection references missing canonical relationship {}",
                            relationship_id.0
                        ))
                    })?;
                if relationship.rel_type != rel_type || !probe.matches(&relationship) {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "relationship property projection candidate {} fails its canonical seek predicate",
                        relationship_id.0
                    )));
                }
                if relationship_matches_endpoint(&relationship, node_id, direction)
                    && property_filter_matches(
                        filter,
                        relationship.id.0,
                        &relationship.properties,
                    )
                {
                    output_count = output_count.saturating_add(1);
                    if consumer(relationship) == GraphScanControl::Stop {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        self.graph_index_read_metrics
            .record_property(probe.index_class(), projection_read_report);
        if projection_control == CanonicalScanControl::Stop {
            return Ok((graph_control, None));
        }
        for relationship in self.relationships.values() {
            if relationship.rel_type != rel_type || !probe.matches(relationship) {
                continue;
            }
            candidate_count = candidate_count.saturating_add(1);
            if relationship_matches_endpoint(relationship, node_id, direction)
                && property_filter_matches(filter, relationship.id.0, &relationship.properties)
            {
                output_count = output_count.saturating_add(1);
                if consumer(relationship.clone()) == GraphScanControl::Stop {
                    return Ok((GraphScanControl::Stop, None));
                }
            }
        }
        let candidate_count_before_pruning = self.relationship_count_for_type(Some(rel_type));
        Ok((
            GraphScanControl::Continue,
            Some(ScanPruningReport {
                target_kind: ScanPruningTargetKind::Relationship,
                label_id: None,
                rel_type_id: Some(rel_type),
                strategy: probe.strategy(),
                pruned: true,
                exact_empty: candidate_count == 0,
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning
                    .saturating_sub(candidate_count),
                candidate_count_before_filter: candidate_count,
                output_count,
                filtered_out_count: candidate_count.saturating_sub(output_count),
            }),
        ))
    }

    fn visit_adjacent_relationships_filter_fallback(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        filter: &PropertyFilter,
        mut consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<(GraphScanControl, Option<ScanPruningReport>)> {
        if self.canonical_base.is_none()
            && let Some(rel_type) = rel_type
            && let Some(candidate) = self.prune_relationship_candidates(Some(rel_type), filter)
        {
            let adjacency_entries = self
                .adjacency_relationship_ids(node_id, rel_type, direction)
                .map(AdjacencyPostingList::len)
                .unwrap_or_default();
            if candidate.rel_ids.len() <= adjacency_entries {
                let candidate_count_before_filter = candidate.rel_ids.len();
                let mut output_count = 0usize;
                for relationship_id in &candidate.rel_ids {
                    let relationship =
                        self.relationships.get(relationship_id).ok_or_else(|| {
                            HawDBError::StorageIntegrity(format!(
                                "relationship property index references missing relationship {}",
                                relationship_id.0
                            ))
                        })?;
                    if !property_filter_matches(filter, relationship.id.0, &relationship.properties)
                    {
                        continue;
                    }
                    output_count = output_count.saturating_add(1);
                    if relationship_matches_endpoint(relationship, node_id, direction)
                        && consumer(relationship.clone()) == GraphScanControl::Stop
                    {
                        return Ok((GraphScanControl::Stop, None));
                    }
                }
                let candidate_count_before_pruning =
                    self.relationship_count_for_type(Some(rel_type));
                return Ok((
                    GraphScanControl::Continue,
                    Some(ScanPruningReport {
                        target_kind: ScanPruningTargetKind::Relationship,
                        label_id: None,
                        rel_type_id: Some(rel_type),
                        strategy: candidate.strategy,
                        pruned: true,
                        exact_empty: candidate.exact_empty,
                        candidate_count_before_pruning,
                        pruned_candidate_count: candidate_count_before_pruning
                            .saturating_sub(candidate_count_before_filter),
                        candidate_count_before_filter,
                        output_count,
                        filtered_out_count: candidate_count_before_filter
                            .saturating_sub(output_count),
                    }),
                ));
            }
        }
        self.visit_adjacent_relationships_owned(node_id, rel_type, direction, |relationship| {
            if property_filter_matches(filter, relationship.id.0, &relationship.properties) {
                consumer(relationship)
            } else {
                GraphScanControl::Continue
            }
        })
        .map(|control| (control, None))
    }

    pub fn try_visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        mut consumer: impl FnMut(RelRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let mut consumer_error = None;
        let control = self.visit_adjacent_relationships_owned(
            node_id,
            rel_type,
            direction,
            |relationship| match consumer(relationship) {
                Ok(control) => control,
                Err(error) => {
                    consumer_error = Some(error);
                    GraphScanControl::Stop
                }
            },
        )?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(control),
        }
    }

    pub fn adjacency_group_stats(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> AdjacencyGroupStats {
        let degree = self
            .adjacency_relationship_ids(node_id, rel_type, direction)
            .map(AdjacencyPostingList::len)
            .unwrap_or_default();
        AdjacencyGroupStats {
            node_id,
            rel_type,
            direction,
            degree,
            layout: adjacency_layout_for_degree(degree),
        }
    }

    pub fn adjacency_group_stats_for_node(
        &self,
        node_id: NodeId,
        direction: AdjacencyDirection,
    ) -> Vec<AdjacencyGroupStats> {
        let adjacency = match direction {
            AdjacencyDirection::Outgoing => &self.outgoing,
            AdjacencyDirection::Incoming => &self.incoming,
        };
        let mut stats = adjacency
            .iter()
            .filter_map(|((group_node, rel_type), rel_ids)| {
                (*group_node == node_id).then_some(AdjacencyGroupStats {
                    node_id,
                    rel_type: *rel_type,
                    direction,
                    degree: rel_ids.len(),
                    layout: adjacency_layout_for_degree(rel_ids.len()),
                })
            })
            .collect::<Vec<_>>();
        stats.sort_by_key(|stats| {
            (
                stats.rel_type,
                adjacency_direction_sort_key(stats.direction),
            )
        });
        stats
    }

    pub fn ordered_adjacency_entries(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> Vec<OrderedAdjacencyEntry> {
        self.adjacency_relationship_ids(node_id, rel_type, direction)
            .into_iter()
            .flat_map(AdjacencyPostingList::iter_copied)
            .collect()
    }

    pub fn ordered_adjacency_entries_for_node(
        &self,
        node_id: NodeId,
        direction: AdjacencyDirection,
    ) -> Vec<OrderedAdjacencyEntry> {
        let adjacency = match direction {
            AdjacencyDirection::Outgoing => &self.outgoing,
            AdjacencyDirection::Incoming => &self.incoming,
        };
        let mut entries = adjacency
            .iter()
            .filter(|((group_node, _), _)| *group_node == node_id)
            .flat_map(|(_, rel_ids)| rel_ids.iter_copied())
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.neighbor_id, entry.relationship_id));
        entries
    }

    pub fn scan_relationships<'a>(
        &'a self,
        rel_type: Option<RelTypeId>,
    ) -> impl Iterator<Item = &'a RelRecord> + 'a {
        self.relationships.values().filter(move |relationship| {
            rel_type
                .map(|rel_type| relationship.rel_type == rel_type)
                .unwrap_or(true)
        })
    }

    pub fn scan_relationships_with_filter_pruning<'a>(
        &'a self,
        rel_type: Option<RelTypeId>,
        filter: Option<&PropertyFilter>,
    ) -> ScanPrunedRelationshipScan<'a> {
        let candidate =
            filter.and_then(|filter| self.prune_relationship_candidates(rel_type, filter));
        let Some(candidate) = candidate else {
            let candidate_count_before_filter = self.relationship_count_for_type(rel_type);
            let relationships = self
                .scan_relationships(rel_type)
                .filter(|relationship| {
                    filter
                        .map(|filter| {
                            property_filter_matches(
                                filter,
                                relationship.id.0,
                                &relationship.properties,
                            )
                        })
                        .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            let output_count = relationships.len();
            return ScanPrunedRelationshipScan {
                relationships,
                report: ScanPruningReport {
                    target_kind: ScanPruningTargetKind::Relationship,
                    label_id: None,
                    rel_type_id: rel_type,
                    strategy: ScanPruningStrategy::FullLabelScan,
                    pruned: false,
                    exact_empty: false,
                    candidate_count_before_pruning: candidate_count_before_filter,
                    pruned_candidate_count: 0,
                    candidate_count_before_filter,
                    output_count,
                    filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
                },
            };
        };

        let candidate_count_before_pruning = self.relationship_count_for_type(rel_type);
        let candidate_count_before_filter = candidate.rel_ids.len();
        let relationships = candidate
            .rel_ids
            .iter()
            .filter_map(|rel_id| self.relationships.get(rel_id))
            .filter(|relationship| self.relationship_matches_type(relationship, rel_type))
            .filter(|relationship| {
                filter
                    .map(|filter| {
                        property_filter_matches(filter, relationship.id.0, &relationship.properties)
                    })
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        let output_count = relationships.len();
        ScanPrunedRelationshipScan {
            relationships,
            report: ScanPruningReport {
                target_kind: ScanPruningTargetKind::Relationship,
                label_id: None,
                rel_type_id: rel_type,
                strategy: candidate.strategy,
                pruned: true,
                exact_empty: candidate.exact_empty,
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning
                    .saturating_sub(candidate_count_before_filter),
                candidate_count_before_filter,
                output_count,
                filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
            },
        }
    }

    pub fn relationship_count_for_type(&self, rel_type: Option<RelTypeId>) -> usize {
        let count = rel_type.map_or(self.basic_statistics.relationship_count, |rel_type| {
            self.basic_statistics
                .rel_type_counts
                .get(&rel_type)
                .copied()
                .unwrap_or_default()
        });
        usize::try_from(count).unwrap_or(usize::MAX)
    }

    fn relationship_matches_type(
        &self,
        relationship: &RelRecord,
        rel_type: Option<RelTypeId>,
    ) -> bool {
        rel_type
            .map(|rel_type| relationship.rel_type == rel_type)
            .unwrap_or(true)
    }

    fn prune_relationship_candidates(
        &self,
        rel_type: Option<RelTypeId>,
        filter: &PropertyFilter,
    ) -> Option<RelationshipScanPruningCandidate> {
        match filter {
            PropertyFilter::And(filters) => {
                self.prune_and_relationship_candidates(rel_type, filters)
            }
            PropertyFilter::Or(filters) => self.prune_or_relationship_candidates(rel_type, filters),
            PropertyFilter::Not(_) => None,
            PropertyFilter::IdEq { value } => Some(RelationshipScanPruningCandidate::exact(
                ScanPruningStrategy::IdEq,
                self.rel_ids_for_id_values(rel_type, std::slice::from_ref(value)),
            )),
            PropertyFilter::IdNotEq { .. } => None,
            PropertyFilter::IdRange { lower, upper } => {
                if lower.is_none() && upper.is_none() {
                    return None;
                }
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::IdRange,
                    self.rel_ids_for_id_range(rel_type, lower.as_ref(), upper.as_ref()),
                ))
            }
            PropertyFilter::IdIn { values } => Some(RelationshipScanPruningCandidate::exact(
                if values.is_empty() {
                    ScanPruningStrategy::Empty
                } else {
                    ScanPruningStrategy::IdIn
                },
                self.rel_ids_for_id_values(rel_type, values),
            )),
            PropertyFilter::Eq { property, value } => {
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyEq {
                        property: property.clone(),
                    },
                    self.rel_ids_for_property_values(
                        rel_type,
                        property,
                        std::slice::from_ref(value),
                    ),
                ))
            }
            PropertyFilter::NotEq { property, value } => {
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyNotEq {
                        property: property.clone(),
                    },
                    self.rel_ids_for_property_not_in_values(
                        rel_type,
                        property,
                        std::slice::from_ref(value),
                    ),
                ))
            }
            PropertyFilter::IsNull { property } => Some(RelationshipScanPruningCandidate::exact(
                ScanPruningStrategy::PropertyMissingOrNull {
                    property: property.clone(),
                },
                self.rel_ids_for_property_missing_or_null(rel_type, property),
            )),
            PropertyFilter::IsNotNull { property } => {
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyExists {
                        property: property.clone(),
                    },
                    self.rel_ids_for_property_exists(rel_type, property),
                ))
            }
            PropertyFilter::ListContains { .. }
            | PropertyFilter::ListContainsLower { .. }
            | PropertyFilter::Contains { .. }
            | PropertyFilter::StartsWith { .. }
            | PropertyFilter::EndsWith { .. }
            | PropertyFilter::RegexMatch { .. } => None,
            PropertyFilter::DefaultIfNullOrEq {
                property,
                empty,
                default,
                value,
                negated,
            } => {
                let strategy = if *negated {
                    ScanPruningStrategy::PropertyDefaultIfNullNotEq {
                        property: property.clone(),
                    }
                } else {
                    ScanPruningStrategy::PropertyDefaultIfNullEq {
                        property: property.clone(),
                    }
                };
                let rel_ids = if *negated {
                    self.rel_ids_for_default_if_null_not_eq(
                        rel_type, property, empty, default, value,
                    )
                } else {
                    self.rel_ids_for_default_if_null_eq(rel_type, property, empty, default, value)
                };
                Some(RelationshipScanPruningCandidate::exact(strategy, rel_ids))
            }
            PropertyFilter::In { property, values } => {
                Some(RelationshipScanPruningCandidate::exact(
                    if values.is_empty() {
                        ScanPruningStrategy::Empty
                    } else {
                        ScanPruningStrategy::PropertyIn {
                            property: property.clone(),
                        }
                    },
                    self.rel_ids_for_property_values(rel_type, property, values),
                ))
            }
            PropertyFilter::Range {
                property,
                lower,
                upper,
            } => {
                if lower.is_none() && upper.is_none() {
                    return None;
                }
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyRange {
                        property: property.clone(),
                    },
                    self.rel_ids_for_property_range(
                        rel_type,
                        property,
                        lower.as_ref(),
                        upper.as_ref(),
                    ),
                ))
            }
        }
    }

    fn prune_and_relationship_candidates(
        &self,
        rel_type: Option<RelTypeId>,
        filters: &[PropertyFilter],
    ) -> Option<RelationshipScanPruningCandidate> {
        let mut best: Option<RelationshipScanPruningCandidate> = None;
        for filter in filters {
            let Some(candidate) = self.prune_relationship_candidates(rel_type, filter) else {
                continue;
            };
            if candidate.exact_empty {
                return Some(candidate);
            }
            if best
                .as_ref()
                .map(|best| candidate.rel_ids.len() < best.rel_ids.len())
                .unwrap_or(true)
            {
                best = Some(candidate);
            }
        }
        best
    }

    fn prune_or_relationship_candidates(
        &self,
        rel_type: Option<RelTypeId>,
        filters: &[PropertyFilter],
    ) -> Option<RelationshipScanPruningCandidate> {
        if filters.is_empty() {
            return Some(RelationshipScanPruningCandidate {
                strategy: ScanPruningStrategy::Empty,
                rel_ids: BTreeSet::new(),
                exact_empty: true,
            });
        }

        let mut rel_ids = BTreeSet::new();
        for filter in filters {
            let candidate = self.prune_relationship_candidates(rel_type, filter)?;
            rel_ids.extend(candidate.rel_ids);
        }
        Some(RelationshipScanPruningCandidate::exact(
            ScanPruningStrategy::OrUnion,
            rel_ids,
        ))
    }

    fn rel_ids_for_id_values(
        &self,
        rel_type: Option<RelTypeId>,
        values: &[Value],
    ) -> BTreeSet<RelId> {
        values
            .iter()
            .filter_map(|value| match value {
                Value::Int(value) => u64::try_from(*value).ok().map(RelId),
                _ => None,
            })
            .filter(|rel_id| {
                self.relationships
                    .get(rel_id)
                    .map(|relationship| self.relationship_matches_type(relationship, rel_type))
                    .unwrap_or(false)
            })
            .collect()
    }

    fn rel_ids_for_type(&self, rel_type: Option<RelTypeId>) -> BTreeSet<RelId> {
        self.relationships
            .values()
            .filter(|relationship| self.relationship_matches_type(relationship, rel_type))
            .map(|relationship| relationship.id)
            .collect()
    }

    fn rel_ids_for_id_range(
        &self,
        rel_type: Option<RelTypeId>,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> BTreeSet<RelId> {
        self.relationships
            .keys()
            .copied()
            .filter(|rel_id| range_bounds_match(&Value::Int(rel_id.0 as i64), lower, upper))
            .filter(|rel_id| {
                self.relationships
                    .get(rel_id)
                    .map(|relationship| self.relationship_matches_type(relationship, rel_type))
                    .unwrap_or(false)
            })
            .collect()
    }

    fn rel_ids_for_property_values(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        values: &[Value],
    ) -> BTreeSet<RelId> {
        if values.is_empty() {
            return BTreeSet::new();
        }
        let values = values.iter().collect::<BTreeSet<_>>();
        self.relationship_property_index
            .iter()
            .filter(|((candidate_rel_type, candidate_property, value), _)| {
                rel_type
                    .map(|rel_type| *candidate_rel_type == rel_type)
                    .unwrap_or(true)
                    && candidate_property == property
                    && values.contains(value)
            })
            .flat_map(|(_, rel_ids)| rel_ids.iter().copied())
            .collect()
    }

    fn rel_ids_for_property_not_in_values(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        values: &[Value],
    ) -> BTreeSet<RelId> {
        let values = values.iter().collect::<BTreeSet<_>>();
        self.relationship_property_index
            .iter()
            .filter(|((candidate_rel_type, candidate_property, value), _)| {
                rel_type
                    .map(|rel_type| *candidate_rel_type == rel_type)
                    .unwrap_or(true)
                    && candidate_property == property
                    && !values.contains(value)
            })
            .flat_map(|(_, rel_ids)| rel_ids.iter().copied())
            .collect()
    }

    fn rel_ids_for_property_exists(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
    ) -> BTreeSet<RelId> {
        self.relationship_property_index
            .iter()
            .filter(|((candidate_rel_type, candidate_property, value), _)| {
                rel_type
                    .map(|rel_type| *candidate_rel_type == rel_type)
                    .unwrap_or(true)
                    && candidate_property == property
                    && value != &Value::Null
            })
            .flat_map(|(_, rel_ids)| rel_ids.iter().copied())
            .collect()
    }

    fn rel_ids_for_property_missing_or_null(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
    ) -> BTreeSet<RelId> {
        let non_null = self.rel_ids_for_property_exists(rel_type, property);
        self.relationships
            .values()
            .filter(|relationship| self.relationship_matches_type(relationship, rel_type))
            .filter(|relationship| !non_null.contains(&relationship.id))
            .map(|relationship| relationship.id)
            .collect()
    }

    fn rel_ids_for_default_if_null_eq(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        empty: &Value,
        default: &Value,
        value: &Value,
    ) -> BTreeSet<RelId> {
        if value == default {
            let mut rel_ids = self.rel_ids_for_property_missing_or_null(rel_type, property);
            let mut values = vec![empty.clone()];
            if value != empty {
                values.push(value.clone());
            }
            rel_ids.extend(self.rel_ids_for_property_values(rel_type, property, &values));
            return rel_ids;
        }

        if value == empty || value == &Value::Null {
            return BTreeSet::new();
        }
        self.rel_ids_for_property_values(rel_type, property, std::slice::from_ref(value))
    }

    fn rel_ids_for_default_if_null_not_eq(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        empty: &Value,
        default: &Value,
        value: &Value,
    ) -> BTreeSet<RelId> {
        let equal_rel_ids =
            self.rel_ids_for_default_if_null_eq(rel_type, property, empty, default, value);
        self.rel_ids_for_type(rel_type)
            .difference(&equal_rel_ids)
            .copied()
            .collect()
    }

    fn rel_ids_for_property_range(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> BTreeSet<RelId> {
        self.relationship_property_index
            .iter()
            .filter(|((candidate_rel_type, candidate_property, value), _)| {
                rel_type
                    .map(|rel_type| *candidate_rel_type == rel_type)
                    .unwrap_or(true)
                    && candidate_property == property
                    && range_bounds_match(value, lower, upper)
            })
            .flat_map(|(_, rel_ids)| rel_ids.iter().copied())
            .collect()
    }

    pub fn relationship(&self, id: RelId) -> Option<&RelRecord> {
        self.relationships.get(&id)
    }

    pub fn node(&self, id: NodeId) -> Option<&NodeRecord> {
        self.nodes.get(&id)
    }

    /// Selects a checkpoint-published segment manifest for the current graph
    /// snapshot. A missing or stale manifest is an explicit graph-scan fallback,
    /// never permission to use an older physical projection.
    pub fn plan_checkpoint_segment_scan(
        &self,
        manifest: Option<&ScanSegmentManifest>,
        predicate: &ScanPredicate,
    ) -> ScanSegmentAccessPlan {
        manifest.map_or_else(
            || ScanSegmentAccessPlan::fallback(ScanSegmentFallback::NoManifest),
            |manifest| manifest.plan_scan(self.commit_epoch, predicate),
        )
    }

    /// Plans the checkpoint-published Source sidecar for the current graph
    /// snapshot. A missing or stale sidecar is represented as an explicit
    /// fallback before payload consumption starts. Once a streaming read has
    /// started, corruption or canonical disagreement fails closed because a
    /// consumer may already have observed rows.
    pub fn plan_published_source_scan(&self, predicate: &ScanPredicate) -> ScanSegmentAccessPlan {
        self.plan_checkpoint_segment_scan(self.source_scan_manifest.as_ref(), predicate)
    }

    /// Reads only the persisted Source ranges selected by the current
    /// checkpoint-published manifest. This is a physical candidate operator,
    /// not a substitute for query residual evaluation.
    pub fn read_published_source_scan_candidates(
        &self,
        predicate: &ScanPredicate,
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
    ) -> Result<SourceScanCandidateRead> {
        self.collect_published_source_scan_candidates(
            predicate,
            SourceScanCandidateLimits::unbounded(io_depth, max_coalesced_bytes, max_wave_bytes),
            None,
        )
    }

    pub fn read_published_source_scan_candidates_with_context(
        &self,
        predicate: &ScanPredicate,
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
        task_context: &RuntimeTaskContext,
    ) -> Result<SourceScanCandidateRead> {
        self.collect_published_source_scan_candidates(
            predicate,
            SourceScanCandidateLimits::unbounded(io_depth, max_coalesced_bytes, max_wave_bytes),
            Some(task_context),
        )
    }

    pub(crate) fn visit_published_source_scan_candidates_bounded(
        &self,
        predicate: &ScanPredicate,
        limits: SourceScanCandidateLimits,
        task_context: Option<&RuntimeTaskContext>,
        consumer: &mut dyn FnMut(SourceScanRow) -> Result<GraphScanControl>,
    ) -> Result<SourceScanCandidateVisit> {
        self.visit_published_source_scan_candidates_internal(
            predicate,
            limits,
            task_context,
            consumer,
        )
    }

    fn collect_published_source_scan_candidates(
        &self,
        predicate: &ScanPredicate,
        limits: SourceScanCandidateLimits,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<SourceScanCandidateRead> {
        let mut rows = Vec::new();
        let visit = self.visit_published_source_scan_candidates_internal(
            predicate,
            limits,
            task_context,
            &mut |row| {
                rows.push(row);
                Ok(GraphScanControl::Continue)
            },
        )?;
        match visit {
            SourceScanCandidateVisit::Rows {
                graph_epoch,
                skipped_segment_count,
                report,
                ..
            } => Ok(SourceScanCandidateRead::Rows {
                graph_epoch,
                skipped_segment_count,
                report,
                rows,
            }),
            SourceScanCandidateVisit::Fallback(reason) => {
                Ok(SourceScanCandidateRead::Fallback(reason))
            }
        }
    }

    fn visit_published_source_scan_candidates_internal(
        &self,
        predicate: &ScanPredicate,
        limits: SourceScanCandidateLimits,
        task_context: Option<&RuntimeTaskContext>,
        consumer: &mut dyn FnMut(SourceScanRow) -> Result<GraphScanControl>,
    ) -> Result<SourceScanCandidateVisit> {
        let SourceScanCandidateLimits {
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            max_live_candidate_bytes,
        } = limits;
        let plan = self.plan_published_source_scan(predicate);
        let ScanSegmentAccessPlan::Read(plan) = plan else {
            let ScanSegmentAccessPlan::Fallback(reason) = plan else {
                unreachable!("source scan plan is read or fallback")
            };
            return Ok(SourceScanCandidateVisit::Fallback(reason));
        };
        let Some(durable) = &self.durable else {
            return Ok(SourceScanCandidateVisit::Fallback(
                ScanSegmentFallback::NoManifest,
            ));
        };

        let reader = &durable.source_scan_reader;
        let ranges = plan
            .segments
            .iter()
            .map(|segment| (segment.segment_id, segment.payload_range.clone()))
            .collect::<BTreeMap<_, _>>();
        let manifest = self
            .source_scan_manifest
            .as_ref()
            .expect("read source scan must have a manifest");
        let checksums = manifest
            .segments()
            .iter()
            .map(|segment| (segment.summary.segment_id, segment.payload_range.checksum))
            .collect::<BTreeMap<_, _>>();
        let segment_row_counts = manifest
            .segments()
            .iter()
            .map(|segment| (segment.summary.segment_id, segment.summary.row_count))
            .collect::<BTreeMap<_, _>>();
        let candidate_count = plan.segments.iter().fold(0usize, |count, segment| {
            let segment_count = segment.candidates.as_ref().map_or_else(
                || segment_row_counts[&segment.segment_id],
                hawdb_storage::CandidateCursor::remaining,
            );
            count.saturating_add(usize::try_from(segment_count).unwrap_or(usize::MAX))
        });
        let graph_epoch = plan.graph_epoch;
        let skipped_segment_count = plan.skipped_segment_count;
        let mut candidates = plan
            .segments
            .into_iter()
            .map(|segment| (segment.segment_id, segment.candidates))
            .collect::<BTreeMap<_, _>>();
        let schedule = SegmentReadScheduler::new(io_depth, max_coalesced_bytes)
            .schedule_with_wave_budget(ranges.values().cloned(), max_wave_bytes);
        let mut consume = |payload: SegmentReadPayload| {
            for segment_id in &payload.range.segment_ids {
                let range = ranges.get(segment_id).ok_or_else(|| {
                    HawDBError::StorageIntegrity(format!(
                        "source scan reader returned unknown segment {segment_id}"
                    ))
                })?;
                let start = usize::try_from(range.offset.saturating_sub(payload.range.offset))
                    .map_err(|_| {
                        HawDBError::StorageIntegrity(
                            "source scan payload offset exceeds address space".to_string(),
                        )
                    })?;
                let end = start
                    .checked_add(usize::try_from(range.length.get()).map_err(|_| {
                        HawDBError::StorageIntegrity(
                            "source scan payload length exceeds address space".to_string(),
                        )
                    })?)
                    .ok_or_else(|| {
                        HawDBError::StorageIntegrity(
                            "source scan payload slice overflows".to_string(),
                        )
                    })?;
                let bytes = payload.bytes.get(start..end).ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "source scan coalesced payload does not cover a segment".to_string(),
                    )
                })?;
                if checksum_bytes(bytes) != checksums[segment_id] {
                    return Err(HawDBError::StorageIntegrity(format!(
                            "source scan segment {segment_id} checksum changed after manifest validation"
                        )));
                }
                let segment_rows = source_scan::decode_payload(bytes)
                    .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
                let candidate_positions = candidates
                    .remove(segment_id)
                    .flatten()
                    .map(|mut cursor| cursor.next_batch(usize::MAX));
                let live_candidate_bytes = segment_rows
                    .iter()
                    .fold(0usize, |total, row| {
                        total.saturating_add(
                            std::mem::size_of::<SourceScanRow>().saturating_add(
                                usize::try_from(estimated_properties_bytes(&row.properties))
                                    .unwrap_or(usize::MAX),
                            ),
                        )
                    })
                    .saturating_add(candidate_positions.as_ref().map_or(0, |positions| {
                        positions.len().saturating_mul(std::mem::size_of::<u64>())
                    }));
                if max_live_candidate_bytes.is_some_and(|limit| live_candidate_bytes > limit) {
                    return Err(HawDBError::Execution(format!(
                        "SourceSegmentScan decoded segment uses {live_candidate_bytes} bytes, exceeding blocking_operator_bytes {}",
                        max_live_candidate_bytes.unwrap_or_default()
                    )));
                }
                for (row_id, row) in segment_rows.into_iter().enumerate() {
                    if candidate_positions
                        .as_ref()
                        .is_some_and(|positions| positions.binary_search(&(row_id as u64)).is_err())
                    {
                        continue;
                    }
                    if consumer(row)? == GraphScanControl::Stop {
                        return Ok(hawdb_storage::SegmentReadControl::Stop);
                    }
                }
            }
            Ok::<_, HawDBError>(hawdb_storage::SegmentReadControl::Continue)
        };
        let executor = SegmentReadExecutor::new(max_wave_bytes);
        let report = match task_context {
            Some(task_context) => {
                executor.execute_with_context_control(reader, &schedule, task_context, &mut consume)
            }
            None => executor.execute_control(reader, &schedule, &mut consume),
        }
        .map_err(|error| match error {
            SegmentReadExecutionError::Stopped(reason) => {
                HawDBError::Execution(format!("runtime task stopped: {reason}"))
            }
            error => HawDBError::StorageIntegrity(error.to_string()),
        })?;
        Ok(SourceScanCandidateVisit::Rows {
            graph_epoch,
            skipped_segment_count,
            report,
            candidate_count,
        })
    }

    fn adjacency_relationship_ids(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> Option<&AdjacencyPostingList> {
        match direction {
            AdjacencyDirection::Outgoing => self.outgoing.get(&(node_id, rel_type)),
            AdjacencyDirection::Incoming => self.incoming.get(&(node_id, rel_type)),
        }
    }
}

fn validate_composite_range_seek(seek: &hawdb_plan::CompositeRangeSeek) -> Result<()> {
    let prefix_len = seek.equality_prefix.len();
    if prefix_len == 0
        || prefix_len >= seek.index_properties.len()
        || seek
            .equality_prefix
            .iter()
            .map(|(property, _)| property)
            .ne(seek.index_properties.iter().take(prefix_len))
        || seek.index_properties[prefix_len] != seek.range_property
        || (seek.lower.is_none() && seek.upper.is_none())
    {
        return Err(HawDBError::Execution(
            "invalid composite range seek shape".to_string(),
        ));
    }
    Ok(())
}

fn node_matches_composite_range(node: &NodeRecord, seek: &hawdb_plan::CompositeRangeSeek) -> bool {
    properties_match_composite_range(&node.properties, seek)
}

fn projected_node_matches_composite_range(
    node: &ProjectedNodeRecord,
    seek: &hawdb_plan::CompositeRangeSeek,
) -> bool {
    properties_match_composite_range(&node.properties, seek)
}

fn properties_match_composite_range(
    properties: &BTreeMap<String, Value>,
    seek: &hawdb_plan::CompositeRangeSeek,
) -> bool {
    seek.equality_prefix
        .iter()
        .all(|(property, value)| properties.get(property) == Some(value))
        && properties.get(&seek.range_property).is_some_and(|value| {
            range_bounds_match(value, seek.lower.as_ref(), seek.upper.as_ref())
        })
}

fn ordered_relationship_key(
    relationship: &RelRecord,
    direction: AdjacencyDirection,
) -> OrderedAdjacencyEntry {
    let neighbor = match direction {
        AdjacencyDirection::Outgoing => relationship.target,
        AdjacencyDirection::Incoming => relationship.source,
    };
    OrderedAdjacencyEntry {
        neighbor_id: neighbor,
        relationship_id: relationship.id,
    }
}

fn push_compact_adjacency_key(
    entries: &mut Vec<OrderedAdjacencyEntry>,
    entry: OrderedAdjacencyEntry,
    memory_budget_bytes: usize,
    charge_key_bytes: &mut impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    let entry_bytes = std::mem::size_of::<OrderedAdjacencyEntry>();
    let required_bytes = entries.len().saturating_add(1).saturating_mul(entry_bytes);
    if required_bytes > memory_budget_bytes {
        return Err(HawDBError::Execution(format!(
            "ordered adjacency keys use {required_bytes} bytes, exceeding blocking_operator_bytes {memory_budget_bytes}"
        )));
    }
    charge_key_bytes(entry_bytes)?;
    entries.push(entry);
    Ok(())
}

fn emit_compact_adjacency_before(
    store: &GraphStore,
    entries: &[OrderedAdjacencyEntry],
    index: &mut usize,
    before: Option<OrderedAdjacencyEntry>,
    consumer: &mut impl FnMut(RelRecord) -> Result<GraphScanControl>,
) -> Result<GraphScanControl> {
    while let Some(entry) = entries.get(*index).copied() {
        if before.is_some_and(|before| entry >= before) {
            break;
        }
        *index = index.saturating_add(1);
        let Some(relationship) = store.relationship_owned(entry.relationship_id)? else {
            return Err(HawDBError::StorageIntegrity(format!(
                "ordered adjacency references missing relationship {}",
                entry.relationship_id.0
            )));
        };
        if consumer(relationship)? == GraphScanControl::Stop {
            return Ok(GraphScanControl::Stop);
        }
    }
    Ok(GraphScanControl::Continue)
}

fn emit_live_adjacency_before(
    store: &GraphStore,
    entries: &mut std::iter::Peekable<impl Iterator<Item = OrderedAdjacencyEntry>>,
    before: Option<OrderedAdjacencyEntry>,
    consumer: &mut impl FnMut(RelRecord) -> Result<GraphScanControl>,
) -> Result<GraphScanControl> {
    while let Some(entry) = entries.peek().copied() {
        if before.is_some_and(|before| entry >= before) {
            break;
        }
        entries.next();
        let Some(relationship) = store.relationships.get(&entry.relationship_id) else {
            return Err(HawDBError::StorageIntegrity(format!(
                "live ordered adjacency references missing relationship {}",
                entry.relationship_id.0
            )));
        };
        if consumer(relationship.clone())? == GraphScanControl::Stop {
            return Ok(GraphScanControl::Stop);
        }
    }
    Ok(GraphScanControl::Continue)
}

enum RelationshipProjectionProbe<'a> {
    Equality {
        rel_type: RelTypeId,
        property: &'a str,
        values: Vec<&'a Value>,
        estimated_entries: u64,
        estimate_report: hawdb_storage::PersistentPropertyProjectionReadReport,
    },
    Range {
        rel_type: RelTypeId,
        property: &'a str,
        lower: Option<&'a (Value, bool)>,
        upper: Option<&'a (Value, bool)>,
        estimated_entries: u64,
        estimate_report: hawdb_storage::PersistentPropertyProjectionReadReport,
    },
}

impl RelationshipProjectionProbe<'_> {
    fn estimated_entries(&self) -> u64 {
        match self {
            Self::Equality {
                estimated_entries, ..
            }
            | Self::Range {
                estimated_entries, ..
            } => *estimated_entries,
        }
    }

    fn estimate_report(&self) -> hawdb_storage::PersistentPropertyProjectionReadReport {
        match self {
            Self::Equality {
                estimate_report, ..
            }
            | Self::Range {
                estimate_report, ..
            } => *estimate_report,
        }
    }

    fn replace_estimate_report(
        &mut self,
        report: hawdb_storage::PersistentPropertyProjectionReadReport,
    ) {
        match self {
            Self::Equality {
                estimate_report, ..
            }
            | Self::Range {
                estimate_report, ..
            } => *estimate_report = report,
        }
    }

    fn strategy(&self) -> ScanPruningStrategy {
        match self {
            Self::Equality {
                property, values, ..
            } if values.len() == 1 => ScanPruningStrategy::PropertyEq {
                property: (*property).to_string(),
            },
            Self::Equality { property, .. } => ScanPruningStrategy::PropertyIn {
                property: (*property).to_string(),
            },
            Self::Range { property, .. } => ScanPruningStrategy::PropertyRange {
                property: (*property).to_string(),
            },
        }
    }

    fn index_class(&self) -> PersistentGraphIndexClass {
        match self {
            Self::Equality { .. } => PersistentGraphIndexClass::RelationshipEquality,
            Self::Range { .. } => PersistentGraphIndexClass::RelationshipRange,
        }
    }

    fn matches(&self, relationship: &RelRecord) -> bool {
        match self {
            Self::Equality {
                property, values, ..
            } => relationship
                .properties
                .get(*property)
                .is_some_and(|actual| values.contains(&actual)),
            Self::Range {
                property,
                lower,
                upper,
                ..
            } => relationship
                .properties
                .get(*property)
                .is_some_and(|value| range_bounds_match(value, *lower, *upper)),
        }
    }

    fn scan(
        &self,
        projection: &PersistentPropertyProjectionReader,
        mut consumer: impl FnMut(
            RelId,
        ) -> std::result::Result<
            CanonicalScanControl,
            PersistentPropertyProjectionError,
        >,
    ) -> std::result::Result<
        (
            hawdb_storage::PersistentPropertyProjectionReadReport,
            CanonicalScanControl,
        ),
        PersistentPropertyProjectionError,
    > {
        match self {
            Self::Equality {
                rel_type,
                property,
                values,
                ..
            } => {
                let mut total = self.estimate_report();
                for value in values {
                    let (report, control) = projection.scan_relationship_equality_candidates(
                        *rel_type,
                        property,
                        value,
                        &mut consumer,
                    )?;
                    accumulate_property_projection_report(&mut total, report);
                    if control == CanonicalScanControl::Stop {
                        return Ok((total, control));
                    }
                }
                Ok((total, CanonicalScanControl::Continue))
            }
            Self::Range {
                rel_type,
                property,
                lower,
                upper,
                ..
            } => {
                let mut total = self.estimate_report();
                let (report, control) = projection.scan_relationship_range_candidates(
                    *rel_type, property, *lower, *upper, consumer,
                )?;
                accumulate_property_projection_report(&mut total, report);
                Ok((total, control))
            }
        }
    }
}

fn accumulate_property_projection_report(
    total: &mut hawdb_storage::PersistentPropertyProjectionReadReport,
    report: hawdb_storage::PersistentPropertyProjectionReadReport,
) {
    total.descriptor_pages_visited = total
        .descriptor_pages_visited
        .saturating_add(report.descriptor_pages_visited);
    total.descriptor_page_bytes_decoded = total
        .descriptor_page_bytes_decoded
        .saturating_add(report.descriptor_page_bytes_decoded);
    total.descriptor_storage_bytes_read = total
        .descriptor_storage_bytes_read
        .saturating_add(report.descriptor_storage_bytes_read);
    total.descriptors_examined = total
        .descriptors_examined
        .saturating_add(report.descriptors_examined);
    total.descriptor_cache_hits = total
        .descriptor_cache_hits
        .saturating_add(report.descriptor_cache_hits);
    total.descriptor_cache_misses = total
        .descriptor_cache_misses
        .saturating_add(report.descriptor_cache_misses);
    total.descriptor_cache_admission_rejections = total
        .descriptor_cache_admission_rejections
        .saturating_add(report.descriptor_cache_admission_rejections);
    total.blocks_considered = total
        .blocks_considered
        .saturating_add(report.blocks_considered);
    total.blocks_pruned = total.blocks_pruned.saturating_add(report.blocks_pruned);
    total.blocks_read = total.blocks_read.saturating_add(report.blocks_read);
    total.bytes_read = total.bytes_read.saturating_add(report.bytes_read);
    total.cache_hits = total.cache_hits.saturating_add(report.cache_hits);
    total.cache_misses = total.cache_misses.saturating_add(report.cache_misses);
    total.entries_decoded = total.entries_decoded.saturating_add(report.entries_decoded);
    total.candidates_returned = total
        .candidates_returned
        .saturating_add(report.candidates_returned);
}

fn relationship_projection_probe<'a>(
    projection: &PersistentPropertyProjectionReader,
    rel_type: RelTypeId,
    filter: &'a PropertyFilter,
) -> std::result::Result<Option<RelationshipProjectionProbe<'a>>, PersistentPropertyProjectionError>
{
    match filter {
        PropertyFilter::And(filters) => {
            let mut best = None;
            let mut selection_report =
                hawdb_storage::PersistentPropertyProjectionReadReport::default();
            for filter in filters {
                let Some(candidate) = relationship_projection_probe(projection, rel_type, filter)?
                else {
                    continue;
                };
                accumulate_property_projection_report(
                    &mut selection_report,
                    candidate.estimate_report(),
                );
                if best
                    .as_ref()
                    .is_none_or(|current: &RelationshipProjectionProbe<'_>| {
                        candidate.estimated_entries() < current.estimated_entries()
                    })
                {
                    best = Some(candidate);
                }
            }
            if let Some(best) = &mut best {
                best.replace_estimate_report(selection_report);
            }
            Ok(best)
        }
        PropertyFilter::Eq { property, value } => {
            if !projection.manifest().supports_relationship(
                rel_type,
                property,
                PersistentPropertyProjectionKind::RelationshipEquality,
            ) {
                return Ok(None);
            }
            let (estimated_entries, estimate_report) =
                projection.estimate_relationship_equality_entries(rel_type, property, value)?;
            Ok(Some(RelationshipProjectionProbe::Equality {
                rel_type,
                property,
                values: vec![value],
                estimated_entries,
                estimate_report,
            }))
        }
        PropertyFilter::In { property, values } => {
            if !projection.manifest().supports_relationship(
                rel_type,
                property,
                PersistentPropertyProjectionKind::RelationshipEquality,
            ) {
                return Ok(None);
            }
            let values = values
                .iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let mut estimated_entries = 0u64;
            let mut estimate_report =
                hawdb_storage::PersistentPropertyProjectionReadReport::default();
            for value in &values {
                let (entries, report) =
                    projection.estimate_relationship_equality_entries(rel_type, property, value)?;
                estimated_entries = estimated_entries.saturating_add(entries);
                accumulate_property_projection_report(&mut estimate_report, report);
            }
            Ok(Some(RelationshipProjectionProbe::Equality {
                rel_type,
                property,
                values,
                estimated_entries,
                estimate_report,
            }))
        }
        PropertyFilter::Range {
            property,
            lower,
            upper,
        } if lower.is_some() || upper.is_some() => {
            if !projection.manifest().supports_relationship(
                rel_type,
                property,
                PersistentPropertyProjectionKind::RelationshipRange,
            ) {
                return Ok(None);
            }
            let (estimated_entries, estimate_report) = projection
                .estimate_relationship_range_entries(
                    rel_type,
                    property,
                    lower.as_ref(),
                    upper.as_ref(),
                )?;
            Ok(Some(RelationshipProjectionProbe::Range {
                rel_type,
                property,
                lower: lower.as_ref(),
                upper: upper.as_ref(),
                estimated_entries,
                estimate_report,
            }))
        }
        _ => Ok(None),
    }
}

fn relationship_matches_endpoint(
    relationship: &RelRecord,
    node_id: NodeId,
    direction: AdjacencyDirection,
) -> bool {
    match direction {
        AdjacencyDirection::Outgoing => relationship.source == node_id,
        AdjacencyDirection::Incoming => relationship.target == node_id,
    }
}
