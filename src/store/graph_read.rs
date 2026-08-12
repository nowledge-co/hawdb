//! Read, scan, seek, and scan-pruning methods for [`GraphStore`].

use super::*;

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
        GraphNodeIterator {
            base: self
                .canonical_base
                .as_ref()
                .map(CanonicalSegmentReader::node_records)
                .map(Iterator::peekable),
            delta: self
                .nodes
                .values()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .peekable(),
            tombstones: self.node_tombstones.clone(),
        }
    }

    pub fn relationship_records_owned(&self) -> GraphRelationshipIterator {
        GraphRelationshipIterator {
            base: self
                .canonical_base
                .as_ref()
                .map(CanonicalSegmentReader::relationship_records)
                .map(Iterator::peekable),
            delta: self
                .relationships
                .values()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .peekable(),
            tombstones: self.relationship_tombstones.clone(),
        }
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
        let Some(reader) = &self.canonical_base else {
            for node in self.nodes.values() {
                if self.node_matches_label(node, label_id)
                    && consumer(node.clone()) == GraphScanControl::Stop
                {
                    return Ok(GraphScanControl::Stop);
                }
            }
            return Ok(GraphScanControl::Continue);
        };

        let mut delta = self.nodes.iter().peekable();
        let mut graph_control = GraphScanControl::Continue;
        let (_, canonical_control) = reader
            .scan_nodes_control(|base| {
                while delta.peek().is_some_and(|(id, _)| **id < base.id) {
                    let (id, node) = delta.next().expect("peeked delta node exists");
                    if !self.node_tombstones.contains(id)
                        && self.node_matches_label(node, label_id)
                        && consumer(node.clone()) == GraphScanControl::Stop
                    {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                }
                if delta.peek().is_some_and(|(id, _)| **id == base.id) {
                    let (id, node) = delta.next().expect("matching delta node exists");
                    if !self.node_tombstones.contains(id)
                        && self.node_matches_label(node, label_id)
                        && consumer(node.clone()) == GraphScanControl::Stop
                    {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                    return Ok(CanonicalScanControl::Continue);
                }
                if !self.node_tombstones.contains(&base.id)
                    && self.node_matches_label(&base, label_id)
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
        for (id, node) in delta {
            if !self.node_tombstones.contains(id)
                && self.node_matches_label(node, label_id)
                && consumer(node.clone()) == GraphScanControl::Stop
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

    pub fn statistics(&self) -> GraphStatistics {
        if !self.canonical_base_out_of_core {
            return compute_statistics_with_basic(
                &self.nodes,
                &self.relationships,
                self.basic_statistics(),
            );
        }
        let mut statistics = self.checkpoint_statistics.clone();
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
        let recomputed = compute_statistics(&self.nodes, &self.relationships, self.commit_epoch);
        // The index-derived side can only speak for declared properties, so
        // the recomputed side is narrowed to the same keys. Comparing against
        // every property would flag the undeclared ones forever.
        let recomputed_property_distinct_counts = recomputed
            .property_distinct_counts
            .into_iter()
            .filter(|((label_id, property), _)| {
                catalog.property_index_id(*label_id, property).is_some()
            })
            .collect();
        DistinctValueStatisticsConsistencyReport::new(
            self.commit_epoch,
            compute_node_property_distinct_counts_from_index(&self.property_index),
            recomputed_property_distinct_counts,
            compute_relationship_property_distinct_counts_from_index(
                &self.relationship_property_index,
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
        for value in values {
            let mut graph_control = GraphScanControl::Continue;
            let (_, canonical_control) = reader
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
        let (_, projection_control) = projection
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
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
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
        let seed_token = query_tokens
            .iter()
            .min_by_key(|token| {
                projection.estimate_full_text_token_entries(label_id, property, token)
            })
            .expect("non-empty full-text query has a seed token");
        let mut graph_control = GraphScanControl::Continue;
        let (_, projection_control) = projection
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
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
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
            .filter_map(|rel_id| self.relationships.get(&rel_id))
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
            .filter_map(|rel_id| self.relationships.get(&rel_id))
    }

    pub fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        mut consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
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
                adjacency
                    .scan_endpoint_entries_control(node_id, direction, rel_type, |entry| {
                        let relationship = match entry {
                            CanonicalAdjacencyEntry::Inline(relationship) => relationship,
                            CanonicalAdjacencyEntry::CanonicalReference { relationship_id } => {
                                reader
                                    .get_relationship(relationship_id)
                                    .map_err(|error| {
                                        skein_storage::CanonicalAdjacencyError::Source(
                                            error.to_string(),
                                        )
                                    })?
                                    .ok_or_else(|| {
                                        skein_storage::CanonicalAdjacencyError::Corrupt(format!(
                                            "canonical adjacency references missing relationship {}",
                                            relationship_id.0
                                        ))
                                    })?
                            }
                        };
                        Ok(consume_canonical(relationship))
                    })
                    .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?
                    .1
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
        let mut entries = self
            .adjacency_relationship_ids(node_id, rel_type, direction)
            .into_iter()
            .flat_map(AdjacencyPostingList::iter_copied)
            .filter_map(|rel_id| {
                let relationship = self.relationships.get(&rel_id)?;
                Some(OrderedAdjacencyEntry {
                    relationship_id: relationship.id,
                    neighbor_id: match direction {
                        AdjacencyDirection::Outgoing => relationship.target,
                        AdjacencyDirection::Incoming => relationship.source,
                    },
                })
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.neighbor_id, entry.relationship_id));
        entries
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
            .filter_map(|rel_id| {
                let relationship = self.relationships.get(&rel_id)?;
                Some(OrderedAdjacencyEntry {
                    relationship_id: relationship.id,
                    neighbor_id: match direction {
                        AdjacencyDirection::Outgoing => relationship.target,
                        AdjacencyDirection::Incoming => relationship.source,
                    },
                })
            })
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
    /// snapshot. An unavailable, corrupted, or stale sidecar is represented as
    /// an explicit fallback so callers keep the canonical graph authoritative.
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
        self.read_published_source_scan_candidates_internal(
            predicate,
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            None,
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
        self.read_published_source_scan_candidates_internal(
            predicate,
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            None,
            Some(task_context),
        )
    }

    pub(crate) fn read_published_source_scan_candidates_bounded(
        &self,
        predicate: &ScanPredicate,
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
        max_candidate_bytes: NonZeroUsize,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<SourceScanCandidateRead> {
        self.read_published_source_scan_candidates_internal(
            predicate,
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            Some(max_candidate_bytes.get()),
            task_context,
        )
    }

    fn read_published_source_scan_candidates_internal(
        &self,
        predicate: &ScanPredicate,
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
        max_candidate_bytes: Option<usize>,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<SourceScanCandidateRead> {
        let plan = self.plan_published_source_scan(predicate);
        let ScanSegmentAccessPlan::Read(plan) = plan else {
            let ScanSegmentAccessPlan::Fallback(reason) = plan else {
                unreachable!("source scan plan is read or fallback")
            };
            return Ok(SourceScanCandidateRead::Fallback(reason));
        };
        let Some(durable) = &self.durable else {
            return Ok(SourceScanCandidateRead::Fallback(
                ScanSegmentFallback::NoManifest,
            ));
        };

        let reader = &durable.source_scan_reader;
        let ranges = plan
            .segments
            .iter()
            .map(|segment| (segment.segment_id, segment.payload_range.clone()))
            .collect::<BTreeMap<_, _>>();
        let checksums = self
            .source_scan_manifest
            .as_ref()
            .expect("read source scan must have a manifest")
            .segments()
            .iter()
            .map(|segment| (segment.summary.segment_id, segment.payload_range.checksum))
            .collect::<BTreeMap<_, _>>();
        let mut candidates = plan
            .segments
            .iter()
            .map(|segment| {
                let positions = segment.candidates.clone().map(|mut cursor| {
                    cursor
                        .next_batch(usize::MAX)
                        .into_iter()
                        .collect::<BTreeSet<_>>()
                });
                (segment.segment_id, positions)
            })
            .collect::<BTreeMap<_, _>>();
        let schedule = SegmentReadScheduler::new(io_depth, max_coalesced_bytes)
            .schedule_with_wave_budget(ranges.values().cloned(), max_wave_bytes);
        let mut rows = Vec::new();
        let mut candidate_bytes = 0usize;
        let mut consume = |payload: SegmentReadPayload| {
            for segment_id in &payload.range.segment_ids {
                let range = ranges.get(segment_id).ok_or_else(|| {
                    SkeinError::StorageIntegrity(format!(
                        "source scan reader returned unknown segment {segment_id}"
                    ))
                })?;
                let start = usize::try_from(range.offset.saturating_sub(payload.range.offset))
                    .map_err(|_| {
                        SkeinError::StorageIntegrity(
                            "source scan payload offset exceeds address space".to_string(),
                        )
                    })?;
                let end = start
                    .checked_add(usize::try_from(range.length.get()).map_err(|_| {
                        SkeinError::StorageIntegrity(
                            "source scan payload length exceeds address space".to_string(),
                        )
                    })?)
                    .ok_or_else(|| {
                        SkeinError::StorageIntegrity(
                            "source scan payload slice overflows".to_string(),
                        )
                    })?;
                let bytes = payload.bytes.get(start..end).ok_or_else(|| {
                    SkeinError::StorageIntegrity(
                        "source scan coalesced payload does not cover a segment".to_string(),
                    )
                })?;
                if checksum_bytes(bytes) != checksums[segment_id] {
                    return Err(SkeinError::StorageIntegrity(format!(
                            "source scan segment {segment_id} checksum changed after manifest validation"
                        )));
                }
                let segment_rows = source_scan::decode_payload(bytes)
                    .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
                let positions = candidates.remove(segment_id).flatten();
                for (row_id, row) in segment_rows.into_iter().enumerate() {
                    if positions
                        .as_ref()
                        .is_some_and(|positions| !positions.contains(&(row_id as u64)))
                    {
                        continue;
                    }
                    let row_bytes = std::mem::size_of::<SourceScanRow>().saturating_add(
                        usize::try_from(estimated_properties_bytes(&row.properties))
                            .unwrap_or(usize::MAX),
                    );
                    let next_candidate_bytes = candidate_bytes.saturating_add(row_bytes);
                    if max_candidate_bytes.is_some_and(|limit| next_candidate_bytes > limit) {
                        return Err(SkeinError::Execution(format!(
                            "SourceSegmentScan candidates exceed blocking_operator_bytes {}",
                            max_candidate_bytes.unwrap_or_default()
                        )));
                    }
                    candidate_bytes = next_candidate_bytes;
                    rows.push(row);
                }
            }
            Ok::<_, SkeinError>(())
        };
        let executor = SegmentReadExecutor::new(max_wave_bytes);
        let report = match task_context {
            Some(task_context) => {
                executor.execute_with_context(reader, &schedule, task_context, &mut consume)
            }
            None => executor.execute(reader, &schedule, &mut consume),
        }
        .map_err(|error| match error {
            SegmentReadExecutionError::Stopped(reason) => {
                SkeinError::Execution(format!("runtime task stopped: {reason}"))
            }
            error => SkeinError::StorageIntegrity(error.to_string()),
        })?;
        Ok(SourceScanCandidateRead::Rows {
            graph_epoch: plan.graph_epoch,
            skipped_segment_count: plan.skipped_segment_count,
            report,
            rows,
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
