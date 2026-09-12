use super::*;
use skein_storage::statistics_refresh::{
    RefreshSpillDirectory, StatsRecord, StatsRunOptions, StatsRunWriter,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerStatisticsRefreshOptions {
    pub memory_budget_bytes: usize,
    pub max_spill_bytes: u64,
    pub max_spill_runs: usize,
    pub max_input_records: u64,
    pub max_generated_facts: u64,
    pub max_path_expansions: u64,
    pub spill_directory: PathBuf,
}

impl OptimizerStatisticsRefreshOptions {
    fn validate(&self) -> Result<()> {
        if self.memory_budget_bytes < 4096 {
            return Err(SkeinError::Semantic(
                "optimizer statistics refresh memory_budget_bytes must be at least 4096"
                    .to_string(),
            ));
        }
        for (name, value) in [
            ("max_spill_bytes", self.max_spill_bytes),
            ("max_spill_runs", self.max_spill_runs as u64),
            ("max_input_records", self.max_input_records),
            ("max_generated_facts", self.max_generated_facts),
            ("max_path_expansions", self.max_path_expansions),
        ] {
            if value == 0 {
                return Err(SkeinError::Semantic(format!(
                    "optimizer statistics refresh {name} must be greater than zero"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerStatisticsRefreshReport {
    pub source_commit_epoch: u64,
    pub node_records_read: u64,
    pub relationship_records_read: u64,
    pub path_expansions: u64,
    pub generated_facts: u64,
    pub spill_run_count: usize,
    pub spilled_bytes: u64,
    pub peak_buffer_bytes: usize,
    pub output_statistics_bytes: usize,
    pub property_group_count: usize,
    pub relationship_property_group_count: usize,
    pub excluded_property_group_count: usize,
    pub excluded_relationship_property_group_count: usize,
    pub index_sample_count: usize,
    pub path_group_count: usize,
    pub bounded_path_group_count: usize,
    pub checkpoint_persisted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OptimizerStatisticsRefreshWork {
    pub(crate) estimated_operations: usize,
    pub(crate) recent_delta_operations: usize,
    pub(crate) source_commit_lag: u64,
}

impl OptimizerStatisticsRefreshReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-optimizer-statistics-refresh-v1",
            "protocol_version": 1,
            "source_commit_epoch": self.source_commit_epoch,
            "node_records_read": self.node_records_read,
            "relationship_records_read": self.relationship_records_read,
            "path_expansions": self.path_expansions,
            "generated_facts": self.generated_facts,
            "spill_run_count": self.spill_run_count,
            "spilled_bytes": self.spilled_bytes,
            "peak_buffer_bytes": self.peak_buffer_bytes,
            "output_statistics_bytes": self.output_statistics_bytes,
            "property_group_count": self.property_group_count,
            "relationship_property_group_count": self.relationship_property_group_count,
            "excluded_property_group_count": self.excluded_property_group_count,
            "excluded_relationship_property_group_count": self.excluded_relationship_property_group_count,
            "index_sample_count": self.index_sample_count,
            "path_group_count": self.path_group_count,
            "bounded_path_group_count": self.bounded_path_group_count,
            "checkpoint_persisted": self.checkpoint_persisted,
        })
    }
}

impl GraphStore {
    pub(crate) fn optimizer_statistics_refresh_work(
        &self,
        catalog: &Catalog,
    ) -> Option<OptimizerStatisticsRefreshWork> {
        if !self.canonical_base_out_of_core || self.durable.is_none() {
            return None;
        }

        let mut missing_sample_count = 0usize;
        let mut stale_updates = 0u64;
        let supported_index_ids = catalog
            .property_indexes()
            .map(|index| index.id)
            .chain(catalog.composite_property_indexes().map(|index| index.id))
            .filter(|index_id| catalog.supports_index_statistics(*index_id));
        for index_id in supported_index_ids {
            match self.checkpoint_statistics.index_samples.get(&index_id) {
                None => missing_sample_count = missing_sample_count.saturating_add(1),
                Some(sample) if sample.is_stale() => {
                    stale_updates = stale_updates.saturating_add(sample.updates_since_sample);
                }
                Some(_) => {}
            }
        }
        let freshness = self
            .checkpoint_statistics
            .advanced_statistics_freshness(self.commit_epoch);
        if freshness == AdvancedStatisticsFreshness::Fresh
            && self.advanced_statistics_dirty.is_empty()
            && missing_sample_count == 0
            && stale_updates == 0
        {
            return None;
        }

        let record_count = self
            .basic_statistics
            .node_count
            .saturating_add(self.basic_statistics.relationship_count);
        let dirty_operations = self.advanced_statistics_dirty.mutation_operations();
        Some(OptimizerStatisticsRefreshWork {
            estimated_operations: usize::try_from(record_count).unwrap_or(usize::MAX).max(1),
            recent_delta_operations: usize::try_from(stale_updates.max(dirty_operations))
                .unwrap_or(usize::MAX)
                .saturating_add(missing_sample_count),
            source_commit_lag: self
                .checkpoint_statistics
                .advanced_statistics_commit_lag(self.commit_epoch),
        })
    }

    pub fn refresh_optimizer_statistics_external(
        &mut self,
        catalog: &Catalog,
        options: &OptimizerStatisticsRefreshOptions,
    ) -> Result<OptimizerStatisticsRefreshReport> {
        options.validate()?;
        if !self.canonical_base_out_of_core {
            return Err(SkeinError::Execution(
                "external optimizer statistics refresh requires out-of-core storage".to_string(),
            ));
        }
        if self.durable.is_none() {
            return Err(SkeinError::Execution(
                "external optimizer statistics refresh requires durable storage".to_string(),
            ));
        }

        let source_commit_epoch = self.commit_epoch;
        let spill = RefreshSpillDirectory::create(&options.spill_directory)?;
        let run_options = StatsRunOptions {
            memory_budget_bytes: options.memory_budget_bytes,
            max_spill_bytes: options.max_spill_bytes,
            max_spill_runs: options.max_spill_runs,
            max_generated_facts: options.max_generated_facts,
        };
        let mut writer = StatsRunWriter::new(spill.path(), catalog, &run_options);
        let mut work = RefreshWork::new(options);

        self.try_visit_nodes_owned(None, |node| {
            work.read_node()?;
            for label in &node.labels {
                for (property, value) in &node.properties {
                    writer.push_node_property(*label, property, value)?;
                }
            }
            writer.push_node_index_entries(&node)?;
            Ok(GraphScanControl::Continue)
        })?;

        self.try_visit_relationships_owned(None, |relationship| {
            work.read_relationship()?;
            writer.push(StatsRecord::RelSource {
                rel_type: relationship.rel_type,
                node: relationship.source,
            })?;
            writer.push(StatsRecord::RelTarget {
                rel_type: relationship.rel_type,
                node: relationship.target,
            })?;
            for (property, value) in &relationship.properties {
                writer.push_relationship_property(relationship.rel_type, property, value)?;
            }
            let source = self.node_owned(relationship.source)?.ok_or_else(|| {
                SkeinError::Storage(format!(
                    "optimizer statistics refresh found relationship {} with missing source {}",
                    relationship.id.0, relationship.source.0
                ))
            })?;
            work.read_node()?;
            let target = self.node_owned(relationship.target)?.ok_or_else(|| {
                SkeinError::Storage(format!(
                    "optimizer statistics refresh found relationship {} with missing target {}",
                    relationship.id.0, relationship.target.0
                ))
            })?;
            work.read_node()?;
            for source_label in &source.labels {
                for target_label in &target.labels {
                    writer.push(StatsRecord::PathCount {
                        source_label: *source_label,
                        rel_type: relationship.rel_type,
                        target_label: *target_label,
                    })?;
                    writer.push(StatsRecord::PathSource {
                        source_label: *source_label,
                        rel_type: relationship.rel_type,
                        target_label: *target_label,
                        node: relationship.source,
                    })?;
                    writer.push(StatsRecord::PathTarget {
                        source_label: *source_label,
                        rel_type: relationship.rel_type,
                        target_label: *target_label,
                        node: relationship.target,
                    })?;
                }
            }
            Ok(GraphScanControl::Continue)
        })?;

        let rel_types = self
            .basic_statistics()
            .rel_type_counts
            .keys()
            .copied()
            .collect::<Vec<_>>();
        self.try_visit_nodes_owned(None, |source| {
            work.read_node()?;
            for source_label in &source.labels {
                for rel_type in &rel_types {
                    collect_bounded_path_facts(
                        self,
                        source.id,
                        1,
                        BoundedPathSpec {
                            root_source: source.id,
                            source_label: *source_label,
                            rel_type: *rel_type,
                        },
                        &mut writer,
                        &mut work,
                    )?;
                }
            }
            Ok(GraphScanControl::Continue)
        })?;

        let basic = self.basic_statistics();
        let (statistics, merge_report) = writer.finish(graph_statistics_from_basic(basic, true))?;
        if source_commit_epoch != self.commit_epoch {
            return Err(SkeinError::Execution(
                "optimizer statistics refresh source epoch changed before publication".to_string(),
            ));
        }
        self.checkpoint_statistics = statistics;
        self.advanced_statistics_dirty = AdvancedStatisticsDirtyState::default();

        Ok(OptimizerStatisticsRefreshReport {
            source_commit_epoch,
            node_records_read: work.node_records_read,
            relationship_records_read: work.relationship_records_read,
            path_expansions: work.path_expansions,
            generated_facts: merge_report.generated_facts,
            spill_run_count: merge_report.spill_run_count,
            spilled_bytes: merge_report.spilled_bytes,
            peak_buffer_bytes: merge_report.peak_buffer_bytes,
            output_statistics_bytes: merge_report.output_statistics_bytes,
            property_group_count: self.checkpoint_statistics.property_distinct_counts.len(),
            relationship_property_group_count: self
                .checkpoint_statistics
                .rel_property_distinct_counts
                .len(),
            excluded_property_group_count: merge_report.excluded_property_group_count,
            excluded_relationship_property_group_count: merge_report
                .excluded_relationship_property_group_count,
            index_sample_count: self.checkpoint_statistics.index_samples.len(),
            path_group_count: self.checkpoint_statistics.path_counts.len(),
            bounded_path_group_count: self.checkpoint_statistics.bounded_path_counts.len(),
            checkpoint_persisted: false,
        })
    }

    pub(crate) fn restore_checkpoint_statistics(
        &mut self,
        statistics: GraphStatistics,
        dirty_state: AdvancedStatisticsDirtyState,
    ) {
        self.checkpoint_statistics = statistics;
        self.advanced_statistics_dirty = dirty_state;
    }

    pub(crate) fn checkpoint_statistics_snapshot(&self) -> GraphStatistics {
        self.checkpoint_statistics.clone()
    }

    pub(crate) fn advanced_statistics_dirty_snapshot(&self) -> AdvancedStatisticsDirtyState {
        self.advanced_statistics_dirty
    }
}

#[derive(Clone, Copy)]
struct BoundedPathSpec {
    root_source: NodeId,
    source_label: LabelId,
    rel_type: RelTypeId,
}

fn collect_bounded_path_facts(
    store: &GraphStore,
    current: NodeId,
    hop: usize,
    spec: BoundedPathSpec,
    writer: &mut StatsRunWriter<'_>,
    work: &mut RefreshWork<'_>,
) -> Result<()> {
    if hop > MAX_BOUNDED_PATH_STAT_HOPS {
        return Ok(());
    }
    store.try_visit_adjacent_relationships_owned(
        current,
        Some(spec.rel_type),
        AdjacencyDirection::Outgoing,
        |relationship| {
            work.read_relationship()?;
            work.expand_path()?;
            let target = store.node_owned(relationship.target)?.ok_or_else(|| {
                SkeinError::Storage(format!(
                    "optimizer statistics refresh found relationship {} with missing target {}",
                    relationship.id.0, relationship.target.0
                ))
            })?;
            work.read_node()?;
            for target_label in &target.labels {
                writer.push(StatsRecord::BoundedPathCount {
                    source_label: spec.source_label,
                    rel_type: spec.rel_type,
                    target_label: *target_label,
                    hop,
                })?;
                writer.push(StatsRecord::BoundedPathSource {
                    source_label: spec.source_label,
                    rel_type: spec.rel_type,
                    target_label: *target_label,
                    hop,
                    node: spec.root_source,
                })?;
                writer.push(StatsRecord::BoundedPathTarget {
                    source_label: spec.source_label,
                    rel_type: spec.rel_type,
                    target_label: *target_label,
                    hop,
                    node: relationship.target,
                })?;
            }
            collect_bounded_path_facts(store, relationship.target, hop + 1, spec, writer, work)?;
            Ok(GraphScanControl::Continue)
        },
    )?;
    Ok(())
}

struct RefreshWork<'a> {
    options: &'a OptimizerStatisticsRefreshOptions,
    node_records_read: u64,
    relationship_records_read: u64,
    path_expansions: u64,
}

impl<'a> RefreshWork<'a> {
    fn new(options: &'a OptimizerStatisticsRefreshOptions) -> Self {
        Self {
            options,
            node_records_read: 0,
            relationship_records_read: 0,
            path_expansions: 0,
        }
    }

    fn read_node(&mut self) -> Result<()> {
        self.node_records_read = self.node_records_read.saturating_add(1);
        self.check_input_budget()
    }

    fn read_relationship(&mut self) -> Result<()> {
        self.relationship_records_read = self.relationship_records_read.saturating_add(1);
        self.check_input_budget()
    }

    fn expand_path(&mut self) -> Result<()> {
        self.path_expansions = self.path_expansions.saturating_add(1);
        if self.path_expansions > self.options.max_path_expansions {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh exceeded max_path_expansions {}",
                self.options.max_path_expansions
            )));
        }
        Ok(())
    }

    fn check_input_budget(&self) -> Result<()> {
        let input_records = self
            .node_records_read
            .saturating_add(self.relationship_records_read);
        if input_records > self.options.max_input_records {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh exceeded max_input_records {}",
                self.options.max_input_records
            )));
        }
        Ok(())
    }
}
