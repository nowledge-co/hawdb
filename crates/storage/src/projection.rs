use crate::{NodeId, NodeRecord, RelId, RelationalPrimaryKeyChangeCapture};
use skein_core::{Catalog, SchemaObjectState, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedGraphDefinition {
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
}

/// Stable graph-to-search projection identity owned by the storage contract.
pub fn projection_document_id_for_node(catalog: &Catalog, node: &NodeRecord) -> Option<String> {
    let kind = node.labels.iter().find_map(|label_id| {
        catalog
            .label_name(*label_id)
            .and_then(projection_kind_for_label)
    })?;
    let external_id = node
        .properties
        .get("id")
        .map(projection_value_to_string)
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| node.id.0.to_string());
    Some(format!("{kind}:{external_id}"))
}

pub fn projection_document_id_for_label_and_properties(
    label: &str,
    properties: &BTreeMap<String, Value>,
    node_id: NodeId,
) -> Option<String> {
    let kind = projection_kind_for_label(label)?;
    let external_id = properties
        .get("id")
        .map(projection_value_to_string)
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| node_id.0.to_string());
    Some(format!("{kind}:{external_id}"))
}

fn projection_kind_for_label(label: &str) -> Option<&'static str> {
    match label {
        "Memory" | "memory" => Some("memory"),
        "Message" | "message" => Some("message"),
        "Entity" | "entity" => Some("entity"),
        "Source" | "source" => Some("source"),
        "SourceChunk" | "source_chunk" | "chunk" => Some("source_chunk"),
        "Community" | "community" => Some("community"),
        _ => None,
    }
}

fn projection_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Binary(value) => format!(
            "\\x{}",
            value
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        Value::Uuid(value) => value.to_string(),
        Value::List(values) => values
            .iter()
            .map(projection_value_to_string)
            .collect::<Vec<_>>()
            .join(","),
        Value::Map(values) => values
            .iter()
            .map(|(key, value)| format!("{key}:{}", projection_value_to_string(value)))
            .collect::<Vec<_>>()
            .join(","),
    }
}

/// Durable, storage-neutral representation of a materialized graph projection.
///
/// Analytics runtimes may construct their own execution view from these CSR and
/// CSC arrays, but storage never depends on a particular graph algorithm crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedGraphArtifactData {
    pub nodes: Vec<NodeId>,
    pub csr_offsets: Vec<usize>,
    pub csr_targets: Vec<usize>,
    pub csc_offsets: Vec<usize>,
    pub csc_sources: Vec<usize>,
}

/// Checkpointed materialized projection metadata and its storage representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedGraphArtifact {
    pub projection_epoch: u64,
    pub commit_epoch: u64,
    pub definition: ProjectedGraphDefinition,
    pub data: ProjectedGraphArtifactData,
}

impl ProjectedGraphArtifactData {
    pub fn new(
        nodes: Vec<NodeId>,
        csr_offsets: Vec<usize>,
        csr_targets: Vec<usize>,
        csc_offsets: Vec<usize>,
        csc_sources: Vec<usize>,
    ) -> std::result::Result<Self, String> {
        validate_offsets("csr_offsets", nodes.len(), &csr_offsets, csr_targets.len())?;
        validate_offsets("csc_offsets", nodes.len(), &csc_offsets, csc_sources.len())?;
        validate_indexes("csr_targets", nodes.len(), &csr_targets)?;
        validate_indexes("csc_sources", nodes.len(), &csc_sources)?;
        if csr_targets.len() != csc_sources.len() {
            return Err("projected graph CSR and CSC edge counts differ".to_string());
        }
        Ok(Self {
            nodes,
            csr_offsets,
            csr_targets,
            csc_offsets,
            csc_sources,
        })
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
    pub fn edge_count(&self) -> usize {
        self.csr_targets.len()
    }
}

fn validate_offsets(
    name: &str,
    node_count: usize,
    offsets: &[usize],
    edge_count: usize,
) -> std::result::Result<(), String> {
    if offsets.len() != node_count.saturating_add(1)
        || offsets.first() != Some(&0)
        || offsets.last() != Some(&edge_count)
        || offsets.windows(2).any(|pair| pair[0] > pair[1])
    {
        return Err(format!("invalid projected graph {name}"));
    }
    Ok(())
}

fn validate_indexes(
    name: &str,
    node_count: usize,
    indexes: &[usize],
) -> std::result::Result<(), String> {
    if indexes.iter().any(|index| *index >= node_count) {
        return Err(format!(
            "projected graph {name} contains an out-of-range node index"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedGraphStatus {
    pub name: String,
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
    pub projection_epoch: Option<u64>,
    pub commit_epoch: Option<u64>,
    pub node_count: Option<usize>,
    pub edge_count: Option<usize>,
    pub reusable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageReclamationWatermark {
    pub current_commit_epoch: u64,
    pub checkpoint_epoch: Option<u64>,
    pub checkpoint_commit_epoch: Option<u64>,
    pub oldest_reader_commit_epoch: Option<u64>,
    pub safe_reclaim_commit_epoch: u64,
    pub durable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StorageRecoveryReport {
    pub open_timings: StorageOpenTimings,
    pub durable: bool,
    pub recovery_mode: crate::RecoveryMode,
    pub max_wal_replay_entries: Option<usize>,
    pub max_wal_replay_bytes: Option<u64>,
    pub max_wal_record_bytes: Option<usize>,
    pub checkpoint_epoch: Option<u64>,
    pub checkpoint_commit_epoch: Option<u64>,
    pub wal_present: bool,
    pub wal_generation: Option<u64>,
    pub wal_replay_start_lsn: Option<u64>,
    pub next_lsn_after_replay: Option<u64>,
    pub replayed_wal_entries: usize,
    pub replayed_wal_bytes: u64,
    pub torn_tail_ignored: bool,
    pub torn_tail_repaired: bool,
    pub discarded_wal_tail_bytes: u64,
    pub torn_tail_reason: Option<String>,
    pub recovered_commit_epoch: u64,
}

/// Monotonic elapsed-time observations for one durable database open.
///
/// These measurements are deliberately separated from recovery correctness:
/// they make manifest/root work and WAL replay independently observable
/// without changing which durable state is accepted. The phase intervals are
/// sequential and therefore their saturated sum must not exceed the total.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StorageOpenTimings {
    pub durable_manifest_open_micros: u64,
    pub checkpoint_root_open_micros: u64,
    pub wal_replay_micros: u64,
    pub post_replay_open_micros: u64,
    pub total_open_micros: u64,
}

impl StorageOpenTimings {
    pub fn accounted_micros(self) -> u64 {
        self.durable_manifest_open_micros
            .saturating_add(self.checkpoint_root_open_micros)
            .saturating_add(self.wal_replay_micros)
            .saturating_add(self.post_replay_open_micros)
    }

    pub fn unaccounted_micros(self) -> u64 {
        self.total_open_micros
            .saturating_sub(self.accounted_micros())
    }

    pub fn is_consistent(self) -> bool {
        self.accounted_micros() <= self.total_open_micros
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoreStableIdMapping {
    pub node_stable_ids: BTreeMap<NodeId, Value>,
    pub relationship_stable_ids: BTreeMap<RelId, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMaintenanceAction {
    pub object_type: String,
    pub object: String,
    pub from_state: SchemaObjectState,
    pub to_state: Option<SchemaObjectState>,
    pub action: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMaintenancePlanItem {
    pub object_type: String,
    pub object: String,
    pub from_state: SchemaObjectState,
    pub to_state: Option<SchemaObjectState>,
    pub action: String,
    pub estimated_operations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyIndexProjectionRebuildAction {
    pub index_kind: String,
    pub label: String,
    pub properties: Vec<String>,
    pub estimated_operations: usize,
    pub indexed_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionChange {
    pub commit_epoch: u64,
    pub upsert_node_ids: Vec<u64>,
    pub delete_document_ids: Vec<String>,
    pub relational_primary_key_changes: RelationalPrimaryKeyChangeCapture,
}

impl SearchProjectionChange {
    pub const fn mutation_id(&self) -> SearchProjectionMutationId {
        SearchProjectionMutationId(self.commit_epoch)
    }

    pub fn operation_count(&self) -> usize {
        self.upsert_node_ids
            .len()
            .saturating_add(self.delete_document_ids.len())
            .saturating_add(self.relational_primary_key_changes.operation_count())
    }

    pub fn estimated_retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.upsert_node_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
            .saturating_add(
                self.delete_document_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<String>()),
            )
            .saturating_add(
                self.delete_document_ids
                    .iter()
                    .map(String::capacity)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.relational_primary_key_changes
                    .estimated_retained_bytes(),
            )
    }
}

pub type SearchProjectionGraphChange = SearchProjectionChange;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchProjectionMutationId(pub u64);

impl SearchProjectionMutationId {
    pub const fn commit_epoch(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchProjectionChangefeedStatus {
    pub graph_commit_epoch: u64,
    pub resume_floor_commit_epoch: u64,
    pub oldest_retained_mutation_id: Option<SearchProjectionMutationId>,
    pub newest_retained_mutation_id: Option<SearchProjectionMutationId>,
    pub first_rebuild_required_mutation_id: Option<SearchProjectionMutationId>,
    pub retained_mutation_count: usize,
    pub retained_bytes: usize,
    pub max_retained_bytes: Option<usize>,
    pub restart_recoverable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionChangefeedReadiness {
    pub ready: bool,
    pub incremental_ready: bool,
    pub graph_commit_epoch: u64,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub durable_projection_source_graph_commit_epoch: Option<u64>,
    pub resume_floor_commit_epoch: u64,
    pub restart_recoverable: bool,
    pub max_operations: Option<usize>,
    pub blocker_codes: Vec<String>,
}

impl SearchProjectionChangefeedStatus {
    pub const fn required_projection_commit_epoch(self) -> u64 {
        let newest_retained_epoch = match self.newest_retained_mutation_id {
            Some(mutation_id) => mutation_id.commit_epoch(),
            None => 0,
        };
        if newest_retained_epoch > self.resume_floor_commit_epoch {
            newest_retained_epoch
        } else {
            self.resume_floor_commit_epoch
        }
    }

    pub const fn projection_commit_lag_after(self, projection_commit_epoch: u64) -> u64 {
        self.required_projection_commit_epoch()
            .saturating_sub(projection_commit_epoch)
    }

    pub const fn can_resume_after(self, source_graph_commit_epoch: u64) -> bool {
        source_graph_commit_epoch >= self.resume_floor_commit_epoch
            && source_graph_commit_epoch <= self.graph_commit_epoch
    }

    pub const fn requires_rebuild_after(self, source_graph_commit_epoch: u64) -> bool {
        source_graph_commit_epoch < self.resume_floor_commit_epoch
    }

    pub fn readiness_after(
        self,
        projection_source_graph_commit_epoch: Option<u64>,
        durable_projection_source_graph_commit_epoch: Option<u64>,
        require_restart_recoverable: bool,
        max_operations: Option<usize>,
    ) -> SearchProjectionChangefeedReadiness {
        let resume_epoch = projection_source_graph_commit_epoch.unwrap_or(0);
        let mut blocker_codes = Vec::new();
        if self.requires_rebuild_after(resume_epoch) {
            blocker_codes.push("search_projection_changefeed_resume_floor_expired".to_string());
        }
        if self
            .first_rebuild_required_mutation_id
            .is_some_and(|mutation_id| mutation_id.commit_epoch() > resume_epoch)
        {
            blocker_codes.push("search_projection_changefeed_rebuild_barrier".to_string());
        }
        if resume_epoch > self.graph_commit_epoch {
            blocker_codes.push("search_projection_source_graph_epoch_ahead".to_string());
        }
        if require_restart_recoverable && !self.restart_recoverable {
            blocker_codes.push("search_projection_changefeed_not_restart_recoverable".to_string());
        }
        if let Some(durable_epoch) = durable_projection_source_graph_commit_epoch
            && durable_epoch > self.graph_commit_epoch
        {
            blocker_codes.push("search_projection_durable_source_graph_epoch_ahead".to_string());
        }
        if let Some(0) = max_operations {
            blocker_codes.push("search_projection_changefeed_batch_limit_zero".to_string());
        }
        let incremental_ready = blocker_codes.is_empty();
        SearchProjectionChangefeedReadiness {
            ready: incremental_ready,
            incremental_ready,
            graph_commit_epoch: self.graph_commit_epoch,
            projection_source_graph_commit_epoch,
            durable_projection_source_graph_commit_epoch,
            resume_floor_commit_epoch: self.resume_floor_commit_epoch,
            restart_recoverable: self.restart_recoverable,
            max_operations,
            blocker_codes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{projection_document_id_for_node, ProjectedGraphArtifactData};
    use crate::{NodeId, NodeRecord};
    use skein_core::{Catalog, Value};
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn projected_graph_artifact_data_validates_both_adjacency_views() {
        let artifact = ProjectedGraphArtifactData::new(
            vec![NodeId(4), NodeId(9)],
            vec![0, 1, 2],
            vec![1, 0],
            vec![0, 1, 2],
            vec![1, 0],
        )
        .unwrap();
        assert_eq!(artifact.node_count(), 2);
        assert_eq!(artifact.edge_count(), 2);

        assert!(ProjectedGraphArtifactData::new(
            vec![NodeId(4)],
            vec![0, 2],
            vec![0],
            vec![0, 1],
            vec![0],
        )
        .is_err());
    }

    #[test]
    fn projection_document_identity_preserves_label_aliases_and_id_fallback() {
        let mut catalog = Catalog::default();
        let source_chunk = catalog.get_or_create_label("SourceChunk");
        let node = NodeRecord {
            id: NodeId(7),
            labels: BTreeSet::from([source_chunk]),
            properties: BTreeMap::from([("id".to_string(), Value::Int(42))]),
        };
        assert_eq!(
            projection_document_id_for_node(&catalog, &node),
            Some("source_chunk:42".to_string())
        );

        let fallback = NodeRecord {
            id: NodeId(9),
            labels: BTreeSet::from([source_chunk]),
            properties: BTreeMap::new(),
        };
        assert_eq!(
            projection_document_id_for_node(&catalog, &fallback),
            Some("source_chunk:9".to_string())
        );
    }
}
