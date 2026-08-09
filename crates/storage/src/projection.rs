use crate::{NodeId, RelId};
use skein_core::{SchemaObjectState, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedGraphDefinition {
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
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
pub struct SearchProjectionGraphChange {
    pub commit_epoch: u64,
    pub upsert_node_ids: Vec<u64>,
    pub delete_document_ids: Vec<String>,
}

impl SearchProjectionGraphChange {
    pub const fn mutation_id(&self) -> SearchProjectionMutationId {
        SearchProjectionMutationId(self.commit_epoch)
    }
}

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
    pub retained_mutation_count: usize,
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
