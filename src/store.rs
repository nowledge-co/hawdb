use crate::analytics::ProjectedGraph;
use crate::error::{Result, SkeinError};
use crate::schema::{
    BasicGraphStatistics, Catalog, ConstraintId, GraphStatistics, IndexId, IndexKind, LabelId,
    PropertyId, PropertyType, RelTypeId, SchemaObjectState, TableDescriptor, TableId, TableKind,
};
use crate::search::{
    search_projection_document_id_for_label_and_properties, search_projection_document_id_for_node,
};
use crate::value::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Cursor, Write};
use std::path::{Path, PathBuf};

const STORAGE_VERSION: &str = "skein-storage-v1";
const CHECKPOINT_FILE: &str = "checkpoint.skein";
const MANIFEST_FILE: &str = "manifest.skein";
const PROJECTED_GRAPHS_FILE: &str = "projected_graphs.skein";
const STABLE_ID_MAPPING_FILE: &str = "stable_ids.skein";
const PROJECTED_GRAPH_ARTIFACT_VERSION: u64 = 1;
const WAL_FILE: &str = "wal.skein";
const MIN_PROPERTY_HISTOGRAM_VALUES: usize = 128;
const MID_PROPERTY_HISTOGRAM_VALUES: usize = 256;
const MAX_PROPERTY_HISTOGRAM_VALUES: usize = 512;
const MID_PROPERTY_HISTOGRAM_DISTINCT_VALUES: usize = 1_024;
const MAX_PROPERTY_HISTOGRAM_DISTINCT_VALUES: usize = 4_096;
const MAX_BOUNDED_PATH_STAT_HOPS: usize = 3;
const DURABLE_COMPRESSION_HEADER: &str = "SKEIN_COMPRESSED_V1";
const DEFAULT_COMPRESSION_LEVEL: i32 = 3;
pub const DENSE_ADJACENCY_DEGREE_THRESHOLD: usize = 64;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurabilityPolicy {
    #[default]
    SyncOnCheckpoint,
    SyncOnEveryWrite,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecoveryMode {
    #[default]
    TolerateTornTail,
    Strict,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DurableCompression {
    #[default]
    Zstd,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalReplayConfig {
    pub recovery_mode: RecoveryMode,
    pub max_entries: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub id: NodeId,
    pub labels: BTreeSet<LabelId>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelRecord {
    pub id: RelId,
    pub source: NodeId,
    pub target: NodeId,
    pub rel_type: RelTypeId,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjacencyDirection {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjacencyLayout {
    Sparse,
    Dense,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedAdjacencyEntry {
    pub relationship_id: RelId,
    pub neighbor_id: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdjacencyGroupStats {
    pub node_id: NodeId,
    pub rel_type: RelTypeId,
    pub direction: AdjacencyDirection,
    pub degree: usize,
    pub layout: AdjacencyLayout,
}

type CompositePropertyKey = Vec<(String, Value)>;
type CompositePropertyIndex = BTreeMap<(LabelId, CompositePropertyKey), BTreeSet<NodeId>>;
type FullTextPropertyIndex = BTreeMap<(LabelId, String, String), BTreeSet<NodeId>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectedNodesCreate {
    pub source_label: String,
    pub source_properties: BTreeMap<String, Value>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipCreate {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipMerge {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipOnCreatePropertyValue {
    Value(Value),
    MatchedRelationshipProperty { property: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipCopyMerge {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub old_rel_type: String,
    pub old_rel_filter: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub new_rel_type: String,
    pub new_rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, RelationshipOnCreatePropertyValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipRetargetMerge {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub old_rel_type: String,
    pub old_rel_filter: BTreeMap<String, Value>,
    pub old_target_label: String,
    pub old_target_filter: Option<PropertyFilter>,
    pub new_target_label: String,
    pub new_target_filter: Option<PropertyFilter>,
    pub new_rel_type: String,
    pub new_rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipSourceRetargetMerge {
    pub old_source_label: String,
    pub old_source_filter: Option<PropertyFilter>,
    pub old_rel_type: String,
    pub old_rel_filter: BTreeMap<String, Value>,
    pub old_target_label: String,
    pub old_target_filter: Option<PropertyFilter>,
    pub new_source_label: String,
    pub new_source_filter: Option<PropertyFilter>,
    pub new_rel_type: String,
    pub new_rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipPropertyUpdate {
    pub source_label: String,
    pub filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub rel_filter: Option<PropertyFilter>,
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipSetAssignment {
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipPropertiesUpdate {
    pub source_label: String,
    pub filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub rel_filter: Option<PropertyFilter>,
    pub assignments: Vec<RelationshipSetAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDeleteRequest {
    pub source_label: String,
    pub filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub rel_filter: Option<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipTargetNodeDelete {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub rel_filter: Option<PropertyFilter>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphMutation {
    CreateNodeLabel {
        label: String,
    },
    CreateRelationshipType {
        rel_type: String,
    },
    CreateNodeTable {
        name: String,
    },
    CreateRelationshipTable {
        name: String,
    },
    CreateProperty {
        table_kind: TableKind,
        table: String,
        property: String,
        value_type: PropertyType,
        nullable: bool,
    },
    AlterTableState {
        table_kind: TableKind,
        table: String,
        state: SchemaObjectState,
    },
    AlterPropertyState {
        table_kind: TableKind,
        table: String,
        property: String,
        state: SchemaObjectState,
    },
    CreateIndex {
        label: String,
        property: String,
    },
    CreateCompositeIndex {
        label: String,
        properties: Vec<String>,
    },
    CreateRangeIndex {
        label: String,
        property: String,
    },
    CreateFullTextIndex {
        label: String,
        property: String,
    },
    CreateUniqueConstraint {
        label: String,
        property: String,
    },
    CreateNodePropertyExistsConstraint {
        label: String,
        property: String,
    },
    CreateRelationshipUniqueConstraint {
        rel_type: String,
        property: String,
    },
    CreateRelationshipPropertyExistsConstraint {
        rel_type: String,
        property: String,
    },
    CreateNode {
        label: String,
        properties: BTreeMap<String, Value>,
    },
    MergeNode {
        label: String,
        match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
        on_match_assignments: Vec<NodeSetAssignment>,
        post_merge_assignments: Vec<NodeSetAssignment>,
    },
    MergeConnectedNodes(ConnectedNodesCreate),
    SetNodeProperty {
        label: String,
        filter: Option<PropertyFilter>,
        property: String,
        value: Value,
    },
    SetNodePropertyAddInt {
        label: String,
        filter: Option<PropertyFilter>,
        property: String,
        amount: i64,
    },
    SetNodeProperties {
        label: String,
        filter: Option<PropertyFilter>,
        assignments: Vec<NodeSetAssignment>,
    },
    SetRelationshipProperty {
        source_label: String,
        filter: Option<PropertyFilter>,
        rel_type: String,
        target_label: String,
        target_filter: Option<PropertyFilter>,
        rel_filter: Option<PropertyFilter>,
        property: String,
        value: Value,
    },
    SetRelationshipProperties {
        source_label: String,
        filter: Option<PropertyFilter>,
        rel_type: String,
        target_label: String,
        target_filter: Option<PropertyFilter>,
        rel_filter: Option<PropertyFilter>,
        assignments: Vec<RelationshipSetAssignment>,
    },
    DeleteNode {
        label: String,
        filter: Option<PropertyFilter>,
        detach: bool,
    },
    DeleteRelationship {
        source_label: String,
        filter: Option<PropertyFilter>,
        rel_type: String,
        target_label: String,
        target_filter: Option<PropertyFilter>,
        rel_filter: Option<PropertyFilter>,
    },
    DeleteRelationshipTargetNodes(RelationshipTargetNodeDelete),
    CreateRelationshipsBetweenMatches(MatchedRelationshipCreate),
    MergeRelationshipsBetweenMatches(MatchedRelationshipMerge),
    MergeRelationshipsFromMatchedRelationships(MatchedRelationshipCopyMerge),
    MergeRelationshipsToMatchedTarget(MatchedRelationshipRetargetMerge),
    MergeRelationshipsFromMatchedTarget(MatchedRelationshipSourceRetargetMerge),
    CreateConnectedNodes(ConnectedNodesCreate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSetAssignment {
    pub property: String,
    pub value: NodeSetValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeSetValue {
    Value(Value),
    Coalesce { default: Value },
    AddInt { amount: i64 },
    DecrementFloorZero,
    PreserveNewerExisting { incoming: Value, preserve: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyFilter {
    And(Vec<PropertyFilter>),
    Or(Vec<PropertyFilter>),
    Not(Box<PropertyFilter>),
    IdEq {
        value: Value,
    },
    IdNotEq {
        value: Value,
    },
    IdRange {
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
    IdIn {
        values: Vec<Value>,
    },
    Eq {
        property: String,
        value: Value,
    },
    NotEq {
        property: String,
        value: Value,
    },
    IsNull {
        property: String,
    },
    IsNotNull {
        property: String,
    },
    In {
        property: String,
        values: Vec<Value>,
    },
    ListContains {
        property: String,
        value: Value,
    },
    Contains {
        property: String,
        value: String,
    },
    StartsWith {
        property: String,
        value: String,
    },
    EndsWith {
        property: String,
        value: String,
    },
    RegexMatch {
        property: String,
        pattern: String,
    },
    DefaultIfNullOrEq {
        property: String,
        empty: Value,
        default: Value,
        value: Value,
        negated: bool,
    },
    Range {
        property: String,
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanPruningStrategy {
    FullLabelScan,
    Empty,
    IdEq,
    IdIn,
    IdRange,
    PropertyEq { property: String },
    PropertyNotEq { property: String },
    PropertyIn { property: String },
    PropertyRange { property: String },
    OrUnion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPruningReport {
    pub label_id: Option<LabelId>,
    pub strategy: ScanPruningStrategy,
    pub pruned: bool,
    pub exact_empty: bool,
    pub candidate_count_before_filter: usize,
    pub output_count: usize,
    pub filtered_out_count: usize,
}

#[derive(Debug, Clone)]
pub struct ScanPrunedNodeScan<'a> {
    pub nodes: Vec<&'a NodeRecord>,
    pub report: ScanPruningReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationSummary {
    pub rows: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedGraphDefinition {
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct ProjectedGraphArtifact {
    projection_epoch: u64,
    commit_epoch: u64,
    definition: ProjectedGraphDefinition,
    graph: ProjectedGraph,
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
    pub recovery_mode: RecoveryMode,
    pub max_wal_replay_entries: Option<usize>,
    pub checkpoint_epoch: Option<u64>,
    pub checkpoint_commit_epoch: Option<u64>,
    pub wal_present: bool,
    pub wal_replay_start_lsn: Option<u64>,
    pub next_lsn_after_replay: Option<u64>,
    pub replayed_wal_entries: usize,
    pub torn_tail_ignored: bool,
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

#[derive(Debug, Default)]
pub struct GraphStore {
    next_node_id: u64,
    next_rel_id: u64,
    commit_epoch: u64,
    nodes: BTreeMap<NodeId, NodeRecord>,
    relationships: BTreeMap<RelId, RelRecord>,
    basic_statistics: BasicGraphStatistics,
    outgoing: BTreeMap<(NodeId, RelTypeId), BTreeSet<RelId>>,
    incoming: BTreeMap<(NodeId, RelTypeId), BTreeSet<RelId>>,
    property_index: BTreeMap<(LabelId, String, Value), BTreeSet<NodeId>>,
    composite_property_index: CompositePropertyIndex,
    full_text_property_index: FullTextPropertyIndex,
    projected_graphs: BTreeMap<String, ProjectedGraphDefinition>,
    projected_graph_artifacts: BTreeMap<String, ProjectedGraphArtifact>,
    stable_id_mapping: StoreStableIdMapping,
    search_projection_change_log_start_epoch: u64,
    search_projection_graph_changes: Vec<SearchProjectionGraphChange>,
    max_search_projection_change_log_entries: Option<usize>,
    storage_recovery_report: StorageRecoveryReport,
    durable: Option<DurableStore>,
}

#[derive(Debug, Clone)]
struct ScanPruningCandidate {
    strategy: ScanPruningStrategy,
    node_ids: BTreeSet<NodeId>,
    exact_empty: bool,
}

impl ScanPruningCandidate {
    fn exact(strategy: ScanPruningStrategy, node_ids: BTreeSet<NodeId>) -> Self {
        Self {
            strategy,
            exact_empty: node_ids.is_empty(),
            node_ids,
        }
    }
}

impl GraphStore {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub fn open(path: impl AsRef<Path>, catalog: &mut Catalog) -> Result<Self> {
        Self::open_with_durability(path, catalog, DurabilityPolicy::default())
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::CreateIfMissing,
            WalReplayConfig::default(),
        )
    }

    pub fn open_with_durability_and_recovery(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::CreateIfMissing,
            WalReplayConfig {
                recovery_mode,
                max_entries: None,
            },
        )
    }

    pub fn open_with_durability_and_replay_config(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        replay_config: WalReplayConfig,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::CreateIfMissing,
            replay_config,
        )
    }

    pub fn open_read_only_with_durability(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::ExistingOnly,
            WalReplayConfig {
                recovery_mode,
                max_entries: None,
            },
        )
    }

    pub fn open_read_only_with_durability_and_replay_config(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        replay_config: WalReplayConfig,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::ExistingOnly,
            replay_config,
        )
    }

    fn open_with_options(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        mode: DurableOpenMode,
        replay_config: WalReplayConfig,
    ) -> Result<Self> {
        let durable = match mode {
            DurableOpenMode::CreateIfMissing => DurableStore::open(path.as_ref(), durability)?,
            DurableOpenMode::ExistingOnly => {
                DurableStore::open_existing_only(path.as_ref(), durability)?
            }
        };
        let mut store = Self {
            next_node_id: 0,
            next_rel_id: 0,
            commit_epoch: 0,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
            basic_statistics: BasicGraphStatistics::default(),
            outgoing: BTreeMap::new(),
            incoming: BTreeMap::new(),
            property_index: BTreeMap::new(),
            composite_property_index: BTreeMap::new(),
            full_text_property_index: BTreeMap::new(),
            projected_graphs: BTreeMap::new(),
            projected_graph_artifacts: BTreeMap::new(),
            stable_id_mapping: StoreStableIdMapping::default(),
            search_projection_change_log_start_epoch: 0,
            search_projection_graph_changes: Vec::new(),
            max_search_projection_change_log_entries: None,
            storage_recovery_report: StorageRecoveryReport::default(),
            durable: Some(durable),
        };
        store.load_checkpoint(catalog)?;
        store.storage_recovery_report = store.replay_wal(catalog, replay_config)?;
        store.validate_relationship_endpoints()?;
        store.refresh_basic_statistics_epoch();
        store.load_projected_graph_artifacts()?;
        store.load_stable_id_mapping()?;
        Ok(store)
    }

    pub fn create_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: BTreeMap<String, Value>,
    ) -> Result<NodeId> {
        let label_id = catalog.get_or_create_label(label);
        let id = NodeId(self.next_node_id);
        let ops = [WalOp::CreateNode {
            id,
            label: label.to_string(),
            properties: properties.clone(),
        }];
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_create_node(id, label, &properties)?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        register_property_index_descriptors(catalog, [label_id], &properties);
        self.apply_create_node(catalog, id, label_id, properties);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_node_label(&mut self, catalog: &mut Catalog, label: &str) -> Result<LabelId> {
        if let Some(id) = catalog.label_id(label) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateNodeLabel {
                label: label.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_label(label);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_relationship_type(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
    ) -> Result<RelTypeId> {
        if let Some(id) = catalog.rel_type_id(rel_type) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRelationshipType {
                rel_type: rel_type.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_rel_type(rel_type);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_node_table(&mut self, catalog: &mut Catalog, name: &str) -> Result<TableId> {
        if let Some(id) = catalog.table_id(TableKind::Node, name) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateNodeTable {
                name: name.to_string(),
            }])?;
        }
        catalog.get_or_create_label(name);
        let id = catalog.get_or_create_table(TableKind::Node, name);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_relationship_table(
        &mut self,
        catalog: &mut Catalog,
        name: &str,
    ) -> Result<TableId> {
        if let Some(id) = catalog.table_id(TableKind::Relationship, name) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRelationshipTable {
                name: name.to_string(),
            }])?;
        }
        catalog.get_or_create_rel_type(name);
        let id = catalog.get_or_create_table(TableKind::Relationship, name);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_property_descriptor(
        &mut self,
        catalog: &mut Catalog,
        table_kind: TableKind,
        table: &str,
        property: &str,
        value_type: PropertyType,
        nullable: bool,
    ) -> Result<PropertyId> {
        let table_id = ensure_table_descriptor(catalog, table_kind, table);
        if let Some(id) = catalog.property_descriptor_id(table_id, property) {
            return Ok(id);
        }
        validate_property_descriptor(catalog, self, table_id, property, value_type, nullable)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateProperty {
                table_kind,
                table: table.to_string(),
                property: property.to_string(),
                value_type,
                nullable,
            }])?;
        }
        let id = catalog.get_or_create_property(table_id, property, value_type, nullable);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn alter_table_state(
        &mut self,
        catalog: &mut Catalog,
        table_kind: TableKind,
        table: &str,
        state: SchemaObjectState,
    ) -> Result<(TableId, bool)> {
        let Some(id) = catalog.table_id(table_kind, table) else {
            return Err(SkeinError::Storage(format!(
                "schema table '{table}' does not exist"
            )));
        };
        let Some(descriptor) = catalog.table_descriptor(id) else {
            return Err(SkeinError::Storage(format!(
                "schema table '{table}' does not exist"
            )));
        };
        if descriptor.state == state {
            return Ok((id, false));
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::AlterTableState {
                table_kind,
                table: table.to_string(),
                state,
            }])?;
        }
        catalog.set_table_state(id, state);
        self.commit_epoch += 1;
        Ok((id, true))
    }

    pub fn alter_property_state(
        &mut self,
        catalog: &mut Catalog,
        table_kind: TableKind,
        table: &str,
        property: &str,
        state: SchemaObjectState,
    ) -> Result<(PropertyId, bool)> {
        let Some(table_id) = catalog.table_id(table_kind, table) else {
            return Err(SkeinError::Storage(format!(
                "schema table '{table}' does not exist"
            )));
        };
        let Some(id) = catalog.property_descriptor_id(table_id, property) else {
            return Err(SkeinError::Storage(format!(
                "schema property '{table}.{property}' does not exist"
            )));
        };
        let Some(descriptor) = catalog.property_descriptor(id) else {
            return Err(SkeinError::Storage(format!(
                "schema property '{table}.{property}' does not exist"
            )));
        };
        if descriptor.state == state {
            return Ok((id, false));
        }
        if state == SchemaObjectState::Public {
            validate_property_descriptor(
                catalog,
                self,
                table_id,
                property,
                descriptor.value_type,
                descriptor.nullable,
            )?;
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::AlterPropertyState {
                table_kind,
                table: table.to_string(),
                property: property.to_string(),
                state,
            }])?;
        }
        catalog.set_property_state(id, state);
        self.commit_epoch += 1;
        Ok((id, true))
    }

    pub fn run_schema_maintenance(
        &mut self,
        catalog: &mut Catalog,
    ) -> Result<Vec<SchemaMaintenanceAction>> {
        self.run_schema_maintenance_with_budget(catalog, None)
    }

    pub fn run_bounded_schema_maintenance(
        &mut self,
        catalog: &mut Catalog,
        max_estimated_operations: usize,
    ) -> Result<Vec<SchemaMaintenanceAction>> {
        self.run_schema_maintenance_with_budget(catalog, Some(max_estimated_operations))
    }

    fn run_schema_maintenance_with_budget(
        &mut self,
        catalog: &mut Catalog,
        max_estimated_operations: Option<usize>,
    ) -> Result<Vec<SchemaMaintenanceAction>> {
        let mut ops = Vec::new();
        let mut actions = Vec::new();
        let mut used_estimated_operations = 0usize;

        let gc_table_ids = catalog
            .table_descriptors()
            .filter(|table| table.state == SchemaObjectState::Gc)
            .map(|table| table.id)
            .collect::<BTreeSet<_>>();

        for property in catalog.property_descriptors().cloned().collect::<Vec<_>>() {
            if gc_table_ids.contains(&property.table_id) {
                continue;
            }
            let Some(table) = catalog.table_descriptor(property.table_id).cloned() else {
                continue;
            };
            match property.state {
                SchemaObjectState::Backfill => {
                    let estimated_operations =
                        self.schema_table_record_count(catalog, &table).max(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    validate_property_descriptor(
                        catalog,
                        self,
                        property.table_id,
                        &property.name,
                        property.value_type,
                        property.nullable,
                    )?;
                    ops.push(WalOp::AlterPropertyState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        property: property.name.clone(),
                        state: SchemaObjectState::Validating,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Validating => {
                    let estimated_operations =
                        self.schema_table_record_count(catalog, &table).max(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    validate_property_descriptor(
                        catalog,
                        self,
                        property.table_id,
                        &property.name,
                        property.value_type,
                        property.nullable,
                    )?;
                    ops.push(WalOp::AlterPropertyState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        property: property.name.clone(),
                        state: SchemaObjectState::Public,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Gc => {
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        1,
                    ) {
                        continue;
                    }
                    ops.push(WalOp::GcPropertyDescriptor {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        property: property.name.clone(),
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: None,
                        action: "gc".to_string(),
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        for table in catalog.table_descriptors().cloned().collect::<Vec<_>>() {
            match table.state {
                SchemaObjectState::Backfill => {
                    let estimated_operations =
                        self.schema_table_record_count(catalog, &table).max(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    ops.push(WalOp::AlterTableState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        state: SchemaObjectState::Validating,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Validating => {
                    let active_property_count = catalog
                        .property_descriptors()
                        .filter(|property| {
                            property.table_id == table.id && property.state != SchemaObjectState::Gc
                        })
                        .count()
                        .max(1);
                    let estimated_operations = self
                        .schema_table_record_count(catalog, &table)
                        .max(1)
                        .saturating_mul(active_property_count);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    validate_table_descriptor(catalog, self, table.id)?;
                    ops.push(WalOp::AlterTableState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        state: SchemaObjectState::Public,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Gc => {
                    let properties = catalog
                        .property_descriptors()
                        .filter(|property| property.table_id == table.id)
                        .cloned()
                        .collect::<Vec<_>>();
                    let estimated_operations = properties.len().saturating_add(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    for property in properties {
                        ops.push(WalOp::GcPropertyDescriptor {
                            table_kind: table.kind,
                            table: table.name.clone(),
                            property: property.name.clone(),
                        });
                        actions.push(SchemaMaintenanceAction {
                            object_type: "property".to_string(),
                            object: format!("{}.{}", table.name, property.name),
                            from_state: property.state,
                            to_state: None,
                            action: "gc".to_string(),
                        });
                    }
                    ops.push(WalOp::GcTableDescriptor {
                        table_kind: table.kind,
                        table: table.name.clone(),
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: None,
                        action: "gc".to_string(),
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        if ops.is_empty() {
            return Ok(actions);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_schema_maintenance_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(actions)
    }

    pub fn plan_schema_maintenance(&self, catalog: &Catalog) -> Vec<SchemaMaintenancePlanItem> {
        let mut plan = Vec::new();

        let gc_table_ids = catalog
            .table_descriptors()
            .filter(|table| table.state == SchemaObjectState::Gc)
            .map(|table| table.id)
            .collect::<BTreeSet<_>>();

        for property in catalog.property_descriptors().cloned() {
            if gc_table_ids.contains(&property.table_id) {
                continue;
            }
            let Some(table) = catalog.table_descriptor(property.table_id) else {
                continue;
            };
            let estimated_operations = self.schema_table_record_count(catalog, table).max(1);
            match property.state {
                SchemaObjectState::Backfill => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                        estimated_operations,
                    });
                }
                SchemaObjectState::Validating => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                        estimated_operations,
                    });
                }
                SchemaObjectState::Gc => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: None,
                        action: "gc".to_string(),
                        estimated_operations: 1,
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        for table in catalog.table_descriptors().cloned() {
            let record_count = self.schema_table_record_count(catalog, &table).max(1);
            match table.state {
                SchemaObjectState::Backfill => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                        estimated_operations: record_count,
                    });
                }
                SchemaObjectState::Validating => {
                    let active_property_count = catalog
                        .property_descriptors()
                        .filter(|property| {
                            property.table_id == table.id && property.state != SchemaObjectState::Gc
                        })
                        .count()
                        .max(1);
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                        estimated_operations: record_count.saturating_mul(active_property_count),
                    });
                }
                SchemaObjectState::Gc => {
                    for property in catalog
                        .property_descriptors()
                        .filter(|property| property.table_id == table.id)
                    {
                        plan.push(SchemaMaintenancePlanItem {
                            object_type: "property".to_string(),
                            object: format!("{}.{}", table.name, property.name),
                            from_state: property.state,
                            to_state: None,
                            action: "gc".to_string(),
                            estimated_operations: 1,
                        });
                    }
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: None,
                        action: "gc".to_string(),
                        estimated_operations: 1,
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        plan
    }

    fn schema_table_record_count(&self, catalog: &Catalog, table: &TableDescriptor) -> usize {
        match table.kind {
            TableKind::Node => {
                let Some(label_id) = catalog.label_id(&table.name) else {
                    return 0;
                };
                self.index_label_record_count(label_id)
            }
            TableKind::Relationship => {
                let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                    return 0;
                };
                self.relationships
                    .values()
                    .filter(|relationship| relationship.rel_type == rel_type_id)
                    .count()
            }
        }
    }

    fn index_label_record_count(&self, label_id: LabelId) -> usize {
        self.nodes
            .values()
            .filter(|node| node.labels.contains(&label_id))
            .count()
    }

    pub fn create_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<IndexId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.property_index_id(label_id, property) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateIndex {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_property_index(label_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_composite_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: &[String],
    ) -> Result<IndexId> {
        if properties.len() < 2 {
            return Err(SkeinError::Storage(
                "composite index requires at least two properties".to_string(),
            ));
        }
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.composite_property_index_id(label_id, properties) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateCompositeIndex {
                label: label.to_string(),
                properties: properties.to_vec(),
            }])?;
        }
        let id = catalog.get_or_create_composite_property_index(label_id, properties);
        self.rebuild_composite_property_index_for_descriptor(label_id, properties);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_range_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<IndexId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.property_index_id_with_kind(label_id, property, IndexKind::Range)
        {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRangeIndex {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id =
            catalog.get_or_create_property_index_with_kind(label_id, property, IndexKind::Range);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_full_text_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<IndexId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) =
            catalog.property_index_id_with_kind(label_id, property, IndexKind::FullText)
        {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateFullTextIndex {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id =
            catalog.get_or_create_property_index_with_kind(label_id, property, IndexKind::FullText);
        self.rebuild_full_text_property_index_for_descriptor(label_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn rebuild_bounded_property_index_projections(
        &mut self,
        catalog: &Catalog,
        max_estimated_operations: usize,
    ) -> Vec<PropertyIndexProjectionRebuildAction> {
        let mut actions = Vec::new();
        let mut used_estimated_operations = 0usize;

        for index in catalog
            .composite_property_indexes()
            .cloned()
            .collect::<Vec<_>>()
        {
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            if !reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            ) {
                continue;
            }
            let indexed_entries =
                self.rebuild_composite_property_index_projection(index.label_id, &index.properties);
            actions.push(PropertyIndexProjectionRebuildAction {
                index_kind: "composite".to_string(),
                label: catalog
                    .label_name(index.label_id)
                    .unwrap_or("<unknown>")
                    .to_string(),
                properties: index.properties,
                estimated_operations,
                indexed_entries,
            });
        }

        for index in catalog.property_indexes().cloned().collect::<Vec<_>>() {
            if index.kind != IndexKind::FullText {
                continue;
            }
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            if !reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            ) {
                continue;
            }
            let indexed_entries =
                self.rebuild_full_text_property_index_projection(index.label_id, &index.property);
            actions.push(PropertyIndexProjectionRebuildAction {
                index_kind: "full_text".to_string(),
                label: catalog
                    .label_name(index.label_id)
                    .unwrap_or("<unknown>")
                    .to_string(),
                properties: vec![index.property],
                estimated_operations,
                indexed_entries,
            });
        }

        actions
    }

    pub fn bounded_property_index_projection_estimated_operations(
        &self,
        catalog: &Catalog,
        max_estimated_operations: usize,
    ) -> usize {
        let mut used_estimated_operations = 0usize;

        for index in catalog.composite_property_indexes() {
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            let _ = reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            );
        }

        for index in catalog.property_indexes() {
            if index.kind != IndexKind::FullText {
                continue;
            }
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            let _ = reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            );
        }

        used_estimated_operations
    }

    pub fn property_index_projection_estimated_operations(&self, catalog: &Catalog) -> usize {
        let composite_operations = catalog
            .composite_property_indexes()
            .map(|index| self.index_label_record_count(index.label_id).max(1))
            .fold(0usize, usize::saturating_add);
        let full_text_operations = catalog
            .property_indexes()
            .filter(|index| index.kind == IndexKind::FullText)
            .map(|index| self.index_label_record_count(index.label_id).max(1))
            .fold(0usize, usize::saturating_add);
        composite_operations.saturating_add(full_text_operations)
    }

    pub fn create_unique_constraint(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.unique_constraint_id(label_id, property) {
            return Ok(id);
        }
        self.validate_unique_constraint(catalog, label_id, property)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateUniqueConstraint {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_unique_constraint(label_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_node_property_exists_constraint(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.node_property_exists_constraint_id(label_id, property) {
            return Ok(id);
        }
        self.validate_node_property_exists_constraint(catalog, label_id, property)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateNodePropertyExistsConstraint {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_node_property_exists_constraint(label_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_relationship_property_exists_constraint(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        if let Some(id) = catalog.relationship_property_exists_constraint_id(rel_type_id, property)
        {
            return Ok(id);
        }
        self.validate_relationship_property_exists_constraint(catalog, rel_type_id, property)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRelationshipPropertyExistsConstraint {
                rel_type: rel_type.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id =
            catalog.get_or_create_relationship_property_exists_constraint(rel_type_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_relationship_unique_constraint(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        if let Some(id) = catalog.relationship_unique_constraint_id(rel_type_id, property) {
            return Ok(id);
        }
        self.validate_relationship_unique_constraint(catalog, rel_type_id, property)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRelationshipUniqueConstraint {
                rel_type: rel_type.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_relationship_unique_constraint(rel_type_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn merge_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
        on_match_assignments: &[NodeSetAssignment],
        post_merge_assignments: &[NodeSetAssignment],
    ) -> Result<(NodeId, bool)> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = self.find_node_by_label_and_properties(label_id, &match_properties) {
            if !on_match_assignments.is_empty() || !post_merge_assignments.is_empty() {
                let mut assignments =
                    Vec::with_capacity(on_match_assignments.len() + post_merge_assignments.len());
                assignments.extend_from_slice(on_match_assignments);
                assignments.extend_from_slice(post_merge_assignments);
                let ops = self.node_set_property_ops(&[id], &assignments)?;
                self.validate_constraints_for_ops(catalog, &ops)?;
                if let Some(durable) = &mut self.durable {
                    durable.append_batch(ops.clone())?;
                }
                self.record_search_projection_graph_changes_for_ops(
                    catalog,
                    self.commit_epoch + 1,
                    &ops,
                );
                for op in ops {
                    self.apply_wal_op(catalog, op);
                }
                self.commit_epoch += 1;
            }
            return Ok((id, false));
        }
        let mut properties = match_properties;
        for (property, value) in on_create_properties {
            properties.insert(property, value);
        }
        apply_node_assignments_to_properties(&mut properties, post_merge_assignments)?;
        let id = NodeId(self.next_node_id);
        let ops = [WalOp::CreateNode {
            id,
            label: label.to_string(),
            properties: properties.clone(),
        }];
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_create_node(id, label, &properties)?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        register_property_index_descriptors(catalog, [label_id], &properties);
        self.apply_create_node(catalog, id, label_id, properties);
        self.commit_epoch += 1;
        Ok((id, true))
    }

    pub fn create_relationship(
        &mut self,
        catalog: &mut Catalog,
        source: NodeId,
        target: NodeId,
        rel_type: &str,
        properties: BTreeMap<String, Value>,
    ) -> Result<RelId> {
        if !self.nodes.contains_key(&source) {
            return Err(SkeinError::Storage(format!(
                "source node {} does not exist",
                source.0
            )));
        }
        if !self.nodes.contains_key(&target) {
            return Err(SkeinError::Storage(format!(
                "target node {} does not exist",
                target.0
            )));
        }
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        let id = RelId(self.next_rel_id);
        if let Some(durable) = &mut self.durable {
            durable.append_create_relationship(id, source, target, rel_type, &properties)?;
        }
        self.apply_create_relationship(id, source, target, rel_type_id, properties);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_relationships_between_matches(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipCreate,
    ) -> Result<Vec<(NodeId, RelId, NodeId)>> {
        let source_label_id = if request.source_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.source_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let target_label_id = if request.target_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.target_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let sources = self
            .matching_node_ids(source_label_id, request.source_filter.as_ref())
            .collect::<Vec<_>>();
        let targets = self
            .matching_node_ids(target_label_id, request.target_filter.as_ref())
            .collect::<Vec<_>>();
        if sources.is_empty() || targets.is_empty() {
            return Ok(Vec::new());
        }

        catalog.get_or_create_rel_type(&request.rel_type);
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::with_capacity(sources.len() * targets.len());
        let mut ops = Vec::with_capacity(sources.len() * targets.len());
        for source in sources {
            for target in &targets {
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.rel_type.clone(),
                    properties: request.rel_properties.clone(),
                });
                rows.push((source, rel, *target));
            }
        }
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(rows)
    }

    pub fn merge_relationships_between_matches(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let source_label_id = if request.source_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.source_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let target_label_id = if request.target_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.target_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let sources = self
            .matching_node_ids(source_label_id, request.source_filter.as_ref())
            .collect::<Vec<_>>();
        let targets = self
            .matching_node_ids(target_label_id, request.target_filter.as_ref())
            .collect::<Vec<_>>();
        if sources.is_empty() || targets.is_empty() {
            return Ok(Vec::new());
        }

        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::with_capacity(sources.len() * targets.len());
        let mut ops = Vec::new();
        for source in sources {
            for target in &targets {
                if let Some(rel) = self.find_relationship_by_property_subset(
                    source,
                    *target,
                    rel_type_id,
                    &request.rel_match_properties,
                ) {
                    rows.push((source, rel, *target, false));
                    continue;
                }
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                let mut properties = request.rel_match_properties.clone();
                for (property, value) in &request.on_create_properties {
                    properties.insert(property.clone(), value.clone());
                }
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.rel_type.clone(),
                    properties,
                });
                rows.push((source, rel, *target, true));
            }
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            if let Some(durable) = &mut self.durable {
                durable.append_batch(ops.clone())?;
            }
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op);
            }
            self.commit_epoch += 1;
        }
        Ok(rows)
    }

    pub fn merge_relationships_from_matched_relationships(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipCopyMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let source_label_id = if request.source_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.source_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let target_label_id = if request.target_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.target_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let Some(old_rel_type_id) = catalog.rel_type_id(&request.old_rel_type) else {
            return Ok(Vec::new());
        };
        let new_rel_type_id = catalog.get_or_create_rel_type(&request.new_rel_type);
        let old_relationships = self
            .scan_relationships(Some(old_rel_type_id))
            .filter(|relationship| {
                properties_contain_all(&relationship.properties, &request.old_rel_filter)
                    && self
                        .nodes
                        .get(&relationship.source)
                        .map(|node| {
                            source_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .source_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
                    && self
                        .nodes
                        .get(&relationship.target)
                        .map(|node| {
                            target_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .target_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
            })
            .cloned()
            .collect::<Vec<_>>();

        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::with_capacity(old_relationships.len());
        let mut ops = Vec::new();
        for old_relationship in old_relationships {
            if let Some(rel) = self.find_relationship_by_property_subset(
                old_relationship.source,
                old_relationship.target,
                new_rel_type_id,
                &request.new_rel_match_properties,
            ) {
                rows.push((old_relationship.source, rel, old_relationship.target, false));
                continue;
            }
            let rel = RelId(next_rel_id);
            next_rel_id += 1;
            let mut properties = request.new_rel_match_properties.clone();
            for (property, value) in &request.on_create_properties {
                let value = match value {
                    RelationshipOnCreatePropertyValue::Value(value) => value.clone(),
                    RelationshipOnCreatePropertyValue::MatchedRelationshipProperty { property } => {
                        old_relationship
                            .properties
                            .get(property)
                            .cloned()
                            .unwrap_or(Value::Null)
                    }
                };
                properties.insert(property.clone(), value);
            }
            ops.push(WalOp::CreateRelationship {
                id: rel,
                source: old_relationship.source,
                target: old_relationship.target,
                rel_type: request.new_rel_type.clone(),
                properties,
            });
            rows.push((old_relationship.source, rel, old_relationship.target, true));
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            if let Some(durable) = &mut self.durable {
                durable.append_batch(ops.clone())?;
            }
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op);
            }
            self.commit_epoch += 1;
        }
        Ok(rows)
    }

    pub fn merge_relationships_to_matched_target(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipRetargetMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let source_label_id = optional_label_id(catalog, &request.source_label);
        if !request.source_label.is_empty() && source_label_id.is_none() {
            return Ok(Vec::new());
        }
        let Some(old_target_label_id) = catalog.label_id(&request.old_target_label) else {
            return Ok(Vec::new());
        };
        let Some(new_target_label_id) = catalog.label_id(&request.new_target_label) else {
            return Ok(Vec::new());
        };
        let Some(old_rel_type_id) = catalog.rel_type_id(&request.old_rel_type) else {
            return Ok(Vec::new());
        };
        let new_rel_type_id = catalog.get_or_create_rel_type(&request.new_rel_type);
        let source_ids = self
            .scan_relationships(Some(old_rel_type_id))
            .filter(|relationship| {
                properties_contain_all(&relationship.properties, &request.old_rel_filter)
                    && self
                        .nodes
                        .get(&relationship.source)
                        .map(|node| {
                            source_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .source_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
                    && self
                        .nodes
                        .get(&relationship.target)
                        .map(|node| {
                            node.labels.contains(&old_target_label_id)
                                && request
                                    .old_target_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
            })
            .map(|relationship| relationship.source)
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = self
            .matching_node_ids(
                Some(new_target_label_id),
                request.new_target_filter.as_ref(),
            )
            .collect::<Vec<_>>();
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::new();
        let mut ops = Vec::new();
        for source in source_ids {
            for target in &target_ids {
                if let Some(rel) = self.find_relationship_by_property_subset(
                    source,
                    *target,
                    new_rel_type_id,
                    &request.new_rel_match_properties,
                ) {
                    rows.push((source, rel, *target, false));
                    continue;
                }
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                let mut properties = request.new_rel_match_properties.clone();
                properties.extend(request.on_create_properties.clone());
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.new_rel_type.clone(),
                    properties,
                });
                rows.push((source, rel, *target, true));
            }
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            if let Some(durable) = &mut self.durable {
                durable.append_batch(ops.clone())?;
            }
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op);
            }
            self.commit_epoch += 1;
        }
        Ok(rows)
    }

    pub fn merge_relationships_from_matched_target(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipSourceRetargetMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let old_source_label_id = optional_label_id(catalog, &request.old_source_label);
        if !request.old_source_label.is_empty() && old_source_label_id.is_none() {
            return Ok(Vec::new());
        }
        let Some(old_target_label_id) = catalog.label_id(&request.old_target_label) else {
            return Ok(Vec::new());
        };
        let new_source_label_id = optional_label_id(catalog, &request.new_source_label);
        if !request.new_source_label.is_empty() && new_source_label_id.is_none() {
            return Ok(Vec::new());
        }
        let Some(old_rel_type_id) = catalog.rel_type_id(&request.old_rel_type) else {
            return Ok(Vec::new());
        };
        let new_rel_type_id = catalog.get_or_create_rel_type(&request.new_rel_type);
        let target_ids = self
            .scan_relationships(Some(old_rel_type_id))
            .filter(|relationship| {
                properties_contain_all(&relationship.properties, &request.old_rel_filter)
                    && self
                        .nodes
                        .get(&relationship.source)
                        .map(|node| {
                            old_source_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .old_source_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
                    && self
                        .nodes
                        .get(&relationship.target)
                        .map(|node| {
                            node.labels.contains(&old_target_label_id)
                                && request
                                    .old_target_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
            })
            .map(|relationship| relationship.target)
            .collect::<BTreeSet<_>>();
        if target_ids.is_empty() {
            return Ok(Vec::new());
        }
        let source_ids = self
            .matching_node_ids(new_source_label_id, request.new_source_filter.as_ref())
            .collect::<Vec<_>>();
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::new();
        let mut ops = Vec::new();
        for source in source_ids {
            for target in &target_ids {
                if let Some(rel) = self.find_relationship_by_property_subset(
                    source,
                    *target,
                    new_rel_type_id,
                    &request.new_rel_match_properties,
                ) {
                    rows.push((source, rel, *target, false));
                    continue;
                }
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                let mut properties = request.new_rel_match_properties.clone();
                properties.extend(request.on_create_properties.clone());
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.new_rel_type.clone(),
                    properties,
                });
                rows.push((source, rel, *target, true));
            }
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            if let Some(durable) = &mut self.durable {
                durable.append_batch(ops.clone())?;
            }
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op);
            }
            self.commit_epoch += 1;
        }
        Ok(rows)
    }

    pub fn set_node_property(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        property: &str,
        value: Value,
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(label_id, filter).collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = ids
            .iter()
            .map(|id| WalOp::SetNodeProperty {
                id: *id,
                property: property.to_string(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn add_int_node_property(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        property: &str,
        amount: i64,
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(label_id, filter).collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = ids
            .iter()
            .map(|id| {
                let node = self.nodes.get(id).ok_or_else(|| {
                    SkeinError::Storage(format!("node {} disappeared during property update", id.0))
                })?;
                let current = match node.properties.get(property) {
                    None | Some(Value::Null) => 0,
                    Some(Value::Int(value)) => *value,
                    Some(value) => {
                        return Err(SkeinError::Execution(format!(
                            "property increment requires an integer or null value, got {value:?}"
                        )));
                    }
                };
                let value = current.checked_add(amount).ok_or_else(|| {
                    SkeinError::Execution("property increment overflowed i64".to_string())
                })?;
                Ok(WalOp::SetNodeProperty {
                    id: *id,
                    property: property.to_string(),
                    value: Value::Int(value),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            if let WalOp::SetNodeProperty {
                id,
                property,
                value,
            } = op
            {
                self.apply_set_node_property(catalog, id, property, value);
            }
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn set_node_properties(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        assignments: &[NodeSetAssignment],
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(label_id, filter).collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.node_set_property_ops(&ids, assignments)?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn set_node_properties_by_ids(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
    ) -> Result<Vec<NodeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.node_set_property_ops(ids, assignments)?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(ids.to_vec())
    }

    fn node_set_property_ops(
        &self,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
    ) -> Result<Vec<WalOp>> {
        let mut ops = Vec::with_capacity(ids.len().saturating_mul(assignments.len()));
        for id in ids {
            let node = self.nodes.get(id).ok_or_else(|| {
                SkeinError::Storage(format!("node {} disappeared during property update", id.0))
            })?;
            for assignment in assignments {
                let value = evaluate_node_set_value(&node.properties, assignment)?;
                ops.push(WalOp::SetNodeProperty {
                    id: *id,
                    property: assignment.property.clone(),
                    value,
                });
            }
        }
        Ok(ops)
    }

    pub fn delete_nodes(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        detach: bool,
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(label_id, filter).collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.delete_node_ops(&ids, detach)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn delete_relationships(
        &mut self,
        catalog: &mut Catalog,
        request: RelationshipDeleteRequest,
    ) -> Result<Vec<RelId>> {
        let Some(source_label_id) = catalog.label_id(&request.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = catalog.label_id(&request.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&request.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids(Some(source_label_id), request.filter.as_ref())
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = request.target_filter.as_ref().map(|filter| {
            self.matching_node_ids(Some(target_label_id), Some(filter))
                .collect::<BTreeSet<_>>()
        });
        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
            return Ok(Vec::new());
        }
        let ids = self
            .relationships
            .values()
            .filter(|relationship| {
                relationship.rel_type == rel_type_id && source_ids.contains(&relationship.source)
            })
            .filter(|relationship| {
                request
                    .rel_filter
                    .as_ref()
                    .map(|filter| {
                        property_filter_matches(filter, relationship.id.0, &relationship.properties)
                    })
                    .unwrap_or(true)
            })
            .filter(|relationship| {
                self.nodes
                    .get(&relationship.target)
                    .map(|target| {
                        target.labels.contains(&target_label_id)
                            && target_ids
                                .as_ref()
                                .map(|ids| ids.contains(&relationship.target))
                                .unwrap_or(true)
                    })
                    .unwrap_or(false)
            })
            .map(|relationship| relationship.id)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = ids
            .iter()
            .copied()
            .map(|id| WalOp::DeleteRelationship { id })
            .collect::<Vec<_>>();
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn delete_relationship_target_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: RelationshipTargetNodeDelete,
    ) -> Result<Vec<NodeId>> {
        let ids = self.relationship_target_node_ids(catalog, &request);
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.delete_node_ops(&ids, request.detach)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    fn relationship_target_node_ids(
        &self,
        catalog: &Catalog,
        request: &RelationshipTargetNodeDelete,
    ) -> Vec<NodeId> {
        let Some(source_label_id) = optional_label_id(catalog, &request.source_label) else {
            return Vec::new();
        };
        let Some(target_label_id) = optional_label_id(catalog, &request.target_label) else {
            return Vec::new();
        };
        let Some(rel_type_id) = catalog.rel_type_id(&request.rel_type) else {
            return Vec::new();
        };
        let source_ids = self
            .matching_node_ids(Some(source_label_id), request.source_filter.as_ref())
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Vec::new();
        }
        let target_ids = request.target_filter.as_ref().map(|filter| {
            self.matching_node_ids(Some(target_label_id), Some(filter))
                .collect::<BTreeSet<_>>()
        });
        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
            return Vec::new();
        }
        self.relationships
            .values()
            .filter(|relationship| {
                relationship.rel_type == rel_type_id && source_ids.contains(&relationship.source)
            })
            .filter(|relationship| {
                request
                    .rel_filter
                    .as_ref()
                    .map(|filter| {
                        property_filter_matches(filter, relationship.id.0, &relationship.properties)
                    })
                    .unwrap_or(true)
            })
            .filter(|relationship| {
                self.nodes
                    .get(&relationship.target)
                    .map(|target| {
                        target.labels.contains(&target_label_id)
                            && target_ids
                                .as_ref()
                                .map(|ids| ids.contains(&relationship.target))
                                .unwrap_or(true)
                    })
                    .unwrap_or(false)
            })
            .map(|relationship| relationship.target)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn set_relationship_property(
        &mut self,
        catalog: &mut Catalog,
        update: RelationshipPropertyUpdate,
    ) -> Result<Vec<RelId>> {
        self.set_relationship_properties(
            catalog,
            RelationshipPropertiesUpdate {
                source_label: update.source_label,
                filter: update.filter,
                rel_type: update.rel_type,
                target_label: update.target_label,
                target_filter: update.target_filter,
                rel_filter: update.rel_filter,
                assignments: vec![RelationshipSetAssignment {
                    property: update.property,
                    value: update.value,
                }],
            },
        )
    }

    pub fn set_relationship_properties(
        &mut self,
        catalog: &mut Catalog,
        update: RelationshipPropertiesUpdate,
    ) -> Result<Vec<RelId>> {
        let Some(source_label_id) = catalog.label_id(&update.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = catalog.label_id(&update.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&update.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids(Some(source_label_id), update.filter.as_ref())
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = self
            .relationships
            .values()
            .filter(|relationship| {
                relationship.rel_type == rel_type_id && source_ids.contains(&relationship.source)
            })
            .filter(|relationship| {
                update
                    .rel_filter
                    .as_ref()
                    .map(|filter| {
                        property_filter_matches(filter, relationship.id.0, &relationship.properties)
                    })
                    .unwrap_or(true)
            })
            .filter(|relationship| {
                self.nodes
                    .get(&relationship.target)
                    .map(|target| target.labels.contains(&target_label_id))
                    .unwrap_or(false)
            })
            .map(|relationship| relationship.id)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops =
            ids.iter()
                .flat_map(|id| {
                    update.assignments.iter().map(move |assignment| {
                        WalOp::SetRelationshipProperty {
                            id: *id,
                            property: assignment.property.clone(),
                            value: assignment.value.clone(),
                        }
                    })
                })
                .collect::<Vec<_>>();
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn create_connected_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: ConnectedNodesCreate,
    ) -> Result<(NodeId, RelId, NodeId)> {
        let source_label_id = catalog.get_or_create_label(&request.source_label);
        let target_label_id = catalog.get_or_create_label(&request.target_label);
        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let source = NodeId(self.next_node_id);
        let target = NodeId(self.next_node_id + 1);
        let relationship = RelId(self.next_rel_id);
        let ops = vec![
            WalOp::CreateNode {
                id: source,
                label: request.source_label.clone(),
                properties: request.source_properties.clone(),
            },
            WalOp::CreateNode {
                id: target,
                label: request.target_label.clone(),
                properties: request.target_properties.clone(),
            },
            WalOp::CreateRelationship {
                id: relationship,
                source,
                target,
                rel_type: request.rel_type.clone(),
                properties: request.rel_properties.clone(),
            },
        ];
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops)?;
        }
        register_property_index_descriptors(catalog, [source_label_id], &request.source_properties);
        register_property_index_descriptors(catalog, [target_label_id], &request.target_properties);
        self.apply_create_node(catalog, source, source_label_id, request.source_properties);
        self.apply_create_node(catalog, target, target_label_id, request.target_properties);
        self.apply_create_relationship(
            relationship,
            source,
            target,
            rel_type_id,
            request.rel_properties,
        );
        self.commit_epoch += 1;
        Ok((source, relationship, target))
    }

    pub fn merge_connected_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: ConnectedNodesCreate,
    ) -> Result<(NodeId, RelId, NodeId, bool)> {
        let source_label_id = catalog.get_or_create_label(&request.source_label);
        let target_label_id = catalog.get_or_create_label(&request.target_label);
        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let source =
            self.find_node_by_label_and_properties(source_label_id, &request.source_properties);
        let target =
            self.find_node_by_label_and_properties(target_label_id, &request.target_properties);
        if let (Some(source), Some(target)) = (source, target) {
            if let Some(relationship) = self.find_relationship_by_properties(
                source,
                target,
                rel_type_id,
                &request.rel_properties,
            ) {
                return Ok((source, relationship, target, false));
            }
        }

        let source = source.unwrap_or(NodeId(self.next_node_id));
        let target = target.unwrap_or_else(|| {
            if source.0 == self.next_node_id {
                NodeId(self.next_node_id + 1)
            } else {
                NodeId(self.next_node_id)
            }
        });
        let relationship = RelId(self.next_rel_id);
        let ops = self.merge_connected_node_ops(&request, source, target, relationship);
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok((source, relationship, target, true))
    }

    pub fn commit_mutations(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
    ) -> Result<MutationSummary> {
        let mut next_node_id = self.next_node_id;
        let mut next_rel_id = self.next_rel_id;
        let mut working_catalog = catalog.clone();
        let mut ops = Vec::new();
        let mut rows = Vec::new();
        let mut pending_nodes = Vec::new();
        let mut pending_relationships = Vec::new();

        for mutation in mutations {
            match mutation {
                GraphMutation::CreateNodeLabel { label } => {
                    if let Some(id) = working_catalog.label_id(&label) {
                        rows.push(BTreeMap::from([
                            ("label_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateNodeLabel {
                            label: label.clone(),
                        });
                        let id = working_catalog.get_or_create_label(&label);
                        rows.push(BTreeMap::from([
                            ("label_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipType { rel_type } => {
                    if let Some(id) = working_catalog.rel_type_id(&rel_type) {
                        rows.push(BTreeMap::from([
                            ("rel_type_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateRelationshipType {
                            rel_type: rel_type.clone(),
                        });
                        let id = working_catalog.get_or_create_rel_type(&rel_type);
                        rows.push(BTreeMap::from([
                            ("rel_type_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateNodeTable { name } => {
                    if let Some(id) = working_catalog.table_id(TableKind::Node, &name) {
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateNodeTable { name: name.clone() });
                        working_catalog.get_or_create_label(&name);
                        let id = working_catalog.get_or_create_table(TableKind::Node, &name);
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipTable { name } => {
                    if let Some(id) = working_catalog.table_id(TableKind::Relationship, &name) {
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateRelationshipTable { name: name.clone() });
                        working_catalog.get_or_create_rel_type(&name);
                        let id =
                            working_catalog.get_or_create_table(TableKind::Relationship, &name);
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateProperty {
                    table_kind,
                    table,
                    property,
                    value_type,
                    nullable,
                } => {
                    let table_id =
                        ensure_table_descriptor(&mut working_catalog, table_kind, &table);
                    if let Some(id) = working_catalog.property_descriptor_id(table_id, &property) {
                        rows.push(BTreeMap::from([
                            ("property_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        validate_property_descriptor(
                            &working_catalog,
                            self,
                            table_id,
                            &property,
                            value_type,
                            nullable,
                        )?;
                        ops.push(WalOp::CreateProperty {
                            table_kind,
                            table: table.clone(),
                            property: property.clone(),
                            value_type,
                            nullable,
                        });
                        let id = working_catalog
                            .get_or_create_property(table_id, &property, value_type, nullable);
                        rows.push(BTreeMap::from([
                            ("property_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::AlterTableState {
                    table_kind,
                    table,
                    state,
                } => {
                    let Some(id) = working_catalog.table_id(table_kind, &table) else {
                        return Err(SkeinError::Storage(format!(
                            "schema table '{table}' does not exist"
                        )));
                    };
                    let Some(descriptor) = working_catalog.table_descriptor(id) else {
                        return Err(SkeinError::Storage(format!(
                            "schema table '{table}' does not exist"
                        )));
                    };
                    let changed = descriptor.state != state;
                    if changed {
                        ops.push(WalOp::AlterTableState {
                            table_kind,
                            table: table.clone(),
                            state,
                        });
                        working_catalog.set_table_state(id, state);
                    }
                    rows.push(BTreeMap::from([
                        ("table_id".to_string(), Value::Int(id.0 as i64)),
                        ("changed".to_string(), Value::Bool(changed)),
                    ]));
                }
                GraphMutation::AlterPropertyState {
                    table_kind,
                    table,
                    property,
                    state,
                } => {
                    let Some(table_id) = working_catalog.table_id(table_kind, &table) else {
                        return Err(SkeinError::Storage(format!(
                            "schema table '{table}' does not exist"
                        )));
                    };
                    let Some(id) = working_catalog.property_descriptor_id(table_id, &property)
                    else {
                        return Err(SkeinError::Storage(format!(
                            "schema property '{table}.{property}' does not exist"
                        )));
                    };
                    let Some(descriptor) = working_catalog.property_descriptor(id) else {
                        return Err(SkeinError::Storage(format!(
                            "schema property '{table}.{property}' does not exist"
                        )));
                    };
                    let changed = descriptor.state != state;
                    if changed {
                        if state == SchemaObjectState::Public {
                            validate_property_descriptor(
                                &working_catalog,
                                self,
                                table_id,
                                &property,
                                descriptor.value_type,
                                descriptor.nullable,
                            )?;
                        }
                        ops.push(WalOp::AlterPropertyState {
                            table_kind,
                            table: table.clone(),
                            property: property.clone(),
                            state,
                        });
                        working_catalog.set_property_state(id, state);
                    }
                    rows.push(BTreeMap::from([
                        ("property_id".to_string(), Value::Int(id.0 as i64)),
                        ("changed".to_string(), Value::Bool(changed)),
                    ]));
                }
                GraphMutation::CreateIndex { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.property_index_id(label_id, &property) {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateIndex {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog.get_or_create_property_index(label_id, &property);
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateCompositeIndex { label, properties } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) =
                        working_catalog.composite_property_index_id(label_id, &properties)
                    {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateCompositeIndex {
                            label: label.clone(),
                            properties: properties.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_composite_property_index(label_id, &properties);
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRangeIndex { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.property_index_id_with_kind(
                        label_id,
                        &property,
                        IndexKind::Range,
                    ) {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateRangeIndex {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog.get_or_create_property_index_with_kind(
                            label_id,
                            &property,
                            IndexKind::Range,
                        );
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateFullTextIndex { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.property_index_id_with_kind(
                        label_id,
                        &property,
                        IndexKind::FullText,
                    ) {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateFullTextIndex {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog.get_or_create_property_index_with_kind(
                            label_id,
                            &property,
                            IndexKind::FullText,
                        );
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateUniqueConstraint { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.unique_constraint_id(label_id, &property) {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_unique_constraint(&working_catalog, label_id, &property)?;
                        ops.push(WalOp::CreateUniqueConstraint {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id =
                            working_catalog.get_or_create_unique_constraint(label_id, &property);
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateNodePropertyExistsConstraint { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) =
                        working_catalog.node_property_exists_constraint_id(label_id, &property)
                    {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_node_property_exists_constraint(
                            &working_catalog,
                            label_id,
                            &property,
                        )?;
                        ops.push(WalOp::CreateNodePropertyExistsConstraint {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_node_property_exists_constraint(label_id, &property);
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipUniqueConstraint { rel_type, property } => {
                    let rel_type_id = working_catalog.get_or_create_rel_type(&rel_type);
                    if let Some(id) =
                        working_catalog.relationship_unique_constraint_id(rel_type_id, &property)
                    {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_relationship_unique_constraint(
                            &working_catalog,
                            rel_type_id,
                            &property,
                        )?;
                        ops.push(WalOp::CreateRelationshipUniqueConstraint {
                            rel_type: rel_type.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_relationship_unique_constraint(rel_type_id, &property);
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipPropertyExistsConstraint {
                    rel_type,
                    property,
                } => {
                    let rel_type_id = working_catalog.get_or_create_rel_type(&rel_type);
                    if let Some(id) = working_catalog
                        .relationship_property_exists_constraint_id(rel_type_id, &property)
                    {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_relationship_property_exists_constraint(
                            &working_catalog,
                            rel_type_id,
                            &property,
                        )?;
                        ops.push(WalOp::CreateRelationshipPropertyExistsConstraint {
                            rel_type: rel_type.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_relationship_property_exists_constraint(
                                rel_type_id,
                                &property,
                            );
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateNode { label, properties } => {
                    let id = NodeId(next_node_id);
                    next_node_id += 1;
                    let label_id = working_catalog.get_or_create_label(&label);
                    ops.push(WalOp::CreateNode {
                        id,
                        label,
                        properties: properties.clone(),
                    });
                    pending_nodes.push((id, label_id, properties));
                    rows.push(BTreeMap::from([(
                        "node_id".to_string(),
                        Value::Int(id.0 as i64),
                    )]));
                }
                GraphMutation::MergeNode {
                    label,
                    match_properties,
                    on_create_properties,
                    on_match_assignments,
                    post_merge_assignments,
                } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    let current =
                        self.find_node_by_label_and_properties(label_id, &match_properties);
                    let pending = pending_nodes
                        .iter()
                        .find(|(_, pending_label, pending_properties)| {
                            *pending_label == label_id
                                && properties_contain_all(pending_properties, &match_properties)
                        })
                        .map(|(id, _, _)| *id);
                    if let Some(id) = current {
                        if !on_match_assignments.is_empty() || !post_merge_assignments.is_empty() {
                            let mut assignments = Vec::with_capacity(
                                on_match_assignments.len() + post_merge_assignments.len(),
                            );
                            assignments.extend(on_match_assignments);
                            assignments.extend(post_merge_assignments);
                            let set_ops = self.node_set_property_ops(&[id], &assignments)?;
                            ops.extend(set_ops);
                        }
                        rows.push(BTreeMap::from([
                            ("node_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else if let Some(id) = pending {
                        if !on_match_assignments.is_empty() || !post_merge_assignments.is_empty() {
                            let mut assignments = Vec::with_capacity(
                                on_match_assignments.len() + post_merge_assignments.len(),
                            );
                            assignments.extend(on_match_assignments);
                            assignments.extend(post_merge_assignments);
                            Self::apply_pending_node_assignments(
                                &mut ops,
                                &mut pending_nodes,
                                id,
                                &assignments,
                            )?;
                        }
                        rows.push(BTreeMap::from([
                            ("node_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        let mut properties = match_properties;
                        for (property, value) in on_create_properties {
                            properties.insert(property, value);
                        }
                        apply_node_assignments_to_properties(
                            &mut properties,
                            &post_merge_assignments,
                        )?;
                        let id = NodeId(next_node_id);
                        next_node_id += 1;
                        ops.push(WalOp::CreateNode {
                            id,
                            label,
                            properties: properties.clone(),
                        });
                        pending_nodes.push((id, label_id, properties));
                        rows.push(BTreeMap::from([
                            ("node_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::MergeConnectedNodes(request) => {
                    let source_label_id =
                        working_catalog.get_or_create_label(&request.source_label);
                    let target_label_id =
                        working_catalog.get_or_create_label(&request.target_label);
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let current_source = self.find_node_by_label_and_properties(
                        source_label_id,
                        &request.source_properties,
                    );
                    let current_target = self.find_node_by_label_and_properties(
                        target_label_id,
                        &request.target_properties,
                    );
                    let pending_source = pending_nodes
                        .iter()
                        .find(|(_, pending_label, pending_properties)| {
                            *pending_label == source_label_id
                                && pending_properties == &request.source_properties
                        })
                        .map(|(id, _, _)| *id);
                    let pending_target = pending_nodes
                        .iter()
                        .find(|(_, pending_label, pending_properties)| {
                            *pending_label == target_label_id
                                && pending_properties == &request.target_properties
                        })
                        .map(|(id, _, _)| *id);
                    let source = current_source.or(pending_source);
                    let target = current_target.or(pending_target);
                    if let (Some(source), Some(target)) = (source, target) {
                        let current_relationship = self.find_relationship_by_properties(
                            source,
                            target,
                            rel_type_id,
                            &request.rel_properties,
                        );
                        let pending_relationship = pending_relationships
                            .iter()
                            .find(
                                |(_, pending_source, pending_target, pending_type, properties)| {
                                    *pending_source == source
                                        && *pending_target == target
                                        && *pending_type == rel_type_id
                                        && properties == &request.rel_properties
                                },
                            )
                            .map(|(id, _, _, _, _)| *id);
                        if let Some(relationship) = current_relationship.or(pending_relationship) {
                            rows.push(merge_relationship_row(source, relationship, target, false));
                            continue;
                        }
                    }

                    let source_created = source.is_none();
                    let source = source.unwrap_or(NodeId(next_node_id));
                    if source.0 == next_node_id {
                        next_node_id += 1;
                        pending_nodes.push((
                            source,
                            source_label_id,
                            request.source_properties.clone(),
                        ));
                    }
                    let target_created = target.is_none();
                    let target = target.unwrap_or(NodeId(next_node_id));
                    if target.0 == next_node_id {
                        next_node_id += 1;
                        pending_nodes.push((
                            target,
                            target_label_id,
                            request.target_properties.clone(),
                        ));
                    }
                    let relationship = RelId(next_rel_id);
                    next_rel_id += 1;
                    if source_created {
                        ops.push(WalOp::CreateNode {
                            id: source,
                            label: request.source_label.clone(),
                            properties: request.source_properties.clone(),
                        });
                    }
                    if target_created {
                        ops.push(WalOp::CreateNode {
                            id: target,
                            label: request.target_label.clone(),
                            properties: request.target_properties.clone(),
                        });
                    }
                    ops.push(WalOp::CreateRelationship {
                        id: relationship,
                        source,
                        target,
                        rel_type: request.rel_type.clone(),
                        properties: request.rel_properties.clone(),
                    });
                    pending_relationships.push((
                        relationship,
                        source,
                        target,
                        rel_type_id,
                        request.rel_properties.clone(),
                    ));
                    rows.push(merge_relationship_row(source, relationship, target, true));
                }
                GraphMutation::SetNodeProperty {
                    label,
                    filter,
                    property,
                    value,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        for id in self.matching_node_ids(label_id, filter.as_ref()) {
                            ops.push(WalOp::SetNodeProperty {
                                id,
                                property: property.clone(),
                                value: value.clone(),
                            });
                            rows.push(BTreeMap::from([(
                                "node_id".to_string(),
                                Value::Int(id.0 as i64),
                            )]));
                        }
                    }
                }
                GraphMutation::SetNodePropertyAddInt {
                    label,
                    filter,
                    property,
                    amount,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        for id in self.matching_node_ids(label_id, filter.as_ref()) {
                            let current = self
                                .nodes
                                .get(&id)
                                .and_then(|node| node.properties.get(&property));
                            let current = match current {
                                None | Some(Value::Null) => 0,
                                Some(Value::Int(value)) => *value,
                                Some(value) => {
                                    return Err(SkeinError::Execution(format!(
                                        "property increment requires an integer or null value, got {value:?}"
                                    )));
                                }
                            };
                            let value = current.checked_add(amount).ok_or_else(|| {
                                SkeinError::Execution(
                                    "property increment overflowed i64".to_string(),
                                )
                            })?;
                            ops.push(WalOp::SetNodeProperty {
                                id,
                                property: property.clone(),
                                value: Value::Int(value),
                            });
                            rows.push(BTreeMap::from([(
                                "node_id".to_string(),
                                Value::Int(id.0 as i64),
                            )]));
                        }
                    }
                }
                GraphMutation::SetNodeProperties {
                    label,
                    filter,
                    assignments,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        let ids = self
                            .matching_node_ids(label_id, filter.as_ref())
                            .collect::<Vec<_>>();
                        ops.extend(self.node_set_property_ops(&ids, &assignments)?);
                        rows.extend(ids.into_iter().map(|id| {
                            BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))])
                        }));
                    }
                }
                GraphMutation::SetRelationshipProperty {
                    source_label,
                    filter,
                    rel_type,
                    target_label,
                    target_filter,
                    rel_filter,
                    property,
                    value,
                } => {
                    let assignments = vec![RelationshipSetAssignment { property, value }];
                    apply_set_relationship_properties_mutation(
                        self,
                        &working_catalog,
                        &mut ops,
                        &mut rows,
                        RelationshipPropertiesUpdate {
                            source_label,
                            filter,
                            rel_type,
                            target_label,
                            target_filter,
                            rel_filter,
                            assignments,
                        },
                    );
                }
                GraphMutation::SetRelationshipProperties {
                    source_label,
                    filter,
                    rel_type,
                    target_label,
                    target_filter,
                    rel_filter,
                    assignments,
                } => {
                    apply_set_relationship_properties_mutation(
                        self,
                        &working_catalog,
                        &mut ops,
                        &mut rows,
                        RelationshipPropertiesUpdate {
                            source_label,
                            filter,
                            rel_type,
                            target_label,
                            target_filter,
                            rel_filter,
                            assignments,
                        },
                    );
                }
                GraphMutation::DeleteNode {
                    label,
                    filter,
                    detach,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        let ids = self
                            .matching_node_ids(label_id, filter.as_ref())
                            .collect::<Vec<_>>();
                        let delete_ops = self.delete_node_ops(&ids, detach)?;
                        for id in ids {
                            rows.push(BTreeMap::from([(
                                "node_id".to_string(),
                                Value::Int(id.0 as i64),
                            )]));
                        }
                        ops.extend(delete_ops);
                    }
                }
                GraphMutation::DeleteRelationship {
                    source_label,
                    filter,
                    rel_type,
                    target_label,
                    target_filter,
                    rel_filter,
                } => {
                    if let (Some(source_label_id), Some(target_label_id), Some(rel_type_id)) = (
                        working_catalog.label_id(&source_label),
                        working_catalog.label_id(&target_label),
                        working_catalog.rel_type_id(&rel_type),
                    ) {
                        let source_ids = self
                            .matching_node_ids(Some(source_label_id), filter.as_ref())
                            .collect::<BTreeSet<_>>();
                        let target_ids = target_filter.as_ref().map(|filter| {
                            self.matching_node_ids(Some(target_label_id), Some(filter))
                                .collect::<BTreeSet<_>>()
                        });
                        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
                            continue;
                        }
                        for relationship in self.relationships.values() {
                            if relationship.rel_type != rel_type_id
                                || !source_ids.contains(&relationship.source)
                            {
                                continue;
                            }
                            if rel_filter
                                .as_ref()
                                .map(|filter| {
                                    !property_filter_matches(
                                        filter,
                                        relationship.id.0,
                                        &relationship.properties,
                                    )
                                })
                                .unwrap_or(false)
                            {
                                continue;
                            }
                            let target_matches = self
                                .nodes
                                .get(&relationship.target)
                                .map(|target| {
                                    target.labels.contains(&target_label_id)
                                        && target_ids
                                            .as_ref()
                                            .map(|ids| ids.contains(&relationship.target))
                                            .unwrap_or(true)
                                })
                                .unwrap_or(false);
                            if target_matches {
                                ops.push(WalOp::DeleteRelationship {
                                    id: relationship.id,
                                });
                                rows.push(BTreeMap::from([(
                                    "rel_id".to_string(),
                                    Value::Int(relationship.id.0 as i64),
                                )]));
                            }
                        }
                    }
                }
                GraphMutation::DeleteRelationshipTargetNodes(request) => {
                    let ids = self.relationship_target_node_ids(&working_catalog, &request);
                    let delete_ops = self.delete_node_ops(&ids, request.detach)?;
                    ops.extend(delete_ops);
                    rows.extend(ids.into_iter().map(|id| {
                        BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))])
                    }));
                }
                GraphMutation::CreateRelationshipsBetweenMatches(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    let target_label_id =
                        optional_label_id(&working_catalog, &request.target_label);
                    if (!request.source_label.is_empty() && source_label_id.is_none())
                        || (!request.target_label.is_empty() && target_label_id.is_none())
                    {
                        continue;
                    }
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let sources = self
                        .matching_node_ids(source_label_id, request.source_filter.as_ref())
                        .collect::<Vec<_>>();
                    let targets = self
                        .matching_node_ids(target_label_id, request.target_filter.as_ref())
                        .collect::<Vec<_>>();
                    for source in sources {
                        for target in &targets {
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.rel_type.clone(),
                                properties: request.rel_properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                rel_type_id,
                                request.rel_properties.clone(),
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsBetweenMatches(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    let target_label_id =
                        optional_label_id(&working_catalog, &request.target_label);
                    if (!request.source_label.is_empty() && source_label_id.is_none())
                        || (!request.target_label.is_empty() && target_label_id.is_none())
                    {
                        continue;
                    }
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let sources = self
                        .matching_node_ids(source_label_id, request.source_filter.as_ref())
                        .collect::<Vec<_>>();
                    let targets = self
                        .matching_node_ids(target_label_id, request.target_filter.as_ref())
                        .collect::<Vec<_>>();
                    for source in sources {
                        for target in &targets {
                            let current = self.find_relationship_by_property_subset(
                                source,
                                *target,
                                rel_type_id,
                                &request.rel_match_properties,
                            );
                            let pending = pending_relationships
                                .iter()
                                .find(
                                    |(
                                        _,
                                        pending_source,
                                        pending_target,
                                        pending_type,
                                        properties,
                                    )| {
                                        *pending_source == source
                                            && *pending_target == *target
                                            && *pending_type == rel_type_id
                                            && properties_contain_all(
                                                properties,
                                                &request.rel_match_properties,
                                            )
                                    },
                                )
                                .map(|(id, _, _, _, _)| *id);
                            if let Some(relationship) = current.or(pending) {
                                rows.push(BTreeMap::from([
                                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                    ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                    ("created".to_string(), Value::Bool(false)),
                                ]));
                                continue;
                            }
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            let mut properties = request.rel_match_properties.clone();
                            for (property, value) in &request.on_create_properties {
                                properties.insert(property.clone(), value.clone());
                            }
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.rel_type.clone(),
                                properties: properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                rel_type_id,
                                properties,
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(true)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsToMatchedTarget(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    if !request.source_label.is_empty() && source_label_id.is_none() {
                        continue;
                    }
                    let Some(old_target_label_id) =
                        working_catalog.label_id(&request.old_target_label)
                    else {
                        continue;
                    };
                    let Some(new_target_label_id) =
                        working_catalog.label_id(&request.new_target_label)
                    else {
                        continue;
                    };
                    let Some(old_rel_type_id) = working_catalog.rel_type_id(&request.old_rel_type)
                    else {
                        continue;
                    };
                    let new_rel_type_id =
                        working_catalog.get_or_create_rel_type(&request.new_rel_type);
                    let source_ids = self
                        .scan_relationships(Some(old_rel_type_id))
                        .filter(|relationship| {
                            properties_contain_all(
                                &relationship.properties,
                                &request.old_rel_filter,
                            ) && self
                                .nodes
                                .get(&relationship.source)
                                .map(|node| {
                                    source_label_id
                                        .map(|label_id| node.labels.contains(&label_id))
                                        .unwrap_or(true)
                                        && request
                                            .source_filter
                                            .as_ref()
                                            .map(|filter| {
                                                property_filter_matches(
                                                    filter,
                                                    node.id.0,
                                                    &node.properties,
                                                )
                                            })
                                            .unwrap_or(true)
                                })
                                .unwrap_or(false)
                                && self
                                    .nodes
                                    .get(&relationship.target)
                                    .map(|node| {
                                        node.labels.contains(&old_target_label_id)
                                            && request
                                                .old_target_filter
                                                .as_ref()
                                                .map(|filter| {
                                                    property_filter_matches(
                                                        filter,
                                                        node.id.0,
                                                        &node.properties,
                                                    )
                                                })
                                                .unwrap_or(true)
                                    })
                                    .unwrap_or(false)
                        })
                        .map(|relationship| relationship.source)
                        .collect::<BTreeSet<_>>();
                    let target_ids = self
                        .matching_node_ids(
                            Some(new_target_label_id),
                            request.new_target_filter.as_ref(),
                        )
                        .collect::<Vec<_>>();
                    for source in source_ids {
                        for target in &target_ids {
                            let current = self.find_relationship_by_property_subset(
                                source,
                                *target,
                                new_rel_type_id,
                                &request.new_rel_match_properties,
                            );
                            let pending = pending_relationships
                                .iter()
                                .find(
                                    |(
                                        _,
                                        pending_source,
                                        pending_target,
                                        pending_type,
                                        properties,
                                    )| {
                                        *pending_source == source
                                            && *pending_target == *target
                                            && *pending_type == new_rel_type_id
                                            && properties_contain_all(
                                                properties,
                                                &request.new_rel_match_properties,
                                            )
                                    },
                                )
                                .map(|(id, _, _, _, _)| *id);
                            if let Some(relationship) = current.or(pending) {
                                rows.push(BTreeMap::from([
                                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                    ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                    ("created".to_string(), Value::Bool(false)),
                                ]));
                                continue;
                            }
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            let mut properties = request.new_rel_match_properties.clone();
                            properties.extend(request.on_create_properties.clone());
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.new_rel_type.clone(),
                                properties: properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                new_rel_type_id,
                                properties,
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(true)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsFromMatchedTarget(request) => {
                    let old_source_label_id =
                        optional_label_id(&working_catalog, &request.old_source_label);
                    if !request.old_source_label.is_empty() && old_source_label_id.is_none() {
                        continue;
                    }
                    let Some(old_target_label_id) =
                        working_catalog.label_id(&request.old_target_label)
                    else {
                        continue;
                    };
                    let new_source_label_id =
                        optional_label_id(&working_catalog, &request.new_source_label);
                    if !request.new_source_label.is_empty() && new_source_label_id.is_none() {
                        continue;
                    }
                    let Some(old_rel_type_id) = working_catalog.rel_type_id(&request.old_rel_type)
                    else {
                        continue;
                    };
                    let new_rel_type_id =
                        working_catalog.get_or_create_rel_type(&request.new_rel_type);
                    let target_ids = self
                        .scan_relationships(Some(old_rel_type_id))
                        .filter(|relationship| {
                            properties_contain_all(
                                &relationship.properties,
                                &request.old_rel_filter,
                            ) && self
                                .nodes
                                .get(&relationship.source)
                                .map(|node| {
                                    old_source_label_id
                                        .map(|label_id| node.labels.contains(&label_id))
                                        .unwrap_or(true)
                                        && request
                                            .old_source_filter
                                            .as_ref()
                                            .map(|filter| {
                                                property_filter_matches(
                                                    filter,
                                                    node.id.0,
                                                    &node.properties,
                                                )
                                            })
                                            .unwrap_or(true)
                                })
                                .unwrap_or(false)
                                && self
                                    .nodes
                                    .get(&relationship.target)
                                    .map(|node| {
                                        node.labels.contains(&old_target_label_id)
                                            && request
                                                .old_target_filter
                                                .as_ref()
                                                .map(|filter| {
                                                    property_filter_matches(
                                                        filter,
                                                        node.id.0,
                                                        &node.properties,
                                                    )
                                                })
                                                .unwrap_or(true)
                                    })
                                    .unwrap_or(false)
                        })
                        .map(|relationship| relationship.target)
                        .collect::<BTreeSet<_>>();
                    let source_ids = self
                        .matching_node_ids(new_source_label_id, request.new_source_filter.as_ref())
                        .collect::<Vec<_>>();
                    for source in source_ids {
                        for target in &target_ids {
                            let current = self.find_relationship_by_property_subset(
                                source,
                                *target,
                                new_rel_type_id,
                                &request.new_rel_match_properties,
                            );
                            let pending = pending_relationships
                                .iter()
                                .find(
                                    |(
                                        _,
                                        pending_source,
                                        pending_target,
                                        pending_type,
                                        properties,
                                    )| {
                                        *pending_source == source
                                            && *pending_target == *target
                                            && *pending_type == new_rel_type_id
                                            && properties_contain_all(
                                                properties,
                                                &request.new_rel_match_properties,
                                            )
                                    },
                                )
                                .map(|(id, _, _, _, _)| *id);
                            if let Some(relationship) = current.or(pending) {
                                rows.push(BTreeMap::from([
                                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                    ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                    ("created".to_string(), Value::Bool(false)),
                                ]));
                                continue;
                            }
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            let mut properties = request.new_rel_match_properties.clone();
                            properties.extend(request.on_create_properties.clone());
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.new_rel_type.clone(),
                                properties: properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                new_rel_type_id,
                                properties,
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(true)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsFromMatchedRelationships(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    let target_label_id =
                        optional_label_id(&working_catalog, &request.target_label);
                    if (!request.source_label.is_empty() && source_label_id.is_none())
                        || (!request.target_label.is_empty() && target_label_id.is_none())
                    {
                        continue;
                    }
                    let Some(old_rel_type_id) = working_catalog.rel_type_id(&request.old_rel_type)
                    else {
                        continue;
                    };
                    let new_rel_type_id =
                        working_catalog.get_or_create_rel_type(&request.new_rel_type);
                    let old_relationships = self
                        .scan_relationships(Some(old_rel_type_id))
                        .filter(|relationship| {
                            properties_contain_all(
                                &relationship.properties,
                                &request.old_rel_filter,
                            ) && self
                                .nodes
                                .get(&relationship.source)
                                .map(|node| {
                                    source_label_id
                                        .map(|label_id| node.labels.contains(&label_id))
                                        .unwrap_or(true)
                                        && request
                                            .source_filter
                                            .as_ref()
                                            .map(|filter| {
                                                property_filter_matches(
                                                    filter,
                                                    node.id.0,
                                                    &node.properties,
                                                )
                                            })
                                            .unwrap_or(true)
                                })
                                .unwrap_or(false)
                                && self
                                    .nodes
                                    .get(&relationship.target)
                                    .map(|node| {
                                        target_label_id
                                            .map(|label_id| node.labels.contains(&label_id))
                                            .unwrap_or(true)
                                            && request
                                                .target_filter
                                                .as_ref()
                                                .map(|filter| {
                                                    property_filter_matches(
                                                        filter,
                                                        node.id.0,
                                                        &node.properties,
                                                    )
                                                })
                                                .unwrap_or(true)
                                    })
                                    .unwrap_or(false)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    for old_relationship in old_relationships {
                        let current = self.find_relationship_by_property_subset(
                            old_relationship.source,
                            old_relationship.target,
                            new_rel_type_id,
                            &request.new_rel_match_properties,
                        );
                        let pending = pending_relationships
                            .iter()
                            .find(
                                |(_, pending_source, pending_target, pending_type, properties)| {
                                    *pending_source == old_relationship.source
                                        && *pending_target == old_relationship.target
                                        && *pending_type == new_rel_type_id
                                        && properties_contain_all(
                                            properties,
                                            &request.new_rel_match_properties,
                                        )
                                },
                            )
                            .map(|(id, _, _, _, _)| *id);
                        if let Some(relationship) = current.or(pending) {
                            rows.push(BTreeMap::from([
                                (
                                    "source_node_id".to_string(),
                                    Value::Int(old_relationship.source.0 as i64),
                                ),
                                (
                                    "target_node_id".to_string(),
                                    Value::Int(old_relationship.target.0 as i64),
                                ),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(false)),
                            ]));
                            continue;
                        }
                        let relationship = RelId(next_rel_id);
                        next_rel_id += 1;
                        let mut properties = request.new_rel_match_properties.clone();
                        for (property, value) in &request.on_create_properties {
                            let value = match value {
                                RelationshipOnCreatePropertyValue::Value(value) => value.clone(),
                                RelationshipOnCreatePropertyValue::MatchedRelationshipProperty {
                                    property,
                                } => old_relationship
                                    .properties
                                    .get(property)
                                    .cloned()
                                    .unwrap_or(Value::Null),
                            };
                            properties.insert(property.clone(), value);
                        }
                        ops.push(WalOp::CreateRelationship {
                            id: relationship,
                            source: old_relationship.source,
                            target: old_relationship.target,
                            rel_type: request.new_rel_type.clone(),
                            properties: properties.clone(),
                        });
                        pending_relationships.push((
                            relationship,
                            old_relationship.source,
                            old_relationship.target,
                            new_rel_type_id,
                            properties,
                        ));
                        rows.push(BTreeMap::from([
                            (
                                "source_node_id".to_string(),
                                Value::Int(old_relationship.source.0 as i64),
                            ),
                            (
                                "target_node_id".to_string(),
                                Value::Int(old_relationship.target.0 as i64),
                            ),
                            ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateConnectedNodes(request) => {
                    let source_label_id =
                        working_catalog.get_or_create_label(&request.source_label);
                    let target_label_id =
                        working_catalog.get_or_create_label(&request.target_label);
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let source_properties = request.source_properties.clone();
                    let target_properties = request.target_properties.clone();
                    let rel_properties = request.rel_properties.clone();
                    let source = NodeId(next_node_id);
                    let target = NodeId(next_node_id + 1);
                    let relationship = RelId(next_rel_id);
                    next_node_id += 2;
                    next_rel_id += 1;
                    ops.push(WalOp::CreateNode {
                        id: source,
                        label: request.source_label,
                        properties: request.source_properties,
                    });
                    ops.push(WalOp::CreateNode {
                        id: target,
                        label: request.target_label,
                        properties: request.target_properties,
                    });
                    ops.push(WalOp::CreateRelationship {
                        id: relationship,
                        source,
                        target,
                        rel_type: request.rel_type,
                        properties: request.rel_properties,
                    });
                    pending_relationships.push((
                        relationship,
                        source,
                        target,
                        rel_type_id,
                        rel_properties,
                    ));
                    pending_nodes.push((source, source_label_id, source_properties));
                    pending_nodes.push((target, target_label_id, target_properties));
                    rows.push(BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                    ]));
                }
            }
        }

        if ops.is_empty() {
            return Ok(MutationSummary { rows });
        }
        self.validate_constraints_for_ops(&working_catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        *catalog = working_catalog;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(MutationSummary { rows })
    }

    fn apply_pending_node_assignments(
        ops: &mut [WalOp],
        pending_nodes: &mut [(NodeId, LabelId, BTreeMap<String, Value>)],
        id: NodeId,
        assignments: &[NodeSetAssignment],
    ) -> Result<()> {
        let Some((_, _, properties)) = pending_nodes
            .iter_mut()
            .find(|(pending_id, _, _)| *pending_id == id)
        else {
            return Ok(());
        };
        for assignment in assignments {
            let value = evaluate_node_set_value(properties, assignment)?;
            properties.insert(assignment.property.clone(), value.clone());
            for op in ops.iter_mut() {
                if let WalOp::CreateNode {
                    id: create_id,
                    properties,
                    ..
                } = op
                {
                    if *create_id == id {
                        properties.insert(assignment.property.clone(), value.clone());
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn checkpoint(&mut self, catalog: &Catalog) -> Result<()> {
        self.checkpoint_with_reader_epoch(catalog, None)
    }

    pub fn checkpoint_with_reader_epoch(
        &mut self,
        catalog: &Catalog,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> Result<()> {
        if self.durable.is_none() {
            return Ok(());
        }
        let projection_epoch = self.next_projection_epoch();
        let projected_graph_artifacts =
            encode_projected_graph_artifacts(catalog, self, projection_epoch);
        let (_, artifacts) = decode_projected_graph_artifacts(&projected_graph_artifacts)?;
        let durable = self.durable.as_mut().expect("durable store must exist");
        durable.write_projected_graph_artifacts(&projected_graph_artifacts)?;
        durable.write_checkpoint(CheckpointImage {
            catalog,
            commit_epoch: self.commit_epoch,
            next_node_id: self.next_node_id,
            next_rel_id: self.next_rel_id,
            nodes: &self.nodes,
            relationships: &self.relationships,
            projected_graphs: &self.projected_graphs,
        })?;
        durable.truncate_wal()?;
        durable.publish_checkpoint_manifest(self.commit_epoch, oldest_reader_commit_epoch)?;
        self.projected_graph_artifacts = artifacts;
        Ok(())
    }

    pub fn rebuild_projected_graph_artifacts(&mut self, catalog: &Catalog) -> Result<()> {
        let projection_epoch = self.next_projection_epoch();
        let projected_graph_artifacts =
            encode_projected_graph_artifacts(catalog, self, projection_epoch);
        let (_, artifacts) = decode_projected_graph_artifacts(&projected_graph_artifacts)?;
        if let Some(durable) = &self.durable {
            durable.write_projected_graph_artifacts(&projected_graph_artifacts)?;
        }
        self.projected_graph_artifacts = artifacts;
        Ok(())
    }

    pub fn storage_version(&self) -> &'static str {
        STORAGE_VERSION
    }

    pub fn commit_epoch(&self) -> u64 {
        self.commit_epoch
    }

    pub fn search_projection_change_log_start_epoch(&self) -> u64 {
        self.search_projection_change_log_start_epoch
    }

    pub fn search_projection_graph_changes_after(
        &self,
        commit_epoch: u64,
    ) -> Vec<SearchProjectionGraphChange> {
        self.search_projection_graph_changes
            .iter()
            .filter(|change| change.commit_epoch > commit_epoch)
            .cloned()
            .collect()
    }

    pub fn set_max_search_projection_change_log_entries(&mut self, max_entries: Option<usize>) {
        self.max_search_projection_change_log_entries = max_entries;
        self.trim_search_projection_graph_change_log();
    }

    pub fn stable_id_mapping(&self) -> StoreStableIdMapping {
        self.stable_id_mapping.clone()
    }

    pub fn ensure_stable_id_mapping(&mut self) -> Result<StoreStableIdMapping> {
        if self
            .durable
            .as_ref()
            .is_some_and(|durable| durable.read_only)
        {
            return Err(SkeinError::Storage(
                "stable id mapping persistence is not allowed in read-only mode".to_string(),
            ));
        }
        let mut changed = false;
        for node in self.nodes.values() {
            if node.properties.contains_key("id")
                || self
                    .stable_id_mapping
                    .node_stable_ids
                    .contains_key(&node.id)
            {
                continue;
            }
            self.stable_id_mapping
                .node_stable_ids
                .insert(node.id, generated_stable_id("node", node.id.0));
            changed = true;
        }
        for relationship in self.relationships.values() {
            if relationship.properties.contains_key("id")
                || self
                    .stable_id_mapping
                    .relationship_stable_ids
                    .contains_key(&relationship.id)
            {
                continue;
            }
            self.stable_id_mapping.relationship_stable_ids.insert(
                relationship.id,
                generated_stable_id("relationship", relationship.id.0),
            );
            changed = true;
        }
        if changed {
            self.write_stable_id_mapping()?;
        }
        Ok(self.stable_id_mapping())
    }

    pub fn storage_reclamation_watermark(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> StorageReclamationWatermark {
        match &self.durable {
            Some(durable) => {
                let safe_reclaim_commit_epoch =
                    if oldest_reader_commit_epoch == durable.oldest_reader_commit_epoch {
                        durable.safe_reclaim_commit_epoch
                    } else {
                        safe_reclaim_commit_epoch(
                            durable.checkpoint_commit_epoch,
                            oldest_reader_commit_epoch,
                        )
                    };
                StorageReclamationWatermark {
                    current_commit_epoch: self.commit_epoch,
                    checkpoint_epoch: Some(durable.checkpoint_epoch),
                    checkpoint_commit_epoch: Some(durable.checkpoint_commit_epoch),
                    oldest_reader_commit_epoch,
                    safe_reclaim_commit_epoch,
                    durable: true,
                }
            }
            None => StorageReclamationWatermark {
                current_commit_epoch: self.commit_epoch,
                checkpoint_epoch: None,
                checkpoint_commit_epoch: None,
                oldest_reader_commit_epoch,
                safe_reclaim_commit_epoch: safe_reclaim_commit_epoch(
                    self.commit_epoch,
                    oldest_reader_commit_epoch,
                ),
                durable: false,
            },
        }
    }

    pub fn storage_recovery_report(&self) -> StorageRecoveryReport {
        self.storage_recovery_report.clone()
    }

    pub fn statistics(&self) -> GraphStatistics {
        compute_statistics_with_basic(&self.nodes, &self.relationships, self.basic_statistics())
    }

    pub fn basic_statistics(&self) -> BasicGraphStatistics {
        let mut statistics = self.basic_statistics.clone();
        statistics.computed_at_commit_epoch = self.commit_epoch;
        statistics
    }

    pub fn snapshot(&self) -> Self {
        Self {
            next_node_id: self.next_node_id,
            next_rel_id: self.next_rel_id,
            commit_epoch: self.commit_epoch,
            nodes: self.nodes.clone(),
            relationships: self.relationships.clone(),
            basic_statistics: self.basic_statistics.clone(),
            outgoing: self.outgoing.clone(),
            incoming: self.incoming.clone(),
            property_index: self.property_index.clone(),
            composite_property_index: self.composite_property_index.clone(),
            full_text_property_index: self.full_text_property_index.clone(),
            projected_graphs: self.projected_graphs.clone(),
            projected_graph_artifacts: self.projected_graph_artifacts.clone(),
            stable_id_mapping: self.stable_id_mapping.clone(),
            search_projection_change_log_start_epoch: self.search_projection_change_log_start_epoch,
            search_projection_graph_changes: self.search_projection_graph_changes.clone(),
            max_search_projection_change_log_entries: self.max_search_projection_change_log_entries,
            storage_recovery_report: self.storage_recovery_report.clone(),
            durable: None,
        }
    }

    fn record_search_projection_graph_changes_for_ops(
        &mut self,
        catalog: &Catalog,
        commit_epoch: u64,
        ops: &[WalOp],
    ) {
        let mut upsert_node_ids = BTreeSet::new();
        let mut delete_document_ids = BTreeSet::new();
        self.collect_search_projection_graph_changes_for_ops(
            catalog,
            ops,
            &mut upsert_node_ids,
            &mut delete_document_ids,
        );
        self.search_projection_graph_changes
            .push(SearchProjectionGraphChange {
                commit_epoch,
                upsert_node_ids: upsert_node_ids.into_iter().map(|id| id.0).collect(),
                delete_document_ids: delete_document_ids.into_iter().collect(),
            });
        self.trim_search_projection_graph_change_log();
    }

    fn trim_search_projection_graph_change_log(&mut self) {
        let Some(max_entries) = self.max_search_projection_change_log_entries else {
            return;
        };
        if self.search_projection_graph_changes.len() <= max_entries {
            return;
        }
        let remove_count = self.search_projection_graph_changes.len() - max_entries;
        if remove_count > 0 {
            if let Some(last_removed) = self
                .search_projection_graph_changes
                .get(remove_count.saturating_sub(1))
            {
                self.search_projection_change_log_start_epoch = self
                    .search_projection_change_log_start_epoch
                    .max(last_removed.commit_epoch);
            }
            self.search_projection_graph_changes.drain(0..remove_count);
        }
    }

    fn refresh_basic_statistics_epoch(&mut self) {
        self.basic_statistics.computed_at_commit_epoch = self.commit_epoch;
    }

    fn add_node_to_basic_statistics(&mut self, node: &NodeRecord) {
        self.basic_statistics.node_count += 1;
        for label_id in &node.labels {
            *self
                .basic_statistics
                .label_counts
                .entry(*label_id)
                .or_default() += 1;
        }
        self.refresh_basic_statistics_epoch();
    }

    fn remove_node_from_basic_statistics(&mut self, node: &NodeRecord) {
        self.basic_statistics.node_count = self.basic_statistics.node_count.saturating_sub(1);
        for label_id in &node.labels {
            decrement_counter(&mut self.basic_statistics.label_counts, label_id);
        }
        self.refresh_basic_statistics_epoch();
    }

    fn add_relationship_to_basic_statistics(&mut self, relationship: &RelRecord) {
        self.basic_statistics.relationship_count += 1;
        *self
            .basic_statistics
            .rel_type_counts
            .entry(relationship.rel_type)
            .or_default() += 1;
        self.refresh_basic_statistics_epoch();
    }

    fn remove_relationship_from_basic_statistics(&mut self, relationship: &RelRecord) {
        self.basic_statistics.relationship_count =
            self.basic_statistics.relationship_count.saturating_sub(1);
        decrement_counter(
            &mut self.basic_statistics.rel_type_counts,
            &relationship.rel_type,
        );
        self.refresh_basic_statistics_epoch();
    }

    fn collect_search_projection_graph_changes_for_ops(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
        upsert_node_ids: &mut BTreeSet<NodeId>,
        delete_document_ids: &mut BTreeSet<String>,
    ) {
        for op in ops {
            match op {
                WalOp::CreateNode {
                    id,
                    label,
                    properties,
                } => {
                    if search_projection_document_id_for_label_and_properties(
                        label, properties, *id,
                    )
                    .is_some()
                    {
                        upsert_node_ids.insert(*id);
                    }
                }
                WalOp::SetNodeProperty { id, property, .. } => {
                    if let Some(node) = self.nodes.get(id) {
                        if let Some(document_id) =
                            search_projection_document_id_for_node(catalog, node)
                        {
                            if property == "id" {
                                delete_document_ids.insert(document_id);
                            }
                            upsert_node_ids.insert(*id);
                        }
                    }
                }
                WalOp::DeleteNode { id } => {
                    if let Some(node) = self.nodes.get(id) {
                        if let Some(document_id) =
                            search_projection_document_id_for_node(catalog, node)
                        {
                            delete_document_ids.insert(document_id);
                        }
                    }
                }
                WalOp::Batch(batch_ops) => self.collect_search_projection_graph_changes_for_ops(
                    catalog,
                    batch_ops,
                    upsert_node_ids,
                    delete_document_ids,
                ),
                WalOp::CreateNodeLabel { .. }
                | WalOp::CreateRelationshipType { .. }
                | WalOp::CreateNodeTable { .. }
                | WalOp::CreateRelationshipTable { .. }
                | WalOp::CreateProperty { .. }
                | WalOp::AlterTableState { .. }
                | WalOp::AlterPropertyState { .. }
                | WalOp::GcTableDescriptor { .. }
                | WalOp::GcPropertyDescriptor { .. }
                | WalOp::CreateIndex { .. }
                | WalOp::CreateCompositeIndex { .. }
                | WalOp::CreateRangeIndex { .. }
                | WalOp::CreateFullTextIndex { .. }
                | WalOp::CreateUniqueConstraint { .. }
                | WalOp::CreateNodePropertyExistsConstraint { .. }
                | WalOp::CreateRelationshipUniqueConstraint { .. }
                | WalOp::CreateRelationshipPropertyExistsConstraint { .. }
                | WalOp::CreateRelationship { .. }
                | WalOp::SetRelationshipProperty { .. }
                | WalOp::DeleteRelationship { .. }
                | WalOp::ProjectGraph { .. } => {}
            }
        }
    }

    fn apply_create_node(
        &mut self,
        catalog: &Catalog,
        id: NodeId,
        label_id: LabelId,
        properties: BTreeMap<String, Value>,
    ) {
        self.apply_create_node_with_labels(catalog, id, BTreeSet::from([label_id]), properties);
    }

    fn apply_create_node_with_labels(
        &mut self,
        catalog: &Catalog,
        id: NodeId,
        labels: BTreeSet<LabelId>,
        properties: BTreeMap<String, Value>,
    ) {
        self.next_node_id = self.next_node_id.max(id.0 + 1);
        if let Some(old_node) = self.nodes.remove(&id) {
            self.remove_node_from_basic_statistics(&old_node);
        }
        self.nodes.insert(
            id,
            NodeRecord {
                id,
                labels,
                properties,
            },
        );
        if let Some(node) = self.nodes.get(&id).cloned() {
            self.add_node_to_basic_statistics(&node);
            for label_id in &node.labels {
                for (property, value) in &node.properties {
                    self.property_index
                        .entry((*label_id, property.clone(), value.clone()))
                        .or_default()
                        .insert(id);
                }
            }
            self.add_node_to_composite_property_indexes(catalog, &node);
            self.add_node_to_full_text_property_indexes(catalog, &node);
        }
    }

    fn add_node_to_composite_property_indexes(&mut self, catalog: &Catalog, node: &NodeRecord) {
        for index in catalog.composite_property_indexes() {
            if !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(key) = composite_property_index_key(node, &index.properties) else {
                continue;
            };
            self.composite_property_index
                .entry((index.label_id, key))
                .or_default()
                .insert(node.id);
        }
    }

    fn remove_node_from_composite_property_indexes(
        &mut self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) {
        for index in catalog.composite_property_indexes() {
            if !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(key) = composite_property_index_key(node, &index.properties) else {
                continue;
            };
            let map_key = (index.label_id, key);
            if let Some(ids) = self.composite_property_index.get_mut(&map_key) {
                ids.remove(&node.id);
                if ids.is_empty() {
                    self.composite_property_index.remove(&map_key);
                }
            }
        }
    }

    fn rebuild_composite_property_index_for_descriptor(
        &mut self,
        label_id: LabelId,
        properties: &[String],
    ) {
        self.rebuild_composite_property_index_projection(label_id, properties);
    }

    fn rebuild_composite_property_index_projection(
        &mut self,
        label_id: LabelId,
        properties: &[String],
    ) -> usize {
        self.composite_property_index
            .retain(|(candidate_label_id, key), _| {
                *candidate_label_id != label_id
                    || key
                        .iter()
                        .map(|(property, _)| property)
                        .ne(properties.iter())
            });
        let mut indexed_entries = 0usize;
        let nodes = self.nodes.values().cloned().collect::<Vec<_>>();
        for node in nodes {
            if !node.labels.contains(&label_id) {
                continue;
            }
            let Some(key) = composite_property_index_key(&node, properties) else {
                continue;
            };
            self.composite_property_index
                .entry((label_id, key))
                .or_default()
                .insert(node.id);
            indexed_entries = indexed_entries.saturating_add(1);
        }
        indexed_entries
    }

    fn add_node_to_full_text_property_indexes(&mut self, catalog: &Catalog, node: &NodeRecord) {
        for index in catalog.property_indexes() {
            if index.kind != IndexKind::FullText || !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(Value::String(value)) = node.properties.get(&index.property) else {
                continue;
            };
            for token in full_text_index_tokens(value) {
                self.full_text_property_index
                    .entry((index.label_id, index.property.clone(), token))
                    .or_default()
                    .insert(node.id);
            }
        }
    }

    fn remove_node_from_full_text_property_indexes(
        &mut self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) {
        for index in catalog.property_indexes() {
            if index.kind != IndexKind::FullText || !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(Value::String(value)) = node.properties.get(&index.property) else {
                continue;
            };
            for token in full_text_index_tokens(value) {
                let map_key = (index.label_id, index.property.clone(), token);
                if let Some(ids) = self.full_text_property_index.get_mut(&map_key) {
                    ids.remove(&node.id);
                    if ids.is_empty() {
                        self.full_text_property_index.remove(&map_key);
                    }
                }
            }
        }
    }

    fn rebuild_full_text_property_index_for_descriptor(
        &mut self,
        label_id: LabelId,
        property: &str,
    ) {
        self.rebuild_full_text_property_index_projection(label_id, property);
    }

    fn rebuild_full_text_property_index_projection(
        &mut self,
        label_id: LabelId,
        property: &str,
    ) -> usize {
        self.full_text_property_index
            .retain(|(candidate_label_id, candidate_property, _), _| {
                *candidate_label_id != label_id || candidate_property != property
            });
        let mut indexed_entries = 0usize;
        let nodes = self.nodes.values().cloned().collect::<Vec<_>>();
        for node in nodes {
            if !node.labels.contains(&label_id) {
                continue;
            }
            let Some(Value::String(value)) = node.properties.get(property) else {
                continue;
            };
            for token in full_text_index_tokens(value) {
                self.full_text_property_index
                    .entry((label_id, property.to_string(), token))
                    .or_default()
                    .insert(node.id);
                indexed_entries = indexed_entries.saturating_add(1);
            }
        }
        indexed_entries
    }

    fn apply_schema_maintenance_op(&mut self, catalog: &mut Catalog, op: WalOp) {
        match op {
            WalOp::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.set_table_state(id, state);
                }
            }
            WalOp::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table) {
                    if let Some(id) = catalog.property_descriptor_id(table_id, &property) {
                        catalog.set_property_state(id, state);
                    }
                }
            }
            WalOp::GcPropertyDescriptor {
                table_kind,
                table,
                property,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table) {
                    if let Some(id) = catalog.property_descriptor_id(table_id, &property) {
                        catalog.remove_property_descriptor(id);
                    }
                }
            }
            WalOp::GcTableDescriptor { table_kind, table } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.remove_table_descriptor(id);
                }
            }
            _ => {}
        }
    }

    fn apply_create_relationship(
        &mut self,
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: RelTypeId,
        properties: BTreeMap<String, Value>,
    ) {
        self.next_rel_id = self.next_rel_id.max(id.0 + 1);
        if let Some(old_relationship) = self.relationships.remove(&id) {
            self.remove_relationship_from_basic_statistics(&old_relationship);
        }
        self.relationships.insert(
            id,
            RelRecord {
                id,
                source,
                target,
                rel_type,
                properties,
            },
        );
        if let Some(relationship) = self.relationships.get(&id).cloned() {
            self.add_relationship_to_basic_statistics(&relationship);
        }
        self.outgoing
            .entry((source, rel_type))
            .or_default()
            .insert(id);
        self.incoming
            .entry((target, rel_type))
            .or_default()
            .insert(id);
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
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
    ) -> ScanPrunedNodeScan<'a> {
        let candidate = filter.and_then(|filter| self.prune_node_candidates(label_id, filter));
        let Some(candidate) = candidate else {
            let candidate_count_before_filter = self.label_node_count(label_id);
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
                    label_id,
                    strategy: ScanPruningStrategy::FullLabelScan,
                    pruned: false,
                    exact_empty: false,
                    candidate_count_before_filter,
                    output_count,
                    filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
                },
            };
        };

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
                label_id,
                strategy: candidate.strategy,
                pruned: true,
                exact_empty: candidate.exact_empty,
                candidate_count_before_filter,
                output_count,
                filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
            },
        }
    }

    fn label_node_count(&self, label_id: Option<LabelId>) -> usize {
        self.nodes
            .values()
            .filter(|node| self.node_matches_label(node, label_id))
            .count()
    }

    fn node_matches_label(&self, node: &NodeRecord, label_id: Option<LabelId>) -> bool {
        label_id
            .map(|label_id| node.labels.contains(&label_id))
            .unwrap_or(true)
    }

    fn prune_node_candidates(
        &self,
        label_id: Option<LabelId>,
        filter: &PropertyFilter,
    ) -> Option<ScanPruningCandidate> {
        match filter {
            PropertyFilter::And(filters) => self.prune_and_node_candidates(label_id, filters),
            PropertyFilter::Or(filters) => self.prune_or_node_candidates(label_id, filters),
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
            PropertyFilter::Eq { property, value } => Some(ScanPruningCandidate::exact(
                ScanPruningStrategy::PropertyEq {
                    property: property.clone(),
                },
                self.node_ids_for_property_values(label_id, property, std::slice::from_ref(value)),
            )),
            PropertyFilter::NotEq { property, value } => Some(ScanPruningCandidate::exact(
                ScanPruningStrategy::PropertyNotEq {
                    property: property.clone(),
                },
                self.node_ids_for_property_not_in_values(
                    label_id,
                    property,
                    std::slice::from_ref(value),
                ),
            )),
            PropertyFilter::IsNull { .. }
            | PropertyFilter::IsNotNull { .. }
            | PropertyFilter::ListContains { .. }
            | PropertyFilter::Contains { .. }
            | PropertyFilter::StartsWith { .. }
            | PropertyFilter::EndsWith { .. }
            | PropertyFilter::RegexMatch { .. }
            | PropertyFilter::DefaultIfNullOrEq { .. } => None,
            PropertyFilter::In { property, values } => Some(ScanPruningCandidate::exact(
                if values.is_empty() {
                    ScanPruningStrategy::Empty
                } else {
                    ScanPruningStrategy::PropertyIn {
                        property: property.clone(),
                    }
                },
                self.node_ids_for_property_values(label_id, property, values),
            )),
            PropertyFilter::Range {
                property,
                lower,
                upper,
            } => {
                if lower.is_none() && upper.is_none() {
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
        label_id: Option<LabelId>,
        filters: &[PropertyFilter],
    ) -> Option<ScanPruningCandidate> {
        let mut best: Option<ScanPruningCandidate> = None;
        for filter in filters {
            let Some(candidate) = self.prune_node_candidates(label_id, filter) else {
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
            let candidate = self.prune_node_candidates(label_id, filter)?;
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
            .cloned()
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

    pub fn outgoing_relationships<'a>(
        &'a self,
        source: NodeId,
        rel_type: RelTypeId,
    ) -> impl Iterator<Item = &'a RelRecord> + 'a {
        self.outgoing
            .get(&(source, rel_type))
            .into_iter()
            .flat_map(|rel_ids| rel_ids.iter())
            .filter_map(|rel_id| self.relationships.get(rel_id))
    }

    pub fn incoming_relationships<'a>(
        &'a self,
        target: NodeId,
        rel_type: RelTypeId,
    ) -> impl Iterator<Item = &'a RelRecord> + 'a {
        self.incoming
            .get(&(target, rel_type))
            .into_iter()
            .flat_map(|rel_ids| rel_ids.iter())
            .filter_map(|rel_id| self.relationships.get(rel_id))
    }

    pub fn adjacency_group_stats(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> AdjacencyGroupStats {
        let degree = self
            .adjacency_relationship_ids(node_id, rel_type, direction)
            .map(BTreeSet::len)
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
            .flat_map(|rel_ids| rel_ids.iter())
            .filter_map(|rel_id| {
                let relationship = self.relationships.get(rel_id)?;
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
            .flat_map(|(_, rel_ids)| rel_ids.iter())
            .filter_map(|rel_id| {
                let relationship = self.relationships.get(rel_id)?;
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

    pub fn relationship(&self, id: RelId) -> Option<&RelRecord> {
        self.relationships.get(&id)
    }

    pub fn node(&self, id: NodeId) -> Option<&NodeRecord> {
        self.nodes.get(&id)
    }

    fn adjacency_relationship_ids(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> Option<&BTreeSet<RelId>> {
        match direction {
            AdjacencyDirection::Outgoing => self.outgoing.get(&(node_id, rel_type)),
            AdjacencyDirection::Incoming => self.incoming.get(&(node_id, rel_type)),
        }
    }

    pub fn register_projected_graph(
        &mut self,
        name: &str,
        definition: ProjectedGraphDefinition,
    ) -> Result<()> {
        if let Some(durable) = &mut self.durable {
            durable.append_project_graph(name, &definition)?;
        }
        self.apply_project_graph_definition(name.to_string(), definition);
        self.commit_epoch += 1;
        Ok(())
    }

    pub fn projected_graph_definition(&self, name: &str) -> Option<&ProjectedGraphDefinition> {
        self.projected_graphs.get(name)
    }

    pub fn projected_graph_artifact(
        &self,
        name: &str,
        definition: &ProjectedGraphDefinition,
    ) -> Option<&ProjectedGraph> {
        let artifact = self.projected_graph_artifacts.get(name)?;
        (artifact.commit_epoch == self.commit_epoch && &artifact.definition == definition)
            .then_some(&artifact.graph)
    }

    pub fn projected_graph_statuses(&self) -> Vec<ProjectedGraphStatus> {
        self.projected_graphs
            .iter()
            .map(|(name, definition)| {
                let artifact = self.projected_graph_artifacts.get(name);
                let reusable = artifact.is_some_and(|artifact| {
                    artifact.commit_epoch == self.commit_epoch && artifact.definition == *definition
                });
                ProjectedGraphStatus {
                    name: name.clone(),
                    node_labels: definition.node_labels.clone(),
                    rel_types: definition.rel_types.clone(),
                    projection_epoch: artifact.map(|artifact| artifact.projection_epoch),
                    commit_epoch: artifact.map(|artifact| artifact.commit_epoch),
                    node_count: artifact.map(|artifact| artifact.graph.node_count()),
                    edge_count: artifact.map(|artifact| artifact.graph.edge_count()),
                    reusable,
                }
            })
            .collect()
    }

    fn apply_project_graph_definition(
        &mut self,
        name: String,
        definition: ProjectedGraphDefinition,
    ) {
        self.projected_graph_artifacts.remove(&name);
        self.projected_graphs.insert(name, definition);
    }

    fn load_projected_graph_artifacts(&mut self) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        self.projected_graph_artifacts = durable
            .load_projected_graph_artifacts()?
            .into_iter()
            .filter(|(name, artifact)| {
                artifact.commit_epoch == self.commit_epoch
                    && self
                        .projected_graphs
                        .get(name)
                        .is_some_and(|definition| definition == &artifact.definition)
            })
            .collect();
        Ok(())
    }

    fn load_stable_id_mapping(&mut self) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        self.stable_id_mapping = durable.load_stable_id_mapping()?;
        Ok(())
    }

    fn write_stable_id_mapping(&self) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        durable.write_stable_id_mapping(&self.stable_id_mapping)
    }

    fn next_projection_epoch(&self) -> u64 {
        let checkpoint_epoch = self
            .durable
            .as_ref()
            .map(|durable| durable.checkpoint_epoch)
            .unwrap_or_default();
        let artifact_epoch = self
            .projected_graph_artifacts
            .values()
            .map(|artifact| artifact.projection_epoch)
            .max()
            .unwrap_or_default();
        checkpoint_epoch.max(artifact_epoch) + 1
    }

    fn find_node_by_label_and_properties(
        &self,
        label_id: LabelId,
        properties: &BTreeMap<String, Value>,
    ) -> Option<NodeId> {
        self.nodes
            .values()
            .find(|node| {
                node.labels.contains(&label_id)
                    && properties
                        .iter()
                        .all(|(key, value)| node.properties.get(key) == Some(value))
            })
            .map(|node| node.id)
    }

    fn find_relationship_by_properties(
        &self,
        source: NodeId,
        target: NodeId,
        rel_type_id: RelTypeId,
        properties: &BTreeMap<String, Value>,
    ) -> Option<RelId> {
        self.outgoing_relationships(source, rel_type_id)
            .find(|relationship| {
                relationship.target == target && &relationship.properties == properties
            })
            .map(|relationship| relationship.id)
    }

    fn find_relationship_by_property_subset(
        &self,
        source: NodeId,
        target: NodeId,
        rel_type_id: RelTypeId,
        properties: &BTreeMap<String, Value>,
    ) -> Option<RelId> {
        self.outgoing_relationships(source, rel_type_id)
            .find(|relationship| {
                relationship.target == target
                    && properties_contain_all(&relationship.properties, properties)
            })
            .map(|relationship| relationship.id)
    }

    fn merge_connected_node_ops(
        &self,
        request: &ConnectedNodesCreate,
        source: NodeId,
        target: NodeId,
        relationship: RelId,
    ) -> Vec<WalOp> {
        let mut ops = Vec::new();
        if !self.nodes.contains_key(&source) {
            ops.push(WalOp::CreateNode {
                id: source,
                label: request.source_label.clone(),
                properties: request.source_properties.clone(),
            });
        }
        if !self.nodes.contains_key(&target) {
            ops.push(WalOp::CreateNode {
                id: target,
                label: request.target_label.clone(),
                properties: request.target_properties.clone(),
            });
        }
        ops.push(WalOp::CreateRelationship {
            id: relationship,
            source,
            target,
            rel_type: request.rel_type.clone(),
            properties: request.rel_properties.clone(),
        });
        ops
    }

    fn matching_node_ids<'a>(
        &'a self,
        label_id: Option<LabelId>,
        filter: Option<&'a PropertyFilter>,
    ) -> impl Iterator<Item = NodeId> + 'a {
        self.scan_nodes_with_filter_pruning(label_id, filter)
            .nodes
            .into_iter()
            .map(|node| node.id)
    }

    fn apply_set_node_property(
        &mut self,
        catalog: &Catalog,
        id: NodeId,
        property: String,
        value: Value,
    ) {
        if let Some(node) = self.nodes.get(&id).cloned() {
            self.remove_node_from_composite_property_indexes(catalog, &node);
            self.remove_node_from_full_text_property_indexes(catalog, &node);
        }
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        let old_value = node.properties.insert(property.clone(), value.clone());
        let labels = node.labels.clone();
        for label_id in labels {
            if let Some(old_value) = &old_value {
                let key = (label_id, property.clone(), old_value.clone());
                if let Some(ids) = self.property_index.get_mut(&key) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.property_index.remove(&key);
                    }
                }
            }
            self.property_index
                .entry((label_id, property.clone(), value.clone()))
                .or_default()
                .insert(id);
        }
        if let Some(node) = self.nodes.get(&id).cloned() {
            self.add_node_to_composite_property_indexes(catalog, &node);
            self.add_node_to_full_text_property_indexes(catalog, &node);
        }
    }

    fn apply_set_relationship_property(&mut self, id: RelId, property: String, value: Value) {
        let Some(relationship) = self.relationships.get_mut(&id) else {
            return;
        };
        relationship.properties.insert(property, value);
    }

    fn validate_constraints_for_ops(&self, catalog: &Catalog, ops: &[WalOp]) -> Result<()> {
        let mut nodes = self.nodes.clone();
        let mut relationships = self.relationships.clone();
        for op in ops {
            apply_wal_op_to_snapshot(catalog, &mut nodes, &mut relationships, op);
        }
        validate_unique_constraints(catalog, &nodes)?;
        validate_relationship_unique_constraints(catalog, &relationships)?;
        validate_node_property_exists_constraints(catalog, &nodes)?;
        validate_relationship_property_exists_constraints(catalog, &relationships)?;
        validate_property_schemas(catalog, &nodes, &relationships)
    }

    fn validate_unique_constraint(
        &self,
        catalog: &Catalog,
        label_id: LabelId,
        property: &str,
    ) -> Result<()> {
        validate_unique_property(catalog, &self.nodes, label_id, property)
    }

    fn validate_node_property_exists_constraint(
        &self,
        catalog: &Catalog,
        label_id: LabelId,
        property: &str,
    ) -> Result<()> {
        validate_node_property_exists(catalog, &self.nodes, label_id, property)
    }

    fn validate_relationship_unique_constraint(
        &self,
        catalog: &Catalog,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> Result<()> {
        validate_unique_relationship_property(catalog, &self.relationships, rel_type_id, property)
    }

    fn validate_relationship_property_exists_constraint(
        &self,
        catalog: &Catalog,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> Result<()> {
        validate_relationship_property_exists(catalog, &self.relationships, rel_type_id, property)
    }

    fn validate_relationship_endpoints(&self) -> Result<()> {
        for relationship in self.relationships.values() {
            if !self.nodes.contains_key(&relationship.source) {
                return Err(SkeinError::Storage(format!(
                    "relationship {} references missing source node {}",
                    relationship.id.0, relationship.source.0
                )));
            }
            if !self.nodes.contains_key(&relationship.target) {
                return Err(SkeinError::Storage(format!(
                    "relationship {} references missing target node {}",
                    relationship.id.0, relationship.target.0
                )));
            }
        }
        Ok(())
    }

    fn delete_node_ops(&self, ids: &[NodeId], detach: bool) -> Result<Vec<WalOp>> {
        let mut relationship_ids = BTreeSet::new();
        for id in ids {
            for relationship in self.relationships.values() {
                if relationship.source == *id || relationship.target == *id {
                    if !detach {
                        return Err(SkeinError::Storage(format!(
                            "node {} has relationships; use DETACH DELETE",
                            id.0
                        )));
                    }
                    relationship_ids.insert(relationship.id);
                }
            }
        }
        let mut ops = relationship_ids
            .into_iter()
            .map(|id| WalOp::DeleteRelationship { id })
            .collect::<Vec<_>>();
        ops.extend(ids.iter().copied().map(|id| WalOp::DeleteNode { id }));
        Ok(ops)
    }

    fn apply_delete_relationship(&mut self, id: RelId) {
        let Some(relationship) = self.relationships.remove(&id) else {
            return;
        };
        self.remove_relationship_from_basic_statistics(&relationship);
        let outgoing_key = (relationship.source, relationship.rel_type);
        if let Some(ids) = self.outgoing.get_mut(&outgoing_key) {
            ids.remove(&id);
            if ids.is_empty() {
                self.outgoing.remove(&outgoing_key);
            }
        }
        let incoming_key = (relationship.target, relationship.rel_type);
        if let Some(ids) = self.incoming.get_mut(&incoming_key) {
            ids.remove(&id);
            if ids.is_empty() {
                self.incoming.remove(&incoming_key);
            }
        }
    }

    fn apply_delete_node(&mut self, catalog: &Catalog, id: NodeId) {
        let Some(node) = self.nodes.remove(&id) else {
            return;
        };
        self.remove_node_from_basic_statistics(&node);
        self.remove_node_from_composite_property_indexes(catalog, &node);
        self.remove_node_from_full_text_property_indexes(catalog, &node);
        for label_id in node.labels {
            for (property, value) in &node.properties {
                let key = (label_id, property.clone(), value.clone());
                if let Some(ids) = self.property_index.get_mut(&key) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.property_index.remove(&key);
                    }
                }
            }
        }
    }

    fn load_checkpoint(&mut self, catalog: &mut Catalog) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        if !durable.checkpoint_path.exists() {
            return Ok(());
        }
        let text = read_durable_text(&durable.checkpoint_path, "checkpoint")?;
        let (body, checksum) = split_checkpoint_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "checkpoint checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        for line in body.lines() {
            if line == "SKEIN_CHECKPOINT_V1" {
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["version", version] => validate_storage_version(version)?,
                ["next_node_id", raw] => {
                    self.next_node_id = parse_u64(raw, "next_node_id")?;
                }
                ["next_rel_id", raw] => {
                    self.next_rel_id = parse_u64(raw, "next_rel_id")?;
                }
                ["commit_epoch", raw] => {
                    self.commit_epoch = parse_u64(raw, "commit_epoch")?;
                }
                ["label", raw_id, raw_name] => {
                    let id = LabelId(parse_u32(raw_id, "label id")?);
                    catalog.import_label(id, decode_string(raw_name)?);
                }
                ["rel_type", raw_id, raw_name] => {
                    let id = RelTypeId(parse_u32(raw_id, "rel type id")?);
                    catalog.import_rel_type(id, decode_string(raw_name)?);
                }
                ["property_index", raw_id, raw_label_id, raw_property] => {
                    catalog.import_property_index(
                        crate::schema::IndexId(parse_u32(raw_id, "property index id")?),
                        LabelId(parse_u32(raw_label_id, "property index label id")?),
                        decode_string(raw_property)?,
                    );
                }
                ["property_index", raw_id, raw_label_id, raw_property, raw_kind] => {
                    catalog.import_property_index_with_kind(
                        crate::schema::IndexId(parse_u32(raw_id, "property index id")?),
                        LabelId(parse_u32(raw_label_id, "property index label id")?),
                        decode_string(raw_property)?,
                        decode_index_kind(raw_kind)?,
                    );
                }
                ["composite_property_index", raw_id, raw_label_id, raw_properties] => {
                    catalog.import_composite_property_index(
                        crate::schema::IndexId(parse_u32(raw_id, "composite property index id")?),
                        LabelId(parse_u32(
                            raw_label_id,
                            "composite property index label id",
                        )?),
                        decode_string_vec(raw_properties)?,
                    );
                }
                ["table", raw_id, raw_kind, raw_name, raw_state] => {
                    let kind = decode_table_kind(raw_kind)?;
                    let name = decode_string(raw_name)?;
                    match kind {
                        TableKind::Node => {
                            catalog.get_or_create_label(&name);
                        }
                        TableKind::Relationship => {
                            catalog.get_or_create_rel_type(&name);
                        }
                    }
                    catalog.import_table(
                        TableId(parse_u32(raw_id, "table id")?),
                        kind,
                        name,
                        decode_schema_object_state(raw_state)?,
                    );
                }
                ["property", raw_id, raw_table_id, raw_name, raw_type, raw_nullable, raw_state] => {
                    catalog.import_property_descriptor(
                        PropertyId(parse_u32(raw_id, "property id")?),
                        TableId(parse_u32(raw_table_id, "property table id")?),
                        decode_string(raw_name)?,
                        decode_property_type(raw_type)?,
                        decode_nullable(raw_nullable)?,
                        decode_schema_object_state(raw_state)?,
                    );
                }
                ["unique_constraint", raw_id, raw_label_id, raw_property] => {
                    catalog.import_unique_constraint(
                        ConstraintId(parse_u32(raw_id, "unique constraint id")?),
                        LabelId(parse_u32(raw_label_id, "unique constraint label id")?),
                        decode_string(raw_property)?,
                    );
                }
                ["node_property_exists_constraint", raw_id, raw_label_id, raw_property] => {
                    catalog.import_node_property_exists_constraint(
                        ConstraintId(parse_u32(raw_id, "node property exists constraint id")?),
                        LabelId(parse_u32(
                            raw_label_id,
                            "node property exists constraint label id",
                        )?),
                        decode_string(raw_property)?,
                    );
                }
                ["relationship_property_exists_constraint", raw_id, raw_rel_type_id, raw_property] =>
                {
                    catalog.import_relationship_property_exists_constraint(
                        ConstraintId(parse_u32(
                            raw_id,
                            "relationship property exists constraint id",
                        )?),
                        RelTypeId(parse_u32(
                            raw_rel_type_id,
                            "relationship property exists constraint rel type id",
                        )?),
                        decode_string(raw_property)?,
                    );
                }
                ["relationship_unique_constraint", raw_id, raw_rel_type_id, raw_property] => {
                    catalog.import_relationship_unique_constraint(
                        ConstraintId(parse_u32(raw_id, "relationship unique constraint id")?),
                        RelTypeId(parse_u32(
                            raw_rel_type_id,
                            "relationship unique constraint rel type id",
                        )?),
                        decode_string(raw_property)?,
                    );
                }
                ["stat_commit_epoch", _]
                | ["stat_histogram_sample_limit", _]
                | ["stat_node_count", _]
                | ["stat_relationship_count", _]
                | ["stat_label_count", _, _]
                | ["stat_rel_type_count", _, _]
                | ["stat_rel_type_source_count", _, _]
                | ["stat_rel_type_target_count", _, _]
                | ["stat_path_count", _, _, _, _]
                | ["stat_path_source_distinct_count", _, _, _, _]
                | ["stat_path_target_distinct_count", _, _, _, _]
                | ["stat_bounded_path_count", _, _, _, _, _]
                | ["stat_bounded_path_source_distinct_count", _, _, _, _, _]
                | ["stat_bounded_path_target_distinct_count", _, _, _, _, _]
                | ["stat_property_distinct_count", _, _, _]
                | ["stat_rel_property_distinct_count", _, _, _]
                | ["stat_rel_property_histogram", _, _, _]
                | ["stat_property_histogram", _, _, _]
                | ["stat_rel_property_histogram_sampled", _, _, _]
                | ["stat_property_histogram_sampled", _, _, _] => {}
                ["project_graph", raw_name, raw_node_labels, raw_rel_types] => {
                    self.apply_project_graph_definition(
                        decode_string(raw_name)?,
                        ProjectedGraphDefinition {
                            node_labels: decode_string_vec(raw_node_labels)?,
                            rel_types: decode_string_vec(raw_rel_types)?,
                        },
                    );
                }
                ["node", raw_id, raw_labels, raw_properties] => {
                    let id = NodeId(parse_u64(raw_id, "node id")?);
                    let labels = parse_label_set(raw_labels)?;
                    let properties = decode_properties(raw_properties)?;
                    register_property_index_descriptors(
                        catalog,
                        labels.iter().copied(),
                        &properties,
                    );
                    self.apply_create_node_with_labels(catalog, id, labels, properties);
                }
                ["rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
                    self.apply_create_relationship(
                        RelId(parse_u64(raw_id, "rel id")?),
                        NodeId(parse_u64(raw_source, "rel source")?),
                        NodeId(parse_u64(raw_target, "rel target")?),
                        RelTypeId(parse_u32(raw_type, "rel type")?),
                        decode_properties(raw_properties)?,
                    );
                }
                [""] => {}
                _ => {
                    return Err(SkeinError::Storage(format!(
                        "invalid checkpoint line: {line}"
                    )));
                }
            }
        }
        self.search_projection_change_log_start_epoch = self.commit_epoch;
        Ok(())
    }

    fn replay_wal(
        &mut self,
        catalog: &mut Catalog,
        config: WalReplayConfig,
    ) -> Result<StorageRecoveryReport> {
        let Some(durable) = &self.durable else {
            return Ok(StorageRecoveryReport::default());
        };
        let wal_path = durable.wal_path.clone();
        let checkpoint_epoch = durable.checkpoint_epoch;
        let checkpoint_commit_epoch = durable.checkpoint_commit_epoch;
        let wal_replay_start_lsn = durable.wal_replay_start_lsn;
        let mut next_lsn = durable.next_lsn;
        let mut replayed_entries = 0_usize;
        let mut torn_tail_reason = None;
        let wal_present = wal_path.exists();
        if !wal_path.exists() {
            return Ok(StorageRecoveryReport {
                durable: true,
                recovery_mode: config.recovery_mode,
                max_wal_replay_entries: config.max_entries,
                checkpoint_epoch: Some(checkpoint_epoch),
                checkpoint_commit_epoch: Some(checkpoint_commit_epoch),
                wal_present,
                wal_replay_start_lsn: Some(wal_replay_start_lsn),
                next_lsn_after_replay: Some(next_lsn),
                replayed_wal_entries: replayed_entries,
                torn_tail_ignored: false,
                torn_tail_reason,
                recovered_commit_epoch: self.commit_epoch,
            });
        }
        let file = File::open(&wal_path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let entry = match WalEntry::decode(&line)? {
                WalDecodeResult::Entry(entry) => entry,
                WalDecodeResult::TornTail(reason) => match config.recovery_mode {
                    RecoveryMode::TolerateTornTail => {
                        torn_tail_reason = Some(reason);
                        break;
                    }
                    RecoveryMode::Strict => {
                        return Err(SkeinError::Storage(format!(
                            "strict WAL recovery rejected torn tail: {reason}"
                        )));
                    }
                },
            };
            if let Some(max_entries) = config.max_entries {
                if replayed_entries >= max_entries {
                    return Err(SkeinError::Storage(format!(
                        "WAL replay entry limit exceeded: max_wal_replay_entries={max_entries}"
                    )));
                }
            }
            replayed_entries += 1;
            next_lsn = next_lsn.max(entry.lsn + 1);
            match entry.op {
                WalOp::Batch(ops) => {
                    let commit_epoch = self.commit_epoch + 1;
                    self.record_search_projection_graph_changes_for_ops(
                        catalog,
                        commit_epoch,
                        &ops,
                    );
                    for op in ops {
                        self.apply_wal_op(catalog, op);
                    }
                    self.commit_epoch += 1;
                }
                op => {
                    let commit_epoch = self.commit_epoch + 1;
                    self.record_search_projection_graph_changes_for_ops(
                        catalog,
                        commit_epoch,
                        std::slice::from_ref(&op),
                    );
                    self.apply_wal_op(catalog, op);
                    self.commit_epoch += 1;
                }
            }
        }
        if let Some(durable) = &mut self.durable {
            durable.next_lsn = next_lsn;
        }
        Ok(StorageRecoveryReport {
            durable: true,
            recovery_mode: config.recovery_mode,
            max_wal_replay_entries: config.max_entries,
            checkpoint_epoch: Some(checkpoint_epoch),
            checkpoint_commit_epoch: Some(checkpoint_commit_epoch),
            wal_present,
            wal_replay_start_lsn: Some(wal_replay_start_lsn),
            next_lsn_after_replay: Some(next_lsn),
            replayed_wal_entries: replayed_entries,
            torn_tail_ignored: torn_tail_reason.is_some(),
            torn_tail_reason,
            recovered_commit_epoch: self.commit_epoch,
        })
    }

    fn apply_wal_op(&mut self, catalog: &mut Catalog, op: WalOp) {
        match op {
            WalOp::CreateNodeLabel { label } => {
                catalog.get_or_create_label(&label);
            }
            WalOp::CreateRelationshipType { rel_type } => {
                catalog.get_or_create_rel_type(&rel_type);
            }
            WalOp::CreateNodeTable { name } => {
                catalog.get_or_create_label(&name);
                catalog.get_or_create_table(TableKind::Node, &name);
            }
            WalOp::CreateRelationshipTable { name } => {
                catalog.get_or_create_rel_type(&name);
                catalog.get_or_create_table(TableKind::Relationship, &name);
            }
            WalOp::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => {
                let table_id = ensure_table_descriptor(catalog, table_kind, &table);
                catalog.get_or_create_property(table_id, &property, value_type, nullable);
            }
            WalOp::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.set_table_state(id, state);
                }
            }
            WalOp::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table) {
                    if let Some(id) = catalog.property_descriptor_id(table_id, &property) {
                        catalog.set_property_state(id, state);
                    }
                }
            }
            WalOp::GcPropertyDescriptor {
                table_kind,
                table,
                property,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table) {
                    if let Some(id) = catalog.property_descriptor_id(table_id, &property) {
                        catalog.remove_property_descriptor(id);
                    }
                }
            }
            WalOp::GcTableDescriptor { table_kind, table } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.remove_table_descriptor(id);
                }
            }
            WalOp::CreateIndex { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_property_index(label_id, &property);
            }
            WalOp::CreateCompositeIndex { label, properties } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_composite_property_index(label_id, &properties);
                self.rebuild_composite_property_index_for_descriptor(label_id, &properties);
            }
            WalOp::CreateRangeIndex { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_property_index_with_kind(
                    label_id,
                    &property,
                    IndexKind::Range,
                );
            }
            WalOp::CreateFullTextIndex { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_property_index_with_kind(
                    label_id,
                    &property,
                    IndexKind::FullText,
                );
                self.rebuild_full_text_property_index_for_descriptor(label_id, &property);
            }
            WalOp::CreateUniqueConstraint { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_unique_constraint(label_id, &property);
            }
            WalOp::CreateNodePropertyExistsConstraint { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_node_property_exists_constraint(label_id, &property);
            }
            WalOp::CreateRelationshipUniqueConstraint { rel_type, property } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                catalog.get_or_create_relationship_unique_constraint(rel_type_id, &property);
            }
            WalOp::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                catalog
                    .get_or_create_relationship_property_exists_constraint(rel_type_id, &property);
            }
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => {
                let label_id = catalog.get_or_create_label(&label);
                register_property_index_descriptors(catalog, [label_id], &properties);
                self.apply_create_node(catalog, id, label_id, properties);
            }
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                self.apply_create_relationship(id, source, target, rel_type_id, properties);
            }
            WalOp::SetNodeProperty {
                id,
                property,
                value,
            } => {
                if let Some(node) = self.nodes.get(&id) {
                    let properties = BTreeMap::from([(property.clone(), value.clone())]);
                    register_property_index_descriptors(
                        catalog,
                        node.labels.iter().copied(),
                        &properties,
                    );
                }
                self.apply_set_node_property(catalog, id, property, value);
            }
            WalOp::SetRelationshipProperty {
                id,
                property,
                value,
            } => {
                self.apply_set_relationship_property(id, property, value);
            }
            WalOp::DeleteNode { id } => {
                self.apply_delete_node(catalog, id);
            }
            WalOp::DeleteRelationship { id } => {
                self.apply_delete_relationship(id);
            }
            WalOp::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => {
                self.apply_project_graph_definition(
                    name,
                    ProjectedGraphDefinition {
                        node_labels,
                        rel_types,
                    },
                );
            }
            WalOp::Batch(ops) => {
                for op in ops {
                    self.apply_wal_op(catalog, op);
                }
            }
        }
    }
}

#[derive(Debug)]
struct DurableStore {
    checkpoint_path: PathBuf,
    manifest_path: PathBuf,
    projected_graphs_path: PathBuf,
    stable_id_mapping_path: PathBuf,
    wal_path: PathBuf,
    checkpoint_epoch: u64,
    checkpoint_commit_epoch: u64,
    oldest_reader_commit_epoch: Option<u64>,
    safe_reclaim_commit_epoch: u64,
    wal_replay_start_lsn: u64,
    next_lsn: u64,
    durability: DurabilityPolicy,
    read_only: bool,
}

struct CheckpointImage<'a> {
    catalog: &'a Catalog,
    commit_epoch: u64,
    next_node_id: u64,
    next_rel_id: u64,
    nodes: &'a BTreeMap<NodeId, NodeRecord>,
    relationships: &'a BTreeMap<RelId, RelRecord>,
    projected_graphs: &'a BTreeMap<String, ProjectedGraphDefinition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableOpenMode {
    CreateIfMissing,
    ExistingOnly,
}

impl DurableStore {
    fn open(path: &Path, durability: DurabilityPolicy) -> Result<Self> {
        fs::create_dir_all(path)?;
        Self::open_existing(path, durability, false)
    }

    fn open_existing_only(path: &Path, durability: DurabilityPolicy) -> Result<Self> {
        if !path.exists() {
            return Err(SkeinError::Storage(format!(
                "read-only database path does not exist: {}",
                path.display()
            )));
        }
        if !path.is_dir() {
            return Err(SkeinError::Storage(format!(
                "read-only database path is not a directory: {}",
                path.display()
            )));
        }
        Self::open_existing(path, durability, true)
    }

    fn open_existing(path: &Path, durability: DurabilityPolicy, read_only: bool) -> Result<Self> {
        let manifest_path = path.join(MANIFEST_FILE);
        let manifest = DurableManifest::load(&manifest_path)?;
        Ok(Self {
            checkpoint_path: path.join(CHECKPOINT_FILE),
            manifest_path,
            projected_graphs_path: path.join(PROJECTED_GRAPHS_FILE),
            stable_id_mapping_path: path.join(STABLE_ID_MAPPING_FILE),
            wal_path: path.join(WAL_FILE),
            checkpoint_epoch: manifest.checkpoint_epoch,
            checkpoint_commit_epoch: manifest.checkpoint_commit_epoch,
            oldest_reader_commit_epoch: manifest.oldest_reader_commit_epoch,
            safe_reclaim_commit_epoch: manifest.safe_reclaim_commit_epoch,
            wal_replay_start_lsn: manifest.wal_replay_start_lsn,
            next_lsn: manifest.next_lsn,
            durability,
            read_only,
        })
    }

    fn append_create_node(
        &mut self,
        id: NodeId,
        label: &str,
        properties: &BTreeMap<String, Value>,
    ) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op: WalOp::CreateNode {
                id,
                label: label.to_string(),
                properties: properties.clone(),
            },
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;
        writeln!(file, "{}", entry.encode())?;
        self.finish_wal_append(&mut file)?;
        self.next_lsn += 1;
        Ok(())
    }

    fn append_create_relationship(
        &mut self,
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: &str,
        properties: &BTreeMap<String, Value>,
    ) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op: WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type: rel_type.to_string(),
                properties: properties.clone(),
            },
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;
        writeln!(file, "{}", entry.encode())?;
        self.finish_wal_append(&mut file)?;
        self.next_lsn += 1;
        Ok(())
    }

    fn append_batch(&mut self, ops: Vec<WalOp>) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op: WalOp::Batch(ops),
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;
        writeln!(file, "{}", entry.encode())?;
        self.finish_wal_append(&mut file)?;
        self.next_lsn += 1;
        Ok(())
    }

    fn append_project_graph(
        &mut self,
        name: &str,
        definition: &ProjectedGraphDefinition,
    ) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op: WalOp::ProjectGraph {
                name: name.to_string(),
                node_labels: definition.node_labels.clone(),
                rel_types: definition.rel_types.clone(),
            },
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;
        writeln!(file, "{}", entry.encode())?;
        self.finish_wal_append(&mut file)?;
        self.next_lsn += 1;
        Ok(())
    }

    fn finish_wal_append(&self, file: &mut File) -> Result<()> {
        file.flush()?;
        if self.durability == DurabilityPolicy::SyncOnEveryWrite {
            file.sync_data()?;
        }
        Ok(())
    }

    fn write_checkpoint(&self, image: CheckpointImage<'_>) -> Result<()> {
        let mut body = String::new();
        body.push_str("SKEIN_CHECKPOINT_V1\n");
        body.push_str(&format!("version\t{STORAGE_VERSION}\n"));
        body.push_str(&format!("commit_epoch\t{}\n", image.commit_epoch));
        body.push_str(&format!("next_node_id\t{}\n", image.next_node_id));
        body.push_str(&format!("next_rel_id\t{}\n", image.next_rel_id));
        for label in image.catalog.labels() {
            if !label.name.is_empty() {
                body.push_str(&format!(
                    "label\t{}\t{}\n",
                    label.id.0,
                    encode_string(&label.name)
                ));
            }
        }
        for rel_type in image.catalog.rel_types() {
            if !rel_type.name.is_empty() {
                body.push_str(&format!(
                    "rel_type\t{}\t{}\n",
                    rel_type.id.0,
                    encode_string(&rel_type.name)
                ));
            }
        }
        for index in image.catalog.property_indexes() {
            body.push_str(&format!(
                "property_index\t{}\t{}\t{}\t{}\n",
                index.id.0,
                index.label_id.0,
                encode_string(&index.property),
                encode_index_kind(index.kind)
            ));
        }
        for index in image.catalog.composite_property_indexes() {
            body.push_str(&format!(
                "composite_property_index\t{}\t{}\t{}\n",
                index.id.0,
                index.label_id.0,
                encode_string_vec(&index.properties)
            ));
        }
        for table in image.catalog.table_descriptors() {
            body.push_str(&format!(
                "table\t{}\t{}\t{}\t{}\n",
                table.id.0,
                encode_table_kind(table.kind),
                encode_string(&table.name),
                encode_schema_object_state(table.state)
            ));
        }
        for property in image.catalog.property_descriptors() {
            body.push_str(&format!(
                "property\t{}\t{}\t{}\t{}\t{}\t{}\n",
                property.id.0,
                property.table_id.0,
                encode_string(&property.name),
                encode_property_type(property.value_type),
                encode_nullable(property.nullable),
                encode_schema_object_state(property.state)
            ));
        }
        for constraint in image.catalog.unique_constraints() {
            let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
                continue;
            };
            body.push_str(&format!(
                "unique_constraint\t{}\t{}\t{}\n",
                constraint.id.0,
                label_id.0,
                encode_string(&constraint.property)
            ));
        }
        for constraint in image.catalog.node_property_exists_constraints() {
            let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
                continue;
            };
            body.push_str(&format!(
                "node_property_exists_constraint\t{}\t{}\t{}\n",
                constraint.id.0,
                label_id.0,
                encode_string(&constraint.property)
            ));
        }
        for constraint in image.catalog.relationship_property_exists_constraints() {
            let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject
            else {
                continue;
            };
            body.push_str(&format!(
                "relationship_property_exists_constraint\t{}\t{}\t{}\n",
                constraint.id.0,
                rel_type_id.0,
                encode_string(&constraint.property)
            ));
        }
        for constraint in image.catalog.relationship_unique_constraints() {
            let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject
            else {
                continue;
            };
            body.push_str(&format!(
                "relationship_unique_constraint\t{}\t{}\t{}\n",
                constraint.id.0,
                rel_type_id.0,
                encode_string(&constraint.property)
            ));
        }
        let statistics = compute_statistics(image.nodes, image.relationships, image.commit_epoch);
        body.push_str(&format!(
            "stat_commit_epoch\t{}\n",
            statistics.computed_at_commit_epoch
        ));
        body.push_str(&format!(
            "stat_histogram_sample_limit\t{}\n",
            statistics.histogram_sample_limit
        ));
        body.push_str(&format!("stat_node_count\t{}\n", statistics.node_count));
        body.push_str(&format!(
            "stat_relationship_count\t{}\n",
            statistics.relationship_count
        ));
        for (label_id, count) in &statistics.label_counts {
            body.push_str(&format!("stat_label_count\t{}\t{}\n", label_id.0, count));
        }
        for (rel_type_id, count) in &statistics.rel_type_counts {
            body.push_str(&format!(
                "stat_rel_type_count\t{}\t{}\n",
                rel_type_id.0, count
            ));
        }
        for (rel_type_id, count) in &statistics.rel_type_source_counts {
            body.push_str(&format!(
                "stat_rel_type_source_count\t{}\t{}\n",
                rel_type_id.0, count
            ));
        }
        for (rel_type_id, count) in &statistics.rel_type_target_counts {
            body.push_str(&format!(
                "stat_rel_type_target_count\t{}\t{}\n",
                rel_type_id.0, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id), count) in &statistics.path_counts {
            body.push_str(&format!(
                "stat_path_count\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id), count) in
            &statistics.path_source_distinct_counts
        {
            body.push_str(&format!(
                "stat_path_source_distinct_count\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id), count) in
            &statistics.path_target_distinct_counts
        {
            body.push_str(&format!(
                "stat_path_target_distinct_count\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id, hops), count) in
            &statistics.bounded_path_counts
        {
            body.push_str(&format!(
                "stat_bounded_path_count\t{}\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id, hops), count) in
            &statistics.bounded_path_source_distinct_counts
        {
            body.push_str(&format!(
                "stat_bounded_path_source_distinct_count\t{}\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id, hops), count) in
            &statistics.bounded_path_target_distinct_counts
        {
            body.push_str(&format!(
                "stat_bounded_path_target_distinct_count\t{}\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
            ));
        }
        for ((label_id, property), count) in &statistics.property_distinct_counts {
            body.push_str(&format!(
                "stat_property_distinct_count\t{}\t{}\t{}\n",
                label_id.0,
                encode_string(property),
                count
            ));
        }
        for ((rel_type_id, property), count) in &statistics.rel_property_distinct_counts {
            body.push_str(&format!(
                "stat_rel_property_distinct_count\t{}\t{}\t{}\n",
                rel_type_id.0,
                encode_string(property),
                count
            ));
        }
        for ((rel_type_id, property), values) in &statistics.rel_property_histograms {
            body.push_str(&format!(
                "stat_rel_property_histogram\t{}\t{}\t{}\n",
                rel_type_id.0,
                encode_string(property),
                encode_value_vec(values)
            ));
        }
        for ((rel_type_id, property), sampled) in &statistics.sampled_rel_property_histograms {
            body.push_str(&format!(
                "stat_rel_property_histogram_sampled\t{}\t{}\t{}\n",
                rel_type_id.0,
                encode_string(property),
                encode_bool(*sampled)
            ));
        }
        for ((label_id, property), values) in &statistics.property_histograms {
            body.push_str(&format!(
                "stat_property_histogram\t{}\t{}\t{}\n",
                label_id.0,
                encode_string(property),
                encode_value_vec(values)
            ));
        }
        for ((label_id, property), sampled) in &statistics.sampled_property_histograms {
            body.push_str(&format!(
                "stat_property_histogram_sampled\t{}\t{}\t{}\n",
                label_id.0,
                encode_string(property),
                encode_bool(*sampled)
            ));
        }
        for (name, definition) in image.projected_graphs {
            body.push_str(&format!(
                "project_graph\t{}\t{}\t{}\n",
                encode_string(name),
                encode_string_vec(&definition.node_labels),
                encode_string_vec(&definition.rel_types)
            ));
        }
        for node in image.nodes.values() {
            body.push_str(&format!(
                "node\t{}\t{}\t{}\n",
                node.id.0,
                encode_label_set(&node.labels),
                encode_properties(&node.properties)
            ));
        }
        for relationship in image.relationships.values() {
            body.push_str(&format!(
                "rel\t{}\t{}\t{}\t{}\t{}\n",
                relationship.id.0,
                relationship.source.0,
                relationship.target.0,
                relationship.rel_type.0,
                encode_properties(&relationship.properties)
            ));
        }
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = self.checkpoint_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            let encoded = encode_durable_text(&data, DurableCompression::default())?;
            file.write_all(&encoded)?;
            file.sync_all()?;
        }
        fs::rename(tmp_path, &self.checkpoint_path)?;
        sync_parent_dir(&self.checkpoint_path)?;
        Ok(())
    }

    fn write_projected_graph_artifacts(&self, body: &str) -> Result<()> {
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = self.projected_graphs_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            let encoded = encode_durable_text(&data, DurableCompression::default())?;
            file.write_all(&encoded)?;
            file.sync_all()?;
        }
        fs::rename(tmp_path, &self.projected_graphs_path)?;
        sync_parent_dir(&self.projected_graphs_path)?;
        Ok(())
    }

    fn load_projected_graph_artifacts(&self) -> Result<BTreeMap<String, ProjectedGraphArtifact>> {
        if !self.projected_graphs_path.exists() {
            return Ok(BTreeMap::new());
        }
        let text = read_durable_text(&self.projected_graphs_path, "projected graph artifact")?;
        let artifacts = split_projected_graph_artifact_checksum(&text)
            .and_then(|(body, checksum)| {
                let actual = checksum_bytes(body.as_bytes());
                if checksum != actual {
                    return Err(SkeinError::Storage(format!(
                        "projected graph artifact checksum mismatch: expected {checksum}, got {actual}"
                    )));
                }
                decode_projected_graph_artifacts(body).map(|(_, artifacts)| artifacts)
            });
        match artifacts {
            Ok(artifacts) => Ok(artifacts),
            Err(_) => {
                if !self.read_only {
                    fs::remove_file(&self.projected_graphs_path)?;
                    sync_parent_dir(&self.projected_graphs_path)?;
                }
                Ok(BTreeMap::new())
            }
        }
    }

    fn write_stable_id_mapping(&self, mapping: &StoreStableIdMapping) -> Result<()> {
        let body = encode_stable_id_mapping(mapping);
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = self.stable_id_mapping_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            let encoded = encode_durable_text(&data, DurableCompression::default())?;
            file.write_all(&encoded)?;
            file.sync_all()?;
        }
        fs::rename(tmp_path, &self.stable_id_mapping_path)?;
        sync_parent_dir(&self.stable_id_mapping_path)?;
        Ok(())
    }

    fn load_stable_id_mapping(&self) -> Result<StoreStableIdMapping> {
        if !self.stable_id_mapping_path.exists() {
            return Ok(StoreStableIdMapping::default());
        }
        let text = read_durable_text(&self.stable_id_mapping_path, "stable id mapping")?;
        let (body, checksum) = split_stable_id_mapping_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "stable id mapping checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        decode_stable_id_mapping(body)
    }

    fn truncate_wal(&mut self) -> Result<()> {
        File::create(&self.wal_path)?.sync_all()?;
        self.next_lsn = 1;
        Ok(())
    }

    fn publish_checkpoint_manifest(
        &mut self,
        checkpoint_commit_epoch: u64,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> Result<()> {
        self.checkpoint_epoch += 1;
        self.checkpoint_commit_epoch = checkpoint_commit_epoch;
        self.oldest_reader_commit_epoch = oldest_reader_commit_epoch;
        self.safe_reclaim_commit_epoch =
            safe_reclaim_commit_epoch(self.checkpoint_commit_epoch, oldest_reader_commit_epoch);
        self.wal_replay_start_lsn = self.next_lsn;
        DurableManifest {
            checkpoint_epoch: self.checkpoint_epoch,
            checkpoint_commit_epoch: self.checkpoint_commit_epoch,
            oldest_reader_commit_epoch,
            safe_reclaim_commit_epoch: self.safe_reclaim_commit_epoch,
            wal_replay_start_lsn: self.next_lsn,
            next_lsn: self.next_lsn,
        }
        .write(&self.manifest_path)
    }
}

#[derive(Debug, Clone, Copy)]
struct DurableManifest {
    checkpoint_epoch: u64,
    checkpoint_commit_epoch: u64,
    oldest_reader_commit_epoch: Option<u64>,
    safe_reclaim_commit_epoch: u64,
    wal_replay_start_lsn: u64,
    next_lsn: u64,
}

impl Default for DurableManifest {
    fn default() -> Self {
        Self {
            checkpoint_epoch: 0,
            checkpoint_commit_epoch: 0,
            oldest_reader_commit_epoch: None,
            safe_reclaim_commit_epoch: 0,
            wal_replay_start_lsn: 1,
            next_lsn: 1,
        }
    }
}

impl DurableManifest {
    fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = fs::read_to_string(path)?;
        let (body, checksum) = split_manifest_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "manifest checksum mismatch: expected {checksum}, got {actual}"
            )));
        }

        let mut manifest = Self::default();
        for line in body.lines() {
            if line == "SKEIN_MANIFEST_V1" {
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["version", version] => validate_storage_version(version)?,
                ["checkpoint_epoch", raw] => {
                    manifest.checkpoint_epoch = parse_u64(raw, "checkpoint epoch")?;
                }
                ["checkpoint_commit_epoch", raw] => {
                    manifest.checkpoint_commit_epoch = parse_u64(raw, "checkpoint commit epoch")?;
                }
                ["oldest_reader_commit_epoch", raw] => {
                    manifest.oldest_reader_commit_epoch =
                        parse_optional_u64(raw, "oldest reader commit epoch")?;
                }
                ["safe_reclaim_commit_epoch", raw] => {
                    manifest.safe_reclaim_commit_epoch =
                        parse_u64(raw, "safe reclaim commit epoch")?;
                }
                ["wal_replay_start_lsn", raw] => {
                    manifest.wal_replay_start_lsn = parse_u64(raw, "wal replay start lsn")?;
                }
                ["next_lsn", raw] => {
                    manifest.next_lsn = parse_u64(raw, "manifest next lsn")?;
                }
                [""] => {}
                _ => {
                    return Err(SkeinError::Storage(format!(
                        "invalid manifest line: {line}"
                    )));
                }
            }
        }
        if manifest.safe_reclaim_commit_epoch == 0 && manifest.checkpoint_commit_epoch > 0 {
            manifest.safe_reclaim_commit_epoch = safe_reclaim_commit_epoch(
                manifest.checkpoint_commit_epoch,
                manifest.oldest_reader_commit_epoch,
            );
        }
        Ok(manifest)
    }

    fn write(&self, path: &Path) -> Result<()> {
        let mut body = String::new();
        body.push_str("SKEIN_MANIFEST_V1\n");
        body.push_str(&format!("version\t{STORAGE_VERSION}\n"));
        body.push_str(&format!("checkpoint_epoch\t{}\n", self.checkpoint_epoch));
        body.push_str(&format!(
            "checkpoint_commit_epoch\t{}\n",
            self.checkpoint_commit_epoch
        ));
        body.push_str(&format!(
            "oldest_reader_commit_epoch\t{}\n",
            encode_optional_u64(self.oldest_reader_commit_epoch)
        ));
        body.push_str(&format!(
            "safe_reclaim_commit_epoch\t{}\n",
            self.safe_reclaim_commit_epoch
        ));
        body.push_str(&format!(
            "wal_replay_start_lsn\t{}\n",
            self.wal_replay_start_lsn
        ));
        body.push_str(&format!("next_lsn\t{}\n", self.next_lsn));
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(data.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(tmp_path, path)?;
        sync_parent_dir(path)?;
        Ok(())
    }
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn safe_reclaim_commit_epoch(
    checkpoint_commit_epoch: u64,
    oldest_reader_commit_epoch: Option<u64>,
) -> u64 {
    oldest_reader_commit_epoch
        .map(|epoch| epoch.saturating_sub(1))
        .unwrap_or(checkpoint_commit_epoch)
}

#[derive(Debug)]
struct WalEntry {
    lsn: u64,
    op: WalOp,
}

enum WalDecodeResult {
    Entry(WalEntry),
    TornTail(String),
}

#[derive(Debug, Clone)]
enum WalOp {
    CreateNodeLabel {
        label: String,
    },
    CreateRelationshipType {
        rel_type: String,
    },
    CreateNodeTable {
        name: String,
    },
    CreateRelationshipTable {
        name: String,
    },
    CreateProperty {
        table_kind: TableKind,
        table: String,
        property: String,
        value_type: PropertyType,
        nullable: bool,
    },
    AlterTableState {
        table_kind: TableKind,
        table: String,
        state: SchemaObjectState,
    },
    AlterPropertyState {
        table_kind: TableKind,
        table: String,
        property: String,
        state: SchemaObjectState,
    },
    GcTableDescriptor {
        table_kind: TableKind,
        table: String,
    },
    GcPropertyDescriptor {
        table_kind: TableKind,
        table: String,
        property: String,
    },
    CreateIndex {
        label: String,
        property: String,
    },
    CreateCompositeIndex {
        label: String,
        properties: Vec<String>,
    },
    CreateRangeIndex {
        label: String,
        property: String,
    },
    CreateFullTextIndex {
        label: String,
        property: String,
    },
    CreateUniqueConstraint {
        label: String,
        property: String,
    },
    CreateNodePropertyExistsConstraint {
        label: String,
        property: String,
    },
    CreateRelationshipUniqueConstraint {
        rel_type: String,
        property: String,
    },
    CreateRelationshipPropertyExistsConstraint {
        rel_type: String,
        property: String,
    },
    CreateNode {
        id: NodeId,
        label: String,
        properties: BTreeMap<String, Value>,
    },
    CreateRelationship {
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: String,
        properties: BTreeMap<String, Value>,
    },
    SetNodeProperty {
        id: NodeId,
        property: String,
        value: Value,
    },
    SetRelationshipProperty {
        id: RelId,
        property: String,
        value: Value,
    },
    DeleteNode {
        id: NodeId,
    },
    DeleteRelationship {
        id: RelId,
    },
    ProjectGraph {
        name: String,
        node_labels: Vec<String>,
        rel_types: Vec<String>,
    },
    Batch(Vec<WalOp>),
}

impl WalEntry {
    fn encode(&self) -> String {
        let payload = match &self.op {
            WalOp::CreateNodeLabel { label } => {
                format!("create_node_label\t{}", encode_string(label))
            }
            WalOp::CreateRelationshipType { rel_type } => {
                format!("create_rel_type\t{}", encode_string(rel_type))
            }
            WalOp::CreateNodeTable { name } => {
                format!("create_node_table\t{}", encode_string(name))
            }
            WalOp::CreateRelationshipTable { name } => {
                format!("create_rel_table\t{}", encode_string(name))
            }
            WalOp::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => format!(
                "create_property\t{}\t{}\t{}\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table),
                encode_string(property),
                encode_property_type(*value_type),
                encode_nullable(*nullable)
            ),
            WalOp::AlterTableState {
                table_kind,
                table,
                state,
            } => format!(
                "alter_table_state\t{}\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table),
                encode_schema_object_state(*state)
            ),
            WalOp::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => format!(
                "alter_property_state\t{}\t{}\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table),
                encode_string(property),
                encode_schema_object_state(*state)
            ),
            WalOp::GcTableDescriptor { table_kind, table } => format!(
                "gc_table_descriptor\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table)
            ),
            WalOp::GcPropertyDescriptor {
                table_kind,
                table,
                property,
            } => format!(
                "gc_property_descriptor\t{}\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table),
                encode_string(property)
            ),
            WalOp::CreateIndex { label, property } => format!(
                "create_index\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateCompositeIndex { label, properties } => format!(
                "create_composite_index\t{}\t{}",
                encode_string(label),
                encode_string_vec(properties)
            ),
            WalOp::CreateRangeIndex { label, property } => format!(
                "create_range_index\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateFullTextIndex { label, property } => format!(
                "create_fulltext_index\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateUniqueConstraint { label, property } => format!(
                "create_unique_constraint\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateNodePropertyExistsConstraint { label, property } => format!(
                "create_node_property_exists_constraint\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateRelationshipUniqueConstraint { rel_type, property } => format!(
                "create_relationship_unique_constraint\t{}\t{}",
                encode_string(rel_type),
                encode_string(property)
            ),
            WalOp::CreateRelationshipPropertyExistsConstraint { rel_type, property } => format!(
                "create_relationship_property_exists_constraint\t{}\t{}",
                encode_string(rel_type),
                encode_string(property)
            ),
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => format!(
                "create_node\t{}\t{}\t{}",
                id.0,
                encode_string(label),
                encode_properties(properties)
            ),
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => format!(
                "create_rel\t{}\t{}\t{}\t{}\t{}",
                id.0,
                source.0,
                target.0,
                encode_string(rel_type),
                encode_properties(properties)
            ),
            WalOp::SetNodeProperty {
                id,
                property,
                value,
            } => format!(
                "set_node_property\t{}\t{}\t{}",
                id.0,
                encode_string(property),
                encode_value(value)
            ),
            WalOp::SetRelationshipProperty {
                id,
                property,
                value,
            } => format!(
                "set_rel_property\t{}\t{}\t{}",
                id.0,
                encode_string(property),
                encode_value(value)
            ),
            WalOp::DeleteNode { id } => {
                format!("delete_node\t{}", id.0)
            }
            WalOp::DeleteRelationship { id } => {
                format!("delete_rel\t{}", id.0)
            }
            WalOp::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => format!(
                "project_graph\t{}\t{}\t{}",
                encode_string(name),
                encode_string_vec(node_labels),
                encode_string_vec(rel_types)
            ),
            WalOp::Batch(ops) => format!(
                "batch\t{}",
                ops.iter()
                    .map(encode_wal_op_for_batch)
                    .collect::<Vec<_>>()
                    .join("|")
            ),
        };
        let body = format!("{}\t{payload}", self.lsn);
        let checksum = checksum_bytes(body.as_bytes());
        format!("{body}\t{checksum}")
    }

    fn decode(line: &str) -> Result<WalDecodeResult> {
        let Some((body, raw_checksum)) = line.rsplit_once('\t') else {
            return Ok(WalDecodeResult::TornTail(
                "missing checksum field".to_string(),
            ));
        };
        let Ok(expected) = raw_checksum.parse::<u64>() else {
            return Ok(WalDecodeResult::TornTail(
                "invalid checksum field".to_string(),
            ));
        };
        let actual = checksum_bytes(body.as_bytes());
        if expected != actual {
            return Ok(WalDecodeResult::TornTail(format!(
                "checksum mismatch: expected {expected}, got {actual}"
            )));
        }
        let fields = body.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            [raw_lsn, "create_node_label", raw_label] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::CreateNodeLabel {
                    label: decode_string(raw_label)?,
                },
            })),
            [raw_lsn, "create_rel_type", raw_type] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::CreateRelationshipType {
                    rel_type: decode_string(raw_type)?,
                },
            })),
            [raw_lsn, "create_node_table", raw_name] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::CreateNodeTable {
                    name: decode_string(raw_name)?,
                },
            })),
            [raw_lsn, "create_rel_table", raw_name] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::CreateRelationshipTable {
                    name: decode_string(raw_name)?,
                },
            })),
            [raw_lsn, "create_property", raw_kind, raw_table, raw_property, raw_type, raw_nullable] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateProperty {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                        property: decode_string(raw_property)?,
                        value_type: decode_property_type(raw_type)?,
                        nullable: decode_nullable(raw_nullable)?,
                    },
                }))
            }
            [raw_lsn, "alter_table_state", raw_kind, raw_table, raw_state] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::AlterTableState {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                        state: decode_schema_object_state(raw_state)?,
                    },
                }))
            }
            [raw_lsn, "alter_property_state", raw_kind, raw_table, raw_property, raw_state] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::AlterPropertyState {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                        property: decode_string(raw_property)?,
                        state: decode_schema_object_state(raw_state)?,
                    },
                }))
            }
            [raw_lsn, "gc_table_descriptor", raw_kind, raw_table] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::GcTableDescriptor {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                    },
                }))
            }
            [raw_lsn, "gc_property_descriptor", raw_kind, raw_table, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::GcPropertyDescriptor {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_index", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateIndex {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_composite_index", raw_label, raw_properties] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateCompositeIndex {
                        label: decode_string(raw_label)?,
                        properties: decode_string_vec(raw_properties)?,
                    },
                }))
            }
            [raw_lsn, "create_range_index", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateRangeIndex {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_fulltext_index", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateFullTextIndex {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_unique_constraint", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateUniqueConstraint {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_node_property_exists_constraint", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateNodePropertyExistsConstraint {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_relationship_unique_constraint", raw_rel_type, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateRelationshipUniqueConstraint {
                        rel_type: decode_string(raw_rel_type)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_relationship_property_exists_constraint", raw_rel_type, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateRelationshipPropertyExistsConstraint {
                        rel_type: decode_string(raw_rel_type)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_node", raw_id, raw_label, raw_properties] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateNode {
                        id: NodeId(parse_u64(raw_id, "wal node id")?),
                        label: decode_string(raw_label)?,
                        properties: decode_properties(raw_properties)?,
                    },
                }))
            }
            [raw_lsn, "create_rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateRelationship {
                        id: RelId(parse_u64(raw_id, "wal rel id")?),
                        source: NodeId(parse_u64(raw_source, "wal rel source")?),
                        target: NodeId(parse_u64(raw_target, "wal rel target")?),
                        rel_type: decode_string(raw_type)?,
                        properties: decode_properties(raw_properties)?,
                    },
                }))
            }
            [raw_lsn, "set_node_property", raw_id, raw_property, raw_value] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::SetNodeProperty {
                        id: NodeId(parse_u64(raw_id, "wal node id")?),
                        property: decode_string(raw_property)?,
                        value: decode_value(raw_value)?,
                    },
                }))
            }
            [raw_lsn, "set_rel_property", raw_id, raw_property, raw_value] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::SetRelationshipProperty {
                        id: RelId(parse_u64(raw_id, "wal rel id")?),
                        property: decode_string(raw_property)?,
                        value: decode_value(raw_value)?,
                    },
                }))
            }
            [raw_lsn, "delete_node", raw_id] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::DeleteNode {
                    id: NodeId(parse_u64(raw_id, "wal node id")?),
                },
            })),
            [raw_lsn, "delete_rel", raw_id] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::DeleteRelationship {
                    id: RelId(parse_u64(raw_id, "wal rel id")?),
                },
            })),
            [raw_lsn, "project_graph", raw_name, raw_node_labels, raw_rel_types] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::ProjectGraph {
                        name: decode_string(raw_name)?,
                        node_labels: decode_string_vec(raw_node_labels)?,
                        rel_types: decode_string_vec(raw_rel_types)?,
                    },
                }))
            }
            [raw_lsn, "batch", raw_ops] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::Batch(decode_wal_batch(raw_ops)?),
            })),
            _ => Err(SkeinError::Storage(format!("invalid wal entry: {line}"))),
        }
    }
}

fn encode_wal_op_for_batch(op: &WalOp) -> String {
    match op {
        WalOp::CreateNodeLabel { label } => {
            format!("create_node_label,{}", encode_string(label))
        }
        WalOp::CreateRelationshipType { rel_type } => {
            format!("create_rel_type,{}", encode_string(rel_type))
        }
        WalOp::CreateNodeTable { name } => {
            format!("create_node_table,{}", encode_string(name))
        }
        WalOp::CreateRelationshipTable { name } => {
            format!("create_rel_table,{}", encode_string(name))
        }
        WalOp::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => format!(
            "create_property,{},{},{},{},{}",
            encode_table_kind(*table_kind),
            encode_string(table),
            encode_string(property),
            encode_property_type(*value_type),
            encode_nullable(*nullable)
        ),
        WalOp::AlterTableState {
            table_kind,
            table,
            state,
        } => format!(
            "alter_table_state,{},{},{}",
            encode_table_kind(*table_kind),
            encode_string(table),
            encode_schema_object_state(*state)
        ),
        WalOp::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => format!(
            "alter_property_state,{},{},{},{}",
            encode_table_kind(*table_kind),
            encode_string(table),
            encode_string(property),
            encode_schema_object_state(*state)
        ),
        WalOp::GcTableDescriptor { table_kind, table } => {
            format!(
                "gc_table_descriptor,{},{}",
                encode_table_kind(*table_kind),
                encode_string(table)
            )
        }
        WalOp::GcPropertyDescriptor {
            table_kind,
            table,
            property,
        } => format!(
            "gc_property_descriptor,{},{},{}",
            encode_table_kind(*table_kind),
            encode_string(table),
            encode_string(property)
        ),
        WalOp::CreateIndex { label, property } => {
            format!(
                "create_index,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateCompositeIndex { label, properties } => {
            format!(
                "create_composite_index,{},{}",
                encode_string(label),
                encode_string_vec(properties)
            )
        }
        WalOp::CreateRangeIndex { label, property } => {
            format!(
                "create_range_index,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateFullTextIndex { label, property } => {
            format!(
                "create_fulltext_index,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateUniqueConstraint { label, property } => {
            format!(
                "create_unique_constraint,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateNodePropertyExistsConstraint { label, property } => {
            format!(
                "create_node_property_exists_constraint,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateRelationshipUniqueConstraint { rel_type, property } => {
            format!(
                "create_relationship_unique_constraint,{},{}",
                encode_string(rel_type),
                encode_string(property)
            )
        }
        WalOp::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            format!(
                "create_relationship_property_exists_constraint,{},{}",
                encode_string(rel_type),
                encode_string(property)
            )
        }
        WalOp::CreateNode {
            id,
            label,
            properties,
        } => format!(
            "create_node,{},{},{}",
            id.0,
            encode_string(label),
            encode_properties(properties)
        ),
        WalOp::CreateRelationship {
            id,
            source,
            target,
            rel_type,
            properties,
        } => format!(
            "create_rel,{},{},{},{},{}",
            id.0,
            source.0,
            target.0,
            encode_string(rel_type),
            encode_properties(properties)
        ),
        WalOp::SetNodeProperty {
            id,
            property,
            value,
        } => format!(
            "set_node_property,{},{},{}",
            id.0,
            encode_string(property),
            encode_value(value)
        ),
        WalOp::SetRelationshipProperty {
            id,
            property,
            value,
        } => format!(
            "set_rel_property,{},{},{}",
            id.0,
            encode_string(property),
            encode_value(value)
        ),
        WalOp::DeleteNode { id } => {
            format!("delete_node,{}", id.0)
        }
        WalOp::DeleteRelationship { id } => {
            format!("delete_rel,{}", id.0)
        }
        WalOp::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => format!(
            "project_graph,{},{},{}",
            encode_string(name),
            encode_string_vec(node_labels),
            encode_string_vec(rel_types)
        ),
        WalOp::Batch(_) => unreachable!("nested wal batches are not encoded"),
    }
}

fn decode_wal_batch(input: &str) -> Result<Vec<WalOp>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input.split('|').map(decode_wal_op_from_batch).collect()
}

fn decode_wal_op_from_batch(input: &str) -> Result<WalOp> {
    let fields = input.split(',').collect::<Vec<_>>();
    match fields.as_slice() {
        ["create_node_label", raw_label] => Ok(WalOp::CreateNodeLabel {
            label: decode_string(raw_label)?,
        }),
        ["create_rel_type", raw_type] => Ok(WalOp::CreateRelationshipType {
            rel_type: decode_string(raw_type)?,
        }),
        ["create_node_table", raw_name] => Ok(WalOp::CreateNodeTable {
            name: decode_string(raw_name)?,
        }),
        ["create_rel_table", raw_name] => Ok(WalOp::CreateRelationshipTable {
            name: decode_string(raw_name)?,
        }),
        ["create_property", raw_kind, raw_table, raw_property, raw_type, raw_nullable] => {
            Ok(WalOp::CreateProperty {
                table_kind: decode_table_kind(raw_kind)?,
                table: decode_string(raw_table)?,
                property: decode_string(raw_property)?,
                value_type: decode_property_type(raw_type)?,
                nullable: decode_nullable(raw_nullable)?,
            })
        }
        ["alter_table_state", raw_kind, raw_table, raw_state] => Ok(WalOp::AlterTableState {
            table_kind: decode_table_kind(raw_kind)?,
            table: decode_string(raw_table)?,
            state: decode_schema_object_state(raw_state)?,
        }),
        ["alter_property_state", raw_kind, raw_table, raw_property, raw_state] => {
            Ok(WalOp::AlterPropertyState {
                table_kind: decode_table_kind(raw_kind)?,
                table: decode_string(raw_table)?,
                property: decode_string(raw_property)?,
                state: decode_schema_object_state(raw_state)?,
            })
        }
        ["gc_table_descriptor", raw_kind, raw_table] => Ok(WalOp::GcTableDescriptor {
            table_kind: decode_table_kind(raw_kind)?,
            table: decode_string(raw_table)?,
        }),
        ["gc_property_descriptor", raw_kind, raw_table, raw_property] => {
            Ok(WalOp::GcPropertyDescriptor {
                table_kind: decode_table_kind(raw_kind)?,
                table: decode_string(raw_table)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_index", raw_label, raw_property] => Ok(WalOp::CreateIndex {
            label: decode_string(raw_label)?,
            property: decode_string(raw_property)?,
        }),
        ["create_composite_index", raw_label, raw_properties] => Ok(WalOp::CreateCompositeIndex {
            label: decode_string(raw_label)?,
            properties: decode_string_vec(raw_properties)?,
        }),
        ["create_range_index", raw_label, raw_property] => Ok(WalOp::CreateRangeIndex {
            label: decode_string(raw_label)?,
            property: decode_string(raw_property)?,
        }),
        ["create_fulltext_index", raw_label, raw_property] => Ok(WalOp::CreateFullTextIndex {
            label: decode_string(raw_label)?,
            property: decode_string(raw_property)?,
        }),
        ["create_unique_constraint", raw_label, raw_property] => {
            Ok(WalOp::CreateUniqueConstraint {
                label: decode_string(raw_label)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_node_property_exists_constraint", raw_label, raw_property] => {
            Ok(WalOp::CreateNodePropertyExistsConstraint {
                label: decode_string(raw_label)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_relationship_unique_constraint", raw_rel_type, raw_property] => {
            Ok(WalOp::CreateRelationshipUniqueConstraint {
                rel_type: decode_string(raw_rel_type)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_relationship_property_exists_constraint", raw_rel_type, raw_property] => {
            Ok(WalOp::CreateRelationshipPropertyExistsConstraint {
                rel_type: decode_string(raw_rel_type)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_node", raw_id, raw_label, raw_properties] => Ok(WalOp::CreateNode {
            id: NodeId(parse_u64(raw_id, "batch node id")?),
            label: decode_string(raw_label)?,
            properties: decode_properties(raw_properties)?,
        }),
        ["create_rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
            Ok(WalOp::CreateRelationship {
                id: RelId(parse_u64(raw_id, "batch rel id")?),
                source: NodeId(parse_u64(raw_source, "batch rel source")?),
                target: NodeId(parse_u64(raw_target, "batch rel target")?),
                rel_type: decode_string(raw_type)?,
                properties: decode_properties(raw_properties)?,
            })
        }
        ["set_node_property", raw_id, raw_property, raw_value] => Ok(WalOp::SetNodeProperty {
            id: NodeId(parse_u64(raw_id, "batch node id")?),
            property: decode_string(raw_property)?,
            value: decode_value(raw_value)?,
        }),
        ["set_rel_property", raw_id, raw_property, raw_value] => {
            Ok(WalOp::SetRelationshipProperty {
                id: RelId(parse_u64(raw_id, "batch rel id")?),
                property: decode_string(raw_property)?,
                value: decode_value(raw_value)?,
            })
        }
        ["delete_node", raw_id] => Ok(WalOp::DeleteNode {
            id: NodeId(parse_u64(raw_id, "batch node id")?),
        }),
        ["delete_rel", raw_id] => Ok(WalOp::DeleteRelationship {
            id: RelId(parse_u64(raw_id, "batch rel id")?),
        }),
        ["project_graph", raw_name, raw_node_labels, raw_rel_types] => Ok(WalOp::ProjectGraph {
            name: decode_string(raw_name)?,
            node_labels: decode_string_vec(raw_node_labels)?,
            rel_types: decode_string_vec(raw_rel_types)?,
        }),
        _ => Err(SkeinError::Storage(format!(
            "invalid batch wal op: {input}"
        ))),
    }
}

fn register_property_index_descriptors(
    catalog: &mut Catalog,
    label_ids: impl IntoIterator<Item = LabelId>,
    properties: &BTreeMap<String, Value>,
) {
    for label_id in label_ids {
        for property in properties.keys() {
            catalog.get_or_create_property_index(label_id, property);
        }
    }
}

fn composite_property_index_key(
    node: &NodeRecord,
    properties: &[String],
) -> Option<Vec<(String, Value)>> {
    properties
        .iter()
        .map(|property| {
            node.properties
                .get(property)
                .cloned()
                .map(|value| (property.clone(), value))
        })
        .collect()
}

fn full_text_index_tokens(value: &str) -> BTreeSet<String> {
    let normalized = value.to_lowercase();
    let chars = normalized.chars().collect::<Vec<_>>();
    let mut tokens = BTreeSet::new();
    for start in 0..chars.len() {
        for width in 1..=3 {
            let end = start + width;
            if end > chars.len() {
                break;
            }
            let token = chars[start..end].iter().collect::<String>();
            if !token.chars().all(char::is_whitespace) {
                tokens.insert(token);
            }
        }
    }
    tokens
}

fn full_text_query_tokens(query: &str) -> Vec<String> {
    full_text_index_tokens(query).into_iter().collect()
}

fn ensure_table_descriptor(catalog: &mut Catalog, kind: TableKind, name: &str) -> TableId {
    match kind {
        TableKind::Node => {
            catalog.get_or_create_label(name);
        }
        TableKind::Relationship => {
            catalog.get_or_create_rel_type(name);
        }
    }
    catalog.get_or_create_table(kind, name)
}

fn validate_property_descriptor(
    catalog: &Catalog,
    store: &GraphStore,
    table_id: TableId,
    property: &str,
    value_type: PropertyType,
    nullable: bool,
) -> Result<()> {
    validate_property_descriptor_with_table_state(
        catalog, store, table_id, property, value_type, nullable, false,
    )
}

fn validate_property_descriptor_with_table_state(
    catalog: &Catalog,
    store: &GraphStore,
    table_id: TableId,
    property: &str,
    value_type: PropertyType,
    nullable: bool,
    force: bool,
) -> Result<()> {
    let Some(table) = catalog.table_descriptor(table_id) else {
        return Err(SkeinError::Storage(format!(
            "property schema references missing table {}",
            table_id.0
        )));
    };
    if !force && table.state != SchemaObjectState::Public {
        return Ok(());
    }
    match table.kind {
        TableKind::Node => {
            let Some(label_id) = catalog.label_id(&table.name) else {
                return Ok(());
            };
            for node in store.nodes.values() {
                if node.labels.contains(&label_id) {
                    validate_property_schema_value(
                        &table.name,
                        property,
                        value_type,
                        nullable,
                        node.properties.get(property),
                        &format!("node {}", node.id.0),
                    )?;
                }
            }
        }
        TableKind::Relationship => {
            let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                return Ok(());
            };
            for relationship in store.relationships.values() {
                if relationship.rel_type == rel_type_id {
                    validate_property_schema_value(
                        &table.name,
                        property,
                        value_type,
                        nullable,
                        relationship.properties.get(property),
                        &format!("relationship {}", relationship.id.0),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn reserve_schema_maintenance_budget(
    used_estimated_operations: &mut usize,
    max_estimated_operations: Option<usize>,
    estimated_operations: usize,
) -> bool {
    let Some(max_estimated_operations) = max_estimated_operations else {
        return true;
    };
    let next = used_estimated_operations.saturating_add(estimated_operations);
    if next > max_estimated_operations {
        return false;
    }
    *used_estimated_operations = next;
    true
}

fn validate_table_descriptor(
    catalog: &Catalog,
    store: &GraphStore,
    table_id: TableId,
) -> Result<()> {
    for property in catalog
        .property_descriptors()
        .filter(|property| property.table_id == table_id)
    {
        if property.state == SchemaObjectState::Gc {
            continue;
        }
        validate_property_descriptor_with_table_state(
            catalog,
            store,
            table_id,
            &property.name,
            property.value_type,
            property.nullable,
            true,
        )?;
    }
    Ok(())
}

fn apply_wal_op_to_snapshot(
    catalog: &Catalog,
    nodes: &mut BTreeMap<NodeId, NodeRecord>,
    relationships: &mut BTreeMap<RelId, RelRecord>,
    op: &WalOp,
) {
    match op {
        WalOp::CreateNode {
            id,
            label,
            properties,
        } => {
            if let Some(label_id) = catalog.label_id(label) {
                nodes.insert(
                    *id,
                    NodeRecord {
                        id: *id,
                        labels: BTreeSet::from([label_id]),
                        properties: properties.clone(),
                    },
                );
            }
        }
        WalOp::SetNodeProperty {
            id,
            property,
            value,
        } => {
            if let Some(node) = nodes.get_mut(id) {
                node.properties.insert(property.clone(), value.clone());
            }
        }
        WalOp::SetRelationshipProperty {
            id,
            property,
            value,
        } => {
            if let Some(relationship) = relationships.get_mut(id) {
                relationship
                    .properties
                    .insert(property.clone(), value.clone());
            }
        }
        WalOp::DeleteNode { id } => {
            nodes.remove(id);
        }
        WalOp::CreateRelationship {
            id,
            source,
            target,
            rel_type,
            properties,
        } => {
            if let Some(rel_type_id) = catalog.rel_type_id(rel_type) {
                relationships.insert(
                    *id,
                    RelRecord {
                        id: *id,
                        source: *source,
                        target: *target,
                        rel_type: rel_type_id,
                        properties: properties.clone(),
                    },
                );
            }
        }
        WalOp::DeleteRelationship { id } => {
            relationships.remove(id);
        }
        WalOp::Batch(ops) => {
            for op in ops {
                apply_wal_op_to_snapshot(catalog, nodes, relationships, op);
            }
        }
        WalOp::CreateNodeLabel { .. }
        | WalOp::CreateRelationshipType { .. }
        | WalOp::CreateNodeTable { .. }
        | WalOp::CreateRelationshipTable { .. }
        | WalOp::CreateProperty { .. }
        | WalOp::AlterTableState { .. }
        | WalOp::AlterPropertyState { .. }
        | WalOp::GcTableDescriptor { .. }
        | WalOp::GcPropertyDescriptor { .. }
        | WalOp::CreateIndex { .. }
        | WalOp::CreateCompositeIndex { .. }
        | WalOp::CreateRangeIndex { .. }
        | WalOp::CreateFullTextIndex { .. }
        | WalOp::CreateUniqueConstraint { .. }
        | WalOp::CreateNodePropertyExistsConstraint { .. }
        | WalOp::CreateRelationshipUniqueConstraint { .. }
        | WalOp::CreateRelationshipPropertyExistsConstraint { .. }
        | WalOp::ProjectGraph { .. } => {}
    }
}

fn encode_projected_graph_artifacts(
    catalog: &Catalog,
    store: &GraphStore,
    projection_epoch: u64,
) -> String {
    let mut body = String::new();
    body.push_str("SKEIN_PROJECTED_GRAPHS_V1\n");
    body.push_str(&format!(
        "artifact_version\t{PROJECTED_GRAPH_ARTIFACT_VERSION}\n"
    ));
    body.push_str(&format!("projection_epoch\t{projection_epoch}\n"));
    body.push_str(&format!("commit_epoch\t{}\n", store.commit_epoch));
    for (name, definition) in &store.projected_graphs {
        let graph = projected_graph_from_definition(catalog, store, definition);
        body.push_str(&format!(
            "graph\t{}\t{}\t{}\t{}\t{}\n",
            encode_string(name),
            encode_string_vec(&definition.node_labels),
            encode_string_vec(&definition.rel_types),
            graph.node_count(),
            graph.edge_count()
        ));
        body.push_str(&format!(
            "nodes\t{}\n",
            encode_u64_vec(graph.nodes().iter().map(|node| node.0))
        ));
        body.push_str(&format!(
            "csr_offsets\t{}\n",
            encode_usize_vec(graph.csr_offsets().iter().copied())
        ));
        body.push_str(&format!(
            "csr_targets\t{}\n",
            encode_usize_vec(graph.csr_targets().iter().copied())
        ));
        body.push_str(&format!(
            "csc_offsets\t{}\n",
            encode_usize_vec(graph.csc_offsets().iter().copied())
        ));
        body.push_str(&format!(
            "csc_sources\t{}\n",
            encode_usize_vec(graph.csc_sources().iter().copied())
        ));
    }
    body
}

fn projected_graph_from_definition(
    catalog: &Catalog,
    store: &GraphStore,
    definition: &ProjectedGraphDefinition,
) -> ProjectedGraph {
    if definition.node_labels.is_empty() && definition.rel_types.is_empty() {
        return ProjectedGraph::from_store(store, None);
    }
    let label_ids = definition
        .node_labels
        .iter()
        .filter_map(|label| catalog.label_id(label))
        .collect::<Vec<_>>();
    if !definition.node_labels.is_empty() && label_ids.is_empty() {
        return ProjectedGraph::empty();
    }
    let rel_type_ids = definition
        .rel_types
        .iter()
        .filter_map(|rel_type| catalog.rel_type_id(rel_type))
        .collect::<Vec<_>>();
    if !definition.rel_types.is_empty() && rel_type_ids.is_empty() {
        if label_ids.is_empty() {
            return ProjectedGraph::from_store_without_edges(store);
        }
        return ProjectedGraph::from_store_labels_without_edges(store, &label_ids);
    }
    ProjectedGraph::from_store_labels_and_rel_types(store, &label_ids, &rel_type_ids)
}

fn decode_projected_graph_artifacts(
    body: &str,
) -> Result<(u64, BTreeMap<String, ProjectedGraphArtifact>)> {
    let mut lines = body.lines();
    match lines.next() {
        Some("SKEIN_PROJECTED_GRAPHS_V1") => {}
        _ => {
            return Err(SkeinError::Storage(
                "invalid projected graph artifact header".to_string(),
            ));
        }
    }
    let artifact_version = decode_projected_graph_u64_header(
        lines.next(),
        "artifact_version",
        "projected graph artifact version",
    )?;
    if artifact_version != PROJECTED_GRAPH_ARTIFACT_VERSION {
        return Err(SkeinError::Storage(format!(
            "unsupported projected graph artifact version: {artifact_version}"
        )));
    }
    let projection_epoch = decode_projected_graph_u64_header(
        lines.next(),
        "projection_epoch",
        "projected graph artifact projection epoch",
    )?;
    let commit_epoch = decode_projected_graph_u64_header(
        lines.next(),
        "commit_epoch",
        "projected graph artifact commit epoch",
    )?;

    let mut artifacts = BTreeMap::new();
    while let Some(line) = lines.next() {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["graph", raw_name, raw_node_labels, raw_rel_types, raw_node_count, raw_edge_count] => {
                let name = decode_string(raw_name)?;
                let definition = ProjectedGraphDefinition {
                    node_labels: decode_string_vec(raw_node_labels)?,
                    rel_types: decode_string_vec(raw_rel_types)?,
                };
                let node_count = parse_u64(raw_node_count, "projected graph artifact node count")?;
                let edge_count = parse_u64(raw_edge_count, "projected graph artifact edge count")?;
                let nodes = decode_projected_graph_nodes_line(lines.next())?;
                let csr_offsets = decode_projected_graph_usize_line(lines.next(), "csr_offsets")?;
                let csr_targets = decode_projected_graph_usize_line(lines.next(), "csr_targets")?;
                let csc_offsets = decode_projected_graph_usize_line(lines.next(), "csc_offsets")?;
                let csc_sources = decode_projected_graph_usize_line(lines.next(), "csc_sources")?;
                if nodes.len() as u64 != node_count {
                    return Err(SkeinError::Storage(format!(
                        "projected graph artifact node count mismatch for {name}"
                    )));
                }
                if csr_targets.len() as u64 != edge_count || csc_sources.len() as u64 != edge_count
                {
                    return Err(SkeinError::Storage(format!(
                        "projected graph artifact edge count mismatch for {name}"
                    )));
                }
                let graph = ProjectedGraph::from_parts(
                    nodes,
                    csr_offsets,
                    csr_targets,
                    csc_offsets,
                    csc_sources,
                )
                .map_err(SkeinError::Storage)?;
                artifacts.insert(
                    name,
                    ProjectedGraphArtifact {
                        projection_epoch,
                        commit_epoch,
                        definition,
                        graph,
                    },
                );
            }
            [""] => {}
            _ => {
                return Err(SkeinError::Storage(format!(
                    "invalid projected graph artifact line: {line}"
                )));
            }
        }
    }
    Ok((commit_epoch, artifacts))
}

fn decode_projected_graph_u64_header(
    line: Option<&str>,
    expected: &str,
    name: &str,
) -> Result<u64> {
    let Some(line) = line else {
        return Err(SkeinError::Storage(format!(
            "missing projected graph artifact {expected}"
        )));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        [field, raw] if *field == expected => parse_u64(raw, name),
        _ => Err(SkeinError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn decode_projected_graph_nodes_line(line: Option<&str>) -> Result<Vec<NodeId>> {
    let Some(line) = line else {
        return Err(SkeinError::Storage(
            "missing projected graph artifact nodes line".to_string(),
        ));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        ["nodes", raw_values] => decode_u64_vec(raw_values, "projected graph artifact node id")
            .map(|nodes| nodes.into_iter().map(NodeId).collect()),
        _ => Err(SkeinError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn decode_projected_graph_usize_line(line: Option<&str>, expected: &str) -> Result<Vec<usize>> {
    let Some(line) = line else {
        return Err(SkeinError::Storage(format!(
            "missing projected graph artifact {expected} line"
        )));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        [name, raw_values] if *name == expected => {
            decode_usize_vec(raw_values, "projected graph artifact index")
        }
        _ => Err(SkeinError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn validate_property_schemas(
    catalog: &Catalog,
    nodes: &BTreeMap<NodeId, NodeRecord>,
    relationships: &BTreeMap<RelId, RelRecord>,
) -> Result<()> {
    for property in catalog.property_descriptors() {
        if property.state != SchemaObjectState::Public {
            continue;
        }
        let Some(table) = catalog.table_descriptor(property.table_id) else {
            continue;
        };
        if table.state != SchemaObjectState::Public {
            continue;
        }
        match table.kind {
            TableKind::Node => {
                let Some(label_id) = catalog.label_id(&table.name) else {
                    continue;
                };
                for node in nodes.values() {
                    if node.labels.contains(&label_id) {
                        validate_property_schema_value(
                            &table.name,
                            &property.name,
                            property.value_type,
                            property.nullable,
                            node.properties.get(&property.name),
                            &format!("node {}", node.id.0),
                        )?;
                    }
                }
            }
            TableKind::Relationship => {
                let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                    continue;
                };
                for relationship in relationships.values() {
                    if relationship.rel_type == rel_type_id {
                        validate_property_schema_value(
                            &table.name,
                            &property.name,
                            property.value_type,
                            property.nullable,
                            relationship.properties.get(&property.name),
                            &format!("relationship {}", relationship.id.0),
                        )?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_property_schema_value(
    table: &str,
    property: &str,
    value_type: PropertyType,
    nullable: bool,
    value: Option<&Value>,
    record: &str,
) -> Result<()> {
    let Some(value) = value else {
        if nullable {
            return Ok(());
        }
        return Err(property_schema_error(
            table,
            property,
            record,
            "property is not nullable",
        ));
    };
    if value == &Value::Null {
        if nullable {
            return Ok(());
        }
        return Err(property_schema_error(
            table,
            property,
            record,
            "property is not nullable",
        ));
    }
    let matches = matches!(
        (value_type, value),
        (PropertyType::Any, _)
            | (PropertyType::Bool, Value::Bool(_))
            | (PropertyType::Int, Value::Int(_))
            | (PropertyType::Float, Value::Float(_))
            | (PropertyType::String, Value::String(_))
            | (PropertyType::List, Value::List(_))
    );
    if matches {
        Ok(())
    } else {
        Err(property_schema_error(
            table,
            property,
            record,
            &format!("expected {}", encode_property_type(value_type)),
        ))
    }
}

fn property_schema_error(table: &str, property: &str, record: &str, reason: &str) -> SkeinError {
    SkeinError::Storage(format!(
        "property schema violation on {record} in {table}({property}): {reason}"
    ))
}

fn validate_unique_constraints(
    catalog: &Catalog,
    nodes: &BTreeMap<NodeId, NodeRecord>,
) -> Result<()> {
    for constraint in catalog.unique_constraints() {
        let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        validate_unique_property(catalog, nodes, label_id, &constraint.property)?;
    }
    Ok(())
}

fn validate_relationship_unique_constraints(
    catalog: &Catalog,
    relationships: &BTreeMap<RelId, RelRecord>,
) -> Result<()> {
    for constraint in catalog.relationship_unique_constraints() {
        let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        validate_unique_relationship_property(
            catalog,
            relationships,
            rel_type_id,
            &constraint.property,
        )?;
    }
    Ok(())
}

fn validate_node_property_exists_constraints(
    catalog: &Catalog,
    nodes: &BTreeMap<NodeId, NodeRecord>,
) -> Result<()> {
    for constraint in catalog.node_property_exists_constraints() {
        let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        validate_node_property_exists(catalog, nodes, label_id, &constraint.property)?;
    }
    Ok(())
}

fn validate_relationship_property_exists_constraints(
    catalog: &Catalog,
    relationships: &BTreeMap<RelId, RelRecord>,
) -> Result<()> {
    for constraint in catalog.relationship_property_exists_constraints() {
        let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        validate_relationship_property_exists(
            catalog,
            relationships,
            rel_type_id,
            &constraint.property,
        )?;
    }
    Ok(())
}

fn validate_node_property_exists(
    catalog: &Catalog,
    nodes: &BTreeMap<NodeId, NodeRecord>,
    label_id: LabelId,
    property: &str,
) -> Result<()> {
    for node in nodes.values() {
        if !node.labels.contains(&label_id) {
            continue;
        }
        match node.properties.get(property) {
            Some(value) if value != &Value::Null => {}
            _ => {
                let label = catalog.label_name(label_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "node property exists constraint violation on :{label}({property}) for node {}",
                    node.id.0
                )));
            }
        }
    }
    Ok(())
}

fn validate_relationship_property_exists(
    catalog: &Catalog,
    relationships: &BTreeMap<RelId, RelRecord>,
    rel_type_id: RelTypeId,
    property: &str,
) -> Result<()> {
    for relationship in relationships.values() {
        if relationship.rel_type != rel_type_id {
            continue;
        }
        match relationship.properties.get(property) {
            Some(value) if value != &Value::Null => {}
            _ => {
                let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "relationship property exists constraint violation on :{rel_type}({property}) for relationship {}",
                    relationship.id.0
                )));
            }
        }
    }
    Ok(())
}

fn validate_unique_property(
    catalog: &Catalog,
    nodes: &BTreeMap<NodeId, NodeRecord>,
    label_id: LabelId,
    property: &str,
) -> Result<()> {
    let mut seen = BTreeMap::<Value, NodeId>::new();
    for node in nodes.values() {
        if !node.labels.contains(&label_id) {
            continue;
        }
        let Some(value) = node.properties.get(property) else {
            continue;
        };
        if value == &Value::Null {
            continue;
        }
        if let Some(previous) = seen.insert(value.clone(), node.id) {
            let label = catalog.label_name(label_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "unique constraint violation on :{label}({property}) for nodes {} and {}",
                previous.0, node.id.0
            )));
        }
    }
    Ok(())
}

fn validate_unique_relationship_property(
    catalog: &Catalog,
    relationships: &BTreeMap<RelId, RelRecord>,
    rel_type_id: RelTypeId,
    property: &str,
) -> Result<()> {
    let mut seen = BTreeMap::<Value, RelId>::new();
    for relationship in relationships.values() {
        if relationship.rel_type != rel_type_id {
            continue;
        }
        let Some(value) = relationship.properties.get(property) else {
            continue;
        };
        if value == &Value::Null {
            continue;
        }
        if let Some(previous) = seen.insert(value.clone(), relationship.id) {
            let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "relationship unique constraint violation on :{rel_type}({property}) for relationships {} and {}",
                previous.0, relationship.id.0
            )));
        }
    }
    Ok(())
}

fn compute_statistics(
    nodes: &BTreeMap<NodeId, NodeRecord>,
    relationships: &BTreeMap<RelId, RelRecord>,
    computed_at_commit_epoch: u64,
) -> GraphStatistics {
    compute_statistics_with_basic(
        nodes,
        relationships,
        compute_basic_statistics(nodes, relationships, computed_at_commit_epoch),
    )
}

fn compute_statistics_with_basic(
    nodes: &BTreeMap<NodeId, NodeRecord>,
    relationships: &BTreeMap<RelId, RelRecord>,
    basic_statistics: BasicGraphStatistics,
) -> GraphStatistics {
    let mut statistics = GraphStatistics {
        computed_at_commit_epoch: basic_statistics.computed_at_commit_epoch,
        histogram_sample_limit: MAX_PROPERTY_HISTOGRAM_VALUES,
        node_count: basic_statistics.node_count,
        relationship_count: basic_statistics.relationship_count,
        label_counts: basic_statistics.label_counts,
        rel_type_counts: basic_statistics.rel_type_counts,
        ..GraphStatistics::default()
    };
    let mut property_values = BTreeMap::<(LabelId, String), BTreeSet<Value>>::new();
    let mut rel_property_values = BTreeMap::<(RelTypeId, String), BTreeSet<Value>>::new();
    let mut rel_type_sources = BTreeMap::<RelTypeId, BTreeSet<NodeId>>::new();
    let mut rel_type_targets = BTreeMap::<RelTypeId, BTreeSet<NodeId>>::new();
    let mut path_sources = BTreeMap::<(LabelId, RelTypeId, LabelId), BTreeSet<NodeId>>::new();
    let mut path_targets = BTreeMap::<(LabelId, RelTypeId, LabelId), BTreeSet<NodeId>>::new();
    let mut outgoing_by_source_type = BTreeMap::<(NodeId, RelTypeId), Vec<NodeId>>::new();

    for node in nodes.values() {
        for label_id in &node.labels {
            for (property, value) in &node.properties {
                property_values
                    .entry((*label_id, property.clone()))
                    .or_default()
                    .insert(value.clone());
            }
        }
    }
    for relationship in relationships.values() {
        rel_type_sources
            .entry(relationship.rel_type)
            .or_default()
            .insert(relationship.source);
        rel_type_targets
            .entry(relationship.rel_type)
            .or_default()
            .insert(relationship.target);
        outgoing_by_source_type
            .entry((relationship.source, relationship.rel_type))
            .or_default()
            .push(relationship.target);
        for (property, value) in &relationship.properties {
            rel_property_values
                .entry((relationship.rel_type, property.clone()))
                .or_default()
                .insert(value.clone());
        }
        if let (Some(source), Some(target)) = (
            nodes.get(&relationship.source),
            nodes.get(&relationship.target),
        ) {
            for source_label in &source.labels {
                for target_label in &target.labels {
                    let path_key = (*source_label, relationship.rel_type, *target_label);
                    *statistics.path_counts.entry(path_key).or_default() += 1;
                    path_sources
                        .entry(path_key)
                        .or_default()
                        .insert(relationship.source);
                    path_targets
                        .entry(path_key)
                        .or_default()
                        .insert(relationship.target);
                }
            }
        }
    }
    statistics.rel_type_source_counts = rel_type_sources
        .into_iter()
        .map(|(rel_type, sources)| (rel_type, sources.len() as u64))
        .collect();
    statistics.rel_type_target_counts = rel_type_targets
        .into_iter()
        .map(|(rel_type, targets)| (rel_type, targets.len() as u64))
        .collect();
    statistics.path_source_distinct_counts = path_sources
        .into_iter()
        .map(|(path, sources)| (path, sources.len() as u64))
        .collect();
    statistics.path_target_distinct_counts = path_targets
        .into_iter()
        .map(|(path, targets)| (path, targets.len() as u64))
        .collect();
    for (key, values) in property_values {
        let histogram_sample_limit = adaptive_histogram_sample_limit(values.len());
        let is_sampled = values.len() > histogram_sample_limit;
        statistics
            .property_distinct_counts
            .insert(key.clone(), values.len() as u64);
        statistics
            .property_histograms
            .insert(key.clone(), sample_histogram_values(values));
        statistics
            .sampled_property_histograms
            .insert(key, is_sampled);
    }
    for (key, values) in rel_property_values {
        let histogram_sample_limit = adaptive_histogram_sample_limit(values.len());
        let is_sampled = values.len() > histogram_sample_limit;
        statistics
            .rel_property_distinct_counts
            .insert(key.clone(), values.len() as u64);
        statistics
            .rel_property_histograms
            .insert(key.clone(), sample_histogram_values(values));
        statistics
            .sampled_rel_property_histograms
            .insert(key, is_sampled);
    }
    let bounded_path_statistics = compute_bounded_path_statistics(
        nodes,
        &outgoing_by_source_type,
        MAX_BOUNDED_PATH_STAT_HOPS,
    );
    statistics.bounded_path_counts = bounded_path_statistics.counts;
    statistics.bounded_path_source_distinct_counts = bounded_path_statistics.source_distinct_counts;
    statistics.bounded_path_target_distinct_counts = bounded_path_statistics.target_distinct_counts;
    statistics
}

fn compute_basic_statistics(
    nodes: &BTreeMap<NodeId, NodeRecord>,
    relationships: &BTreeMap<RelId, RelRecord>,
    computed_at_commit_epoch: u64,
) -> BasicGraphStatistics {
    let mut statistics = BasicGraphStatistics {
        computed_at_commit_epoch,
        node_count: nodes.len() as u64,
        relationship_count: relationships.len() as u64,
        ..BasicGraphStatistics::default()
    };
    for node in nodes.values() {
        for label_id in &node.labels {
            *statistics.label_counts.entry(*label_id).or_default() += 1;
        }
    }
    for relationship in relationships.values() {
        *statistics
            .rel_type_counts
            .entry(relationship.rel_type)
            .or_default() += 1;
    }
    statistics
}

fn decrement_counter<K>(counts: &mut BTreeMap<K, u64>, key: &K)
where
    K: Ord,
{
    let Some(count) = counts.get_mut(key) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(key);
    }
}

#[derive(Debug, Default)]
struct BoundedPathStatistics {
    counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    source_distinct_counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    target_distinct_counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
}

fn compute_bounded_path_statistics(
    nodes: &BTreeMap<NodeId, NodeRecord>,
    outgoing_by_source_type: &BTreeMap<(NodeId, RelTypeId), Vec<NodeId>>,
    max_hops: usize,
) -> BoundedPathStatistics {
    let mut accumulator = BoundedPathStatAccumulator::default();
    let context = BoundedPathStatContext {
        nodes,
        outgoing_by_source_type,
        max_hops,
    };
    let rel_types = outgoing_by_source_type
        .keys()
        .map(|(_, rel_type)| *rel_type)
        .collect::<BTreeSet<_>>();
    for source in nodes.values() {
        for source_label in &source.labels {
            for rel_type in &rel_types {
                context.collect(
                    source.id,
                    source.id,
                    *source_label,
                    *rel_type,
                    1,
                    &mut accumulator,
                );
            }
        }
    }
    BoundedPathStatistics {
        counts: accumulator.counts,
        source_distinct_counts: accumulator
            .sources
            .into_iter()
            .map(|(path, sources)| (path, sources.len() as u64))
            .collect(),
        target_distinct_counts: accumulator
            .targets
            .into_iter()
            .map(|(path, targets)| (path, targets.len() as u64))
            .collect(),
    }
}

struct BoundedPathStatContext<'a> {
    nodes: &'a BTreeMap<NodeId, NodeRecord>,
    outgoing_by_source_type: &'a BTreeMap<(NodeId, RelTypeId), Vec<NodeId>>,
    max_hops: usize,
}

#[derive(Debug, Default)]
struct BoundedPathStatAccumulator {
    counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    sources: BTreeMap<(LabelId, RelTypeId, LabelId, usize), BTreeSet<NodeId>>,
    targets: BTreeMap<(LabelId, RelTypeId, LabelId, usize), BTreeSet<NodeId>>,
}

impl BoundedPathStatContext<'_> {
    fn collect(
        &self,
        root_source: NodeId,
        current: NodeId,
        source_label: LabelId,
        rel_type: RelTypeId,
        hop: usize,
        accumulator: &mut BoundedPathStatAccumulator,
    ) {
        if hop > self.max_hops {
            return;
        }
        let Some(targets) = self.outgoing_by_source_type.get(&(current, rel_type)) else {
            return;
        };
        for target_id in targets {
            let Some(target) = self.nodes.get(target_id) else {
                continue;
            };
            for target_label in &target.labels {
                let path_key = (source_label, rel_type, *target_label, hop);
                *accumulator.counts.entry(path_key).or_default() += 1;
                accumulator
                    .sources
                    .entry(path_key)
                    .or_default()
                    .insert(root_source);
                accumulator
                    .targets
                    .entry(path_key)
                    .or_default()
                    .insert(*target_id);
            }
            self.collect(
                root_source,
                *target_id,
                source_label,
                rel_type,
                hop + 1,
                accumulator,
            );
        }
    }
}

fn adaptive_histogram_sample_limit(distinct_count: usize) -> usize {
    if distinct_count <= MID_PROPERTY_HISTOGRAM_DISTINCT_VALUES {
        MIN_PROPERTY_HISTOGRAM_VALUES
    } else if distinct_count <= MAX_PROPERTY_HISTOGRAM_DISTINCT_VALUES {
        MID_PROPERTY_HISTOGRAM_VALUES
    } else {
        MAX_PROPERTY_HISTOGRAM_VALUES
    }
}

fn sample_histogram_values(values: BTreeSet<Value>) -> Vec<Value> {
    let len = values.len();
    let sample_limit = adaptive_histogram_sample_limit(len);
    if len <= sample_limit {
        return values.into_iter().collect();
    }
    let sorted = values.into_iter().collect::<Vec<_>>();
    (0..sample_limit)
        .map(|sample_index| {
            let value_index = sample_index * (len - 1) / (sample_limit - 1);
            sorted[value_index].clone()
        })
        .collect()
}

fn property_filter_matches(
    filter: &PropertyFilter,
    id: u64,
    properties: &BTreeMap<String, Value>,
) -> bool {
    match filter {
        PropertyFilter::And(filters) => filters
            .iter()
            .all(|filter| property_filter_matches(filter, id, properties)),
        PropertyFilter::Or(filters) => filters
            .iter()
            .any(|filter| property_filter_matches(filter, id, properties)),
        PropertyFilter::Not(filter) => !property_filter_matches(filter, id, properties),
        PropertyFilter::IdEq { value } => &Value::Int(id as i64) == value,
        PropertyFilter::IdNotEq { value } => &Value::Int(id as i64) != value,
        PropertyFilter::IdRange { lower, upper } => {
            range_bounds_match(&Value::Int(id as i64), lower.as_ref(), upper.as_ref())
        }
        PropertyFilter::IdIn { values } => {
            values.iter().any(|value| value == &Value::Int(id as i64))
        }
        PropertyFilter::Eq { property, value } => properties
            .get(property)
            .map(|actual| actual == value)
            .unwrap_or(false),
        PropertyFilter::NotEq { property, value } => properties
            .get(property)
            .map(|actual| actual != value)
            .unwrap_or(false),
        PropertyFilter::IsNull { property } => properties
            .get(property)
            .map(|actual| actual == &Value::Null)
            .unwrap_or(true),
        PropertyFilter::IsNotNull { property } => properties
            .get(property)
            .map(|actual| actual != &Value::Null)
            .unwrap_or(false),
        PropertyFilter::In { property, values } => properties
            .get(property)
            .map(|actual| values.iter().any(|value| value == actual))
            .unwrap_or(false),
        PropertyFilter::ListContains { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::List(values) => Some(values.iter().any(|actual| actual == value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::Contains { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.contains(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::StartsWith { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.starts_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::EndsWith { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.ends_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::RegexMatch { property, pattern } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(crate::regex_cache::regex_is_match(pattern, actual)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::DefaultIfNullOrEq {
            property,
            empty,
            default,
            value,
            negated,
        } => {
            let actual = properties.get(property).unwrap_or(&Value::Null);
            let normalized = if actual == &Value::Null || actual == empty {
                default
            } else {
                actual
            };
            let matches = normalized == value;
            if *negated {
                !matches
            } else {
                matches
            }
        }
        PropertyFilter::Range {
            property,
            lower,
            upper,
        } => properties
            .get(property)
            .map(|actual| range_bounds_match(actual, lower.as_ref(), upper.as_ref()))
            .unwrap_or(false),
    }
}

fn properties_contain_all(
    properties: &BTreeMap<String, Value>,
    required: &BTreeMap<String, Value>,
) -> bool {
    required
        .iter()
        .all(|(property, value)| properties.get(property) == Some(value))
}

fn adjacency_layout_for_degree(degree: usize) -> AdjacencyLayout {
    if degree >= DENSE_ADJACENCY_DEGREE_THRESHOLD {
        AdjacencyLayout::Dense
    } else {
        AdjacencyLayout::Sparse
    }
}

fn adjacency_direction_sort_key(direction: AdjacencyDirection) -> u8 {
    match direction {
        AdjacencyDirection::Outgoing => 0,
        AdjacencyDirection::Incoming => 1,
    }
}

fn generated_stable_id(kind: &str, physical_id: u64) -> Value {
    Value::Map(BTreeMap::from([
        (
            "source".to_string(),
            Value::String("skein-stable-id-v1".to_string()),
        ),
        ("kind".to_string(), Value::String(kind.to_string())),
        (
            "physical_id".to_string(),
            Value::String(physical_id.to_string()),
        ),
    ]))
}

fn evaluate_node_set_value(
    properties: &BTreeMap<String, Value>,
    assignment: &NodeSetAssignment,
) -> Result<Value> {
    match &assignment.value {
        NodeSetValue::Value(value) => Ok(value.clone()),
        NodeSetValue::Coalesce { default } => Ok(match properties.get(&assignment.property) {
            None | Some(Value::Null) => default.clone(),
            Some(value) => value.clone(),
        }),
        NodeSetValue::AddInt { amount } => {
            let current = match properties.get(&assignment.property) {
                None | Some(Value::Null) => 0,
                Some(Value::Int(value)) => *value,
                Some(value) => {
                    return Err(SkeinError::Execution(format!(
                        "property increment requires an integer or null value, got {value:?}"
                    )));
                }
            };
            Ok(Value::Int(current.checked_add(*amount).ok_or_else(
                || SkeinError::Execution("property increment overflowed i64".to_string()),
            )?))
        }
        NodeSetValue::DecrementFloorZero => {
            let current = match properties.get(&assignment.property) {
                None | Some(Value::Null) => 0,
                Some(Value::Int(value)) => *value,
                Some(value) => {
                    return Err(SkeinError::Execution(format!(
                        "property decrement requires an integer or null value, got {value:?}"
                    )));
                }
            };
            Ok(Value::Int(if current > 0 { current - 1 } else { 0 }))
        }
        NodeSetValue::PreserveNewerExisting { incoming, preserve } => {
            let current = properties
                .get(&assignment.property)
                .cloned()
                .unwrap_or(Value::Null);
            if incoming == &Value::Null {
                return Ok(current);
            }
            if *preserve
                && current != Value::Null
                && value_gt_for_preserve_newer_existing(&current, incoming)
            {
                return Ok(current);
            }
            Ok(incoming.clone())
        }
    }
}

fn value_gt_for_preserve_newer_existing(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => left > right,
        (Value::Float(left), Value::Float(right)) => left > right,
        (Value::Int(left), Value::Float(right)) => (*left as f64) > *right,
        (Value::Float(left), Value::Int(right)) => *left > (*right as f64),
        (Value::String(left), Value::String(right)) => left > right,
        _ => false,
    }
}

fn apply_node_assignments_to_properties(
    properties: &mut BTreeMap<String, Value>,
    assignments: &[NodeSetAssignment],
) -> Result<()> {
    for assignment in assignments {
        let value = evaluate_node_set_value(properties, assignment)?;
        properties.insert(assignment.property.clone(), value);
    }
    Ok(())
}

fn optional_label_id(catalog: &Catalog, label: &str) -> Option<LabelId> {
    if label.is_empty() {
        None
    } else {
        catalog.label_id(label)
    }
}

fn apply_set_relationship_properties_mutation(
    store: &GraphStore,
    catalog: &Catalog,
    ops: &mut Vec<WalOp>,
    rows: &mut Vec<BTreeMap<String, Value>>,
    update: RelationshipPropertiesUpdate,
) {
    let (Some(source_label_id), Some(target_label_id), Some(rel_type_id)) = (
        catalog.label_id(&update.source_label),
        catalog.label_id(&update.target_label),
        catalog.rel_type_id(&update.rel_type),
    ) else {
        return;
    };
    let source_ids = store
        .matching_node_ids(Some(source_label_id), update.filter.as_ref())
        .collect::<BTreeSet<_>>();
    for relationship in store.relationships.values() {
        if relationship.rel_type != rel_type_id || !source_ids.contains(&relationship.source) {
            continue;
        }
        if update
            .rel_filter
            .as_ref()
            .map(|filter| {
                !property_filter_matches(filter, relationship.id.0, &relationship.properties)
            })
            .unwrap_or(false)
        {
            continue;
        }
        let target_matches = store
            .nodes
            .get(&relationship.target)
            .map(|target| {
                target.labels.contains(&target_label_id)
                    && update
                        .target_filter
                        .as_ref()
                        .map(|filter| {
                            property_filter_matches(filter, target.id.0, &target.properties)
                        })
                        .unwrap_or(true)
            })
            .unwrap_or(false);
        if !target_matches {
            continue;
        }
        for assignment in &update.assignments {
            ops.push(WalOp::SetRelationshipProperty {
                id: relationship.id,
                property: assignment.property.clone(),
                value: assignment.value.clone(),
            });
        }
        rows.push(BTreeMap::from([(
            "rel_id".to_string(),
            Value::Int(relationship.id.0 as i64),
        )]));
    }
}

fn range_bounds_match(
    value: &Value,
    lower: Option<&(Value, bool)>,
    upper: Option<&(Value, bool)>,
) -> bool {
    if let Some((bound, inclusive)) = lower {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering == std::cmp::Ordering::Less
            || (ordering == std::cmp::Ordering::Equal && !inclusive)
        {
            return false;
        }
    }
    if let Some((bound, inclusive)) = upper {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering == std::cmp::Ordering::Greater
            || (ordering == std::cmp::Ordering::Equal && !inclusive)
        {
            return false;
        }
    }
    true
}

fn comparable_value_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => Some(left.total_cmp(right)),
        (Value::Int(left), Value::Float(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float(left), Value::Int(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn merge_relationship_row(
    source: NodeId,
    relationship: RelId,
    target: NodeId,
    created: bool,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
        ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
        ("created".to_string(), Value::Bool(created)),
    ])
}

fn split_checkpoint_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "checkpoint missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "checkpoint checksum")?;
    Ok((body, checksum))
}

fn split_manifest_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "manifest missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "manifest checksum")?;
    Ok((body, checksum))
}

fn split_projected_graph_artifact_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "projected graph artifact missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "projected graph artifact checksum")?;
    Ok((body, checksum))
}

fn split_stable_id_mapping_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "stable id mapping missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "stable id mapping checksum")?;
    Ok((body, checksum))
}

fn encode_durable_text(text: &str, compression: DurableCompression) -> Result<Vec<u8>> {
    match compression {
        DurableCompression::Zstd => encode_zstd_durable_text(text),
    }
}

fn encode_zstd_durable_text(text: &str) -> Result<Vec<u8>> {
    let compressed = zstd::stream::encode_all(text.as_bytes(), DEFAULT_COMPRESSION_LEVEL)
        .map_err(|error| SkeinError::Storage(format!("zstd compression failed: {error}")))?;
    let compressed_checksum = checksum_bytes(&compressed);
    let uncompressed_checksum = checksum_bytes(text.as_bytes());
    let header = format!(
        "{DURABLE_COMPRESSION_HEADER}\ncodec\tzstd\nuncompressed_checksum\t{uncompressed_checksum}\ncompressed_checksum\t{compressed_checksum}\nuncompressed_len\t{}\ncompressed_len\t{}\n\n",
        text.len(),
        compressed.len()
    );
    let mut encoded = header.into_bytes();
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

fn read_durable_text(path: &Path, name: &str) -> Result<String> {
    let bytes = fs::read(path)?;
    if bytes.starts_with(DURABLE_COMPRESSION_HEADER.as_bytes()) {
        decode_compressed_durable_text(&bytes, name)
    } else {
        String::from_utf8(bytes)
            .map_err(|error| SkeinError::Storage(format!("{name} is not valid UTF-8: {error}")))
    }
}

fn decode_compressed_durable_text(bytes: &[u8], name: &str) -> Result<String> {
    let Some(header_end) = bytes.windows(2).position(|window| window == b"\n\n") else {
        return Err(SkeinError::Storage(format!(
            "{name} compressed envelope missing header terminator"
        )));
    };
    let header = std::str::from_utf8(&bytes[..header_end]).map_err(|error| {
        SkeinError::Storage(format!(
            "{name} compressed envelope header is invalid: {error}"
        ))
    })?;
    let payload = &bytes[header_end + 2..];
    let mut codec = None;
    let mut compressed_checksum = None;
    let mut uncompressed_checksum = None;
    let mut compressed_len = None;
    let mut uncompressed_len = None;
    for line in header.lines() {
        if line == DURABLE_COMPRESSION_HEADER {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["codec", value] => codec = Some(*value),
            ["compressed_checksum", value] => {
                compressed_checksum = Some(parse_u64(value, "compressed checksum")?);
            }
            ["uncompressed_checksum", value] => {
                uncompressed_checksum = Some(parse_u64(value, "uncompressed checksum")?);
            }
            ["compressed_len", value] => {
                compressed_len = Some(parse_usize(value, "compressed length")?);
            }
            ["uncompressed_len", value] => {
                uncompressed_len = Some(parse_usize(value, "uncompressed length")?);
            }
            _ => {
                return Err(SkeinError::Storage(format!(
                    "{name} compressed envelope has invalid header line: {line}"
                )));
            }
        }
    }
    if codec != Some("zstd") {
        return Err(SkeinError::Storage(format!(
            "{name} compressed envelope uses unsupported codec"
        )));
    }
    let expected_compressed_len = compressed_len.ok_or_else(|| {
        SkeinError::Storage(format!("{name} compressed envelope missing compressed_len"))
    })?;
    if payload.len() != expected_compressed_len {
        return Err(SkeinError::Storage(format!(
            "{name} compressed length mismatch: expected {expected_compressed_len}, got {}",
            payload.len()
        )));
    }
    let expected_compressed_checksum = compressed_checksum.ok_or_else(|| {
        SkeinError::Storage(format!(
            "{name} compressed envelope missing compressed_checksum"
        ))
    })?;
    let actual_compressed_checksum = checksum_bytes(payload);
    if actual_compressed_checksum != expected_compressed_checksum {
        return Err(SkeinError::Storage(format!(
            "{name} compressed checksum mismatch: expected {expected_compressed_checksum}, got {actual_compressed_checksum}"
        )));
    }
    let decoded = zstd::stream::decode_all(Cursor::new(payload)).map_err(|error| {
        SkeinError::Storage(format!("{name} zstd decompression failed: {error}"))
    })?;
    let expected_uncompressed_len = uncompressed_len.ok_or_else(|| {
        SkeinError::Storage(format!(
            "{name} compressed envelope missing uncompressed_len"
        ))
    })?;
    if decoded.len() != expected_uncompressed_len {
        return Err(SkeinError::Storage(format!(
            "{name} uncompressed length mismatch: expected {expected_uncompressed_len}, got {}",
            decoded.len()
        )));
    }
    let expected_uncompressed_checksum = uncompressed_checksum.ok_or_else(|| {
        SkeinError::Storage(format!(
            "{name} compressed envelope missing uncompressed_checksum"
        ))
    })?;
    let actual_uncompressed_checksum = checksum_bytes(&decoded);
    if actual_uncompressed_checksum != expected_uncompressed_checksum {
        return Err(SkeinError::Storage(format!(
            "{name} uncompressed checksum mismatch: expected {expected_uncompressed_checksum}, got {actual_uncompressed_checksum}"
        )));
    }
    String::from_utf8(decoded).map_err(|error| {
        SkeinError::Storage(format!(
            "{name} decompressed payload is not valid UTF-8: {error}"
        ))
    })
}

fn encode_label_set(labels: &BTreeSet<LabelId>) -> String {
    labels
        .iter()
        .map(|label| label.0.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_label_set(input: &str) -> Result<BTreeSet<LabelId>> {
    if input.is_empty() {
        return Ok(BTreeSet::new());
    }
    input
        .split(',')
        .map(|raw| parse_u32(raw, "label id").map(LabelId))
        .collect()
}

fn encode_string_vec(values: &[String]) -> String {
    values
        .iter()
        .map(|value| encode_string(value))
        .collect::<Vec<_>>()
        .join(":")
}

fn decode_string_vec(input: &str) -> Result<Vec<String>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input.split(':').map(decode_string).collect()
}

fn encode_stable_id_mapping(mapping: &StoreStableIdMapping) -> String {
    let mut body = String::new();
    body.push_str("SKEIN_STABLE_ID_MAPPING_V1\n");
    body.push_str(&format!("version\t{STORAGE_VERSION}\n"));
    for (id, stable_id) in &mapping.node_stable_ids {
        body.push_str(&format!("node\t{}\t{}\n", id.0, encode_value(stable_id)));
    }
    for (id, stable_id) in &mapping.relationship_stable_ids {
        body.push_str(&format!("rel\t{}\t{}\n", id.0, encode_value(stable_id)));
    }
    body
}

fn decode_stable_id_mapping(body: &str) -> Result<StoreStableIdMapping> {
    let mut mapping = StoreStableIdMapping::default();
    for line in body.lines() {
        if line == "SKEIN_STABLE_ID_MAPPING_V1" {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["version", version] => validate_storage_version(version)?,
            ["node", raw_id, raw_value] => {
                mapping.node_stable_ids.insert(
                    NodeId(parse_u64(raw_id, "stable id node id")?),
                    decode_value(raw_value)?,
                );
            }
            ["rel", raw_id, raw_value] => {
                mapping.relationship_stable_ids.insert(
                    RelId(parse_u64(raw_id, "stable id relationship id")?),
                    decode_value(raw_value)?,
                );
            }
            [""] => {}
            _ => {
                return Err(SkeinError::Storage(format!(
                    "invalid stable id mapping line: {line}"
                )));
            }
        }
    }
    Ok(mapping)
}

fn encode_value_vec(values: &[Value]) -> String {
    values
        .iter()
        .map(|value| encode_string(&encode_value(value)))
        .collect::<Vec<_>>()
        .join(":")
}

fn encode_u64_vec(values: impl IntoIterator<Item = u64>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_u64_vec(input: &str, name: &str) -> Result<Vec<u64>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|value| parse_u64(value, name))
        .collect()
}

fn encode_usize_vec(values: impl IntoIterator<Item = usize>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_usize_vec(input: &str, name: &str) -> Result<Vec<usize>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|value| {
            value
                .parse()
                .map_err(|_| SkeinError::Storage(format!("invalid {name}: {value}")))
        })
        .collect()
}

fn encode_properties(properties: &BTreeMap<String, Value>) -> String {
    properties
        .iter()
        .map(|(key, value)| format!("{}={}", encode_string(key), encode_value(value)))
        .collect::<Vec<_>>()
        .join(";")
}

fn decode_properties(input: &str) -> Result<BTreeMap<String, Value>> {
    let mut properties = BTreeMap::new();
    if input.is_empty() {
        return Ok(properties);
    }
    for pair in input.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(SkeinError::Storage(format!(
                "invalid property pair: {pair}"
            )));
        };
        properties.insert(decode_string(key)?, decode_value(value)?);
    }
    Ok(properties)
}

fn encode_value(value: &Value) -> String {
    match value {
        Value::Null => "n".to_string(),
        Value::Bool(false) => "b0".to_string(),
        Value::Bool(true) => "b1".to_string(),
        Value::Int(value) => format!("i{value}"),
        Value::Float(value) => format!("f{}", value.to_bits()),
        Value::String(value) => format!("s{}", encode_string(value)),
        Value::List(values) => format!(
            "l{}",
            values
                .iter()
                .map(|value| encode_string(&encode_value(value)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Map(values) => format!(
            "m{}",
            values
                .iter()
                .map(|(key, value)| format!(
                    "{}={}",
                    encode_string(key),
                    encode_string(&encode_value(value))
                ))
                .collect::<Vec<_>>()
                .join(";")
        ),
    }
}

fn decode_value(input: &str) -> Result<Value> {
    if input.is_empty() {
        return Err(SkeinError::Storage("empty encoded value".to_string()));
    }
    let (kind, rest) = input.split_at(1);
    match kind {
        "n" if rest.is_empty() => Ok(Value::Null),
        "b" => match rest {
            "0" => Ok(Value::Bool(false)),
            "1" => Ok(Value::Bool(true)),
            _ => Err(SkeinError::Storage(format!("invalid bool value: {input}"))),
        },
        "i" => parse_i64(rest, "integer value").map(Value::Int),
        "f" => parse_u64(rest, "float value")
            .map(f64::from_bits)
            .map(Value::Float),
        "s" => decode_string(rest).map(Value::String),
        "l" => decode_list_value(rest),
        "m" => decode_map_value(rest),
        _ => Err(SkeinError::Storage(format!(
            "invalid encoded value: {input}"
        ))),
    }
}

fn decode_list_value(input: &str) -> Result<Value> {
    if input.is_empty() {
        return Ok(Value::List(Vec::new()));
    }
    input
        .split(',')
        .map(|item| decode_string(item).and_then(|value| decode_value(&value)))
        .collect::<Result<Vec<_>>>()
        .map(Value::List)
}

fn decode_map_value(input: &str) -> Result<Value> {
    let mut values = BTreeMap::new();
    if input.is_empty() {
        return Ok(Value::Map(values));
    }
    for item in input.split(';') {
        let Some((key, value)) = item.split_once('=') else {
            return Err(SkeinError::Storage(format!(
                "invalid encoded map item: {item}"
            )));
        };
        values.insert(
            decode_string(key)?,
            decode_string(value).and_then(|value| decode_value(&value))?,
        );
    }
    Ok(Value::Map(values))
}

fn encode_table_kind(kind: TableKind) -> &'static str {
    match kind {
        TableKind::Node => "node",
        TableKind::Relationship => "relationship",
    }
}

fn decode_table_kind(input: &str) -> Result<TableKind> {
    match input {
        "node" => Ok(TableKind::Node),
        "relationship" => Ok(TableKind::Relationship),
        _ => Err(SkeinError::Storage(format!("invalid table kind: {input}"))),
    }
}

fn encode_property_type(value_type: PropertyType) -> &'static str {
    match value_type {
        PropertyType::Any => "any",
        PropertyType::Bool => "bool",
        PropertyType::Int => "int",
        PropertyType::Float => "float",
        PropertyType::String => "string",
        PropertyType::List => "list",
    }
}

fn decode_property_type(input: &str) -> Result<PropertyType> {
    match input {
        "any" => Ok(PropertyType::Any),
        "bool" => Ok(PropertyType::Bool),
        "int" => Ok(PropertyType::Int),
        "float" => Ok(PropertyType::Float),
        "string" => Ok(PropertyType::String),
        "list" => Ok(PropertyType::List),
        _ => Err(SkeinError::Storage(format!(
            "invalid property type: {input}"
        ))),
    }
}

fn encode_index_kind(kind: IndexKind) -> &'static str {
    match kind {
        IndexKind::Equality => "equality",
        IndexKind::Range => "range",
        IndexKind::FullText => "fulltext",
    }
}

fn decode_index_kind(input: &str) -> Result<IndexKind> {
    match input {
        "equality" => Ok(IndexKind::Equality),
        "range" => Ok(IndexKind::Range),
        "fulltext" => Ok(IndexKind::FullText),
        _ => Err(SkeinError::Storage(format!("invalid index kind: {input}"))),
    }
}

fn encode_nullable(nullable: bool) -> &'static str {
    if nullable {
        "nullable"
    } else {
        "not_null"
    }
}

fn encode_bool(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

fn decode_nullable(input: &str) -> Result<bool> {
    match input {
        "nullable" => Ok(true),
        "not_null" => Ok(false),
        _ => Err(SkeinError::Storage(format!(
            "invalid nullable flag: {input}"
        ))),
    }
}

fn encode_schema_object_state(state: SchemaObjectState) -> &'static str {
    match state {
        SchemaObjectState::DeleteOnly => "delete_only",
        SchemaObjectState::WriteOnly => "write_only",
        SchemaObjectState::Backfill => "backfill",
        SchemaObjectState::Validating => "validating",
        SchemaObjectState::Public => "public",
        SchemaObjectState::Gc => "gc",
    }
}

fn decode_schema_object_state(input: &str) -> Result<SchemaObjectState> {
    match input {
        "delete_only" => Ok(SchemaObjectState::DeleteOnly),
        "write_only" => Ok(SchemaObjectState::WriteOnly),
        "backfill" => Ok(SchemaObjectState::Backfill),
        "validating" => Ok(SchemaObjectState::Validating),
        "public" => Ok(SchemaObjectState::Public),
        "gc" => Ok(SchemaObjectState::Gc),
        _ => Err(SkeinError::Storage(format!(
            "invalid schema object state: {input}"
        ))),
    }
}

fn encode_string(input: &str) -> String {
    input
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn decode_string(input: &str) -> Result<String> {
    if !input.len().is_multiple_of(2) {
        return Err(SkeinError::Storage(format!(
            "invalid hex string length: {}",
            input.len()
        )));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for offset in (0..input.len()).step_by(2) {
        let byte = u8::from_str_radix(&input[offset..offset + 2], 16)
            .map_err(|_| SkeinError::Storage(format!("invalid hex string: {input}")))?;
        bytes.push(byte);
    }
    String::from_utf8(bytes).map_err(|error| SkeinError::Storage(error.to_string()))
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn parse_u64(input: &str, name: &str) -> Result<u64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_u32(input: &str, name: &str) -> Result<u32> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_usize(input: &str, name: &str) -> Result<usize> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_i64(input: &str, name: &str) -> Result<i64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn encode_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn parse_optional_u64(input: &str, name: &str) -> Result<Option<u64>> {
    if input == "none" {
        Ok(None)
    } else {
        parse_u64(input, name).map(Some)
    }
}

fn validate_storage_version(version: &str) -> Result<()> {
    if version == STORAGE_VERSION {
        return Ok(());
    }
    Err(SkeinError::Storage(format!(
        "unsupported storage version: {version}; expected {STORAGE_VERSION}"
    )))
}

#[cfg(test)]
mod tests {
    use super::{
        checksum_bytes, compute_statistics, encode_durable_text, read_durable_text,
        AdjacencyDirection, AdjacencyGroupStats, AdjacencyLayout, ConnectedNodesCreate,
        DurableCompression, GraphStore, NodeId, NodeRecord, OrderedAdjacencyEntry,
        ProjectedGraphDefinition, PropertyFilter, RelId, RelRecord, RelTypeId, ScanPruningStrategy,
        DENSE_ADJACENCY_DEGREE_THRESHOLD, DURABLE_COMPRESSION_HEADER,
    };
    use crate::schema::{Catalog, LabelId};
    use crate::value::Value;
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::Write;

    #[test]
    fn replays_relationships_from_wal_and_rebuilds_adjacency() {
        let path = unique_test_dir("rel_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(
                    &mut catalog,
                    source,
                    target,
                    "RELATES_TO",
                    properties([("weight", Value::Int(7))]),
                )
                .unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();
            let rels = store
                .outgoing_relationships(NodeId(0), rel_type)
                .collect::<Vec<_>>();
            assert_eq!(rels.len(), 1);
            assert_eq!(rels[0].target, NodeId(1));
            assert_eq!(rels[0].properties.get("weight"), Some(&Value::Int(7)));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn ordered_adjacency_entries_sort_by_neighbor_then_relationship_id() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        let target_one = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
            .unwrap();
        let target_two = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
            .unwrap();
        let target_three = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(3))]))
            .unwrap();

        let rel_three = store
            .create_relationship(
                &mut catalog,
                source,
                target_three,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        let rel_one = store
            .create_relationship(
                &mut catalog,
                source,
                target_one,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        let rel_two = store
            .create_relationship(
                &mut catalog,
                source,
                target_two,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

        let outgoing =
            store.ordered_adjacency_entries(source, rel_type, AdjacencyDirection::Outgoing);
        let incoming =
            store.ordered_adjacency_entries(target_one, rel_type, AdjacencyDirection::Incoming);

        assert_eq!(
            outgoing,
            vec![
                OrderedAdjacencyEntry {
                    relationship_id: rel_one,
                    neighbor_id: target_one,
                },
                OrderedAdjacencyEntry {
                    relationship_id: rel_two,
                    neighbor_id: target_two,
                },
                OrderedAdjacencyEntry {
                    relationship_id: rel_three,
                    neighbor_id: target_three,
                },
            ]
        );
        assert_eq!(
            incoming,
            vec![OrderedAdjacencyEntry {
                relationship_id: rel_one,
                neighbor_id: source,
            }]
        );
    }

    #[test]
    fn ordered_adjacency_entries_for_node_sort_across_relationship_types() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        let target_one = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
            .unwrap();
        let target_two = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
            .unwrap();

        let rel_late_neighbor = store
            .create_relationship(
                &mut catalog,
                source,
                target_two,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        let rel_early_neighbor = store
            .create_relationship(
                &mut catalog,
                source,
                target_one,
                "RELATES_TO",
                BTreeMap::new(),
            )
            .unwrap();

        let outgoing =
            store.ordered_adjacency_entries_for_node(source, AdjacencyDirection::Outgoing);
        let incoming =
            store.ordered_adjacency_entries_for_node(target_one, AdjacencyDirection::Incoming);

        assert_eq!(
            outgoing,
            vec![
                OrderedAdjacencyEntry {
                    relationship_id: rel_early_neighbor,
                    neighbor_id: target_one,
                },
                OrderedAdjacencyEntry {
                    relationship_id: rel_late_neighbor,
                    neighbor_id: target_two,
                },
            ]
        );
        assert_eq!(
            incoming,
            vec![OrderedAdjacencyEntry {
                relationship_id: rel_early_neighbor,
                neighbor_id: source,
            }]
        );
    }

    #[test]
    fn adjacency_group_stats_classify_sparse_and_dense_groups() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();

        for target_index in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
            let target = store
                .create_node(
                    &mut catalog,
                    "Entity",
                    properties([("id", Value::Int(target_index as i64))]),
                )
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
                .unwrap();
        }
        let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

        let dense_stats =
            store.adjacency_group_stats(source, rel_type, AdjacencyDirection::Outgoing);
        let sparse_stats =
            store.adjacency_group_stats(NodeId(1), rel_type, AdjacencyDirection::Incoming);

        assert_eq!(dense_stats.degree, DENSE_ADJACENCY_DEGREE_THRESHOLD);
        assert_eq!(dense_stats.layout, AdjacencyLayout::Dense);
        assert_eq!(sparse_stats.degree, 1);
        assert_eq!(sparse_stats.layout, AdjacencyLayout::Sparse);
    }

    #[test]
    fn adjacency_group_stats_for_node_reports_each_relationship_type() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        let mentions_target = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
            .unwrap();
        let relates_target = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                mentions_target,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                relates_target,
                "RELATES_TO",
                BTreeMap::new(),
            )
            .unwrap();
        let mentions = catalog.rel_type_id("MENTIONS").unwrap();
        let relates_to = catalog.rel_type_id("RELATES_TO").unwrap();

        let stats = store.adjacency_group_stats_for_node(source, AdjacencyDirection::Outgoing);

        assert_eq!(
            stats,
            vec![
                AdjacencyGroupStats {
                    node_id: source,
                    rel_type: mentions,
                    direction: AdjacencyDirection::Outgoing,
                    degree: 1,
                    layout: AdjacencyLayout::Sparse,
                },
                AdjacencyGroupStats {
                    node_id: source,
                    rel_type: relates_to,
                    direction: AdjacencyDirection::Outgoing,
                    degree: 1,
                    layout: AdjacencyLayout::Sparse,
                },
            ]
        );
    }

    #[test]
    fn checkpoints_relationships_and_truncates_wal() {
        let path = unique_test_dir("rel_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "RELATES_TO", BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();
            let rels = store
                .outgoing_relationships(NodeId(0), rel_type)
                .collect::<Vec<_>>();
            assert_eq!(rels.len(), 1);
            assert_eq!(rels[0].source, NodeId(0));
            assert_eq!(rels[0].target, NodeId(1));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_checkpoint_relationship_with_missing_endpoint() {
        let path = unique_test_dir("rel_checkpoint_missing_endpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "RELATES_TO", BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        rewrite_checksummed_file(
            &path.join("checkpoint.skein"),
            "rel\t0\t0\t1\t0\t",
            "rel\t0\t0\t99\t0\t",
            "checkpoint",
        );

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("relationship 0 references missing target node 99"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_publishes_manifest_with_epoch_boundary() {
        let path = unique_test_dir("manifest_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        let checkpoint_bytes = std::fs::read(path.join("checkpoint.skein")).unwrap();
        assert!(checkpoint_bytes.starts_with(DURABLE_COMPRESSION_HEADER.as_bytes()));
        let header_end = checkpoint_bytes
            .windows(2)
            .position(|window| window == b"\n\n")
            .unwrap();
        let header = std::str::from_utf8(&checkpoint_bytes[..header_end]).unwrap();
        assert!(header.contains("codec\tzstd\n"));
        let checkpoint = read_durable_text(&path.join("checkpoint.skein"), "checkpoint").unwrap();
        assert!(checkpoint.contains("commit_epoch\t2\n"));
        let manifest = std::fs::read_to_string(path.join("manifest.skein")).unwrap();
        assert!(manifest.contains("SKEIN_MANIFEST_V1\n"));
        assert!(manifest.contains("checkpoint_epoch\t1\n"));
        assert!(manifest.contains("checkpoint_commit_epoch\t2\n"));
        assert!(manifest.contains("oldest_reader_commit_epoch\tnone\n"));
        assert!(manifest.contains("safe_reclaim_commit_epoch\t2\n"));
        assert!(manifest.contains("wal_replay_start_lsn\t1\n"));
        assert!(manifest.contains("next_lsn\t1\n"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn reopen_after_checkpoint_uses_manifest_lsn_without_overwriting_wal() {
        let path = unique_test_dir("manifest_reopen_lsn");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
        }

        let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        assert!(wal.starts_with("1\tcreate_node\t"));
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let nodes = store.scan_nodes(Some(label)).collect::<Vec<_>>();
        assert_eq!(nodes.len(), 2);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_corrupt_manifest() {
        let path = unique_test_dir("corrupt_manifest");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        let manifest_path = path.join("manifest.skein");
        let manifest = std::fs::read_to_string(&manifest_path).unwrap();
        std::fs::write(
            &manifest_path,
            manifest.replace("checkpoint_epoch\t1\n", "checkpoint_epoch\t2\n"),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error.to_string().contains("manifest checksum mismatch"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_unsupported_manifest_storage_version() {
        let path = unique_test_dir("manifest_storage_version");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        rewrite_checksummed_file(
            &path.join("manifest.skein"),
            "version\tskein-storage-v1\n",
            "version\tskein-storage-v0\n",
            "manifest",
        );

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported storage version: skein-storage-v0"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_unsupported_checkpoint_storage_version() {
        let path = unique_test_dir("checkpoint_storage_version");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        std::fs::remove_file(path.join("manifest.skein")).unwrap();
        rewrite_checksummed_file(
            &path.join("checkpoint.skein"),
            "version\tskein-storage-v1\n",
            "version\tskein-storage-v0\n",
            "checkpoint",
        );

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported storage version: skein-storage-v0"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn replays_property_index_from_wal() {
        let path = unique_test_dir("property_index_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("id", Value::Int(1)),
                        ("title", Value::String("Graph foundations".to_string())),
                    ]),
                )
                .unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let label = catalog.label_id("Memory").unwrap();
            let nodes = store
                .seek_nodes_by_property(label, "id", &Value::Int(1))
                .collect::<Vec<_>>();
            assert_eq!(nodes.len(), 1);
            assert_eq!(
                nodes[0].properties.get("title"),
                Some(&Value::String("Graph foundations".to_string()))
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rebuilds_property_index_from_checkpoint() {
        let path = unique_test_dir("property_index_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("id", Value::Int(1)),
                        ("title", Value::String("Graph foundations".to_string())),
                    ]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let label = catalog.label_id("Memory").unwrap();
            let nodes = store
                .seek_nodes_by_property(
                    label,
                    "title",
                    &Value::String("Graph foundations".to_string()),
                )
                .collect::<Vec<_>>();
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].properties.get("id"), Some(&Value::Int(1)));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn scan_pruning_uses_property_eq_index_for_unique_key() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:1".to_string())),
                    ("title", Value::String("Graph foundations".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:2".to_string())),
                    ("title", Value::String("Storage notes".to_string())),
                ]),
            )
            .unwrap();

        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            Some(label),
            Some(&PropertyFilter::Eq {
                property: "stable_id".to_string(),
                value: Value::String("memory:2".to_string()),
            }),
        );

        assert_eq!(scan.nodes.len(), 1);
        assert_eq!(
            scan.nodes[0].properties.get("title"),
            Some(&Value::String("Storage notes".to_string()))
        );
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyEq {
                property: "stable_id".to_string()
            }
        );
        assert!(scan.report.pruned);
        assert_eq!(scan.report.candidate_count_before_filter, 1);
        assert_eq!(scan.report.filtered_out_count, 0);
    }

    #[test]
    fn scan_pruning_treats_in_values_as_enum_set() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for state in ["active", "deleted", "forgotten"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("state", Value::String(state.to_string()))]),
                )
                .unwrap();
        }

        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            Some(label),
            Some(&PropertyFilter::In {
                property: "state".to_string(),
                values: vec![
                    Value::String("deleted".to_string()),
                    Value::String("forgotten".to_string()),
                ],
            }),
        );
        let states = scan
            .nodes
            .iter()
            .map(|node| node.properties.get("state").unwrap().clone())
            .collect::<BTreeSet<_>>();

        assert_eq!(
            states,
            BTreeSet::from([
                Value::String("deleted".to_string()),
                Value::String("forgotten".to_string()),
            ])
        );
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyIn {
                property: "state".to_string()
            }
        );
        assert_eq!(scan.report.candidate_count_before_filter, 2);
    }

    #[test]
    fn scan_pruning_uses_smallest_candidate_in_and_filter() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:1".to_string())),
                    ("state", Value::String("active".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:2".to_string())),
                    ("state", Value::String("active".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:3".to_string())),
                    ("state", Value::String("forgotten".to_string())),
                ]),
            )
            .unwrap();

        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            Some(label),
            Some(&PropertyFilter::And(vec![
                PropertyFilter::In {
                    property: "state".to_string(),
                    values: vec![
                        Value::String("active".to_string()),
                        Value::String("forgotten".to_string()),
                    ],
                },
                PropertyFilter::Eq {
                    property: "stable_id".to_string(),
                    value: Value::String("memory:2".to_string()),
                },
            ])),
        );

        assert_eq!(scan.nodes.len(), 1);
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyEq {
                property: "stable_id".to_string()
            }
        );
        assert_eq!(scan.report.candidate_count_before_filter, 1);
    }

    #[test]
    fn scan_pruning_unions_prunable_or_branches() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for state in ["active", "deleted", "forgotten"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("state", Value::String(state.to_string()))]),
                )
                .unwrap();
        }

        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            Some(label),
            Some(&PropertyFilter::Or(vec![
                PropertyFilter::Eq {
                    property: "state".to_string(),
                    value: Value::String("active".to_string()),
                },
                PropertyFilter::Eq {
                    property: "state".to_string(),
                    value: Value::String("forgotten".to_string()),
                },
            ])),
        );

        assert_eq!(scan.nodes.len(), 2);
        assert_eq!(scan.report.strategy, ScanPruningStrategy::OrUnion);
        assert_eq!(scan.report.candidate_count_before_filter, 2);
        assert!(scan.report.pruned);
    }

    #[test]
    fn scan_pruning_reports_exact_empty_for_empty_in_filter() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("state", Value::String("active".to_string()))]),
            )
            .unwrap();

        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            Some(label),
            Some(&PropertyFilter::In {
                property: "state".to_string(),
                values: vec![],
            }),
        );

        assert!(scan.nodes.is_empty());
        assert_eq!(scan.report.strategy, ScanPruningStrategy::Empty);
        assert!(scan.report.exact_empty);
        assert_eq!(scan.report.candidate_count_before_filter, 0);
    }

    #[test]
    fn scan_pruning_uses_property_range_for_numeric_and_iso_date_strings() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("importance", Value::Float(0.2)),
                    ("updated_at", Value::String("2026-07-01".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("importance", Value::Float(0.7)),
                    ("updated_at", Value::String("2026-07-15".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("importance", Value::Float(0.9)),
                    ("updated_at", Value::String("2026-08-01".to_string())),
                ]),
            )
            .unwrap();

        let label = catalog.label_id("Memory").unwrap();
        let numeric_scan = store.scan_nodes_with_filter_pruning(
            Some(label),
            Some(&PropertyFilter::Range {
                property: "importance".to_string(),
                lower: Some((Value::Float(0.5), true)),
                upper: Some((Value::Float(0.8), true)),
            }),
        );
        assert_eq!(numeric_scan.nodes.len(), 1);
        assert_eq!(
            numeric_scan.report.strategy,
            ScanPruningStrategy::PropertyRange {
                property: "importance".to_string()
            }
        );
        assert_eq!(numeric_scan.report.candidate_count_before_filter, 1);

        let date_scan = store.scan_nodes_with_filter_pruning(
            Some(label),
            Some(&PropertyFilter::Range {
                property: "updated_at".to_string(),
                lower: Some((Value::String("2026-07-01".to_string()), true)),
                upper: Some((Value::String("2026-07-31".to_string()), true)),
            }),
        );
        assert_eq!(date_scan.nodes.len(), 2);
        assert_eq!(
            date_scan.report.strategy,
            ScanPruningStrategy::PropertyRange {
                property: "updated_at".to_string()
            }
        );
        assert_eq!(date_scan.report.candidate_count_before_filter, 2);
    }

    #[test]
    fn scan_pruning_falls_back_for_unindexed_string_contains() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("title", Value::String("Graph foundations".to_string()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("title", Value::String("Storage notes".to_string()))]),
            )
            .unwrap();

        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            Some(label),
            Some(&PropertyFilter::Contains {
                property: "title".to_string(),
                value: "Graph".to_string(),
            }),
        );

        assert_eq!(scan.nodes.len(), 1);
        assert_eq!(scan.report.strategy, ScanPruningStrategy::FullLabelScan);
        assert!(!scan.report.pruned);
        assert_eq!(scan.report.candidate_count_before_filter, 2);
        assert_eq!(scan.report.filtered_out_count, 1);
    }

    #[test]
    fn list_values_round_trip_through_checkpoint() {
        let path = unique_test_dir("list_value_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([(
                        "tags",
                        Value::List(vec![
                            Value::String("graph".to_string()),
                            Value::String("storage".to_string()),
                        ]),
                    )]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let node = store.scan_nodes(None).next().unwrap();
            assert_eq!(
                node.properties.get("tags"),
                Some(&Value::List(vec![
                    Value::String("graph".to_string()),
                    Value::String("storage".to_string()),
                ]))
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn loads_projected_graph_artifact_from_checkpoint() {
        let path = unique_test_dir("projected_graph_artifact_cache");
        let definition = ProjectedGraphDefinition {
            node_labels: vec!["Memory".to_string()],
            rel_types: vec!["LINKS".to_string()],
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "LINKS", BTreeMap::new())
                .unwrap();
            store
                .register_projected_graph("MemoryGraph", definition.clone())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        let artifact_bytes = std::fs::read(path.join("projected_graphs.skein")).unwrap();
        assert!(artifact_bytes.starts_with(DURABLE_COMPRESSION_HEADER.as_bytes()));
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let artifact = store
                .projected_graph_artifact("MemoryGraph", &definition)
                .unwrap();
            assert_eq!(artifact.node_count(), 2);
            assert_eq!(artifact.edge_count(), 1);
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn ignores_projected_graph_artifact_after_wal_replay_advances_epoch() {
        let path = unique_test_dir("projected_graph_artifact_stale");
        let definition = ProjectedGraphDefinition {
            node_labels: vec!["Memory".to_string()],
            rel_types: vec!["LINKS".to_string()],
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "LINKS", BTreeMap::new())
                .unwrap();
            store
                .register_projected_graph("MemoryGraph", definition.clone())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
                .unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            assert!(store
                .projected_graph_artifact("MemoryGraph", &definition)
                .is_none());
            let status = store
                .projected_graph_statuses()
                .into_iter()
                .find(|status| status.name == "MemoryGraph")
                .unwrap();
            assert!(!status.reusable);
            assert_eq!(status.projection_epoch, None);
            assert_eq!(status.commit_epoch, None);
            assert_eq!(status.node_count, None);
            assert_eq!(status.edge_count, None);
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn stops_replay_at_torn_wal_tail() {
        let path = unique_test_dir("torn_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
        }
        std::fs::OpenOptions::new()
            .append(true)
            .open(path.join("wal.skein"))
            .unwrap()
            .write_all(b"torn-entry-without-checksum")
            .unwrap();

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let nodes = store.scan_nodes(Some(label)).collect::<Vec<_>>();
        assert_eq!(nodes.len(), 1);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn skips_torn_batch_wal_without_partial_path_recovery() {
        let path = unique_test_dir("torn_batch_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_connected_nodes(
                    &mut catalog,
                    ConnectedNodesCreate {
                        source_label: "Memory".to_string(),
                        source_properties: properties([("id", Value::Int(1))]),
                        rel_type: "MENTIONS".to_string(),
                        rel_properties: BTreeMap::new(),
                        target_label: "Entity".to_string(),
                        target_properties: properties([("id", Value::Int(10))]),
                    },
                )
                .unwrap();
        }
        let wal_path = path.join("wal.skein");
        let wal = std::fs::read_to_string(&wal_path).unwrap();
        let torn = wal.rsplit_once('\t').unwrap().0;
        std::fs::write(&wal_path, torn).unwrap();

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        assert!(store.scan_nodes(None).next().is_none());
        assert!(catalog.rel_type_id("MENTIONS").is_none());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn adaptive_histograms_use_medium_sample_for_medium_cardinality() {
        let nodes = histogram_nodes("score", 2_000);
        let statistics = compute_statistics(&nodes, &BTreeMap::new(), 1);
        let histogram = statistics
            .property_histograms
            .get(&(LabelId(0), "score".to_string()))
            .unwrap();

        assert_eq!(statistics.histogram_sample_limit, 512);
        assert_eq!(histogram.len(), 256);
        assert_eq!(histogram.first(), Some(&Value::Int(0)));
        assert_eq!(histogram.last(), Some(&Value::Int(1_999)));
        assert_eq!(
            statistics
                .property_distinct_counts
                .get(&(LabelId(0), "score".to_string())),
            Some(&2_000)
        );
        assert_eq!(
            statistics
                .sampled_property_histograms
                .get(&(LabelId(0), "score".to_string())),
            Some(&true)
        );
    }

    #[test]
    fn adaptive_histograms_use_max_sample_for_large_cardinality() {
        let nodes = histogram_nodes("score", 5_000);
        let statistics = compute_statistics(&nodes, &BTreeMap::new(), 1);
        let histogram = statistics
            .property_histograms
            .get(&(LabelId(0), "score".to_string()))
            .unwrap();

        assert_eq!(histogram.len(), 512);
        assert_eq!(histogram.first(), Some(&Value::Int(0)));
        assert_eq!(histogram.last(), Some(&Value::Int(4_999)));
    }

    #[test]
    fn statistics_track_relationship_property_distinct_counts() {
        let relationships = (0..10)
            .map(|id| {
                (
                    RelId(id),
                    RelRecord {
                        id: RelId(id),
                        source: NodeId(id),
                        target: NodeId(id + 100),
                        rel_type: RelTypeId(0),
                        properties: properties([("weight", Value::Int((id % 4) as i64))]),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        let statistics = compute_statistics(&BTreeMap::new(), &relationships, 1);

        assert_eq!(
            statistics
                .rel_property_distinct_counts
                .get(&(RelTypeId(0), "weight".to_string())),
            Some(&4)
        );
        assert_eq!(
            statistics
                .rel_property_histograms
                .get(&(RelTypeId(0), "weight".to_string()))
                .and_then(|values| values.first()),
            Some(&Value::Int(0))
        );
        assert_eq!(
            statistics
                .rel_property_histograms
                .get(&(RelTypeId(0), "weight".to_string()))
                .and_then(|values| values.last()),
            Some(&Value::Int(3))
        );
        assert_eq!(
            statistics
                .sampled_rel_property_histograms
                .get(&(RelTypeId(0), "weight".to_string())),
            Some(&false)
        );
    }

    #[test]
    fn statistics_track_path_source_and_target_coverage() {
        let nodes = [
            (0, LabelId(0)),
            (1, LabelId(0)),
            (10, LabelId(1)),
            (11, LabelId(1)),
            (12, LabelId(1)),
            (20, LabelId(1)),
            (21, LabelId(1)),
        ]
        .into_iter()
        .map(|(id, label)| {
            (
                NodeId(id),
                NodeRecord {
                    id: NodeId(id),
                    labels: BTreeSet::from([label]),
                    properties: BTreeMap::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
        let relationships = [
            (0, 0, 10),
            (1, 0, 11),
            (2, 1, 11),
            (3, 1, 12),
            (4, 1, 12),
            (5, 10, 20),
            (6, 11, 20),
            (7, 12, 21),
        ]
        .into_iter()
        .map(|(id, source, target)| {
            (
                RelId(id),
                RelRecord {
                    id: RelId(id),
                    source: NodeId(source),
                    target: NodeId(target),
                    rel_type: RelTypeId(0),
                    properties: BTreeMap::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

        let statistics = compute_statistics(&nodes, &relationships, 1);
        let path = (LabelId(0), RelTypeId(0), LabelId(1));

        assert_eq!(statistics.path_counts.get(&path), Some(&5));
        assert_eq!(statistics.path_source_distinct_counts.get(&path), Some(&2));
        assert_eq!(statistics.path_target_distinct_counts.get(&path), Some(&3));

        let two_hop_path = (LabelId(0), RelTypeId(0), LabelId(1), 2);
        assert_eq!(statistics.bounded_path_counts.get(&two_hop_path), Some(&5));
        assert_eq!(
            statistics
                .bounded_path_source_distinct_counts
                .get(&two_hop_path),
            Some(&2)
        );
        assert_eq!(
            statistics
                .bounded_path_target_distinct_counts
                .get(&two_hop_path),
            Some(&2)
        );
    }

    #[test]
    fn adaptive_histograms_track_relationship_sampling() {
        let relationships = (0..2_000)
            .map(|id| {
                (
                    RelId(id),
                    RelRecord {
                        id: RelId(id),
                        source: NodeId(id),
                        target: NodeId(id + 100),
                        rel_type: RelTypeId(0),
                        properties: properties([("score", Value::Int(id as i64))]),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        let statistics = compute_statistics(&BTreeMap::new(), &relationships, 1);
        let histogram = statistics
            .rel_property_histograms
            .get(&(RelTypeId(0), "score".to_string()))
            .unwrap();

        assert_eq!(histogram.len(), 256);
        assert_eq!(histogram.first(), Some(&Value::Int(0)));
        assert_eq!(histogram.last(), Some(&Value::Int(1_999)));
        assert_eq!(
            statistics
                .sampled_rel_property_histograms
                .get(&(RelTypeId(0), "score".to_string())),
            Some(&true)
        );
    }

    fn properties<const N: usize>(entries: [(&str, Value); N]) -> BTreeMap<String, Value> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    fn histogram_nodes(property: &str, count: u64) -> BTreeMap<NodeId, NodeRecord> {
        (0..count)
            .map(|id| {
                (
                    NodeId(id),
                    NodeRecord {
                        id: NodeId(id),
                        labels: BTreeSet::from([LabelId(0)]),
                        properties: properties([(property, Value::Int(id as i64))]),
                    },
                )
            })
            .collect()
    }

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_store_{name}_{nanos}"))
    }

    fn rewrite_checksummed_file(path: &std::path::Path, from: &str, to: &str, kind: &str) {
        let was_compressed = std::fs::read(path)
            .unwrap()
            .starts_with(DURABLE_COMPRESSION_HEADER.as_bytes());
        let text = read_durable_text(path, kind).unwrap();
        let (body, _) = text.rsplit_once("checksum\t").unwrap();
        let body = body.replace(from, to);
        let checksum = checksum_bytes(body.as_bytes());
        let rewritten = format!("{body}checksum\t{checksum}\n");
        if was_compressed {
            std::fs::write(
                path,
                encode_durable_text(&rewritten, DurableCompression::default()).unwrap(),
            )
            .unwrap();
        } else {
            std::fs::write(path, rewritten.as_bytes()).unwrap();
        }
        let rewritten = read_durable_text(path, kind).unwrap();
        assert!(
            rewritten.contains(to),
            "{kind} rewrite did not update storage version"
        );
    }
}
