//! Immutable relational index visibility and bounded committed/transaction overlays.
//!
//! The embedded facade selects and publishes these views. Storage owns the
//! pinned readers, ordered merge, and cumulative constraint-read accounting.

mod authoritative;
mod constraint_qualification;
mod qualification;
mod row_source;
mod transaction;

pub use authoritative::AuthoritativeRelationalConstraintIndex;
pub use constraint_qualification::{
    ConstraintProbeIdentity, RelationalConstraintQualificationProbeReport,
    RelationalConstraintQualificationReport, RelationalConstraintQualificationUse,
    RELATIONAL_CONSTRAINT_QUALIFICATION_PROTOCOL,
};
pub use qualification::{
    RelationalIndexQualificationProbe, RelationalIndexQualificationProbeKind,
    RelationalIndexQualificationProbeReport, RelationalIndexViewQualificationOptions,
    RelationalIndexViewQualificationReport, RELATIONAL_INDEX_VIEW_QUALIFICATION_PROTOCOL,
};
pub use row_source::{map_index_row_snapshot_error, CanonicalRelationalIndexRowSource};
pub use transaction::RelationalTransactionIndexView;

use crate::{
    RelationalIndexChangeCapture, RelationalIndexChangeCaptureLimits, RelationalIndexChangeKind,
    RelationalIndexRangeScan, RelationalIndexReadLimits, RelationalIndexReadReport,
    RelationalIndexRecoveryReadReport, RelationalIndexRecoveryReader, RelationalIndexScanDirection,
    RelationalIndexShadowError, RelationalIndexShadowManifest, RelationalIndexShadowReader,
    RelationalKey,
};
use skein_integrity::Sha256Digest;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroUsize,
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalIndexReadViewBackendReport {
    Base(RelationalIndexReadReport),
    Recovered(RelationalIndexRecoveryReadReport),
}

impl RelationalIndexReadViewBackendReport {
    fn bytes_read(&self) -> Option<usize> {
        match self {
            Self::Base(report) => Some(report.bytes_read),
            Self::Recovered(report) => report.base.bytes_read.checked_add(report.delta_bytes_read),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexReadViewReport {
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub root_set_digest: String,
    pub backend: RelationalIndexReadViewBackendReport,
    pub live_batches_visited: usize,
    pub live_entries_visited: usize,
    pub live_entries_matched: usize,
    pub live_bytes_visited: usize,
    pub rows_visited: usize,
    pub stopped_early: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationalIndexStorageResidencyReport {
    pub serving: bool,
    pub base_generation: Option<u64>,
    pub recovery_delta_generation: Option<u64>,
    pub base_commit_epoch: Option<u64>,
    pub visible_commit_epoch: Option<u64>,
    pub root_count: usize,
    pub base_page_count: u64,
    pub base_artifact_bytes: u64,
    pub recovery_delta_pages: usize,
    pub recovery_delta_entries: usize,
    pub recovery_delta_artifact_bytes: u64,
    pub live_batches: usize,
    pub live_entries: usize,
    pub live_encoded_bytes: usize,
}

impl RelationalIndexStorageResidencyReport {
    pub fn canonical_artifact_bytes(&self) -> u64 {
        self.base_artifact_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalIndexReadViewKind {
    Base,
    Recovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexReadViewIdentity {
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub root_set_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexProbeStatistics {
    /// Checkpoint epoch that produced every count in this snapshot.
    pub source_commit_epoch: u64,
    pub distinct_non_null_values: u64,
    pub non_null_rows: u64,
    /// Maximum rows observed for one prefix value. This bounds skew but is
    /// intentionally not the selectivity estimate for an ordinary probe.
    pub fanout: u64,
}

impl RelationalIndexProbeStatistics {
    /// Returns the exact average number of rows per non-null prefix value,
    /// rounded up so the planner never estimates an existing probe as empty.
    pub fn average_fanout(self) -> u64 {
        if self.distinct_non_null_values == 0 {
            return 0;
        }
        self.non_null_rows.div_ceil(self.distinct_non_null_values)
    }
}

#[derive(Clone)]
enum RelationalIndexReadBackend {
    Base(Arc<RelationalIndexShadowReader>),
    Recovered(Arc<RelationalIndexRecoveryReader>),
}

#[derive(Debug)]
struct RelationalIndexLiveChange {
    index_key: Arc<RelationalKey>,
    primary_key: RelationalKey,
    kind: RelationalIndexChangeKind,
    encoded_bytes: usize,
}

#[derive(Debug)]
struct RelationalIndexLiveBatch {
    commit_epoch: u64,
    changes: Arc<[RelationalIndexLiveChange]>,
}

#[derive(Debug, Default)]
struct RelationalIndexLiveBatchVisit {
    entries_visited: usize,
    bytes_visited: usize,
}

impl RelationalIndexLiveBatchVisit {
    fn include(
        &mut self,
        change: &RelationalIndexLiveChange,
    ) -> std::result::Result<(), RelationalIndexShadowError> {
        self.entries_visited = self
            .entries_visited
            .checked_add(1)
            .ok_or_else(|| admission("relational index live entry counter overflow"))?;
        self.bytes_visited = self
            .bytes_visited
            .checked_add(change.encoded_bytes)
            .ok_or_else(|| admission("relational index live byte counter overflow"))?;
        Ok(())
    }

    fn merge(&mut self, other: Self) -> std::result::Result<(), RelationalIndexShadowError> {
        self.entries_visited = self
            .entries_visited
            .checked_add(other.entries_visited)
            .ok_or_else(|| admission("relational index live entry counter overflow"))?;
        self.bytes_visited = self
            .bytes_visited
            .checked_add(other.bytes_visited)
            .ok_or_else(|| admission("relational index live byte counter overflow"))?;
        Ok(())
    }
}

impl RelationalIndexLiveBatch {
    fn prefix_changes(&self, prefix: &RelationalKey) -> &[RelationalIndexLiveChange] {
        let start = self
            .changes
            .partition_point(|change| change.index_key.as_ref() < prefix);
        let width = self.changes[start..]
            .partition_point(|change| change.index_key.0.starts_with(&prefix.0));
        &self.changes[start..start.saturating_add(width)]
    }

    fn exact_changes(&self, key: &RelationalKey) -> &[RelationalIndexLiveChange] {
        let start = self
            .changes
            .partition_point(|change| change.index_key.as_ref() < key);
        let width =
            self.changes[start..].partition_point(|change| change.index_key.as_ref() == key);
        &self.changes[start..start.saturating_add(width)]
    }

    fn visit_selector(
        &self,
        selector: RelationalIndexReadSelector<'_>,
        visit: impl FnMut(
            &RelationalIndexLiveChange,
        ) -> std::result::Result<(), RelationalIndexShadowError>,
    ) -> std::result::Result<RelationalIndexLiveBatchVisit, RelationalIndexShadowError> {
        match selector {
            RelationalIndexReadSelector::Exact(key) => {
                visit_live_index_changes(self.exact_changes(key).iter(), visit)
            }
            RelationalIndexReadSelector::Prefix(prefix) => {
                visit_live_index_changes(self.prefix_changes(prefix).iter(), visit)
            }
            RelationalIndexReadSelector::Range(scan) => {
                let changes = self.prefix_changes(&scan.prefix);
                match scan.direction {
                    RelationalIndexScanDirection::Forward => visit_live_index_changes(
                        changes.iter().filter(|change| {
                            scan.exclusive_bound
                                .as_ref()
                                .is_none_or(|bound| change.index_key.as_ref() > bound)
                        }),
                        visit,
                    ),
                    RelationalIndexScanDirection::Backward => visit_live_index_changes(
                        changes.iter().rev().filter(|change| {
                            scan.exclusive_bound
                                .as_ref()
                                .is_none_or(|bound| change.index_key.as_ref() < bound)
                        }),
                        visit,
                    ),
                }
            }
        }
    }

    fn visit_prefixes(
        &self,
        prefixes: &BTreeSet<RelationalKey>,
        mut visit: impl FnMut(
            &RelationalIndexLiveChange,
        ) -> std::result::Result<(), RelationalIndexShadowError>,
    ) -> std::result::Result<RelationalIndexLiveBatchVisit, RelationalIndexShadowError> {
        let mut report = RelationalIndexLiveBatchVisit::default();
        for prefix in prefixes {
            report.merge(
                self.visit_selector(RelationalIndexReadSelector::Prefix(prefix), &mut visit)?,
            )?;
        }
        Ok(report)
    }
}

fn visit_live_index_changes<'a>(
    changes: impl Iterator<Item = &'a RelationalIndexLiveChange>,
    mut visit: impl FnMut(
        &RelationalIndexLiveChange,
    ) -> std::result::Result<(), RelationalIndexShadowError>,
) -> std::result::Result<RelationalIndexLiveBatchVisit, RelationalIndexShadowError> {
    let mut report = RelationalIndexLiveBatchVisit::default();
    for change in changes {
        report.include(change)?;
        visit(change)?;
    }
    Ok(report)
}

type RelationalIndexLiveBatchChain = Vec<Arc<RelationalIndexLiveBatch>>;
type RelationalIndexLivePartitions =
    BTreeMap<String, BTreeMap<String, Arc<RelationalIndexLiveBatchChain>>>;

#[derive(Debug, Clone)]
struct RelationalIndexLiveOverlay {
    partitions: Arc<RelationalIndexLivePartitions>,
    batch_count: usize,
    entry_count: usize,
    encoded_bytes: usize,
}

impl RelationalIndexLiveOverlay {
    fn empty() -> Self {
        Self {
            partitions: Arc::new(BTreeMap::new()),
            batch_count: 0,
            entry_count: 0,
            encoded_bytes: 0,
        }
    }

    fn append(
        &self,
        commit_epoch: u64,
        capture: RelationalIndexChangeCapture,
        limits: RelationalIndexChangeCaptureLimits,
    ) -> Result<Self, String> {
        let (changes, encoded_bytes) = match capture {
            RelationalIndexChangeCapture::Captured {
                changes,
                encoded_bytes,
            } => (changes, encoded_bytes),
            RelationalIndexChangeCapture::Invalidated { reason } => return Err(reason),
        };
        if changes.is_empty() {
            return Ok(self.clone());
        }
        let entry_count = self
            .entry_count
            .checked_add(changes.len())
            .ok_or_else(|| "relational index live entry accounting overflow".to_string())?;
        let total_bytes = self
            .encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| "relational index live byte accounting overflow".to_string())?;
        if entry_count > limits.max_entries.get() || total_bytes > limits.max_bytes.get() {
            return Err(format!(
                "relational index live overlay exceeds max_entries={} or max_bytes={}",
                limits.max_entries, limits.max_bytes
            ));
        }
        let mut captured_bytes = 0usize;
        let mut captured = BTreeMap::<
            String,
            BTreeMap<
                String,
                BTreeMap<
                    Arc<RelationalKey>,
                    Vec<(RelationalKey, RelationalIndexChangeKind, usize)>,
                >,
            >,
        >::new();
        for change in changes {
            let change_bytes = change.estimated_encoded_bytes().ok_or_else(|| {
                "relational index live change has an invalid encoded size".to_string()
            })?;
            captured_bytes = captured_bytes
                .checked_add(change_bytes)
                .ok_or_else(|| "relational index live byte accounting overflow".to_string())?;
            captured
                .entry(change.table)
                .or_default()
                .entry(change.index)
                .or_default()
                .entry(Arc::new(change.index_key))
                .or_default()
                .push((change.primary_key, change.kind, change_bytes));
        }
        if captured_bytes != encoded_bytes {
            return Err(format!(
                "relational index live capture declares {encoded_bytes} bytes but contains {captured_bytes} bytes"
            ));
        }

        let batch_count = self
            .batch_count
            .checked_add(1)
            .ok_or_else(|| "relational index live batch accounting overflow".to_string())?;
        let mut partitions = Arc::clone(&self.partitions);
        {
            let partition_map = Arc::make_mut(&mut partitions);
            for (table, indexes) in captured {
                let target_indexes = partition_map.entry(table).or_default();
                for (index, index_keys) in indexes {
                    let mut partition_changes = Vec::new();
                    for (index_key, changes) in index_keys {
                        for (primary_key, kind, change_bytes) in changes {
                            partition_changes.push(RelationalIndexLiveChange {
                                index_key: Arc::clone(&index_key),
                                primary_key,
                                kind,
                                encoded_bytes: change_bytes,
                            });
                        }
                    }
                    let batches = target_indexes
                        .entry(index)
                        .or_insert_with(|| Arc::new(Vec::new()));
                    Arc::make_mut(batches).push(Arc::new(RelationalIndexLiveBatch {
                        commit_epoch,
                        changes: Arc::from(partition_changes),
                    }));
                }
            }
        }
        Ok(Self {
            partitions,
            batch_count,
            entry_count,
            encoded_bytes: total_bytes,
        })
    }

    fn batch_count(&self) -> usize {
        self.batch_count
    }

    fn batches(&self, table: &str, index: &str) -> Option<&[Arc<RelationalIndexLiveBatch>]> {
        self.partitions
            .get(table)?
            .get(index)
            .map(|batches| batches.as_slice())
    }

    fn touches(&self, table: &str, index: &str) -> bool {
        self.batches(table, index).is_some()
    }
}

#[derive(Clone, Copy)]
enum RelationalIndexReadSelector<'a> {
    Exact(&'a RelationalKey),
    Prefix(&'a RelationalKey),
    Range(&'a RelationalIndexRangeScan),
}

impl RelationalIndexReadSelector<'_> {
    fn matches(&self, key: &RelationalKey) -> bool {
        match self {
            Self::Exact(expected) => key == *expected,
            Self::Prefix(prefix) => key.0.starts_with(&prefix.0),
            Self::Range(scan) => scan.matches(key),
        }
    }
}

struct OrderedIndexEntryMerge<'a, F> {
    pending: BTreeMap<(RelationalKey, RelationalKey), RelationalIndexChangeKind>,
    visit: &'a mut F,
    max_rows: usize,
    rows_visited: usize,
    stopped_early: bool,
    error: Option<RelationalIndexShadowError>,
    direction: RelationalIndexScanDirection,
}

impl<F> OrderedIndexEntryMerge<'_, F>
where
    F: FnMut(&RelationalKey, &RelationalKey) -> bool,
{
    fn emit(&mut self, index_key: &RelationalKey, primary_key: &RelationalKey) -> bool {
        let Some(rows_visited) = self.rows_visited.checked_add(1) else {
            self.error = Some(admission("relational index output row counter overflow"));
            return false;
        };
        if rows_visited > self.max_rows {
            self.error = Some(admission(format!(
                "relational index read exceeds row limit {} after ordered merge",
                self.max_rows
            )));
            return false;
        }
        self.rows_visited = rows_visited;
        if !(self.visit)(index_key, primary_key) {
            self.stopped_early = true;
            return false;
        }
        true
    }

    fn visit_base(&mut self, index_key: &RelationalKey, primary_key: &RelationalKey) -> bool {
        let base_entry = (index_key.clone(), primary_key.clone());
        while match self.direction {
            RelationalIndexScanDirection::Forward => self
                .pending
                .first_key_value()
                .is_some_and(|(entry, _)| entry < &base_entry),
            RelationalIndexScanDirection::Backward => self
                .pending
                .last_key_value()
                .is_some_and(|(entry, _)| entry > &base_entry),
        } {
            let pending = match self.direction {
                RelationalIndexScanDirection::Forward => self.pending.pop_first(),
                RelationalIndexScanDirection::Backward => self.pending.pop_last(),
            };
            let Some(((pending_index_key, pending_primary_key), kind)) = pending else {
                break;
            };
            if kind == RelationalIndexChangeKind::Insert
                && !self.emit(&pending_index_key, &pending_primary_key)
            {
                return false;
            }
        }
        match self.pending.remove(&base_entry) {
            Some(RelationalIndexChangeKind::Delete) => true,
            Some(RelationalIndexChangeKind::Insert) | None => self.emit(index_key, primary_key),
        }
    }

    fn finish(&mut self) {
        while !self.stopped_early && self.error.is_none() {
            let pending = match self.direction {
                RelationalIndexScanDirection::Forward => self.pending.pop_first(),
                RelationalIndexScanDirection::Backward => self.pending.pop_last(),
            };
            let Some(((index_key, primary_key), kind)) = pending else {
                break;
            };
            if kind == RelationalIndexChangeKind::Insert && !self.emit(&index_key, &primary_key) {
                break;
            }
        }
    }
}

/// One immutable, generation-bound relational index view.
///
/// The outer `Arc` is cloned into embedded-store snapshots. The selected base
/// and recovery manifests therefore cannot drift underneath a pinned reader,
/// while a newer store publication can install another view independently.
pub struct RelationalIndexReadView {
    identity: RelationalIndexReadViewIdentity,
    backend: RelationalIndexReadBackend,
    live: RelationalIndexLiveOverlay,
}

impl fmt::Debug for RelationalIndexReadView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationalIndexReadView")
            .field("identity", &self.identity)
            .field("kind", &self.kind())
            .field("poisoned", &self.is_poisoned())
            .finish()
    }
}

impl RelationalIndexReadView {
    pub fn from_base(reader: RelationalIndexShadowReader) -> Self {
        let manifest = reader.manifest();
        Self {
            identity: RelationalIndexReadViewIdentity {
                base_generation: manifest.generation,
                delta_generation: None,
                base_commit_epoch: manifest.source_commit_epoch,
                visible_commit_epoch: manifest.source_commit_epoch,
                root_set_digest: manifest.root_set_digest,
            },
            backend: RelationalIndexReadBackend::Base(Arc::new(reader)),
            live: RelationalIndexLiveOverlay::empty(),
        }
    }

    pub fn from_recovered(reader: RelationalIndexRecoveryReader) -> Self {
        let base = reader.base_manifest();
        let recovered = reader.manifest();
        Self {
            identity: RelationalIndexReadViewIdentity {
                base_generation: base.generation,
                delta_generation: Some(recovered.delta_generation),
                base_commit_epoch: base.source_commit_epoch,
                visible_commit_epoch: recovered.recovered_commit_epoch,
                root_set_digest: base.root_set_digest,
            },
            backend: RelationalIndexReadBackend::Recovered(Arc::new(reader)),
            live: RelationalIndexLiveOverlay::empty(),
        }
    }

    pub fn identity(&self) -> RelationalIndexReadViewIdentity {
        self.identity
    }

    pub fn kind(&self) -> RelationalIndexReadViewKind {
        match self.backend {
            RelationalIndexReadBackend::Base(_) => RelationalIndexReadViewKind::Base,
            RelationalIndexReadBackend::Recovered(_) => RelationalIndexReadViewKind::Recovered,
        }
    }

    pub fn is_poisoned(&self) -> bool {
        match &self.backend {
            RelationalIndexReadBackend::Base(reader) => reader.is_poisoned(),
            RelationalIndexReadBackend::Recovered(reader) => reader.is_poisoned(),
        }
    }

    pub fn fresh_probe_statistics(
        &self,
        table: &str,
        index: &str,
        prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics> {
        // A manifest aggregate remains exact across graph-only commits and
        // writes to other indexes. A matching live partition or recovered
        // delta, however, cannot derive per-prefix distinct counts safely.
        if self.live.touches(table, index)
            || matches!(&self.backend, RelationalIndexReadBackend::Recovered(_))
        {
            return None;
        }
        let manifest = match &self.backend {
            RelationalIndexReadBackend::Base(reader) => reader.manifest(),
            RelationalIndexReadBackend::Recovered(reader) => reader.base_manifest(),
        };
        let statistics = manifest
            .root(table, index)?
            .statistics
            .leading_prefix(prefix_len)?;
        Some(RelationalIndexProbeStatistics {
            source_commit_epoch: manifest.source_commit_epoch,
            distinct_non_null_values: statistics.distinct_non_null_values,
            non_null_rows: statistics.non_null_rows,
            fanout: statistics.fanout,
        })
    }

    pub fn advance(
        &self,
        next_commit_epoch: u64,
        capture: Option<RelationalIndexChangeCapture>,
        limits: RelationalIndexChangeCaptureLimits,
    ) -> Result<Self, String> {
        let expected = self
            .identity
            .visible_commit_epoch
            .checked_add(1)
            .ok_or_else(|| "relational index read-view epoch overflow".to_string())?;
        if next_commit_epoch != expected {
            return Err(format!(
                "relational index read view expected commit epoch {expected}, got {next_commit_epoch}"
            ));
        }
        if self.is_poisoned() {
            return Err("relational index read view is poisoned".to_string());
        }
        let live = capture.map_or_else(
            || Ok(self.live.clone()),
            |capture| self.live.append(next_commit_epoch, capture, limits),
        )?;
        let mut identity = self.identity;
        identity.visible_commit_epoch = next_commit_epoch;
        Ok(Self {
            identity,
            backend: self.backend.clone(),
            live,
        })
    }

    pub fn live_batch_count(&self) -> usize {
        self.live.batch_count()
    }

    pub fn live_entry_count(&self) -> usize {
        self.live.entry_count
    }

    pub fn live_encoded_bytes(&self) -> usize {
        self.live.encoded_bytes
    }

    pub fn residency_report(&self) -> RelationalIndexStorageResidencyReport {
        let identity = self.identity();
        let (base, recovery) = match &self.backend {
            RelationalIndexReadBackend::Base(reader) => (reader.manifest(), None),
            RelationalIndexReadBackend::Recovered(reader) => {
                (reader.base_manifest(), Some(reader.manifest()))
            }
        };
        RelationalIndexStorageResidencyReport {
            serving: true,
            base_generation: Some(identity.base_generation),
            recovery_delta_generation: identity.delta_generation,
            base_commit_epoch: Some(identity.base_commit_epoch),
            visible_commit_epoch: Some(identity.visible_commit_epoch),
            root_count: base.roots.len(),
            base_page_count: base.page_count,
            base_artifact_bytes: base.page_bytes.saturating_mul(base.page_count),
            recovery_delta_pages: recovery.map_or(0, |manifest| manifest.delta_pages()),
            recovery_delta_entries: recovery.map_or(0, |manifest| manifest.delta_entries()),
            recovery_delta_artifact_bytes: recovery.map_or(0, |manifest| manifest.artifact_bytes()),
            live_batches: self.live_batch_count(),
            live_entries: self.live_entry_count(),
            live_encoded_bytes: self.live_encoded_bytes(),
        }
    }

    pub fn visit_exact_postings(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey) -> bool,
    ) -> std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.visit_postings(
            table,
            index,
            RelationalIndexReadSelector::Exact(key),
            limits,
            visit,
        )
    }

    pub fn visit_prefix_postings(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey) -> bool,
    ) -> std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.visit_postings(
            table,
            index,
            RelationalIndexReadSelector::Prefix(prefix),
            limits,
            visit,
        )
    }

    pub fn visit_prefix_entries(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.visit_range_entries(
            table,
            index,
            &RelationalIndexRangeScan {
                prefix: prefix.clone(),
                exclusive_bound: None,
                direction: RelationalIndexScanDirection::Forward,
            },
            limits,
            visit,
        )
    }

    pub fn visit_prefix_entries_many(
        &self,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        if prefixes.is_empty() {
            return Err(admission("batch index lookup requires at least one prefix"));
        }
        let prefix_width = prefixes[0].0.len();
        if prefixes.iter().any(|prefix| prefix.0.len() != prefix_width) {
            return Err(admission(
                "batch index prefixes must have one common key width",
            ));
        }
        if self.is_poisoned() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index read view is poisoned".to_string(),
            ));
        }

        let mut live_entries_visited = 0usize;
        let mut live_entries_matched = 0usize;
        let mut live_bytes_visited = 0usize;
        let mut pending_live = BTreeMap::new();
        let mut previous_epoch = self.durable_commit_epoch();
        let mut live_batches_visited = 0usize;
        let selected_prefixes = prefixes.iter().cloned().collect::<BTreeSet<_>>();
        for batch in self.live.batches(table, index).unwrap_or(&[]) {
            if batch.commit_epoch <= previous_epoch
                || batch.commit_epoch > self.identity.visible_commit_epoch
            {
                return Err(RelationalIndexShadowError::Corrupt(format!(
                    "relational index live batch epoch {} is outside ({previous_epoch}, {}]",
                    batch.commit_epoch, self.identity.visible_commit_epoch
                )));
            }
            previous_epoch = batch.commit_epoch;
            live_batches_visited = live_batches_visited
                .checked_add(1)
                .ok_or_else(|| admission("relational index live batch counter overflow"))?;
            let batch_report = batch.visit_prefixes(&selected_prefixes, |change| {
                pending_live.insert(
                    (change.index_key.as_ref().clone(), change.primary_key.clone()),
                    change.kind,
                );
                if pending_live.len() >= limits.max_rows.get() {
                    return Err(admission(format!(
                        "relational index live batch merge needs {} entries, exhausting row limit {}",
                        pending_live.len(),
                        limits.max_rows
                    )));
                }
                Ok(())
            })?;
            live_entries_visited = live_entries_visited
                .checked_add(batch_report.entries_visited)
                .ok_or_else(|| admission("relational index live entry counter overflow"))?;
            live_entries_matched = live_entries_matched
                .checked_add(batch_report.entries_visited)
                .ok_or_else(|| admission("relational index matched-live counter overflow"))?;
            live_bytes_visited = live_bytes_visited
                .checked_add(batch_report.bytes_visited)
                .ok_or_else(|| admission("relational index live byte counter overflow"))?;
        }
        let backend_byte_limit = limits
            .max_bytes
            .get()
            .checked_sub(live_bytes_visited)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission(format!(
                    "relational index live batch merge needs {live_bytes_visited} bytes, exhausting byte limit {}",
                    limits.max_bytes
                ))
            })?;
        let backend_row_limit = limits
            .max_rows
            .get()
            .checked_sub(pending_live.len())
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission("relational index live batch merge exhausted its row budget")
            })?;
        let backend_limits = RelationalIndexReadLimits {
            max_rows: backend_row_limit,
            max_bytes: backend_byte_limit,
            ..limits
        };
        let mut merge = OrderedIndexEntryMerge {
            pending: pending_live,
            visit: &mut visit,
            max_rows: limits.max_rows.get(),
            rows_visited: 0,
            stopped_early: false,
            error: None,
            direction: RelationalIndexScanDirection::Forward,
        };
        let backend = {
            let mut emit_backend = |index_key: &RelationalKey, primary_key: &RelationalKey| {
                merge.visit_base(index_key, primary_key)
            };
            match &self.backend {
                RelationalIndexReadBackend::Base(reader) => {
                    RelationalIndexReadViewBackendReport::Base(reader.visit_prefix_entries_many(
                        table,
                        index,
                        prefixes,
                        backend_limits,
                        |_, index_key, primary_key| emit_backend(index_key, primary_key),
                    )?)
                }
                RelationalIndexReadBackend::Recovered(reader) => {
                    RelationalIndexReadViewBackendReport::Recovered(
                        reader.visit_prefix_entries_many(
                            table,
                            index,
                            prefixes,
                            backend_limits,
                            &mut emit_backend,
                        )?,
                    )
                }
            }
        };
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        merge.finish();
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        let total_bytes = backend
            .bytes_read()
            .ok_or_else(|| admission("relational index backend byte counter overflow"))?
            .checked_add(live_bytes_visited)
            .ok_or_else(|| admission("relational index batch read byte counter overflow"))?;
        if total_bytes > limits.max_bytes.get() {
            return Err(admission(format!(
                "relational index batch read needs {total_bytes} bytes including live changes, exceeding byte limit {}",
                limits.max_bytes
            )));
        }
        let mut report = RelationalIndexReadViewReport {
            base_generation: self.identity.base_generation,
            delta_generation: self.identity.delta_generation,
            base_commit_epoch: self.identity.base_commit_epoch,
            visible_commit_epoch: self.identity.visible_commit_epoch,
            root_set_digest: self.identity.root_set_digest.to_string(),
            backend,
            live_batches_visited,
            live_entries_visited,
            live_entries_matched,
            live_bytes_visited,
            rows_visited: merge.rows_visited,
            stopped_early: merge.stopped_early,
        };
        report.stopped_early |= match &report.backend {
            RelationalIndexReadViewBackendReport::Base(backend) => backend.stopped_early,
            RelationalIndexReadViewBackendReport::Recovered(backend) => backend.stopped_early,
        };
        Ok(report)
    }

    pub fn visit_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        if self.is_poisoned() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index read view is poisoned".to_string(),
            ));
        }
        let selector = RelationalIndexReadSelector::Range(scan);
        let mut live_entries_visited = 0usize;
        let mut live_entries_matched = 0usize;
        let mut live_bytes_visited = 0usize;
        let mut pending_live = BTreeMap::new();
        let mut previous_epoch = self.durable_commit_epoch();
        let mut live_batches_visited = 0usize;
        for batch in self.live.batches(table, index).unwrap_or(&[]) {
            if batch.commit_epoch <= previous_epoch
                || batch.commit_epoch > self.identity.visible_commit_epoch
            {
                return Err(RelationalIndexShadowError::Corrupt(format!(
                    "relational index live batch epoch {} is outside ({previous_epoch}, {}]",
                    batch.commit_epoch, self.identity.visible_commit_epoch
                )));
            }
            previous_epoch = batch.commit_epoch;
            live_batches_visited = live_batches_visited
                .checked_add(1)
                .ok_or_else(|| admission("relational index live batch counter overflow"))?;
            let batch_report = batch.visit_selector(selector, |change| {
                pending_live.insert(
                    (change.index_key.as_ref().clone(), change.primary_key.clone()),
                    change.kind,
                );
                if pending_live.len() >= limits.max_rows.get() {
                    return Err(admission(format!(
                        "relational index live ordered merge needs {} entries, exhausting row limit {}",
                        pending_live.len(),
                        limits.max_rows
                    )));
                }
                Ok(())
            })?;
            live_entries_visited = live_entries_visited
                .checked_add(batch_report.entries_visited)
                .ok_or_else(|| admission("relational index live entry counter overflow"))?;
            live_entries_matched = live_entries_matched
                .checked_add(batch_report.entries_visited)
                .ok_or_else(|| admission("relational index matched-live counter overflow"))?;
            live_bytes_visited = live_bytes_visited
                .checked_add(batch_report.bytes_visited)
                .ok_or_else(|| admission("relational index live byte counter overflow"))?;
        }
        let backend_byte_limit = limits
            .max_bytes
            .get()
            .checked_sub(live_bytes_visited)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission(format!(
                    "relational index live merge needs {live_bytes_visited} bytes, exhausting byte limit {}",
                    limits.max_bytes
                ))
            })?;
        let backend_row_limit = limits
            .max_rows
            .get()
            .checked_sub(pending_live.len())
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission("relational index live ordered merge exhausted its row budget")
            })?;
        let backend_limits = RelationalIndexReadLimits {
            max_rows: backend_row_limit,
            max_bytes: backend_byte_limit,
            ..limits
        };
        let mut merge = OrderedIndexEntryMerge {
            pending: pending_live,
            visit: &mut visit,
            max_rows: limits.max_rows.get(),
            rows_visited: 0,
            stopped_early: false,
            error: None,
            direction: scan.direction,
        };
        let backend = {
            let mut emit_backend = |index_key: &RelationalKey, primary_key: &RelationalKey| {
                merge.visit_base(index_key, primary_key)
            };
            match &self.backend {
                RelationalIndexReadBackend::Base(reader) => {
                    RelationalIndexReadViewBackendReport::Base(reader.visit_range_entries(
                        table,
                        index,
                        scan,
                        backend_limits,
                        &mut emit_backend,
                    )?)
                }
                RelationalIndexReadBackend::Recovered(reader) => {
                    RelationalIndexReadViewBackendReport::Recovered(reader.visit_range_entries(
                        table,
                        index,
                        scan,
                        backend_limits,
                        &mut emit_backend,
                    )?)
                }
            }
        };
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        merge.finish();
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        let total_bytes = backend
            .bytes_read()
            .ok_or_else(|| admission("relational index backend byte counter overflow"))?
            .checked_add(live_bytes_visited)
            .ok_or_else(|| admission("relational index read byte counter overflow"))?;
        if total_bytes > limits.max_bytes.get() {
            return Err(admission(format!(
                "relational index read needs {total_bytes} bytes including live changes, exceeding byte limit {}",
                limits.max_bytes
            )));
        }
        let mut report = RelationalIndexReadViewReport {
            base_generation: self.identity.base_generation,
            delta_generation: self.identity.delta_generation,
            base_commit_epoch: self.identity.base_commit_epoch,
            visible_commit_epoch: self.identity.visible_commit_epoch,
            root_set_digest: self.identity.root_set_digest.to_string(),
            backend,
            live_batches_visited,
            live_entries_visited,
            live_entries_matched,
            live_bytes_visited,
            rows_visited: merge.rows_visited,
            stopped_early: merge.stopped_early,
        };
        report.stopped_early |= match &report.backend {
            RelationalIndexReadViewBackendReport::Base(backend) => backend.stopped_early,
            RelationalIndexReadViewBackendReport::Recovered(backend) => backend.stopped_early,
        };
        Ok(report)
    }

    fn visit_postings(
        &self,
        table: &str,
        index: &str,
        selector: RelationalIndexReadSelector<'_>,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey) -> bool,
    ) -> std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        if self.is_poisoned() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index read view is poisoned".to_string(),
            ));
        }
        let mut live_entries_visited = 0usize;
        let mut live_entries_matched = 0usize;
        let mut live_bytes_visited = 0usize;
        let mut pending_live = BTreeMap::new();
        let mut previous_epoch = self.durable_commit_epoch();
        let mut live_batches_visited = 0usize;
        for batch in self.live.batches(table, index).unwrap_or(&[]) {
            if batch.commit_epoch <= previous_epoch
                || batch.commit_epoch > self.identity.visible_commit_epoch
            {
                return Err(RelationalIndexShadowError::Corrupt(format!(
                    "relational index live batch epoch {} is outside ({previous_epoch}, {}]",
                    batch.commit_epoch, self.identity.visible_commit_epoch
                )));
            }
            previous_epoch = batch.commit_epoch;
            live_batches_visited = live_batches_visited
                .checked_add(1)
                .ok_or_else(|| admission("relational index live batch counter overflow"))?;
            let mut batch_pending = BTreeMap::new();
            let batch_report = batch.visit_selector(selector, |change| {
                match change.kind {
                    RelationalIndexChangeKind::Insert => {
                        batch_pending.insert(change.primary_key.clone(), change.kind);
                    }
                    RelationalIndexChangeKind::Delete => {
                        batch_pending
                            .entry(change.primary_key.clone())
                            .or_insert(change.kind);
                    }
                }
                if batch_pending.len() >= limits.max_rows.get() {
                    return Err(admission(format!(
                        "relational index live merge needs {} row locators, exhausting row limit {}",
                        batch_pending.len(), limits.max_rows
                    )));
                }
                Ok(())
            })?;
            for (primary_key, kind) in batch_pending {
                pending_live.insert(primary_key, kind);
                if pending_live.len() >= limits.max_rows.get() {
                    return Err(admission(format!(
                        "relational index live merge needs {} row locators, exhausting row limit {}",
                        pending_live.len(), limits.max_rows
                    )));
                }
            }
            live_entries_visited = live_entries_visited
                .checked_add(batch_report.entries_visited)
                .ok_or_else(|| admission("relational index live entry counter overflow"))?;
            live_entries_matched = live_entries_matched
                .checked_add(batch_report.entries_visited)
                .ok_or_else(|| admission("relational index matched-live counter overflow"))?;
            live_bytes_visited = live_bytes_visited
                .checked_add(batch_report.bytes_visited)
                .ok_or_else(|| admission("relational index live byte counter overflow"))?;
        }
        let backend_byte_limit = limits
            .max_bytes
            .get()
            .checked_sub(live_bytes_visited)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                admission(format!(
                    "relational index live merge needs {live_bytes_visited} bytes, exhausting byte limit {}",
                    limits.max_bytes
                ))
            })?;
        let backend_row_limit = limits
            .max_rows
            .get()
            .checked_sub(pending_live.len())
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| admission("relational index live merge exhausted its row budget"))?;
        let backend_limits = RelationalIndexReadLimits {
            max_rows: backend_row_limit,
            max_bytes: backend_byte_limit,
            ..limits
        };
        let mut rows_visited = 0usize;
        let mut stopped_early = false;
        let mut callback_error = None;
        let backend = {
            let mut emit_backend = |primary_key: &RelationalKey| {
                if matches!(
                    pending_live.remove(primary_key),
                    Some(RelationalIndexChangeKind::Delete)
                ) {
                    return true;
                }
                let Some(next_rows_visited) = rows_visited.checked_add(1) else {
                    callback_error =
                        Some(admission("relational index output row counter overflow"));
                    return false;
                };
                if next_rows_visited > limits.max_rows.get() {
                    callback_error = Some(admission(format!(
                        "relational index read exceeds row limit {} after live merge",
                        limits.max_rows
                    )));
                    return false;
                }
                rows_visited = next_rows_visited;
                if !visit(primary_key) {
                    stopped_early = true;
                    return false;
                }
                true
            };
            match (&self.backend, &selector) {
                (
                    RelationalIndexReadBackend::Base(reader),
                    RelationalIndexReadSelector::Exact(key),
                ) => RelationalIndexReadViewBackendReport::Base(reader.visit_exact_postings(
                    table,
                    index,
                    key,
                    backend_limits,
                    &mut emit_backend,
                )?),
                (
                    RelationalIndexReadBackend::Base(reader),
                    RelationalIndexReadSelector::Prefix(prefix),
                ) => RelationalIndexReadViewBackendReport::Base(reader.visit_prefix_postings(
                    table,
                    index,
                    prefix,
                    backend_limits,
                    &mut emit_backend,
                )?),
                (
                    RelationalIndexReadBackend::Recovered(reader),
                    RelationalIndexReadSelector::Exact(key),
                ) => RelationalIndexReadViewBackendReport::Recovered(reader.visit_exact_postings(
                    table,
                    index,
                    key,
                    backend_limits,
                    &mut emit_backend,
                )?),
                (
                    RelationalIndexReadBackend::Recovered(reader),
                    RelationalIndexReadSelector::Prefix(prefix),
                ) => {
                    RelationalIndexReadViewBackendReport::Recovered(reader.visit_prefix_postings(
                        table,
                        index,
                        prefix,
                        backend_limits,
                        &mut emit_backend,
                    )?)
                }
                (_, RelationalIndexReadSelector::Range(_)) => {
                    return Err(RelationalIndexShadowError::Admission(
                        "range selectors require ordered entry traversal".to_string(),
                    ));
                }
            }
        };
        if let Some(error) = callback_error {
            return Err(error);
        }
        if !stopped_early {
            for (primary_key, kind) in pending_live {
                if kind == RelationalIndexChangeKind::Delete {
                    continue;
                }
                rows_visited = rows_visited
                    .checked_add(1)
                    .ok_or_else(|| admission("relational index output row counter overflow"))?;
                if rows_visited > limits.max_rows.get() {
                    return Err(admission(format!(
                        "relational index read exceeds row limit {} after live merge",
                        limits.max_rows
                    )));
                }
                if !visit(&primary_key) {
                    stopped_early = true;
                    break;
                }
            }
        }
        let total_bytes = backend
            .bytes_read()
            .ok_or_else(|| admission("relational index backend byte counter overflow"))?
            .checked_add(live_bytes_visited)
            .ok_or_else(|| admission("relational index read byte counter overflow"))?;
        if total_bytes > limits.max_bytes.get() {
            return Err(admission(format!(
                "relational index read needs {total_bytes} bytes including live changes, exceeding byte limit {}",
                limits.max_bytes
            )));
        }
        let mut report = RelationalIndexReadViewReport {
            base_generation: self.identity.base_generation,
            delta_generation: self.identity.delta_generation,
            base_commit_epoch: self.identity.base_commit_epoch,
            visible_commit_epoch: self.identity.visible_commit_epoch,
            root_set_digest: self.identity.root_set_digest.to_string(),
            backend,
            live_batches_visited,
            live_entries_visited,
            live_entries_matched,
            live_bytes_visited,
            rows_visited,
            stopped_early,
        };
        report.stopped_early |= match &report.backend {
            RelationalIndexReadViewBackendReport::Base(backend) => backend.stopped_early,
            RelationalIndexReadViewBackendReport::Recovered(backend) => backend.stopped_early,
        };
        Ok(report)
    }

    fn durable_commit_epoch(&self) -> u64 {
        match &self.backend {
            RelationalIndexReadBackend::Base(reader) => reader.manifest().source_commit_epoch,
            RelationalIndexReadBackend::Recovered(reader) => {
                reader.manifest().recovered_commit_epoch
            }
        }
    }

    pub fn base_manifest(&self) -> &RelationalIndexShadowManifest {
        match &self.backend {
            RelationalIndexReadBackend::Base(reader) => reader.manifest(),
            RelationalIndexReadBackend::Recovered(reader) => reader.base_manifest(),
        }
    }
}

fn admission(message: impl Into<String>) -> RelationalIndexShadowError {
    RelationalIndexShadowError::Admission(message.into())
}

#[cfg(test)]
mod tests;
