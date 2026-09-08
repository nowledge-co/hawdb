use super::{
    codec, durability, relational_row_delta_manifest_generation_file,
    relational_row_delta_run_file, RelationalRowDeltaBaseBinding, RelationalRowDeltaConfig,
    RelationalRowDeltaError, RelationalRowDeltaGeneration, RelationalRowDeltaManifest,
    RelationalRowDeltaPublicationPhase, RelationalRowDeltaReadReport, RelationalRowDeltaReport,
    RelationalRowDeltaTableMetadata, RowDeltaKey, RowDeltaRunContext, RowDeltaRunDescriptor,
    RowDeltaValue, COMPLETE_PUBLICATION_TRACE, RELATIONAL_ROW_DELTA_MANIFEST_FILE,
    RELATIONAL_ROW_DELTA_PUBLICATION_LOCK_FILE,
};
use crate::relational::row_page::{RelationalRowPageRecoveredValue, RelationalRowPageRootReader};
use crate::relational::{
    ordered_key::encode_ordered_relational_key, RelationalKey, RelationalOverflowRootReader,
    RelationalRecoverySourceIdentity, RelationalRowChange, RelationalRowChangeCapture,
    RelationalRowChangeCaptureLimits, RelationalRowPagePublicationConfig, RelationalState,
};
use crate::{durable_replace_file, sync_directory};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ROW_DELTA_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowRootSelection {
    IndependentLatest,
    CanonicalCheckpoint,
}

#[derive(Debug)]
pub struct RelationalRowDeltaBuilder {
    directory: PathBuf,
    base: RelationalRowDeltaBaseBinding,
    row_page_config: RelationalRowPagePublicationConfig,
    delta_generation: u64,
    expected_previous: Option<RelationalRowDeltaGeneration>,
    config: RelationalRowDeltaConfig,
    tables: Vec<RelationalRowDeltaTableMetadata>,
    schema_set_digest: skein_integrity::Sha256Digest,
    visible_commit_epoch: u64,
    replayed_batches: u64,
    dirty: BTreeMap<RowDeltaKey, RowDeltaValue>,
    dirty_bytes: usize,
    runs: Vec<RowDeltaRunDescriptor>,
    run_bytes: u64,
    peak_dirty_entries: usize,
    peak_dirty_bytes: usize,
    row_root_selection: RowRootSelection,
    invalidated: Option<String>,
}

impl RelationalRowDeltaBuilder {
    pub fn validate_base_state(
        base: &RelationalRowPageRootReader,
        state: &RelationalState,
        config: RelationalRowDeltaConfig,
    ) -> Result<(), RelationalRowDeltaError> {
        let tables = table_metadata_for_state(state)?;
        codec::validate_tables_against_base(base, tables, config)?;
        Ok(())
    }

    pub fn new_for_recovery(
        directory: &Path,
        base: &RelationalRowPageRootReader,
        expected_previous: Option<RelationalRowDeltaGeneration>,
        state: &RelationalState,
        config: RelationalRowDeltaConfig,
    ) -> Result<Self, RelationalRowDeltaError> {
        let delta_generation = next_recovery_delta_generation(base, expected_previous)?;
        Self::new_for_state(
            directory,
            base,
            delta_generation,
            expected_previous,
            state,
            config,
        )
    }

    /// Creates a recovery builder whose immutable base is selected by an
    /// enclosing canonical checkpoint manifest rather than the row-page
    /// subsystem's independent latest selector.
    pub fn new_for_checkpoint_recovery(
        directory: &Path,
        base: &RelationalRowPageRootReader,
        expected_previous: Option<RelationalRowDeltaGeneration>,
        state: &RelationalState,
        config: RelationalRowDeltaConfig,
    ) -> Result<Self, RelationalRowDeltaError> {
        let delta_generation = next_recovery_delta_generation(base, expected_previous)?;
        let tables = table_metadata_for_state(state)?;
        Self::new_inner(
            directory,
            base,
            delta_generation,
            expected_previous,
            tables,
            config,
            RowRootSelection::CanonicalCheckpoint,
        )
    }

    pub fn new_for_state(
        directory: &Path,
        base: &RelationalRowPageRootReader,
        delta_generation: u64,
        expected_previous: Option<RelationalRowDeltaGeneration>,
        state: &RelationalState,
        config: RelationalRowDeltaConfig,
    ) -> Result<Self, RelationalRowDeltaError> {
        let tables = table_metadata_for_state(state)?;
        Self::new(
            directory,
            base,
            delta_generation,
            expected_previous,
            tables,
            config,
        )
    }

    pub fn new(
        directory: &Path,
        base: &RelationalRowPageRootReader,
        delta_generation: u64,
        expected_previous: Option<RelationalRowDeltaGeneration>,
        tables: Vec<RelationalRowDeltaTableMetadata>,
        config: RelationalRowDeltaConfig,
    ) -> Result<Self, RelationalRowDeltaError> {
        Self::new_inner(
            directory,
            base,
            delta_generation,
            expected_previous,
            tables,
            config,
            RowRootSelection::IndependentLatest,
        )
    }

    fn new_inner(
        directory: &Path,
        base: &RelationalRowPageRootReader,
        delta_generation: u64,
        expected_previous: Option<RelationalRowDeltaGeneration>,
        tables: Vec<RelationalRowDeltaTableMetadata>,
        config: RelationalRowDeltaConfig,
        row_root_selection: RowRootSelection,
    ) -> Result<Self, RelationalRowDeltaError> {
        config.validate_run_policy()?;
        if delta_generation == 0 {
            return Err(RelationalRowDeltaError::Admission(
                "row delta generation must be non-zero".to_string(),
            ));
        }
        if config.max_manifest_bytes.get() < codec::MANIFEST_HEADER_BYTES {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta manifest requires {} bytes, exceeding limit {}",
                codec::MANIFEST_HEADER_BYTES,
                config.max_manifest_bytes
            )));
        }
        let tables = codec::validate_tables_against_base(base, tables, config)?;
        let schema_set_digest = codec::schema_set_digest(&tables)?;
        let row_page_config = base.publication_config();
        let base = RelationalRowDeltaBaseBinding::from_reader(base);
        if expected_previous.is_some_and(|previous| {
            previous.base_generation == base.generation
                && previous.delta_generation >= delta_generation
        }) {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta generation {delta_generation} must exceed the previous generation"
            )));
        }
        let generation_manifest = directory.join(relational_row_delta_manifest_generation_file(
            base.generation,
            delta_generation,
        ));
        if generation_manifest.exists() {
            return Err(RelationalRowDeltaError::Admission(format!(
                "immutable row delta generation manifest {} already exists",
                generation_manifest.display()
            )));
        }
        fs::create_dir_all(directory).map_err(durability("create row delta directory"))?;
        Ok(Self {
            directory: directory.to_path_buf(),
            base,
            row_page_config,
            delta_generation,
            expected_previous,
            config,
            tables,
            schema_set_digest,
            visible_commit_epoch: base.source_commit_epoch,
            replayed_batches: 0,
            dirty: BTreeMap::new(),
            dirty_bytes: 0,
            runs: Vec::new(),
            run_bytes: 0,
            peak_dirty_entries: 0,
            peak_dirty_bytes: 0,
            row_root_selection,
            invalidated: None,
        })
    }

    pub const fn capture_limits(&self) -> RelationalRowChangeCaptureLimits {
        self.config.capture_limits()
    }

    pub const fn base_commit_epoch(&self) -> u64 {
        self.base.source_commit_epoch
    }

    pub const fn visible_commit_epoch(&self) -> u64 {
        self.visible_commit_epoch
    }

    /// Looks up the newest value already staged by this unpublished builder.
    ///
    /// The bounded dirty map takes precedence over immutable runs. Run reads
    /// use the same checksummed decoder as a published delta reader, allowing
    /// WAL recovery to hydrate one later transaction without retaining all
    /// earlier recovered rows in memory.
    pub fn lookup_staged(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<
        (
            Option<RelationalRowPageRecoveredValue>,
            RelationalRowDeltaReadReport,
        ),
        RelationalRowDeltaError,
    > {
        self.require_available()?;
        let Ok(table_ordinal) = self
            .tables
            .binary_search_by(|candidate| candidate.table.as_str().cmp(table))
        else {
            return Ok((None, RelationalRowDeltaReadReport::default()));
        };
        let table_ordinal = u32::try_from(table_ordinal).map_err(|_| {
            RelationalRowDeltaError::Corrupt("row delta table ordinal does not fit u32".to_string())
        })?;
        let encoded_primary_key = encode_ordered_relational_key(primary_key).map_err(|error| {
            RelationalRowDeltaError::Admission(format!(
                "row delta lookup key cannot be encoded: {error}"
            ))
        })?;
        if encoded_primary_key.len() > self.config.row_limits.max_key_bytes.get() {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta lookup key contains {} bytes, exceeding limit {}",
                encoded_primary_key.len(),
                self.config.row_limits.max_key_bytes
            )));
        }
        let key = RowDeltaKey {
            table_ordinal,
            encoded_primary_key,
        };
        if let Some(value) = self.dirty.get(&key) {
            let value = super::reader::decode_staged_value(
                value,
                &self.tables[table_ordinal as usize],
                self.config,
            )?;
            return Ok((
                Some(value),
                RelationalRowDeltaReadReport {
                    entries_visited: 1,
                    ..RelationalRowDeltaReadReport::default()
                },
            ));
        }
        super::reader::lookup_runs(
            RowDeltaRunContext {
                directory: &self.directory,
                base: self.base,
                delta_generation: self.delta_generation,
                schema_set_digest: self.schema_set_digest,
                tables: &self.tables,
                config: self.config,
            },
            &self.runs,
            table,
            primary_key,
        )
    }

    pub fn record(
        &mut self,
        epoch: u64,
        capture: RelationalRowChangeCapture,
    ) -> Result<(), RelationalRowDeltaError> {
        self.require_available()?;
        let result = self.record_inner(epoch, capture);
        if let Err(error) = &result {
            self.invalidated = Some(error.to_string());
        }
        result
    }

    pub fn advance_empty(&mut self, epoch: u64) -> Result<(), RelationalRowDeltaError> {
        self.record(
            epoch,
            RelationalRowChangeCapture::Captured {
                changes: Vec::new(),
                encoded_bytes: 0,
            },
        )
    }

    #[cfg(test)]
    pub fn finish(
        self,
        expected_visible_commit_epoch: u64,
        overflow_root: Option<&RelationalOverflowRootReader>,
    ) -> Result<RelationalRowDeltaReport, RelationalRowDeltaError> {
        let recovery_source = RelationalRecoverySourceIdentity::for_test(
            self.base.source_commit_epoch,
            expected_visible_commit_epoch,
        );
        self.finish_inner(
            expected_visible_commit_epoch,
            recovery_source,
            overflow_root,
            None,
        )
    }

    /// Publishes a recovery delta with exact final row counts bound to the
    /// same relational state that produced its captured row changes.
    pub fn finish_with_state(
        mut self,
        expected_visible_commit_epoch: u64,
        recovery_source: RelationalRecoverySourceIdentity,
        overflow_root: Option<&RelationalOverflowRootReader>,
        final_state: &RelationalState,
    ) -> Result<RelationalRowDeltaReport, RelationalRowDeltaError> {
        let final_tables = table_metadata_for_state(final_state)?;
        if final_tables.len() != self.tables.len()
            || final_tables
                .iter()
                .zip(&self.tables)
                .any(|(final_table, base_table)| {
                    final_table.table != base_table.table
                        || final_table.schema_digest != base_table.schema_digest
                        || final_table.column_count != base_table.column_count
                })
        {
            return Err(RelationalRowDeltaError::RequiresCheckpoint {
                tables: final_tables
                    .iter()
                    .map(|table| table.table.clone())
                    .collect(),
            });
        }
        self.tables = final_tables;
        self.finish_inner(
            expected_visible_commit_epoch,
            recovery_source,
            overflow_root,
            None,
        )
    }

    pub(super) fn finish_inner(
        mut self,
        expected_visible_commit_epoch: u64,
        recovery_source: RelationalRecoverySourceIdentity,
        overflow_root: Option<&RelationalOverflowRootReader>,
        stop_after: Option<RelationalRowDeltaPublicationPhase>,
    ) -> Result<RelationalRowDeltaReport, RelationalRowDeltaError> {
        self.require_available()?;
        if self.visible_commit_epoch != expected_visible_commit_epoch {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta consumed through epoch {}, expected {expected_visible_commit_epoch}",
                self.visible_commit_epoch
            )));
        }
        self.flush()?;
        maybe_stop(
            stop_after,
            RelationalRowDeltaPublicationPhase::CandidateRunsDurable,
        )?;
        let overflow_binding = overflow_root.map(|reader| reader.manifest().binding());
        if overflow_binding
            .is_some_and(|binding| binding.source_commit_epoch != expected_visible_commit_epoch)
        {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta overflow root epoch does not match visible epoch {expected_visible_commit_epoch}"
            )));
        }
        let total_entries = self.runs.iter().try_fold(0u64, |entries, run| {
            entries.checked_add(run.entry_count as u64).ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta total entry count overflow".to_string(),
                )
            })
        })?;
        let manifest = RelationalRowDeltaManifest {
            base: self.base,
            delta_generation: self.delta_generation,
            visible_commit_epoch: expected_visible_commit_epoch,
            recovery_source,
            schema_set_digest: self.schema_set_digest,
            run_set_digest: codec::run_set_digest(&self.runs)?,
            overflow_root: overflow_binding,
            tables: self.tables,
            runs: self.runs,
            total_entries,
        };
        super::reader::validate_candidate_overflow_closure(
            &self.directory,
            &manifest,
            self.config,
            overflow_root,
        )?;
        let encoded_manifest = codec::encode_manifest(&manifest, self.config)?;
        let generation_manifest =
            self.directory
                .join(relational_row_delta_manifest_generation_file(
                    manifest.base.generation,
                    manifest.delta_generation,
                ));
        let generation_tmp = generation_manifest.with_extension("skein.tmp");
        remove_if_exists(&generation_tmp)?;
        write_synced(&generation_tmp, &encoded_manifest)?;
        durable_publish_immutable(&generation_tmp, &generation_manifest)?;
        maybe_stop(
            stop_after,
            RelationalRowDeltaPublicationPhase::CandidateManifestDurable,
        )?;

        let _row_page_lock = super::super::publication::acquire_publication_lock(&self.directory)?;
        let actual_base = match self.row_root_selection {
            RowRootSelection::IndependentLatest => {
                RelationalRowPageRootReader::open_latest(&self.directory, self.row_page_config)?
                    .as_ref()
                    .map(RelationalRowDeltaBaseBinding::from_reader)
            }
            RowRootSelection::CanonicalCheckpoint => {
                Some(RelationalRowDeltaBaseBinding::from_reader(
                    &RelationalRowPageRootReader::open_generation(
                        &self.directory,
                        self.base.generation,
                        self.row_page_config,
                    )?,
                ))
            }
        };
        if actual_base != Some(self.base) {
            return Err(RelationalRowDeltaError::StaleBase {
                expected: self.base,
                actual: actual_base,
            });
        }
        let _delta_lock = acquire_publication_lock(&self.directory)?;
        let latest = self.directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE);
        let actual_previous = codec::read_manifest_if_exists(&latest, self.config)?
            .map(|manifest| manifest.generation());
        if actual_previous != self.expected_previous {
            return Err(RelationalRowDeltaError::StaleGeneration {
                expected_previous: self.expected_previous,
                actual_previous,
            });
        }
        maybe_stop(
            stop_after,
            RelationalRowDeltaPublicationPhase::BaseRevalidated,
        )?;
        let latest_tmp = latest.with_extension("skein.tmp");
        remove_if_exists(&latest_tmp)?;
        write_synced(&latest_tmp, &encoded_manifest)?;
        durable_replace_file(&latest_tmp, &latest)
            .map_err(durability("publish latest row delta manifest"))?;

        Ok(RelationalRowDeltaReport {
            generation: manifest.generation(),
            base_commit_epoch: manifest.base.source_commit_epoch,
            visible_commit_epoch: manifest.visible_commit_epoch,
            runs: manifest.runs.len(),
            entries: manifest.total_entries,
            run_bytes: self.run_bytes,
            manifest_bytes: encoded_manifest.len() as u64,
            replayed_batches: self.replayed_batches,
            peak_dirty_entries: self.peak_dirty_entries,
            peak_dirty_bytes: self.peak_dirty_bytes,
            events: COMPLETE_PUBLICATION_TRACE,
        })
    }

    fn record_inner(
        &mut self,
        epoch: u64,
        capture: RelationalRowChangeCapture,
    ) -> Result<(), RelationalRowDeltaError> {
        self.require_current_or_next_epoch(epoch)?;
        let changes = match capture {
            RelationalRowChangeCapture::Captured { changes, .. } => changes,
            RelationalRowChangeCapture::RequiresCheckpoint { tables } => {
                return Err(RelationalRowDeltaError::RequiresCheckpoint { tables });
            }
            RelationalRowChangeCapture::Invalidated { reason } => {
                return Err(RelationalRowDeltaError::Invalidated(reason));
            }
        };
        if epoch == self.base.source_commit_epoch && !changes.is_empty() {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta cannot apply changes at the immutable base epoch".to_string(),
            ));
        }
        if changes.len() > self.config.max_dirty_entries.get() {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta capture contains {} changes, exceeding limit {}",
                changes.len(),
                self.config.max_dirty_entries
            )));
        }
        let advances_epoch = epoch != self.visible_commit_epoch;
        for change in changes {
            let (key, value) = self.encode_change(epoch, change)?;
            self.insert_change(key, value)?;
        }
        if advances_epoch {
            self.replayed_batches = self.replayed_batches.checked_add(1).ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta replayed batch count overflow".to_string(),
                )
            })?;
        }
        self.visible_commit_epoch = epoch;
        Ok(())
    }

    fn encode_change(
        &self,
        epoch: u64,
        change: RelationalRowChange,
    ) -> Result<(RowDeltaKey, RowDeltaValue), RelationalRowDeltaError> {
        let table_ordinal = self
            .tables
            .binary_search_by(|table| table.table.cmp(&change.table))
            .map_err(|_| {
                RelationalRowDeltaError::Corrupt(format!(
                    "row delta change references unknown table {}",
                    change.table
                ))
            })?;
        let table = &self.tables[table_ordinal];
        let encoded_primary_key =
            encode_ordered_relational_key(&change.primary_key).map_err(|error| {
                RelationalRowDeltaError::Corrupt(format!(
                    "row delta primary key cannot be encoded: {error}"
                ))
            })?;
        if encoded_primary_key.len() > self.config.row_limits.max_key_bytes.get() {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta key contains {} bytes, exceeding limit {}",
                encoded_primary_key.len(),
                self.config.row_limits.max_key_bytes
            )));
        }
        let (is_present, encoded_row) = match change.row {
            Some(row) => (
                true,
                super::super::value::encode_row(
                    &row,
                    table.column_count.get() as usize,
                    self.config.row_limits,
                )?,
            ),
            None => (false, Vec::new()),
        };
        let charged_bytes =
            codec::charged_entry_bytes(encoded_primary_key.len(), encoded_row.len())?;
        Ok((
            RowDeltaKey {
                table_ordinal: table_ordinal as u32,
                encoded_primary_key,
            },
            RowDeltaValue {
                is_present,
                encoded_row,
                last_modified_epoch: epoch,
                charged_bytes,
            },
        ))
    }

    fn insert_change(
        &mut self,
        key: RowDeltaKey,
        value: RowDeltaValue,
    ) -> Result<(), RelationalRowDeltaError> {
        let inserted_bytes = value.charged_bytes;
        if value.charged_bytes > self.config.max_dirty_bytes.get() {
            return Err(RelationalRowDeltaError::Admission(format!(
                "one row delta entry needs {} bytes, exceeding dirty limit {}",
                value.charged_bytes, self.config.max_dirty_bytes
            )));
        }
        let existing_bytes = self
            .dirty
            .get(&key)
            .map_or(0, |existing| existing.charged_bytes);
        let next_entries = self
            .dirty
            .len()
            .checked_add(usize::from(existing_bytes == 0))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta dirty entry accounting overflow".to_string(),
                )
            })?;
        let next_bytes = self
            .dirty_bytes
            .checked_sub(existing_bytes)
            .and_then(|bytes| bytes.checked_add(value.charged_bytes))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta dirty byte accounting overflow".to_string(),
                )
            })?;
        if !self.dirty.is_empty()
            && (next_entries > self.config.max_dirty_entries.get()
                || next_bytes > self.config.max_dirty_bytes.get())
        {
            self.flush()?;
        }
        let replaced = self.dirty.insert(key, value);
        if let Some(replaced) = replaced {
            self.dirty_bytes = self
                .dirty_bytes
                .checked_sub(replaced.charged_bytes)
                .ok_or_else(|| {
                    RelationalRowDeltaError::Corrupt(
                        "row delta replacement byte accounting underflow".to_string(),
                    )
                })?;
        }
        self.dirty_bytes = self
            .dirty_bytes
            .checked_add(inserted_bytes)
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta dirty byte accounting overflow".to_string(),
                )
            })?;
        self.peak_dirty_entries = self.peak_dirty_entries.max(self.dirty.len());
        self.peak_dirty_bytes = self.peak_dirty_bytes.max(self.dirty_bytes);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), RelationalRowDeltaError> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        if self.runs.len() >= self.config.max_runs.get() {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta needs more than {} immutable runs",
                self.config.max_runs
            )));
        }
        let ordinal = u32::try_from(self.runs.len()).map_err(|_| {
            RelationalRowDeltaError::Admission("row delta run ordinal does not fit u32".to_string())
        })?;
        let final_path = self.directory.join(relational_row_delta_run_file(
            self.base.generation,
            self.delta_generation,
            ordinal,
        ));
        if final_path.exists() {
            return Err(RelationalRowDeltaError::Admission(format!(
                "immutable row delta run {} already exists",
                final_path.display()
            )));
        }
        let tmp_path = final_path.with_extension("skein.tmp");
        remove_if_exists(&tmp_path)?;
        let estimated_run_bytes = codec::estimated_run_encoded_len(&self.dirty)?;
        let next_run_bytes = self
            .run_bytes
            .checked_add(estimated_run_bytes)
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta run byte accounting overflow".to_string(),
                )
            })?;
        if next_run_bytes > self.config.max_run_bytes.get() {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta runs require {next_run_bytes} bytes, exceeding limit {}",
                self.config.max_run_bytes
            )));
        }
        let (lower_key, _) = self
            .dirty
            .first_key_value()
            .expect("non-empty row delta run");
        let (upper_key, _) = self
            .dirty
            .last_key_value()
            .expect("non-empty row delta run");
        codec::ensure_next_run_manifest_capacity(
            &self.tables,
            &self.runs,
            lower_key.encoded_primary_key.len(),
            upper_key.encoded_primary_key.len(),
            self.config,
        )?;
        let descriptor = codec::write_run(
            &tmp_path,
            codec::RunWrite {
                base: self.base,
                delta_generation: self.delta_generation,
                schema_set_digest: self.schema_set_digest,
                ordinal,
                entries: &self.dirty,
            },
            self.config,
        )?;
        debug_assert_eq!(descriptor.encoded_len, estimated_run_bytes);
        durable_publish_immutable(&tmp_path, &final_path)?;
        self.runs.push(descriptor);
        self.run_bytes = next_run_bytes;
        self.dirty.clear();
        self.dirty_bytes = 0;
        Ok(())
    }

    fn require_available(&self) -> Result<(), RelationalRowDeltaError> {
        if let Some(reason) = &self.invalidated {
            return Err(RelationalRowDeltaError::Invalidated(reason.clone()));
        }
        Ok(())
    }

    fn require_current_or_next_epoch(&self, epoch: u64) -> Result<(), RelationalRowDeltaError> {
        if epoch == self.visible_commit_epoch {
            return Ok(());
        }
        let expected = self.visible_commit_epoch.checked_add(1).ok_or_else(|| {
            RelationalRowDeltaError::Corrupt("row delta visible commit epoch overflow".to_string())
        })?;
        if epoch != expected {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta epoch gap: expected {expected}, found {epoch}"
            )));
        }
        Ok(())
    }
}

fn acquire_publication_lock(directory: &Path) -> Result<File, RelationalRowDeltaError> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join(RELATIONAL_ROW_DELTA_PUBLICATION_LOCK_FILE))
        .map_err(durability("open row delta publication lock"))?;
    lock.lock()
        .map_err(durability("lock row delta publication"))?;
    Ok(lock)
}

fn write_synced(path: &Path, encoded: &[u8]) -> Result<(), RelationalRowDeltaError> {
    let mut file = File::create(path).map_err(durability("create row delta manifest candidate"))?;
    file.write_all(encoded)
        .map_err(durability("write row delta manifest candidate"))?;
    file.sync_all()
        .map_err(durability("sync row delta manifest candidate"))
}

fn durable_publish_immutable(
    source: &Path,
    destination: &Path,
) -> Result<(), RelationalRowDeltaError> {
    if destination.exists() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "immutable row delta artifact {} already exists",
            destination.display()
        )));
    }
    durable_replace_file(source, destination)
        .map_err(durability("publish immutable row delta artifact"))
}

fn remove_if_exists(path: &Path) -> Result<(), RelationalRowDeltaError> {
    match fs::remove_file(path) {
        Ok(()) => sync_directory(path.parent().expect("row delta path has a parent")).map_err(
            durability("sync row delta directory after candidate cleanup"),
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(durability("remove stale row delta candidate")(error)),
    }
}

fn maybe_stop(
    stop_after: Option<RelationalRowDeltaPublicationPhase>,
    phase: RelationalRowDeltaPublicationPhase,
) -> Result<(), RelationalRowDeltaError> {
    if stop_after == Some(phase) {
        return Err(RelationalRowDeltaError::Durability(format!(
            "injected stop after {phase:?}"
        )));
    }
    Ok(())
}

fn next_recovery_delta_generation(
    base: &RelationalRowPageRootReader,
    expected_previous: Option<RelationalRowDeltaGeneration>,
) -> Result<u64, RelationalRowDeltaError> {
    let minimum = expected_previous
        .filter(|previous| previous.base_generation == base.manifest().generation)
        .map_or(Ok(1), |previous| {
            previous.delta_generation.checked_add(1).ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta generation space is exhausted".to_string(),
                )
            })
        })?;
    Ok(next_row_delta_generation().max(minimum))
}

fn next_row_delta_generation() -> u64 {
    let clock = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let sequence = NEXT_ROW_DELTA_GENERATION.fetch_add(1, Ordering::Relaxed);
    let generation = clock.rotate_left(17) ^ sequence ^ ((std::process::id() as u64) << 32);
    generation.max(1)
}

fn table_metadata_for_state(
    state: &RelationalState,
) -> Result<Vec<RelationalRowDeltaTableMetadata>, RelationalRowDeltaError> {
    state
        .table_schemas()
        .map(|schema| {
            let column_count = u32::try_from(schema.columns.len()).map_err(|_| {
                RelationalRowDeltaError::Admission(format!(
                    "row delta table {} column count does not fit u32",
                    schema.name
                ))
            })?;
            let column_count = NonZeroU32::new(column_count).ok_or_else(|| {
                RelationalRowDeltaError::Corrupt(format!(
                    "row delta table {} has no columns",
                    schema.name
                ))
            })?;
            let schema_digest = state
                .table_schema_digest(&schema.name)
                .map_err(|error| RelationalRowDeltaError::Corrupt(error.to_string()))?
                .ok_or_else(|| {
                    RelationalRowDeltaError::Corrupt(format!(
                        "row delta table {} disappeared while deriving its schema fence",
                        schema.name
                    ))
                })?;
            Ok(RelationalRowDeltaTableMetadata {
                table: schema.name.clone(),
                schema_digest,
                column_count,
                row_count: u64::try_from(state.row_count(&schema.name)).map_err(|_| {
                    RelationalRowDeltaError::Admission(format!(
                        "row delta table {} row count does not fit u64",
                        schema.name
                    ))
                })?,
            })
        })
        .collect()
}

#[cfg(test)]
mod lock_tests {
    use super::*;

    #[test]
    fn publication_lock_contract() {
        crate::file_lock_tests::assert_contract(
            RELATIONAL_ROW_DELTA_PUBLICATION_LOCK_FILE,
            acquire_publication_lock,
            "open row delta publication lock",
        );
    }

    #[test]
    #[ignore = "deterministic local publication lock campaign"]
    fn publication_lock_state_machine_campaign() {
        crate::file_lock_tests::assert_state_machine(
            RELATIONAL_ROW_DELTA_PUBLICATION_LOCK_FILE,
            acquire_publication_lock,
        );
    }
}
