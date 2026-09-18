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

use super::{
    codec, durability, relational_row_delta_manifest_generation_file,
    relational_row_delta_run_file, RelationalRowDeltaConfig, RelationalRowDeltaError,
    RelationalRowDeltaGeneration, RelationalRowDeltaManifest, RelationalRowDeltaReadReport,
    RelationalRowDeltaTableMetadata, RowDeltaRunContext, RowDeltaRunDescriptor, RowDeltaValue,
    RELATIONAL_ROW_DELTA_MANIFEST_FILE,
};
use crate::io::read_exact_at;
use crate::relational::row_page::demand::RelationalRowPageProjectedOverlayValue;
use crate::relational::row_page::RelationalRowPageRecoveredValue;
#[cfg(test)]
use crate::relational::RelationalRecoverySourceIdentity;
use crate::relational::{
    ordered_key::{decode_ordered_relational_key, encode_ordered_relational_key},
    RelationalKey, RelationalOverflowRootReader, RelationalRecoveryFence, RelationalRow,
    RelationalRowPageRootReader, RelationalValue,
};
use hawdb_integrity::{IntegrityDigest, IntegrityHasher};
use std::collections::VecDeque;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct RelationalRowDeltaReader {
    directory: PathBuf,
    manifest: Arc<RelationalRowDeltaManifest>,
    config: RelationalRowDeltaConfig,
    poisoned: AtomicBool,
}

impl RelationalRowDeltaReader {
    pub fn latest_generation(
        directory: &Path,
        config: RelationalRowDeltaConfig,
    ) -> Result<Option<RelationalRowDeltaGeneration>, RelationalRowDeltaError> {
        let path = directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE);
        Ok(codec::read_manifest_if_exists(&path, config)?.map(|manifest| manifest.generation()))
    }

    #[cfg(test)]
    pub fn open_latest(
        directory: &Path,
        expected_base: &RelationalRowPageRootReader,
        expected_visible_commit_epoch: u64,
        config: RelationalRowDeltaConfig,
    ) -> Result<Option<Self>, RelationalRowDeltaError> {
        Self::open_latest_with_recovery_fence(
            directory,
            expected_base,
            RelationalRecoveryFence::new(
                expected_visible_commit_epoch,
                RelationalRecoverySourceIdentity::for_test(
                    expected_base.manifest().source_commit_epoch,
                    expected_visible_commit_epoch,
                ),
            ),
            config,
        )
    }

    pub fn open_latest_with_recovery_fence(
        directory: &Path,
        expected_base: &RelationalRowPageRootReader,
        expected_recovery: RelationalRecoveryFence,
        config: RelationalRowDeltaConfig,
    ) -> Result<Option<Self>, RelationalRowDeltaError> {
        let path = directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE);
        codec::read_manifest_if_exists(&path, config)?
            .map(|manifest| {
                Self::from_manifest(
                    directory,
                    manifest,
                    expected_base,
                    expected_recovery,
                    config,
                )
            })
            .transpose()
    }

    #[cfg(test)]
    pub fn open_generation(
        directory: &Path,
        generation: RelationalRowDeltaGeneration,
        expected_base: &RelationalRowPageRootReader,
        expected_visible_commit_epoch: u64,
        config: RelationalRowDeltaConfig,
    ) -> Result<Self, RelationalRowDeltaError> {
        Self::open_generation_with_recovery_fence(
            directory,
            generation,
            expected_base,
            RelationalRecoveryFence::new(
                expected_visible_commit_epoch,
                RelationalRecoverySourceIdentity::for_test(
                    expected_base.manifest().source_commit_epoch,
                    expected_visible_commit_epoch,
                ),
            ),
            config,
        )
    }

    pub fn open_generation_with_recovery_fence(
        directory: &Path,
        generation: RelationalRowDeltaGeneration,
        expected_base: &RelationalRowPageRootReader,
        expected_recovery: RelationalRecoveryFence,
        config: RelationalRowDeltaConfig,
    ) -> Result<Self, RelationalRowDeltaError> {
        let path = directory.join(relational_row_delta_manifest_generation_file(
            generation.base_generation,
            generation.delta_generation,
        ));
        let manifest = codec::read_manifest(&path, config)?;
        if manifest.generation() != generation {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta generation manifest {generation:?} identifies {:?}",
                manifest.generation()
            )));
        }
        Self::from_manifest(
            directory,
            manifest,
            expected_base,
            expected_recovery,
            config,
        )
    }

    fn from_manifest(
        directory: &Path,
        manifest: RelationalRowDeltaManifest,
        expected_base: &RelationalRowPageRootReader,
        expected_recovery: RelationalRecoveryFence,
        config: RelationalRowDeltaConfig,
    ) -> Result<Self, RelationalRowDeltaError> {
        config.validate_run_policy()?;
        let base = expected_base.manifest();
        if manifest.base.generation != base.generation
            || manifest.base.source_commit_epoch != base.source_commit_epoch
            || manifest.base.root_set_digest != base.root_set_digest
            || manifest.visible_commit_epoch != expected_recovery.commit_epoch
            || manifest.recovery_source != expected_recovery.source
        {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta fence {}/{}/{}/{:?} does not match base {}/{} and expected recovery {expected_recovery:?}",
                manifest.base.generation,
                manifest.base.source_commit_epoch,
                manifest.visible_commit_epoch,
                manifest.recovery_source,
                base.generation,
                base.source_commit_epoch
            )));
        }
        if manifest.tables.len() != base.tables.len()
            || manifest
                .tables
                .iter()
                .zip(&base.tables)
                .any(|(table, base_table)| {
                    table.table != base_table.table
                        || table.schema_digest != base_table.schema_digest
                })
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta table schemas do not match the selected row root".to_string(),
            ));
        }
        for run in &manifest.runs {
            codec::validate_artifact_length(
                &directory.join(relational_row_delta_run_file(
                    manifest.base.generation,
                    manifest.delta_generation,
                    run.ordinal,
                )),
                run.encoded_len,
            )?;
        }
        Ok(Self {
            directory: directory.to_path_buf(),
            manifest: Arc::new(manifest),
            config,
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn manifest(&self) -> &RelationalRowDeltaManifest {
        &self.manifest
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub fn validate_overflow_root(
        &self,
        overflow_root: Option<&RelationalOverflowRootReader>,
    ) -> Result<(), RelationalRowDeltaError> {
        let actual = overflow_root.map(|reader| reader.manifest().binding());
        if actual != self.manifest.overflow_root {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta overflow binding {:?} differs from selected root {actual:?}",
                self.manifest.overflow_root
            )));
        }
        Ok(())
    }

    /// Visits the immutable runs in publication order using one bounded key
    /// and row buffer at a time.
    ///
    /// Callback effects are provisional until this method returns `Ok`. A
    /// callback that returns `false` stops after the emitted entry's binding
    /// and checksum have been verified, without reading the remainder of that
    /// run or recomputing its complete artifact digest.
    pub fn visit_entries(
        &self,
        visit: impl FnMut(&str, &RelationalKey, &RelationalRowPageRecoveredValue, u64) -> bool,
    ) -> Result<RelationalRowDeltaReadReport, RelationalRowDeltaError> {
        if self.is_poisoned() {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta reader is poisoned".to_string(),
            ));
        }
        let result = visit_manifest_entries(&self.directory, &self.manifest, self.config, visit);
        if result.as_ref().is_err_and(should_poison) {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    /// Visits only entries in one ordered table/key range.
    ///
    /// Runs whose key bounds cannot intersect the requested range remain
    /// unopened. Within an intersecting run, keys before the lower bound are
    /// skipped and traversal stops once the upper bound is crossed. Each
    /// emitted entry is independently binding- and checksum-verified; bytes
    /// outside the requested suffix are deliberately not demand-verified.
    pub fn visit_range_entries(
        &self,
        table: &str,
        lower: Bound<&RelationalKey>,
        upper: Bound<&RelationalKey>,
        mut visit: impl FnMut(&RelationalKey, &RelationalRowPageRecoveredValue, u64) -> bool,
    ) -> Result<RelationalRowDeltaReadReport, RelationalRowDeltaError> {
        if self.is_poisoned() {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta reader is poisoned".to_string(),
            ));
        }
        let result = self.visit_range_entries_inner(table, lower, upper, &mut visit);
        if result.as_ref().is_err_and(should_poison) {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    pub(in crate::relational::row_page) fn range_sources(
        &self,
        table: &str,
        lower: Bound<&RelationalKey>,
        upper: Bound<&RelationalKey>,
        requested_fields: &[usize],
        binds_overlay_overflow: bool,
        max_sources: usize,
    ) -> Result<
        (
            Vec<RelationalRowDeltaRunRangeCursor<'_>>,
            RelationalRowDeltaReadReport,
        ),
        RelationalRowDeltaError,
    > {
        if self.is_poisoned() {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta reader is poisoned".to_string(),
            ));
        }
        let result = self.range_sources_inner(
            table,
            lower,
            upper,
            requested_fields,
            binds_overlay_overflow,
            max_sources,
        );
        if result.as_ref().is_err_and(should_poison) {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    fn range_sources_inner(
        &self,
        table: &str,
        lower: Bound<&RelationalKey>,
        upper: Bound<&RelationalKey>,
        requested_fields: &[usize],
        binds_overlay_overflow: bool,
        max_sources: usize,
    ) -> Result<
        (
            Vec<RelationalRowDeltaRunRangeCursor<'_>>,
            RelationalRowDeltaReadReport,
        ),
        RelationalRowDeltaError,
    > {
        let Ok(table_ordinal) = self
            .manifest
            .tables
            .binary_search_by(|candidate| candidate.table.as_str().cmp(table))
        else {
            return Ok((Vec::new(), RelationalRowDeltaReadReport::default()));
        };
        let table_ordinal = u32::try_from(table_ordinal).map_err(|_| {
            RelationalRowDeltaError::Corrupt("row delta table ordinal does not fit u32".to_string())
        })?;
        let encoded_lower = encode_range_bound(lower, self.config)?;
        let encoded_upper = encode_range_bound(upper, self.config)?;
        if encoded_bounds_are_empty(encoded_lower.as_ref(), encoded_upper.as_ref()) {
            return Ok((Vec::new(), RelationalRowDeltaReadReport::default()));
        }
        let matching = self
            .manifest
            .runs
            .iter()
            .filter(|run| {
                run_intersects_range(
                    run,
                    table_ordinal,
                    encoded_lower.as_ref(),
                    encoded_upper.as_ref(),
                )
            })
            .collect::<Vec<_>>();
        if matching.len() > max_sources {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta range needs {} merge sources, exceeding remaining source limit {max_sources}",
                matching.len()
            )));
        }

        let context = RowDeltaRunContext {
            directory: &self.directory,
            base: self.manifest.base,
            delta_generation: self.manifest.delta_generation,
            schema_set_digest: self.manifest.schema_set_digest,
            tables: &self.manifest.tables,
            config: self.config,
        };
        let encoded_lower =
            encoded_lower.map(|(key, inclusive)| (Arc::<[u8]>::from(key), inclusive));
        let encoded_upper =
            encoded_upper.map(|(key, inclusive)| (Arc::<[u8]>::from(key), inclusive));
        let requested_fields = Arc::<[usize]>::from(requested_fields);
        let mut sources = Vec::with_capacity(matching.len());
        let mut report = RelationalRowDeltaReadReport::default();
        let file_pool = Arc::new(Mutex::new(RowDeltaRunFilePool::new(
            self.config.max_range_open_files.get(),
        )));
        for run in matching {
            report.runs_read = report.runs_read.checked_add(1).ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta range run counter overflow".to_string(),
                )
            })?;
            report.bytes_read =
                report
                    .bytes_read
                    .checked_add(run.encoded_len)
                    .ok_or_else(|| {
                        RelationalRowDeltaError::Admission(
                            "row delta range byte counter overflow".to_string(),
                        )
                    })?;
            sources.push(RelationalRowDeltaRunRangeCursor {
                reader: self,
                run: RowDeltaRunCursor::open_pooled(context, run, Arc::clone(&file_pool))?,
                table_ordinal,
                lower: encoded_lower.clone(),
                upper: encoded_upper.clone(),
                requested_fields: Arc::clone(&requested_fields),
                binds_overlay_overflow,
                exhausted: false,
            });
        }
        file_pool
            .lock()
            .map_err(|_| {
                RelationalRowDeltaError::Durability(
                    "row delta range file pool is poisoned".to_string(),
                )
            })?
            .snapshot()
            .apply_to(&mut report);
        Ok((sources, report))
    }

    fn visit_range_entries_inner(
        &self,
        table: &str,
        lower: Bound<&RelationalKey>,
        upper: Bound<&RelationalKey>,
        visit: &mut impl FnMut(&RelationalKey, &RelationalRowPageRecoveredValue, u64) -> bool,
    ) -> Result<RelationalRowDeltaReadReport, RelationalRowDeltaError> {
        let Ok(table_ordinal) = self
            .manifest
            .tables
            .binary_search_by(|candidate| candidate.table.as_str().cmp(table))
        else {
            return Ok(RelationalRowDeltaReadReport::default());
        };
        let table_ordinal = u32::try_from(table_ordinal).map_err(|_| {
            RelationalRowDeltaError::Corrupt("row delta table ordinal does not fit u32".to_string())
        })?;
        let encoded_lower = encode_range_bound(lower, self.config)?;
        let encoded_upper = encode_range_bound(upper, self.config)?;
        if encoded_bounds_are_empty(encoded_lower.as_ref(), encoded_upper.as_ref()) {
            return Ok(RelationalRowDeltaReadReport::default());
        }

        let mut report = RelationalRowDeltaReadReport::default();
        for run in &self.manifest.runs {
            if !run_intersects_range(
                run,
                table_ordinal,
                encoded_lower.as_ref(),
                encoded_upper.as_ref(),
            ) {
                continue;
            }
            let mut callback_stopped = false;
            let _completed = visit_run(
                RowDeltaRunContext {
                    directory: &self.directory,
                    base: self.manifest.base,
                    delta_generation: self.manifest.delta_generation,
                    schema_set_digest: self.manifest.schema_set_digest,
                    tables: &self.manifest.tables,
                    config: self.config,
                },
                run,
                |candidate_table, candidate_key, value, epoch| {
                    match candidate_table.cmp(table) {
                        std::cmp::Ordering::Less => return Ok(true),
                        std::cmp::Ordering::Greater => return Ok(false),
                        std::cmp::Ordering::Equal => {}
                    }
                    if key_precedes_lower(candidate_key, lower) {
                        return Ok(true);
                    }
                    if key_exceeds_upper(candidate_key, upper) {
                        return Ok(false);
                    }
                    report.entries_visited =
                        report.entries_visited.checked_add(1).ok_or_else(|| {
                            RelationalRowDeltaError::Admission(
                                "row delta range entry counter overflow".to_string(),
                            )
                        })?;
                    if !visit(candidate_key, value, epoch) {
                        callback_stopped = true;
                        return Ok(false);
                    }
                    Ok(true)
                },
            )?;
            report.runs_read = report.runs_read.checked_add(1).ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta range run counter overflow".to_string(),
                )
            })?;
            report.peak_open_files = 1;
            report.bytes_read =
                report
                    .bytes_read
                    .checked_add(run.encoded_len)
                    .ok_or_else(|| {
                        RelationalRowDeltaError::Admission(
                            "row delta range byte counter overflow".to_string(),
                        )
                    })?;
            if callback_stopped {
                report.stopped_early = true;
                break;
            }
        }
        Ok(report)
    }

    /// Looks up the newest immutable recovery-delta value without materializing
    /// the complete delta generation. Runs are inspected newest first because
    /// a later run supersedes the same key in an earlier run.
    pub fn lookup(
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
        if self.is_poisoned() {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta reader is poisoned".to_string(),
            ));
        }
        let result = self.lookup_inner(table, primary_key);
        if result.as_ref().is_err_and(should_poison) {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    fn lookup_inner(
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
        lookup_runs(
            RowDeltaRunContext {
                directory: &self.directory,
                base: self.manifest.base,
                delta_generation: self.manifest.delta_generation,
                schema_set_digest: self.manifest.schema_set_digest,
                tables: &self.manifest.tables,
                config: self.config,
            },
            &self.manifest.runs,
            table,
            primary_key,
        )
    }
}

pub(super) fn lookup_runs(
    context: RowDeltaRunContext<'_>,
    runs: &[RowDeltaRunDescriptor],
    table: &str,
    primary_key: &RelationalKey,
) -> Result<
    (
        Option<RelationalRowPageRecoveredValue>,
        RelationalRowDeltaReadReport,
    ),
    RelationalRowDeltaError,
> {
    let Ok(table_ordinal) = context
        .tables
        .binary_search_by(|candidate| candidate.table.as_str().cmp(table))
    else {
        return Ok((None, RelationalRowDeltaReadReport::default()));
    };
    let table_ordinal = u32::try_from(table_ordinal).map_err(|_| {
        RelationalRowDeltaError::Corrupt("row delta table ordinal does not fit u32".to_string())
    })?;
    let encoded_key = encode_ordered_relational_key(primary_key).map_err(|error| {
        RelationalRowDeltaError::Admission(format!(
            "row delta lookup key cannot be encoded: {error}"
        ))
    })?;
    if encoded_key.len() > context.config.row_limits.max_key_bytes.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta lookup key contains {} bytes, exceeding limit {}",
            encoded_key.len(),
            context.config.row_limits.max_key_bytes
        )));
    }

    let target = (table_ordinal, encoded_key.as_slice());
    let mut report = RelationalRowDeltaReadReport::default();
    for run in runs.iter().rev() {
        let lower = (
            run.lower_bound.table_ordinal,
            run.lower_bound.encoded_primary_key.as_slice(),
        );
        let upper = (
            run.upper_bound.table_ordinal,
            run.upper_bound.encoded_primary_key.as_slice(),
        );
        if target < lower || target > upper {
            continue;
        }

        let mut found = None;
        let completed = visit_run(context, run, |candidate_table, candidate_key, value, _| {
            report.entries_visited = report.entries_visited.checked_add(1).ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta lookup entry counter overflow".to_string(),
                )
            })?;
            match candidate_table
                .cmp(table)
                .then_with(|| candidate_key.cmp(primary_key))
            {
                std::cmp::Ordering::Less => Ok(true),
                std::cmp::Ordering::Equal => {
                    found = Some(value.clone());
                    Ok(false)
                }
                std::cmp::Ordering::Greater => Ok(false),
            }
        })?;
        report.runs_read = report.runs_read.checked_add(1).ok_or_else(|| {
            RelationalRowDeltaError::Admission("row delta lookup run counter overflow".to_string())
        })?;
        report.peak_open_files = 1;
        report.bytes_read = report
            .bytes_read
            .checked_add(run.encoded_len)
            .and_then(|bytes| bytes.checked_add(run.descriptor_bytes))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta lookup byte counter overflow".to_string(),
                )
            })?;
        report.stopped_early |= !completed;
        if found.is_some() {
            return Ok((found, report));
        }
    }
    Ok((None, report))
}

fn encode_range_bound(
    bound: Bound<&RelationalKey>,
    config: RelationalRowDeltaConfig,
) -> Result<Option<(Vec<u8>, bool)>, RelationalRowDeltaError> {
    let (key, inclusive) = match bound {
        Bound::Unbounded => return Ok(None),
        Bound::Included(key) => (key, true),
        Bound::Excluded(key) => (key, false),
    };
    let encoded = encode_ordered_relational_key(key).map_err(|error| {
        RelationalRowDeltaError::Admission(format!(
            "row delta range key cannot be encoded: {error}"
        ))
    })?;
    if encoded.len() > config.row_limits.max_key_bytes.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta range key contains {} bytes, exceeding limit {}",
            encoded.len(),
            config.row_limits.max_key_bytes
        )));
    }
    Ok(Some((encoded, inclusive)))
}

fn encoded_bounds_are_empty(
    lower: Option<&(Vec<u8>, bool)>,
    upper: Option<&(Vec<u8>, bool)>,
) -> bool {
    match (lower, upper) {
        (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) => {
            lower > upper || (lower == upper && !(*lower_inclusive && *upper_inclusive))
        }
        _ => false,
    }
}

fn run_intersects_range(
    run: &RowDeltaRunDescriptor,
    table_ordinal: u32,
    lower: Option<&(Vec<u8>, bool)>,
    upper: Option<&(Vec<u8>, bool)>,
) -> bool {
    if run.upper_bound.table_ordinal < table_ordinal
        || run.lower_bound.table_ordinal > table_ordinal
    {
        return false;
    }
    if run.upper_bound.table_ordinal == table_ordinal
        && lower.is_some_and(|(key, inclusive)| {
            run.upper_bound.encoded_primary_key < *key
                || (run.upper_bound.encoded_primary_key == *key && !inclusive)
        })
    {
        return false;
    }
    if run.lower_bound.table_ordinal == table_ordinal
        && upper.is_some_and(|(key, inclusive)| {
            run.lower_bound.encoded_primary_key > *key
                || (run.lower_bound.encoded_primary_key == *key && !inclusive)
        })
    {
        return false;
    }
    true
}

fn key_precedes_lower(key: &RelationalKey, lower: Bound<&RelationalKey>) -> bool {
    match lower {
        Bound::Unbounded => false,
        Bound::Included(lower) => key < lower,
        Bound::Excluded(lower) => key <= lower,
    }
}

fn key_exceeds_upper(key: &RelationalKey, upper: Bound<&RelationalKey>) -> bool {
    match upper {
        Bound::Unbounded => false,
        Bound::Included(upper) => key > upper,
        Bound::Excluded(upper) => key >= upper,
    }
}

pub(super) fn validate_candidate_overflow_closure(
    directory: &Path,
    manifest: &RelationalRowDeltaManifest,
    config: RelationalRowDeltaConfig,
    overflow_root: Option<&RelationalOverflowRootReader>,
) -> Result<(), RelationalRowDeltaError> {
    let expected_binding = overflow_root.map(|reader| reader.manifest().binding());
    if manifest.overflow_root != expected_binding {
        return Err(RelationalRowDeltaError::Admission(
            "row delta candidate has a mismatched overflow binding".to_string(),
        ));
    }
    let mut closure_error = None;
    let report = visit_manifest_entries(directory, manifest, config, |_, _, value, _| {
        let RelationalRowPageRecoveredValue::Present(row) = value else {
            return true;
        };
        for reference in row.values().iter().filter_map(|value| match value {
            RelationalValue::Overflow(reference) => Some(reference),
            _ => None,
        }) {
            let Some(root) = overflow_root else {
                continue;
            };
            match root.contains(reference) {
                Ok(true) => {}
                Ok(false) => {
                    closure_error = Some(RelationalRowDeltaError::Admission(format!(
                        "row delta references missing overflow extent {}",
                        reference.digest
                    )));
                    return false;
                }
                Err(error) => {
                    closure_error = Some(match error {
                        crate::relational::RelationalOverflowPublicationError::Admission(
                            message,
                        ) => RelationalRowDeltaError::Admission(message),
                        crate::relational::RelationalOverflowPublicationError::Corrupt(message) => {
                            RelationalRowDeltaError::Corrupt(message)
                        }
                        crate::relational::RelationalOverflowPublicationError::Durability(
                            message,
                        ) => RelationalRowDeltaError::Durability(message),
                        crate::relational::RelationalOverflowPublicationError::MissingExtent(
                            digest,
                        ) => RelationalRowDeltaError::Admission(format!(
                            "row delta references missing overflow extent {digest}"
                        )),
                        crate::relational::RelationalOverflowPublicationError::Stopped(reason) => {
                            RelationalRowDeltaError::Invalidated(format!(
                                "overflow validation stopped: {reason}"
                            ))
                        }
                        crate::relational::RelationalOverflowPublicationError::StaleGeneration {
                            expected_previous,
                            actual_previous,
                        } => RelationalRowDeltaError::Admission(format!(
                            "overflow root changed: expected {expected_previous:?}, found {actual_previous:?}"
                        )),
                    });
                    return false;
                }
            }
        }
        true
    })?;
    if let Some(error) = closure_error {
        return Err(error);
    }
    if report.stopped_early {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta overflow validation stopped without an error".to_string(),
        ));
    }
    Ok(())
}

fn visit_manifest_entries(
    directory: &Path,
    manifest: &RelationalRowDeltaManifest,
    config: RelationalRowDeltaConfig,
    mut visit: impl FnMut(&str, &RelationalKey, &RelationalRowPageRecoveredValue, u64) -> bool,
) -> Result<RelationalRowDeltaReadReport, RelationalRowDeltaError> {
    let mut report = RelationalRowDeltaReadReport::default();
    for run in &manifest.runs {
        let completed = visit_run(
            RowDeltaRunContext {
                directory,
                base: manifest.base,
                delta_generation: manifest.delta_generation,
                schema_set_digest: manifest.schema_set_digest,
                tables: &manifest.tables,
                config,
            },
            run,
            |table, key, value, epoch| {
                report.entries_visited =
                    report.entries_visited.checked_add(1).ok_or_else(|| {
                        RelationalRowDeltaError::Admission(
                            "row delta read entry counter overflow".to_string(),
                        )
                    })?;
                Ok(visit(table, key, value, epoch))
            },
        )?;
        report.runs_read += 1;
        report.peak_open_files = 1;
        report.bytes_read = report
            .bytes_read
            .checked_add(run.encoded_len)
            .and_then(|bytes| bytes.checked_add(run.descriptor_bytes))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta read byte counter overflow".to_string(),
                )
            })?;
        if !completed {
            report.stopped_early = true;
            break;
        }
    }
    Ok(report)
}

fn visit_run(
    context: RowDeltaRunContext<'_>,
    run: &RowDeltaRunDescriptor,
    mut visit: impl FnMut(
        &str,
        &RelationalKey,
        &RelationalRowPageRecoveredValue,
        u64,
    ) -> Result<bool, RelationalRowDeltaError>,
) -> Result<bool, RelationalRowDeltaError> {
    let mut cursor = RowDeltaRunCursor::open(context, run)?;
    while let Some(entry) = cursor.next_entry()? {
        if !visit(
            &context.tables[entry.table_ordinal as usize].table,
            &entry.primary_key,
            &entry.value,
            entry.last_modified_epoch,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

struct RowDeltaRunEntry {
    table_ordinal: u32,
    primary_key: RelationalKey,
    value: RelationalRowPageRecoveredValue,
    last_modified_epoch: u64,
}

struct EncodedRowDeltaRunEntry {
    table_ordinal: u32,
    encoded_primary_key: Vec<u8>,
    encoded_row: Vec<u8>,
    kind: u8,
    last_modified_epoch: u64,
}

struct RowDeltaRunCursor<'a> {
    context: RowDeltaRunContext<'a>,
    run: &'a RowDeltaRunDescriptor,
    file: RowDeltaRunFileSource,
    payload_start: u64,
    entry_ordinal: u32,
    expected_payload_offset: u64,
    previous_key: Option<(u32, Vec<u8>)>,
    expected_content_digest: Option<IntegrityDigest>,
    content_hasher: Option<IntegrityHasher>,
    artifact_hasher: Option<IntegrityHasher>,
    initialized: bool,
    completed: bool,
}

impl<'a> RowDeltaRunCursor<'a> {
    fn open(
        context: RowDeltaRunContext<'a>,
        run: &'a RowDeltaRunDescriptor,
    ) -> Result<Self, RelationalRowDeltaError> {
        let path = context.directory.join(relational_row_delta_run_file(
            context.base.generation,
            context.delta_generation,
            run.ordinal,
        ));
        codec::validate_artifact_length(&path, run.encoded_len)?;
        let file = RowDeltaRunFileSource::Dedicated(Arc::new(
            File::open(&path).map_err(durability("open row delta run"))?,
        ));
        Self::open_with_source(context, run, file)
    }

    fn open_pooled(
        context: RowDeltaRunContext<'a>,
        run: &'a RowDeltaRunDescriptor,
        pool: Arc<Mutex<RowDeltaRunFilePool>>,
    ) -> Result<Self, RelationalRowDeltaError> {
        let path = context.directory.join(relational_row_delta_run_file(
            context.base.generation,
            context.delta_generation,
            run.ordinal,
        ));
        codec::validate_artifact_length(&path, run.encoded_len)?;
        Ok(Self::uninitialized(
            context,
            run,
            RowDeltaRunFileSource::Pooled { path, pool },
        ))
    }

    fn open_with_source(
        context: RowDeltaRunContext<'a>,
        run: &'a RowDeltaRunDescriptor,
        file: RowDeltaRunFileSource,
    ) -> Result<Self, RelationalRowDeltaError> {
        let mut cursor = Self::uninitialized(context, run, file);
        let opened = cursor.file.open()?;
        cursor.initialize(&opened)?;
        Ok(cursor)
    }

    fn uninitialized(
        context: RowDeltaRunContext<'a>,
        run: &'a RowDeltaRunDescriptor,
        file: RowDeltaRunFileSource,
    ) -> Self {
        Self {
            context,
            run,
            file,
            payload_start: 0,
            entry_ordinal: 0,
            expected_payload_offset: 0,
            previous_key: None,
            expected_content_digest: None,
            content_hasher: None,
            artifact_hasher: None,
            initialized: false,
            completed: false,
        }
    }

    fn initialize(&mut self, opened: &File) -> Result<(), RelationalRowDeltaError> {
        if self.initialized {
            return Ok(());
        }
        let mut header = [0u8; codec::RUN_HEADER_BYTES];
        read_exact_at(opened, &mut header, 0).map_err(durability("read row delta run header"))?;
        let decoded_header = codec::decode_run_header(
            &header,
            self.context.base,
            self.context.delta_generation,
            self.context.schema_set_digest,
            self.run,
        )?;
        let mut content_hasher = IntegrityHasher::new();
        content_hasher.update(codec::run_integrity_prefix(&header));
        let mut artifact_hasher = IntegrityHasher::new();
        artifact_hasher.update(&header);
        let mut descriptor = [0u8; codec::ENTRY_DESCRIPTOR_BYTES];
        for entry_ordinal in 0..self.run.entry_count {
            let offset = descriptor_offset(entry_ordinal)?;
            read_exact_at(opened, &mut descriptor, offset)
                .map_err(durability("read row delta entry descriptor"))?;
            codec::decode_entry_descriptor(&descriptor)?;
            content_hasher.update(&descriptor);
            artifact_hasher.update(&descriptor);
        }
        let payload_start = (codec::RUN_HEADER_BYTES as u64)
            .checked_add(self.run.descriptor_bytes)
            .ok_or_else(|| {
                RelationalRowDeltaError::Corrupt("row delta payload offset overflow".to_string())
            })?;
        self.payload_start = payload_start;
        self.expected_content_digest = Some(decoded_header.content_digest);
        self.content_hasher = Some(content_hasher);
        self.artifact_hasher = Some(artifact_hasher);
        self.initialized = true;
        Ok(())
    }

    fn next_entry(&mut self) -> Result<Option<RowDeltaRunEntry>, RelationalRowDeltaError> {
        let Some(entry) = self.next_encoded_entry()? else {
            return Ok(None);
        };
        let primary_key = decode_delta_primary_key(&entry.encoded_primary_key)?;
        let value = decode_value(
            entry.kind,
            &entry.encoded_row,
            &self.context.tables[entry.table_ordinal as usize],
            self.context.config,
        )?;
        Ok(Some(RowDeltaRunEntry {
            table_ordinal: entry.table_ordinal,
            primary_key,
            value,
            last_modified_epoch: entry.last_modified_epoch,
        }))
    }

    fn next_encoded_entry(
        &mut self,
    ) -> Result<Option<EncodedRowDeltaRunEntry>, RelationalRowDeltaError> {
        if self.completed {
            return Ok(None);
        }
        let initialized_file = if self.initialized {
            None
        } else {
            let file = self.file.open()?;
            self.initialize(&file)?;
            Some(file)
        };
        if self.entry_ordinal == self.run.entry_count {
            self.finish()?;
            return Ok(None);
        }
        let entry_ordinal = self.entry_ordinal;
        let file = match initialized_file {
            Some(file) => file,
            None => self.file.open()?,
        };
        let mut descriptor = [0u8; codec::ENTRY_DESCRIPTOR_BYTES];
        read_exact_at(&file, &mut descriptor, descriptor_offset(entry_ordinal)?)
            .map_err(durability("read row delta entry descriptor"))?;
        let decoded = codec::decode_entry_descriptor(&descriptor)?;
        if decoded.table_ordinal as usize >= self.context.tables.len()
            || decoded.last_modified_epoch <= self.context.base.source_commit_epoch
            || decoded.last_modified_epoch < self.run.start_epoch
            || decoded.last_modified_epoch > self.run.end_epoch
            || decoded.key_offset != self.expected_payload_offset
            || decoded.row_offset
                != decoded
                    .key_offset
                    .checked_add(decoded.key_len as u64)
                    .ok_or_else(|| {
                        RelationalRowDeltaError::Corrupt(
                            "row delta row offset overflow".to_string(),
                        )
                    })?
            || decoded.key_len == 0
            || decoded.key_len as usize > self.context.config.row_limits.max_key_bytes.get()
            || decoded.row_len as usize > self.context.config.row_limits.max_row_bytes.get()
            || (decoded.kind == 0) != (decoded.row_len == 0)
        {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "invalid row delta entry descriptor at ordinal {entry_ordinal}"
            )));
        }
        let mut encoded_key = vec![0u8; decoded.key_len as usize];
        let mut encoded_row = vec![0u8; decoded.row_len as usize];
        let key_offset = self
            .payload_start
            .checked_add(decoded.key_offset)
            .ok_or_else(|| {
                RelationalRowDeltaError::Corrupt("row delta key offset overflow".to_string())
            })?;
        let row_offset = self
            .payload_start
            .checked_add(decoded.row_offset)
            .ok_or_else(|| {
                RelationalRowDeltaError::Corrupt("row delta row offset overflow".to_string())
            })?;
        read_exact_at(&file, &mut encoded_key, key_offset)
            .map_err(durability("read row delta primary key"))?;
        read_exact_at(&file, &mut encoded_row, row_offset)
            .map_err(durability("read row delta row"))?;
        self.expected_payload_offset = decoded
            .row_offset
            .checked_add(decoded.row_len as u64)
            .ok_or_else(|| {
                RelationalRowDeltaError::Corrupt("row delta payload range overflow".to_string())
            })?;
        self.content_hasher
            .as_mut()
            .expect("active run has a content hasher")
            .update(&encoded_key);
        self.content_hasher
            .as_mut()
            .expect("active run has a content hasher")
            .update(&encoded_row);
        self.artifact_hasher
            .as_mut()
            .expect("active run has an artifact hasher")
            .update(&encoded_key);
        self.artifact_hasher
            .as_mut()
            .expect("active run has an artifact hasher")
            .update(&encoded_row);
        let mut entry_hasher = IntegrityHasher::new();
        entry_hasher.update(&encoded_key);
        entry_hasher.update(&encoded_row);
        let entry_crc32c = entry_hasher.finish().crc32c.get();
        let expected_binding = codec::entry_binding(
            self.context.base,
            self.context.delta_generation,
            self.run.ordinal,
            entry_ordinal,
            &descriptor[..48],
            &encoded_key,
            &encoded_row,
        );
        if entry_crc32c != decoded.entry_crc32c || expected_binding != decoded.binding {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta entry {entry_ordinal} checksum or binding mismatch"
            )));
        }
        if self.previous_key.as_ref().is_some_and(|(table, key)| {
            *table > decoded.table_ordinal
                || (*table == decoded.table_ordinal && key >= &encoded_key)
        }) {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta entries are not strictly ordered".to_string(),
            ));
        }
        if entry_ordinal == 0
            && (decoded.table_ordinal != self.run.lower_bound.table_ordinal
                || encoded_key != self.run.lower_bound.encoded_primary_key)
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta lower key bound mismatch".to_string(),
            ));
        }
        if entry_ordinal + 1 == self.run.entry_count
            && (decoded.table_ordinal != self.run.upper_bound.table_ordinal
                || encoded_key != self.run.upper_bound.encoded_primary_key)
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta upper key bound mismatch".to_string(),
            ));
        }
        self.previous_key = Some((decoded.table_ordinal, encoded_key.clone()));
        self.entry_ordinal += 1;
        Ok(Some(EncodedRowDeltaRunEntry {
            table_ordinal: decoded.table_ordinal,
            encoded_primary_key: encoded_key,
            encoded_row,
            kind: decoded.kind,
            last_modified_epoch: decoded.last_modified_epoch,
        }))
    }

    fn finish(&mut self) -> Result<(), RelationalRowDeltaError> {
        if self.expected_payload_offset != self.run.payload_bytes {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta payload coverage mismatch".to_string(),
            ));
        }
        if self
            .content_hasher
            .take()
            .expect("active run has a content hasher")
            .finish()
            != self
                .expected_content_digest
                .take()
                .expect("initialized run has an expected content digest")
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta run content checksum mismatch".to_string(),
            ));
        }
        if self
            .artifact_hasher
            .take()
            .expect("active run has an artifact hasher")
            .finish()
            != self.run.digest
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta run artifact checksum mismatch".to_string(),
            ));
        }
        self.completed = true;
        Ok(())
    }
}

enum RowDeltaRunFileSource {
    Dedicated(Arc<File>),
    Pooled {
        path: PathBuf,
        pool: Arc<Mutex<RowDeltaRunFilePool>>,
    },
}

impl RowDeltaRunFileSource {
    fn open(&self) -> Result<Arc<File>, RelationalRowDeltaError> {
        match self {
            Self::Dedicated(file) => Ok(Arc::clone(file)),
            Self::Pooled { path, pool } => pool
                .lock()
                .map_err(|_| {
                    RelationalRowDeltaError::Durability(
                        "row delta range file pool is poisoned".to_string(),
                    )
                })?
                .open(path),
        }
    }

    fn pool_snapshot(&self) -> Result<RowDeltaRunFilePoolSnapshot, RelationalRowDeltaError> {
        match self {
            Self::Dedicated(_) => Ok(RowDeltaRunFilePoolSnapshot::default()),
            Self::Pooled { pool, .. } => Ok(pool
                .lock()
                .map_err(|_| {
                    RelationalRowDeltaError::Durability(
                        "row delta range file pool is poisoned".to_string(),
                    )
                })?
                .snapshot()),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::relational::row_page) struct RowDeltaRunFilePoolSnapshot {
    peak_open_files: usize,
    file_opens: usize,
    hits: usize,
    misses: usize,
}

impl RowDeltaRunFilePoolSnapshot {
    pub(in crate::relational::row_page) fn apply_to(
        self,
        report: &mut RelationalRowDeltaReadReport,
    ) {
        report.peak_open_files = self.peak_open_files;
        report.range_file_opens = self.file_opens;
        report.range_file_pool_hits = self.hits;
        report.range_file_pool_misses = self.misses;
    }
}

struct RowDeltaRunFilePool {
    capacity: usize,
    files: VecDeque<(PathBuf, Arc<File>)>,
    peak_open_files: usize,
    file_opens: usize,
    hits: usize,
    misses: usize,
}

impl RowDeltaRunFilePool {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            files: VecDeque::new(),
            peak_open_files: 0,
            file_opens: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn open(&mut self, path: &Path) -> Result<Arc<File>, RelationalRowDeltaError> {
        if let Some(position) = self
            .files
            .iter()
            .position(|(candidate, _)| candidate == path)
        {
            self.hits = self.hits.saturating_add(1);
            let entry = self
                .files
                .remove(position)
                .expect("row delta file pool position came from the same deque");
            let file = Arc::clone(&entry.1);
            self.files.push_back(entry);
            return Ok(file);
        }
        let file = Arc::new(File::open(path).map_err(durability("open row delta run"))?);
        self.file_opens = self.file_opens.saturating_add(1);
        self.misses = self.misses.saturating_add(1);
        if self.files.len() == self.capacity {
            self.files.pop_front();
        }
        self.files
            .push_back((path.to_path_buf(), Arc::clone(&file)));
        self.peak_open_files = self.peak_open_files.max(self.files.len());
        Ok(file)
    }

    fn snapshot(&self) -> RowDeltaRunFilePoolSnapshot {
        RowDeltaRunFilePoolSnapshot {
            peak_open_files: self.peak_open_files,
            file_opens: self.file_opens,
            hits: self.hits,
            misses: self.misses,
        }
    }
}

pub(in crate::relational::row_page) struct RelationalRowDeltaRunRangeCursor<'a> {
    reader: &'a RelationalRowDeltaReader,
    run: RowDeltaRunCursor<'a>,
    table_ordinal: u32,
    lower: Option<(Arc<[u8]>, bool)>,
    upper: Option<(Arc<[u8]>, bool)>,
    requested_fields: Arc<[usize]>,
    binds_overlay_overflow: bool,
    exhausted: bool,
}

impl RelationalRowDeltaRunRangeCursor<'_> {
    pub(in crate::relational::row_page) fn file_pool_snapshot(
        &self,
    ) -> Result<RowDeltaRunFilePoolSnapshot, RelationalRowDeltaError> {
        self.run.file.pool_snapshot()
    }

    pub(in crate::relational::row_page) fn next(
        &mut self,
    ) -> Result<
        Option<(RelationalKey, RelationalRowPageProjectedOverlayValue, u64)>,
        RelationalRowDeltaError,
    > {
        let result = self.next_inner();
        if result.as_ref().is_err_and(should_poison) {
            self.reader.poisoned.store(true, Ordering::Release);
        }
        result
    }

    fn next_inner(
        &mut self,
    ) -> Result<
        Option<(RelationalKey, RelationalRowPageProjectedOverlayValue, u64)>,
        RelationalRowDeltaError,
    > {
        if self.exhausted {
            return Ok(None);
        }
        while let Some(entry) = self.run.next_encoded_entry()? {
            match entry.table_ordinal.cmp(&self.table_ordinal) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => {
                    self.exhausted = true;
                    return Ok(None);
                }
                std::cmp::Ordering::Equal => {}
            }
            if self.lower.as_ref().is_some_and(|(lower, inclusive)| {
                entry.encoded_primary_key.as_slice() < lower.as_ref()
                    || (!inclusive && entry.encoded_primary_key.as_slice() == lower.as_ref())
            }) {
                continue;
            }
            if self.upper.as_ref().is_some_and(|(upper, inclusive)| {
                entry.encoded_primary_key.as_slice() > upper.as_ref()
                    || (!inclusive && entry.encoded_primary_key.as_slice() == upper.as_ref())
            }) {
                self.exhausted = true;
                return Ok(None);
            }
            let primary_key = decode_delta_primary_key(&entry.encoded_primary_key)?;
            let value = decode_projected_value(
                entry.kind,
                &entry.encoded_row,
                &self.run.context.tables[entry.table_ordinal as usize],
                &self.requested_fields,
                self.binds_overlay_overflow,
                self.run.context.config,
            )?;
            return Ok(Some((primary_key, value, entry.last_modified_epoch)));
        }
        self.exhausted = true;
        Ok(None)
    }

    pub(in crate::relational::row_page) const fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

fn descriptor_offset(entry_ordinal: u32) -> Result<u64, RelationalRowDeltaError> {
    (codec::RUN_HEADER_BYTES as u64)
        .checked_add(
            u64::from(entry_ordinal)
                .checked_mul(codec::ENTRY_DESCRIPTOR_BYTES as u64)
                .ok_or_else(|| {
                    RelationalRowDeltaError::Corrupt(
                        "row delta descriptor offset overflow".to_string(),
                    )
                })?,
        )
        .ok_or_else(|| {
            RelationalRowDeltaError::Corrupt("row delta descriptor offset overflow".to_string())
        })
}

pub(super) fn decode_staged_value(
    value: &RowDeltaValue,
    table: &RelationalRowDeltaTableMetadata,
    config: RelationalRowDeltaConfig,
) -> Result<RelationalRowPageRecoveredValue, RelationalRowDeltaError> {
    decode_value(
        u8::from(value.is_present),
        &value.encoded_row,
        table,
        config,
    )
}

fn decode_delta_primary_key(encoded: &[u8]) -> Result<RelationalKey, RelationalRowDeltaError> {
    decode_ordered_relational_key(encoded).map_err(|error| {
        RelationalRowDeltaError::Corrupt(format!(
            "row delta primary key cannot be decoded: {error}"
        ))
    })
}

fn decode_projected_value(
    kind: u8,
    encoded_row: &[u8],
    table: &super::RelationalRowDeltaTableMetadata,
    requested_fields: &[usize],
    binds_overlay_overflow: bool,
    config: RelationalRowDeltaConfig,
) -> Result<RelationalRowPageProjectedOverlayValue, RelationalRowDeltaError> {
    if kind == 0 {
        return Ok(RelationalRowPageProjectedOverlayValue::Deleted);
    }
    let fields = super::super::value::decode_row_fields(
        encoded_row,
        table.column_count.get() as usize,
        Some(requested_fields),
        config.row_limits,
    )?;
    Ok(RelationalRowPageProjectedOverlayValue::Present {
        fields: fields.into_boxed_slice(),
        binds_overlay_overflow,
    })
}

fn decode_value(
    kind: u8,
    encoded_row: &[u8],
    table: &super::RelationalRowDeltaTableMetadata,
    config: RelationalRowDeltaConfig,
) -> Result<RelationalRowPageRecoveredValue, RelationalRowDeltaError> {
    if kind == 0 {
        return Ok(RelationalRowPageRecoveredValue::Deleted);
    }
    let fields = super::super::value::decode_row_fields(
        encoded_row,
        table.column_count.get() as usize,
        None,
        config.row_limits,
    )?;
    let values = fields.into_iter().map(|field| field.value).collect();
    Ok(RelationalRowPageRecoveredValue::Present(
        RelationalRow::new(values),
    ))
}

fn should_poison(error: &RelationalRowDeltaError) -> bool {
    matches!(
        error,
        RelationalRowDeltaError::Corrupt(_) | RelationalRowDeltaError::Durability(_)
    )
}
