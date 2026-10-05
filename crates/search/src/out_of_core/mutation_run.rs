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

//! Immutable target-specific retractions for incremental search mutations.

use super::{
    SearchOutOfCoreConfig, SearchOutOfCoreManifestBody, SearchOutOfCoreMutationRunManifest,
    SearchOutOfCoreSegmentReader,
};
use crate::error::{HawDBError, Result};
#[cfg(test)]
use crate::lexical_projection::{DocumentsDigest, LexicalProjectionReader};
use crate::SearchAnalyzerLexicon;
#[cfg(test)]
use crate::{checksum_bytes, SearchDocument};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;

mod streaming;
pub(crate) mod terms;

pub(super) const MUTATION_RUN_FORMAT: &str = "HAWDB_SEARCH_MUTATION_RUN_V1";
const MUTATION_RUN_PREFIX: &str = "search_projection_mutation_run.";

fn size_overflow() -> HawDBError {
    HawDBError::Storage("search mutation-run working size overflow".into())
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or_else(size_overflow)
}

fn multiply(left: u64, right: usize) -> Result<u64> {
    left.checked_mul(right as u64).ok_or_else(size_overflow)
}

/// Prefix admission for all run ownership, plus the later closure-validation
/// sets. Transient read/serde space is checked without retaining its charge.
#[derive(Debug)]
pub(super) struct MutationRunBudget {
    limit: u64,
    retained: u64,
}

impl MutationRunBudget {
    pub(super) fn new(limit: u64, runs: usize, segments: usize) -> Result<Self> {
        let mut budget = Self { limit, retained: 0 };
        if runs != 0 {
            budget.retain(add(
                multiply(runs as u64, std::mem::size_of::<SearchMutationRun>())?,
                multiply(segments as u64, crate::build_memory::SET_ENTRY_BYTES)?,
            )?)?;
        }
        Ok(budget)
    }

    fn check(&self, additional: u64) -> Result<u64> {
        let total = add(self.retained, additional)?;
        if total > self.limit {
            return Err(HawDBError::Storage(format!(
                "search mutation-run working set requires {total} bytes, exceeding {}",
                self.limit,
            )));
        }
        Ok(total)
    }

    pub(super) fn admit_entries(&self, entries: &[SearchMutationRunEntry]) -> Result<()> {
        self.check(streaming::working_bytes(
            entries.len(),
            streaming::entries_scalar(entries)?,
        )?)?;
        Ok(())
    }

    fn retain(&mut self, additional: u64) -> Result<()> {
        self.retained = self.check(additional)?;
        Ok(())
    }
}

/// Preflight this fixed JSON schema without allocating. Unlike record-only
/// manifest preflight, count scalar strings too: unique_terms is Vec<String>.
/// The capacity formula includes pinned Vec growth/overlap, each array's
/// minimum capacity, owned string bytes and reusable serde/error scratch.
#[cfg(test)]
fn decode_capacity(bytes: &[u8]) -> Result<u64> {
    let mut objects = 0u64;
    let mut arrays = 0u64;
    let mut strings = 0u64;
    let mut quoted = false;
    let mut escaped = false;
    let mut token_bytes = 0u64;
    let mut largest = 8u64;
    for &byte in bytes {
        if quoted {
            token_bytes += 1;
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
                largest = largest.max(token_bytes);
                token_bytes = 0;
            }
        } else {
            match byte {
                b'"' => {
                    largest = largest.max(token_bytes);
                    token_bytes = 0;
                    strings += 1;
                    quoted = true;
                }
                b'{' => objects += 1,
                b'[' => arrays += 1,
                b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E' => token_bytes += 1,
                _ => {
                    largest = largest.max(token_bytes);
                    token_bytes = 0;
                }
            }
        }
    }
    largest = largest.max(token_bytes);
    let entry_bytes = std::mem::size_of::<SearchMutationRunEntry>();
    let entry_slots = multiply(multiply(objects, 3)?, entry_bytes)?;
    let term_slots = multiply(multiply(strings, 3)?, std::mem::size_of::<String>())?;
    let minima = multiply(multiply(arrays, 4)?, entry_bytes)?;
    add(
        add(
            add(add(bytes.len() as u64, entry_slots)?, term_slots)?,
            minima,
        )?,
        add(multiply(largest, 8)?, 4096)?,
    )
}

pub(super) fn artifact_file(generation: u64) -> String {
    format!("{MUTATION_RUN_PREFIX}{generation}.hawdb")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SearchMutationOperation {
    Delete,
    Replace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[serde(deny_unknown_fields)]
pub(super) struct SearchMutationRetraction {
    pub(super) documents_digest: u64,
    pub(super) lexical_document_len: u64,
    pub(super) unique_terms: terms::Terms,
}

impl SearchMutationRetraction {
    #[cfg(test)]
    pub(super) fn from_document(
        document: &SearchDocument,
        projection: &LexicalProjectionReader,
        analyzer: &SearchAnalyzerLexicon,
    ) -> Result<Self> {
        let (lexical_document_len, unique_terms) =
            projection.document_retraction(document, analyzer)?;
        let mut digest = DocumentsDigest::default();
        digest.add_bytes(crate::encode_search_document_line(document).as_bytes());
        Ok(Self {
            documents_digest: digest.finish(),
            lexical_document_len,
            unique_terms: unique_terms.into(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[serde(deny_unknown_fields)]
pub(super) struct SearchMutationRunEntry {
    pub(super) document_id: String,
    pub(super) target_segment_id: u64,
    pub(super) operation: SearchMutationOperation,
    pub(super) retraction: SearchMutationRetraction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[serde(deny_unknown_fields)]
pub(super) struct SearchMutationRunBody {
    format: String,
    generation: u64,
    analyzer_digest: u64,
    entries: Vec<SearchMutationRunEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchMutationRunEnvelope {
    body: SearchMutationRunBody,
    checksum: u64,
}

#[derive(Debug)]
pub(super) struct SearchMutationRun {
    body: SearchMutationRunBody,
    open_working_bytes: u64,
}

/// One target-bound predicate shared by every read path of a validated closure.
/// Runs are ordered by document ID; the segment match must remain separate so
/// an old version's retraction cannot hide a replacement with the same ID.
#[derive(Debug, Default)]
pub(super) struct MutationVisibility {
    runs: Vec<SearchMutationRun>,
}

impl MutationVisibility {
    pub(super) fn publication_budget(
        &self,
        limit: u64,
        content_segments: usize,
        append_run: bool,
    ) -> Result<MutationRunBudget> {
        let runs = self
            .runs
            .len()
            .checked_add(usize::from(append_run))
            .ok_or_else(size_overflow)?;
        let mut budget = MutationRunBudget::new(limit, runs, content_segments)?;
        for run in &self.runs {
            budget.check(run.open_working_bytes)?;
            budget.retain(add(
                add(
                    run.body.retained_capacity()?,
                    (std::mem::size_of::<terms::TermFile>() + 2 * std::mem::size_of::<usize>())
                        as u64,
                )?,
                multiply(
                    run.entries().len() as u64,
                    crate::build_memory::SET_ENTRY_BYTES,
                )?,
            )?)?;
        }
        Ok(budget)
    }

    pub(super) fn from_validated_runs(runs: Vec<SearchMutationRun>) -> Self {
        Self { runs }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub(super) fn has_target_segment(&self, segment_id: u64) -> bool {
        self.retractions()
            .any(|entry| entry.target_segment_id == segment_id)
    }

    pub(super) fn is_visible(&self, segment_id: u64, document_id: &str) -> bool {
        !self.runs.iter().any(|run| {
            let entries = run.entries();
            let Ok(mut index) =
                entries.binary_search_by(|entry| entry.document_id.as_str().cmp(document_id))
            else {
                return false;
            };
            while index > 0 && entries[index - 1].document_id == document_id {
                index -= 1;
            }
            entries[index..]
                .iter()
                .take_while(|entry| entry.document_id == document_id)
                .any(|entry| entry.target_segment_id == segment_id)
        })
    }

    pub(super) fn retractions(&self) -> impl Iterator<Item = &SearchMutationRunEntry> {
        self.runs.iter().flat_map(SearchMutationRun::entries)
    }

    pub(super) fn visible_count(
        &self,
        content_segment_id: u64,
        segment: &super::SearchSegmentDescriptorEntry,
    ) -> Result<usize> {
        let hidden = self
            .retractions()
            .filter(|entry| {
                entry.target_segment_id == content_segment_id
                    && entry.document_id >= segment.first_document_id
                    && entry.document_id <= segment.last_document_id
            })
            .count();
        segment
            .document_count
            .checked_sub(hidden)
            .ok_or_else(|| HawDBError::Storage("search mutation visibility count underflow".into()))
    }
}

impl SearchMutationRunBody {
    fn retained_capacity(&self) -> Result<u64> {
        let mut bytes = add(
            self.format.capacity() as u64,
            multiply(
                self.entries.capacity() as u64,
                std::mem::size_of::<SearchMutationRunEntry>(),
            )?,
        )?;
        for entry in &self.entries {
            bytes = add(bytes, entry.document_id.capacity() as u64)?;
            bytes = add(bytes, entry.retraction.unique_terms.retained_bytes()?)?;
        }
        Ok(bytes)
    }

    #[cfg(test)]
    pub(super) fn new(
        generation: u64,
        analyzer_digest: u64,
        entries: Vec<SearchMutationRunEntry>,
    ) -> Result<Self> {
        let body = Self {
            format: MUTATION_RUN_FORMAT.to_string(),
            generation,
            analyzer_digest,
            entries,
        };
        body.validate()?;
        Ok(body)
    }

    #[cfg(test)]
    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let body = serde_json::to_vec(self).map_err(|error| {
            HawDBError::Storage(format!("failed to encode search mutation run: {error}"))
        })?;
        serde_json::to_vec(&SearchMutationRunEnvelope {
            body: self.clone(),
            checksum: checksum_bytes(&body),
        })
        .map_err(|error| {
            HawDBError::Storage(format!(
                "failed to encode search mutation-run envelope: {error}"
            ))
        })
    }

    fn validate(&self) -> Result<()> {
        if self.format != MUTATION_RUN_FORMAT || self.generation == 0 || self.entries.is_empty() {
            return Err(HawDBError::Storage(
                "search mutation-run header or entries are invalid".to_string(),
            ));
        }
        let mut previous_key: Option<(&str, u64)> = None;
        for entry in &self.entries {
            let key = (entry.document_id.as_str(), entry.target_segment_id);
            if entry.document_id.is_empty() || previous_key.is_some_and(|previous| previous >= key)
            {
                return Err(HawDBError::Storage(
                    "search mutation-run entries are not ordered by document and target segment"
                        .to_string(),
                ));
            }
            entry.retraction.unique_terms.validate()?;
            previous_key = Some(key);
        }
        Ok(())
    }
}

impl SearchMutationRun {
    pub(super) fn open(
        root: &Path,
        manifest: &SearchOutOfCoreMutationRunManifest,
        max_bytes: u64,
        expected_analyzer_digest: u64,
        budget: &mut MutationRunBudget,
    ) -> Result<Self> {
        if manifest.len > max_bytes {
            return Err(HawDBError::Storage(
                "search mutation-run artifact exceeds the configured read budget".to_string(),
            ));
        }
        budget.check(8192)?;
        let file = hawdb_storage::file_io::File::open(root.join(&manifest.file))?;
        let scalar = streaming::preflight(&file, manifest)?;
        let working_bytes = streaming::working_bytes(manifest.entry_count, scalar)?;
        budget.check(working_bytes)?;
        let file = terms::TermFile::new(file, None)?;
        let envelope = streaming::decode(file, manifest, scalar)?;
        let target_index_bytes = multiply(
            manifest.entry_count as u64,
            crate::build_memory::SET_ENTRY_BYTES,
        )?;
        if crate::build_control::json::checksum_with_context(&envelope.body, None)?
            != envelope.checksum
        {
            return Err(HawDBError::Storage(
                "search mutation-run envelope checksum mismatch".to_string(),
            ));
        }
        envelope.body.validate()?;
        if envelope.body.generation != manifest.generation
            || envelope.body.analyzer_digest != manifest.analyzer_digest
            || envelope.body.entries.len() != manifest.entry_count
        {
            return Err(HawDBError::Storage(
                "search mutation-run metadata does not match its manifest reference".to_string(),
            ));
        }
        if envelope.body.analyzer_digest != expected_analyzer_digest {
            return Err(HawDBError::Storage(
                "search mutation-run analyzer identity does not match the active reader"
                    .to_string(),
            ));
        }
        let retained = add(
            envelope.body.retained_capacity()?,
            (std::mem::size_of::<terms::TermFile>() + 2 * std::mem::size_of::<usize>()) as u64,
        )?;
        budget.retain(add(retained, target_index_bytes)?)?;
        Ok(Self {
            body: envelope.body,
            open_working_bytes: working_bytes,
        })
    }

    pub(super) fn entries(&self) -> &[SearchMutationRunEntry] {
        &self.body.entries
    }
}

pub(super) fn validate_closure(
    manifest: &SearchOutOfCoreManifestBody,
    runs: &[SearchMutationRun],
) -> Result<()> {
    if runs.len() != manifest.mutation_runs.len() {
        return Err(HawDBError::Storage(
            "search mutation-run closure does not match the active manifest".to_string(),
        ));
    }
    // Existing content-only manifests retain their historical validation path.
    // The exact reconstruction becomes mandatory when retractions make the
    // top-level identity depend on artifacts outside the content segments.
    if runs.is_empty() {
        return Ok(());
    }
    let active_segments = manifest
        .segments
        .iter()
        .map(|segment| segment.segment_id)
        .collect::<BTreeSet<_>>();
    let mut visible_document_count =
        manifest
            .segments
            .iter()
            .try_fold(0usize, |total, segment| {
                total.checked_add(segment.document_count).ok_or_else(|| {
                    HawDBError::Storage("search mutation-run document count overflows".to_string())
                })
            })?;
    let mut visible_documents_digest = manifest.segments.iter().fold(0u64, |digest, segment| {
        crate::lexical_projection::DocumentsDigest::combine(digest, segment.documents_digest)
    });
    let mut retracted_targets = BTreeSet::new();
    for run in runs {
        for entry in run.entries() {
            if !active_segments.contains(&entry.target_segment_id) {
                return Err(HawDBError::Storage(
                    "search mutation-run targets a segment outside the active manifest".to_string(),
                ));
            }
            if !retracted_targets.insert((entry.target_segment_id, entry.document_id.as_str())) {
                return Err(HawDBError::Storage(
                    "search mutation-run retracts the same segment document more than once"
                        .to_string(),
                ));
            }
            visible_document_count = visible_document_count.checked_sub(1).ok_or_else(|| {
                HawDBError::Storage(
                    "search mutation-run retractions exceed the active content count".to_string(),
                )
            })?;
            visible_documents_digest = crate::lexical_projection::DocumentsDigest::replace(
                visible_documents_digest,
                entry.retraction.documents_digest,
                0,
            );
        }
    }
    if visible_document_count != manifest.document_count
        || visible_documents_digest != manifest.documents_digest
    {
        return Err(HawDBError::Storage(
            "search mutation-run retractions do not match the active manifest identity".to_string(),
        ));
    }
    Ok(())
}

/// Verify each retraction against the immutable version it claims to remove.
/// Checksums and aggregate identity alone cannot prove that a target exists or
/// that its term/length contribution is exact. Stage one version at a time and
/// compare spillable contributions without retaining a complete body or term set.
pub(super) fn validate_targets(
    root: &Path,
    segments: &[SearchOutOfCoreSegmentReader],
    runs: &[SearchMutationRun],
    config: &SearchOutOfCoreConfig,
    analyzer: &SearchAnalyzerLexicon,
) -> Result<()> {
    if runs.is_empty() {
        return Ok(());
    }
    let task = hawdb_core::RuntimeTaskContext::default().with_memory_reservation(
        hawdb_core::RuntimeMemoryReservation::new(config.max_reanalysis_working_bytes.get(), 0),
    );
    let memory = crate::build_memory::BuildMemory::new(&task)?;
    let mut stage = super::generation_writer::StageDirectory::create(root, &memory, &task)?;
    stage.reserve_disk(
        config
            .max_uncompressed_segment_bytes
            .get()
            .checked_add(config.max_reanalysis_spill_bytes.get())
            .ok_or_else(size_overflow)?,
    );
    let path = crate::build_memory::path::OwnedPath::join(
        &stage.path,
        Path::new("validate-target.body"),
        &memory,
        &task,
    )?;
    for entry in runs.iter().flat_map(SearchMutationRun::entries) {
        let missing = || {
            HawDBError::Storage(
                "search mutation-run target document is absent from its content segment".into(),
            )
        };
        let artifact = segments
            .iter()
            .find(|segment| segment.content_segment_id == entry.target_segment_id)
            .ok_or_else(missing)?;
        let id = entry.document_id.as_str();
        let position = artifact
            .descriptor
            .segments
            .binary_search_by(|segment| {
                if segment.last_document_id.as_str() < id {
                    std::cmp::Ordering::Less
                } else if segment.first_document_id.as_str() > id {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .map_err(|_| missing())?;
        if !artifact.lexical_projection.probe_document_id(id)?.present {
            return Err(missing());
        }
        let file = hawdb_storage::file_io::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&*path)?;
        let source = super::hydration::source::Source::capture(
            artifact,
            &artifact.descriptor.segments[position],
            id,
            file,
            (
                config.max_uncompressed_segment_bytes.get(),
                config.max_document_header_bytes.get(),
            ),
            &memory,
            &task,
        )?;
        let mut terms = entry.retraction.unique_terms.cursor()?;
        let length = artifact.lexical_projection.source_retraction(
            &source,
            analyzer,
            crate::lexical_projection::retraction::RetractionContext {
                root: &stage.path,
                memory: &memory,
                task: &task,
                needs_chinese: source.needs_chinese,
            },
            |term| {
                if terms.next()?.as_deref() != Some(&term) {
                    return Err(HawDBError::Storage(
                        "search mutation-run retraction does not match its target document".into(),
                    ));
                }
                Ok(())
            },
        )?;
        if terms.next()?.is_some()
            || length != entry.retraction.lexical_document_len
            || source.documents_digest()? != entry.retraction.documents_digest
        {
            return Err(HawDBError::Storage(
                "search mutation-run retraction does not match its target document".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_allocation as allocation;

    #[test]
    fn empty_retractions_obey_the_shared_term_file_quota() {
        let root = crate::out_of_core::tests::test_dir("empty_retraction_quota");
        std::fs::create_dir_all(&root).unwrap();
        let memory = crate::build_memory::BuildMemory::new(&Default::default()).unwrap();
        let file = terms::TermFile::new(
            hawdb_storage::file_io::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(root.join("terms.json"))
                .unwrap(),
            Some(&memory),
        )
        .unwrap();
        assert!(terms::Writer::new(file.clone(), &memory, 1).is_err());
        assert_eq!(file.file.metadata().unwrap().len(), 0);
        let empty = terms::Writer::new(file.clone(), &memory, 2)
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(empty.len(), 0);
        empty.validate().unwrap();
        assert!(terms::Writer::new(file.clone(), &memory, 3).is_err());
        assert_eq!(file.file.metadata().unwrap().len(), 2);
        drop(empty);
        drop(file);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn streamed_terms_preserve_escaped_json_and_detect_late_range_corruption() {
        let root = crate::out_of_core::tests::test_dir("mutation_json_ranges");
        std::fs::create_dir_all(&root).unwrap();
        let mut target = entry("memory:escaped");
        target.retraction.unique_terms =
            vec!["graph".to_owned(), "quote\"slash\\\u{96ea}".to_owned()].into();
        let body = SearchMutationRunBody::new(3, 5, vec![target]).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body.encode().unwrap()).unwrap();
        let text = serde_json::to_string_pretty(&value)
            .unwrap()
            .replace("\"graph\"", "\"gr\\u0061ph\"");
        let path = root.join(artifact_file(3));
        std::fs::write(&path, text.as_bytes()).unwrap();
        let manifest = SearchOutOfCoreMutationRunManifest {
            generation: 3,
            file: artifact_file(3),
            len: text.len() as u64,
            checksum: checksum_bytes(text.as_bytes()),
            entry_count: 1,
            analyzer_digest: 5,
        };
        let mut budget = MutationRunBudget::new(64 * 1024, 1, 1).unwrap();
        let run = SearchMutationRun::open(&root, &manifest, u64::MAX, 5, &mut budget).unwrap();
        assert_eq!(run.body, body);
        // Alter a syntactically valid term after open without changing its size.
        let changed = text.replace("gr\\u0061ph", "gr\\u0061px");
        assert_ne!(changed, text);
        std::fs::write(&path, changed).unwrap();
        let error = run.entries()[0]
            .retraction
            .unique_terms
            .visit(|_| Ok(()))
            .unwrap_err();
        assert!(
            error.to_string().contains("range checksum mismatch"),
            "{error}"
        );
        drop(run);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn mutation_decode_preflight_covers_scalar_terms_and_escaped_strings() {
        let _serial = allocation::serial();
        for count in [0, 1, 8, 512, 16_385] {
            let mut target = entry("memory:allocation");
            target.retraction.unique_terms = (0..count)
                .map(|index| format!("t{index:05}\"\\\n雪"))
                .collect();
            let body = SearchMutationRunBody::new(3, 5, vec![target]).unwrap();
            let bytes = body.encode().unwrap();
            let bound = decode_capacity(&bytes).unwrap();
            let (decoded, peak) = allocation::measure(|| {
                serde_json::from_slice::<SearchMutationRunEnvelope>(&bytes).unwrap()
            });
            assert!(
                peak as u64 <= bound,
                "terms={count}, peak={peak}, bound={bound}"
            );
            assert!(decoded.body.retained_capacity().unwrap() <= bound);
            assert_eq!(decoded.body, body);
            drop(decoded);
            assert_eq!(allocation::live(), 0);
        }
        let mut target = entry("memory:long-term");
        target.retraction.unique_terms = vec!["\"\\\n雪".repeat(16_384)].into();
        let bytes = SearchMutationRunBody::new(3, 5, vec![target])
            .unwrap()
            .encode()
            .unwrap();
        let (decoded, peak) = allocation::measure(|| {
            serde_json::from_slice::<SearchMutationRunEnvelope>(&bytes).unwrap()
        });
        assert!(peak as u64 <= decode_capacity(&bytes).unwrap());
        drop(decoded);
        assert_eq!(allocation::live(), 0);
    }

    #[test]
    fn mutation_working_budget_rejects_combined_runs_before_second_decode() {
        let _serial = allocation::serial();
        let mut sequence = 0u64;
        let path = loop {
            let candidate = std::env::temp_dir().join(format!(
                "hawdb-mutation-budget-{}-{sequence}",
                std::process::id()
            ));
            match std::fs::create_dir(&candidate) {
                Ok(()) => break candidate,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    sequence += 1;
                }
                Err(error) => panic!("create mutation fixture: {error}"),
            }
        };
        let mut manifests = Vec::new();
        let mut phases = Vec::new();
        for generation in [3, 4] {
            let mut target = entry(&format!("memory:{generation:03}"));
            target.retraction.unique_terms =
                (0..4096).map(|index| format!("term{index:05}")).collect();
            let bytes = SearchMutationRunBody::new(generation, 5, vec![target])
                .unwrap()
                .encode()
                .unwrap();
            let file = artifact_file(generation);
            std::fs::write(path.join(&file), &bytes).unwrap();
            phases.push(streaming::working_bytes(1, 30).unwrap());
            assert!(*phases.last().unwrap() < bytes.len() as u64);
            manifests.push(SearchOutOfCoreMutationRunManifest {
                generation,
                file,
                len: bytes.len() as u64,
                checksum: checksum_bytes(&bytes),
                entry_count: 1,
                analyzer_digest: 5,
            });
        }
        let base = MutationRunBudget::new(u64::MAX, 2, 1).unwrap().retained;
        let first_limit = base + phases[0];
        let mut rejected = MutationRunBudget::new(first_limit - 1, 2, 1).unwrap();
        let (error, rejected_peak) = allocation::measure(|| {
            SearchMutationRun::open(&path, &manifests[0], u64::MAX, 5, &mut rejected).unwrap_err()
        });
        assert!(error.to_string().contains("mutation-run working set"));
        assert_eq!(rejected.retained, base);
        drop(error);
        // Rejected decoding scanned fixed blocks without retaining the term array.
        assert!(rejected_peak as u64 <= 2 * manifests[0].len + 8192);
        assert_eq!(allocation::live(), 0);

        let limit = base + phases.iter().copied().max().unwrap();
        let mut budget = MutationRunBudget::new(limit, 2, 1).unwrap();
        let (first, peak) = allocation::measure(|| {
            SearchMutationRun::open(&path, &manifests[0], u64::MAX, 5, &mut budget).unwrap()
        });
        assert!(peak as u64 <= limit);
        let retained = budget.retained;
        let (error, peak) = allocation::measure(|| {
            SearchMutationRun::open(&path, &manifests[1], u64::MAX, 5, &mut budget).unwrap_err()
        });
        assert!(error.to_string().contains("mutation-run working set"));
        assert!(peak as u64 <= limit);
        assert_eq!(budget.retained, retained);
        drop(error);
        drop(first);
        assert_eq!(allocation::live(), 0);
        let mut standalone = MutationRunBudget::new(limit, 2, 1).unwrap();
        SearchMutationRun::open(&path, &manifests[1], u64::MAX, 5, &mut standalone).unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }

    fn entry(document_id: &str) -> SearchMutationRunEntry {
        SearchMutationRunEntry {
            document_id: document_id.to_string(),
            target_segment_id: 7,
            operation: SearchMutationOperation::Replace,
            retraction: SearchMutationRetraction {
                documents_digest: 11,
                lexical_document_len: 3,
                unique_terms: vec!["graph".to_string(), "memory".to_string()].into(),
            },
        }
    }

    #[test]
    fn visibility_preserves_latest_version_after_repeated_replacements() {
        let first = entry("memory:001");
        let mut second = first.clone();
        second.target_segment_id = 8;
        let visibility = MutationVisibility::from_validated_runs(vec![
            SearchMutationRun {
                open_working_bytes: 0,
                body: SearchMutationRunBody::new(3, 5, vec![first]).unwrap(),
            },
            SearchMutationRun {
                open_working_bytes: 0,
                body: SearchMutationRunBody::new(4, 5, vec![second]).unwrap(),
            },
        ]);
        for segment in [7, 8, 9] {
            assert_eq!(visibility.is_visible(segment, "memory:001"), segment == 9);
            assert!(visibility.is_visible(segment, "memory:002"));
        }
        let mut combined = entry("memory:001");
        combined.target_segment_id = 8;
        let combined_visibility =
            MutationVisibility::from_validated_runs(vec![SearchMutationRun {
                open_working_bytes: 0,
                body: SearchMutationRunBody::new(5, 5, vec![entry("memory:001"), combined])
                    .unwrap(),
            }]);
        assert!(!combined_visibility.is_visible(7, "memory:001"));
        assert!(!combined_visibility.is_visible(8, "memory:001"));
        assert!(combined_visibility.is_visible(9, "memory:001"));
        assert!(MutationVisibility::default().is_visible(7, "memory:001"));
    }

    #[test]
    fn encoding_requires_ordered_document_target_and_term_entries() {
        assert!(
            SearchMutationRunBody::new(3, 5, vec![entry("memory:002"), entry("memory:001")])
                .is_err()
        );
        let mut same_document = entry("memory:001");
        same_document.target_segment_id = 8;
        assert!(SearchMutationRunBody::new(3, 5, vec![entry("memory:001"), same_document]).is_ok());
        let mut repeated_term = entry("memory:001");
        repeated_term
            .retraction
            .unique_terms
            .owned_mut()
            .push("memory".to_string());
        assert!(SearchMutationRunBody::new(3, 5, vec![repeated_term]).is_err());
    }

    #[test]
    fn encoding_round_trips_a_checksumming_envelope() {
        let body = SearchMutationRunBody::new(3, 5, vec![entry("memory:001")]).unwrap();
        let encoded = body.encode().unwrap();
        let envelope: SearchMutationRunEnvelope = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(envelope.body, body);
        assert_eq!(
            checksum_bytes(&serde_json::to_vec(&envelope.body).unwrap()),
            envelope.checksum
        );
    }
}
