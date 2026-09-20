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

use super::{checksum_bytes, SearchOutOfCoreManifestBody, SearchOutOfCoreMutationRunManifest};
use crate::bounded_file::read_bounded_file;
use crate::error::{HawDBError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;

const MUTATION_RUN_FORMAT: &str = "HAWDB_SEARCH_MUTATION_RUN_V1";
const MUTATION_RUN_PREFIX: &str = "search_projection_mutation_run.";

pub(super) fn artifact_file(generation: u64) -> String {
    format!("{MUTATION_RUN_PREFIX}{generation}.hawdb")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SearchMutationOperation {
    Delete,
    Replace,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SearchMutationRetraction {
    pub(super) documents_digest: u64,
    pub(super) lexical_document_len: u64,
    pub(super) unique_terms: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SearchMutationRunEntry {
    pub(super) document_id: String,
    pub(super) target_segment_id: u64,
    pub(super) operation: SearchMutationOperation,
    pub(super) retraction: SearchMutationRetraction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

impl SearchMutationRunBody {
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
        let mut previous_document_id = None;
        for entry in &self.entries {
            if entry.document_id.is_empty()
                || previous_document_id
                    .is_some_and(|previous: &String| previous >= &entry.document_id)
            {
                return Err(HawDBError::Storage(
                    "search mutation-run entries are not strictly ordered by document id"
                        .to_string(),
                ));
            }
            let mut previous_term = None;
            for term in &entry.retraction.unique_terms {
                if term.is_empty()
                    || previous_term.is_some_and(|previous: &String| previous >= term)
                {
                    return Err(HawDBError::Storage(
                        "search mutation-run retraction terms are not strictly ordered".to_string(),
                    ));
                }
                previous_term = Some(term);
            }
            previous_document_id = Some(&entry.document_id);
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
    ) -> Result<Self> {
        if manifest.len > max_bytes {
            return Err(HawDBError::Storage(
                "search mutation-run artifact exceeds the configured read budget".to_string(),
            ));
        }
        let bytes = read_bounded_file(&root.join(&manifest.file), manifest.len)?;
        if bytes.len() as u64 != manifest.len || checksum_bytes(&bytes) != manifest.checksum {
            return Err(HawDBError::Storage(
                "search mutation-run artifact length or checksum mismatch".to_string(),
            ));
        }
        let envelope: SearchMutationRunEnvelope =
            serde_json::from_slice(&bytes).map_err(|error| {
                HawDBError::Storage(format!("invalid search mutation-run artifact: {error}"))
            })?;
        let body_bytes = serde_json::to_vec(&envelope.body).map_err(|error| {
            HawDBError::Storage(format!("failed to verify search mutation run: {error}"))
        })?;
        if checksum_bytes(&body_bytes) != envelope.checksum {
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
        Ok(Self {
            body: envelope.body,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(document_id: &str) -> SearchMutationRunEntry {
        SearchMutationRunEntry {
            document_id: document_id.to_string(),
            target_segment_id: 7,
            operation: SearchMutationOperation::Replace,
            retraction: SearchMutationRetraction {
                documents_digest: 11,
                lexical_document_len: 3,
                unique_terms: vec!["graph".to_string(), "memory".to_string()],
            },
        }
    }

    #[test]
    fn encoding_requires_sorted_unique_document_and_term_entries() {
        assert!(
            SearchMutationRunBody::new(3, 5, vec![entry("memory:002"), entry("memory:001")])
                .is_err()
        );
        let mut repeated_term = entry("memory:001");
        repeated_term
            .retraction
            .unique_terms
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
