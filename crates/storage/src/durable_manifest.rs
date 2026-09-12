//! The v1 durable manifest codec and publication binding validation.

use crate::artifact_files::{checkpoint_generation_file, wal_generation_file};
use crate::text::parse_u64;
use crate::{
    durable_replace_file, AppendGenerationArtifacts, AppendSegmentArtifactMetadata,
    CanonicalAdjacencyArtifactMetadata, CanonicalAdjacencyGenerationArtifacts,
    GraphDescriptorTreeArtifactMetadata, RelationalIndexArtifactMetadata,
    RelationalIndexGenerationArtifacts, RelationalOverflowArtifactMetadata,
    RelationalOverflowGenerationArtifacts, RelationalRowPageArtifactMetadata,
    RelationalRowPageGenerationArtifacts,
};
use skein_core::{Result, SkeinError};
use skein_integrity::{checksum_u64 as checksum_bytes, Sha256Digest};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy)]
pub struct DurableManifest {
    pub checkpoint_generation: Option<u64>,
    pub checkpoint_encoded_len: Option<u64>,
    pub checkpoint_encoded_checksum: Option<u64>,
    pub checkpoint_encoded_sha256: Option<Sha256Digest>,
    pub canonical_manifest_encoded_len: Option<u64>,
    pub canonical_manifest_encoded_checksum: Option<u64>,
    pub canonical_manifest_encoded_sha256: Option<Sha256Digest>,
    pub canonical_adjacency_generation_artifacts: Option<CanonicalAdjacencyGenerationArtifacts>,
    pub property_spill_manifest_encoded_len: Option<u64>,
    pub property_spill_manifest_encoded_checksum: Option<u64>,
    pub property_spill_manifest_encoded_sha256: Option<Sha256Digest>,
    pub property_projection_manifest_encoded_len: Option<u64>,
    pub property_projection_manifest_encoded_checksum: Option<u64>,
    pub property_projection_manifest_encoded_sha256: Option<Sha256Digest>,
    pub relational_row_generation_artifacts: Option<RelationalRowPageGenerationArtifacts>,
    pub relational_overflow_generation_artifacts: Option<RelationalOverflowGenerationArtifacts>,
    pub relational_index_generation_artifacts: Option<RelationalIndexGenerationArtifacts>,
    pub append_generation_artifacts: Option<AppendGenerationArtifacts>,
    pub wal_generation: u64,
    pub checkpoint_epoch: u64,
    pub checkpoint_commit_epoch: u64,
    pub oldest_reader_commit_epoch: Option<u64>,
    pub safe_reclaim_commit_epoch: u64,
    pub wal_replay_start_lsn: u64,
    pub next_lsn: u64,
    pub source_scan_commit_epoch: Option<u64>,
    pub source_scan_descriptor_checksum: Option<u64>,
}

#[derive(Default)]
struct RelationalIndexManifestFields {
    generation: Option<u64>,
    source_commit_epoch: Option<u64>,
    catalog_schema_digest: Option<Sha256Digest>,
    root_set_digest: Option<Sha256Digest>,
    page_encoded_len: Option<u64>,
    page_encoded_checksum: Option<u64>,
    page_encoded_sha256: Option<Sha256Digest>,
    manifest_encoded_len: Option<u64>,
    manifest_encoded_checksum: Option<u64>,
    manifest_encoded_sha256: Option<Sha256Digest>,
}

#[derive(Default)]
struct RelationalRootManifestFields {
    generation: Option<u64>,
    source_commit_epoch: Option<u64>,
    root_set_digest: Option<Sha256Digest>,
    manifest_encoded_len: Option<u64>,
    manifest_encoded_checksum: Option<u64>,
    manifest_encoded_sha256: Option<Sha256Digest>,
}

#[derive(Default)]
struct CanonicalAdjacencyGenerationFields {
    generation: Option<u64>,
    source_commit_epoch: Option<u64>,
    relationship_count: Option<u64>,
    entry_count: Option<u64>,
    artifact_encoded_len: Option<u64>,
    artifact_encoded_crc32c: Option<u64>,
    artifact_encoded_sha256: Option<Sha256Digest>,
    descriptor_root_encoded_len: Option<u64>,
    descriptor_root_encoded_crc32c: Option<u64>,
    descriptor_root_encoded_sha256: Option<Sha256Digest>,
}

impl CanonicalAdjacencyGenerationFields {
    fn finish(self) -> Result<Option<CanonicalAdjacencyGenerationArtifacts>> {
        let presence = [
            self.generation.is_some(),
            self.source_commit_epoch.is_some(),
            self.relationship_count.is_some(),
            self.entry_count.is_some(),
            self.artifact_encoded_len.is_some(),
            self.artifact_encoded_crc32c.is_some(),
            self.artifact_encoded_sha256.is_some(),
            self.descriptor_root_encoded_len.is_some(),
            self.descriptor_root_encoded_crc32c.is_some(),
            self.descriptor_root_encoded_sha256.is_some(),
        ];
        if presence.iter().all(|present| !present) {
            return Ok(None);
        }
        if !presence.iter().all(|present| *present) {
            return Err(SkeinError::Storage(
                "manifest canonical adjacency generation binding is incomplete".to_string(),
            ));
        }
        Ok(Some(CanonicalAdjacencyGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            relationship_count: self
                .relationship_count
                .expect("complete binding has relationship count"),
            entry_count: self.entry_count.expect("complete binding has entry count"),
            adjacency_artifact: CanonicalAdjacencyArtifactMetadata {
                encoded_len: self
                    .artifact_encoded_len
                    .expect("complete binding has adjacency artifact length"),
                encoded_crc32c: self
                    .artifact_encoded_crc32c
                    .expect("complete binding has adjacency artifact CRC32C"),
                encoded_sha256: self
                    .artifact_encoded_sha256
                    .expect("complete binding has adjacency artifact SHA-256"),
            },
            descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata {
                encoded_len: self
                    .descriptor_root_encoded_len
                    .expect("complete binding has descriptor root length"),
                encoded_crc32c: u32::try_from(
                    self.descriptor_root_encoded_crc32c
                        .expect("complete binding has descriptor root CRC32C"),
                )
                .map_err(|_| {
                    SkeinError::Storage(
                        "canonical adjacency descriptor root checksum exceeds CRC32C range"
                            .to_string(),
                    )
                })?,
                encoded_sha256: self
                    .descriptor_root_encoded_sha256
                    .expect("complete binding has descriptor root SHA-256"),
            },
        }))
    }
}

impl RelationalRootManifestFields {
    fn presence(&self) -> [bool; 6] {
        [
            self.generation.is_some(),
            self.source_commit_epoch.is_some(),
            self.root_set_digest.is_some(),
            self.manifest_encoded_len.is_some(),
            self.manifest_encoded_checksum.is_some(),
            self.manifest_encoded_sha256.is_some(),
        ]
    }

    fn require_complete(&self, artifact: &str) -> Result<bool> {
        let presence = self.presence();
        if presence.iter().all(|present| !present) {
            return Ok(false);
        }
        if !presence.iter().all(|present| *present) {
            return Err(SkeinError::Storage(format!(
                "manifest {artifact} generation binding is incomplete"
            )));
        }
        Ok(true)
    }

    fn finish_row(self) -> Result<Option<RelationalRowPageGenerationArtifacts>> {
        if !self.require_complete("relational row-page")? {
            return Ok(None);
        }
        Ok(Some(RelationalRowPageGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            root_set_digest: self
                .root_set_digest
                .expect("complete binding has root-set digest"),
            manifest_artifact: RelationalRowPageArtifactMetadata {
                encoded_len: self
                    .manifest_encoded_len
                    .expect("complete binding has manifest length"),
                encoded_crc32c: u32::try_from(
                    self.manifest_encoded_checksum
                        .expect("complete binding has manifest checksum"),
                )
                .map_err(|_| {
                    SkeinError::Storage(
                        "relational row-page manifest checksum exceeds CRC32C range".to_string(),
                    )
                })?,
                encoded_sha256: self
                    .manifest_encoded_sha256
                    .expect("complete binding has manifest SHA-256"),
            },
        }))
    }

    fn finish_append(self) -> Result<Option<AppendGenerationArtifacts>> {
        if !self.require_complete("append")? {
            return Ok(None);
        }
        Ok(Some(AppendGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            root_set_digest: self
                .root_set_digest
                .expect("complete binding has root-set digest"),
            manifest_artifact: AppendSegmentArtifactMetadata {
                encoded_len: self
                    .manifest_encoded_len
                    .expect("complete binding has manifest length"),
                encoded_crc32c: u32::try_from(
                    self.manifest_encoded_checksum
                        .expect("complete binding has manifest checksum"),
                )
                .map_err(|_| {
                    SkeinError::Storage("append manifest checksum exceeds CRC32C range".to_string())
                })?,
                encoded_sha256: self
                    .manifest_encoded_sha256
                    .expect("complete binding has manifest SHA-256"),
            },
        }))
    }

    fn finish_overflow(self) -> Result<Option<RelationalOverflowGenerationArtifacts>> {
        if !self.require_complete("relational overflow")? {
            return Ok(None);
        }
        Ok(Some(RelationalOverflowGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            root_set_digest: self
                .root_set_digest
                .expect("complete binding has root-set digest"),
            manifest_artifact: RelationalOverflowArtifactMetadata {
                encoded_len: self
                    .manifest_encoded_len
                    .expect("complete binding has manifest length"),
                encoded_crc32c: u32::try_from(
                    self.manifest_encoded_checksum
                        .expect("complete binding has manifest checksum"),
                )
                .map_err(|_| {
                    SkeinError::Storage(
                        "relational overflow manifest checksum exceeds CRC32C range".to_string(),
                    )
                })?,
                encoded_sha256: self
                    .manifest_encoded_sha256
                    .expect("complete binding has manifest SHA-256"),
            },
        }))
    }
}

impl RelationalIndexManifestFields {
    fn finish(self) -> Result<Option<RelationalIndexGenerationArtifacts>> {
        let presence = [
            self.generation.is_some(),
            self.source_commit_epoch.is_some(),
            self.catalog_schema_digest.is_some(),
            self.root_set_digest.is_some(),
            self.page_encoded_len.is_some(),
            self.page_encoded_checksum.is_some(),
            self.page_encoded_sha256.is_some(),
            self.manifest_encoded_len.is_some(),
            self.manifest_encoded_checksum.is_some(),
            self.manifest_encoded_sha256.is_some(),
        ];
        if presence.iter().all(|present| !present) {
            return Ok(None);
        }
        if !presence.iter().all(|present| *present) {
            return Err(SkeinError::Storage(
                "manifest relational index generation binding is incomplete".to_string(),
            ));
        }
        Ok(Some(RelationalIndexGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            catalog_schema_digest: self
                .catalog_schema_digest
                .expect("complete binding has catalog schema digest"),
            root_set_digest: self
                .root_set_digest
                .expect("complete binding has root-set digest"),
            page_artifact: RelationalIndexArtifactMetadata {
                encoded_len: self
                    .page_encoded_len
                    .expect("complete binding has page length"),
                encoded_crc32c: self
                    .page_encoded_checksum
                    .expect("complete binding has page checksum"),
                encoded_sha256: self
                    .page_encoded_sha256
                    .expect("complete binding has page SHA-256"),
            },
            manifest_artifact: RelationalIndexArtifactMetadata {
                encoded_len: self
                    .manifest_encoded_len
                    .expect("complete binding has manifest length"),
                encoded_crc32c: self
                    .manifest_encoded_checksum
                    .expect("complete binding has manifest checksum"),
                encoded_sha256: self
                    .manifest_encoded_sha256
                    .expect("complete binding has manifest SHA-256"),
            },
        }))
    }
}

pub fn artifact_metadata_presence_consistent(
    encoded_len: Option<u64>,
    encoded_checksum: Option<u64>,
    encoded_sha256: Option<Sha256Digest>,
) -> bool {
    let present = encoded_len.is_some();
    encoded_checksum.is_some() == present && encoded_sha256.is_some() == present
}

impl Default for DurableManifest {
    fn default() -> Self {
        Self::initial_generation()
    }
}

impl DurableManifest {
    pub const fn initial_generation() -> Self {
        Self {
            checkpoint_generation: None,
            checkpoint_encoded_len: None,
            checkpoint_encoded_checksum: None,
            checkpoint_encoded_sha256: None,
            canonical_manifest_encoded_len: None,
            canonical_manifest_encoded_checksum: None,
            canonical_manifest_encoded_sha256: None,
            canonical_adjacency_generation_artifacts: None,
            property_spill_manifest_encoded_len: None,
            property_spill_manifest_encoded_checksum: None,
            property_spill_manifest_encoded_sha256: None,
            property_projection_manifest_encoded_len: None,
            property_projection_manifest_encoded_checksum: None,
            property_projection_manifest_encoded_sha256: None,
            relational_row_generation_artifacts: None,
            relational_overflow_generation_artifacts: None,
            relational_index_generation_artifacts: None,
            append_generation_artifacts: None,
            wal_generation: 0,
            checkpoint_epoch: 0,
            checkpoint_commit_epoch: 0,
            oldest_reader_commit_epoch: None,
            safe_reclaim_commit_epoch: 0,
            wal_replay_start_lsn: 1,
            next_lsn: 1,
            source_scan_commit_epoch: None,
            source_scan_descriptor_checksum: None,
        }
    }

    pub fn checkpoint_path(self, root: &Path) -> PathBuf {
        root.join(checkpoint_generation_file(
            self.checkpoint_generation.unwrap_or(self.checkpoint_epoch),
        ))
    }

    pub fn wal_path(self, root: &Path) -> PathBuf {
        root.join(wal_generation_file(self.wal_generation))
    }

    pub fn validate(self) -> Result<()> {
        if self.wal_replay_start_lsn == 0 || self.next_lsn == 0 {
            return Err(SkeinError::Storage(
                "manifest WAL LSN values must be non-zero".to_string(),
            ));
        }
        if self.next_lsn < self.wal_replay_start_lsn {
            return Err(SkeinError::Storage(format!(
                "manifest next LSN {} precedes replay start LSN {}",
                self.next_lsn, self.wal_replay_start_lsn
            )));
        }
        if !artifact_metadata_presence_consistent(
            self.canonical_manifest_encoded_len,
            self.canonical_manifest_encoded_checksum,
            self.canonical_manifest_encoded_sha256,
        ) {
            return Err(SkeinError::Storage(
                "manifest canonical segment metadata is incomplete".to_string(),
            ));
        }
        if let Some(binding) = self.canonical_adjacency_generation_artifacts {
            if binding.generation == 0
                || binding.generation != self.checkpoint_epoch
                || binding.source_commit_epoch != self.checkpoint_commit_epoch
            {
                return Err(SkeinError::Storage(format!(
                    "manifest canonical adjacency generation/epoch {}/{} does not match checkpoint {}/{}",
                    binding.generation,
                    binding.source_commit_epoch,
                    self.checkpoint_epoch,
                    self.checkpoint_commit_epoch
                )));
            }
            if binding.adjacency_artifact.encoded_len == 0
                || binding.descriptor_root_artifact.encoded_len == 0
            {
                return Err(SkeinError::Storage(
                    "manifest canonical adjacency artifacts must not be empty".to_string(),
                ));
            }
            if binding.entry_count
                != binding.relationship_count.checked_mul(2).ok_or_else(|| {
                    SkeinError::Storage(
                        "manifest canonical adjacency relationship count overflow".to_string(),
                    )
                })?
            {
                return Err(SkeinError::Storage(
                    "manifest canonical adjacency entry count must be twice its relationship count"
                        .to_string(),
                ));
            }
        }
        if self.canonical_adjacency_generation_artifacts.is_some()
            && self.canonical_manifest_encoded_len.is_none()
        {
            return Err(SkeinError::Storage(
                "manifest canonical adjacency requires canonical segments".to_string(),
            ));
        }
        if self.canonical_manifest_encoded_len.is_some()
            && self.canonical_adjacency_generation_artifacts.is_none()
        {
            return Err(SkeinError::Storage(
                "manifest canonical segments require canonical adjacency".to_string(),
            ));
        }
        if !artifact_metadata_presence_consistent(
            self.property_spill_manifest_encoded_len,
            self.property_spill_manifest_encoded_checksum,
            self.property_spill_manifest_encoded_sha256,
        ) {
            return Err(SkeinError::Storage(
                "manifest property spill metadata is incomplete".to_string(),
            ));
        }
        if self.property_spill_manifest_encoded_len.is_some()
            && self.canonical_manifest_encoded_len.is_none()
        {
            return Err(SkeinError::Storage(
                "manifest property spills require canonical segments".to_string(),
            ));
        }
        if !artifact_metadata_presence_consistent(
            self.property_projection_manifest_encoded_len,
            self.property_projection_manifest_encoded_checksum,
            self.property_projection_manifest_encoded_sha256,
        ) {
            return Err(SkeinError::Storage(
                "manifest property projection metadata is incomplete".to_string(),
            ));
        }
        if self.property_projection_manifest_encoded_len.is_some()
            && self.canonical_manifest_encoded_len.is_none()
        {
            return Err(SkeinError::Storage(
                "manifest property projections require canonical segments".to_string(),
            ));
        }
        for (artifact, binding) in [
            (
                "relational row-page",
                self.relational_row_generation_artifacts.map(|binding| {
                    (
                        binding.generation,
                        binding.source_commit_epoch,
                        binding.manifest_artifact.encoded_len,
                    )
                }),
            ),
            (
                "relational overflow",
                self.relational_overflow_generation_artifacts
                    .map(|binding| {
                        (
                            binding.generation,
                            binding.source_commit_epoch,
                            binding.manifest_artifact.encoded_len,
                        )
                    }),
            ),
            (
                "append",
                self.append_generation_artifacts.map(|binding| {
                    (
                        binding.generation,
                        binding.source_commit_epoch,
                        binding.manifest_artifact.encoded_len,
                    )
                }),
            ),
        ] {
            if let Some((generation, source_commit_epoch, manifest_bytes)) = binding {
                if generation == 0 {
                    return Err(SkeinError::Storage(format!(
                        "manifest {artifact} generation must be non-zero"
                    )));
                }
                if generation != self.checkpoint_epoch
                    || source_commit_epoch != self.checkpoint_commit_epoch
                {
                    return Err(SkeinError::Storage(format!(
                        "manifest {artifact} generation/epoch {generation}/{source_commit_epoch} does not match checkpoint {}/{}",
                        self.checkpoint_epoch, self.checkpoint_commit_epoch
                    )));
                }
                if manifest_bytes == 0 {
                    return Err(SkeinError::Storage(format!(
                        "manifest {artifact} generation manifest must not be empty"
                    )));
                }
            }
        }
        if let Some(binding) = self.relational_index_generation_artifacts {
            if binding.generation == 0 {
                return Err(SkeinError::Storage(
                    "manifest relational index generation must be non-zero".to_string(),
                ));
            }
            if binding.generation != self.checkpoint_epoch
                || binding.source_commit_epoch != self.checkpoint_commit_epoch
            {
                return Err(SkeinError::Storage(format!(
                    "manifest relational index generation/epoch {}/{} does not match checkpoint {}/{}",
                    binding.generation,
                    binding.source_commit_epoch,
                    self.checkpoint_epoch,
                    self.checkpoint_commit_epoch,
                )));
            }
            if binding.manifest_artifact.encoded_len == 0 {
                return Err(SkeinError::Storage(
                    "manifest relational index generation manifest must not be empty".to_string(),
                ));
            }
        }
        if self.wal_generation != self.checkpoint_epoch {
            return Err(SkeinError::Storage(format!(
                "manifest WAL generation {} does not match checkpoint epoch {}",
                self.wal_generation, self.checkpoint_epoch
            )));
        }
        match self.checkpoint_generation {
            Some(generation) => {
                if generation != self.checkpoint_epoch {
                    return Err(SkeinError::Storage(format!(
                            "manifest checkpoint generation {generation} does not match checkpoint epoch {}",
                            self.checkpoint_epoch
                        )));
                }
                if self.checkpoint_encoded_len.is_none()
                    || self.checkpoint_encoded_checksum.is_none()
                    || self.checkpoint_encoded_sha256.is_none()
                {
                    return Err(SkeinError::Storage(
                        "manifest checkpoint artifact metadata is incomplete".to_string(),
                    ));
                }
                if self.relational_row_generation_artifacts.is_none()
                    || self.relational_overflow_generation_artifacts.is_none()
                {
                    return Err(SkeinError::Storage(
                        "published checkpoint must bind relational row-page and overflow generations"
                            .to_string(),
                    ));
                }
            }
            None => {
                if self.checkpoint_epoch != 0
                    || self.checkpoint_commit_epoch != 0
                    || self.checkpoint_encoded_len.is_some()
                    || self.checkpoint_encoded_checksum.is_some()
                    || self.checkpoint_encoded_sha256.is_some()
                    || self.canonical_manifest_encoded_len.is_some()
                    || self.canonical_manifest_encoded_checksum.is_some()
                    || self.canonical_manifest_encoded_sha256.is_some()
                    || self.canonical_adjacency_generation_artifacts.is_some()
                    || self.property_spill_manifest_encoded_len.is_some()
                    || self.property_spill_manifest_encoded_checksum.is_some()
                    || self.property_spill_manifest_encoded_sha256.is_some()
                    || self.property_projection_manifest_encoded_len.is_some()
                    || self.property_projection_manifest_encoded_checksum.is_some()
                    || self.property_projection_manifest_encoded_sha256.is_some()
                    || self.relational_row_generation_artifacts.is_some()
                    || self.relational_overflow_generation_artifacts.is_some()
                    || self.relational_index_generation_artifacts.is_some()
                    || self.append_generation_artifacts.is_some()
                {
                    return Err(SkeinError::Storage(
                        "manifest without a checkpoint must describe generation zero".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        Self::decode(&fs::read_to_string(path)?)
    }

    fn decode(text: &str) -> Result<Self> {
        let (body, checksum) = split_manifest_checksum(text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "manifest checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        let mut lines = body.lines();
        if lines.next() != Some(MANIFEST_HEADER_V1) {
            return Err(SkeinError::Storage(
                "manifest is missing the V1 format header".to_string(),
            ));
        }
        let mut manifest = Self::initial_generation();
        let mut canonical_adjacency = CanonicalAdjacencyGenerationFields::default();
        let mut relational_row = RelationalRootManifestFields::default();
        let mut relational_overflow = RelationalRootManifestFields::default();
        let mut append = RelationalRootManifestFields::default();
        let mut relational_index = RelationalIndexManifestFields::default();
        let mut seen_fields = BTreeSet::new();
        for line in lines {
            let fields = line.split('\t').collect::<Vec<_>>();
            if fields == [""] {
                continue;
            }
            let field = fields[0];
            if !seen_fields.insert(field) {
                return Err(SkeinError::Storage(format!(
                    "manifest has duplicate field: {field}"
                )));
            }
            match fields.as_slice() {
                ["version", version] => validate_storage_version(version)?,
                ["checkpoint_generation", raw] => {
                    manifest.checkpoint_generation =
                        parse_optional_u64(raw, "checkpoint generation")?;
                }
                ["checkpoint_encoded_len", raw] => {
                    manifest.checkpoint_encoded_len =
                        parse_optional_u64(raw, "checkpoint encoded length")?;
                }
                ["checkpoint_encoded_checksum", raw] => {
                    manifest.checkpoint_encoded_checksum =
                        parse_optional_u64(raw, "checkpoint encoded checksum")?;
                }
                ["checkpoint_encoded_sha256", raw] => {
                    manifest.checkpoint_encoded_sha256 =
                        parse_optional_sha256(raw, "checkpoint encoded SHA-256")?;
                }
                ["canonical_manifest_encoded_len", raw] => {
                    manifest.canonical_manifest_encoded_len =
                        parse_optional_u64(raw, "canonical manifest encoded length")?;
                }
                ["canonical_manifest_encoded_checksum", raw] => {
                    manifest.canonical_manifest_encoded_checksum =
                        parse_optional_u64(raw, "canonical manifest encoded checksum")?;
                }
                ["canonical_manifest_encoded_sha256", raw] => {
                    manifest.canonical_manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "canonical manifest encoded SHA-256")?;
                }
                ["canonical_adjacency_generation", raw] => {
                    canonical_adjacency.generation =
                        parse_optional_u64(raw, "canonical adjacency generation")?;
                }
                ["canonical_adjacency_source_commit_epoch", raw] => {
                    canonical_adjacency.source_commit_epoch =
                        parse_optional_u64(raw, "canonical adjacency source commit epoch")?;
                }
                ["canonical_adjacency_relationship_count", raw] => {
                    canonical_adjacency.relationship_count =
                        parse_optional_u64(raw, "canonical adjacency relationship count")?;
                }
                ["canonical_adjacency_entry_count", raw] => {
                    canonical_adjacency.entry_count =
                        parse_optional_u64(raw, "canonical adjacency entry count")?;
                }
                ["canonical_adjacency_artifact_encoded_len", raw] => {
                    canonical_adjacency.artifact_encoded_len =
                        parse_optional_u64(raw, "canonical adjacency artifact encoded length")?;
                }
                ["canonical_adjacency_artifact_encoded_crc32c", raw] => {
                    canonical_adjacency.artifact_encoded_crc32c =
                        parse_optional_u64(raw, "canonical adjacency artifact CRC32C")?;
                }
                ["canonical_adjacency_artifact_encoded_sha256", raw] => {
                    canonical_adjacency.artifact_encoded_sha256 =
                        parse_optional_sha256(raw, "canonical adjacency artifact SHA-256")?;
                }
                ["canonical_adjacency_descriptor_root_encoded_len", raw] => {
                    canonical_adjacency.descriptor_root_encoded_len = parse_optional_u64(
                        raw,
                        "canonical adjacency descriptor root encoded length",
                    )?;
                }
                ["canonical_adjacency_descriptor_root_encoded_crc32c", raw] => {
                    canonical_adjacency.descriptor_root_encoded_crc32c =
                        parse_optional_u64(raw, "canonical adjacency descriptor root CRC32C")?;
                }
                ["canonical_adjacency_descriptor_root_encoded_sha256", raw] => {
                    canonical_adjacency.descriptor_root_encoded_sha256 =
                        parse_optional_sha256(raw, "canonical adjacency descriptor root SHA-256")?;
                }
                ["property_spill_manifest_encoded_len", raw] => {
                    manifest.property_spill_manifest_encoded_len =
                        parse_optional_u64(raw, "property spill manifest encoded length")?;
                }
                ["property_spill_manifest_encoded_checksum", raw] => {
                    manifest.property_spill_manifest_encoded_checksum =
                        parse_optional_u64(raw, "property spill manifest encoded checksum")?;
                }
                ["property_spill_manifest_encoded_sha256", raw] => {
                    manifest.property_spill_manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "property spill manifest encoded SHA-256")?;
                }
                ["property_projection_manifest_encoded_len", raw] => {
                    manifest.property_projection_manifest_encoded_len =
                        parse_optional_u64(raw, "property projection manifest encoded length")?;
                }
                ["property_projection_manifest_encoded_checksum", raw] => {
                    manifest.property_projection_manifest_encoded_checksum =
                        parse_optional_u64(raw, "property projection manifest encoded checksum")?;
                }
                ["property_projection_manifest_encoded_sha256", raw] => {
                    manifest.property_projection_manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "property projection manifest encoded SHA-256")?;
                }
                ["relational_row_generation", raw] => {
                    relational_row.generation =
                        parse_optional_u64(raw, "relational row-page generation")?;
                }
                ["relational_row_source_commit_epoch", raw] => {
                    relational_row.source_commit_epoch =
                        parse_optional_u64(raw, "relational row-page source commit epoch")?;
                }
                ["relational_row_root_set_sha256", raw] => {
                    relational_row.root_set_digest =
                        parse_optional_sha256(raw, "relational row-page root-set SHA-256")?;
                }
                ["relational_row_manifest_encoded_len", raw] => {
                    relational_row.manifest_encoded_len =
                        parse_optional_u64(raw, "relational row-page manifest encoded length")?;
                }
                ["relational_row_manifest_encoded_checksum", raw] => {
                    relational_row.manifest_encoded_checksum =
                        parse_optional_u64(raw, "relational row-page manifest encoded checksum")?;
                }
                ["relational_row_manifest_encoded_sha256", raw] => {
                    relational_row.manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "relational row-page manifest encoded SHA-256")?;
                }
                ["relational_overflow_generation", raw] => {
                    relational_overflow.generation =
                        parse_optional_u64(raw, "relational overflow generation")?;
                }
                ["relational_overflow_source_commit_epoch", raw] => {
                    relational_overflow.source_commit_epoch =
                        parse_optional_u64(raw, "relational overflow source commit epoch")?;
                }
                ["relational_overflow_root_set_sha256", raw] => {
                    relational_overflow.root_set_digest =
                        parse_optional_sha256(raw, "relational overflow root-set SHA-256")?;
                }
                ["relational_overflow_manifest_encoded_len", raw] => {
                    relational_overflow.manifest_encoded_len =
                        parse_optional_u64(raw, "relational overflow manifest encoded length")?;
                }
                ["relational_overflow_manifest_encoded_checksum", raw] => {
                    relational_overflow.manifest_encoded_checksum =
                        parse_optional_u64(raw, "relational overflow manifest encoded checksum")?;
                }
                ["relational_overflow_manifest_encoded_sha256", raw] => {
                    relational_overflow.manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "relational overflow manifest encoded SHA-256")?;
                }
                ["append_generation", raw] => {
                    append.generation = parse_optional_u64(raw, "append generation")?;
                }
                ["append_source_commit_epoch", raw] => {
                    append.source_commit_epoch =
                        parse_optional_u64(raw, "append source commit epoch")?;
                }
                ["append_root_set_sha256", raw] => {
                    append.root_set_digest = parse_optional_sha256(raw, "append root-set SHA-256")?;
                }
                ["append_manifest_encoded_len", raw] => {
                    append.manifest_encoded_len =
                        parse_optional_u64(raw, "append manifest encoded length")?;
                }
                ["append_manifest_encoded_checksum", raw] => {
                    append.manifest_encoded_checksum =
                        parse_optional_u64(raw, "append manifest encoded checksum")?;
                }
                ["append_manifest_encoded_sha256", raw] => {
                    append.manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "append manifest encoded SHA-256")?;
                }
                ["relational_index_generation", raw] => {
                    relational_index.generation =
                        parse_optional_u64(raw, "relational index generation")?;
                }
                ["relational_index_source_commit_epoch", raw] => {
                    relational_index.source_commit_epoch =
                        parse_optional_u64(raw, "relational index source commit epoch")?;
                }
                ["relational_index_catalog_schema_sha256", raw] => {
                    relational_index.catalog_schema_digest =
                        parse_optional_sha256(raw, "relational index catalog schema SHA-256")?;
                }
                ["relational_index_root_set_sha256", raw] => {
                    relational_index.root_set_digest =
                        parse_optional_sha256(raw, "relational index root-set SHA-256")?;
                }
                ["relational_index_page_encoded_len", raw] => {
                    relational_index.page_encoded_len =
                        parse_optional_u64(raw, "relational index page encoded length")?;
                }
                ["relational_index_page_encoded_checksum", raw] => {
                    relational_index.page_encoded_checksum =
                        parse_optional_u64(raw, "relational index page encoded checksum")?;
                }
                ["relational_index_page_encoded_sha256", raw] => {
                    relational_index.page_encoded_sha256 =
                        parse_optional_sha256(raw, "relational index page encoded SHA-256")?;
                }
                ["relational_index_manifest_encoded_len", raw] => {
                    relational_index.manifest_encoded_len =
                        parse_optional_u64(raw, "relational index manifest encoded length")?;
                }
                ["relational_index_manifest_encoded_checksum", raw] => {
                    relational_index.manifest_encoded_checksum =
                        parse_optional_u64(raw, "relational index manifest encoded checksum")?;
                }
                ["relational_index_manifest_encoded_sha256", raw] => {
                    relational_index.manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "relational index manifest encoded SHA-256")?;
                }
                ["wal_generation", raw] => {
                    manifest.wal_generation = parse_u64(raw, "WAL generation")?;
                }
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
                ["source_scan_commit_epoch", raw] => {
                    manifest.source_scan_commit_epoch =
                        parse_optional_u64(raw, "source scan commit epoch")?;
                }
                ["source_scan_descriptor_checksum", raw] => {
                    manifest.source_scan_descriptor_checksum =
                        parse_optional_u64(raw, "source scan descriptor checksum")?;
                }
                _ => {
                    return Err(SkeinError::Storage(format!(
                        "invalid manifest line: {line}"
                    )));
                }
            }
        }
        for required in [
            "version",
            "checkpoint_generation",
            "checkpoint_encoded_len",
            "checkpoint_encoded_checksum",
            "checkpoint_encoded_sha256",
            "canonical_manifest_encoded_len",
            "canonical_manifest_encoded_checksum",
            "canonical_manifest_encoded_sha256",
            "canonical_adjacency_generation",
            "canonical_adjacency_source_commit_epoch",
            "canonical_adjacency_relationship_count",
            "canonical_adjacency_entry_count",
            "canonical_adjacency_artifact_encoded_len",
            "canonical_adjacency_artifact_encoded_crc32c",
            "canonical_adjacency_artifact_encoded_sha256",
            "canonical_adjacency_descriptor_root_encoded_len",
            "canonical_adjacency_descriptor_root_encoded_crc32c",
            "canonical_adjacency_descriptor_root_encoded_sha256",
            "property_spill_manifest_encoded_len",
            "property_spill_manifest_encoded_checksum",
            "property_spill_manifest_encoded_sha256",
            "property_projection_manifest_encoded_len",
            "property_projection_manifest_encoded_checksum",
            "property_projection_manifest_encoded_sha256",
            "relational_row_generation",
            "relational_row_source_commit_epoch",
            "relational_row_root_set_sha256",
            "relational_row_manifest_encoded_len",
            "relational_row_manifest_encoded_checksum",
            "relational_row_manifest_encoded_sha256",
            "relational_overflow_generation",
            "relational_overflow_source_commit_epoch",
            "relational_overflow_root_set_sha256",
            "relational_overflow_manifest_encoded_len",
            "relational_overflow_manifest_encoded_checksum",
            "relational_overflow_manifest_encoded_sha256",
            "relational_index_generation",
            "relational_index_source_commit_epoch",
            "relational_index_catalog_schema_sha256",
            "relational_index_root_set_sha256",
            "relational_index_page_encoded_len",
            "relational_index_page_encoded_checksum",
            "relational_index_page_encoded_sha256",
            "relational_index_manifest_encoded_len",
            "relational_index_manifest_encoded_checksum",
            "relational_index_manifest_encoded_sha256",
            "wal_generation",
            "checkpoint_epoch",
            "checkpoint_commit_epoch",
            "oldest_reader_commit_epoch",
            "safe_reclaim_commit_epoch",
            "wal_replay_start_lsn",
            "next_lsn",
            "source_scan_commit_epoch",
            "source_scan_descriptor_checksum",
        ] {
            if !seen_fields.contains(required) {
                return Err(SkeinError::Storage(format!(
                    "manifest is missing required field: {required}"
                )));
            }
        }
        manifest.relational_row_generation_artifacts = relational_row.finish_row()?;
        manifest.canonical_adjacency_generation_artifacts = canonical_adjacency.finish()?;
        manifest.relational_overflow_generation_artifacts =
            relational_overflow.finish_overflow()?;
        manifest.relational_index_generation_artifacts = relational_index.finish()?;
        manifest.append_generation_artifacts = append.finish_append()?;
        if manifest.safe_reclaim_commit_epoch == 0 && manifest.checkpoint_commit_epoch > 0 {
            manifest.safe_reclaim_commit_epoch = safe_reclaim_commit_epoch(
                manifest.checkpoint_commit_epoch,
                manifest.oldest_reader_commit_epoch,
            );
        }
        manifest.validate()?;
        Ok(manifest)
    }

    fn encode(&self) -> String {
        let mut body = String::new();
        body.push_str(&format!("{MANIFEST_HEADER_V1}\n"));
        body.push_str(&format!("version\t{STORAGE_VERSION}\n"));
        body.push_str(&format!(
            "checkpoint_generation\t{}\n",
            encode_optional_u64(self.checkpoint_generation)
        ));
        body.push_str(&format!(
            "checkpoint_encoded_len\t{}\n",
            encode_optional_u64(self.checkpoint_encoded_len)
        ));
        body.push_str(&format!(
            "checkpoint_encoded_checksum\t{}\n",
            encode_optional_u64(self.checkpoint_encoded_checksum)
        ));
        body.push_str(&format!(
            "checkpoint_encoded_sha256\t{}\n",
            encode_optional_sha256(self.checkpoint_encoded_sha256)
        ));
        body.push_str(&format!(
            "canonical_manifest_encoded_len\t{}\n",
            encode_optional_u64(self.canonical_manifest_encoded_len)
        ));
        body.push_str(&format!(
            "canonical_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(self.canonical_manifest_encoded_checksum)
        ));
        body.push_str(&format!(
            "canonical_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(self.canonical_manifest_encoded_sha256)
        ));
        let canonical_adjacency = self.canonical_adjacency_generation_artifacts;
        body.push_str(&format!(
            "canonical_adjacency_generation\t{}\n",
            encode_optional_u64(canonical_adjacency.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "canonical_adjacency_source_commit_epoch\t{}\n",
            encode_optional_u64(canonical_adjacency.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "canonical_adjacency_relationship_count\t{}\n",
            encode_optional_u64(canonical_adjacency.map(|binding| binding.relationship_count))
        ));
        body.push_str(&format!(
            "canonical_adjacency_entry_count\t{}\n",
            encode_optional_u64(canonical_adjacency.map(|binding| binding.entry_count))
        ));
        body.push_str(&format!(
            "canonical_adjacency_artifact_encoded_len\t{}\n",
            encode_optional_u64(
                canonical_adjacency.map(|binding| binding.adjacency_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_artifact_encoded_crc32c\t{}\n",
            encode_optional_u64(
                canonical_adjacency.map(|binding| binding.adjacency_artifact.encoded_crc32c)
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_artifact_encoded_sha256\t{}\n",
            encode_optional_sha256(
                canonical_adjacency.map(|binding| binding.adjacency_artifact.encoded_sha256)
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_descriptor_root_encoded_len\t{}\n",
            encode_optional_u64(
                canonical_adjacency.map(|binding| binding.descriptor_root_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_descriptor_root_encoded_crc32c\t{}\n",
            encode_optional_u64(
                canonical_adjacency
                    .map(|binding| { u64::from(binding.descriptor_root_artifact.encoded_crc32c) })
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_descriptor_root_encoded_sha256\t{}\n",
            encode_optional_sha256(
                canonical_adjacency.map(|binding| binding.descriptor_root_artifact.encoded_sha256)
            )
        ));
        body.push_str(&format!(
            "property_spill_manifest_encoded_len\t{}\n",
            encode_optional_u64(self.property_spill_manifest_encoded_len)
        ));
        body.push_str(&format!(
            "property_spill_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(self.property_spill_manifest_encoded_checksum)
        ));
        body.push_str(&format!(
            "property_spill_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(self.property_spill_manifest_encoded_sha256)
        ));
        body.push_str(&format!(
            "property_projection_manifest_encoded_len\t{}\n",
            encode_optional_u64(self.property_projection_manifest_encoded_len)
        ));
        body.push_str(&format!(
            "property_projection_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(self.property_projection_manifest_encoded_checksum)
        ));
        body.push_str(&format!(
            "property_projection_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(self.property_projection_manifest_encoded_sha256)
        ));
        let relational_row = self.relational_row_generation_artifacts;
        body.push_str(&format!(
            "relational_row_generation\t{}\n",
            encode_optional_u64(relational_row.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "relational_row_source_commit_epoch\t{}\n",
            encode_optional_u64(relational_row.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "relational_row_root_set_sha256\t{}\n",
            encode_optional_sha256(relational_row.map(|binding| binding.root_set_digest))
        ));
        body.push_str(&format!(
            "relational_row_manifest_encoded_len\t{}\n",
            encode_optional_u64(
                relational_row.map(|binding| binding.manifest_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "relational_row_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(
                relational_row.map(|binding| u64::from(binding.manifest_artifact.encoded_crc32c))
            )
        ));
        body.push_str(&format!(
            "relational_row_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(
                relational_row.map(|binding| binding.manifest_artifact.encoded_sha256)
            )
        ));
        let relational_overflow = self.relational_overflow_generation_artifacts;
        body.push_str(&format!(
            "relational_overflow_generation\t{}\n",
            encode_optional_u64(relational_overflow.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "relational_overflow_source_commit_epoch\t{}\n",
            encode_optional_u64(relational_overflow.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "relational_overflow_root_set_sha256\t{}\n",
            encode_optional_sha256(relational_overflow.map(|binding| binding.root_set_digest))
        ));
        body.push_str(&format!(
            "relational_overflow_manifest_encoded_len\t{}\n",
            encode_optional_u64(
                relational_overflow.map(|binding| binding.manifest_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "relational_overflow_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(
                relational_overflow
                    .map(|binding| u64::from(binding.manifest_artifact.encoded_crc32c))
            )
        ));
        body.push_str(&format!(
            "relational_overflow_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(
                relational_overflow.map(|binding| binding.manifest_artifact.encoded_sha256)
            )
        ));
        let append = self.append_generation_artifacts;
        body.push_str(&format!(
            "append_generation\t{}\n",
            encode_optional_u64(append.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "append_source_commit_epoch\t{}\n",
            encode_optional_u64(append.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "append_root_set_sha256\t{}\n",
            encode_optional_sha256(append.map(|binding| binding.root_set_digest))
        ));
        body.push_str(&format!(
            "append_manifest_encoded_len\t{}\n",
            encode_optional_u64(append.map(|binding| binding.manifest_artifact.encoded_len))
        ));
        body.push_str(&format!(
            "append_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(
                append.map(|binding| u64::from(binding.manifest_artifact.encoded_crc32c))
            )
        ));
        body.push_str(&format!(
            "append_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(append.map(|binding| binding.manifest_artifact.encoded_sha256))
        ));
        let relational_index = self.relational_index_generation_artifacts;
        body.push_str(&format!(
            "relational_index_generation\t{}\n",
            encode_optional_u64(relational_index.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "relational_index_source_commit_epoch\t{}\n",
            encode_optional_u64(relational_index.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "relational_index_catalog_schema_sha256\t{}\n",
            encode_optional_sha256(relational_index.map(|binding| binding.catalog_schema_digest))
        ));
        body.push_str(&format!(
            "relational_index_root_set_sha256\t{}\n",
            encode_optional_sha256(relational_index.map(|binding| binding.root_set_digest))
        ));
        body.push_str(&format!(
            "relational_index_page_encoded_len\t{}\n",
            encode_optional_u64(relational_index.map(|binding| binding.page_artifact.encoded_len))
        ));
        body.push_str(&format!(
            "relational_index_page_encoded_checksum\t{}\n",
            encode_optional_u64(
                relational_index.map(|binding| binding.page_artifact.encoded_crc32c)
            )
        ));
        body.push_str(&format!(
            "relational_index_page_encoded_sha256\t{}\n",
            encode_optional_sha256(
                relational_index.map(|binding| binding.page_artifact.encoded_sha256)
            )
        ));
        body.push_str(&format!(
            "relational_index_manifest_encoded_len\t{}\n",
            encode_optional_u64(
                relational_index.map(|binding| binding.manifest_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "relational_index_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(
                relational_index.map(|binding| binding.manifest_artifact.encoded_crc32c)
            )
        ));
        body.push_str(&format!(
            "relational_index_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(
                relational_index.map(|binding| binding.manifest_artifact.encoded_sha256)
            )
        ));
        body.push_str(&format!("wal_generation\t{}\n", self.wal_generation));
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
        body.push_str(&format!(
            "source_scan_commit_epoch\t{}\n",
            encode_optional_u64(self.source_scan_commit_epoch)
        ));
        body.push_str(&format!(
            "source_scan_descriptor_checksum\t{}\n",
            encode_optional_u64(self.source_scan_descriptor_checksum)
        ));
        let checksum = checksum_bytes(body.as_bytes());
        format!("{body}checksum\t{checksum}\n")
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        let data = self.encode();
        let tmp_path = path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(data.as_bytes())?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, path)?;
        Ok(())
    }
}

pub const STORAGE_VERSION: &str = "skein-storage-v1";
const MANIFEST_HEADER_V1: &str = "SKEIN_MANIFEST_V1";

pub fn safe_reclaim_commit_epoch(
    checkpoint_commit_epoch: u64,
    oldest_reader_commit_epoch: Option<u64>,
) -> u64 {
    oldest_reader_commit_epoch
        .map(|epoch| epoch.saturating_sub(1))
        .unwrap_or(checkpoint_commit_epoch)
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

fn encode_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn encode_optional_sha256(value: Option<Sha256Digest>) -> String {
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

fn parse_optional_sha256(input: &str, name: &str) -> Result<Option<Sha256Digest>> {
    if input == "none" {
        Ok(None)
    } else {
        input
            .parse()
            .map(Some)
            .map_err(|error| SkeinError::Storage(format!("invalid {name}: {error}")))
    }
}

pub fn validate_storage_version(version: &str) -> Result<()> {
    if version == STORAGE_VERSION {
        return Ok(());
    }
    Err(SkeinError::Storage(format!(
        "unsupported storage version: {version}; expected {STORAGE_VERSION}"
    )))
}
