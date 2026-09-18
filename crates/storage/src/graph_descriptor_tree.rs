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

//! Bounded construction and publish-last roots for graph descriptor pages.
//!
//! The tree builder spills page references between levels. Its resident state
//! is therefore bounded by one descriptor page and one interior-page group,
//! rather than by the number of descriptors. Publication is deliberately
//! separate from query activation: the page artifact becomes durable first,
//! and the small selecting root is replaced last.

use crate::durable_replace_file;
use crate::graph_descriptor_page::{
    decode_page_ref, encode_page_ref, GraphDescriptorKind, GraphDescriptorPageError,
    GraphDescriptorPageLimits, GraphDescriptorPageRef,
};
use hawdb_integrity::{integrity_digest, Crc32c, IntegrityHasher, Sha256Digest};
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const ROOT_MAGIC: &[u8; 8] = b"SKGDROOT";
const ROOT_VERSION: u16 = 1;
const ROOT_PREFIX_BYTES: usize = 116;
const ROOT_HEADER_BYTES: usize = 152;
const ROOT_INTEGRITY_OFFSET: usize = 116;
const DEFAULT_MAX_ROOT_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_PAGE_COUNT: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_PAGE_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_MAX_INTERMEDIATE_BYTES: u64 = 64 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphDescriptorTreeBuildConfig {
    pub page_limits: GraphDescriptorPageLimits,
    pub max_root_bytes: NonZeroUsize,
    pub max_page_count: NonZeroU64,
    pub max_page_artifact_bytes: NonZeroU64,
    pub max_intermediate_bytes: NonZeroU64,
}

impl Default for GraphDescriptorTreeBuildConfig {
    fn default() -> Self {
        Self {
            page_limits: GraphDescriptorPageLimits::default(),
            max_root_bytes: NonZeroUsize::new(DEFAULT_MAX_ROOT_BYTES)
                .expect("default graph descriptor root limit is non-zero"),
            max_page_count: NonZeroU64::new(DEFAULT_MAX_PAGE_COUNT)
                .expect("default graph descriptor page count limit is non-zero"),
            max_page_artifact_bytes: NonZeroU64::new(DEFAULT_MAX_PAGE_ARTIFACT_BYTES)
                .expect("default graph descriptor artifact byte limit is non-zero"),
            max_intermediate_bytes: NonZeroU64::new(DEFAULT_MAX_INTERMEDIATE_BYTES)
                .expect("default graph descriptor spill limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDescriptorTreePaths {
    pub page_artifact: PathBuf,
    pub root_manifest: PathBuf,
}

impl GraphDescriptorTreePaths {
    pub fn new(page_artifact: impl Into<PathBuf>, root_manifest: impl Into<PathBuf>) -> Self {
        Self {
            page_artifact: page_artifact.into(),
            root_manifest: root_manifest.into(),
        }
    }

    fn page_tmp(&self) -> PathBuf {
        self.page_artifact.with_extension("hawdb.tmp")
    }

    fn root_tmp(&self) -> PathBuf {
        self.root_manifest.with_extension("hawdb.tmp")
    }

    fn ref_run(&self, level: u32) -> PathBuf {
        let file_name = self
            .page_artifact
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("graph-descriptors.pages.hawdb");
        self.page_artifact
            .with_file_name(format!(".{file_name}.refs.{level}.tmp"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDescriptorTreeRoot {
    pub kind: GraphDescriptorKind,
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub page_artifact_id: u64,
    pub page_artifact_len: u64,
    pub page_artifact_crc32c: Crc32c,
    pub page_artifact_sha256: Sha256Digest,
    pub descriptor_count: u64,
    pub page_count: u64,
    pub leaf_page_count: u64,
    pub height: u32,
    pub root: Option<GraphDescriptorPageRef>,
}

impl GraphDescriptorTreeRoot {
    pub fn encode(
        &self,
        config: GraphDescriptorTreeBuildConfig,
    ) -> Result<Vec<u8>, GraphDescriptorTreeError> {
        validate_root(self, config, ErrorClass::Admission)?;
        let encoded_ref = self
            .root
            .as_ref()
            .map(encode_page_ref)
            .transpose()
            .map_err(GraphDescriptorTreeError::Page)?
            .unwrap_or_default();
        let encoded_len = ROOT_HEADER_BYTES
            .checked_add(encoded_ref.len())
            .ok_or_else(|| admission("graph descriptor root length overflow"))?;
        if encoded_len > config.max_root_bytes.get() {
            return Err(admission(format!(
                "graph descriptor root contains {encoded_len} bytes, exceeding limit {}",
                config.max_root_bytes
            )));
        }
        let ref_len = u32::try_from(encoded_ref.len())
            .map_err(|_| admission("graph descriptor root reference length exceeds u32"))?;
        let mut prefix = Vec::with_capacity(ROOT_PREFIX_BYTES);
        prefix.extend_from_slice(ROOT_MAGIC);
        prefix.extend_from_slice(&ROOT_VERSION.to_le_bytes());
        prefix.extend_from_slice(&0u16.to_le_bytes());
        prefix.push(self.kind.tag());
        prefix.extend_from_slice(&[0u8; 3]);
        prefix.extend_from_slice(&self.generation.to_le_bytes());
        prefix.extend_from_slice(&self.source_commit_epoch.to_le_bytes());
        prefix.extend_from_slice(&self.page_artifact_id.to_le_bytes());
        prefix.extend_from_slice(&self.page_artifact_len.to_le_bytes());
        prefix.extend_from_slice(&self.page_artifact_crc32c.get().to_le_bytes());
        prefix.extend_from_slice(self.page_artifact_sha256.as_bytes());
        prefix.extend_from_slice(&self.descriptor_count.to_le_bytes());
        prefix.extend_from_slice(&self.page_count.to_le_bytes());
        prefix.extend_from_slice(&self.leaf_page_count.to_le_bytes());
        prefix.extend_from_slice(&self.height.to_le_bytes());
        prefix.extend_from_slice(&ref_len.to_le_bytes());
        debug_assert_eq!(prefix.len(), ROOT_PREFIX_BYTES);

        let mut hasher = IntegrityHasher::new();
        hasher.update(&prefix);
        hasher.update(&encoded_ref);
        let digest = hasher.finish();
        let mut encoded = Vec::with_capacity(encoded_len);
        encoded.extend_from_slice(&prefix);
        encoded.extend_from_slice(&digest.crc32c.get().to_le_bytes());
        encoded.extend_from_slice(digest.sha256.as_bytes());
        encoded.extend_from_slice(&encoded_ref);
        debug_assert_eq!(encoded.len(), encoded_len);
        Ok(encoded)
    }

    pub fn decode(
        encoded: &[u8],
        config: GraphDescriptorTreeBuildConfig,
    ) -> Result<Self, GraphDescriptorTreeError> {
        if encoded.len() > config.max_root_bytes.get() {
            return Err(admission(format!(
                "graph descriptor root contains {} bytes, exceeding limit {}",
                encoded.len(),
                config.max_root_bytes
            )));
        }
        if encoded.len() < ROOT_HEADER_BYTES || &encoded[..8] != ROOT_MAGIC {
            return Err(corrupt("invalid graph descriptor root header"));
        }
        let version = read_u16(&encoded[8..10]);
        let flags = read_u16(&encoded[10..12]);
        if version != ROOT_VERSION || flags != 0 || encoded[13..16] != [0u8; 3] {
            return Err(corrupt(format!(
                "unsupported graph descriptor root version {version}, flags {flags}, or reserved fields"
            )));
        }
        let ref_len = read_u32(&encoded[112..116]) as usize;
        let expected_len = ROOT_HEADER_BYTES
            .checked_add(ref_len)
            .ok_or_else(|| corrupt("graph descriptor root length overflow"))?;
        if encoded.len() != expected_len {
            return Err(corrupt(format!(
                "graph descriptor root length mismatch: expected {expected_len}, got {}",
                encoded.len()
            )));
        }
        let encoded_ref = &encoded[ROOT_HEADER_BYTES..];
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded[..ROOT_INTEGRITY_OFFSET]);
        hasher.update(encoded_ref);
        let digest = hasher.finish();
        if digest.crc32c.get() != read_u32(&encoded[116..120])
            || digest.sha256.as_bytes() != &encoded[120..152]
        {
            return Err(corrupt("graph descriptor root checksum mismatch"));
        }
        let root = if encoded_ref.is_empty() {
            None
        } else {
            Some(
                decode_page_ref(encoded_ref, config.page_limits)
                    .map_err(GraphDescriptorTreeError::Page)?,
            )
        };
        let root_manifest = Self {
            kind: GraphDescriptorKind::from_tag(encoded[12])
                .map_err(GraphDescriptorTreeError::Page)?,
            generation: read_u64(&encoded[16..24]),
            source_commit_epoch: read_u64(&encoded[24..32]),
            page_artifact_id: read_u64(&encoded[32..40]),
            page_artifact_len: read_u64(&encoded[40..48]),
            page_artifact_crc32c: Crc32c::new(read_u32(&encoded[48..52])),
            page_artifact_sha256: Sha256Digest::from_bytes(
                encoded[52..84]
                    .try_into()
                    .expect("graph descriptor artifact digest has a fixed length"),
            ),
            descriptor_count: read_u64(&encoded[84..92]),
            page_count: read_u64(&encoded[92..100]),
            leaf_page_count: read_u64(&encoded[100..108]),
            height: read_u32(&encoded[108..112]),
            root,
        };
        validate_root(&root_manifest, config, ErrorClass::Corrupt)?;
        Ok(root_manifest)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphDescriptorTreeBuildReport {
    pub descriptor_count: u64,
    pub page_count: u64,
    pub leaf_page_count: u64,
    pub interior_page_count: u64,
    pub page_artifact_bytes: u64,
    pub root_bytes: u64,
    pub total_intermediate_bytes: u64,
    pub peak_intermediate_level_bytes: u64,
    pub peak_resident_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphDescriptorTreeOpenReport {
    pub root_bytes_read: u64,
    pub page_payload_bytes_read: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphDescriptorTreeArtifactMetadata {
    pub encoded_len: u64,
    pub encoded_crc32c: u32,
    pub encoded_sha256: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphDescriptorTreeGenerationArtifacts {
    pub kind: GraphDescriptorKind,
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub root_artifact: GraphDescriptorTreeArtifactMetadata,
}

#[derive(Debug)]
pub enum GraphDescriptorTreeError {
    Io(std::io::Error),
    Page(GraphDescriptorPageError),
    Admission(String),
    Corrupt(String),
}

impl Display for GraphDescriptorTreeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Page(error) => Display::fmt(error, formatter),
            Self::Admission(message) => {
                write!(
                    formatter,
                    "graph descriptor tree admission failed: {message}"
                )
            }
            Self::Corrupt(message) => write!(formatter, "corrupt graph descriptor tree: {message}"),
        }
    }
}

impl std::error::Error for GraphDescriptorTreeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Page(error) => Some(error),
            Self::Admission(_) | Self::Corrupt(_) => None,
        }
    }
}

impl From<std::io::Error> for GraphDescriptorTreeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<GraphDescriptorPageError> for GraphDescriptorTreeError {
    fn from(error: GraphDescriptorPageError) -> Self {
        Self::Page(error)
    }
}

fn admission(message: impl Into<String>) -> GraphDescriptorTreeError {
    GraphDescriptorTreeError::Admission(message.into())
}

fn corrupt(message: impl Into<String>) -> GraphDescriptorTreeError {
    GraphDescriptorTreeError::Corrupt(message.into())
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

impl ErrorClass {
    fn error(self, message: impl Into<String>) -> GraphDescriptorTreeError {
        match self {
            Self::Admission => admission(message),
            Self::Corrupt => corrupt(message),
        }
    }
}

fn validate_root(
    root: &GraphDescriptorTreeRoot,
    config: GraphDescriptorTreeBuildConfig,
    class: ErrorClass,
) -> Result<(), GraphDescriptorTreeError> {
    if root.generation == 0 || root.page_artifact_id == 0 {
        return Err(
            class.error("graph descriptor root generation and artifact id must be non-zero")
        );
    }
    if root.page_count > config.max_page_count.get() {
        return Err(class.error(format!(
            "graph descriptor root declares {} pages, exceeding limit {}",
            root.page_count, config.max_page_count
        )));
    }
    if root.page_artifact_len > config.max_page_artifact_bytes.get() {
        return Err(class.error(format!(
            "graph descriptor root declares {} artifact bytes, exceeding limit {}",
            root.page_artifact_len, config.max_page_artifact_bytes
        )));
    }
    match &root.root {
        None => {
            let empty_digest = integrity_digest(&[]);
            if root.descriptor_count != 0
                || root.page_count != 0
                || root.leaf_page_count != 0
                || root.height != 0
                || root.page_artifact_len != 0
                || root.page_artifact_crc32c != empty_digest.crc32c
                || root.page_artifact_sha256 != empty_digest.sha256
            {
                return Err(class.error(
                    "empty graph descriptor root has non-empty counts or artifact metadata",
                ));
            }
        }
        Some(reference) => {
            reference
                .validate(config.page_limits)
                .map_err(|error| class.error(error.to_string()))?;
            if root.descriptor_count == 0
                || root.page_count == 0
                || root.leaf_page_count == 0
                || root.leaf_page_count > root.page_count
                || root.page_artifact_len == 0
            {
                return Err(
                    class.error("non-empty graph descriptor root has empty or invalid counts")
                );
            }
            if reference.artifact_id != root.page_artifact_id
                || reference.physical_generation > root.generation
                || reference
                    .offset
                    .checked_add(reference.length.get())
                    .is_none_or(|end| end > root.page_artifact_len)
            {
                return Err(class
                    .error("graph descriptor root reference is outside its bound page artifact"));
            }
            if root.height == 0 && (root.page_count != 1 || root.leaf_page_count != 1) {
                return Err(class.error(
                    "height-zero graph descriptor tree must contain exactly one leaf page",
                ));
            }
            if root.height > 0 && root.page_count == root.leaf_page_count {
                return Err(
                    class.error("non-zero graph descriptor tree height requires interior pages")
                );
            }
            let interior_page_count = root.page_count - root.leaf_page_count;
            if u64::from(root.height) > interior_page_count {
                return Err(class.error(format!(
                    "graph descriptor tree height {} exceeds its {interior_page_count} interior pages",
                    root.height
                )));
            }
        }
    }
    Ok(())
}

mod builder;
pub(crate) mod demand;
pub use builder::GraphDescriptorTreeBuilder;

#[derive(Debug)]
pub struct PreparedGraphDescriptorTree {
    paths: GraphDescriptorTreePaths,
    config: GraphDescriptorTreeBuildConfig,
    root: GraphDescriptorTreeRoot,
    encoded_root: Vec<u8>,
    report: GraphDescriptorTreeBuildReport,
    page_tmp: PathBuf,
    published: bool,
}

impl PreparedGraphDescriptorTree {
    pub fn root(&self) -> &GraphDescriptorTreeRoot {
        &self.root
    }

    pub const fn report(&self) -> GraphDescriptorTreeBuildReport {
        self.report
    }

    pub fn publish(mut self) -> Result<GraphDescriptorTreeWriteOutput, GraphDescriptorTreeError> {
        if self.paths.page_artifact.exists() || self.paths.root_manifest.exists() {
            return Err(admission(
                "graph descriptor publication refuses to replace an existing generation artifact",
            ));
        }
        durable_publish_immutable(&self.page_tmp, &self.paths.page_artifact)?;
        let root_tmp = self.paths.root_tmp();
        remove_if_exists(&root_tmp)?;
        {
            let mut file = File::create(&root_tmp)?;
            file.write_all(&self.encoded_root)?;
            file.sync_all()?;
        }
        if let Err(error) = durable_publish_immutable(&root_tmp, &self.paths.root_manifest) {
            let _ = fs::remove_file(&root_tmp);
            return Err(error);
        }
        self.published = true;
        let root_integrity = integrity_digest(&self.encoded_root);
        Ok(GraphDescriptorTreeWriteOutput {
            root: self.root.clone(),
            report: self.report,
            root_artifact: GraphDescriptorTreeArtifactMetadata {
                encoded_len: self.encoded_root.len() as u64,
                encoded_crc32c: root_integrity.crc32c.get(),
                encoded_sha256: root_integrity.sha256,
            },
        })
    }

    pub fn verify_encoded_root(&self) -> Result<(), GraphDescriptorTreeError> {
        let decoded = GraphDescriptorTreeRoot::decode(&self.encoded_root, self.config)?;
        if decoded != self.root {
            return Err(corrupt(
                "prepared graph descriptor root does not round-trip exactly",
            ));
        }
        Ok(())
    }
}

impl Drop for PreparedGraphDescriptorTree {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.page_tmp);
            let _ = fs::remove_file(self.paths.root_tmp());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDescriptorTreeWriteOutput {
    pub root: GraphDescriptorTreeRoot,
    pub report: GraphDescriptorTreeBuildReport,
    pub root_artifact: GraphDescriptorTreeArtifactMetadata,
}

impl GraphDescriptorTreeWriteOutput {
    pub const fn generation_artifacts(&self) -> GraphDescriptorTreeGenerationArtifacts {
        GraphDescriptorTreeGenerationArtifacts {
            kind: self.root.kind,
            generation: self.root.generation,
            source_commit_epoch: self.root.source_commit_epoch,
            root_artifact: self.root_artifact,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GraphDescriptorTreeRootReader {
    root: Arc<GraphDescriptorTreeRoot>,
    paths: GraphDescriptorTreePaths,
    report: GraphDescriptorTreeOpenReport,
}

impl GraphDescriptorTreeRootReader {
    pub fn open(
        paths: GraphDescriptorTreePaths,
        config: GraphDescriptorTreeBuildConfig,
    ) -> Result<Self, GraphDescriptorTreeError> {
        Self::open_inner(paths, None, config)
    }

    pub fn open_bound(
        paths: GraphDescriptorTreePaths,
        binding: GraphDescriptorTreeGenerationArtifacts,
        config: GraphDescriptorTreeBuildConfig,
    ) -> Result<Self, GraphDescriptorTreeError> {
        Self::open_inner(paths, Some(binding), config)
    }

    fn open_inner(
        paths: GraphDescriptorTreePaths,
        binding: Option<GraphDescriptorTreeGenerationArtifacts>,
        config: GraphDescriptorTreeBuildConfig,
    ) -> Result<Self, GraphDescriptorTreeError> {
        let encoded = read_bounded_root(&paths.root_manifest, config.max_root_bytes)?;
        if let Some(binding) = binding {
            let digest = integrity_digest(&encoded);
            if encoded.len() as u64 != binding.root_artifact.encoded_len
                || digest.crc32c.get() != binding.root_artifact.encoded_crc32c
                || digest.sha256 != binding.root_artifact.encoded_sha256
            {
                return Err(corrupt(
                    "graph descriptor root does not match its canonical artifact binding",
                ));
            }
        }
        let root = GraphDescriptorTreeRoot::decode(&encoded, config)?;
        if let Some(binding) = binding
            && (root.kind != binding.kind
                || root.generation != binding.generation
                || root.source_commit_epoch != binding.source_commit_epoch)
        {
            return Err(corrupt(format!(
                "graph descriptor root identity {:?}/{}/{} does not match canonical binding {:?}/{}/{}",
                root.kind,
                root.generation,
                root.source_commit_epoch,
                binding.kind,
                binding.generation,
                binding.source_commit_epoch
            )));
        }
        let actual_len = File::open(&paths.page_artifact)?.metadata()?.len();
        if actual_len != root.page_artifact_len {
            return Err(corrupt(format!(
                "graph descriptor page artifact length mismatch: expected {}, got {actual_len}",
                root.page_artifact_len
            )));
        }
        Ok(Self {
            root: Arc::new(root),
            paths,
            report: GraphDescriptorTreeOpenReport {
                root_bytes_read: encoded.len() as u64,
                page_payload_bytes_read: 0,
            },
        })
    }

    pub fn root(&self) -> &GraphDescriptorTreeRoot {
        &self.root
    }

    pub const fn report(&self) -> GraphDescriptorTreeOpenReport {
        self.report
    }

    pub fn page_artifact_path(&self) -> &Path {
        &self.paths.page_artifact
    }
}

fn read_bounded_root(
    path: &Path,
    max_root_bytes: NonZeroUsize,
) -> Result<Vec<u8>, GraphDescriptorTreeError> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    if len > max_root_bytes.get() as u64 {
        return Err(admission(format!(
            "graph descriptor root contains {len} bytes, exceeding limit {max_root_bytes}"
        )));
    }
    let read_limit = max_root_bytes
        .get()
        .checked_add(1)
        .ok_or_else(|| admission("graph descriptor root read limit overflow"))?
        as u64;
    let mut encoded = Vec::with_capacity(len as usize);
    file.take(read_limit).read_to_end(&mut encoded)?;
    if encoded.len() > max_root_bytes.get() {
        return Err(admission(format!(
            "graph descriptor root exceeds limit {max_root_bytes}"
        )));
    }
    Ok(encoded)
}

fn remove_if_exists(path: &Path) -> Result<(), GraphDescriptorTreeError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn durable_publish_immutable(
    source: &Path,
    destination: &Path,
) -> Result<(), GraphDescriptorTreeError> {
    if destination.exists() {
        return Err(admission(format!(
            "immutable graph descriptor artifact {} already exists",
            destination.display()
        )));
    }
    durable_replace_file(source, destination).map_err(GraphDescriptorTreeError::Io)
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("u16 field has a fixed length"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("u32 field has a fixed length"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("u64 field has a fixed length"))
}

#[cfg(test)]
mod tests;
