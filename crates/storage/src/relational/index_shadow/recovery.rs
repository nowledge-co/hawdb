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

//! Bounded, non-serving WAL recovery deltas for relational index shadows.
//!
//! Delta artifacts are derived state. The base shadow and canonical WAL remain
//! authoritative until a complete recovery manifest is published last.

use super::super::{
    RelationalIndexChange, RelationalIndexChangeCapture, RelationalIndexChangeCaptureLimits,
    RelationalIndexChangeKind, RelationalIndexRangeScan, RelationalIndexScanDirection,
    RelationalKey, RelationalRecoveryFence, RelationalRecoverySourceIdentity, RelationalState,
    RELATIONAL_RECOVERY_SOURCE_BYTES,
};
use super::{
    decode_bytes, decode_utf8, encode_bytes, encode_relational_key, read_bounded_file, read_u16,
    read_u32, read_u64, take, RelationalIndexGenerationArtifacts,
    RelationalIndexGenerationIdentity, RelationalIndexReadLimits, RelationalIndexReadReport,
    RelationalIndexShadowConfig, RelationalIndexShadowError, RelationalIndexShadowReader,
};
use crate::{
    durable_replace_file, ContentDigest, ManifestGeneration, RepresentationKind, SegmentCache,
    SegmentCacheError, SegmentCacheKey, StoreId,
};
use hawdb_integrity::{IntegrityDigest, IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

const DELTA_MANIFEST_MAGIC: &[u8; 8] = b"SKRIDXR1";
const DELTA_PAGE_MAGIC: &[u8; 8] = b"SKRIDXD1";
const DELTA_MANIFEST_FORMAT_VERSION: u16 = 2;
const DELTA_PAGE_FORMAT_VERSION: u16 = 1;
const DELTA_MANIFEST_HEADER_BYTES: usize = 148;
const DELTA_MANIFEST_INTEGRITY_OFFSET: usize = 112;
const DELTA_PAGE_HEADER_BYTES: usize = 104;
const DELTA_DESCRIPTOR_FIXED_BYTES: usize = 72;
const DELTA_DESCRIPTOR_LENGTH_BYTES: usize = 4;
const DELTA_SELECTOR_FIXED_BYTES: usize = 16;
const DELTA_ENTRY_FIXED_BYTES: usize = 17;
static NEXT_DELTA_GENERATION: AtomicU64 = AtomicU64::new(1);

pub const RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE: &str =
    "relational-index-recovery.manifest.hawdb";
pub const DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_ENTRIES: usize = 100_000;
pub const DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_INDEX_RECOVERY_PAGES: usize = 4096;
pub const DEFAULT_RELATIONAL_INDEX_RECOVERY_MANIFEST_BYTES: usize = 1024 * 1024;

pub fn relational_index_recovery_delta_file(
    base_generation: u64,
    delta_generation: u64,
    ordinal: u32,
) -> String {
    format!("relational-index-recovery-{base_generation}-{delta_generation}-{ordinal}.delta.hawdb")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexRecoveryConfig {
    pub max_dirty_entries: NonZeroUsize,
    pub max_dirty_bytes: NonZeroUsize,
    pub max_delta_pages: NonZeroUsize,
    pub max_manifest_bytes: NonZeroUsize,
}

impl Default for RelationalIndexRecoveryConfig {
    fn default() -> Self {
        Self {
            max_dirty_entries: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_ENTRIES)
                .expect("default recovery dirty entry limit is non-zero"),
            max_dirty_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_BYTES)
                .expect("default recovery dirty byte limit is non-zero"),
            max_delta_pages: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_RECOVERY_PAGES)
                .expect("default recovery page limit is non-zero"),
            max_manifest_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_RECOVERY_MANIFEST_BYTES)
                .expect("default recovery manifest limit is non-zero"),
        }
    }
}

impl RelationalIndexRecoveryConfig {
    pub fn capture_limits(self) -> RelationalIndexChangeCaptureLimits {
        RelationalIndexChangeCaptureLimits {
            max_entries: self.max_dirty_entries,
            max_bytes: self.max_dirty_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeltaPageDescriptor {
    ordinal: u32,
    start_epoch: u64,
    end_epoch: u64,
    entry_count: u32,
    encoded_len: u64,
    digest: IntegrityDigest,
    selectors: Vec<DeltaPageSelector>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeltaPageSelector {
    table: String,
    index: String,
    lower_index_key: Vec<u8>,
    upper_index_key: Vec<u8>,
}

impl DeltaPageSelector {
    fn may_match_prefix(&self, prefix: &[u8]) -> bool {
        self.upper_index_key.as_slice() >= prefix
            && encoded_prefix_upper_bound(prefix)
                .is_none_or(|upper_bound| self.lower_index_key.as_slice() < upper_bound.as_slice())
    }

    fn encoded_len(&self) -> Result<usize, RelationalIndexShadowError> {
        DELTA_SELECTOR_FIXED_BYTES
            .checked_add(self.table.len())
            .and_then(|bytes| bytes.checked_add(self.index.len()))
            .and_then(|bytes| bytes.checked_add(self.lower_index_key.len()))
            .and_then(|bytes| bytes.checked_add(self.upper_index_key.len()))
            .ok_or_else(|| admission("recovery delta selector length overflow"))
    }
}

impl DeltaPageDescriptor {
    fn selector_bytes(&self) -> Result<usize, RelationalIndexShadowError> {
        self.selectors.iter().try_fold(0_usize, |bytes, selector| {
            bytes
                .checked_add(selector.encoded_len()?)
                .ok_or_else(|| admission("recovery delta selector payload overflow"))
        })
    }

    fn may_match_prefixes(&self, table: &str, index: &str, prefixes: &BTreeSet<Vec<u8>>) -> bool {
        self.may_match_selector(table, index, |selector| {
            prefixes
                .iter()
                .any(|prefix| selector.may_match_prefix(prefix))
        })
    }

    fn may_match_exact_key(&self, table: &str, index: &str, key: &[u8]) -> bool {
        self.may_match_selector(table, index, |selector| {
            selector.lower_index_key.as_slice() <= key && key <= selector.upper_index_key.as_slice()
        })
    }

    fn may_match_range(
        &self,
        table: &str,
        index: &str,
        prefix: &[u8],
        exclusive_bound: Option<&[u8]>,
        direction: RelationalIndexScanDirection,
    ) -> bool {
        self.may_match_selector(table, index, |selector| {
            selector.may_match_prefix(prefix)
                && exclusive_bound.is_none_or(|bound| match direction {
                    RelationalIndexScanDirection::Forward => {
                        selector.upper_index_key.as_slice() > bound
                    }
                    RelationalIndexScanDirection::Backward => {
                        selector.lower_index_key.as_slice() < bound
                    }
                })
        })
    }

    fn may_match_selector(
        &self,
        table: &str,
        index: &str,
        matches: impl FnMut(&DeltaPageSelector) -> bool,
    ) -> bool {
        self.selectors.is_empty()
            || self
                .selectors
                .iter()
                .filter(|selector| selector.table == table && selector.index == index)
                .any(matches)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexRecoveryManifest {
    pub base_generation: u64,
    pub delta_generation: u64,
    pub base_commit_epoch: u64,
    pub recovered_commit_epoch: u64,
    pub recovery_source: RelationalRecoverySourceIdentity,
    pages: Vec<DeltaPageDescriptor>,
}

impl RelationalIndexRecoveryManifest {
    pub fn delta_pages(&self) -> usize {
        self.pages.len()
    }

    pub fn delta_entries(&self) -> usize {
        self.pages
            .iter()
            .map(|page| page.entry_count as usize)
            .sum()
    }

    pub fn artifact_bytes(&self) -> u64 {
        self.pages
            .iter()
            .fold(0u64, |bytes, page| bytes.saturating_add(page.encoded_len))
    }

    fn encode(
        &self,
        config: RelationalIndexRecoveryConfig,
    ) -> Result<Vec<u8>, RelationalIndexShadowError> {
        validate_manifest(self, config, false)?;
        let payload_capacity = self.pages.iter().try_fold(0_usize, |bytes, page| {
            let selector_bytes = page.selector_bytes()?;
            bytes
                .checked_add(DELTA_DESCRIPTOR_LENGTH_BYTES)
                .and_then(|bytes| bytes.checked_add(DELTA_DESCRIPTOR_FIXED_BYTES))
                .and_then(|bytes| bytes.checked_add(selector_bytes))
                .ok_or_else(|| admission("recovery manifest payload length overflow"))
        })?;
        let mut payload = Vec::with_capacity(payload_capacity);
        for page in &self.pages {
            let descriptor = encode_delta_page_descriptor(page)?;
            let descriptor_len = u32::try_from(descriptor.len())
                .map_err(|_| admission("recovery delta descriptor length does not fit u32"))?;
            payload.extend_from_slice(&descriptor_len.to_le_bytes());
            payload.extend_from_slice(&descriptor);
        }
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            RelationalIndexShadowError::Admission(
                "recovery manifest payload length does not fit u64".to_string(),
            )
        })?;
        let page_count = u32::try_from(self.pages.len()).map_err(|_| {
            RelationalIndexShadowError::Admission(
                "recovery manifest page count does not fit u32".to_string(),
            )
        })?;
        let mut encoded = Vec::with_capacity(DELTA_MANIFEST_HEADER_BYTES + payload.len());
        encoded.extend_from_slice(DELTA_MANIFEST_MAGIC);
        encoded.extend_from_slice(&DELTA_MANIFEST_FORMAT_VERSION.to_le_bytes());
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        encoded.extend_from_slice(&self.base_generation.to_le_bytes());
        encoded.extend_from_slice(&self.delta_generation.to_le_bytes());
        encoded.extend_from_slice(&self.base_commit_epoch.to_le_bytes());
        encoded.extend_from_slice(&self.recovered_commit_epoch.to_le_bytes());
        self.recovery_source
            .encode_into(&mut encoded)
            .map_err(|reason| RelationalIndexShadowError::Admission(reason.to_string()))?;
        encoded.extend_from_slice(&page_count.to_le_bytes());
        encoded.extend_from_slice(&payload_len.to_le_bytes());
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded);
        hasher.update(&payload);
        let digest = hasher.finish();
        encoded.extend_from_slice(&digest.crc32c.get().to_le_bytes());
        encoded.extend_from_slice(digest.sha256.as_bytes());
        debug_assert_eq!(encoded.len(), DELTA_MANIFEST_HEADER_BYTES);
        encoded.extend_from_slice(&payload);
        if encoded.len() > config.max_manifest_bytes.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "recovery manifest contains {} bytes, exceeding limit {}",
                encoded.len(),
                config.max_manifest_bytes
            )));
        }
        Ok(encoded)
    }

    fn decode(
        encoded: &[u8],
        config: RelationalIndexRecoveryConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        if encoded.len() < DELTA_MANIFEST_HEADER_BYTES || &encoded[..8] != DELTA_MANIFEST_MAGIC {
            return Err(RelationalIndexShadowError::Corrupt(
                "invalid relational index recovery manifest header".to_string(),
            ));
        }
        if encoded.len() > config.max_manifest_bytes.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "recovery manifest contains {} bytes, exceeding limit {}",
                encoded.len(),
                config.max_manifest_bytes
            )));
        }
        let version = read_u16(&encoded[8..10]);
        let flags = read_u16(&encoded[10..12]);
        if version != DELTA_MANIFEST_FORMAT_VERSION || flags != 0 {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "unsupported relational index recovery manifest version {version} or flags {flags}"
            )));
        }
        let base_generation = read_u64(&encoded[12..20]);
        let delta_generation = read_u64(&encoded[20..28]);
        let base_commit_epoch = read_u64(&encoded[28..36]);
        let recovered_commit_epoch = read_u64(&encoded[36..44]);
        let recovery_source = RelationalRecoverySourceIdentity::decode(
            &encoded[44..44 + RELATIONAL_RECOVERY_SOURCE_BYTES],
        )
        .map_err(|reason| RelationalIndexShadowError::Corrupt(reason.to_string()))?;
        let page_count = read_u32(&encoded[100..104]) as usize;
        if page_count > config.max_delta_pages.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "recovery manifest declares {page_count} pages, exceeding limit {}",
                config.max_delta_pages
            )));
        }
        let payload_len = usize::try_from(read_u64(&encoded[104..112])).map_err(|_| {
            RelationalIndexShadowError::Corrupt(
                "recovery manifest payload length overflows usize".to_string(),
            )
        })?;
        let expected_len = DELTA_MANIFEST_HEADER_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| {
                RelationalIndexShadowError::Corrupt("recovery manifest length overflow".to_string())
            })?;
        if encoded.len() != expected_len {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index recovery manifest length mismatch".to_string(),
            ));
        }
        let payload = &encoded[DELTA_MANIFEST_HEADER_BYTES..];
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded[..DELTA_MANIFEST_INTEGRITY_OFFSET]);
        hasher.update(payload);
        let digest = hasher.finish();
        if digest.crc32c.get() != read_u32(&encoded[112..116])
            || digest.sha256.as_bytes() != &encoded[116..148]
        {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index recovery manifest checksum mismatch".to_string(),
            ));
        }
        let mut pages = Vec::with_capacity(page_count);
        let mut offset = 0usize;
        for _ in 0..page_count {
            let descriptor_len = read_u32(take(
                payload,
                &mut offset,
                DELTA_DESCRIPTOR_LENGTH_BYTES,
                "delta descriptor length",
            )?) as usize;
            let descriptor = take(payload, &mut offset, descriptor_len, "delta descriptor")?;
            pages.push(decode_delta_page_descriptor(descriptor, config)?);
        }
        if offset != payload.len() {
            return Err(corrupt(
                "relational index recovery manifest contains trailing descriptor bytes",
            ));
        }
        let manifest = Self {
            base_generation,
            delta_generation,
            base_commit_epoch,
            recovered_commit_epoch,
            recovery_source,
            pages,
        };
        validate_manifest(&manifest, config, true)?;
        Ok(manifest)
    }
}

fn encode_delta_page_descriptor(
    descriptor: &DeltaPageDescriptor,
) -> Result<Vec<u8>, RelationalIndexShadowError> {
    let selector_count = u32::try_from(descriptor.selectors.len())
        .map_err(|_| admission("recovery delta selector count does not fit u32"))?;
    let selector_bytes = descriptor.selector_bytes()?;
    let capacity = DELTA_DESCRIPTOR_FIXED_BYTES
        .checked_add(selector_bytes)
        .ok_or_else(|| admission("recovery delta descriptor length overflow"))?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(&descriptor.ordinal.to_le_bytes());
    encoded.extend_from_slice(&descriptor.start_epoch.to_le_bytes());
    encoded.extend_from_slice(&descriptor.end_epoch.to_le_bytes());
    encoded.extend_from_slice(&descriptor.entry_count.to_le_bytes());
    encoded.extend_from_slice(&descriptor.encoded_len.to_le_bytes());
    encoded.extend_from_slice(&descriptor.digest.crc32c.get().to_le_bytes());
    encoded.extend_from_slice(descriptor.digest.sha256.as_bytes());
    encoded.extend_from_slice(&selector_count.to_le_bytes());
    debug_assert_eq!(encoded.len(), DELTA_DESCRIPTOR_FIXED_BYTES);
    for selector in &descriptor.selectors {
        encode_bytes(&mut encoded, selector.table.as_bytes())?;
        encode_bytes(&mut encoded, selector.index.as_bytes())?;
        encode_bytes(&mut encoded, &selector.lower_index_key)?;
        encode_bytes(&mut encoded, &selector.upper_index_key)?;
    }
    Ok(encoded)
}

fn decode_delta_page_descriptor(
    encoded: &[u8],
    config: RelationalIndexRecoveryConfig,
) -> Result<DeltaPageDescriptor, RelationalIndexShadowError> {
    if encoded.len() < DELTA_DESCRIPTOR_FIXED_BYTES {
        return Err(corrupt("truncated recovery delta descriptor"));
    }
    let mut offset = 0usize;
    let ordinal = read_u32(take(encoded, &mut offset, 4, "delta ordinal")?);
    let start_epoch = read_u64(take(encoded, &mut offset, 8, "delta start epoch")?);
    let end_epoch = read_u64(take(encoded, &mut offset, 8, "delta end epoch")?);
    let entry_count = read_u32(take(encoded, &mut offset, 4, "delta entry count")?);
    let encoded_len = read_u64(take(encoded, &mut offset, 8, "delta encoded length")?);
    let crc32c = read_u32(take(encoded, &mut offset, 4, "delta CRC32C")?);
    let sha256 = Sha256Digest::from_bytes(
        take(encoded, &mut offset, SHA256_BYTES, "delta SHA-256")?
            .try_into()
            .expect("delta SHA-256 length was checked"),
    );
    let selector_count = read_u32(take(encoded, &mut offset, 4, "delta selector count")?) as usize;
    if selector_count > encoded.len().saturating_sub(offset) / DELTA_SELECTOR_FIXED_BYTES {
        return Err(corrupt(
            "recovery delta descriptor selector count is impossible",
        ));
    }
    let mut selectors = Vec::with_capacity(selector_count);
    for _ in 0..selector_count {
        let (table, next) = decode_bytes(
            encoded,
            offset,
            config.max_dirty_bytes.get(),
            "recovery delta selector table",
        )?;
        offset = next;
        let (index, next) = decode_bytes(
            encoded,
            offset,
            config.max_dirty_bytes.get(),
            "recovery delta selector index",
        )?;
        offset = next;
        let (lower_index_key, next) = decode_bytes(
            encoded,
            offset,
            config.max_dirty_bytes.get(),
            "recovery delta selector lower key",
        )?;
        offset = next;
        let (upper_index_key, next) = decode_bytes(
            encoded,
            offset,
            config.max_dirty_bytes.get(),
            "recovery delta selector upper key",
        )?;
        offset = next;
        selectors.push(DeltaPageSelector {
            table: decode_utf8(table, "recovery delta selector table")?,
            index: decode_utf8(index, "recovery delta selector index")?,
            lower_index_key: lower_index_key.to_vec(),
            upper_index_key: upper_index_key.to_vec(),
        });
    }
    if offset != encoded.len() {
        return Err(corrupt("recovery delta descriptor contains trailing bytes"));
    }
    Ok(DeltaPageDescriptor {
        ordinal,
        start_epoch,
        end_epoch,
        entry_count,
        encoded_len,
        digest: IntegrityDigest {
            crc32c: hawdb_integrity::Crc32c::new(crc32c),
            sha256,
        },
        selectors,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DeltaKey {
    table: String,
    index: String,
    index_key: Vec<u8>,
    primary_key: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeltaValue {
    kind: RelationalIndexChangeKind,
    epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeltaEntry {
    key: DeltaKey,
    value: DeltaValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexRecoveryReport {
    pub base_generation: u64,
    pub delta_generation: u64,
    pub base_commit_epoch: u64,
    pub recovered_commit_epoch: u64,
    pub delta_pages: usize,
    pub delta_entries: usize,
    pub artifact_bytes: u64,
    pub manifest_bytes: u64,
    pub flushes: usize,
    pub peak_dirty_entries: usize,
    pub peak_dirty_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct RelationalIndexRecoveryBuilder {
    directory: PathBuf,
    base_generation: u64,
    delta_generation: u64,
    base_commit_epoch: u64,
    config: RelationalIndexRecoveryConfig,
    dirty: BTreeMap<DeltaKey, DeltaValue>,
    dirty_bytes: usize,
    dirty_start_epoch: Option<u64>,
    dirty_end_epoch: Option<u64>,
    pages: Vec<DeltaPageDescriptor>,
    artifact_bytes: u64,
    peak_dirty_entries: usize,
    peak_dirty_bytes: usize,
}

impl RelationalIndexRecoveryBuilder {
    pub fn new(
        directory: &Path,
        base_generation: u64,
        base_commit_epoch: u64,
        config: RelationalIndexRecoveryConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        if base_generation == 0 {
            return Err(RelationalIndexShadowError::Admission(
                "recovery delta requires a non-zero base generation".to_string(),
            ));
        }
        fs::create_dir_all(directory).map_err(|error| {
            RelationalIndexShadowError::Durability(format!(
                "create relational index recovery directory: {error}"
            ))
        })?;
        Ok(Self {
            directory: directory.to_path_buf(),
            base_generation,
            delta_generation: next_delta_generation(),
            base_commit_epoch,
            config,
            dirty: BTreeMap::new(),
            dirty_bytes: 0,
            dirty_start_epoch: None,
            dirty_end_epoch: None,
            pages: Vec::new(),
            artifact_bytes: 0,
            peak_dirty_entries: 0,
            peak_dirty_bytes: 0,
        })
    }

    pub fn capture_limits(&self) -> RelationalIndexChangeCaptureLimits {
        self.config.capture_limits()
    }

    pub fn base_commit_epoch(&self) -> u64 {
        self.base_commit_epoch
    }

    pub fn record(
        &mut self,
        epoch: u64,
        capture: RelationalIndexChangeCapture,
    ) -> Result<(), RelationalIndexShadowError> {
        if epoch <= self.base_commit_epoch {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "recovery delta epoch {epoch} is not after base epoch {}",
                self.base_commit_epoch
            )));
        }
        let RelationalIndexChangeCapture::Captured { changes, .. } = capture else {
            let RelationalIndexChangeCapture::Invalidated { reason } = capture else {
                unreachable!()
            };
            return Err(RelationalIndexShadowError::Admission(reason));
        };
        for change in changes {
            let (key, value, entry_bytes) = encode_change(change, epoch)?;
            let key_exists = self.dirty.contains_key(&key);
            let existing_bytes = if key_exists { entry_bytes } else { 0 };
            let next_entries = self.dirty.len() + usize::from(!key_exists);
            let next_bytes = self
                .dirty_bytes
                .checked_sub(existing_bytes)
                .and_then(|bytes| bytes.checked_add(entry_bytes))
                .ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "recovery dirty byte accounting overflow".to_string(),
                    )
                })?;
            if !self.dirty.is_empty()
                && (next_entries > self.config.max_dirty_entries.get()
                    || next_bytes > self.config.max_dirty_bytes.get())
            {
                self.flush()?;
            }
            if entry_bytes > self.config.max_dirty_bytes.get() {
                return Err(RelationalIndexShadowError::Admission(format!(
                    "one recovery delta entry needs {entry_bytes} bytes, exceeding dirty limit {}",
                    self.config.max_dirty_bytes
                )));
            }
            if let Some(previous) = self.dirty.insert(key, value) {
                debug_assert!(previous.epoch <= epoch);
            } else {
                self.dirty_bytes = self.dirty_bytes.checked_add(entry_bytes).ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "recovery dirty byte accounting overflow".to_string(),
                    )
                })?;
            }
            self.dirty_start_epoch = Some(
                self.dirty_start_epoch
                    .map_or(epoch, |value| value.min(epoch)),
            );
            self.dirty_end_epoch =
                Some(self.dirty_end_epoch.map_or(epoch, |value| value.max(epoch)));
            self.peak_dirty_entries = self.peak_dirty_entries.max(self.dirty.len());
            self.peak_dirty_bytes = self.peak_dirty_bytes.max(self.dirty_bytes);
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn finish(
        self,
        recovered_commit_epoch: u64,
    ) -> Result<RelationalIndexRecoveryReport, RelationalIndexShadowError> {
        let recovery_source = RelationalRecoverySourceIdentity::for_test(
            self.base_commit_epoch,
            recovered_commit_epoch,
        );
        self.finish_with_recovery_source(recovered_commit_epoch, recovery_source)
    }

    pub fn finish_with_recovery_source(
        mut self,
        recovered_commit_epoch: u64,
        recovery_source: RelationalRecoverySourceIdentity,
    ) -> Result<RelationalIndexRecoveryReport, RelationalIndexShadowError> {
        if recovered_commit_epoch < self.base_commit_epoch {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "recovered epoch {recovered_commit_epoch} precedes base epoch {}",
                self.base_commit_epoch
            )));
        }
        self.flush()?;
        let mut pages = self.pages;
        bound_delta_page_selectors(&mut pages, self.config)?;
        let manifest = RelationalIndexRecoveryManifest {
            base_generation: self.base_generation,
            delta_generation: self.delta_generation,
            base_commit_epoch: self.base_commit_epoch,
            recovered_commit_epoch,
            recovery_source,
            pages,
        };
        let encoded_manifest = manifest.encode(self.config)?;
        let manifest_path = self.directory.join(RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE);
        let manifest_tmp = manifest_path.with_extension("hawdb.tmp");
        write_synced(&manifest_tmp, &encoded_manifest, "write recovery manifest")?;
        durable_replace_file(&manifest_tmp, &manifest_path).map_err(|error| {
            RelationalIndexShadowError::Durability(format!(
                "publish relational index recovery manifest: {error}"
            ))
        })?;
        Ok(RelationalIndexRecoveryReport {
            base_generation: manifest.base_generation,
            delta_generation: manifest.delta_generation,
            base_commit_epoch: manifest.base_commit_epoch,
            recovered_commit_epoch: manifest.recovered_commit_epoch,
            delta_pages: manifest.pages.len(),
            delta_entries: manifest.delta_entries(),
            artifact_bytes: self.artifact_bytes,
            manifest_bytes: encoded_manifest.len() as u64,
            flushes: manifest.pages.len(),
            peak_dirty_entries: self.peak_dirty_entries,
            peak_dirty_bytes: self.peak_dirty_bytes,
        })
    }

    fn flush(&mut self) -> Result<(), RelationalIndexShadowError> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        if self.pages.len() >= self.config.max_delta_pages.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "recovery delta needs more than {} pages",
                self.config.max_delta_pages
            )));
        }
        let ordinal = u32::try_from(self.pages.len()).map_err(|_| {
            RelationalIndexShadowError::Admission(
                "recovery delta page ordinal does not fit u32".to_string(),
            )
        })?;
        let entry_count = u32::try_from(self.dirty.len()).map_err(|_| {
            RelationalIndexShadowError::Admission(
                "recovery delta entry count does not fit u32".to_string(),
            )
        })?;
        let start_epoch = self
            .dirty_start_epoch
            .expect("non-empty dirty overlay has a start epoch");
        let end_epoch = self
            .dirty_end_epoch
            .expect("non-empty dirty overlay has an end epoch");
        let final_path = self.directory.join(relational_index_recovery_delta_file(
            self.base_generation,
            self.delta_generation,
            ordinal,
        ));
        let tmp_path = final_path.with_extension("hawdb.tmp");
        let (encoded_len, digest) = write_delta_page(
            &tmp_path,
            DeltaPageWrite {
                base_generation: self.base_generation,
                delta_generation: self.delta_generation,
                base_commit_epoch: self.base_commit_epoch,
                ordinal,
                start_epoch,
                end_epoch,
                entry_count,
                payload_bytes: self.dirty_bytes,
                entries: &self.dirty,
            },
            self.config,
        )?;
        durable_replace_file(&tmp_path, &final_path).map_err(|error| {
            RelationalIndexShadowError::Durability(format!(
                "publish relational index recovery delta page: {error}"
            ))
        })?;
        self.pages.push(DeltaPageDescriptor {
            ordinal,
            start_epoch,
            end_epoch,
            entry_count,
            encoded_len,
            digest,
            selectors: delta_page_selectors(&self.dirty),
        });
        self.artifact_bytes = self.artifact_bytes.saturating_add(encoded_len);
        self.dirty.clear();
        self.dirty_bytes = 0;
        self.dirty_start_epoch = None;
        self.dirty_end_epoch = None;
        Ok(())
    }
}

fn delta_page_selectors(entries: &BTreeMap<DeltaKey, DeltaValue>) -> Vec<DeltaPageSelector> {
    let mut selectors = Vec::<DeltaPageSelector>::new();
    for key in entries.keys() {
        if let Some(selector) = selectors.last_mut()
            && selector.table == key.table
            && selector.index == key.index
        {
            selector.upper_index_key.clone_from(&key.index_key);
            continue;
        }
        selectors.push(DeltaPageSelector {
            table: key.table.clone(),
            index: key.index.clone(),
            lower_index_key: key.index_key.clone(),
            upper_index_key: key.index_key.clone(),
        });
    }
    selectors
}

/// Keeps recovery-delta page pruning best-effort. If selectors would exceed
/// the existing manifest budget, an affected page simply remains unpruned.
fn bound_delta_page_selectors(
    pages: &mut [DeltaPageDescriptor],
    config: RelationalIndexRecoveryConfig,
) -> Result<(), RelationalIndexShadowError> {
    let static_bytes = DELTA_MANIFEST_HEADER_BYTES
        .checked_add(
            pages
                .len()
                .checked_mul(
                    DELTA_DESCRIPTOR_LENGTH_BYTES
                        .checked_add(DELTA_DESCRIPTOR_FIXED_BYTES)
                        .expect("recovery descriptor constants do not overflow"),
                )
                .ok_or_else(|| admission("recovery manifest static descriptor bytes overflow"))?,
        )
        .ok_or_else(|| admission("recovery manifest static byte count overflow"))?;
    if static_bytes > config.max_manifest_bytes.get() {
        return Err(admission(format!(
            "recovery manifest needs {static_bytes} static bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let mut remaining = config.max_manifest_bytes.get() - static_bytes;
    for page in pages {
        let selector_bytes = page.selector_bytes()?;
        if selector_bytes > remaining {
            page.selectors.clear();
        } else {
            remaining -= selector_bytes;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexRecoveryReadReport {
    pub base: RelationalIndexReadReport,
    pub delta_pages_read: usize,
    pub delta_pages_skipped: usize,
    pub delta_bytes_read: usize,
    pub delta_file_pages_read: usize,
    pub delta_file_bytes_read: usize,
    pub delta_cache_hits: usize,
    pub delta_cache_misses: usize,
    pub delta_cache_admission_rejections: usize,
    pub delta_entries_visited: usize,
    pub rows_visited: usize,
    pub stopped_early: bool,
}

struct RecoveryOrderedMerge<'a, F> {
    pending: BTreeMap<(RelationalKey, RelationalKey), RelationalIndexChangeKind>,
    visit: &'a mut F,
    max_rows: usize,
    rows_visited: usize,
    stopped_early: bool,
    error: Option<RelationalIndexShadowError>,
    direction: RelationalIndexScanDirection,
}

impl<F> RecoveryOrderedMerge<'_, F>
where
    F: FnMut(&RelationalKey, &RelationalKey) -> bool,
{
    fn emit(&mut self, index_key: &RelationalKey, primary_key: &RelationalKey) -> bool {
        let Some(rows_visited) = self.rows_visited.checked_add(1) else {
            self.error = Some(admission("recovery index output row counter overflow"));
            return false;
        };
        if rows_visited > self.max_rows {
            self.error = Some(admission(format!(
                "recovery index lookup exceeds row limit {}",
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

pub struct RelationalIndexRecoveryReader {
    base: RelationalIndexShadowReader,
    manifest: RelationalIndexRecoveryManifest,
    config: RelationalIndexRecoveryConfig,
    page_cache: Option<Arc<SegmentCache>>,
    store_id: StoreId,
    poisoned: AtomicBool,
}

#[derive(Debug, Clone, Copy)]
struct RecoveryDeltaPageRead {
    cache_hit: bool,
    cache_miss: bool,
    cache_admission_rejected: bool,
}

impl RelationalIndexRecoveryReader {
    pub fn open_latest(
        directory: &Path,
        expected_recovery: RelationalRecoveryFence,
        shadow_config: RelationalIndexShadowConfig,
        recovery_config: RelationalIndexRecoveryConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        Self::open_latest_inner(
            directory,
            expected_recovery,
            shadow_config,
            recovery_config,
            None,
            StoreId::default(),
        )
    }

    pub fn open_latest_with_cache(
        directory: &Path,
        expected_recovery: RelationalRecoveryFence,
        shadow_config: RelationalIndexShadowConfig,
        recovery_config: RelationalIndexRecoveryConfig,
        page_cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        Self::open_latest_inner(
            directory,
            expected_recovery,
            shadow_config,
            recovery_config,
            Some(page_cache),
            store_id,
        )
    }

    pub fn open_generation_with_cache(
        directory: &Path,
        expected_base: RelationalIndexGenerationIdentity,
        expected_recovery: RelationalRecoveryFence,
        shadow_config: RelationalIndexShadowConfig,
        recovery_config: RelationalIndexRecoveryConfig,
        page_cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        let base = RelationalIndexShadowReader::open_generation_with_cache(
            directory,
            expected_base,
            shadow_config,
            Arc::clone(&page_cache),
            store_id,
        )?;
        Self::open_with_base(
            directory,
            base,
            expected_recovery,
            recovery_config,
            Some(page_cache),
            store_id,
        )
    }

    pub fn open_bound_generation_with_cache(
        directory: &Path,
        base_binding: RelationalIndexGenerationArtifacts,
        expected_recovery: RelationalRecoveryFence,
        shadow_config: RelationalIndexShadowConfig,
        recovery_config: RelationalIndexRecoveryConfig,
        page_cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        let base = RelationalIndexShadowReader::open_bound_generation_with_cache(
            directory,
            base_binding,
            shadow_config,
            Arc::clone(&page_cache),
            store_id,
        )?;
        Self::open_with_base(
            directory,
            base,
            expected_recovery,
            recovery_config,
            Some(page_cache),
            store_id,
        )
    }

    fn open_latest_inner(
        directory: &Path,
        expected_recovery: RelationalRecoveryFence,
        shadow_config: RelationalIndexShadowConfig,
        recovery_config: RelationalIndexRecoveryConfig,
        page_cache: Option<Arc<SegmentCache>>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        let base = if let Some(cache) = &page_cache {
            RelationalIndexShadowReader::open_latest_with_cache(
                directory,
                shadow_config,
                Arc::clone(cache),
                store_id,
            )?
        } else {
            RelationalIndexShadowReader::open_latest(directory, shadow_config)?
        };
        Self::open_with_base(
            directory,
            base,
            expected_recovery,
            recovery_config,
            page_cache,
            store_id,
        )
    }

    fn open_with_base(
        directory: &Path,
        base: RelationalIndexShadowReader,
        expected_recovery: RelationalRecoveryFence,
        recovery_config: RelationalIndexRecoveryConfig,
        page_cache: Option<Arc<SegmentCache>>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        let manifest_path = directory.join(RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE);
        let encoded = read_bounded_file(
            &manifest_path,
            recovery_config.max_manifest_bytes.get(),
            "relational index recovery manifest",
        )?;
        let manifest = RelationalIndexRecoveryManifest::decode(&encoded, recovery_config)?;
        if manifest.base_generation != base.manifest().generation
            || manifest.base_commit_epoch != base.manifest().source_commit_epoch
            || manifest.recovered_commit_epoch != expected_recovery.commit_epoch
            || manifest.recovery_source != expected_recovery.source
        {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "recovery manifest fence {}/{}/{}/{:?} does not match base {}/{} and expected recovery {expected_recovery:?}",
                manifest.base_generation,
                manifest.base_commit_epoch,
                manifest.recovered_commit_epoch,
                manifest.recovery_source,
                base.manifest().generation,
                base.manifest().source_commit_epoch
            )));
        }
        Ok(Self {
            base,
            manifest,
            config: recovery_config,
            page_cache,
            store_id,
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn manifest(&self) -> &RelationalIndexRecoveryManifest {
        &self.manifest
    }

    pub fn base_manifest(&self) -> &super::RelationalIndexShadowManifest {
        self.base.manifest()
    }

    pub fn validate_required_roots(
        &self,
        state: &RelationalState,
    ) -> Result<(), RelationalIndexShadowError> {
        self.base.validate_required_roots(state)
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire) || self.base.is_poisoned()
    }

    pub fn visit_exact_postings(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey) -> bool,
    ) -> Result<RelationalIndexRecoveryReadReport, RelationalIndexShadowError> {
        self.visit_merged_exact(table, index, key, limits, visit)
    }

    pub fn visit_prefix_postings(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey) -> bool,
    ) -> Result<RelationalIndexRecoveryReadReport, RelationalIndexShadowError> {
        self.visit_prefix_entries(table, index, prefix, limits, |_, primary_key| {
            visit(primary_key)
        })
    }

    pub fn visit_prefix_entries(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexRecoveryReadReport, RelationalIndexShadowError> {
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

    /// Merges recovery-delta and immutable postings for equally wide prefixes
    /// while charging one shared index-read budget.
    pub fn visit_prefix_entries_many(
        &self,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexRecoveryReadReport, RelationalIndexShadowError> {
        let mut encoded_prefixes = BTreeSet::new();
        let mut prefix_width = None;
        for prefix in prefixes {
            if let Some(width) = prefix_width {
                if width != prefix.0.len() {
                    return Err(admission(
                        "batch index prefixes must have one common key width",
                    ));
                }
            } else {
                prefix_width = Some(prefix.0.len());
            }
            encoded_prefixes.insert(encode_relational_key(prefix)?);
        }
        if encoded_prefixes.is_empty() {
            return Ok(RelationalIndexRecoveryReadReport {
                base: RelationalIndexReadReport::default(),
                delta_pages_read: 0,
                delta_pages_skipped: 0,
                delta_bytes_read: 0,
                delta_file_pages_read: 0,
                delta_file_bytes_read: 0,
                delta_cache_hits: 0,
                delta_cache_misses: 0,
                delta_cache_admission_rejections: 0,
                delta_entries_visited: 0,
                rows_visited: 0,
                stopped_early: false,
            });
        }
        if self.is_poisoned() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index recovery reader is poisoned".to_string(),
            ));
        }

        let mut pending = BTreeMap::new();
        let mut report = RelationalIndexRecoveryReadReport {
            base: RelationalIndexReadReport::default(),
            delta_pages_read: 0,
            delta_pages_skipped: 0,
            delta_bytes_read: 0,
            delta_file_pages_read: 0,
            delta_file_bytes_read: 0,
            delta_cache_hits: 0,
            delta_cache_misses: 0,
            delta_cache_admission_rejections: 0,
            delta_entries_visited: 0,
            rows_visited: 0,
            stopped_early: false,
        };
        for descriptor in &self.manifest.pages {
            if !descriptor.may_match_prefixes(table, index, &encoded_prefixes) {
                report.delta_pages_skipped = report
                    .delta_pages_skipped
                    .checked_add(1)
                    .ok_or_else(|| admission("recovery batch skipped-page counter overflow"))?;
                continue;
            }
            if report.delta_pages_read >= limits.max_pages.get() {
                return Err(admission(format!(
                    "recovery index batch lookup exceeds page limit {}",
                    limits.max_pages
                )));
            }
            let encoded_len = usize::try_from(descriptor.encoded_len)
                .map_err(|_| corrupt("recovery delta encoded length overflows usize"))?;
            let total_bytes = report
                .delta_bytes_read
                .checked_add(encoded_len)
                .ok_or_else(|| admission("recovery batch read byte counter overflow"))?;
            if total_bytes > limits.max_bytes.get() {
                return Err(admission(format!(
                    "recovery index batch lookup needs {total_bytes} bytes, exceeding byte limit {}",
                    limits.max_bytes
                )));
            }
            let remaining_file_bytes = limits
                .max_file_bytes
                .checked_sub(report.delta_file_bytes_read)
                .ok_or_else(|| admission("recovery batch file-byte counter exceeds its limit"))?;
            let page_read = self.visit_page(descriptor, remaining_file_bytes, |entry| {
                report.delta_entries_visited = report
                    .delta_entries_visited
                    .checked_add(1)
                    .ok_or_else(|| admission("recovery batch delta entry counter overflow"))?;
                if entry.key.table != table
                    || entry.key.index != index
                    || !encoded_prefixes
                        .iter()
                        .any(|prefix| entry.key.index_key.starts_with(prefix))
                {
                    return Ok(());
                }
                let index_key = super::demand_read::decode_relational_key(&entry.key.index_key)
                    .inspect_err(|_| self.poison())?;
                let primary_key = super::demand_read::decode_relational_key(&entry.key.primary_key)
                    .inspect_err(|_| self.poison())?;
                pending.insert((index_key, primary_key), entry.value.kind);
                if pending.len() >= limits.max_rows.get() {
                    return Err(admission(format!(
                        "recovery batch ordered merge needs {} entries, exhausting row limit {}",
                        pending.len(),
                        limits.max_rows
                    )));
                }
                Ok(())
            })?;
            report.delta_pages_read += 1;
            report.delta_bytes_read += encoded_len;
            report.delta_cache_hits += usize::from(page_read.cache_hit);
            report.delta_cache_misses += usize::from(page_read.cache_miss);
            report.delta_cache_admission_rejections +=
                usize::from(page_read.cache_admission_rejected);
            if !page_read.cache_hit {
                report.delta_file_pages_read += 1;
                report.delta_file_bytes_read += encoded_len;
            }
        }
        let base_pages = limits
            .max_pages
            .get()
            .checked_sub(report.delta_pages_read)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| admission("recovery batch ordered merge exhausted its page budget"))?;
        let base_bytes = limits
            .max_bytes
            .get()
            .checked_sub(report.delta_bytes_read)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| admission("recovery batch ordered merge exhausted its byte budget"))?;
        let reserved_inserts = pending
            .values()
            .filter(|kind| **kind == RelationalIndexChangeKind::Insert)
            .count();
        let base_rows = limits
            .max_rows
            .get()
            .checked_sub(reserved_inserts)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| admission("recovery batch ordered merge exhausted its row budget"))?;
        let base_limits = RelationalIndexReadLimits {
            max_pages: base_pages,
            max_rows: base_rows,
            max_bytes: base_bytes,
            max_file_bytes: limits
                .max_file_bytes
                .checked_sub(report.delta_file_bytes_read)
                .ok_or_else(|| {
                    admission("recovery batch ordered merge exhausted its file-byte budget")
                })?,
            ..limits
        };
        let mut merge = RecoveryOrderedMerge {
            pending,
            visit: &mut visit,
            max_rows: limits.max_rows.get(),
            rows_visited: 0,
            stopped_early: false,
            error: None,
            direction: RelationalIndexScanDirection::Forward,
        };
        report.base = self.base.visit_prefix_entries_many(
            table,
            index,
            prefixes,
            base_limits,
            |_, index_key, primary_key| merge.visit_base(index_key, primary_key),
        )?;
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        merge.finish();
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        report.rows_visited = merge.rows_visited;
        report.stopped_early = merge.stopped_early || report.base.stopped_early;
        Ok(report)
    }

    pub fn visit_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Result<RelationalIndexRecoveryReadReport, RelationalIndexShadowError> {
        let encoded_prefix = encode_relational_key(&scan.prefix)?;
        let encoded_bound = scan
            .exclusive_bound
            .as_ref()
            .map(encode_relational_key)
            .transpose()?;
        if self.is_poisoned() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index recovery reader is poisoned".to_string(),
            ));
        }
        let mut pending = BTreeMap::new();
        let mut report = RelationalIndexRecoveryReadReport {
            base: RelationalIndexReadReport::default(),
            delta_pages_read: 0,
            delta_pages_skipped: 0,
            delta_bytes_read: 0,
            delta_file_pages_read: 0,
            delta_file_bytes_read: 0,
            delta_cache_hits: 0,
            delta_cache_misses: 0,
            delta_cache_admission_rejections: 0,
            delta_entries_visited: 0,
            rows_visited: 0,
            stopped_early: false,
        };
        for descriptor in &self.manifest.pages {
            if !descriptor.may_match_range(
                table,
                index,
                &encoded_prefix,
                encoded_bound.as_deref(),
                scan.direction,
            ) {
                report.delta_pages_skipped = report
                    .delta_pages_skipped
                    .checked_add(1)
                    .ok_or_else(|| admission("recovery skipped-page counter overflow"))?;
                continue;
            }
            if report.delta_pages_read >= limits.max_pages.get() {
                return Err(admission(format!(
                    "recovery index lookup exceeds page limit {}",
                    limits.max_pages
                )));
            }
            let encoded_len = usize::try_from(descriptor.encoded_len)
                .map_err(|_| corrupt("recovery delta encoded length overflows usize"))?;
            let total_bytes = report
                .delta_bytes_read
                .checked_add(encoded_len)
                .ok_or_else(|| admission("recovery read byte counter overflow"))?;
            if total_bytes > limits.max_bytes.get() {
                return Err(admission(format!(
                    "recovery index lookup needs {total_bytes} bytes, exceeding byte limit {}",
                    limits.max_bytes
                )));
            }
            let remaining_file_bytes = limits
                .max_file_bytes
                .checked_sub(report.delta_file_bytes_read)
                .ok_or_else(|| admission("recovery delta file-byte counter exceeds its limit"))?;
            let page_read = self.visit_page(descriptor, remaining_file_bytes, |entry| {
                report.delta_entries_visited = report
                    .delta_entries_visited
                    .checked_add(1)
                    .ok_or_else(|| admission("recovery delta entry counter overflow"))?;
                if entry.key.table != table
                    || entry.key.index != index
                    || !entry.key.index_key.starts_with(&encoded_prefix)
                    || encoded_bound
                        .as_ref()
                        .is_some_and(|bound| match scan.direction {
                            RelationalIndexScanDirection::Forward => entry.key.index_key <= *bound,
                            RelationalIndexScanDirection::Backward => entry.key.index_key >= *bound,
                        })
                {
                    return Ok(());
                }
                let index_key = super::demand_read::decode_relational_key(&entry.key.index_key)
                    .inspect_err(|_| self.poison())?;
                let primary_key = super::demand_read::decode_relational_key(&entry.key.primary_key)
                    .inspect_err(|_| self.poison())?;
                pending.insert((index_key, primary_key), entry.value.kind);
                if pending.len() >= limits.max_rows.get() {
                    return Err(admission(format!(
                        "recovery ordered merge needs {} entries, exhausting row limit {}",
                        pending.len(),
                        limits.max_rows
                    )));
                }
                Ok(())
            })?;
            report.delta_pages_read += 1;
            report.delta_bytes_read += encoded_len;
            report.delta_cache_hits += usize::from(page_read.cache_hit);
            report.delta_cache_misses += usize::from(page_read.cache_miss);
            report.delta_cache_admission_rejections +=
                usize::from(page_read.cache_admission_rejected);
            if !page_read.cache_hit {
                report.delta_file_pages_read += 1;
                report.delta_file_bytes_read += encoded_len;
            }
        }
        let base_pages = limits
            .max_pages
            .get()
            .checked_sub(report.delta_pages_read)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| admission("recovery ordered merge exhausted its page budget"))?;
        let base_bytes = limits
            .max_bytes
            .get()
            .checked_sub(report.delta_bytes_read)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| admission("recovery ordered merge exhausted its byte budget"))?;
        let reserved_inserts = pending
            .values()
            .filter(|kind| **kind == RelationalIndexChangeKind::Insert)
            .count();
        let base_rows = limits
            .max_rows
            .get()
            .checked_sub(reserved_inserts)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| admission("recovery ordered merge exhausted its row budget"))?;
        let base_limits = RelationalIndexReadLimits {
            max_pages: base_pages,
            max_rows: base_rows,
            max_bytes: base_bytes,
            max_file_bytes: limits
                .max_file_bytes
                .checked_sub(report.delta_file_bytes_read)
                .ok_or_else(|| {
                    admission("recovery ordered merge exhausted its file-byte budget")
                })?,
            ..limits
        };
        let mut merge = RecoveryOrderedMerge {
            pending,
            visit: &mut visit,
            max_rows: limits.max_rows.get(),
            rows_visited: 0,
            stopped_early: false,
            error: None,
            direction: scan.direction,
        };
        report.base = self.base.visit_range_entries(
            table,
            index,
            scan,
            base_limits,
            |index_key, primary_key| merge.visit_base(index_key, primary_key),
        )?;
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        merge.finish();
        if let Some(error) = merge.error.take() {
            return Err(error);
        }
        report.rows_visited = merge.rows_visited;
        report.stopped_early = merge.stopped_early || report.base.stopped_early;
        Ok(report)
    }

    fn visit_merged_exact(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey) -> bool,
    ) -> Result<RelationalIndexRecoveryReadReport, RelationalIndexShadowError> {
        if self.is_poisoned() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index recovery reader is poisoned".to_string(),
            ));
        }
        let mut rows = BTreeSet::new();
        let mut collect = |key: &RelationalKey| {
            rows.insert(key.clone());
            true
        };
        let base = self
            .base
            .visit_exact_postings(table, index, key, limits, &mut collect)?;
        let selector_key = encode_relational_key(key)?;
        let mut report = RelationalIndexRecoveryReadReport {
            base,
            delta_pages_read: 0,
            delta_pages_skipped: 0,
            delta_bytes_read: 0,
            delta_file_pages_read: 0,
            delta_file_bytes_read: 0,
            delta_cache_hits: 0,
            delta_cache_misses: 0,
            delta_cache_admission_rejections: 0,
            delta_entries_visited: 0,
            rows_visited: 0,
            stopped_early: false,
        };
        for descriptor in &self.manifest.pages {
            if !descriptor.may_match_exact_key(table, index, &selector_key) {
                report.delta_pages_skipped = report
                    .delta_pages_skipped
                    .checked_add(1)
                    .ok_or_else(|| admission("recovery skipped-page counter overflow"))?;
                continue;
            }
            let total_pages = report
                .base
                .pages_read
                .checked_add(report.delta_pages_read)
                .ok_or_else(|| admission("recovery read page counter overflow"))?;
            if total_pages >= limits.max_pages.get() {
                return Err(admission(format!(
                    "recovery index lookup exceeds page limit {}",
                    limits.max_pages
                )));
            }
            let encoded_len = usize::try_from(descriptor.encoded_len)
                .map_err(|_| corrupt("recovery delta encoded length overflows usize"))?;
            let total_bytes = report
                .base
                .bytes_read
                .checked_add(report.delta_bytes_read)
                .and_then(|bytes| bytes.checked_add(encoded_len))
                .ok_or_else(|| admission("recovery read byte counter overflow"))?;
            if total_bytes > limits.max_bytes.get() {
                return Err(admission(format!(
                    "recovery index lookup needs {total_bytes} bytes, exceeding byte limit {}",
                    limits.max_bytes
                )));
            }
            let file_bytes_read = report
                .base
                .file_bytes_read
                .checked_add(report.delta_file_bytes_read)
                .ok_or_else(|| admission("recovery file byte counter overflow"))?;
            let remaining_file_bytes = limits
                .max_file_bytes
                .checked_sub(file_bytes_read)
                .ok_or_else(|| admission("recovery file byte counter exceeds its limit"))?;
            let page_read = self.visit_page(descriptor, remaining_file_bytes, |entry| {
                report.delta_entries_visited = report
                    .delta_entries_visited
                    .checked_add(1)
                    .ok_or_else(|| admission("recovery delta entry counter overflow"))?;
                if entry.key.table != table
                    || entry.key.index != index
                    || entry.key.index_key != selector_key
                {
                    return Ok(());
                }
                let primary_key = super::demand_read::decode_relational_key(&entry.key.primary_key)
                    .inspect_err(|_| self.poison())?;
                match entry.value.kind {
                    RelationalIndexChangeKind::Delete => {
                        rows.remove(&primary_key);
                    }
                    RelationalIndexChangeKind::Insert => {
                        rows.insert(primary_key);
                    }
                }
                if rows.len() > limits.max_rows.get() {
                    return Err(admission(format!(
                        "recovery index lookup exceeds row limit {}",
                        limits.max_rows
                    )));
                }
                Ok(())
            })?;
            report.delta_pages_read += 1;
            report.delta_bytes_read += encoded_len;
            report.delta_cache_hits += usize::from(page_read.cache_hit);
            report.delta_cache_misses += usize::from(page_read.cache_miss);
            report.delta_cache_admission_rejections +=
                usize::from(page_read.cache_admission_rejected);
            if !page_read.cache_hit {
                report.delta_file_pages_read += 1;
                report.delta_file_bytes_read += encoded_len;
            }
        }
        for row in rows {
            report.rows_visited += 1;
            if !visit(&row) {
                report.stopped_early = true;
                break;
            }
        }
        Ok(report)
    }

    fn visit_page(
        &self,
        descriptor: &DeltaPageDescriptor,
        max_file_bytes: usize,
        visit: impl FnMut(DeltaEntry) -> Result<(), RelationalIndexShadowError>,
    ) -> Result<RecoveryDeltaPageRead, RelationalIndexShadowError> {
        let cache_key = SegmentCacheKey {
            store_id: self.store_id,
            manifest_generation: ManifestGeneration(self.manifest.delta_generation),
            segment_id: descriptor.ordinal as u64,
            content_digest: ContentDigest(descriptor.digest.crc32c.as_u64()),
            representation: RepresentationKind::RelationalIndexRecoveryDelta,
        };
        if let Some(cache) = &self.page_cache
            && let Some(encoded) = cache.get(&cache_key)
        {
            let result = decode_delta_page(
                &encoded,
                self.manifest.base_generation,
                self.manifest.delta_generation,
                self.manifest.base_commit_epoch,
                descriptor,
                self.config,
                visit,
            );
            if result.as_ref().is_err_and(should_poison) {
                self.poison();
            }
            return result.map(|()| RecoveryDeltaPageRead {
                cache_hit: true,
                cache_miss: false,
                cache_admission_rejected: false,
            });
        }
        let encoded_len = usize::try_from(descriptor.encoded_len)
            .map_err(|_| corrupt("recovery delta encoded length overflows usize"))?;
        if encoded_len > max_file_bytes {
            return Err(admission(format!(
                "recovery index lookup needs {encoded_len} file bytes, exceeding remaining file byte budget {max_file_bytes}"
            )));
        }
        let path = self
            .base
            .directory
            .join(relational_index_recovery_delta_file(
                self.manifest.base_generation,
                self.manifest.delta_generation,
                descriptor.ordinal,
            ));
        let result = read_bounded_file(
            &path,
            self.config.max_dirty_bytes.get() + DELTA_PAGE_HEADER_BYTES,
            "relational index recovery delta page",
        )
        .and_then(|encoded| {
            if encoded.len() as u64 != descriptor.encoded_len {
                return Err(corrupt(
                    "recovery delta page length disagrees with manifest",
                ));
            }
            let digest = digest_encoded_delta_page(&encoded)?;
            if digest != descriptor.digest {
                return Err(corrupt(
                    "recovery delta page digest disagrees with manifest",
                ));
            }
            decode_delta_page(
                &encoded,
                self.manifest.base_generation,
                self.manifest.delta_generation,
                self.manifest.base_commit_epoch,
                descriptor,
                self.config,
                visit,
            )?;
            let mut cache_admission_rejected = false;
            if let Some(cache) = &self.page_cache {
                match cache.insert(cache_key, encoded) {
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.error(),
                            SegmentCacheError::EntryTooLarge { .. }
                                | SegmentCacheError::PinnedCapacity { .. }
                        ) =>
                    {
                        cache_admission_rejected = true;
                    }
                    Err(error) => {
                        return Err(corrupt(format!(
                            "relational index recovery cache rejected immutable delta identity: {error}"
                        )));
                    }
                }
            }
            Ok(RecoveryDeltaPageRead {
                cache_hit: false,
                cache_miss: self.page_cache.is_some(),
                cache_admission_rejected,
            })
        });
        if result.as_ref().is_err_and(should_poison) {
            self.poison();
        }
        result
    }

    fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }
}

fn encoded_prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let position = prefix.iter().rposition(|byte| *byte != u8::MAX)?;
    let mut upper_bound = prefix[..=position].to_vec();
    upper_bound[position] = upper_bound[position].saturating_add(1);
    Some(upper_bound)
}

fn encode_change(
    change: RelationalIndexChange,
    epoch: u64,
) -> Result<(DeltaKey, DeltaValue, usize), RelationalIndexShadowError> {
    let key = DeltaKey {
        table: change.table,
        index: change.index,
        index_key: encode_relational_key(&change.index_key)?,
        primary_key: encode_relational_key(&change.primary_key)?,
    };
    let encoded_bytes = delta_entry_bytes(&key)?;
    Ok((
        key,
        DeltaValue {
            kind: change.kind,
            epoch,
        },
        encoded_bytes,
    ))
}

fn delta_entry_bytes(key: &DeltaKey) -> Result<usize, RelationalIndexShadowError> {
    DELTA_ENTRY_FIXED_BYTES
        .checked_add(key.table.len())
        .and_then(|bytes| bytes.checked_add(key.index.len()))
        .and_then(|bytes| bytes.checked_add(key.index_key.len()))
        .and_then(|bytes| bytes.checked_add(key.primary_key.len()))
        .ok_or_else(|| {
            RelationalIndexShadowError::Admission(
                "recovery delta entry length overflow".to_string(),
            )
        })
}

struct DeltaPageWrite<'a> {
    base_generation: u64,
    delta_generation: u64,
    base_commit_epoch: u64,
    ordinal: u32,
    start_epoch: u64,
    end_epoch: u64,
    entry_count: u32,
    payload_bytes: usize,
    entries: &'a BTreeMap<DeltaKey, DeltaValue>,
}

fn write_delta_page(
    path: &Path,
    page: DeltaPageWrite<'_>,
    config: RelationalIndexRecoveryConfig,
) -> Result<(u64, IntegrityDigest), RelationalIndexShadowError> {
    if page.payload_bytes > config.max_dirty_bytes.get()
        || page.entries.len() != page.entry_count as usize
    {
        return Err(RelationalIndexShadowError::Admission(format!(
            "recovery delta page payload contains {} bytes, exceeding limit {} or disagreeing with its entry count",
            page.payload_bytes,
            config.max_dirty_bytes
        )));
    }
    let payload_len = u64::try_from(page.payload_bytes).map_err(|_| {
        RelationalIndexShadowError::Admission(
            "recovery delta payload length does not fit u64".to_string(),
        )
    })?;
    let mut prefix = Vec::with_capacity(68);
    prefix.extend_from_slice(DELTA_PAGE_MAGIC);
    prefix.extend_from_slice(&DELTA_PAGE_FORMAT_VERSION.to_le_bytes());
    prefix.extend_from_slice(&0_u16.to_le_bytes());
    prefix.extend_from_slice(&page.base_generation.to_le_bytes());
    prefix.extend_from_slice(&page.delta_generation.to_le_bytes());
    prefix.extend_from_slice(&page.base_commit_epoch.to_le_bytes());
    prefix.extend_from_slice(&page.ordinal.to_le_bytes());
    prefix.extend_from_slice(&page.start_epoch.to_le_bytes());
    prefix.extend_from_slice(&page.end_epoch.to_le_bytes());
    prefix.extend_from_slice(&page.entry_count.to_le_bytes());
    prefix.extend_from_slice(&payload_len.to_le_bytes());
    debug_assert_eq!(prefix.len(), 68);
    let mut hasher = IntegrityHasher::new();
    hasher.update(&prefix);
    update_delta_entries_digest(&mut hasher, page.entries)?;
    let digest = hasher.finish();
    let mut header = prefix;
    header.extend_from_slice(&digest.crc32c.get().to_le_bytes());
    header.extend_from_slice(digest.sha256.as_bytes());
    debug_assert_eq!(header.len(), DELTA_PAGE_HEADER_BYTES);

    let mut file = File::create(path).map_err(|error| {
        RelationalIndexShadowError::Durability(format!(
            "create relational index recovery delta page: {error}"
        ))
    })?;
    let mut artifact_hasher = IntegrityHasher::new();
    file.write_all(&header).map_err(|error| {
        RelationalIndexShadowError::Durability(format!(
            "write relational index recovery delta header: {error}"
        ))
    })?;
    artifact_hasher.update(&header);
    write_delta_entries(&mut file, &mut artifact_hasher, page.entries)?;
    file.sync_all().map_err(|error| {
        RelationalIndexShadowError::Durability(format!(
            "sync relational index recovery delta page: {error}"
        ))
    })?;
    let encoded_len = (DELTA_PAGE_HEADER_BYTES as u64)
        .checked_add(payload_len)
        .ok_or_else(|| {
            RelationalIndexShadowError::Admission(
                "recovery delta encoded length overflow".to_string(),
            )
        })?;
    Ok((encoded_len, artifact_hasher.finish()))
}

fn update_delta_entries_digest(
    hasher: &mut IntegrityHasher,
    entries: &BTreeMap<DeltaKey, DeltaValue>,
) -> Result<(), RelationalIndexShadowError> {
    for (key, value) in entries {
        let kind = [delta_kind_tag(value.kind)];
        hasher.update(&kind);
        update_length_prefixed_digest(hasher, key.table.as_bytes())?;
        update_length_prefixed_digest(hasher, key.index.as_bytes())?;
        update_length_prefixed_digest(hasher, &key.index_key)?;
        update_length_prefixed_digest(hasher, &key.primary_key)?;
    }
    Ok(())
}

fn write_delta_entries(
    file: &mut File,
    hasher: &mut IntegrityHasher,
    entries: &BTreeMap<DeltaKey, DeltaValue>,
) -> Result<(), RelationalIndexShadowError> {
    for (key, value) in entries {
        let kind = [delta_kind_tag(value.kind)];
        write_hashed(file, hasher, &kind)?;
        write_length_prefixed(file, hasher, key.table.as_bytes())?;
        write_length_prefixed(file, hasher, key.index.as_bytes())?;
        write_length_prefixed(file, hasher, &key.index_key)?;
        write_length_prefixed(file, hasher, &key.primary_key)?;
    }
    Ok(())
}

fn update_length_prefixed_digest(
    hasher: &mut IntegrityHasher,
    bytes: &[u8],
) -> Result<(), RelationalIndexShadowError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        RelationalIndexShadowError::Admission(
            "recovery delta field length does not fit u32".to_string(),
        )
    })?;
    hasher.update(&len.to_le_bytes());
    hasher.update(bytes);
    Ok(())
}

fn write_length_prefixed(
    file: &mut File,
    hasher: &mut IntegrityHasher,
    bytes: &[u8],
) -> Result<(), RelationalIndexShadowError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        RelationalIndexShadowError::Admission(
            "recovery delta field length does not fit u32".to_string(),
        )
    })?;
    write_hashed(file, hasher, &len.to_le_bytes())?;
    write_hashed(file, hasher, bytes)
}

fn write_hashed(
    file: &mut File,
    hasher: &mut IntegrityHasher,
    bytes: &[u8],
) -> Result<(), RelationalIndexShadowError> {
    file.write_all(bytes).map_err(|error| {
        RelationalIndexShadowError::Durability(format!(
            "write relational index recovery delta entry: {error}"
        ))
    })?;
    hasher.update(bytes);
    Ok(())
}

const fn delta_kind_tag(kind: RelationalIndexChangeKind) -> u8 {
    match kind {
        RelationalIndexChangeKind::Delete => 0,
        RelationalIndexChangeKind::Insert => 1,
    }
}

fn decode_delta_page(
    encoded: &[u8],
    expected_base_generation: u64,
    expected_delta_generation: u64,
    expected_base_commit_epoch: u64,
    descriptor: &DeltaPageDescriptor,
    config: RelationalIndexRecoveryConfig,
    mut visit: impl FnMut(DeltaEntry) -> Result<(), RelationalIndexShadowError>,
) -> Result<(), RelationalIndexShadowError> {
    if encoded.len() < DELTA_PAGE_HEADER_BYTES || &encoded[..8] != DELTA_PAGE_MAGIC {
        return Err(corrupt("invalid relational index recovery delta header"));
    }
    let version = read_u16(&encoded[8..10]);
    let flags = read_u16(&encoded[10..12]);
    let base_generation = read_u64(&encoded[12..20]);
    let delta_generation = read_u64(&encoded[20..28]);
    let base_commit_epoch = read_u64(&encoded[28..36]);
    let ordinal = read_u32(&encoded[36..40]);
    let start_epoch = read_u64(&encoded[40..48]);
    let end_epoch = read_u64(&encoded[48..56]);
    let entry_count = read_u32(&encoded[56..60]);
    let payload_len = usize::try_from(read_u64(&encoded[60..68]))
        .map_err(|_| corrupt("recovery delta payload length overflows usize"))?;
    if version != DELTA_PAGE_FORMAT_VERSION
        || flags != 0
        || base_generation != expected_base_generation
        || delta_generation != expected_delta_generation
        || base_commit_epoch != expected_base_commit_epoch
        || ordinal != descriptor.ordinal
        || start_epoch != descriptor.start_epoch
        || end_epoch != descriptor.end_epoch
        || entry_count != descriptor.entry_count
    {
        return Err(corrupt("recovery delta page fence disagrees with manifest"));
    }
    if payload_len > config.max_dirty_bytes.get()
        || encoded.len() != DELTA_PAGE_HEADER_BYTES.saturating_add(payload_len)
    {
        return Err(corrupt("recovery delta page payload length mismatch"));
    }
    let payload = &encoded[DELTA_PAGE_HEADER_BYTES..];
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..68]);
    hasher.update(payload);
    let digest = hasher.finish();
    if digest.crc32c.get() != read_u32(&encoded[68..72])
        || digest.sha256.as_bytes() != &encoded[72..104]
    {
        return Err(corrupt("recovery delta page checksum mismatch"));
    }
    let mut offset = 0usize;
    let mut previous_key = None;
    for _ in 0..entry_count {
        let kind = match *payload
            .get(offset)
            .ok_or_else(|| corrupt("truncated recovery delta operation"))?
        {
            0 => RelationalIndexChangeKind::Delete,
            1 => RelationalIndexChangeKind::Insert,
            value => return Err(corrupt(format!("unknown recovery delta operation {value}"))),
        };
        offset += 1;
        let (table, next) = decode_bytes(payload, offset, 64 * 1024, "delta table")?;
        offset = next;
        let (index, next) = decode_bytes(payload, offset, 64 * 1024, "delta index")?;
        offset = next;
        let (index_key, next) = decode_bytes(
            payload,
            offset,
            config.max_dirty_bytes.get(),
            "delta index key",
        )?;
        offset = next;
        let (primary_key, next) = decode_bytes(
            payload,
            offset,
            config.max_dirty_bytes.get(),
            "delta primary key",
        )?;
        offset = next;
        let entry = DeltaEntry {
            key: DeltaKey {
                table: decode_utf8(table, "delta table")?,
                index: decode_utf8(index, "delta index")?,
                index_key: index_key.to_vec(),
                primary_key: primary_key.to_vec(),
            },
            value: DeltaValue {
                kind,
                epoch: end_epoch,
            },
        };
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous >= &entry.key)
        {
            return Err(corrupt("recovery delta entries are not strictly ordered"));
        }
        previous_key = Some(entry.key.clone());
        visit(entry)?;
    }
    if offset != payload.len() {
        return Err(corrupt("recovery delta entries contain trailing bytes"));
    }
    Ok(())
}

fn digest_encoded_delta_page(
    encoded: &[u8],
) -> Result<IntegrityDigest, RelationalIndexShadowError> {
    if encoded.len() < DELTA_PAGE_HEADER_BYTES {
        return Err(corrupt("truncated recovery delta page"));
    }
    let mut hasher = IntegrityHasher::new();
    hasher.update(encoded);
    Ok(hasher.finish())
}

fn validate_manifest(
    manifest: &RelationalIndexRecoveryManifest,
    config: RelationalIndexRecoveryConfig,
    corrupt_input: bool,
) -> Result<(), RelationalIndexShadowError> {
    let fail = |message: String| {
        if corrupt_input {
            RelationalIndexShadowError::Corrupt(message)
        } else {
            RelationalIndexShadowError::Admission(message)
        }
    };
    if manifest.base_generation == 0
        || manifest.delta_generation == 0
        || manifest.recovered_commit_epoch < manifest.base_commit_epoch
        || manifest.pages.len() > config.max_delta_pages.get()
    {
        return Err(fail(
            "invalid relational index recovery manifest fence".to_string(),
        ));
    }
    manifest
        .recovery_source
        .validate()
        .map_err(|reason| fail(reason.to_string()))?;
    if manifest.recovery_source.end_lsn - manifest.recovery_source.start_lsn
        != manifest.recovered_commit_epoch - manifest.base_commit_epoch
    {
        return Err(fail(
            "relational index recovery source length does not match its commit epoch range"
                .to_string(),
        ));
    }
    let mut previous_end = manifest.base_commit_epoch;
    for (position, page) in manifest.pages.iter().enumerate() {
        if page.ordinal as usize != position
            || page.entry_count == 0
            || page.start_epoch <= manifest.base_commit_epoch
            || page.start_epoch > page.end_epoch
            || page.start_epoch < previous_end
            || page.end_epoch > manifest.recovered_commit_epoch
            || page.encoded_len < DELTA_PAGE_HEADER_BYTES as u64
            || page.encoded_len > (DELTA_PAGE_HEADER_BYTES + config.max_dirty_bytes.get()) as u64
        {
            return Err(fail(format!(
                "invalid relational index recovery delta descriptor at ordinal {position}"
            )));
        }
        let mut previous_selector: Option<(&str, &str)> = None;
        for selector in &page.selectors {
            let identity = (selector.table.as_str(), selector.index.as_str());
            if selector.table.is_empty()
                || selector.index.is_empty()
                || selector.lower_index_key > selector.upper_index_key
                || previous_selector.is_some_and(|previous| previous >= identity)
            {
                return Err(fail(format!(
                    "invalid relational index recovery delta selector at page ordinal {position}"
                )));
            }
            previous_selector = Some(identity);
        }
        previous_end = page.end_epoch;
    }
    Ok(())
}

fn next_delta_generation() -> u64 {
    let clock = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let sequence = NEXT_DELTA_GENERATION.fetch_add(1, Ordering::Relaxed);
    let generation = clock.rotate_left(17) ^ sequence ^ ((std::process::id() as u64) << 32);
    generation.max(1)
}

fn write_synced(
    path: &Path,
    encoded: &[u8],
    context: &str,
) -> Result<(), RelationalIndexShadowError> {
    let mut file = File::create(path)
        .map_err(|error| RelationalIndexShadowError::Durability(format!("{context}: {error}")))?;
    file.write_all(encoded)
        .map_err(|error| RelationalIndexShadowError::Durability(format!("{context}: {error}")))?;
    file.sync_all()
        .map_err(|error| RelationalIndexShadowError::Durability(format!("{context}: {error}")))
}

fn admission(message: impl Into<String>) -> RelationalIndexShadowError {
    RelationalIndexShadowError::Admission(message.into())
}

fn corrupt(message: impl Into<String>) -> RelationalIndexShadowError {
    RelationalIndexShadowError::Corrupt(message.into())
}

fn should_poison(error: &RelationalIndexShadowError) -> bool {
    matches!(
        error,
        RelationalIndexShadowError::Corrupt(_) | RelationalIndexShadowError::Durability(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(selectors: Vec<DeltaPageSelector>) -> DeltaPageDescriptor {
        DeltaPageDescriptor {
            ordinal: 0,
            start_epoch: 1,
            end_epoch: 1,
            entry_count: 1,
            encoded_len: DELTA_PAGE_HEADER_BYTES as u64,
            digest: IntegrityDigest {
                crc32c: hawdb_integrity::Crc32c::new(1),
                sha256: Sha256Digest::from_bytes([0; SHA256_BYTES]),
            },
            selectors,
        }
    }

    #[test]
    fn page_selector_prunes_disjoint_keys_but_keeps_matching_prefixes() {
        let descriptor = descriptor(vec![DeltaPageSelector {
            table: "documents".to_string(),
            index: "documents_owner_idx".to_string(),
            lower_index_key: vec![0x10],
            upper_index_key: vec![0x1f],
        }]);

        assert!(descriptor.may_match_exact_key("documents", "documents_owner_idx", &[0x14]));
        assert!(!descriptor.may_match_exact_key("documents", "documents_owner_idx", &[0x20]));
        assert!(descriptor.may_match_range(
            "documents",
            "documents_owner_idx",
            &[0x10],
            None,
            RelationalIndexScanDirection::Forward,
        ));
        assert!(!descriptor.may_match_range(
            "documents",
            "documents_owner_idx",
            &[0x10],
            Some(&[0x1f]),
            RelationalIndexScanDirection::Forward,
        ));
    }

    #[test]
    fn manifest_round_trips_page_selectors_and_rejects_descriptor_tail() {
        let page = descriptor(vec![DeltaPageSelector {
            table: "documents".to_string(),
            index: "documents_owner_idx".to_string(),
            lower_index_key: vec![0x10],
            upper_index_key: vec![0x1f],
        }]);
        let manifest = RelationalIndexRecoveryManifest {
            base_generation: 1,
            delta_generation: 2,
            base_commit_epoch: 0,
            recovered_commit_epoch: 1,
            recovery_source: RelationalRecoverySourceIdentity::for_test(0, 1),
            pages: vec![page.clone()],
        };
        let config = RelationalIndexRecoveryConfig::default();

        let encoded = manifest.encode(config).expect("encode recovery manifest");
        assert_eq!(
            RelationalIndexRecoveryManifest::decode(&encoded, config)
                .expect("decode recovery manifest"),
            manifest
        );

        let mut descriptor = encode_delta_page_descriptor(&page).expect("encode descriptor");
        descriptor.push(0);
        assert!(matches!(
            decode_delta_page_descriptor(&descriptor, config),
            Err(RelationalIndexShadowError::Corrupt(_))
        ));
    }

    #[test]
    fn selector_budget_falls_back_to_an_unpruned_page() {
        let mut pages = vec![descriptor(vec![DeltaPageSelector {
            table: "documents".to_string(),
            index: "documents_owner_idx".to_string(),
            lower_index_key: vec![0x10],
            upper_index_key: vec![0x1f],
        }])];
        let static_bytes = DELTA_MANIFEST_HEADER_BYTES
            + DELTA_DESCRIPTOR_LENGTH_BYTES
            + DELTA_DESCRIPTOR_FIXED_BYTES;
        let config = RelationalIndexRecoveryConfig {
            max_manifest_bytes: NonZeroUsize::new(static_bytes).expect("static bytes are non-zero"),
            ..RelationalIndexRecoveryConfig::default()
        };

        bound_delta_page_selectors(&mut pages, config).expect("selector budget fallback");

        assert!(pages[0].selectors.is_empty());
        assert!(pages[0].may_match_exact_key("other", "other_idx", &[0xff]));
    }
}
