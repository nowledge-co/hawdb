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

//! Immutable, checksummed pages for graph block descriptors.
//!
//! This module owns only the durable page representation. Publication and
//! serving activation are deliberately separate so a new format cannot become
//! selected before its writer, demand reader, recovery, and qualification
//! contracts are complete.

use hawdb_integrity::{Crc32c, IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::fmt::{self, Display, Formatter};
use std::num::{NonZeroU64, NonZeroUsize};

const PAGE_MAGIC: &[u8; 8] = b"SKGDPG01";
const PAGE_VERSION: u16 = 1;
pub(crate) const GRAPH_DESCRIPTOR_PAGE_HEADER_BYTES: usize = 84;
pub(crate) const GRAPH_DESCRIPTOR_FIELD_HEADER_BYTES: usize = 6;
const PAGE_HEADER_BYTES: usize = GRAPH_DESCRIPTOR_PAGE_HEADER_BYTES;
const FIELD_HEADER_BYTES: usize = GRAPH_DESCRIPTOR_FIELD_HEADER_BYTES;
const INTERIOR_ENTRY_FIELD: u16 = 1;
const LEAF_ENTRY_FIELD: u16 = 2;

pub const DEFAULT_GRAPH_DESCRIPTOR_PAGE_BYTES: usize = 512 * 1024;
pub const DEFAULT_GRAPH_DESCRIPTOR_PAGE_ENTRIES: usize = 4096;
pub const DEFAULT_GRAPH_DESCRIPTOR_KEY_BYTES: usize = 16 * 1024;
pub const DEFAULT_GRAPH_DESCRIPTOR_VALUE_BYTES: usize = 448 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphDescriptorPageLimits {
    pub max_page_bytes: NonZeroUsize,
    pub max_entries: NonZeroUsize,
    pub max_key_bytes: NonZeroUsize,
    pub max_value_bytes: NonZeroUsize,
}

impl Default for GraphDescriptorPageLimits {
    fn default() -> Self {
        Self {
            max_page_bytes: NonZeroUsize::new(DEFAULT_GRAPH_DESCRIPTOR_PAGE_BYTES)
                .expect("default graph descriptor page limit is non-zero"),
            max_entries: NonZeroUsize::new(DEFAULT_GRAPH_DESCRIPTOR_PAGE_ENTRIES)
                .expect("default graph descriptor entry limit is non-zero"),
            max_key_bytes: NonZeroUsize::new(DEFAULT_GRAPH_DESCRIPTOR_KEY_BYTES)
                .expect("default graph descriptor key limit is non-zero"),
            max_value_bytes: NonZeroUsize::new(DEFAULT_GRAPH_DESCRIPTOR_VALUE_BYTES)
                .expect("default graph descriptor value limit is non-zero"),
        }
    }
}

impl GraphDescriptorPageLimits {
    pub const fn max_payload_bytes(self) -> Option<usize> {
        self.max_page_bytes.get().checked_sub(PAGE_HEADER_BYTES)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GraphDescriptorPageId(NonZeroU64);

impl GraphDescriptorPageId {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GraphDescriptorKind {
    CanonicalSegment,
    PropertySpill,
    CanonicalAdjacency,
    PropertyProjection,
}

impl GraphDescriptorKind {
    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::CanonicalSegment => 1,
            Self::PropertySpill => 2,
            Self::CanonicalAdjacency => 3,
            Self::PropertyProjection => 4,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Result<Self, GraphDescriptorPageError> {
        match tag {
            1 => Ok(Self::CanonicalSegment),
            2 => Ok(Self::PropertySpill),
            3 => Ok(Self::CanonicalAdjacency),
            4 => Ok(Self::PropertyProjection),
            _ => Err(GraphDescriptorPageError::Corrupt(format!(
                "unknown graph descriptor kind {tag}"
            ))),
        }
    }
}

/// An immutable physical page reference carried by an interior page or root.
///
/// `physical_generation` is intentionally distinct from the selecting root
/// generation. A future COW publisher may reuse an immutable page from an
/// older physical generation without changing its identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDescriptorPageRef {
    pub artifact_id: u64,
    pub physical_generation: u64,
    pub page_id: GraphDescriptorPageId,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_crc32c: Crc32c,
    pub content_sha256: Sha256Digest,
    pub lower_bound: Vec<u8>,
    pub upper_bound: Vec<u8>,
}

impl GraphDescriptorPageRef {
    pub fn validate(
        &self,
        limits: GraphDescriptorPageLimits,
    ) -> Result<(), GraphDescriptorPageError> {
        validate_page_ref(self, limits, ErrorClass::Admission)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDescriptorInteriorEntry {
    pub child: GraphDescriptorPageRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDescriptorLeafEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImmutableGraphDescriptorPageBody {
    Interior(Vec<GraphDescriptorInteriorEntry>),
    Leaf(Vec<GraphDescriptorLeafEntry>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmutableGraphDescriptorPage {
    pub kind: GraphDescriptorKind,
    pub physical_generation: u64,
    pub source_commit_epoch: u64,
    pub page_id: GraphDescriptorPageId,
    pub body: ImmutableGraphDescriptorPageBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphDescriptorPageError {
    Admission(String),
    Corrupt(String),
}

impl Display for GraphDescriptorPageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(
                    formatter,
                    "graph descriptor page admission failed: {message}"
                )
            }
            Self::Corrupt(message) => write!(formatter, "corrupt graph descriptor page: {message}"),
        }
    }
}

impl std::error::Error for GraphDescriptorPageError {}

impl ImmutableGraphDescriptorPage {
    pub fn encode(
        &self,
        limits: GraphDescriptorPageLimits,
    ) -> Result<Vec<u8>, GraphDescriptorPageError> {
        validate_page(self, limits, ErrorClass::Admission)?;
        let max_payload_bytes = limits.max_payload_bytes().ok_or_else(|| {
            GraphDescriptorPageError::Admission(format!(
                "page byte limit {} is smaller than the fixed header",
                limits.max_page_bytes
            ))
        })?;
        let (page_kind, entry_count, payload) = encode_body(&self.body, max_payload_bytes)?;
        let total_bytes = PAGE_HEADER_BYTES
            .checked_add(payload.len())
            .ok_or_else(|| {
                GraphDescriptorPageError::Admission("encoded page size overflow".to_string())
            })?;
        if total_bytes > limits.max_page_bytes.get() {
            return Err(GraphDescriptorPageError::Admission(format!(
                "encoded page contains {total_bytes} bytes, exceeding limit {}",
                limits.max_page_bytes
            )));
        }
        let entry_count = u32::try_from(entry_count).map_err(|_| {
            GraphDescriptorPageError::Admission("entry count does not fit u32".to_string())
        })?;
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            GraphDescriptorPageError::Admission("payload length does not fit u64".to_string())
        })?;
        let mut encoded = Vec::with_capacity(total_bytes);
        encoded.extend_from_slice(PAGE_MAGIC);
        encoded.extend_from_slice(&PAGE_VERSION.to_le_bytes());
        encoded.push(page_kind.tag());
        encoded.push(self.kind.tag());
        encoded.extend_from_slice(&self.physical_generation.to_le_bytes());
        encoded.extend_from_slice(&self.source_commit_epoch.to_le_bytes());
        encoded.extend_from_slice(&self.page_id.get().to_le_bytes());
        encoded.extend_from_slice(&entry_count.to_le_bytes());
        encoded.extend_from_slice(&payload_len.to_le_bytes());
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded);
        hasher.update(&payload);
        let digest = hasher.finish();
        encoded.extend_from_slice(&digest.crc32c.get().to_le_bytes());
        encoded.extend_from_slice(digest.sha256.as_bytes());
        debug_assert_eq!(encoded.len(), PAGE_HEADER_BYTES);
        encoded.extend_from_slice(&payload);
        Ok(encoded)
    }

    /// Encodes the page and derives the exact immutable reference a parent
    /// page or root must persist.
    pub fn encode_with_ref(
        &self,
        artifact_id: u64,
        offset: u64,
        limits: GraphDescriptorPageLimits,
    ) -> Result<(Vec<u8>, GraphDescriptorPageRef), GraphDescriptorPageError> {
        if artifact_id == 0 {
            return Err(GraphDescriptorPageError::Admission(
                "referenced artifact id must be non-zero".to_string(),
            ));
        }
        let encoded = self.encode(limits)?;
        let (lower_bound, upper_bound) = self.key_bounds();
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| {
            GraphDescriptorPageError::Admission("encoded page length does not fit u64".to_string())
        })?;
        let length =
            NonZeroU64::new(encoded_len).expect("an encoded graph descriptor page is non-empty");
        offset.checked_add(length.get()).ok_or_else(|| {
            GraphDescriptorPageError::Admission("page file range overflows u64".to_string())
        })?;
        let reference = GraphDescriptorPageRef {
            artifact_id,
            physical_generation: self.physical_generation,
            page_id: self.page_id,
            offset,
            length,
            content_crc32c: Crc32c::new(read_u32(&encoded[48..52])),
            content_sha256: Sha256Digest::from_bytes(
                encoded[52..84]
                    .try_into()
                    .expect("encoded page digest has a fixed length"),
            ),
            lower_bound: lower_bound.to_vec(),
            upper_bound: upper_bound.to_vec(),
        };
        validate_page_ref(&reference, limits, ErrorClass::Admission)?;
        Ok((encoded, reference))
    }

    pub fn decode(
        encoded: &[u8],
        limits: GraphDescriptorPageLimits,
    ) -> Result<Self, GraphDescriptorPageError> {
        if encoded.len() > limits.max_page_bytes.get() {
            return Err(GraphDescriptorPageError::Admission(format!(
                "encoded page contains {} bytes, exceeding limit {}",
                encoded.len(),
                limits.max_page_bytes
            )));
        }
        if encoded.len() < PAGE_HEADER_BYTES || &encoded[..8] != PAGE_MAGIC {
            return Err(GraphDescriptorPageError::Corrupt(
                "invalid page header".to_string(),
            ));
        }
        let version = read_u16(&encoded[8..10]);
        if version != PAGE_VERSION {
            return Err(GraphDescriptorPageError::Corrupt(format!(
                "unsupported page version {version}"
            )));
        }
        let page_kind = GraphDescriptorPageKind::from_tag(encoded[10])?;
        let kind = GraphDescriptorKind::from_tag(encoded[11])?;
        let physical_generation = read_u64(&encoded[12..20]);
        let source_commit_epoch = read_u64(&encoded[20..28]);
        let page_id = page_id(read_u64(&encoded[28..36]), "page id")?;
        let entry_count = read_u32(&encoded[36..40]) as usize;
        if entry_count > limits.max_entries.get() {
            return Err(GraphDescriptorPageError::Admission(format!(
                "page declares {entry_count} entries, exceeding limit {}",
                limits.max_entries
            )));
        }
        let payload_len = usize::try_from(read_u64(&encoded[40..48])).map_err(|_| {
            GraphDescriptorPageError::Corrupt("payload length overflows usize".to_string())
        })?;
        let expected_len = PAGE_HEADER_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| GraphDescriptorPageError::Corrupt("page length overflow".to_string()))?;
        if encoded.len() != expected_len {
            return Err(GraphDescriptorPageError::Corrupt(format!(
                "page length mismatch: expected {expected_len}, got {}",
                encoded.len()
            )));
        }
        let payload = &encoded[PAGE_HEADER_BYTES..];
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded[..48]);
        hasher.update(payload);
        let actual = hasher.finish();
        if actual.crc32c.get() != read_u32(&encoded[48..52])
            || actual.sha256.as_bytes() != &encoded[52..52 + SHA256_BYTES]
        {
            return Err(GraphDescriptorPageError::Corrupt(
                "page checksum mismatch".to_string(),
            ));
        }
        let body = decode_body(page_kind, entry_count, payload, limits)?;
        let page = Self {
            kind,
            physical_generation,
            source_commit_epoch,
            page_id,
            body,
        };
        validate_page(&page, limits, ErrorClass::Corrupt)?;
        Ok(page)
    }

    /// Verifies an outer immutable reference and the page's own checksum from
    /// the same bounded byte image before returning decoded state.
    pub fn decode_bound(
        reference: &GraphDescriptorPageRef,
        expected_kind: GraphDescriptorKind,
        expected_source_commit_epoch: u64,
        encoded: &[u8],
        limits: GraphDescriptorPageLimits,
    ) -> Result<Self, GraphDescriptorPageError> {
        validate_page_ref(reference, limits, ErrorClass::Corrupt)?;
        if u64::try_from(encoded.len()).ok() != Some(reference.length.get()) {
            return Err(GraphDescriptorPageError::Corrupt(format!(
                "bound page length mismatch: expected {}, got {}",
                reference.length,
                encoded.len()
            )));
        }
        if encoded.len() < PAGE_HEADER_BYTES {
            return Err(GraphDescriptorPageError::Corrupt(
                "bound page is shorter than its header".to_string(),
            ));
        }
        if read_u32(&encoded[48..52]) != reference.content_crc32c.get()
            || &encoded[52..84] != reference.content_sha256.as_bytes()
        {
            return Err(GraphDescriptorPageError::Corrupt(
                "bound page digest does not match its reference".to_string(),
            ));
        }
        let page = Self::decode(encoded, limits)?;
        if page.kind != expected_kind
            || page.physical_generation != reference.physical_generation
            || page.source_commit_epoch != expected_source_commit_epoch
            || page.page_id != reference.page_id
        {
            return Err(GraphDescriptorPageError::Corrupt(
                "bound page identity does not match its reference".to_string(),
            ));
        }
        let (lower_bound, upper_bound) = page.key_bounds();
        if lower_bound != reference.lower_bound || upper_bound != reference.upper_bound {
            return Err(GraphDescriptorPageError::Corrupt(
                "bound page key range does not match its reference".to_string(),
            ));
        }
        Ok(page)
    }

    fn key_bounds(&self) -> (&[u8], &[u8]) {
        match &self.body {
            ImmutableGraphDescriptorPageBody::Interior(entries) => (
                &entries
                    .first()
                    .expect("validated page is non-empty")
                    .child
                    .lower_bound,
                &entries
                    .last()
                    .expect("validated page is non-empty")
                    .child
                    .upper_bound,
            ),
            ImmutableGraphDescriptorPageBody::Leaf(entries) => (
                &entries.first().expect("validated page is non-empty").key,
                &entries.last().expect("validated page is non-empty").key,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphDescriptorPageKind {
    Interior,
    Leaf,
}

impl GraphDescriptorPageKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Interior => 1,
            Self::Leaf => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, GraphDescriptorPageError> {
        match tag {
            1 => Ok(Self::Interior),
            2 => Ok(Self::Leaf),
            _ => Err(GraphDescriptorPageError::Corrupt(format!(
                "unknown graph descriptor page kind {tag}"
            ))),
        }
    }
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

fn invalid(error_class: ErrorClass, message: impl Into<String>) -> GraphDescriptorPageError {
    match error_class {
        ErrorClass::Admission => GraphDescriptorPageError::Admission(message.into()),
        ErrorClass::Corrupt => GraphDescriptorPageError::Corrupt(message.into()),
    }
}

fn validate_page(
    page: &ImmutableGraphDescriptorPage,
    limits: GraphDescriptorPageLimits,
    error_class: ErrorClass,
) -> Result<(), GraphDescriptorPageError> {
    if page.physical_generation == 0 {
        return Err(invalid(error_class, "physical generation must be non-zero"));
    }
    match &page.body {
        ImmutableGraphDescriptorPageBody::Interior(entries) => {
            validate_entry_count(entries.len(), limits, error_class)?;
            let mut previous_upper: Option<&[u8]> = None;
            for entry in entries {
                validate_page_ref(&entry.child, limits, error_class)?;
                if entry.child.physical_generation > page.physical_generation {
                    return Err(invalid(
                        error_class,
                        "interior page must not reference a future physical generation",
                    ));
                }
                if previous_upper.is_some_and(|upper| upper >= entry.child.lower_bound.as_slice()) {
                    return Err(invalid(
                        error_class,
                        "interior child key ranges must be strictly ordered and disjoint",
                    ));
                }
                previous_upper = Some(&entry.child.upper_bound);
            }
        }
        ImmutableGraphDescriptorPageBody::Leaf(entries) => {
            validate_entry_count(entries.len(), limits, error_class)?;
            let mut previous: Option<&[u8]> = None;
            for entry in entries {
                validate_key(&entry.key, limits, error_class, "leaf key")?;
                if previous.is_some_and(|previous| previous >= entry.key.as_slice()) {
                    return Err(invalid(
                        error_class,
                        "leaf keys must be strictly increasing",
                    ));
                }
                if entry.value.is_empty() {
                    return Err(invalid(error_class, "descriptor value must be non-empty"));
                }
                if entry.value.len() > limits.max_value_bytes.get() {
                    return Err(invalid(
                        error_class,
                        format!(
                            "descriptor value contains {} bytes, exceeding limit {}",
                            entry.value.len(),
                            limits.max_value_bytes
                        ),
                    ));
                }
                previous = Some(&entry.key);
            }
        }
    }
    Ok(())
}

fn validate_entry_count(
    count: usize,
    limits: GraphDescriptorPageLimits,
    error_class: ErrorClass,
) -> Result<(), GraphDescriptorPageError> {
    if count == 0 {
        return Err(invalid(error_class, "descriptor page must not be empty"));
    }
    if count > limits.max_entries.get() {
        return Err(invalid(
            error_class,
            format!(
                "descriptor page contains {count} entries, exceeding limit {}",
                limits.max_entries
            ),
        ));
    }
    Ok(())
}

fn validate_page_ref(
    reference: &GraphDescriptorPageRef,
    limits: GraphDescriptorPageLimits,
    error_class: ErrorClass,
) -> Result<(), GraphDescriptorPageError> {
    if reference.artifact_id == 0 {
        return Err(invalid(
            error_class,
            "referenced artifact id must be non-zero",
        ));
    }
    if reference.physical_generation == 0 {
        return Err(invalid(
            error_class,
            "referenced physical generation must be non-zero",
        ));
    }
    if reference.length.get() < PAGE_HEADER_BYTES as u64 {
        return Err(invalid(
            error_class,
            format!(
                "referenced page length {} is smaller than the fixed header {PAGE_HEADER_BYTES}",
                reference.length
            ),
        ));
    }
    let referenced_page_bytes = usize::try_from(reference.length.get()).map_err(|_| {
        invalid(
            error_class,
            "referenced page length does not fit this platform",
        )
    })?;
    if referenced_page_bytes > limits.max_page_bytes.get() {
        return Err(invalid(
            error_class,
            format!(
                "referenced page length {} exceeds limit {}",
                reference.length, limits.max_page_bytes
            ),
        ));
    }
    reference
        .offset
        .checked_add(reference.length.get())
        .ok_or_else(|| invalid(error_class, "referenced page range overflows u64"))?;
    validate_key(
        &reference.lower_bound,
        limits,
        error_class,
        "reference lower bound",
    )?;
    validate_key(
        &reference.upper_bound,
        limits,
        error_class,
        "reference upper bound",
    )?;
    if reference.lower_bound > reference.upper_bound {
        return Err(invalid(
            error_class,
            "referenced page lower bound exceeds its upper bound",
        ));
    }
    Ok(())
}

fn validate_key(
    key: &[u8],
    limits: GraphDescriptorPageLimits,
    error_class: ErrorClass,
    context: &str,
) -> Result<(), GraphDescriptorPageError> {
    if key.is_empty() {
        return Err(invalid(error_class, format!("{context} must be non-empty")));
    }
    if key.len() > limits.max_key_bytes.get() {
        return Err(invalid(
            error_class,
            format!(
                "{context} contains {} bytes, exceeding limit {}",
                key.len(),
                limits.max_key_bytes
            ),
        ));
    }
    Ok(())
}

fn encode_body(
    body: &ImmutableGraphDescriptorPageBody,
    max_payload_bytes: usize,
) -> Result<(GraphDescriptorPageKind, usize, Vec<u8>), GraphDescriptorPageError> {
    let mut payload = Vec::new();
    match body {
        ImmutableGraphDescriptorPageBody::Interior(entries) => {
            for entry in entries {
                let encoded = encode_page_ref(&entry.child)?;
                encode_field(
                    &mut payload,
                    INTERIOR_ENTRY_FIELD,
                    &encoded,
                    max_payload_bytes,
                )?;
            }
            Ok((GraphDescriptorPageKind::Interior, entries.len(), payload))
        }
        ImmutableGraphDescriptorPageBody::Leaf(entries) => {
            for entry in entries {
                let mut encoded = Vec::new();
                encode_bytes(&mut encoded, &entry.key)?;
                encode_bytes(&mut encoded, &entry.value)?;
                encode_field(&mut payload, LEAF_ENTRY_FIELD, &encoded, max_payload_bytes)?;
            }
            Ok((GraphDescriptorPageKind::Leaf, entries.len(), payload))
        }
    }
}

fn decode_body(
    page_kind: GraphDescriptorPageKind,
    entry_count: usize,
    payload: &[u8],
    limits: GraphDescriptorPageLimits,
) -> Result<ImmutableGraphDescriptorPageBody, GraphDescriptorPageError> {
    match page_kind {
        GraphDescriptorPageKind::Interior => {
            let mut entries = Vec::with_capacity(entry_count);
            for field in Fields::new(payload) {
                let (tag, bytes) = field?;
                if tag != INTERIOR_ENTRY_FIELD {
                    continue;
                }
                entries.push(GraphDescriptorInteriorEntry {
                    child: decode_page_ref(bytes, limits)?,
                });
                ensure_not_overdeclared(entry_count, entries.len())?;
            }
            ensure_entry_count(entry_count, entries.len())?;
            Ok(ImmutableGraphDescriptorPageBody::Interior(entries))
        }
        GraphDescriptorPageKind::Leaf => {
            let mut entries = Vec::with_capacity(entry_count);
            for field in Fields::new(payload) {
                let (tag, bytes) = field?;
                if tag != LEAF_ENTRY_FIELD {
                    continue;
                }
                let (key, offset) = decode_bytes(bytes, 0, limits.max_key_bytes.get(), "leaf key")?;
                let (value, offset) = decode_bytes(
                    bytes,
                    offset,
                    limits.max_value_bytes.get(),
                    "descriptor value",
                )?;
                if offset != bytes.len() {
                    return Err(GraphDescriptorPageError::Corrupt(
                        "leaf entry contains trailing bytes".to_string(),
                    ));
                }
                entries.push(GraphDescriptorLeafEntry {
                    key: key.to_vec(),
                    value: value.to_vec(),
                });
                ensure_not_overdeclared(entry_count, entries.len())?;
            }
            ensure_entry_count(entry_count, entries.len())?;
            Ok(ImmutableGraphDescriptorPageBody::Leaf(entries))
        }
    }
}

pub(crate) fn encode_page_ref(
    reference: &GraphDescriptorPageRef,
) -> Result<Vec<u8>, GraphDescriptorPageError> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&reference.artifact_id.to_le_bytes());
    encoded.extend_from_slice(&reference.physical_generation.to_le_bytes());
    encoded.extend_from_slice(&reference.page_id.get().to_le_bytes());
    encoded.extend_from_slice(&reference.offset.to_le_bytes());
    encoded.extend_from_slice(&reference.length.get().to_le_bytes());
    encoded.extend_from_slice(&reference.content_crc32c.get().to_le_bytes());
    encoded.extend_from_slice(reference.content_sha256.as_bytes());
    encode_bytes(&mut encoded, &reference.lower_bound)?;
    encode_bytes(&mut encoded, &reference.upper_bound)?;
    Ok(encoded)
}

pub(crate) fn decode_page_ref(
    encoded: &[u8],
    limits: GraphDescriptorPageLimits,
) -> Result<GraphDescriptorPageRef, GraphDescriptorPageError> {
    const FIXED_BYTES: usize = 76;
    if encoded.len() < FIXED_BYTES {
        return Err(GraphDescriptorPageError::Corrupt(
            "truncated graph descriptor page reference".to_string(),
        ));
    }
    let artifact_id = read_u64(&encoded[0..8]);
    let physical_generation = read_u64(&encoded[8..16]);
    let page_id = page_id(read_u64(&encoded[16..24]), "referenced page id")?;
    let offset = read_u64(&encoded[24..32]);
    let length = NonZeroU64::new(read_u64(&encoded[32..40])).ok_or_else(|| {
        GraphDescriptorPageError::Corrupt("referenced page length must be non-zero".to_string())
    })?;
    let content_crc32c = Crc32c::new(read_u32(&encoded[40..44]));
    let content_sha256 = Sha256Digest::from_bytes(
        encoded[44..76]
            .try_into()
            .expect("page reference digest has a fixed length"),
    );
    let (lower_bound, offset_after_lower) = decode_bytes(
        encoded,
        FIXED_BYTES,
        limits.max_key_bytes.get(),
        "reference lower bound",
    )?;
    let (upper_bound, end) = decode_bytes(
        encoded,
        offset_after_lower,
        limits.max_key_bytes.get(),
        "reference upper bound",
    )?;
    if end != encoded.len() {
        return Err(GraphDescriptorPageError::Corrupt(
            "page reference contains trailing bytes".to_string(),
        ));
    }
    let reference = GraphDescriptorPageRef {
        artifact_id,
        physical_generation,
        page_id,
        offset,
        length,
        content_crc32c,
        content_sha256,
        lower_bound: lower_bound.to_vec(),
        upper_bound: upper_bound.to_vec(),
    };
    validate_page_ref(&reference, limits, ErrorClass::Corrupt)?;
    Ok(reference)
}

fn encode_field(
    output: &mut Vec<u8>,
    tag: u16,
    bytes: &[u8],
    max_payload_bytes: usize,
) -> Result<(), GraphDescriptorPageError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        GraphDescriptorPageError::Admission("field length does not fit u32".to_string())
    })?;
    let next_len = output
        .len()
        .checked_add(FIELD_HEADER_BYTES)
        .and_then(|len| len.checked_add(bytes.len()))
        .ok_or_else(|| {
            GraphDescriptorPageError::Admission("encoded field size overflow".to_string())
        })?;
    if next_len > max_payload_bytes {
        return Err(GraphDescriptorPageError::Admission(format!(
            "graph descriptor payload would contain {next_len} bytes, exceeding limit {max_payload_bytes}"
        )));
    }
    output.extend_from_slice(&tag.to_le_bytes());
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), GraphDescriptorPageError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        GraphDescriptorPageError::Admission("byte string length does not fit u32".to_string())
    })?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn decode_bytes<'a>(
    bytes: &'a [u8],
    offset: usize,
    max_bytes: usize,
    context: &str,
) -> Result<(&'a [u8], usize), GraphDescriptorPageError> {
    let len_bytes = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| GraphDescriptorPageError::Corrupt(format!("truncated {context} length")))?;
    let len = read_u32(len_bytes) as usize;
    if len > max_bytes {
        return Err(GraphDescriptorPageError::Admission(format!(
            "{context} contains {len} bytes, exceeding limit {max_bytes}"
        )));
    }
    let start = offset + 4;
    let end = start
        .checked_add(len)
        .ok_or_else(|| GraphDescriptorPageError::Corrupt(format!("{context} length overflow")))?;
    let value = bytes
        .get(start..end)
        .ok_or_else(|| GraphDescriptorPageError::Corrupt(format!("truncated {context}")))?;
    Ok((value, end))
}

struct Fields<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> Fields<'a> {
    const fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }
}

impl<'a> Iterator for Fields<'a> {
    type Item = Result<(u16, &'a [u8]), GraphDescriptorPageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.payload.len() {
            return None;
        }
        let header = match self
            .payload
            .get(self.offset..self.offset + FIELD_HEADER_BYTES)
        {
            Some(header) => header,
            None => {
                self.offset = self.payload.len();
                return Some(Err(GraphDescriptorPageError::Corrupt(
                    "truncated field header".to_string(),
                )));
            }
        };
        let tag = read_u16(&header[..2]);
        let len = read_u32(&header[2..6]) as usize;
        let start = self.offset + FIELD_HEADER_BYTES;
        let end = match start.checked_add(len) {
            Some(end) => end,
            None => {
                self.offset = self.payload.len();
                return Some(Err(GraphDescriptorPageError::Corrupt(
                    "field length overflow".to_string(),
                )));
            }
        };
        let value = match self.payload.get(start..end) {
            Some(value) => value,
            None => {
                self.offset = self.payload.len();
                return Some(Err(GraphDescriptorPageError::Corrupt(
                    "truncated field payload".to_string(),
                )));
            }
        };
        self.offset = end;
        Some(Ok((tag, value)))
    }
}

fn ensure_not_overdeclared(
    declared: usize,
    decoded: usize,
) -> Result<(), GraphDescriptorPageError> {
    if decoded > declared {
        return Err(GraphDescriptorPageError::Corrupt(
            "page contains more entries than declared".to_string(),
        ));
    }
    Ok(())
}

fn ensure_entry_count(expected: usize, actual: usize) -> Result<(), GraphDescriptorPageError> {
    if expected != actual {
        return Err(GraphDescriptorPageError::Corrupt(format!(
            "page entry count mismatch: declared {expected}, decoded {actual}"
        )));
    }
    Ok(())
}

fn page_id(value: u64, context: &str) -> Result<GraphDescriptorPageId, GraphDescriptorPageError> {
    NonZeroU64::new(value)
        .map(GraphDescriptorPageId::new)
        .ok_or_else(|| GraphDescriptorPageError::Corrupt(format!("{context} must be non-zero")))
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
mod tests {
    use super::*;

    fn page_id(value: u64) -> GraphDescriptorPageId {
        GraphDescriptorPageId::new(NonZeroU64::new(value).unwrap())
    }

    fn leaf_page() -> ImmutableGraphDescriptorPage {
        ImmutableGraphDescriptorPage {
            kind: GraphDescriptorKind::CanonicalAdjacency,
            physical_generation: 7,
            source_commit_epoch: 11,
            page_id: page_id(3),
            body: ImmutableGraphDescriptorPageBody::Leaf(vec![
                GraphDescriptorLeafEntry {
                    key: b"a".to_vec(),
                    value: b"descriptor-a".to_vec(),
                },
                GraphDescriptorLeafEntry {
                    key: b"z".to_vec(),
                    value: b"descriptor-z".to_vec(),
                },
            ]),
        }
    }

    #[test]
    fn leaf_page_round_trips_with_exact_bound_reference() {
        let limits = GraphDescriptorPageLimits::default();
        let page = leaf_page();
        let (encoded, reference) = page.encode_with_ref(91, 4096, limits).unwrap();
        assert_eq!(reference.artifact_id, 91);
        assert_eq!(reference.offset, 4096);
        assert_eq!(reference.lower_bound, b"a");
        assert_eq!(reference.upper_bound, b"z");
        assert_eq!(
            ImmutableGraphDescriptorPage::decode_bound(
                &reference,
                GraphDescriptorKind::CanonicalAdjacency,
                11,
                &encoded,
                limits,
            )
            .unwrap(),
            page
        );
    }

    #[test]
    fn interior_page_round_trips_cross_generation_children() {
        let limits = GraphDescriptorPageLimits::default();
        let (_, first) = leaf_page().encode_with_ref(11, 0, limits).unwrap();
        let mut second_page = leaf_page();
        second_page.physical_generation = 8;
        second_page.page_id = page_id(4);
        if let ImmutableGraphDescriptorPageBody::Leaf(entries) = &mut second_page.body {
            entries[0].key = b"za".to_vec();
            entries[1].key = b"zz".to_vec();
        }
        let (_, second) = second_page.encode_with_ref(12, 0, limits).unwrap();
        let page = ImmutableGraphDescriptorPage {
            kind: GraphDescriptorKind::CanonicalAdjacency,
            physical_generation: 9,
            source_commit_epoch: 11,
            page_id: page_id(5),
            body: ImmutableGraphDescriptorPageBody::Interior(vec![
                GraphDescriptorInteriorEntry { child: first },
                GraphDescriptorInteriorEntry { child: second },
            ]),
        };
        let encoded = page.encode(limits).unwrap();
        assert_eq!(
            ImmutableGraphDescriptorPage::decode(&encoded, limits).unwrap(),
            page
        );
    }

    #[test]
    fn encoder_rejects_values_that_cannot_fit_the_declared_limit() {
        let mut page = leaf_page();
        let limits = GraphDescriptorPageLimits {
            max_value_bytes: NonZeroUsize::new(4).unwrap(),
            ..GraphDescriptorPageLimits::default()
        };
        let error = page.encode(limits).unwrap_err();
        assert!(matches!(error, GraphDescriptorPageError::Admission(_)));

        if let ImmutableGraphDescriptorPageBody::Leaf(entries) = &mut page.body {
            entries[0].value = vec![1; 4];
            entries[1].value = vec![2; 4];
        }
        assert!(page.encode(limits).is_ok());
    }

    #[test]
    fn parent_reference_requires_a_non_zero_artifact_identity() {
        let error = leaf_page()
            .encode_with_ref(0, 0, GraphDescriptorPageLimits::default())
            .unwrap_err();
        assert!(matches!(error, GraphDescriptorPageError::Admission(_)));
    }

    #[test]
    fn decoder_rejects_bit_flips_and_truncation() {
        let limits = GraphDescriptorPageLimits::default();
        let mut encoded = leaf_page().encode(limits).unwrap();
        encoded[PAGE_HEADER_BYTES + 8] ^= 0x40;
        assert!(matches!(
            ImmutableGraphDescriptorPage::decode(&encoded, limits),
            Err(GraphDescriptorPageError::Corrupt(_))
        ));

        let encoded = leaf_page().encode(limits).unwrap();
        assert!(matches!(
            ImmutableGraphDescriptorPage::decode(&encoded[..encoded.len() - 1], limits),
            Err(GraphDescriptorPageError::Corrupt(_))
        ));
    }

    #[test]
    fn bound_decode_rejects_identity_digest_and_range_drift() {
        let limits = GraphDescriptorPageLimits::default();
        let page = leaf_page();
        let (encoded, reference) = page.encode_with_ref(5, 0, limits).unwrap();

        let mut wrong_digest = reference.clone();
        wrong_digest.content_crc32c = Crc32c::new(reference.content_crc32c.get() ^ 1);
        assert!(matches!(
            ImmutableGraphDescriptorPage::decode_bound(
                &wrong_digest,
                page.kind,
                page.source_commit_epoch,
                &encoded,
                limits,
            ),
            Err(GraphDescriptorPageError::Corrupt(_))
        ));

        let mut wrong_range = reference.clone();
        wrong_range.upper_bound = b"zz".to_vec();
        assert!(matches!(
            ImmutableGraphDescriptorPage::decode_bound(
                &wrong_range,
                page.kind,
                page.source_commit_epoch,
                &encoded,
                limits,
            ),
            Err(GraphDescriptorPageError::Corrupt(_))
        ));

        assert!(matches!(
            ImmutableGraphDescriptorPage::decode_bound(
                &reference,
                GraphDescriptorKind::PropertySpill,
                page.source_commit_epoch,
                &encoded,
                limits,
            ),
            Err(GraphDescriptorPageError::Corrupt(_))
        ));
    }

    #[test]
    fn page_ranges_must_be_strictly_ordered() {
        let limits = GraphDescriptorPageLimits::default();
        let (_, child) = leaf_page().encode_with_ref(1, 0, limits).unwrap();
        let page = ImmutableGraphDescriptorPage {
            kind: GraphDescriptorKind::CanonicalAdjacency,
            physical_generation: 8,
            source_commit_epoch: 11,
            page_id: page_id(9),
            body: ImmutableGraphDescriptorPageBody::Interior(vec![
                GraphDescriptorInteriorEntry {
                    child: child.clone(),
                },
                GraphDescriptorInteriorEntry { child },
            ]),
        };
        assert!(matches!(
            page.encode(limits),
            Err(GraphDescriptorPageError::Admission(_))
        ));
    }

    #[test]
    fn interior_pages_reject_future_or_impossible_child_references() {
        let limits = GraphDescriptorPageLimits::default();
        let (_, mut child) = leaf_page().encode_with_ref(1, 0, limits).unwrap();
        child.physical_generation = 9;
        let mut page = ImmutableGraphDescriptorPage {
            kind: GraphDescriptorKind::CanonicalAdjacency,
            physical_generation: 8,
            source_commit_epoch: 11,
            page_id: page_id(9),
            body: ImmutableGraphDescriptorPageBody::Interior(vec![GraphDescriptorInteriorEntry {
                child,
            }]),
        };
        assert!(matches!(
            page.encode(limits),
            Err(GraphDescriptorPageError::Admission(_))
        ));

        if let ImmutableGraphDescriptorPageBody::Interior(entries) = &mut page.body {
            entries[0].child.physical_generation = 7;
            entries[0].child.length = NonZeroU64::new((PAGE_HEADER_BYTES - 1) as u64).unwrap();
        }
        assert!(matches!(
            page.encode(limits),
            Err(GraphDescriptorPageError::Admission(_))
        ));
    }

    #[test]
    fn large_descriptor_values_fit_without_changing_the_page_ceiling() {
        let limits = GraphDescriptorPageLimits::default();
        let page = ImmutableGraphDescriptorPage {
            kind: GraphDescriptorKind::CanonicalSegment,
            physical_generation: 1,
            source_commit_epoch: 1,
            page_id: page_id(1),
            body: ImmutableGraphDescriptorPageBody::Leaf(vec![GraphDescriptorLeafEntry {
                key: b"canonical-segment".to_vec(),
                value: vec![0xab; 384 * 1024],
            }]),
        };
        let encoded = page.encode(limits).unwrap();
        assert!(encoded.len() <= DEFAULT_GRAPH_DESCRIPTOR_PAGE_BYTES);
        assert_eq!(
            ImmutableGraphDescriptorPage::decode(&encoded, limits).unwrap(),
            page
        );
    }
}
