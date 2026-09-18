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

use hawdb_integrity::{IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};

const PAGE_MAGIC: &[u8; 8] = b"SKINIDX1";
const PAGE_VERSION: u16 = 1;
const PAGE_HEADER_BYTES: usize = 84;

#[cfg(test)]
mod verified_cache_tests;
const FIELD_HEADER_BYTES: usize = 6;

const ROOT_IDENTITY_FIELD: u16 = 1;
const ROOT_SCHEMA_DIGEST_FIELD: u16 = 2;
const ROOT_CHILD_FIELD: u16 = 3;
const ROOT_HEIGHT_FIELD: u16 = 4;
const INTERIOR_ENTRY_FIELD: u16 = 10;
const LEAF_ENTRY_FIELD: u16 = 11;
const POSTING_ENTRY_FIELD: u16 = 12;
const POSTING_NEXT_FIELD: u16 = 13;

pub const DEFAULT_IMMUTABLE_INDEX_PAGE_BYTES: usize = 64 * 1024;
pub const DEFAULT_IMMUTABLE_INDEX_PAGE_ENTRIES: usize = 4096;
pub const DEFAULT_IMMUTABLE_INDEX_KEY_BYTES: usize = 16 * 1024;
pub const DEFAULT_IMMUTABLE_INDEX_ROW_ID_BYTES: usize = 16 * 1024;
pub const DEFAULT_IMMUTABLE_INDEX_IDENTITY_BYTES: usize = 16 * 1024;
pub const DEFAULT_IMMUTABLE_INDEX_INLINE_POSTINGS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexPageId(NonZeroU64);

impl IndexPageId {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexRowId(Vec<u8>);

impl IndexRowId {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImmutableIndexPageLimits {
    pub max_page_bytes: NonZeroUsize,
    pub max_entries: NonZeroUsize,
    pub max_key_bytes: NonZeroUsize,
    pub max_row_id_bytes: NonZeroUsize,
    pub max_identity_bytes: NonZeroUsize,
    pub max_inline_postings: NonZeroUsize,
}

impl Default for ImmutableIndexPageLimits {
    fn default() -> Self {
        Self {
            max_page_bytes: NonZeroUsize::new(DEFAULT_IMMUTABLE_INDEX_PAGE_BYTES)
                .expect("default index page byte limit is non-zero"),
            max_entries: NonZeroUsize::new(DEFAULT_IMMUTABLE_INDEX_PAGE_ENTRIES)
                .expect("default index page entry limit is non-zero"),
            max_key_bytes: NonZeroUsize::new(DEFAULT_IMMUTABLE_INDEX_KEY_BYTES)
                .expect("default index key limit is non-zero"),
            max_row_id_bytes: NonZeroUsize::new(DEFAULT_IMMUTABLE_INDEX_ROW_ID_BYTES)
                .expect("default index row id limit is non-zero"),
            max_identity_bytes: NonZeroUsize::new(DEFAULT_IMMUTABLE_INDEX_IDENTITY_BYTES)
                .expect("default index identity limit is non-zero"),
            max_inline_postings: NonZeroUsize::new(DEFAULT_IMMUTABLE_INDEX_INLINE_POSTINGS)
                .expect("default inline posting limit is non-zero"),
        }
    }
}

impl ImmutableIndexPageLimits {
    pub const fn max_payload_bytes(self) -> Option<usize> {
        self.max_page_bytes.get().checked_sub(PAGE_HEADER_BYTES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexIdentity {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRootPage {
    pub identity: IndexIdentity,
    pub schema_digest: Sha256Digest,
    pub child: IndexPageId,
    pub height: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInteriorEntry {
    pub upper_bound: Vec<u8>,
    pub child: IndexPageId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInteriorPage {
    pub entries: Vec<IndexInteriorEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexLeafPosting {
    Inline(Vec<IndexRowId>),
    Page { first: IndexPageId, total_rows: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexLeafEntry {
    pub key: Vec<u8>,
    pub posting: IndexLeafPosting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexLeafPage {
    pub entries: Vec<IndexLeafEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPostingPage {
    pub next: Option<IndexPageId>,
    pub row_ids: Vec<IndexRowId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImmutableIndexPageBody {
    Root(IndexRootPage),
    Interior(IndexInteriorPage),
    Leaf(IndexLeafPage),
    Posting(IndexPostingPage),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmutableIndexPage {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub page_id: IndexPageId,
    pub body: ImmutableIndexPageBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImmutableIndexPageError {
    Admission(String),
    Corrupt(String),
}

impl fmt::Display for ImmutableIndexPageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => write!(formatter, "index page admission failed: {message}"),
            Self::Corrupt(message) => write!(formatter, "corrupt index page: {message}"),
        }
    }
}

impl std::error::Error for ImmutableIndexPageError {}

impl ImmutableIndexPage {
    pub fn encode_slot(
        &self,
        limits: ImmutableIndexPageLimits,
    ) -> Result<Vec<u8>, ImmutableIndexPageError> {
        let mut slot = self.encode(limits)?;
        slot.resize(limits.max_page_bytes.get(), 0);
        Ok(slot)
    }

    pub fn encode(
        &self,
        limits: ImmutableIndexPageLimits,
    ) -> Result<Vec<u8>, ImmutableIndexPageError> {
        validate_page(self, limits, ErrorClass::Admission)?;
        let max_payload_bytes = limits
            .max_page_bytes
            .get()
            .checked_sub(PAGE_HEADER_BYTES)
            .ok_or_else(|| {
                ImmutableIndexPageError::Admission(format!(
                    "page byte limit {} is smaller than the fixed header",
                    limits.max_page_bytes
                ))
            })?;
        let (kind, entry_count, payload) = encode_body(&self.body, max_payload_bytes)?;
        let total_bytes = PAGE_HEADER_BYTES
            .checked_add(payload.len())
            .ok_or_else(|| {
                ImmutableIndexPageError::Admission("encoded page size overflow".to_string())
            })?;
        if total_bytes > limits.max_page_bytes.get() {
            return Err(ImmutableIndexPageError::Admission(format!(
                "encoded page contains {total_bytes} bytes, exceeding limit {}",
                limits.max_page_bytes
            )));
        }
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            ImmutableIndexPageError::Admission("payload length does not fit u64".to_string())
        })?;
        let entry_count = u32::try_from(entry_count).map_err(|_| {
            ImmutableIndexPageError::Admission("entry count does not fit u32".to_string())
        })?;
        let mut encoded = Vec::with_capacity(total_bytes);
        encoded.extend_from_slice(PAGE_MAGIC);
        encoded.extend_from_slice(&PAGE_VERSION.to_le_bytes());
        encoded.push(kind.tag());
        encoded.push(0);
        encoded.extend_from_slice(&self.generation.to_le_bytes());
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

    pub fn decode(
        encoded: &[u8],
        limits: ImmutableIndexPageLimits,
    ) -> Result<Self, ImmutableIndexPageError> {
        Self::decode_inner(encoded, limits, false)
    }

    fn decode_inner(
        encoded: &[u8],
        limits: ImmutableIndexPageLimits,
        page_integrity_verified: bool,
    ) -> Result<Self, ImmutableIndexPageError> {
        if encoded.len() > limits.max_page_bytes.get() {
            return Err(ImmutableIndexPageError::Admission(format!(
                "encoded page contains {} bytes, exceeding limit {}",
                encoded.len(),
                limits.max_page_bytes
            )));
        }
        if encoded.len() < PAGE_HEADER_BYTES || &encoded[..8] != PAGE_MAGIC {
            return Err(ImmutableIndexPageError::Corrupt(
                "invalid page header".to_string(),
            ));
        }
        let version = read_u16(&encoded[8..10]);
        let kind = IndexPageKind::from_tag(encoded[10])?;
        let flags = encoded[11];
        if version != PAGE_VERSION || flags != 0 {
            return Err(ImmutableIndexPageError::Corrupt(format!(
                "unsupported page version {version} or flags {flags}"
            )));
        }
        let generation = read_u64(&encoded[12..20]);
        let source_commit_epoch = read_u64(&encoded[20..28]);
        let page_id = page_id(read_u64(&encoded[28..36]), "page id")?;
        let entry_count = read_u32(&encoded[36..40]) as usize;
        if entry_count > limits.max_entries.get() {
            return Err(ImmutableIndexPageError::Admission(format!(
                "page declares {entry_count} entries, exceeding limit {}",
                limits.max_entries
            )));
        }
        let payload_len = usize::try_from(read_u64(&encoded[40..48])).map_err(|_| {
            ImmutableIndexPageError::Corrupt("payload length overflows usize".to_string())
        })?;
        let expected_len = PAGE_HEADER_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| ImmutableIndexPageError::Corrupt("page length overflow".to_string()))?;
        if encoded.len() != expected_len {
            return Err(ImmutableIndexPageError::Corrupt(format!(
                "page length mismatch: expected {expected_len}, got {}",
                encoded.len()
            )));
        }
        let payload = &encoded[PAGE_HEADER_BYTES..];
        if !page_integrity_verified {
            #[cfg(test)]
            crate::cache::record_page_integrity_check();
            let mut hasher = IntegrityHasher::new();
            hasher.update(&encoded[..48]);
            hasher.update(payload);
            let digest = hasher.finish();
            let expected_crc = read_u32(&encoded[48..52]);
            if digest.crc32c.get() != expected_crc
                || digest.sha256.as_bytes() != &encoded[52..52 + SHA256_BYTES]
            {
                return Err(ImmutableIndexPageError::Corrupt(
                    "page payload checksum mismatch".to_string(),
                ));
            }
        }
        let body = decode_body(kind, entry_count, payload, limits)?;
        let page = Self {
            generation,
            source_commit_epoch,
            page_id,
            body,
        };
        validate_page(&page, limits, ErrorClass::Corrupt)?;
        Ok(page)
    }

    pub fn decode_slot(
        slot: &[u8],
        limits: ImmutableIndexPageLimits,
    ) -> Result<Self, ImmutableIndexPageError> {
        Self::decode_slot_inner(slot, limits, false)
    }

    pub(crate) fn decode_cached_slot(
        slot: &crate::SegmentCacheLease,
        limits: ImmutableIndexPageLimits,
    ) -> Result<Self, ImmutableIndexPageError> {
        Self::decode_slot_inner(slot, limits, slot.page_integrity_verified())
    }

    fn decode_slot_inner(
        slot: &[u8],
        limits: ImmutableIndexPageLimits,
        page_integrity_verified: bool,
    ) -> Result<Self, ImmutableIndexPageError> {
        if slot.len() != limits.max_page_bytes.get() {
            return Err(ImmutableIndexPageError::Corrupt(format!(
                "index page slot has {} bytes, expected {}",
                slot.len(),
                limits.max_page_bytes
            )));
        }
        if slot.len() < PAGE_HEADER_BYTES {
            return Err(ImmutableIndexPageError::Corrupt(
                "index page slot is smaller than its header".to_string(),
            ));
        }
        let payload_len = usize::try_from(read_u64(&slot[40..48])).map_err(|_| {
            ImmutableIndexPageError::Corrupt("payload length overflows usize".to_string())
        })?;
        let encoded_len = PAGE_HEADER_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| ImmutableIndexPageError::Corrupt("page length overflow".to_string()))?;
        if encoded_len > slot.len() {
            return Err(ImmutableIndexPageError::Corrupt(
                "page payload exceeds its fixed slot".to_string(),
            ));
        }
        if slot[encoded_len..].iter().any(|byte| *byte != 0) {
            return Err(ImmutableIndexPageError::Corrupt(
                "index page slot contains non-zero trailing bytes".to_string(),
            ));
        }
        Self::decode_inner(&slot[..encoded_len], limits, page_integrity_verified)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexPageKind {
    Root,
    Interior,
    Leaf,
    Posting,
}

impl IndexPageKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Root => 1,
            Self::Interior => 2,
            Self::Leaf => 3,
            Self::Posting => 4,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, ImmutableIndexPageError> {
        match tag {
            1 => Ok(Self::Root),
            2 => Ok(Self::Interior),
            3 => Ok(Self::Leaf),
            4 => Ok(Self::Posting),
            _ => Err(ImmutableIndexPageError::Corrupt(format!(
                "unknown page kind {tag}"
            ))),
        }
    }
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

fn invalid(error_class: ErrorClass, message: impl Into<String>) -> ImmutableIndexPageError {
    match error_class {
        ErrorClass::Admission => ImmutableIndexPageError::Admission(message.into()),
        ErrorClass::Corrupt => ImmutableIndexPageError::Corrupt(message.into()),
    }
}

fn validate_page(
    page: &ImmutableIndexPage,
    limits: ImmutableIndexPageLimits,
    error_class: ErrorClass,
) -> Result<(), ImmutableIndexPageError> {
    if page.generation == 0 {
        return Err(invalid(error_class, "manifest generation must be non-zero"));
    }
    match &page.body {
        ImmutableIndexPageBody::Root(root) => {
            let identity_bytes = root
                .identity
                .namespace
                .len()
                .checked_add(root.identity.name.len())
                .ok_or_else(|| invalid(error_class, "index identity size overflow"))?;
            if identity_bytes > limits.max_identity_bytes.get() {
                return Err(invalid(
                    error_class,
                    format!(
                        "index identity contains {identity_bytes} bytes, exceeding limit {}",
                        limits.max_identity_bytes
                    ),
                ));
            }
            if root.identity.namespace.is_empty() || root.identity.name.is_empty() {
                return Err(invalid(error_class, "index identity must be non-empty"));
            }
            if root.height == 0 {
                return Err(invalid(error_class, "root height must be non-zero"));
            }
            if root.child == page.page_id {
                return Err(invalid(error_class, "root page must not reference itself"));
            }
        }
        ImmutableIndexPageBody::Interior(interior) => {
            validate_entry_count(interior.entries.len(), limits, error_class)?;
            validate_sorted_keys(
                interior
                    .entries
                    .iter()
                    .map(|entry| entry.upper_bound.as_slice()),
                limits,
                error_class,
            )?;
            if interior
                .entries
                .iter()
                .any(|entry| entry.child == page.page_id)
                || interior
                    .entries
                    .windows(2)
                    .any(|pair| pair[0].child >= pair[1].child)
            {
                return Err(invalid(
                    error_class,
                    "interior child page ids must be strictly ordered and non-self-referential",
                ));
            }
        }
        ImmutableIndexPageBody::Leaf(leaf) => {
            validate_max_entry_count(leaf.entries.len(), limits, error_class)?;
            validate_sorted_keys(
                leaf.entries.iter().map(|entry| entry.key.as_slice()),
                limits,
                error_class,
            )?;
            for entry in &leaf.entries {
                if let IndexLeafPosting::Inline(row_ids) = &entry.posting {
                    if row_ids.is_empty() {
                        return Err(invalid(error_class, "inline posting must be non-empty"));
                    }
                    if row_ids.len() > limits.max_inline_postings.get() {
                        return Err(invalid(
                            error_class,
                            format!(
                                "inline posting contains {} rows, exceeding limit {}",
                                row_ids.len(),
                                limits.max_inline_postings
                            ),
                        ));
                    }
                    validate_sorted_row_ids(row_ids, limits, error_class)?;
                } else if let IndexLeafPosting::Page { total_rows, .. } = &entry.posting
                    && *total_rows == 0
                {
                    return Err(invalid(
                        error_class,
                        "posting page reference must declare at least one row",
                    ));
                }
            }
        }
        ImmutableIndexPageBody::Posting(posting) => {
            validate_entry_count(posting.row_ids.len(), limits, error_class)?;
            validate_sorted_row_ids(&posting.row_ids, limits, error_class)?;
            if posting.next.is_some_and(|next| next <= page.page_id) {
                return Err(invalid(
                    error_class,
                    "posting page must reference only a later page",
                ));
            }
        }
    }
    Ok(())
}

fn validate_entry_count(
    count: usize,
    limits: ImmutableIndexPageLimits,
    error_class: ErrorClass,
) -> Result<(), ImmutableIndexPageError> {
    if count == 0 {
        return Err(invalid(error_class, "non-root page must not be empty"));
    }
    validate_max_entry_count(count, limits, error_class)
}

fn validate_max_entry_count(
    count: usize,
    limits: ImmutableIndexPageLimits,
    error_class: ErrorClass,
) -> Result<(), ImmutableIndexPageError> {
    if count > limits.max_entries.get() {
        return Err(invalid(
            error_class,
            format!(
                "page contains {count} entries, exceeding limit {}",
                limits.max_entries
            ),
        ));
    }
    Ok(())
}

fn validate_sorted_keys<'a>(
    keys: impl Iterator<Item = &'a [u8]>,
    limits: ImmutableIndexPageLimits,
    error_class: ErrorClass,
) -> Result<(), ImmutableIndexPageError> {
    let mut previous: Option<&[u8]> = None;
    for key in keys {
        if key.len() > limits.max_key_bytes.get() {
            return Err(invalid(
                error_class,
                format!(
                    "index key contains {} bytes, exceeding limit {}",
                    key.len(),
                    limits.max_key_bytes
                ),
            ));
        }
        if previous.is_some_and(|previous| previous >= key) {
            return Err(invalid(
                error_class,
                "page keys must be strictly increasing",
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

fn validate_sorted_row_ids(
    row_ids: &[IndexRowId],
    limits: ImmutableIndexPageLimits,
    error_class: ErrorClass,
) -> Result<(), ImmutableIndexPageError> {
    if row_ids.iter().any(|row_id| row_id.as_bytes().is_empty()) {
        return Err(invalid(error_class, "row id must be non-empty"));
    }
    if let Some(row_id) = row_ids
        .iter()
        .find(|row_id| row_id.as_bytes().len() > limits.max_row_id_bytes.get())
    {
        return Err(invalid(
            error_class,
            format!(
                "row id contains {} bytes, exceeding limit {}",
                row_id.as_bytes().len(),
                limits.max_row_id_bytes
            ),
        ));
    }
    if row_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid(
            error_class,
            "posting row ids must be strictly increasing",
        ));
    }
    Ok(())
}

fn encode_body(
    body: &ImmutableIndexPageBody,
    max_payload_bytes: usize,
) -> Result<(IndexPageKind, usize, Vec<u8>), ImmutableIndexPageError> {
    let mut payload = Vec::new();
    match body {
        ImmutableIndexPageBody::Root(root) => {
            let mut identity = Vec::new();
            encode_bytes(&mut identity, root.identity.namespace.as_bytes())?;
            encode_bytes(&mut identity, root.identity.name.as_bytes())?;
            encode_field(
                &mut payload,
                ROOT_IDENTITY_FIELD,
                &identity,
                max_payload_bytes,
            )?;
            encode_field(
                &mut payload,
                ROOT_SCHEMA_DIGEST_FIELD,
                root.schema_digest.as_bytes(),
                max_payload_bytes,
            )?;
            encode_field(
                &mut payload,
                ROOT_CHILD_FIELD,
                &root.child.get().to_le_bytes(),
                max_payload_bytes,
            )?;
            encode_field(
                &mut payload,
                ROOT_HEIGHT_FIELD,
                &root.height.to_le_bytes(),
                max_payload_bytes,
            )?;
            Ok((IndexPageKind::Root, 1, payload))
        }
        ImmutableIndexPageBody::Interior(interior) => {
            for entry in &interior.entries {
                let mut encoded = Vec::new();
                encode_bytes(&mut encoded, &entry.upper_bound)?;
                encoded.extend_from_slice(&entry.child.get().to_le_bytes());
                encode_field(
                    &mut payload,
                    INTERIOR_ENTRY_FIELD,
                    &encoded,
                    max_payload_bytes,
                )?;
            }
            Ok((IndexPageKind::Interior, interior.entries.len(), payload))
        }
        ImmutableIndexPageBody::Leaf(leaf) => {
            for entry in &leaf.entries {
                let mut encoded = Vec::new();
                encode_bytes(&mut encoded, &entry.key)?;
                match &entry.posting {
                    IndexLeafPosting::Inline(row_ids) => {
                        encoded.push(0);
                        let count = u32::try_from(row_ids.len()).map_err(|_| {
                            ImmutableIndexPageError::Admission(
                                "inline posting count does not fit u32".to_string(),
                            )
                        })?;
                        encoded.extend_from_slice(&count.to_le_bytes());
                        for row_id in row_ids {
                            encode_bytes(&mut encoded, row_id.as_bytes())?;
                        }
                    }
                    IndexLeafPosting::Page { first, total_rows } => {
                        encoded.push(1);
                        encoded.extend_from_slice(&first.get().to_le_bytes());
                        encoded.extend_from_slice(&total_rows.to_le_bytes());
                    }
                }
                encode_field(&mut payload, LEAF_ENTRY_FIELD, &encoded, max_payload_bytes)?;
            }
            Ok((IndexPageKind::Leaf, leaf.entries.len(), payload))
        }
        ImmutableIndexPageBody::Posting(posting) => {
            if let Some(next) = posting.next {
                encode_field(
                    &mut payload,
                    POSTING_NEXT_FIELD,
                    &next.get().to_le_bytes(),
                    max_payload_bytes,
                )?;
            }
            for row_id in &posting.row_ids {
                encode_field(
                    &mut payload,
                    POSTING_ENTRY_FIELD,
                    row_id.as_bytes(),
                    max_payload_bytes,
                )?;
            }
            Ok((IndexPageKind::Posting, posting.row_ids.len(), payload))
        }
    }
}

fn decode_body(
    kind: IndexPageKind,
    entry_count: usize,
    payload: &[u8],
    limits: ImmutableIndexPageLimits,
) -> Result<ImmutableIndexPageBody, ImmutableIndexPageError> {
    let fields = Fields::new(payload);
    match kind {
        IndexPageKind::Root => decode_root(fields, entry_count, limits),
        IndexPageKind::Interior => decode_interior(fields, entry_count, limits),
        IndexPageKind::Leaf => decode_leaf(fields, entry_count, limits),
        IndexPageKind::Posting => decode_posting(fields, entry_count, limits),
    }
}

fn decode_root(
    fields: Fields<'_>,
    entry_count: usize,
    limits: ImmutableIndexPageLimits,
) -> Result<ImmutableIndexPageBody, ImmutableIndexPageError> {
    if entry_count != 1 {
        return Err(ImmutableIndexPageError::Corrupt(
            "root page must declare one logical entry".to_string(),
        ));
    }
    let mut identity = None;
    let mut schema_digest = None;
    let mut child = None;
    let mut height = None;
    for field in fields {
        let (tag, bytes) = field?;
        match tag {
            ROOT_IDENTITY_FIELD => {
                set_once(&mut identity, decode_identity(bytes, limits)?, "identity")?
            }
            ROOT_SCHEMA_DIGEST_FIELD => {
                if bytes.len() != SHA256_BYTES {
                    return Err(ImmutableIndexPageError::Corrupt(
                        "root schema digest has an invalid length".to_string(),
                    ));
                }
                let digest = Sha256Digest::from_bytes(
                    bytes.try_into().expect("schema digest length was checked"),
                );
                set_once(&mut schema_digest, digest, "schema digest")?;
            }
            ROOT_CHILD_FIELD => {
                set_once(&mut child, decode_page_id(bytes, "root child")?, "child")?
            }
            ROOT_HEIGHT_FIELD => {
                if bytes.len() != 4 {
                    return Err(ImmutableIndexPageError::Corrupt(
                        "root height has an invalid length".to_string(),
                    ));
                }
                set_once(&mut height, read_u32(bytes), "height")?;
            }
            _ => {}
        }
    }
    Ok(ImmutableIndexPageBody::Root(IndexRootPage {
        identity: required(identity, "identity")?,
        schema_digest: required(schema_digest, "schema digest")?,
        child: required(child, "child")?,
        height: required(height, "height")?,
    }))
}

fn decode_interior(
    fields: Fields<'_>,
    entry_count: usize,
    limits: ImmutableIndexPageLimits,
) -> Result<ImmutableIndexPageBody, ImmutableIndexPageError> {
    let mut entries = Vec::with_capacity(entry_count);
    for field in fields {
        let (tag, bytes) = field?;
        if tag != INTERIOR_ENTRY_FIELD {
            continue;
        }
        let (upper_bound, offset) =
            decode_bytes(bytes, 0, limits.max_key_bytes.get(), "separator")?;
        let child_bytes = bytes.get(offset..offset + 8).ok_or_else(|| {
            ImmutableIndexPageError::Corrupt("truncated interior child id".to_string())
        })?;
        if offset + 8 != bytes.len() {
            return Err(ImmutableIndexPageError::Corrupt(
                "interior entry contains trailing bytes".to_string(),
            ));
        }
        entries.push(IndexInteriorEntry {
            upper_bound: upper_bound.to_vec(),
            child: page_id(read_u64(child_bytes), "interior child")?,
        });
        if entries.len() > entry_count {
            return Err(ImmutableIndexPageError::Corrupt(
                "interior page contains more entries than declared".to_string(),
            ));
        }
    }
    ensure_entry_count(entry_count, entries.len())?;
    Ok(ImmutableIndexPageBody::Interior(IndexInteriorPage {
        entries,
    }))
}

fn decode_leaf(
    fields: Fields<'_>,
    entry_count: usize,
    limits: ImmutableIndexPageLimits,
) -> Result<ImmutableIndexPageBody, ImmutableIndexPageError> {
    let mut entries = Vec::with_capacity(entry_count);
    for field in fields {
        let (tag, bytes) = field?;
        if tag != LEAF_ENTRY_FIELD {
            continue;
        }
        let (key, mut offset) = decode_bytes(bytes, 0, limits.max_key_bytes.get(), "leaf key")?;
        let posting_kind = *bytes.get(offset).ok_or_else(|| {
            ImmutableIndexPageError::Corrupt("missing leaf posting kind".to_string())
        })?;
        offset += 1;
        let posting = match posting_kind {
            0 => {
                let count_bytes = bytes.get(offset..offset + 4).ok_or_else(|| {
                    ImmutableIndexPageError::Corrupt("truncated inline posting count".to_string())
                })?;
                offset += 4;
                let count = read_u32(count_bytes) as usize;
                if count > limits.max_inline_postings.get() {
                    return Err(ImmutableIndexPageError::Admission(format!(
                        "inline posting declares {count} rows, exceeding limit {}",
                        limits.max_inline_postings
                    )));
                }
                let mut row_ids = Vec::with_capacity(count);
                for _ in 0..count {
                    let (row_id, next) = decode_bytes(
                        bytes,
                        offset,
                        limits.max_row_id_bytes.get(),
                        "inline row id",
                    )?;
                    row_ids.push(IndexRowId::new(row_id.to_vec()));
                    offset = next;
                }
                IndexLeafPosting::Inline(row_ids)
            }
            1 => {
                let page_bytes = bytes.get(offset..offset + 8).ok_or_else(|| {
                    ImmutableIndexPageError::Corrupt("truncated posting page reference".to_string())
                })?;
                offset += 8;
                let total_rows_bytes = bytes.get(offset..offset + 8).ok_or_else(|| {
                    ImmutableIndexPageError::Corrupt(
                        "truncated posting page cardinality".to_string(),
                    )
                })?;
                offset += 8;
                IndexLeafPosting::Page {
                    first: page_id(read_u64(page_bytes), "posting page")?,
                    total_rows: read_u64(total_rows_bytes),
                }
            }
            _ => {
                return Err(ImmutableIndexPageError::Corrupt(format!(
                    "unknown leaf posting kind {posting_kind}"
                )))
            }
        };
        if offset != bytes.len() {
            return Err(ImmutableIndexPageError::Corrupt(
                "leaf entry contains trailing bytes".to_string(),
            ));
        }
        entries.push(IndexLeafEntry {
            key: key.to_vec(),
            posting,
        });
        if entries.len() > entry_count {
            return Err(ImmutableIndexPageError::Corrupt(
                "leaf page contains more entries than declared".to_string(),
            ));
        }
    }
    ensure_entry_count(entry_count, entries.len())?;
    Ok(ImmutableIndexPageBody::Leaf(IndexLeafPage { entries }))
}

fn decode_posting(
    fields: Fields<'_>,
    entry_count: usize,
    limits: ImmutableIndexPageLimits,
) -> Result<ImmutableIndexPageBody, ImmutableIndexPageError> {
    let mut row_ids = Vec::with_capacity(entry_count);
    let mut next = None;
    for field in fields {
        let (tag, bytes) = field?;
        match tag {
            POSTING_NEXT_FIELD => set_once(
                &mut next,
                Some(decode_page_id(bytes, "next posting page")?),
                "next posting page",
            )?,
            POSTING_ENTRY_FIELD => {
                if bytes.len() > limits.max_row_id_bytes.get() {
                    return Err(ImmutableIndexPageError::Admission(format!(
                        "posting row id contains {} bytes, exceeding limit {}",
                        bytes.len(),
                        limits.max_row_id_bytes
                    )));
                }
                row_ids.push(IndexRowId::new(bytes.to_vec()));
                if row_ids.len() > entry_count {
                    return Err(ImmutableIndexPageError::Corrupt(
                        "posting page contains more entries than declared".to_string(),
                    ));
                }
            }
            _ => {}
        }
    }
    ensure_entry_count(entry_count, row_ids.len())?;
    Ok(ImmutableIndexPageBody::Posting(IndexPostingPage {
        next: next.flatten(),
        row_ids,
    }))
}

fn encode_field(
    output: &mut Vec<u8>,
    tag: u16,
    bytes: &[u8],
    max_payload_bytes: usize,
) -> Result<(), ImmutableIndexPageError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        ImmutableIndexPageError::Admission("field length does not fit u32".to_string())
    })?;
    let next_len = output
        .len()
        .checked_add(FIELD_HEADER_BYTES)
        .and_then(|len| len.checked_add(bytes.len()))
        .ok_or_else(|| {
            ImmutableIndexPageError::Admission("encoded field size overflow".to_string())
        })?;
    if next_len > max_payload_bytes {
        return Err(ImmutableIndexPageError::Admission(format!(
            "index page payload would contain {next_len} bytes, exceeding limit {max_payload_bytes}"
        )));
    }
    output.extend_from_slice(&tag.to_le_bytes());
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ImmutableIndexPageError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        ImmutableIndexPageError::Admission("byte string length does not fit u32".to_string())
    })?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn decode_identity(
    bytes: &[u8],
    limits: ImmutableIndexPageLimits,
) -> Result<IndexIdentity, ImmutableIndexPageError> {
    let (namespace, offset) =
        decode_bytes(bytes, 0, limits.max_identity_bytes.get(), "index namespace")?;
    let remaining_limit = limits
        .max_identity_bytes
        .get()
        .saturating_sub(namespace.len());
    let (name, offset) = decode_bytes(bytes, offset, remaining_limit, "index name")?;
    if offset != bytes.len() {
        return Err(ImmutableIndexPageError::Corrupt(
            "index identity contains trailing bytes".to_string(),
        ));
    }
    let namespace = std::str::from_utf8(namespace)
        .map_err(|_| ImmutableIndexPageError::Corrupt("index namespace is not UTF-8".to_string()))?
        .to_string();
    let name = std::str::from_utf8(name)
        .map_err(|_| ImmutableIndexPageError::Corrupt("index name is not UTF-8".to_string()))?
        .to_string();
    Ok(IndexIdentity { namespace, name })
}

fn decode_bytes<'a>(
    bytes: &'a [u8],
    offset: usize,
    max_bytes: usize,
    context: &str,
) -> Result<(&'a [u8], usize), ImmutableIndexPageError> {
    let len_bytes = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| ImmutableIndexPageError::Corrupt(format!("truncated {context} length")))?;
    let len = read_u32(len_bytes) as usize;
    if len > max_bytes {
        return Err(ImmutableIndexPageError::Admission(format!(
            "{context} contains {len} bytes, exceeding limit {max_bytes}"
        )));
    }
    let start = offset + 4;
    let end = start
        .checked_add(len)
        .ok_or_else(|| ImmutableIndexPageError::Corrupt(format!("{context} length overflow")))?;
    let value = bytes
        .get(start..end)
        .ok_or_else(|| ImmutableIndexPageError::Corrupt(format!("truncated {context}")))?;
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
    type Item = Result<(u16, &'a [u8]), ImmutableIndexPageError>;

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
                return Some(Err(ImmutableIndexPageError::Corrupt(
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
                return Some(Err(ImmutableIndexPageError::Corrupt(
                    "field length overflow".to_string(),
                )));
            }
        };
        let value = match self.payload.get(start..end) {
            Some(value) => value,
            None => {
                self.offset = self.payload.len();
                return Some(Err(ImmutableIndexPageError::Corrupt(
                    "truncated field payload".to_string(),
                )));
            }
        };
        self.offset = end;
        Some(Ok((tag, value)))
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T, field: &str) -> Result<(), ImmutableIndexPageError> {
    if slot.replace(value).is_some() {
        return Err(ImmutableIndexPageError::Corrupt(format!(
            "root page contains duplicate {field} field"
        )));
    }
    Ok(())
}

fn required<T>(value: Option<T>, field: &str) -> Result<T, ImmutableIndexPageError> {
    value.ok_or_else(|| {
        ImmutableIndexPageError::Corrupt(format!("root page is missing {field} field"))
    })
}

fn ensure_entry_count(expected: usize, actual: usize) -> Result<(), ImmutableIndexPageError> {
    if expected != actual {
        return Err(ImmutableIndexPageError::Corrupt(format!(
            "page entry count mismatch: declared {expected}, decoded {actual}"
        )));
    }
    Ok(())
}

fn decode_page_id(bytes: &[u8], context: &str) -> Result<IndexPageId, ImmutableIndexPageError> {
    if bytes.len() != 8 {
        return Err(ImmutableIndexPageError::Corrupt(format!(
            "{context} has an invalid length"
        )));
    }
    page_id(read_u64(bytes), context)
}

fn page_id(value: u64, context: &str) -> Result<IndexPageId, ImmutableIndexPageError> {
    NonZeroU64::new(value)
        .map(IndexPageId::new)
        .ok_or_else(|| ImmutableIndexPageError::Corrupt(format!("{context} must be non-zero")))
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
    use hawdb_integrity::integrity_digest;

    fn page_id(value: u64) -> IndexPageId {
        IndexPageId::new(NonZeroU64::new(value).unwrap())
    }

    fn schema_digest() -> Sha256Digest {
        integrity_digest(b"table schema").sha256
    }

    fn row_id(value: u64) -> IndexRowId {
        IndexRowId::new(value.to_be_bytes().to_vec())
    }

    fn round_trip(body: ImmutableIndexPageBody) {
        let page = ImmutableIndexPage {
            generation: 7,
            source_commit_epoch: 6,
            page_id: page_id(1),
            body,
        };
        let limits = ImmutableIndexPageLimits::default();
        let encoded = page.encode(limits).expect("encode page");
        assert_eq!(ImmutableIndexPage::decode(&encoded, limits).unwrap(), page);
    }

    #[test]
    fn all_page_kinds_round_trip() {
        round_trip(ImmutableIndexPageBody::Root(IndexRootPage {
            identity: IndexIdentity {
                namespace: "documents".to_string(),
                name: "documents_owner_idx".to_string(),
            },
            schema_digest: schema_digest(),
            child: page_id(2),
            height: 2,
        }));
        round_trip(ImmutableIndexPageBody::Interior(IndexInteriorPage {
            entries: vec![
                IndexInteriorEntry {
                    upper_bound: b"a".to_vec(),
                    child: page_id(2),
                },
                IndexInteriorEntry {
                    upper_bound: b"z".to_vec(),
                    child: page_id(3),
                },
            ],
        }));
        round_trip(ImmutableIndexPageBody::Leaf(IndexLeafPage {
            entries: vec![
                IndexLeafEntry {
                    key: b"a".to_vec(),
                    posting: IndexLeafPosting::Inline(vec![row_id(1), row_id(2)]),
                },
                IndexLeafEntry {
                    key: b"z".to_vec(),
                    posting: IndexLeafPosting::Page {
                        first: page_id(4),
                        total_rows: 2,
                    },
                },
            ],
        }));
        round_trip(ImmutableIndexPageBody::Posting(IndexPostingPage {
            next: Some(page_id(5)),
            row_ids: vec![row_id(3), row_id(9)],
        }));
    }

    #[test]
    fn fixed_slot_round_trip_rejects_trailing_corruption() {
        let page = ImmutableIndexPage {
            generation: 9,
            source_commit_epoch: 400,
            page_id: page_id(1),
            body: ImmutableIndexPageBody::Posting(IndexPostingPage {
                next: None,
                row_ids: vec![row_id(1), row_id(2)],
            }),
        };
        let limits = ImmutableIndexPageLimits {
            max_page_bytes: NonZeroUsize::new(4096).unwrap(),
            ..ImmutableIndexPageLimits::default()
        };
        let slot = page.encode_slot(limits).unwrap();
        assert_eq!(slot.len(), 4096);
        assert_eq!(
            ImmutableIndexPage::decode_slot(&slot, limits).unwrap(),
            page
        );

        let mut corrupt = slot;
        *corrupt.last_mut().unwrap() = 1;
        assert!(matches!(
            ImmutableIndexPage::decode_slot(&corrupt, limits),
            Err(ImmutableIndexPageError::Corrupt(message))
                if message == "index page slot contains non-zero trailing bytes"
        ));
    }

    #[test]
    fn decoder_rejects_corruption_and_truncation() {
        let page = ImmutableIndexPage {
            generation: 2,
            source_commit_epoch: 2,
            page_id: page_id(1),
            body: ImmutableIndexPageBody::Posting(IndexPostingPage {
                next: None,
                row_ids: vec![row_id(1)],
            }),
        };
        let limits = ImmutableIndexPageLimits::default();
        let mut corrupt = page.encode(limits).unwrap();
        *corrupt.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            ImmutableIndexPage::decode(&corrupt, limits),
            Err(ImmutableIndexPageError::Corrupt(message))
                if message == "page payload checksum mismatch"
        ));
        corrupt.pop();
        assert!(ImmutableIndexPage::decode(&corrupt, limits).is_err());
    }

    #[test]
    fn encoder_and_decoder_share_limits() {
        let page = ImmutableIndexPage {
            generation: 1,
            source_commit_epoch: 1,
            page_id: page_id(1),
            body: ImmutableIndexPageBody::Leaf(IndexLeafPage {
                entries: vec![IndexLeafEntry {
                    key: b"oversized".to_vec(),
                    posting: IndexLeafPosting::Inline(vec![row_id(1)]),
                }],
            }),
        };
        let limits = ImmutableIndexPageLimits {
            max_key_bytes: NonZeroUsize::new(4).unwrap(),
            ..ImmutableIndexPageLimits::default()
        };
        assert!(matches!(
            page.encode(limits),
            Err(ImmutableIndexPageError::Admission(_))
        ));

        let encoded = page.encode(ImmutableIndexPageLimits::default()).unwrap();
        assert!(matches!(
            ImmutableIndexPage::decode(&encoded, limits),
            Err(ImmutableIndexPageError::Admission(_))
        ));
    }

    #[test]
    fn unknown_fields_are_forward_compatible() {
        let page = ImmutableIndexPage {
            generation: 3,
            source_commit_epoch: 3,
            page_id: page_id(1),
            body: ImmutableIndexPageBody::Root(IndexRootPage {
                identity: IndexIdentity {
                    namespace: "documents".to_string(),
                    name: "__primary__".to_string(),
                },
                schema_digest: schema_digest(),
                child: page_id(2),
                height: 1,
            }),
        };
        let limits = ImmutableIndexPageLimits::default();
        let encoded = page.encode(limits).unwrap();
        let mut payload = encoded[PAGE_HEADER_BYTES..].to_vec();
        encode_field(&mut payload, 999, b"future", usize::MAX).unwrap();
        let mut extended = encoded[..PAGE_HEADER_BYTES].to_vec();
        extended[40..48].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        let mut hasher = IntegrityHasher::new();
        hasher.update(&extended[..48]);
        hasher.update(&payload);
        let digest = hasher.finish();
        extended[48..52].copy_from_slice(&digest.crc32c.get().to_le_bytes());
        extended[52..84].copy_from_slice(digest.sha256.as_bytes());
        extended.extend_from_slice(&payload);
        assert_eq!(ImmutableIndexPage::decode(&extended, limits).unwrap(), page);
    }

    #[test]
    fn page_ordering_and_generation_fences_fail_closed() {
        let unsorted = ImmutableIndexPage {
            generation: 4,
            source_commit_epoch: 4,
            page_id: page_id(1),
            body: ImmutableIndexPageBody::Leaf(IndexLeafPage {
                entries: vec![
                    IndexLeafEntry {
                        key: b"z".to_vec(),
                        posting: IndexLeafPosting::Inline(vec![row_id(1)]),
                    },
                    IndexLeafEntry {
                        key: b"a".to_vec(),
                        posting: IndexLeafPosting::Inline(vec![row_id(2)]),
                    },
                ],
            }),
        };
        assert!(matches!(
            unsorted.encode(ImmutableIndexPageLimits::default()),
            Err(ImmutableIndexPageError::Admission(_))
        ));

        let invalid_generation = ImmutableIndexPage {
            generation: 0,
            source_commit_epoch: 5,
            page_id: page_id(1),
            body: ImmutableIndexPageBody::Posting(IndexPostingPage {
                next: None,
                row_ids: vec![row_id(1)],
            }),
        };
        assert!(matches!(
            invalid_generation.encode(ImmutableIndexPageLimits::default()),
            Err(ImmutableIndexPageError::Admission(_))
        ));

        let duplicate_child = ImmutableIndexPage {
            generation: 4,
            source_commit_epoch: 5,
            page_id: page_id(10),
            body: ImmutableIndexPageBody::Interior(IndexInteriorPage {
                entries: vec![
                    IndexInteriorEntry {
                        upper_bound: b"a".to_vec(),
                        child: page_id(2),
                    },
                    IndexInteriorEntry {
                        upper_bound: b"z".to_vec(),
                        child: page_id(2),
                    },
                ],
            }),
        };
        assert!(matches!(
            duplicate_child.encode(ImmutableIndexPageLimits::default()),
            Err(ImmutableIndexPageError::Admission(_))
        ));

        let backward_posting_link = ImmutableIndexPage {
            generation: 4,
            source_commit_epoch: 5,
            page_id: page_id(10),
            body: ImmutableIndexPageBody::Posting(IndexPostingPage {
                next: Some(page_id(9)),
                row_ids: vec![row_id(1)],
            }),
        };
        assert!(matches!(
            backward_posting_link.encode(ImmutableIndexPageLimits::default()),
            Err(ImmutableIndexPageError::Admission(_))
        ));
    }

    #[test]
    fn encoder_rejects_page_budget_before_accumulating_all_entries() {
        let page = ImmutableIndexPage {
            generation: 1,
            source_commit_epoch: 10_000,
            page_id: page_id(1),
            body: ImmutableIndexPageBody::Leaf(IndexLeafPage {
                entries: (0..128)
                    .map(|ordinal| IndexLeafEntry {
                        key: format!("{ordinal:04}-{}", "x".repeat(1024)).into_bytes(),
                        posting: IndexLeafPosting::Inline(vec![row_id(ordinal)]),
                    })
                    .collect(),
            }),
        };
        let limits = ImmutableIndexPageLimits {
            max_page_bytes: NonZeroUsize::new(4096).unwrap(),
            ..ImmutableIndexPageLimits::default()
        };
        assert!(matches!(
            page.encode(limits),
            Err(ImmutableIndexPageError::Admission(message))
                if message.contains("page payload would contain")
        ));
    }
}
