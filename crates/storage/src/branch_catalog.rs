//! Bounded, deterministic identity and catalog records for project branches.
//!
//! This module is the storage-owned codec seam for the branching contract.  It
//! deliberately does not open branches or publish files yet; those operations
//! will build on these validated records.  The wire format is versioned and
//! checksummed so a future publisher can reject an incomplete or ambiguous
//! catalog before changing any durable selector.

use crate::durability;
use hawdb_core::Uuid;
use hawdb_integrity::crc32c;
use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 8] = b"HBCATV1\0";
const VERSION: u16 = 1;
const MAX_CATALOG_BYTES: usize = 16 * 1024 * 1024;
const MAX_BRANCHES: u32 = 100_000;
const MAX_NAME_BYTES: usize = 128;
const MAX_OWNER_BYTES: usize = 256;
const MAX_REQUEST_KEY_BYTES: usize = 256;
const DIGEST_BYTES: usize = 32;
static CANDIDATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Stable project-scoped branch identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchId(Uuid);

impl BranchId {
    pub fn new(value: Uuid) -> Result<Self, CatalogError> {
        if value.is_nil() {
            return Err(CatalogError::InvalidIdentity("branch UUID must not be nil"));
        }
        Ok(Self(value))
    }

    pub fn parse(value: &str) -> Result<Self, CatalogError> {
        let uuid = value
            .parse::<Uuid>()
            .map_err(|_| CatalogError::InvalidIdentity("branch UUID is not valid"))?;
        Self::new(uuid)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Case-sensitive, validated catalog name.  Names are never used as paths.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchName(String);

impl BranchName {
    pub fn new(value: impl Into<String>) -> Result<Self, CatalogError> {
        let value = value.into();
        validate_name(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_encoded(value: String) -> Result<Self, CatalogError> {
        validate_catalog_name(&value)?;
        Ok(Self(value))
    }
}

/// Explicit selector alternatives prevent UUID-looking names from being
/// silently reinterpreted by an open operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchSelector<'a> {
    Id(BranchId),
    Name(&'a BranchName),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchState {
    Creating,
    Ready,
    Expired,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Pending,
    Succeeded,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRecord {
    pub id: BranchId,
    pub name: BranchName,
    pub parent_id: Option<BranchId>,
    pub source_commit_epoch: u64,
    pub base_root_digest: Option<[u8; DIGEST_BYTES]>,
    pub metadata_revision: u64,
    pub state: BranchState,
    pub owner: Option<String>,
    pub expires_at_unix_seconds: Option<i64>,
    pub create_request_key: String,
    pub request_fingerprint: [u8; DIGEST_BYTES],
    pub create_outcome: CreateOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub project_id: BranchId,
    pub revision: u64,
    pub branches: Vec<BranchRecord>,
}

impl Catalog {
    pub fn validate(&self) -> Result<(), CatalogError> {
        if self.branches.len() > MAX_BRANCHES as usize {
            return Err(CatalogError::Limit("branch count"));
        }
        let mut ordered = self.branches.clone();
        ordered.sort_by_key(|branch| branch.id);
        for pair in ordered.windows(2) {
            if pair[0].id == pair[1].id {
                return Err(CatalogError::Duplicate("branch UUID"));
            }
        }
        let mut names = BTreeSet::new();
        let mut main_count = 0;
        for branch in &self.branches {
            if !names.insert(branch.name.clone()) {
                return Err(CatalogError::Duplicate("branch name"));
            }
            if branch.id == self.project_id {
                return Err(CatalogError::InvalidIdentity(
                    "project UUID and branch UUID must be distinct",
                ));
            }
            validate_catalog_name(branch.name.as_str())?;
            if branch.name.as_str() == "main" {
                main_count += 1;
                if branch.parent_id.is_some() {
                    return Err(CatalogError::InvalidState(
                        "main branch cannot have a parent",
                    ));
                }
            }
            if branch.parent_id == Some(branch.id) {
                return Err(CatalogError::InvalidState(
                    "branch cannot be its own parent",
                ));
            }
            if let Some(parent_id) = branch.parent_id
                && !self
                    .branches
                    .iter()
                    .any(|candidate| candidate.id == parent_id)
            {
                return Err(CatalogError::InvalidIdentity(
                    "branch parent UUID is not present in the catalog",
                ));
            }
            if branch.name.as_str().starts_with("agent/")
                && branch.name.as_str() != format!("agent/{}", branch.id.as_uuid())
            {
                return Err(CatalogError::InvalidName);
            }
            validate_bounded_string(
                &branch.create_request_key,
                MAX_REQUEST_KEY_BYTES,
                "create request key",
            )?;
            if let Some(owner) = &branch.owner {
                validate_bounded_string(owner, MAX_OWNER_BYTES, "owner")?;
            }
            if branch.state == BranchState::Deleted
                && branch.create_outcome == CreateOutcome::Pending
            {
                return Err(CatalogError::InvalidState(
                    "deleted branch cannot have a pending create outcome",
                ));
            }
        }
        if main_count > 1 {
            return Err(CatalogError::Duplicate("main branch"));
        }
        Ok(())
    }

    /// Encode in UUID order.  Sorting is part of the codec contract, so two
    /// equivalent catalogs have byte-identical representations.
    pub fn encode(&self) -> Result<Vec<u8>, CatalogError> {
        self.validate()?;
        let mut branches = self.branches.clone();
        branches.sort_by_key(|branch| branch.id);
        let mut bytes = Vec::with_capacity(128);
        bytes.extend_from_slice(MAGIC);
        put_u16(&mut bytes, VERSION);
        bytes.extend_from_slice(self.project_id.as_uuid().as_bytes());
        put_u64(&mut bytes, self.revision);
        put_u32(&mut bytes, branches.len() as u32);
        for branch in branches {
            encode_branch(&mut bytes, &branch)?;
        }
        let checksum = crc32c(&bytes).get();
        put_u32(&mut bytes, checksum);
        if bytes.len() > MAX_CATALOG_BYTES {
            return Err(CatalogError::Limit("catalog bytes"));
        }
        Ok(bytes)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CatalogError> {
        if encoded.len() > MAX_CATALOG_BYTES {
            return Err(CatalogError::Limit("catalog bytes"));
        }
        if encoded.len() < MAGIC.len() + 2 + 16 + 8 + 4 + 4 {
            return Err(CatalogError::Truncated);
        }
        let checksum_offset = encoded.len() - 4;
        let expected = u32::from_le_bytes(
            encoded[checksum_offset..]
                .try_into()
                .map_err(|_| CatalogError::Truncated)?,
        );
        let actual = crc32c(&encoded[..checksum_offset]).get();
        if actual != expected {
            return Err(CatalogError::Checksum);
        }
        let mut reader = Reader::new(&encoded[..checksum_offset]);
        if reader.take(MAGIC.len())? != MAGIC {
            return Err(CatalogError::Version);
        }
        if reader.u16()? != VERSION {
            return Err(CatalogError::Version);
        }
        let project_id = BranchId::new(Uuid::from_bytes(reader.array()?))?;
        let revision = reader.u64()?;
        let count = reader.u32()?;
        if count > MAX_BRANCHES {
            return Err(CatalogError::Limit("branch count"));
        }
        let mut branches = Vec::with_capacity(count as usize);
        for _ in 0..count {
            branches.push(decode_branch(&mut reader)?);
        }
        if !reader.is_empty() {
            return Err(CatalogError::TrailingBytes);
        }
        let catalog = Self {
            project_id,
            revision,
            branches,
        };
        catalog.validate()?;
        Ok(catalog)
    }
}

/// Read a published catalog after applying the same byte bound as the decoder.
pub fn read_catalog(path: &Path) -> io::Result<Catalog> {
    let length = fs::metadata(path)?.len();
    if length > MAX_CATALOG_BYTES as u64 {
        return Err(invalid_data("branch catalog exceeds its byte limit"));
    }
    let mut file = fs::File::open(path)?;
    let mut encoded = Vec::with_capacity(length as usize);
    file.read_to_end(&mut encoded)?;
    Catalog::decode(&encoded).map_err(|error| invalid_data(error.to_string()))
}

/// Publish a catalog with candidate-file sync followed by atomic replacement.
///
/// The destination is never opened for writing.  A failed write or sync removes
/// only its private candidate; a failed replacement is returned without retry,
/// because the caller cannot infer whether the directory operation reached the
/// filesystem.  The caller must reopen before attempting another publication.
pub fn write_catalog(path: &Path, catalog: &Catalog) -> io::Result<()> {
    let encoded = catalog
        .encode()
        .map_err(|error| invalid_data(error.to_string()))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| invalid_data("branch catalog destination has no parent"))?;
    let sequence = CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let candidate = parent.join(format!(
        ".{}.candidate-{}-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("catalog"),
        std::process::id(),
        sequence
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&candidate)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        drop(file);
        durability::durable_replace_file(&candidate, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&candidate);
    }
    result
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn validate_name(value: &str) -> Result<(), CatalogError> {
    validate_bounded_string(value, MAX_NAME_BYTES, "branch name")?;
    if value == "main" || value.starts_with("agent/") {
        return Err(CatalogError::ReservedName);
    }
    validate_catalog_name(value)
}

fn validate_catalog_name(value: &str) -> Result<(), CatalogError> {
    validate_bounded_string(value, MAX_NAME_BYTES, "branch name")?;
    if value.starts_with('/') || value.ends_with('/') || value.contains('\\') {
        return Err(CatalogError::InvalidName);
    }
    for component in value.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(CatalogError::InvalidName);
        }
        if !component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(CatalogError::InvalidName);
        }
        if component.bytes().any(|byte| byte.is_ascii_whitespace()) {
            return Err(CatalogError::InvalidName);
        }
    }
    Ok(())
}

fn validate_bounded_string(
    value: &str,
    maximum: usize,
    field: &'static str,
) -> Result<(), CatalogError> {
    if value.is_empty() || value.len() > maximum || !value.is_ascii() {
        return Err(CatalogError::Limit(field));
    }
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(CatalogError::InvalidName);
    }
    Ok(())
}

fn encode_branch(output: &mut Vec<u8>, branch: &BranchRecord) -> Result<(), CatalogError> {
    output.extend_from_slice(branch.id.as_uuid().as_bytes());
    put_string(output, branch.name.as_str(), MAX_NAME_BYTES, "branch name")?;
    put_optional_uuid(output, branch.parent_id);
    put_u64(output, branch.source_commit_epoch);
    put_optional_bytes(output, branch.base_root_digest);
    put_u64(output, branch.metadata_revision);
    output.push(state_byte(branch.state));
    put_optional_string(output, branch.owner.as_deref(), MAX_OWNER_BYTES, "owner")?;
    match branch.expires_at_unix_seconds {
        Some(value) => {
            output.push(1);
            put_i64(output, value);
        }
        None => output.push(0),
    }
    put_string(
        output,
        &branch.create_request_key,
        MAX_REQUEST_KEY_BYTES,
        "create request key",
    )?;
    output.extend_from_slice(&branch.request_fingerprint);
    output.push(outcome_byte(branch.create_outcome));
    Ok(())
}

fn decode_branch(reader: &mut Reader<'_>) -> Result<BranchRecord, CatalogError> {
    let id = BranchId::new(Uuid::from_bytes(reader.array()?))?;
    let name = BranchName::from_encoded(reader.string(MAX_NAME_BYTES, "branch name")?)?;
    let parent_id = reader.optional_uuid()?;
    let source_commit_epoch = reader.u64()?;
    let base_root_digest = reader.optional_array()?;
    let metadata_revision = reader.u64()?;
    let state = parse_state(reader.byte()?)?;
    let owner = reader.optional_string(MAX_OWNER_BYTES, "owner")?;
    let expires_at_unix_seconds = if reader.byte()? == 1 {
        Some(reader.i64()?)
    } else {
        None
    };
    let create_request_key = reader.string(MAX_REQUEST_KEY_BYTES, "create request key")?;
    let request_fingerprint = reader.array()?;
    let create_outcome = parse_outcome(reader.byte()?)?;
    Ok(BranchRecord {
        id,
        name,
        parent_id,
        source_commit_epoch,
        base_root_digest,
        metadata_revision,
        state,
        owner,
        expires_at_unix_seconds,
        create_request_key,
        request_fingerprint,
        create_outcome,
    })
}

fn state_byte(value: BranchState) -> u8 {
    match value {
        BranchState::Creating => 0,
        BranchState::Ready => 1,
        BranchState::Expired => 2,
        BranchState::Deleting => 3,
        BranchState::Deleted => 4,
    }
}

fn parse_state(value: u8) -> Result<BranchState, CatalogError> {
    match value {
        0 => Ok(BranchState::Creating),
        1 => Ok(BranchState::Ready),
        2 => Ok(BranchState::Expired),
        3 => Ok(BranchState::Deleting),
        4 => Ok(BranchState::Deleted),
        _ => Err(CatalogError::InvalidEnum("branch state")),
    }
}

fn outcome_byte(value: CreateOutcome) -> u8 {
    match value {
        CreateOutcome::Pending => 0,
        CreateOutcome::Succeeded => 1,
        CreateOutcome::Aborted => 2,
    }
}

fn parse_outcome(value: u8) -> Result<CreateOutcome, CatalogError> {
    match value {
        0 => Ok(CreateOutcome::Pending),
        1 => Ok(CreateOutcome::Succeeded),
        2 => Ok(CreateOutcome::Aborted),
        _ => Err(CatalogError::InvalidEnum("create outcome")),
    }
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_string(
    output: &mut Vec<u8>,
    value: &str,
    maximum: usize,
    field: &'static str,
) -> Result<(), CatalogError> {
    validate_bounded_string(value, maximum, field)?;
    let length = u16::try_from(value.len()).map_err(|_| CatalogError::Limit(field))?;
    put_u16(output, length);
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_optional_string(
    output: &mut Vec<u8>,
    value: Option<&str>,
    maximum: usize,
    field: &'static str,
) -> Result<(), CatalogError> {
    match value {
        Some(value) => {
            output.push(1);
            put_string(output, value, maximum, field)?;
        }
        None => output.push(0),
    }
    Ok(())
}

fn put_optional_uuid(output: &mut Vec<u8>, value: Option<BranchId>) {
    match value {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(value.as_uuid().as_bytes());
        }
        None => output.push(0),
    }
}

fn put_optional_bytes(output: &mut Vec<u8>, value: Option<[u8; DIGEST_BYTES]>) {
    match value {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value);
        }
        None => output.push(0),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], CatalogError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(CatalogError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CatalogError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, CatalogError> {
        Ok(*self.take(1)?.first().ok_or(CatalogError::Truncated)?)
    }

    fn u16(&mut self) -> Result<u16, CatalogError> {
        Ok(u16::from_le_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| CatalogError::Truncated)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, CatalogError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| CatalogError::Truncated)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, CatalogError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CatalogError::Truncated)?,
        ))
    }

    fn i64(&mut self) -> Result<i64, CatalogError> {
        Ok(i64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CatalogError::Truncated)?,
        ))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CatalogError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CatalogError::Truncated)
    }

    fn optional_uuid(&mut self) -> Result<Option<BranchId>, CatalogError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(BranchId::new(Uuid::from_bytes(self.array()?))?)),
            _ => Err(CatalogError::InvalidEnum("optional UUID marker")),
        }
    }

    fn optional_array(&mut self) -> Result<Option<[u8; DIGEST_BYTES]>, CatalogError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.array()?)),
            _ => Err(CatalogError::InvalidEnum("optional digest marker")),
        }
    }

    fn string(&mut self, maximum: usize, field: &'static str) -> Result<String, CatalogError> {
        let length = self.u16()? as usize;
        if length == 0 || length > maximum {
            return Err(CatalogError::Limit(field));
        }
        let value = std::str::from_utf8(self.take(length)?)
            .map_err(|_| CatalogError::InvalidUtf8(field))?;
        validate_bounded_string(value, maximum, field)?;
        Ok(value.to_owned())
    }

    fn optional_string(
        &mut self,
        maximum: usize,
        field: &'static str,
    ) -> Result<Option<String>, CatalogError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.string(maximum, field)?)),
            _ => Err(CatalogError::InvalidEnum("optional string marker")),
        }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    Checksum,
    Duplicate(&'static str),
    InvalidEnum(&'static str),
    InvalidIdentity(&'static str),
    InvalidName,
    InvalidState(&'static str),
    InvalidUtf8(&'static str),
    Limit(&'static str),
    ReservedName,
    TrailingBytes,
    Truncated,
    Version,
}

impl Display for CatalogError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Checksum => formatter.write_str("branch catalog checksum mismatch"),
            Self::Duplicate(field) => write!(formatter, "duplicate branch catalog {field}"),
            Self::InvalidEnum(field) => write!(formatter, "invalid branch catalog {field}"),
            Self::InvalidIdentity(message) => formatter.write_str(message),
            Self::InvalidName => formatter.write_str("invalid branch catalog name"),
            Self::InvalidState(message) => formatter.write_str(message),
            Self::InvalidUtf8(field) => write!(formatter, "branch catalog {field} is not UTF-8"),
            Self::Limit(field) => write!(formatter, "branch catalog {field} exceeds its limit"),
            Self::ReservedName => formatter.write_str("branch catalog name is reserved"),
            Self::TrailingBytes => formatter.write_str("branch catalog has trailing bytes"),
            Self::Truncated => formatter.write_str("branch catalog is truncated"),
            Self::Version => formatter.write_str("unsupported branch catalog version"),
        }
    }
}

impl std::error::Error for CatalogError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    static DIRECTORY_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    fn id(byte: u8) -> BranchId {
        BranchId::new(Uuid::from_bytes([byte; 16])).unwrap()
    }

    fn record(byte: u8, name: &str) -> BranchRecord {
        BranchRecord {
            id: id(byte),
            name: BranchName::new(name).unwrap(),
            parent_id: None,
            source_commit_epoch: 7,
            base_root_digest: Some([byte; DIGEST_BYTES]),
            metadata_revision: 3,
            state: BranchState::Ready,
            owner: Some("agent-1".to_string()),
            expires_at_unix_seconds: Some(42),
            create_request_key: format!("request-{byte}"),
            request_fingerprint: [byte.wrapping_add(1); DIGEST_BYTES],
            create_outcome: CreateOutcome::Succeeded,
        }
    }

    fn catalog() -> Catalog {
        Catalog {
            project_id: id(99),
            revision: 11,
            branches: vec![record(2, "second"), record(1, "first")],
        }
    }

    fn temporary_catalog_path() -> (PathBuf, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-branch-catalog-{}-{}",
            std::process::id(),
            DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("catalog.hawdb");
        (directory, path)
    }

    #[test]
    fn names_enforce_the_catalog_path_independent_contract() {
        for value in [
            "", "main", "agent/x", "/x", "x/", "x//y", "x/../y", "x/y\\z", "x y",
        ] {
            assert!(BranchName::new(value).is_err(), "accepted {value:?}");
        }
        for value in ["Foo", "foo", "agentx", "a/b.c_2"] {
            assert!(BranchName::new(value).is_ok(), "rejected {value:?}");
        }
        assert!(BranchName::new("agent/generated").is_err());
    }

    #[test]
    fn catalog_codec_accepts_reserved_engine_names_only_in_catalog_records() {
        let mut main = record(1, "ordinary-main");
        main.name = BranchName::from_encoded("main".to_string()).unwrap();
        main.parent_id = None;
        let mut generated = record(2, "generated");
        generated.name =
            BranchName::from_encoded(format!("agent/{}", generated.id.as_uuid())).unwrap();
        generated.parent_id = Some(main.id);
        let catalog = Catalog {
            project_id: id(99),
            revision: 1,
            branches: vec![main, generated],
        };
        let encoded = catalog.encode().unwrap();
        assert_eq!(Catalog::decode(&encoded).unwrap().branches.len(), 2);
    }

    #[test]
    fn codec_is_deterministic_and_round_trips_unsorted_records() {
        let catalog = catalog();
        let mut reversed = catalog.clone();
        reversed.branches.reverse();
        let encoded = catalog.encode().unwrap();
        assert_eq!(encoded, reversed.encode().unwrap());
        let mut expected = catalog.clone();
        expected.branches.sort_by_key(|branch| branch.id);
        assert_eq!(Catalog::decode(&encoded).unwrap(), expected);
    }

    #[test]
    fn file_publication_syncs_and_reopens_the_canonical_catalog() {
        let (directory, path) = temporary_catalog_path();
        let catalog = catalog();
        write_catalog(&path, &catalog).unwrap();
        assert_eq!(read_catalog(&path).unwrap().revision, catalog.revision);
        assert!(directory
            .read_dir()
            .unwrap()
            .all(|entry| entry.unwrap().file_name() == "catalog.hawdb"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn file_publication_failure_keeps_previous_bytes_and_cleans_candidate() {
        let (directory, path) = temporary_catalog_path();
        let first = catalog();
        write_catalog(&path, &first).unwrap();
        let before = fs::read(&path).unwrap();
        let _failure = durability::fail_durable_replace_for_destination("catalog.hawdb");
        assert!(write_catalog(
            &path,
            &Catalog {
                revision: 12,
                ..first
            }
        )
        .is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(directory
            .read_dir()
            .unwrap()
            .all(|entry| entry.unwrap().file_name() == "catalog.hawdb"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn read_rejects_an_oversized_catalog_before_loading_bytes() {
        let (directory, path) = temporary_catalog_path();
        let file = fs::File::create(&path).unwrap();
        file.set_len((MAX_CATALOG_BYTES + 1) as u64).unwrap();
        let error = read_catalog(&path).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn codec_rejects_tampering_versions_duplicates_and_trailing_bytes() {
        let catalog = Catalog {
            project_id: id(99),
            revision: 1,
            branches: vec![record(1, "one")],
        };
        let encoded = catalog.encode().unwrap();
        let mut tampered = encoded.clone();
        tampered[10] ^= 1;
        assert_eq!(Catalog::decode(&tampered), Err(CatalogError::Checksum));

        let mut trailing = encoded.clone();
        trailing.insert(trailing.len() - 4, 0);
        let checksum = crc32c(&trailing[..trailing.len() - 4]).get();
        let checksum_offset = trailing.len() - 4;
        trailing[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
        assert_eq!(Catalog::decode(&trailing), Err(CatalogError::TrailingBytes));

        let duplicate = Catalog {
            project_id: id(99),
            revision: 1,
            branches: vec![record(1, "one"), record(1, "two")],
        };
        assert_eq!(
            duplicate.encode(),
            Err(CatalogError::Duplicate("branch UUID"))
        );
    }

    #[test]
    fn invalid_uuid_and_deleted_pending_state_fail_closed() {
        assert!(BranchId::new(Uuid::nil()).is_err());
        assert!(BranchId::parse("not-a-uuid").is_err());
        let mut branch = record(1, "one");
        branch.state = BranchState::Deleted;
        branch.create_outcome = CreateOutcome::Pending;
        let catalog = Catalog {
            project_id: id(99),
            revision: 1,
            branches: vec![branch],
        };
        assert_eq!(
            catalog.encode(),
            Err(CatalogError::InvalidState(
                "deleted branch cannot have a pending create outcome"
            ))
        );
    }
}
