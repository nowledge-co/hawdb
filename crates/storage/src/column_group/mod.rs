//! Standalone columnar node-group format layer.
//!
//! Implements §3.1–§3.3 and §3.5 of
//! `docs/specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md`: physical column
//! chunk encodings over the logical `Value` types, an id-ordered node-group
//! artifact with a fixed-layout directory and checksummed footer, per-chunk
//! zone maps reusing the `FieldSummary` machinery, and a generation-bound
//! deletion vector sidecar, and a standalone layered manifest publication
//! protocol. These types are not yet wired into `GraphStore` checkpointing;
//! their durable reopen contract is exercised independently first.

pub mod deletion;
pub mod encoding;
pub mod group;
pub mod manifest;
pub mod zone;

use skein_core::PropertyId;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::ManifestGeneration;

/// Magic bytes framing a column group artifact (header and footer).
pub const COLUMN_GROUP_MAGIC: &[u8; 9] = b"SKNCOLG01";
/// Magic bytes framing a deletion vector sidecar (header and footer).
pub const DELETION_VECTOR_MAGIC: &[u8; 9] = b"SKNCOLDV1";

/// Stable identity carried by one deletion-vector artifact.
///
/// `group_generation` identifies the immutable physical group whose row
/// ordinals the bitmap addresses. `publication_generation` identifies the
/// first manifest generation that published this cumulative bitmap. The two
/// generations deliberately differ when a checkpoint publishes new deletes
/// against an older group without rewriting its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletionVectorBinding {
    group_id: u64,
    group_generation: ManifestGeneration,
    publication_generation: ManifestGeneration,
    row_count: u32,
}

impl DeletionVectorBinding {
    pub fn new(
        group_id: u64,
        group_generation: ManifestGeneration,
        publication_generation: ManifestGeneration,
        row_count: u32,
    ) -> Result<Self, ColumnGroupError> {
        if publication_generation.0 < group_generation.0 {
            return Err(ColumnGroupError::Unsupported(format!(
                "deletion vector publication generation {} precedes physical group generation {}",
                publication_generation.0, group_generation.0
            )));
        }
        Ok(Self {
            group_id,
            group_generation,
            publication_generation,
            row_count,
        })
    }

    pub const fn group_id(self) -> u64 {
        self.group_id
    }

    pub const fn group_generation(self) -> ManifestGeneration {
        self.group_generation
    }

    pub const fn publication_generation(self) -> ManifestGeneration {
        self.publication_generation
    }

    pub const fn row_count(self) -> u32 {
        self.row_count
    }
}

/// Errors surfaced by the column group format layer.
///
/// Mirrors the `CanonicalSegmentError` conventions: `Corrupt` carries a
/// human-readable description of the integrity violation, and every decode
/// failure is an error, never a panic.
#[derive(Debug)]
pub enum ColumnGroupError {
    Io(std::io::Error),
    /// The caller handed the writer data the format cannot represent.
    Unsupported(String),
    /// The artifact bytes violate the format contract.
    Corrupt(String),
    /// A deletion vector was presented against a group or generation it is
    /// not bound to (§3.5.3(d)).
    DeletionVectorGroupMismatch {
        expected_group: u64,
        actual_group: u64,
        expected_group_generation: u64,
        actual_group_generation: u64,
    },
    /// A reader pinned to an older manifest was handed a deletion vector
    /// first published by a newer manifest.
    DeletionVectorFromFuture {
        snapshot_generation: u64,
        publication_generation: u64,
    },
    /// A manifest candidate was prepared from a generation that is no longer
    /// current. The caller must rebuild from the newly published generation.
    StaleManifestGeneration {
        expected_parent: Option<u64>,
        actual_parent: Option<u64>,
    },
    /// A requested property has no column chunk in the group.
    PropertyMissing(PropertyId),
    /// A requested row index is outside the group's row count.
    RowOutOfRange {
        row_index: u32,
        row_count: u32,
    },
}

impl Display for ColumnGroupError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Unsupported(message) | Self::Corrupt(message) => formatter.write_str(message),
            Self::DeletionVectorGroupMismatch {
                expected_group,
                actual_group,
                expected_group_generation,
                actual_group_generation,
            } => write!(
                formatter,
                "deletion vector addresses group {actual_group} created in generation \
                 {actual_group_generation}, not group {expected_group} created in generation \
                 {expected_group_generation}"
            ),
            Self::DeletionVectorFromFuture {
                snapshot_generation,
                publication_generation,
            } => write!(
                formatter,
                "deletion vector published in generation {publication_generation} cannot be \
                 read from snapshot generation {snapshot_generation}"
            ),
            Self::StaleManifestGeneration {
                expected_parent,
                actual_parent,
            } => write!(
                formatter,
                "column-group manifest was prepared from generation {expected_parent:?}, but the \
                 active parent is {actual_parent:?}"
            ),
            Self::PropertyMissing(property) => {
                write!(
                    formatter,
                    "column group has no chunk for property {}",
                    property.0
                )
            }
            Self::RowOutOfRange {
                row_index,
                row_count,
            } => write!(
                formatter,
                "row index {row_index} is outside the group row count {row_count}"
            ),
        }
    }
}

impl Error for ColumnGroupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ColumnGroupError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

fn corrupt(message: impl Into<String>) -> ColumnGroupError {
    ColumnGroupError::Corrupt(message.into())
}

fn unsupported(message: impl Into<String>) -> ColumnGroupError {
    ColumnGroupError::Unsupported(message.into())
}
