//! Standalone columnar node-group format layer.
//!
//! Implements §3.1–§3.3 and §3.5 of
//! `docs/specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md`: physical column
//! chunk encodings over the logical `Value` types, an id-ordered node-group
//! artifact with a fixed-layout directory and checksummed footer, per-chunk
//! zone maps reusing the `FieldSummary` machinery, and a generation-bound
//! deletion vector sidecar. This is a pure format layer: nothing here is
//! wired into `GraphStore`, checkpointing, or recovery.

pub mod encoding;
pub mod group;
pub mod zone;

use skein_core::PropertyId;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Magic bytes framing a column group artifact (header and footer).
pub const COLUMN_GROUP_MAGIC: &[u8; 9] = b"SKNCOLG01";
/// Magic bytes framing a deletion vector sidecar (header and footer).
pub const DELETION_VECTOR_MAGIC: &[u8; 9] = b"SKNCOLDV1";

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
    DeletionVectorMismatch {
        expected_group: u64,
        actual_group: u64,
        expected_generation: u64,
        actual_generation: u64,
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
            Self::DeletionVectorMismatch {
                expected_group,
                actual_group,
                expected_generation,
                actual_generation,
            } => write!(
                formatter,
                "deletion vector is bound to group {actual_group} generation \
                 {actual_generation}, not group {expected_group} generation \
                 {expected_generation}"
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
