// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Schema-bound vector index definitions shared by SQL and Cypher.
//!
//! A descriptor is a definition, not serving authority. Publication identity,
//! source epoch and ordinal ownership belong to its registered Consumer pin.

use crate::{HawDBError, PropertyId, Result, TableId, VectorDimension, VectorDistance};

mod codec;

/// Upper bound used to admit descriptor encoding/decoding before allocation.
pub const MAX_VECTOR_INDEX_DESCRIPTOR_BYTES: usize = 4096;

/// Immutable source and publication identity of an eligible vector domain.
/// This is a query binding, never independent authority to publish or serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorIndexReadIdentity {
    pub database: crate::Uuid,
    pub branch: crate::Uuid,
    pub consumer_id_hash: [u8; 32],
    pub target_locator_hash: [u8; 32],
    pub registration: crate::Uuid,
    pub projection: crate::Uuid,
    pub checkpoint: crate::Uuid,
    pub target_incarnation: std::num::NonZeroU64,
    pub generation: std::num::NonZeroU64,
    pub operation: std::num::NonZeroU64,
    pub source_epoch: u64,
}

/// Query semantics, independent of the persistent index backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum VectorIndexQueryMode {
    /// Fix an approximate window before applying canonical query predicates.
    #[default]
    AnnWindow,
    /// Exhaust the declared eligible domain before selecting exact nearest rows.
    Exact,
    /// Restrict approximate candidate generation to a complete eligible domain.
    FilteredAnn,
}

impl VectorIndexQueryMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "ann_window" => Ok(Self::AnnWindow),
            "exact" => Ok(Self::Exact),
            "filtered_ann" => Ok(Self::FilteredAnn),
            _ => Err(invalid(
                "query mode must be 'ann_window', 'exact' or 'filtered_ann'",
            )),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AnnWindow => "ann_window",
            Self::Exact => "exact",
            Self::FilteredAnn => "filtered_ann",
        }
    }

    pub const fn requires_eligible_domain(self) -> bool {
        matches!(self, Self::Exact | Self::FilteredAnn)
    }

    pub const fn filter_position(self) -> &'static str {
        match self {
            Self::AnnWindow => "after_window",
            Self::Exact => "before_ranking",
            Self::FilteredAnn => "before_candidates",
        }
    }
}

/// Resolved field identity. Relational schemas currently identify columns by
/// normalized table name and ordinal; the descriptor's schema digest fences
/// reinterpretation of that ordinal after a schema change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorIndexTarget {
    Relational {
        table: String,
        column: String,
        column_ordinal: u32,
    },
    NodeProperty {
        table: String,
        property: String,
        table_id: TableId,
        property_id: PropertyId,
    },
}

/// Persistent algorithm settings, independent of per-operation admission.
/// Runtime memory limits and cancellation are supplied at build/query time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIndexBackend {
    DiskAnn {
        max_degree: u32,
        build_beam: u32,
        alpha_milli: u32,
        lsg: Option<VectorIndexLsg>,
    },
    RaBitQ {
        bit_width: u8,
        segment_rows: u32,
        transform_seed: u64,
    },
}

/// Integer-scaled persistent parameters have canonical equality and encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorIndexLsg {
    pub sample_size: u32,
    pub neighbors: u32,
    pub exponent_milli: u32,
}

impl Default for VectorIndexBackend {
    fn default() -> Self {
        Self::DiskAnn {
            max_degree: 32,
            build_beam: 128,
            alpha_milli: 1200,
            lsg: None,
        }
    }
}

impl VectorIndexBackend {
    pub const fn name(self) -> &'static str {
        match self {
            Self::DiskAnn { .. } => "diskann",
            Self::RaBitQ { .. } => "rabitq",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexDescriptor {
    name: String,
    target: VectorIndexTarget,
    dimension: VectorDimension,
    distance: VectorDistance,
    backend: VectorIndexBackend,
    schema_digest: [u8; 32],
}

impl VectorIndexDescriptor {
    #[doc(hidden)]
    pub fn retained_capacity_bytes(&self) -> usize {
        let (table, field) = match &self.target {
            VectorIndexTarget::Relational { table, column, .. } => (table, column),
            VectorIndexTarget::NodeProperty {
                table, property, ..
            } => (table, property),
        };
        std::mem::size_of::<Self>()
            .saturating_add(self.name.capacity())
            .saturating_add(table.capacity())
            .saturating_add(field.capacity())
    }

    /// Constructs a validated definition. The facade must resolve the target,
    /// dimension and digest against its pinned canonical schema before use.
    pub fn new(
        name: String,
        target: VectorIndexTarget,
        dimension: VectorDimension,
        distance: VectorDistance,
        backend: VectorIndexBackend,
        schema_digest: [u8; 32],
    ) -> Result<Self> {
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        {
            return Err(invalid(
                "name must be 1..128 ASCII letters, digits, '.', '_' or '-'",
            ));
        }
        let (table, field) = match &target {
            VectorIndexTarget::Relational { table, column, .. } => (table, column),
            VectorIndexTarget::NodeProperty {
                table, property, ..
            } => (table, property),
        };
        for name in [table, field] {
            if name.is_empty() || name.len() > 1024 || name.contains('\0') {
                return Err(invalid(
                    "target names must contain 1..1024 non-NUL UTF-8 bytes",
                ));
            }
        }
        match backend {
            VectorIndexBackend::RaBitQ {
                bit_width,
                segment_rows,
                ..
            } => {
                if !matches!(bit_width, 1 | 4) || segment_rows == 0 {
                    return Err(invalid(
                        "RaBitQ requires 1 or 4 bits and a positive segment size",
                    ));
                }
                if distance != VectorDistance::Cosine {
                    return Err(invalid(
                        "RaBitQ indexes currently support cosine distance only",
                    ));
                }
            }
            VectorIndexBackend::DiskAnn {
                max_degree,
                build_beam,
                alpha_milli,
                lsg,
            } => {
                if max_degree < 2 || build_beam < max_degree || alpha_milli < 1000 {
                    return Err(invalid(
                        "DiskANN requires degree >= 2, beam >= degree and alpha >= 1",
                    ));
                }
                if let Some(lsg) = lsg
                    && (lsg.sample_size < 2
                        || lsg.neighbors == 0
                        || lsg.neighbors > lsg.sample_size
                        || lsg.exponent_milli == 0
                        || lsg.exponent_milli > 2000)
                {
                    return Err(invalid("invalid DiskANN local scaling parameters"));
                }
            }
        }
        Ok(Self {
            name,
            target,
            dimension,
            distance,
            backend,
            schema_digest,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn target(&self) -> &VectorIndexTarget {
        &self.target
    }

    pub const fn dimension(&self) -> VectorDimension {
        self.dimension
    }

    pub const fn distance(&self) -> VectorDistance {
        self.distance
    }

    pub const fn backend(&self) -> VectorIndexBackend {
        self.backend
    }

    pub const fn schema_digest(&self) -> &[u8; 32] {
        &self.schema_digest
    }

    pub fn retained_bytes(&self) -> usize {
        let (table, field) = match &self.target {
            VectorIndexTarget::Relational { table, column, .. } => (table, column),
            VectorIndexTarget::NodeProperty {
                table, property, ..
            } => (table, property),
        };
        std::mem::size_of::<Self>() + self.name.capacity() + table.capacity() + field.capacity()
    }
}

fn invalid(reason: &str) -> HawDBError {
    HawDBError::Semantic(format!("invalid vector index: {reason}"))
}

// Retained query ownership: VectorIndexQueryMode.
crate::impl_retained_heap_for_copy!(VectorIndexQueryMode);
