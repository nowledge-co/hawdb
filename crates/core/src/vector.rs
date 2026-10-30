// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Shared finite Float32 values and exact distance operators for SQL and Cypher.
//! The canonical payload is a little-endian u32 dimension followed by f32 bits.
//! Byte-backed storage permits unaligned, allocation-free views of row pages.

use crate::{HawDBError, Result, ValueRef, VectorDimension};

mod kernel;
use kernel::{Input, Kernel, Mode, Sums};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Vector {
    encoded: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VectorRef<'a> {
    encoded: &'a [u8],
    dimension: VectorDimension,
}

impl Vector {
    pub fn new(coordinates: &[f32]) -> Result<Self> {
        let dimension = u32::try_from(coordinates.len())
            .ok()
            .and_then(VectorDimension::new)
            .ok_or_else(|| invalid("dimension must be a positive u32"))?;
        if coordinates.iter().any(|value| !value.is_finite()) {
            return Err(invalid("coordinates must be finite"));
        }
        let bytes = coordinates
            .len()
            .checked_mul(4)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| invalid("payload length overflow"))?;
        let mut encoded = Vec::with_capacity(bytes);
        encoded.extend_from_slice(&dimension.get().to_le_bytes());
        for value in coordinates {
            encoded.extend_from_slice(&value.to_le_bytes());
        }
        Ok(Self { encoded })
    }

    /// Takes ownership of an already admitted payload without an additional copy.
    pub fn from_encoded(encoded: Vec<u8>) -> Result<Self> {
        Self::from_encoded_with_work(encoded, || Ok(()), |_| Ok(()), std::convert::identity)
    }

    /// Validates in bounded units before taking ownership, without rescanning.
    pub fn from_encoded_with_work<T, E>(
        encoded: Vec<u8>,
        start: impl FnMut() -> std::result::Result<T, E>,
        finish: impl FnMut(T) -> std::result::Result<(), E>,
        invalid: impl Fn(HawDBError) -> E,
    ) -> std::result::Result<Self, E> {
        VectorRef::from_encoded_with_work(&encoded, start, finish, invalid)?;
        Ok(Self { encoded })
    }

    pub fn dimension(&self) -> VectorDimension {
        VectorDimension::new(u32::from_le_bytes(
            self.encoded[..4].try_into().expect("validated dimension"),
        ))
        .expect("nonzero dimension")
    }
    pub fn retained_capacity(&self) -> usize {
        self.encoded.capacity()
    }

    pub fn as_ref(&self) -> VectorRef<'_> {
        VectorRef {
            dimension: self.dimension(),
            encoded: &self.encoded,
        }
    }

    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }
}

impl<'a> VectorRef<'a> {
    /// Validates length and finite components before any caller-owned allocation.
    pub fn from_encoded(encoded: &'a [u8]) -> Result<Self> {
        Self::from_encoded_with_work(encoded, || Ok(()), |_| Ok(()), std::convert::identity)
    }

    /// Each work unit scans at most 64 KiB; callbacks own admission and cancellation.
    pub fn from_encoded_with_work<T, E>(
        encoded: &'a [u8],
        mut start: impl FnMut() -> std::result::Result<T, E>,
        mut finish: impl FnMut(T) -> std::result::Result<(), E>,
        map_invalid: impl Fn(HawDBError) -> E,
    ) -> std::result::Result<Self, E> {
        let shape = || -> Result<VectorDimension> {
            let prefix: [u8; 4] = encoded
                .get(..4)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| invalid("truncated dimension"))?;
            let dimension = VectorDimension::new(u32::from_le_bytes(prefix))
                .ok_or_else(|| invalid("dimension must be positive"))?;
            let expected = usize::try_from(dimension.get())
                .ok()
                .and_then(|n| n.checked_mul(4))
                .and_then(|n| n.checked_add(4))
                .ok_or_else(|| invalid("payload length overflow"))?;
            if encoded.len() != expected {
                return Err(invalid("payload length does not match dimension"));
            }
            Ok(dimension)
        };
        let dimension = shape().map_err(&map_invalid)?;
        for block in encoded[4..].chunks(64 * 1024) {
            let unit = start()?;
            let finite = block.chunks_exact(4).all(|bytes| {
                f32::from_le_bytes(bytes.try_into().expect("fixed-width coordinate")).is_finite()
            });
            finish(unit)?;
            if !finite {
                return Err(map_invalid(invalid("coordinates must be finite")));
            }
        }
        Ok(Self { dimension, encoded })
    }

    pub const fn dimension(self) -> VectorDimension {
        self.dimension
    }
    pub const fn encoded(self) -> &'a [u8] {
        self.encoded
    }

    pub fn coordinates(self) -> impl ExactSizeIterator<Item = f32> + 'a {
        self.encoded[4..]
            .chunks_exact(4)
            .map(|bytes| f32::from_le_bytes(bytes.try_into().expect("fixed-width coordinate")))
    }

    /// The caller must admit the payload before materializing a borrowed value.
    pub fn to_owned(self) -> Vector {
        Vector {
            encoded: self.encoded.to_vec(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorDistance {
    Cosine,
    Euclidean,
}

impl VectorDistance {
    pub fn from_function_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("cosine_distance") {
            Some(Self::Cosine)
        } else if name.eq_ignore_ascii_case("l2_distance") {
            Some(Self::Euclidean)
        } else {
            None
        }
    }

    pub const fn function_name(self) -> &'static str {
        match self {
            Self::Cosine => "cosine_distance",
            Self::Euclidean => "l2_distance",
        }
    }

    pub fn distance(self, left: VectorRef<'_>, right: VectorRef<'_>) -> Result<f64> {
        self.distance_with_work(left, right, || Ok(()))
    }

    /// Cooperatively scores canonical values without copying their payloads.
    pub fn distance_with_work(
        self,
        left: VectorRef<'_>,
        right: VectorRef<'_>,
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<f64> {
        if left.dimension != right.dimension {
            return Err(invalid("distance dimensions must match"));
        }
        if distance_kernel_name(self, left.dimension.get() as usize) == "scalar" {
            return self.scalar_distance_coordinates(
                left.coordinates(),
                right.coordinates(),
                checkpoint,
            );
        }
        self.distance_inputs(
            Input::Encoded(&left.encoded[4..]),
            Input::Encoded(&right.encoded[4..]),
            checkpoint,
        )
    }

    /// Scores admitted raw projection vectors with the same mathematical
    /// contract as SQL/Cypher, without creating canonical payload copies.
    pub fn distance_slices_with_work(
        self,
        left: &[f32],
        right: &[f32],
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<f64> {
        if left.len() != right.len() {
            return Err(invalid("distance dimensions must match"));
        }
        if left.is_empty() || u32::try_from(left.len()).is_err() {
            return Err(invalid("dimension must be a positive u32"));
        }
        if distance_kernel_name(self, left.len()) == "scalar" {
            return self.scalar_distance_coordinates(
                left.iter().copied(),
                right.iter().copied(),
                checkpoint,
            );
        }
        self.distance_inputs(Input::Slice(left), Input::Slice(right), checkpoint)
    }

    /// Scores a raw query against a validated, byte-backed immutable vector
    /// without allocating an aligned coordinate copy.
    pub fn distance_slice_to_vector_with_work(
        self,
        left: &[f32],
        right: VectorRef<'_>,
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<f64> {
        if left.len() != right.dimension.get() as usize {
            return Err(invalid("distance dimensions must match"));
        }
        if distance_kernel_name(self, left.len()) == "scalar" {
            return self.scalar_distance_coordinates(
                left.iter().copied(),
                right.coordinates(),
                checkpoint,
            );
        }
        self.distance_inputs(
            Input::Slice(left),
            Input::Encoded(&right.encoded[4..]),
            checkpoint,
        )
    }

    fn distance_inputs(
        self,
        left: Input<'_>,
        right: Input<'_>,
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<f64> {
        let mode = match self {
            Self::Cosine => Mode::Cosine,
            Self::Euclidean => Mode::Euclidean,
        };
        let sums = Kernel::for_mode(mode, left.len()).calculate(left, right, mode, checkpoint)?;
        finish_distance(self, &sums, sums.left.sqrt())
    }

    fn scalar_distance_coordinates(
        self,
        left: impl Iterator<Item = f32>,
        right: impl Iterator<Item = f32>,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<f64> {
        let (mut dot, mut left_norm, mut right_norm, mut squared) =
            (0.0f64, 0.0f64, 0.0f64, 0.0f64);
        for (index, (left, right)) in left.zip(right).enumerate() {
            // Read at most 64 KiB across both operands between checkpoints.
            if index % (64 * 1024 / (2 * std::mem::size_of::<f32>())) == 0 {
                checkpoint()?;
            }
            if !left.is_finite() || !right.is_finite() {
                return Err(invalid("coordinates must be finite"));
            }
            let (left, right) = (f64::from(left), f64::from(right));
            match self {
                Self::Euclidean => squared += (left - right).powi(2),
                Self::Cosine => {
                    dot += left * right;
                    left_norm += left * left;
                    right_norm += right * right;
                }
            }
        }
        checkpoint()?;
        match self {
            Self::Euclidean => Ok(squared.sqrt()),
            Self::Cosine if left_norm == 0.0 || right_norm == 0.0 => {
                Err(invalid("cosine distance requires nonzero vectors"))
            }
            Self::Cosine => {
                Ok(1.0 - (dot / (left_norm.sqrt() * right_norm.sqrt())).clamp(-1.0, 1.0))
            }
        }
    }

    pub fn evaluate(self, left: ValueRef<'_>, right: ValueRef<'_>) -> Result<Option<f64>> {
        match (left, right) {
            (ValueRef::Null, _) | (_, ValueRef::Null) => Ok(None),
            (ValueRef::Vector(left), ValueRef::Vector(right)) => {
                self.distance(left, right).map(Some)
            }
            _ => Err(invalid("distance arguments must be vectors")),
        }
    }
}

/// Selected exact-arithmetic implementation, independent of ANN backend selection.
#[doc(hidden)]
#[inline]
pub fn distance_kernel_name(metric: VectorDistance, dimension: usize) -> &'static str {
    Kernel::for_mode(
        match metric {
            VectorDistance::Cosine => Mode::Cosine,
            VectorDistance::Euclidean => Mode::Euclidean,
        },
        dimension,
    )
    .1
}

/// Borrowed, validated query state shared by exact projection scoring and reranking.
/// Preparation retains no coordinate copies. Each distance keeps f64 arithmetic,
/// coordinate-order sums and the same bounded work callbacks as `VectorDistance`.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct PreparedVectorQuery<'a> {
    coordinates: &'a [f32],
    norm: f64,
    kernel: Kernel,
}

impl<'a> PreparedVectorQuery<'a> {
    pub fn new(coordinates: &'a [f32]) -> Result<Self> {
        Self::with_work(coordinates, || Ok(()))
    }

    pub fn with_work(
        coordinates: &'a [f32],
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<Self> {
        if coordinates.is_empty() || u32::try_from(coordinates.len()).is_err() {
            return Err(invalid("dimension must be a positive u32"));
        }
        let kernel = Kernel::for_mode(Mode::PreparedCosine, coordinates.len());
        let norm = Kernel::for_mode(Mode::Norm, coordinates.len())
            .calculate(
                Input::Slice(coordinates),
                Input::Slice(coordinates),
                Mode::Norm,
                checkpoint,
            )?
            .left
            .sqrt();
        Ok(Self {
            coordinates,
            norm,
            kernel,
        })
    }

    pub fn for_distance(
        metric: VectorDistance,
        coordinates: &'a [f32],
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<Self> {
        let query = Self::with_work(coordinates, checkpoint)?;
        if metric == VectorDistance::Cosine && query.norm == 0.0 {
            return Err(invalid("cosine distance requires nonzero vectors"));
        }
        Ok(query)
    }

    pub fn coordinates(&self) -> &'a [f32] {
        self.coordinates
    }

    pub fn distance_slices_with_work(
        &self,
        metric: VectorDistance,
        right: &[f32],
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<f64> {
        if self.coordinates.len() != right.len() {
            return Err(invalid("distance dimensions must match"));
        }
        // Tiny vectors cannot amortize the prepared-kernel wrapper. Keep the
        // direct ordered scalar path even when a query norm is available.
        if self.coordinates.len() < 16
            || (metric == VectorDistance::Euclidean
                && distance_kernel_name(metric, right.len()) == "scalar")
        {
            return metric.distance_slices_with_work(self.coordinates, right, checkpoint);
        }
        self.distance_input(metric, Input::Slice(right), checkpoint)
    }

    pub fn distance_to_vector_with_work(
        &self,
        metric: VectorDistance,
        right: VectorRef<'_>,
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<f64> {
        if self.coordinates.len() != right.dimension.get() as usize {
            return Err(invalid("distance dimensions must match"));
        }
        if self.coordinates.len() < 16
            || (metric == VectorDistance::Euclidean
                && distance_kernel_name(metric, self.coordinates.len()) == "scalar")
        {
            return metric.distance_slice_to_vector_with_work(self.coordinates, right, checkpoint);
        }
        self.distance_input(metric, Input::Encoded(&right.encoded[4..]), checkpoint)
    }

    fn distance_input(
        &self,
        metric: VectorDistance,
        right: Input<'_>,
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<f64> {
        let mode = match metric {
            VectorDistance::Cosine => Mode::PreparedCosine,
            VectorDistance::Euclidean => Mode::Euclidean,
        };
        let kernel = match metric {
            VectorDistance::Cosine => self.kernel,
            VectorDistance::Euclidean => Kernel::for_mode(mode, self.coordinates.len()),
        };
        let sums = kernel.calculate(Input::Slice(self.coordinates), right, mode, checkpoint)?;
        finish_distance(metric, &sums, self.norm)
    }

    /// Search's positive-only relevance, computed directly rather than as 1-distance
    /// so subtraction cannot change final score bits. Zero vectors have no score.
    pub fn positive_cosine_similarity(&self, right: &[f32]) -> Result<Option<f64>> {
        if self.coordinates.len() != right.len() {
            return Ok(None);
        }
        let sums = self.kernel.calculate(
            Input::Slice(self.coordinates),
            Input::Slice(right),
            Mode::PreparedCosine,
            || Ok(()),
        )?;
        if self.norm == 0.0 || sums.right == 0.0 {
            return Ok(None);
        }
        Ok(Some(
            (sums.dot / (self.norm * sums.right.sqrt())).clamp(0.0, 1.0),
        ))
    }
}

fn finish_distance(metric: VectorDistance, sums: &Sums, left_norm: f64) -> Result<f64> {
    match metric {
        VectorDistance::Euclidean => Ok(sums.squared.sqrt()),
        VectorDistance::Cosine if left_norm == 0.0 || sums.right == 0.0 => {
            Err(invalid("cosine distance requires nonzero vectors"))
        }
        VectorDistance::Cosine => {
            Ok(1.0 - (sums.dot / (left_norm * sums.right.sqrt())).clamp(-1.0, 1.0))
        }
    }
}

fn invalid(reason: &str) -> HawDBError {
    HawDBError::Semantic(format!("invalid vector: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_distance_preserves_signed_cosine_and_canonical_parity() {
        let query = [1.0, 0.0];
        let values = [[1.0, 0.0], [0.0, 1.0], [-0.6, 0.8], [-1.0, 0.0]];
        for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
            for coordinates in values {
                let canonical = metric
                    .distance(
                        Vector::new(&query).unwrap().as_ref(),
                        Vector::new(&coordinates).unwrap().as_ref(),
                    )
                    .unwrap();
                let projection = metric
                    .distance_slices_with_work(&query, &coordinates, || Ok(()))
                    .unwrap();
                assert_eq!(projection.to_bits(), canonical.to_bits());
                let encoded = Vector::new(&coordinates).unwrap();
                let mut unaligned = vec![0];
                unaligned.extend_from_slice(encoded.encoded());
                let mapped = metric
                    .distance_slice_to_vector_with_work(
                        &query,
                        VectorRef::from_encoded(&unaligned[1..]).unwrap(),
                        || Ok(()),
                    )
                    .unwrap();
                assert_eq!(mapped.to_bits(), canonical.to_bits());
            }
        }
        let distance = |coordinates: &[f32]| {
            VectorDistance::Cosine
                .distance_slices_with_work(&query, coordinates, || Ok(()))
                .unwrap()
        };
        assert!(distance(&values[2]) > 1.0);
        assert!(distance(&values[2]) < distance(&values[3]));
        assert_eq!(distance(&values[3]), 2.0);
        for coordinates in [&[][..], &[0.0][..], &[f32::NAN, 0.0][..], &[0.0, 0.0][..]] {
            assert!(VectorDistance::Cosine
                .distance_slices_with_work(&query, coordinates, || Ok(()))
                .is_err());
        }
    }

    #[test]
    fn projection_distance_preserves_cooperative_failure() {
        let coordinates = vec![1.0; 20_000];
        let mut checkpoints = 0;
        let error = VectorDistance::Cosine
            .distance_slices_with_work(&coordinates, &coordinates, || {
                checkpoints += 1;
                if checkpoints == 2 {
                    Err(HawDBError::ProjectionBusy)
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert!(matches!(error, HawDBError::ProjectionBusy));
        assert_eq!(checkpoints, 2);
        let encoded = Vector::new(&coordinates).unwrap();
        let error = VectorDistance::Cosine
            .distance_slice_to_vector_with_work(&coordinates, encoded.as_ref(), || {
                Err(HawDBError::ProjectionBusy)
            })
            .unwrap_err();
        assert!(matches!(error, HawDBError::ProjectionBusy));
        assert!(VectorDistance::Cosine
            .distance_slice_to_vector_with_work(&[1.0], encoded.as_ref(), || Ok(()))
            .is_err());
    }

    #[test]
    fn vector_encoding_rejects_malformed_and_nonfinite_payloads() {
        let value = Vector::new(&[1.0, -2.5]).unwrap();
        assert_eq!(value.encoded(), &[2, 0, 0, 0, 0, 0, 128, 63, 0, 0, 32, 192]);
        assert_eq!(
            Vector::from_encoded(value.encoded().to_vec()).unwrap(),
            value
        );
        let mut unaligned = vec![0];
        unaligned.extend_from_slice(value.encoded());
        assert_eq!(
            VectorRef::from_encoded(&unaligned[1..]).unwrap(),
            value.as_ref()
        );
        for size in 0..value.encoded().len() {
            assert!(VectorRef::from_encoded(&value.encoded()[..size]).is_err());
        }
        assert!(VectorRef::from_encoded(&[0, 0, 0, 0]).is_err());
        assert!(VectorRef::from_encoded(&u32::MAX.to_le_bytes()).is_err());
        for component in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(Vector::new(&[component]).is_err());
            let mut encoded = 1u32.to_le_bytes().to_vec();
            encoded.extend_from_slice(&component.to_le_bytes());
            assert!(VectorRef::from_encoded(&encoded).is_err());
        }
        assert!(Vector::new(&[]).is_err());
    }

    #[test]
    fn vector_distance_is_mathematical_and_shared() {
        let left = Vector::new(&[1.0, 0.0]).unwrap();
        let opposite = Vector::new(&[-1.0, 0.0]).unwrap();
        let zero = Vector::new(&[0.0, 0.0]).unwrap();
        assert_eq!(
            VectorDistance::Cosine
                .distance(left.as_ref(), opposite.as_ref())
                .unwrap(),
            2.0
        );
        assert_eq!(
            VectorDistance::Euclidean
                .distance(left.as_ref(), zero.as_ref())
                .unwrap(),
            1.0
        );
        assert!(VectorDistance::Cosine
            .distance(left.as_ref(), zero.as_ref())
            .is_err());
        assert!(VectorDistance::Euclidean
            .distance(left.as_ref(), Vector::new(&[1.0]).unwrap().as_ref())
            .is_err());
        assert_eq!(
            VectorDistance::Cosine
                .evaluate(ValueRef::Null, ValueRef::Vector(zero.as_ref()))
                .unwrap(),
            None
        );
        let large = Vector::new(&[f32::MAX, f32::MAX]).unwrap();
        assert!(VectorDistance::Euclidean
            .distance(large.as_ref(), zero.as_ref())
            .unwrap()
            .is_finite());
        assert!(
            VectorDistance::Cosine
                .distance(large.as_ref(), large.as_ref())
                .unwrap()
                .abs()
                < 1e-12
        );
    }
}

impl std::hash::Hash for VectorRef<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.encoded.hash(state);
    }
}

#[cfg(test)]
mod work_tests {
    use super::*;

    #[test]
    fn shared_vector_validation_stops_between_bounded_coordinate_blocks() {
        let vector = Vector::new(&vec![1.0; 32769]).unwrap();
        let calls = std::cell::Cell::new(0);
        let error = Vector::from_encoded_with_work(
            vector.encoded().to_vec(),
            || {
                calls.set(calls.get() + 1);
                if calls.get() == 2 {
                    Err("cancelled".to_owned())
                } else {
                    Ok(())
                }
            },
            |_| Ok(()),
            |error| error.to_string(),
        )
        .unwrap_err();
        assert_eq!(error, "cancelled");
        assert_eq!(calls.get(), 2);
        let mut encoded = vector.encoded().to_vec();
        let last = encoded.len() - 4;
        encoded[last..].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(Vector::from_encoded(encoded).is_err());
    }
}
