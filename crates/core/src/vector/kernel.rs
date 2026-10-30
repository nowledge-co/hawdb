// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded exact arithmetic. SIMD products retain coordinate-order f64 sums.

use super::{invalid, Result};
use std::sync::OnceLock;

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_endian = "little"
))]
mod avx2;
#[cfg(test)]
mod measurement;
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
mod neon;
#[cfg(test)]
mod tests;

const WORK_COORDINATES: usize = 64 * 1024 / (2 * size_of::<f32>());

#[derive(Clone, Copy)]
pub(super) enum Input<'a> {
    Slice(&'a [f32]),
    Encoded(&'a [u8]),
}

impl Input<'_> {
    pub(super) fn len(self) -> usize {
        match self {
            Self::Slice(values) => values.len(),
            Self::Encoded(bytes) => bytes.len() / 4,
        }
    }

    fn block(self, start: usize, end: usize) -> Self {
        match self {
            Self::Slice(values) => Self::Slice(&values[start..end]),
            Self::Encoded(bytes) => Self::Encoded(&bytes[start * 4..end * 4]),
        }
    }

    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"),
        target_endian = "little"
    ))]
    fn get(self, index: usize) -> f32 {
        match self {
            Self::Slice(values) => values[index],
            Self::Encoded(bytes) => f32::from_le_bytes(
                bytes[index * 4..index * 4 + 4]
                    .try_into()
                    .expect("coordinate"),
            ),
        }
    }

    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"),
        target_endian = "little"
    ))]
    fn as_ptr(self) -> *const f32 {
        match self {
            Self::Slice(values) => values.as_ptr(),
            Self::Encoded(bytes) => bytes.as_ptr().cast(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Cosine = 0,
    PreparedCosine = 1,
    Euclidean = 2,
    Norm = 3,
}

#[derive(Default)]
pub(super) struct Sums {
    pub dot: f64,
    pub left: f64,
    pub right: f64,
    pub squared: f64,
}

type Function = unsafe fn(Input<'_>, Input<'_>, Mode, &mut Sums) -> bool;

/// A function whose CPU requirements were checked by the constructor.
#[derive(Clone, Copy)]
pub(super) struct Kernel(Function, pub(super) &'static str);

impl Kernel {
    #[inline]
    pub(super) fn get() -> Self {
        static KERNEL: OnceLock<Kernel> = OnceLock::new();
        *KERNEL.get_or_init(|| {
            #[cfg(all(
                any(target_arch = "x86", target_arch = "x86_64"),
                target_endian = "little"
            ))]
            if std::is_x86_feature_detected!("avx2") {
                return Self(avx2::run, "avx2");
            }
            #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
            if std::arch::is_aarch64_feature_detected!("neon") {
                return Self(neon::run, "neon");
            }
            Self(scalar, "scalar")
        })
    }

    #[inline]
    pub(super) fn for_mode(mode: Mode, dimension: usize) -> Self {
        // Dispatch overhead dominates tiny vectors. On native AArch64, ordered
        // single-accumulator L2/normalization did not beat the portable loop.
        if dimension < 16 {
            return Self(scalar, "scalar");
        }
        #[cfg(target_arch = "aarch64")]
        if matches!(mode, Mode::Euclidean | Mode::Norm) {
            return Self(scalar, "scalar");
        }
        let _ = mode;
        Self::get()
    }

    #[inline]
    pub(super) fn calculate(
        self,
        left: Input<'_>,
        right: Input<'_>,
        mode: Mode,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<Sums> {
        assert_eq!(left.len(), right.len());
        let mut sums = Sums::default();
        if left.len() != 0 && left.len() < 16 {
            checkpoint()?;
            // SAFETY: portable scalar code has no ISA precondition; lengths match.
            if !unsafe { scalar(left, right, mode, &mut sums) } {
                return Err(invalid("coordinates must be finite"));
            }
            checkpoint()?;
            return Ok(sums);
        }
        for start in (0..left.len()).step_by(WORK_COORDINATES) {
            checkpoint()?;
            let end = left.len().min(start + WORK_COORDINATES);
            // SAFETY: construction checked CPU support; blocks have equal lengths.
            // Inputs are initialized f32 or complete little-endian coordinates.
            if !unsafe {
                (self.0)(
                    left.block(start, end),
                    right.block(start, end),
                    mode,
                    &mut sums,
                )
            } {
                return Err(invalid("coordinates must be finite"));
            }
        }
        checkpoint()?;
        Ok(sums)
    }
}

unsafe fn scalar(left: Input<'_>, right: Input<'_>, mode: Mode, sums: &mut Sums) -> bool {
    let decode = |bytes: &[u8]| f32::from_le_bytes(bytes.try_into().expect("coordinate"));
    match (left, right) {
        (Input::Slice(left), Input::Slice(right)) => {
            scalar_values(left.iter().copied(), right.iter().copied(), mode, sums)
        }
        (Input::Slice(left), Input::Encoded(right)) => scalar_values(
            left.iter().copied(),
            right.chunks_exact(4).map(decode),
            mode,
            sums,
        ),
        (Input::Encoded(left), Input::Slice(right)) => scalar_values(
            left.chunks_exact(4).map(decode),
            right.iter().copied(),
            mode,
            sums,
        ),
        (Input::Encoded(left), Input::Encoded(right)) => scalar_values(
            left.chunks_exact(4).map(decode),
            right.chunks_exact(4).map(decode),
            mode,
            sums,
        ),
    }
}

fn scalar_values(
    left: impl Iterator<Item = f32>,
    right: impl Iterator<Item = f32>,
    mode: Mode,
    sums: &mut Sums,
) -> bool {
    // Select the operation outside the coordinate loop. Keep this portable
    // reference as direct arithmetic, without a per-coordinate helper/dispatch.
    match mode {
        Mode::Cosine => {
            for (left, right) in left.zip(right) {
                if !left.is_finite() || !right.is_finite() {
                    return false;
                }
                let (left, right) = (f64::from(left), f64::from(right));
                sums.dot += left * right;
                sums.left += left * left;
                sums.right += right * right;
            }
        }
        Mode::PreparedCosine => {
            for (left, right) in left.zip(right) {
                if !left.is_finite() || !right.is_finite() {
                    return false;
                }
                let (left, right) = (f64::from(left), f64::from(right));
                sums.dot += left * right;
                sums.right += right * right;
            }
        }
        Mode::Euclidean => {
            for (left, right) in left.zip(right) {
                if !left.is_finite() || !right.is_finite() {
                    return false;
                }
                let (left, right) = (f64::from(left), f64::from(right));
                sums.squared += (left - right).powi(2);
            }
        }
        Mode::Norm => {
            for (left, right) in left.zip(right) {
                if !left.is_finite() || !right.is_finite() {
                    return false;
                }
                let left = f64::from(left);
                sums.left += left * left;
            }
        }
    }
    true
}

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"),
    target_endian = "little"
))]
fn scalar_coordinate(left: f32, right: f32, mode: Mode, sums: &mut Sums) -> bool {
    if !left.is_finite() || !right.is_finite() {
        return false;
    }
    let (left, right) = (f64::from(left), f64::from(right));
    match mode {
        Mode::Cosine => {
            sums.dot += left * right;
            sums.left += left * left;
            sums.right += right * right;
        }
        Mode::PreparedCosine => {
            sums.dot += left * right;
            sums.right += right * right;
        }
        Mode::Euclidean => sums.squared += (left - right).powi(2),
        Mode::Norm => sums.left += left * left,
    }
    true
}
