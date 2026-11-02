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

//! Bounded exact decimal values. Arithmetic intermediates have at most 114
//! digits; stored values never own an arbitrary-precision allocation.

use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalError {
    InvalidLiteral,
    InvalidType,
    InvalidEncoding,
    Overflow,
    DivisionByZero,
}

impl Display for DecimalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLiteral => "invalid or overlong decimal literal",
            Self::InvalidType => "decimal requires precision 1..38 and scale 0..precision",
            Self::InvalidEncoding => "invalid decimal encoding",
            Self::Overflow => "decimal result exceeds 38 coefficient digits or scale 38",
            Self::DivisionByZero => "decimal division by zero",
        })
    }
}

impl std::error::Error for DecimalError {}

/// A validated SQL DECIMAL(precision, scale) declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DecimalType {
    precision: u8,
    scale: u8,
}

impl DecimalType {
    pub const DEFAULT: Self = Self {
        precision: 38,
        scale: 0,
    };

    pub const fn new(precision: u8, scale: u8) -> Result<Self, DecimalError> {
        if precision == 0 || precision > Decimal::MAX_PRECISION || scale > precision {
            return Err(DecimalError::InvalidType);
        }
        Ok(Self { precision, scale })
    }

    pub const fn precision(self) -> u8 {
        self.precision
    }
    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Round to the declared scale, then enforce precision on the rounded value.
    pub fn coerce(self, value: Decimal) -> Result<Decimal, DecimalError> {
        let value = value.rescale(self.scale)?;
        if value.precision() > self.precision {
            return Err(DecimalError::Overflow);
        }
        Ok(value)
    }

    pub const fn accepts(self, value: Decimal) -> bool {
        value.scale == self.scale && value.precision() <= self.precision
    }
}

impl Display for DecimalType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DECIMAL({},{})", self.precision, self.scale)
    }
}

/// An exact signed coefficient times 10 to the negative scale.
///
/// Coefficients have at most 38 digits and scales are 0..38. Add, subtract,
/// multiply and remainder are exact or fail; division and explicit rescaling
/// round to nearest with midpoint away from zero. Equality, hashing and ordering
/// are numeric: 1.0 and 1.00 are equal, although display retains their scales.
#[derive(Clone, Copy)]
pub struct Decimal {
    // Byte storage avoids imposing i128 alignment on every Value enum slot.
    coefficient: [u8; 16],
    scale: u8,
}

impl Decimal {
    pub const MAX_PRECISION: u8 = 38;
    pub const MAX_COEFFICIENT: i128 = 10_i128.pow(38) - 1;
    pub const ZERO: Self = Self {
        coefficient: [0; 16],
        scale: 0,
    };
    pub const ENCODED_LEN: usize = 17;
    pub const ORDERED_LEN: usize = 40;
    pub const MAX_LITERAL_BYTES: usize = 256;

    pub const fn new(coefficient: i128, scale: u8) -> Result<Self, DecimalError> {
        if coefficient.unsigned_abs() > Self::MAX_COEFFICIENT as u128 || scale > 38 {
            return Err(DecimalError::Overflow);
        }
        Ok(Self {
            coefficient: coefficient.to_le_bytes(),
            scale,
        })
    }

    pub const fn coefficient(self) -> i128 {
        i128::from_le_bytes(self.coefficient)
    }
    pub const fn scale(self) -> u8 {
        self.scale
    }

    pub const fn precision(self) -> u8 {
        let mut value = self.coefficient().unsigned_abs();
        let mut digits = 1;
        while value >= 10 {
            value /= 10;
            digits += 1;
        }
        digits
    }

    /// The smallest declaration that retains this value's scale.
    pub const fn decimal_type(self) -> DecimalType {
        let precision = self.precision();
        DecimalType {
            precision: if precision > self.scale {
                precision
            } else {
                self.scale
            },
            scale: self.scale,
        }
    }

    pub const fn checked_neg(self) -> Self {
        Self {
            coefficient: (-self.coefficient()).to_le_bytes(),
            scale: self.scale,
        }
    }

    fn exact(mut coefficient: BigInt, mut scale: u32) -> Result<Self, DecimalError> {
        let limit = BigInt::from(Self::MAX_COEFFICIENT);
        while (scale > 38 || coefficient.abs() > limit) && scale > 0 {
            if !(&coefficient % 10_u8).is_zero() {
                return Err(DecimalError::Overflow);
            }
            coefficient /= 10_u8;
            scale -= 1;
        }
        Self::new(
            coefficient.to_i128().ok_or(DecimalError::Overflow)?,
            scale.try_into().map_err(|_| DecimalError::Overflow)?,
        )
    }

    fn aligned(self, other: Self) -> (BigInt, BigInt, u8) {
        let scale = self.scale.max(other.scale);
        (
            BigInt::from(self.coefficient()) * ten(u32::from(scale - self.scale)),
            BigInt::from(other.coefficient()) * ten(u32::from(scale - other.scale)),
            scale,
        )
    }

    pub fn checked_add(self, other: Self) -> Result<Self, DecimalError> {
        let (left, right, scale) = self.aligned(other);
        Self::exact(left + right, scale.into())
    }

    pub fn checked_sub(self, other: Self) -> Result<Self, DecimalError> {
        self.checked_add(other.checked_neg())
    }

    pub fn checked_mul(self, other: Self) -> Result<Self, DecimalError> {
        Self::exact(
            BigInt::from(self.coefficient()) * BigInt::from(other.coefficient()),
            u32::from(self.scale) + u32::from(other.scale),
        )
    }

    pub fn checked_rem(self, other: Self) -> Result<Self, DecimalError> {
        if other.coefficient() == 0 {
            return Err(DecimalError::DivisionByZero);
        }
        let (left, right, scale) = self.aligned(other);
        Self::exact(left % right, scale.into())
    }

    /// Round once to at most 38 coefficient digits and scale at most 38.
    /// Very small quotients may round to zero at scale 38.
    pub fn checked_div(self, other: Self) -> Result<Self, DecimalError> {
        if other.coefficient() == 0 {
            return Err(DecimalError::DivisionByZero);
        }
        let numerator = BigInt::from(self.coefficient()) * ten(other.scale.into());
        let denominator = BigInt::from(other.coefficient()) * ten(self.scale.into());
        let integral = (&numerator / &denominator).abs();
        let digits = if integral.is_zero() {
            0
        } else {
            integral.to_string().len()
        };
        if digits > 38 {
            return Err(DecimalError::Overflow);
        }
        let mut scale = 38 - digits as u32;
        loop {
            let scaled = &numerator * ten(scale);
            let mut quotient = &scaled / &denominator;
            let remainder = &scaled % &denominator;
            if remainder.abs() * 2_u8 >= denominator.abs() {
                quotient += if scaled.sign() == denominator.sign() {
                    1_i8
                } else {
                    -1
                };
            }
            if quotient.abs() <= BigInt::from(Self::MAX_COEFFICIENT) {
                return Self::new(
                    quotient.to_i128().ok_or(DecimalError::Overflow)?,
                    scale as u8,
                );
            }
            if scale == 0 {
                return Err(DecimalError::Overflow);
            }
            // Recompute from the original rational to avoid double rounding.
            scale -= 1;
        }
    }

    pub fn rescale(self, scale: u8) -> Result<Self, DecimalError> {
        if scale > 38 {
            return Err(DecimalError::Overflow);
        }
        if scale >= self.scale {
            return Self::new(
                (BigInt::from(self.coefficient()) * ten(u32::from(scale - self.scale)))
                    .to_i128()
                    .ok_or(DecimalError::Overflow)?,
                scale,
            );
        }
        let divisor = 10_i128.pow(u32::from(self.scale - scale));
        let mut quotient = self.coefficient() / divisor;
        // Multiplying the remainder by two could overflow i128.
        if (self.coefficient() % divisor).unsigned_abs() >= (divisor / 2) as u128 {
            quotient += self.coefficient().signum();
        }
        Self::new(quotient, scale)
    }

    /// Exact integral conversion, rejecting fractions and BIGINT overflow.
    pub fn to_i64(self) -> Result<i64, DecimalError> {
        let divisor = 10_i128.pow(self.scale.into());
        let coefficient = self.coefficient();
        if coefficient % divisor != 0 {
            return Err(DecimalError::Overflow);
        }
        (coefficient / divisor)
            .try_into()
            .map_err(|_| DecimalError::Overflow)
    }

    pub fn to_le_bytes(self) -> [u8; Self::ENCODED_LEN] {
        let mut bytes = [0; Self::ENCODED_LEN];
        bytes[..16].copy_from_slice(&self.coefficient);
        bytes[16] = self.scale;
        bytes
    }

    pub fn from_le_bytes(bytes: [u8; Self::ENCODED_LEN]) -> Result<Self, DecimalError> {
        Self::new(
            i128::from_le_bytes(bytes[..16].try_into().expect("fixed coefficient width")),
            bytes[16],
        )
        .map_err(|_| DecimalError::InvalidEncoding)
    }

    /// Canonical, fixed-width numeric order. Existing scalar tags are external.
    pub fn to_ordered_bytes(self) -> [u8; Self::ORDERED_LEN] {
        let mut encoded = [0; Self::ORDERED_LEN];
        let coefficient = self.coefficient();
        if coefficient == 0 {
            encoded[0] = 1;
            return encoded;
        }
        let mut value = coefficient.unsigned_abs();
        let mut digits = [0; 38];
        let mut first = 38;
        while value != 0 {
            first -= 1;
            digits[first] = (value % 10) as u8;
            value /= 10;
        }
        let count = 38 - first;
        encoded[0] = if coefficient < 0 { 0 } else { 2 };
        encoded[1] = (count + 38 - usize::from(self.scale)) as u8;
        encoded[2..2 + count].copy_from_slice(&digits[first..]);
        if coefficient < 0 {
            encoded[1..].iter_mut().for_each(|byte| *byte = !*byte);
        }
        encoded
    }

    pub fn from_ordered_bytes(mut encoded: [u8; Self::ORDERED_LEN]) -> Result<Self, DecimalError> {
        match encoded[0] {
            0 => encoded[1..].iter_mut().for_each(|byte| *byte = !*byte),
            1 if encoded[1..].iter().all(|byte| *byte == 0) => return Ok(Self::ZERO),
            2 => (),
            _ => return Err(DecimalError::InvalidEncoding),
        }
        if !(1..=76).contains(&encoded[1])
            || encoded[2] == 0
            || encoded[2..].iter().any(|byte| *byte > 9)
        {
            return Err(DecimalError::InvalidEncoding);
        }
        let mut coefficient = 0_i128;
        for digit in &encoded[2..] {
            coefficient = coefficient * 10 + i128::from(*digit);
        }
        if encoded[0] == 0 {
            coefficient = -coefficient;
        }
        let mut scale = 76 - encoded[1];
        while scale > 38 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        Self::new(coefficient, scale).map_err(|_| DecimalError::InvalidEncoding)
    }

    /// Next value in the entire bounded Decimal domain, for exclusive prefix
    /// bounds. Adding one unit of the input scale would skip representable keys.
    pub fn next_up(self) -> Option<Self> {
        let mut scaled = BigInt::from(self.coefficient()) * ten(u32::from(38 - self.scale));
        for scale in (0..=38).rev() {
            let candidate = &scaled + 1_u8;
            if candidate.abs() <= BigInt::from(Self::MAX_COEFFICIENT) {
                return Self::new(candidate.to_i128()?, scale).ok();
            }
            let negative_fraction = scaled.is_negative() && !(&scaled % 10_u8).is_zero();
            scaled /= 10_u8;
            if negative_fraction {
                scaled -= 1_u8;
            }
        }
        None
    }

    fn normalized(self) -> (i128, u8) {
        let mut coefficient = self.coefficient();
        let mut scale = self.scale;
        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        (coefficient, scale)
    }
}

fn ten(power: u32) -> BigInt {
    BigInt::from(10_u8).pow(power)
}

impl From<i64> for Decimal {
    fn from(value: i64) -> Self {
        Self {
            coefficient: i128::from(value).to_le_bytes(),
            scale: 0,
        }
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.normalized() == other.normalized()
    }
}
impl Eq for Decimal {}
impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_ordered_bytes().cmp(&other.to_ordered_bytes())
    }
}
impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.normalized().hash(state);
    }
}
impl fmt::Debug for Decimal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Decimal({self})")
    }
}
impl Display for Decimal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let coefficient = self.coefficient();
        if coefficient < 0 {
            f.write_str("-")?;
        }
        let magnitude = coefficient.unsigned_abs();
        if self.scale == 0 {
            return write!(f, "{magnitude}");
        }
        let divisor = 10_u128.pow(self.scale.into());
        write!(
            f,
            "{}.{:0width$}",
            magnitude / divisor,
            magnitude % divisor,
            width = usize::from(self.scale)
        )
    }
}

impl FromStr for Decimal {
    type Err = DecimalError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.is_empty() || text.len() > Self::MAX_LITERAL_BYTES {
            return Err(DecimalError::InvalidLiteral);
        }
        let (negative, unsigned) = match text.as_bytes()[0] {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(at) => {
                let exponent = unsigned[at + 1..]
                    .parse::<i16>()
                    .map_err(|_| DecimalError::InvalidLiteral)?;
                if exponent.unsigned_abs() > 1024 {
                    return Err(DecimalError::Overflow);
                }
                (&unsigned[..at], exponent)
            }
            None => (unsigned, 0),
        };
        let mut digits = [0_u8; Self::MAX_LITERAL_BYTES];
        let mut count = 0;
        let mut fraction = 0_i16;
        let mut point = false;
        for byte in mantissa.bytes() {
            match byte {
                b'.' if !point => point = true,
                b'0'..=b'9' => {
                    digits[count] = byte - b'0';
                    count += 1;
                    fraction += i16::from(point);
                }
                _ => return Err(DecimalError::InvalidLiteral),
            }
        }
        if count == 0 {
            return Err(DecimalError::InvalidLiteral);
        }
        let mut scale = fraction - exponent;
        let Some(first) = digits[..count].iter().position(|digit| *digit != 0) else {
            return Self::new(0, scale.clamp(0, 38) as u8);
        };
        while scale > 0 && (count - first > 38 || scale > 38) && digits[count - 1] == 0 {
            count -= 1;
            scale -= 1;
        }
        if count - first > 38
            || scale > 38
            || i32::try_from(count - first).expect("bounded literal") - i32::from(scale.min(0)) > 38
        {
            return Err(DecimalError::Overflow);
        }
        let mut coefficient = 0_i128;
        for digit in &digits[first..count] {
            coefficient = coefficient * 10 + i128::from(*digit);
        }
        if scale < 0 {
            coefficient *= 10_i128.pow(u32::from(scale.unsigned_abs()));
            scale = 0;
        }
        Self::new(
            if negative { -coefficient } else { coefficient },
            scale as u8,
        )
    }
}

#[cfg(test)]
mod tests;
