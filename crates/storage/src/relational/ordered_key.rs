use super::{RelationalKey, RelationalValue};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OrderedRelationalKeyError {
    UnsupportedOverflow,
    Corrupt(String),
}

impl fmt::Display for OrderedRelationalKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedOverflow => {
                formatter.write_str("ordered relational keys must not contain overflow references")
            }
            Self::Corrupt(message) => {
                write!(formatter, "corrupt ordered relational key: {message}")
            }
        }
    }
}

impl std::error::Error for OrderedRelationalKeyError {}

pub(super) fn encode_ordered_relational_key(
    key: &RelationalKey,
) -> Result<Vec<u8>, OrderedRelationalKeyError> {
    if key.0.is_empty() {
        return Err(OrderedRelationalKeyError::Corrupt(
            "key contains no values".to_string(),
        ));
    }
    let mut encoded = Vec::new();
    for value in &key.0 {
        encode_ordered_relational_value(&mut encoded, value)?;
    }
    Ok(encoded)
}

pub(super) fn encode_ordered_relational_value(
    encoded: &mut Vec<u8>,
    value: &RelationalValue,
) -> Result<(), OrderedRelationalKeyError> {
    match value {
        RelationalValue::Null => encoded.push(0),
        RelationalValue::Boolean(value) => {
            encoded.push(1);
            encoded.push(u8::from(*value));
        }
        RelationalValue::BigInt(value) => {
            encoded.push(2);
            encoded.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        RelationalValue::DoublePrecision(value) => {
            encoded.push(3);
            let bits = value.to_bits();
            let ordered = if bits >> 63 == 0 {
                bits ^ (1_u64 << 63)
            } else {
                !bits
            };
            encoded.extend_from_slice(&ordered.to_be_bytes());
        }
        RelationalValue::Text(value) => {
            encoded.push(4);
            encode_escaped_bytes(encoded, value.as_bytes());
        }
        RelationalValue::Bytea(value) => {
            encoded.push(5);
            encode_escaped_bytes(encoded, value);
        }
        RelationalValue::Overflow(_) => {
            return Err(OrderedRelationalKeyError::UnsupportedOverflow);
        }
    }
    Ok(())
}

pub(super) fn validate_ordered_relational_key(
    encoded: &[u8],
) -> Result<(), OrderedRelationalKeyError> {
    let mut decoder = OrderedKeyDecoder::new(encoded);
    let mut values = 0usize;
    while !decoder.is_empty() {
        let _ = decoder.skip_value()?;
        values = values.checked_add(1).ok_or_else(|| {
            OrderedRelationalKeyError::Corrupt("value count overflow".to_string())
        })?;
    }
    if values == 0 {
        return Err(OrderedRelationalKeyError::Corrupt(
            "key contains no values".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn ordered_relational_key_prefix_ends(
    encoded: &[u8],
    prefix_ends: &mut Vec<usize>,
) -> Result<usize, OrderedRelationalKeyError> {
    prefix_ends.clear();
    let mut decoder = OrderedKeyDecoder::new(encoded);
    let mut leading_non_null_values = 0usize;
    let mut saw_null = false;
    while !decoder.is_empty() {
        saw_null |= decoder.skip_value()?;
        if !saw_null {
            leading_non_null_values = leading_non_null_values.checked_add(1).ok_or_else(|| {
                OrderedRelationalKeyError::Corrupt("value count overflow".to_string())
            })?;
        }
        prefix_ends.push(decoder.offset);
    }
    if prefix_ends.is_empty() {
        return Err(OrderedRelationalKeyError::Corrupt(
            "key contains no values".to_string(),
        ));
    }
    Ok(leading_non_null_values)
}

pub(super) fn decode_ordered_relational_key(
    encoded: &[u8],
) -> Result<RelationalKey, OrderedRelationalKeyError> {
    let mut key = RelationalKey(Vec::new());
    decode_ordered_relational_key_into(encoded, &mut key)?;
    Ok(key)
}

pub(super) fn decode_ordered_relational_key_into(
    encoded: &[u8],
    key: &mut RelationalKey,
) -> Result<(), OrderedRelationalKeyError> {
    let mut decoder = OrderedKeyDecoder::new(encoded);
    let mut value_count = 0usize;
    while !decoder.is_empty() {
        if let Some(value) = key.0.get_mut(value_count) {
            decoder.value_into(value)?;
        } else {
            key.0.push(decoder.value()?);
        }
        value_count = value_count.checked_add(1).ok_or_else(|| {
            OrderedRelationalKeyError::Corrupt("value count overflow".to_string())
        })?;
    }
    if value_count == 0 {
        return Err(OrderedRelationalKeyError::Corrupt(
            "key contains no values".to_string(),
        ));
    }
    key.0.truncate(value_count);
    Ok(())
}

fn encode_escaped_bytes(encoded: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            encoded.extend_from_slice(&[0, 255]);
        } else {
            encoded.push(*byte);
        }
    }
    encoded.extend_from_slice(&[0, 0]);
}

struct OrderedKeyDecoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> OrderedKeyDecoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.encoded.len()
    }

    fn value(&mut self) -> Result<RelationalValue, OrderedRelationalKeyError> {
        match self.byte("value tag")? {
            0 => Ok(RelationalValue::Null),
            1 => match self.byte("boolean value")? {
                0 => Ok(RelationalValue::Boolean(false)),
                1 => Ok(RelationalValue::Boolean(true)),
                tag => Err(OrderedRelationalKeyError::Corrupt(format!(
                    "invalid boolean tag {tag}"
                ))),
            },
            2 => {
                let ordered = u64::from_be_bytes(self.fixed("BIGINT value")?);
                Ok(RelationalValue::BigInt((ordered ^ (1_u64 << 63)) as i64))
            }
            3 => {
                let ordered = u64::from_be_bytes(self.fixed("DOUBLE PRECISION value")?);
                let bits = if ordered >> 63 == 1 {
                    ordered ^ (1_u64 << 63)
                } else {
                    !ordered
                };
                Ok(RelationalValue::DoublePrecision(f64::from_bits(bits)))
            }
            4 => {
                let bytes = self.escaped_bytes(true)?;
                Ok(RelationalValue::Text(String::from_utf8(bytes).map_err(
                    |error| {
                        OrderedRelationalKeyError::Corrupt(format!(
                            "TEXT key is not valid UTF-8: {error}"
                        ))
                    },
                )?))
            }
            5 => Ok(RelationalValue::Bytea(self.escaped_bytes(true)?)),
            tag => Err(OrderedRelationalKeyError::Corrupt(format!(
                "invalid value tag {tag}"
            ))),
        }
    }

    fn value_into(
        &mut self,
        target: &mut RelationalValue,
    ) -> Result<(), OrderedRelationalKeyError> {
        match self.byte("value tag")? {
            0 => *target = RelationalValue::Null,
            1 => {
                *target = match self.byte("boolean value")? {
                    0 => RelationalValue::Boolean(false),
                    1 => RelationalValue::Boolean(true),
                    tag => {
                        return Err(OrderedRelationalKeyError::Corrupt(format!(
                            "invalid boolean tag {tag}"
                        )))
                    }
                };
            }
            2 => {
                let ordered = u64::from_be_bytes(self.fixed("BIGINT value")?);
                *target = RelationalValue::BigInt((ordered ^ (1_u64 << 63)) as i64);
            }
            3 => {
                let ordered = u64::from_be_bytes(self.fixed("DOUBLE PRECISION value")?);
                let bits = if ordered >> 63 == 1 {
                    ordered ^ (1_u64 << 63)
                } else {
                    !ordered
                };
                *target = RelationalValue::DoublePrecision(f64::from_bits(bits));
            }
            4 => {
                let mut bytes = match std::mem::replace(target, RelationalValue::Null) {
                    RelationalValue::Text(value) => value.into_bytes(),
                    _ => Vec::new(),
                };
                self.escaped_bytes_into(&mut bytes)?;
                *target = RelationalValue::Text(String::from_utf8(bytes).map_err(|error| {
                    OrderedRelationalKeyError::Corrupt(format!(
                        "TEXT key is not valid UTF-8: {error}"
                    ))
                })?);
            }
            5 => {
                let mut bytes = match std::mem::replace(target, RelationalValue::Null) {
                    RelationalValue::Bytea(value) => value,
                    _ => Vec::new(),
                };
                self.escaped_bytes_into(&mut bytes)?;
                *target = RelationalValue::Bytea(bytes);
            }
            tag => {
                return Err(OrderedRelationalKeyError::Corrupt(format!(
                    "invalid value tag {tag}"
                )))
            }
        }
        Ok(())
    }

    fn skip_value(&mut self) -> Result<bool, OrderedRelationalKeyError> {
        match self.byte("value tag")? {
            0 => Ok(true),
            1 => match self.byte("boolean value")? {
                0 | 1 => Ok(false),
                tag => Err(OrderedRelationalKeyError::Corrupt(format!(
                    "invalid boolean tag {tag}"
                ))),
            },
            2 => self.skip_fixed(8, "BIGINT value").map(|()| false),
            3 => self.skip_fixed(8, "DOUBLE PRECISION value").map(|()| false),
            4 | 5 => self.escaped_bytes(false).map(|_| false),
            tag => Err(OrderedRelationalKeyError::Corrupt(format!(
                "invalid value tag {tag}"
            ))),
        }
    }

    fn escaped_bytes(&mut self, materialize: bool) -> Result<Vec<u8>, OrderedRelationalKeyError> {
        let mut decoded = Vec::new();
        loop {
            let byte = self.byte("escaped key value")?;
            if byte != 0 {
                if materialize {
                    decoded.push(byte);
                }
                continue;
            }
            match self.byte("escaped key terminator")? {
                0 => return Ok(decoded),
                255 => {
                    if materialize {
                        decoded.push(0);
                    }
                }
                tag => {
                    return Err(OrderedRelationalKeyError::Corrupt(format!(
                        "invalid escaped key tag {tag}"
                    )));
                }
            }
        }
    }

    fn escaped_bytes_into(
        &mut self,
        decoded: &mut Vec<u8>,
    ) -> Result<(), OrderedRelationalKeyError> {
        decoded.clear();
        loop {
            let byte = self.byte("escaped key value")?;
            if byte != 0 {
                decoded.push(byte);
                continue;
            }
            match self.byte("escaped key terminator")? {
                0 => return Ok(()),
                255 => decoded.push(0),
                tag => {
                    return Err(OrderedRelationalKeyError::Corrupt(format!(
                        "invalid escaped key tag {tag}"
                    )))
                }
            }
        }
    }

    fn byte(&mut self, context: &str) -> Result<u8, OrderedRelationalKeyError> {
        let byte =
            self.encoded.get(self.offset).copied().ok_or_else(|| {
                OrderedRelationalKeyError::Corrupt(format!("truncated {context}"))
            })?;
        self.offset += 1;
        Ok(byte)
    }

    fn fixed<const N: usize>(
        &mut self,
        context: &str,
    ) -> Result<[u8; N], OrderedRelationalKeyError> {
        let end = self.offset.checked_add(N).ok_or_else(|| {
            OrderedRelationalKeyError::Corrupt(format!("{context} length overflow"))
        })?;
        let bytes = self
            .encoded
            .get(self.offset..end)
            .ok_or_else(|| OrderedRelationalKeyError::Corrupt(format!("truncated {context}")))?;
        self.offset = end;
        Ok(bytes
            .try_into()
            .expect("ordered key field has fixed length"))
    }

    fn skip_fixed(
        &mut self,
        length: usize,
        context: &str,
    ) -> Result<(), OrderedRelationalKeyError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            OrderedRelationalKeyError::Corrupt(format!("{context} length overflow"))
        })?;
        if end > self.encoded.len() {
            return Err(OrderedRelationalKeyError::Corrupt(format!(
                "truncated {context}"
            )));
        }
        self.offset = end;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_is_reversible_and_order_preserving() {
        let values = vec![
            RelationalValue::Null,
            RelationalValue::Boolean(false),
            RelationalValue::Boolean(true),
            RelationalValue::BigInt(i64::MIN),
            RelationalValue::BigInt(-1),
            RelationalValue::BigInt(0),
            RelationalValue::BigInt(i64::MAX),
            RelationalValue::DoublePrecision(f64::from_bits(u64::MAX)),
            RelationalValue::DoublePrecision(f64::NEG_INFINITY),
            RelationalValue::DoublePrecision(-0.0),
            RelationalValue::DoublePrecision(0.0),
            RelationalValue::DoublePrecision(f64::INFINITY),
            RelationalValue::DoublePrecision(f64::NAN),
            RelationalValue::Text(String::new()),
            RelationalValue::Text("a".to_string()),
            RelationalValue::Text("a\0b".to_string()),
            RelationalValue::Text("aa".to_string()),
            RelationalValue::Bytea(Vec::new()),
            RelationalValue::Bytea(vec![0]),
            RelationalValue::Bytea(vec![0, 1]),
            RelationalValue::Bytea(vec![1]),
        ];
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
        let keys = values
            .into_iter()
            .map(|value| RelationalKey(vec![value]))
            .collect::<Vec<_>>();
        let encoded = keys
            .iter()
            .map(encode_ordered_relational_key)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        for (expected, encoded) in keys.iter().zip(&encoded) {
            validate_ordered_relational_key(encoded).unwrap();
            assert_eq!(decode_ordered_relational_key(encoded).unwrap(), *expected);
        }
    }

    #[test]
    fn prefix_boundaries_report_only_leading_non_null_values() {
        let key = RelationalKey(vec![
            RelationalValue::Text("tenant-a".to_string()),
            RelationalValue::Null,
            RelationalValue::BigInt(7),
        ]);
        let encoded = encode_ordered_relational_key(&key).unwrap();
        let mut prefix_ends = Vec::new();

        let leading_non_null =
            ordered_relational_key_prefix_ends(&encoded, &mut prefix_ends).unwrap();

        assert_eq!(leading_non_null, 1);
        assert_eq!(prefix_ends.len(), 3);
        assert_eq!(
            &encoded[..prefix_ends[0]],
            encode_ordered_relational_key(&RelationalKey(vec![RelationalValue::Text(
                "tenant-a".to_string()
            )]))
            .unwrap()
        );
        assert_eq!(prefix_ends[2], encoded.len());
    }

    #[test]
    fn composite_keys_round_trip_and_are_prefix_safe() {
        let keys = [
            RelationalKey(vec![
                RelationalValue::Text("a".to_string()),
                RelationalValue::BigInt(1),
            ]),
            RelationalKey(vec![
                RelationalValue::Text("a\0".to_string()),
                RelationalValue::BigInt(0),
            ]),
            RelationalKey(vec![
                RelationalValue::Text("aa".to_string()),
                RelationalValue::BigInt(-1),
            ]),
        ];
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        let encoded = keys
            .iter()
            .map(encode_ordered_relational_key)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        for (expected, encoded) in keys.iter().zip(&encoded) {
            assert_eq!(decode_ordered_relational_key(encoded).unwrap(), *expected);
        }
    }

    #[test]
    fn in_place_decode_reuses_variable_width_key_storage() {
        let first = RelationalKey(vec![
            RelationalValue::Text("primary-key-with-capacity".to_string()),
            RelationalValue::Bytea(vec![1; 64]),
        ]);
        let second = RelationalKey(vec![
            RelationalValue::Text("short-key".to_string()),
            RelationalValue::Bytea(vec![2; 16]),
        ]);
        let mut decoded = RelationalKey(Vec::new());
        decode_ordered_relational_key_into(
            &encode_ordered_relational_key(&first).unwrap(),
            &mut decoded,
        )
        .unwrap();
        let text_pointer = match &decoded.0[0] {
            RelationalValue::Text(value) => value.as_ptr() as usize,
            value => panic!("expected TEXT key, got {value:?}"),
        };
        let bytea_pointer = match &decoded.0[1] {
            RelationalValue::Bytea(value) => value.as_ptr() as usize,
            value => panic!("expected BYTEA key, got {value:?}"),
        };

        decode_ordered_relational_key_into(
            &encode_ordered_relational_key(&second).unwrap(),
            &mut decoded,
        )
        .unwrap();
        assert_eq!(decoded, second);
        assert_eq!(
            match &decoded.0[0] {
                RelationalValue::Text(value) => value.as_ptr() as usize,
                value => panic!("expected TEXT key, got {value:?}"),
            },
            text_pointer
        );
        assert_eq!(
            match &decoded.0[1] {
                RelationalValue::Bytea(value) => value.as_ptr() as usize,
                value => panic!("expected BYTEA key, got {value:?}"),
            },
            bytea_pointer
        );
    }

    #[test]
    fn invalid_encodings_fail_closed() {
        for encoded in [
            &[][..],
            &[1][..],
            &[1, 2][..],
            &[2, 0][..],
            &[4, b'a', 0][..],
            &[4, 0, 1][..],
            &[7][..],
        ] {
            assert!(validate_ordered_relational_key(encoded).is_err());
            assert!(decode_ordered_relational_key(encoded).is_err());
        }
    }
}
