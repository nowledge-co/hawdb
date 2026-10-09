// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb::Value;

/// Canonical FNV-1a over schema plus ordered rows. Type tags, little-endian
/// lengths and IEEE float bits make precision/type loss observable. It is a
/// parity oracle, not a security hash and not the benchmark's query timer.
pub(super) struct Checksum(u64);

impl Checksum {
    pub(super) fn new(columns: &[String]) -> Self {
        let mut result = Self(0xcbf29ce484222325);
        result.bytes(b"HDBBOUND1");
        result.length(columns.len());
        for column in columns {
            result.value(&Value::String(column.clone()));
        }
        result
    }

    fn bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
        }
    }

    fn length(&mut self, length: usize) {
        self.bytes(&(length as u64).to_le_bytes());
    }

    fn value(&mut self, value: &Value) {
        match value {
            Value::Null => self.bytes(&[0]),
            Value::Bool(value) => self.bytes(&[1, u8::from(*value)]),
            Value::Int(value) => {
                self.bytes(&[2]);
                self.bytes(&value.to_le_bytes());
            }
            Value::Float(value) => {
                self.bytes(&[3]);
                self.bytes(&value.to_bits().to_le_bytes());
            }
            Value::String(value) => {
                self.bytes(&[4]);
                self.length(value.len());
                self.bytes(value.as_bytes());
            }
            Value::Binary(value) => {
                self.bytes(&[5]);
                self.length(value.len());
                self.bytes(value);
            }
            Value::Uuid(value) => {
                self.bytes(&[6]);
                self.bytes(value.as_bytes());
            }
            Value::List(values) => {
                self.bytes(&[7]);
                self.length(values.len());
                for value in values {
                    self.value(value);
                }
            }
            Value::Map(values) => {
                self.bytes(&[8]);
                self.length(values.len());
                for (key, value) in values {
                    self.bytes(&[4]);
                    self.length(key.len());
                    self.bytes(key.as_bytes());
                    self.value(value);
                }
            }
        }
    }

    pub(super) fn row(&mut self, values: &[Value]) {
        self.bytes(&[0xff]);
        self.length(values.len());
        for value in values {
            self.value(value);
        }
    }

    pub(super) fn hex(&self) -> String {
        format!("{:016x}", self.0)
    }
}
