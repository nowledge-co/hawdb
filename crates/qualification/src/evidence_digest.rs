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

use hawdb::{QueryRow, QueryRows, Row, Value};
use sha2::{Digest, Sha256};

pub(crate) trait EvidenceRows {
    fn row_count(&self) -> usize;
    fn hash_rows(&self, hasher: &mut Sha256);
}

pub(crate) fn rows_sha256(rows: &(impl EvidenceRows + ?Sized)) -> String {
    let mut hasher = Sha256::new();
    hasher.update((rows.row_count() as u64).to_le_bytes());
    rows.hash_rows(&mut hasher);
    format!("{:x}", hasher.finalize())
}

impl EvidenceRows for QueryRows {
    fn row_count(&self) -> usize {
        self.len()
    }

    fn hash_rows(&self, hasher: &mut Sha256) {
        for row in self {
            hash_row(hasher, row.len(), row.iter());
        }
    }
}

impl EvidenceRows for [Row] {
    fn row_count(&self) -> usize {
        self.len()
    }

    fn hash_rows(&self, hasher: &mut Sha256) {
        for row in self {
            hash_row(
                hasher,
                row.len(),
                row.iter().map(|(name, value)| (name.as_str(), value)),
            );
        }
    }
}

impl EvidenceRows for Vec<Row> {
    fn row_count(&self) -> usize {
        self.len()
    }

    fn hash_rows(&self, hasher: &mut Sha256) {
        self.as_slice().hash_rows(hasher);
    }
}

impl<const N: usize> EvidenceRows for [Row; N] {
    fn row_count(&self) -> usize {
        N
    }

    fn hash_rows(&self, hasher: &mut Sha256) {
        self.as_slice().hash_rows(hasher);
    }
}

impl EvidenceRows for Vec<QueryRow> {
    fn row_count(&self) -> usize {
        self.len()
    }

    fn hash_rows(&self, hasher: &mut Sha256) {
        for row in self {
            hash_row(hasher, row.len(), row.iter());
        }
    }
}

fn hash_row<'a>(
    hasher: &mut Sha256,
    len: usize,
    fields: impl Iterator<Item = (&'a str, &'a Value)>,
) {
    hasher.update((len as u64).to_le_bytes());
    for (name, value) in fields {
        hash_bytes(hasher, name.as_bytes());
        hash_value(hasher, value);
    }
}

pub(crate) fn hash_value(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::Null => hasher.update([0]),
        Value::Bool(value) => {
            hasher.update([1]);
            hasher.update([u8::from(*value)]);
        }
        Value::Int(value) => {
            hasher.update([2]);
            hasher.update(value.to_le_bytes());
        }
        Value::Float(value) => {
            hasher.update([3]);
            hasher.update(value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            hasher.update([4]);
            hash_bytes(hasher, value.as_bytes());
        }
        Value::Binary(value) => {
            hasher.update([7]);
            hash_bytes(hasher, value);
        }
        Value::Uuid(value) => {
            hasher.update([8]);
            hasher.update(value.as_bytes());
        }
        Value::List(values) => {
            hasher.update([5]);
            hasher.update((values.len() as u64).to_le_bytes());
            for value in values {
                hash_value(hasher, value);
            }
        }
        Value::Map(values) => {
            hasher.update([6]);
            hasher.update((values.len() as u64).to_le_bytes());
            for (name, value) in values {
                hash_bytes(hasher, name.as_bytes());
                hash_value(hasher, value);
            }
        }
    }
}

pub(crate) fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb::Uuid;

    #[test]
    fn uuid_values_have_a_distinct_canonical_digest() {
        let uuid = Uuid::parse_str("0198f7c9-64a1-7d6a-8e67-5df1dcb3e319").unwrap();
        let value = Value::Uuid(uuid);
        let mut first = Sha256::new();
        let mut second = Sha256::new();
        let mut binary = Sha256::new();

        hash_value(&mut first, &value);
        hash_value(&mut second, &value);
        hash_value(&mut binary, &Value::Binary(uuid.as_bytes().to_vec()));

        let first_digest = first.finalize();
        let second_digest = second.finalize();
        let binary_digest = binary.finalize();

        assert_eq!(first_digest, second_digest);
        assert_ne!(second_digest, binary_digest);
    }
}
