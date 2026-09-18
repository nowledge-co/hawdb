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

use hawdb_integrity::{IntegrityHasher, Sha256Digest};

const RECOVERY_SOURCE_DOMAIN: &[u8] = b"HAWDB_RELATIONAL_RECOVERY_SOURCE_V1\0";
pub(crate) const RELATIONAL_RECOVERY_SOURCE_BYTES: usize = 56;

/// Stable identity of the exact WAL record sequence used to build relational
/// recovery artifacts. `end_lsn` is exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRecoverySourceIdentity {
    pub wal_generation: u64,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub record_sequence_sha256: Sha256Digest,
}

/// Expected recovered commit and the exact WAL source that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRecoveryFence {
    pub commit_epoch: u64,
    pub source: RelationalRecoverySourceIdentity,
}

impl RelationalRecoveryFence {
    pub const fn new(commit_epoch: u64, source: RelationalRecoverySourceIdentity) -> Self {
        Self {
            commit_epoch,
            source,
        }
    }
}

impl RelationalRecoverySourceIdentity {
    pub fn validate(self) -> Result<(), &'static str> {
        if self.start_lsn >= self.end_lsn {
            return Err("recovery source LSN range must be non-empty");
        }
        Ok(())
    }

    pub(crate) fn encode_into(self, encoded: &mut Vec<u8>) -> Result<(), &'static str> {
        self.validate()?;
        encoded.extend_from_slice(&self.wal_generation.to_le_bytes());
        encoded.extend_from_slice(&self.start_lsn.to_le_bytes());
        encoded.extend_from_slice(&self.end_lsn.to_le_bytes());
        encoded.extend_from_slice(self.record_sequence_sha256.as_bytes());
        Ok(())
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, &'static str> {
        if encoded.len() != RELATIONAL_RECOVERY_SOURCE_BYTES {
            return Err("recovery source encoding must contain exactly 56 bytes");
        }
        let identity = Self {
            wal_generation: u64::from_le_bytes(
                encoded[0..8]
                    .try_into()
                    .expect("recovery WAL generation has a fixed length"),
            ),
            start_lsn: u64::from_le_bytes(
                encoded[8..16]
                    .try_into()
                    .expect("recovery start LSN has a fixed length"),
            ),
            end_lsn: u64::from_le_bytes(
                encoded[16..24]
                    .try_into()
                    .expect("recovery end LSN has a fixed length"),
            ),
            record_sequence_sha256: Sha256Digest::from_bytes(
                encoded[24..56]
                    .try_into()
                    .expect("recovery source digest has a fixed length"),
            ),
        };
        identity.validate()?;
        Ok(identity)
    }

    #[cfg(test)]
    pub(crate) fn for_test(start_lsn: u64, end_lsn: u64) -> Self {
        let mut hasher = IntegrityHasher::new();
        hasher.update(RECOVERY_SOURCE_DOMAIN);
        hasher.update(&1_u64.to_le_bytes());
        hasher.update(&start_lsn.to_le_bytes());
        hasher.update(&end_lsn.to_le_bytes());
        Self {
            wal_generation: 1,
            start_lsn,
            end_lsn,
            record_sequence_sha256: hasher.finish().sha256,
        }
    }
}

/// Incremental builder used while a validated WAL prefix is replayed.
pub struct RelationalRecoverySourceBuilder {
    wal_generation: u64,
    start_lsn: u64,
    next_lsn: u64,
    hasher: IntegrityHasher,
}

impl RelationalRecoverySourceBuilder {
    pub fn new(wal_generation: u64, start_lsn: u64) -> Self {
        let mut hasher = IntegrityHasher::new();
        hasher.update(RECOVERY_SOURCE_DOMAIN);
        hasher.update(&wal_generation.to_le_bytes());
        hasher.update(&start_lsn.to_le_bytes());
        Self {
            wal_generation,
            start_lsn,
            next_lsn: start_lsn,
            hasher,
        }
    }

    pub fn record(
        &mut self,
        lsn: u64,
        payload_len: u64,
        payload_sha256: Sha256Digest,
    ) -> Result<(), &'static str> {
        if lsn != self.next_lsn {
            return Err("recovery source records must be added in contiguous LSN order");
        }
        self.hasher.update(&lsn.to_le_bytes());
        self.hasher.update(&payload_len.to_le_bytes());
        self.hasher.update(payload_sha256.as_bytes());
        self.next_lsn = self
            .next_lsn
            .checked_add(1)
            .ok_or("recovery source LSN overflow")?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<RelationalRecoverySourceIdentity, &'static str> {
        self.hasher.update(&self.next_lsn.to_le_bytes());
        let identity = RelationalRecoverySourceIdentity {
            wal_generation: self.wal_generation,
            start_lsn: self.start_lsn,
            end_lsn: self.next_lsn,
            record_sequence_sha256: self.hasher.finish().sha256,
        };
        identity.validate()?;
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_source_binds_record_order_and_payload() {
        let digest_a = Sha256Digest::from_bytes([1; 32]);
        let digest_b = Sha256Digest::from_bytes([2; 32]);
        let mut left = RelationalRecoverySourceBuilder::new(7, 11);
        left.record(11, 3, digest_a).unwrap();
        left.record(12, 5, digest_b).unwrap();

        let mut right = RelationalRecoverySourceBuilder::new(7, 11);
        right.record(11, 5, digest_b).unwrap();
        right.record(12, 3, digest_a).unwrap();

        assert_ne!(left.finish().unwrap(), right.finish().unwrap());
    }

    #[test]
    fn recovery_source_rejects_empty_and_non_contiguous_ranges() {
        assert!(RelationalRecoverySourceBuilder::new(1, 4).finish().is_err());
        let mut builder = RelationalRecoverySourceBuilder::new(1, 4);
        assert!(builder
            .record(5, 1, Sha256Digest::from_bytes([0; 32]))
            .is_err());
    }

    #[test]
    fn recovery_source_accepts_initial_wal_generation() {
        let mut builder = RelationalRecoverySourceBuilder::new(0, 0);
        builder
            .record(0, 1, Sha256Digest::from_bytes([0; 32]))
            .unwrap();

        let source = builder.finish().unwrap();
        assert_eq!(source.wal_generation, 0);
        assert_eq!(source.start_lsn, 0);
        assert_eq!(source.end_lsn, 1);
    }
}
