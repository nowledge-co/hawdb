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

use super::{SearchProjectionConsumerId, SearchProjectionConsumerState};
use hawdb_core::{HawDBError, Result, Uuid};
use hawdb_integrity::IntegrityHasher;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const MAX_CONSUMERS: usize = 64;
const MAX_REGISTRY_BYTES: usize = 64 * 1024;
const REGISTRY_FILE: &str = "projection_consumers.meta";
const PROTOCOL: &str = "hawdb-projection-consumers-v1";

// Declaration order is lexicographic for canonical payload serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub checkpoint_uuid: String,
    pub durable_complete_through_epoch: u64,
    pub expires_at_commit_epoch: u64,
    pub id: String,
    pub max_idle_commits: u64,
    pub projection_uuid: String,
    pub registration_uuid: String,
    pub snapshot_encoded_len: u64,
    pub snapshot_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    #[serde(deserialize_with = "bounded_records")]
    consumers: Vec<Record>,
    database_uuid: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    protocol: String,
    payload: Payload,
    payload_sha256: String,
}

#[derive(Debug, Default)]
pub struct ConsumerRegistry {
    pub records: BTreeMap<String, Record>,
    pub verified: BTreeMap<String, SearchProjectionConsumerState>,
    pub database_uuid: Option<Uuid>,
    pub unavailable: bool,
}

impl ConsumerRegistry {
    pub fn load(root: Option<&Path>, identity: Option<Uuid>) -> Self {
        let Some(root) = root else {
            return Self::default();
        };
        let result = (|| -> Result<Option<Payload>> {
            let file = match File::open(root.join(REGISTRY_FILE)) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            if file.metadata()?.len() > MAX_REGISTRY_BYTES as u64 {
                return Err(invalid("file exceeds byte limit"));
            }
            // Also bound a file that grows after metadata is read.
            let mut bytes = vec![0; MAX_REGISTRY_BYTES + 1];
            let mut reader = file.take((MAX_REGISTRY_BYTES + 1) as u64);
            let mut length = 0;
            while length < bytes.len() {
                let count = reader.read(&mut bytes[length..])?;
                if count == 0 {
                    break;
                }
                length += count;
            }
            bytes.truncate(length);
            decode(&bytes).map(Some)
        })();
        match result {
            Ok(Some(payload)) => Self {
                database_uuid: payload.database_uuid.parse().ok(),
                records: payload
                    .consumers
                    .into_iter()
                    .map(|record| (record.id.clone(), record))
                    .collect(),
                ..Self::default()
            },
            Ok(None) => Self {
                database_uuid: identity,
                unavailable: identity.is_some(),
                ..Self::default()
            },
            Err(_) => Self {
                database_uuid: identity,
                unavailable: true,
                ..Self::default()
            },
        }
    }

    pub fn publish<BeforeReplace, AfterReplace>(
        &self,
        root: &Path,
        before_replace: BeforeReplace,
        after_replace: AfterReplace,
    ) -> Result<()>
    where
        BeforeReplace: FnOnce() -> Result<()>,
        AfterReplace: FnOnce() -> Result<()>,
    {
        let identity = self
            .database_uuid
            .ok_or_else(|| invalid("missing database identity"))?;
        let payload = Payload {
            consumers: self.records.values().cloned().collect(),
            database_uuid: identity.to_string(),
        };
        validate(&payload)?;
        let canonical =
            serde_json::to_vec(&payload).map_err(|error| invalid(&error.to_string()))?;
        let envelope = Envelope {
            protocol: PROTOCOL.into(),
            payload,
            payload_sha256: digest(&canonical),
        };
        let mut bytes =
            serde_json::to_vec(&envelope).map_err(|error| invalid(&error.to_string()))?;
        bytes.push(b'\n');
        if bytes.len() > MAX_REGISTRY_BYTES {
            return Err(invalid("encoded file exceeds byte limit"));
        }
        let target = root.join(REGISTRY_FILE);
        let temporary = target.with_extension("meta.tmp");
        // Database writer exclusion owns this fixed temporary across restarts.
        match std::fs::remove_file(&temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        let guard = Temporary(temporary.clone());
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        before_replace()?;
        hawdb_storage::durable_replace_file(&temporary, &target)?;
        drop(guard);
        after_replace()?;
        Ok(())
    }

    pub fn state(
        &self,
        record: &Record,
        identity: Option<Uuid>,
        epoch: u64,
        floor: u64,
    ) -> SearchProjectionConsumerState {
        use super::SearchProjectionConsumerRebuildReason as Reason;
        use SearchProjectionConsumerState as State;
        let reason = if self.unavailable {
            Some(Reason::RegistryUnavailable)
        } else if identity != self.database_uuid {
            Some(Reason::DatabaseIdentityMismatch)
        } else if record.durable_complete_through_epoch > epoch {
            Some(Reason::SourceRewound)
        } else if epoch >= record.expires_at_commit_epoch {
            Some(Reason::Expired {
                expires_at_commit_epoch: record.expires_at_commit_epoch,
            })
        } else if record.durable_complete_through_epoch < floor {
            Some(Reason::RetentionLimitExceeded {
                resume_floor_commit_epoch: floor,
            })
        } else {
            None
        };
        reason.map(State::RebuildRequired).unwrap_or_else(|| {
            self.verified
                .get(&record.id)
                .cloned()
                .unwrap_or(State::Unverified)
        })
    }
}

struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn decode(bytes: &[u8]) -> Result<Payload> {
    if bytes.len() > MAX_REGISTRY_BYTES {
        return Err(invalid("file exceeds byte limit"));
    }
    let mut stream = serde_json::Deserializer::from_slice(bytes).into_iter::<Envelope>();
    let envelope = stream
        .next()
        .ok_or_else(|| invalid("empty file"))?
        .map_err(|error| invalid(&error.to_string()))?;
    if &bytes[stream.byte_offset()..] != b"\n" {
        return Err(invalid("invalid trailing data"));
    }
    if envelope.protocol != PROTOCOL {
        return Err(invalid("unknown protocol"));
    }
    validate(&envelope.payload)?;
    let canonical =
        serde_json::to_vec(&envelope.payload).map_err(|error| invalid(&error.to_string()))?;
    if envelope.payload_sha256 != digest(&canonical) {
        return Err(invalid("payload digest mismatch"));
    }
    Ok(envelope.payload)
}

fn validate(payload: &Payload) -> Result<()> {
    canonical_uuid(&payload.database_uuid)?;
    if payload.consumers.len() > MAX_CONSUMERS {
        return Err(invalid("consumer limit exceeded"));
    }
    let mut previous: Option<&str> = None;
    for record in &payload.consumers {
        SearchProjectionConsumerId::new(record.id.clone())?;
        if previous.is_some_and(|id| id >= record.id.as_str()) {
            return Err(invalid("duplicate or unordered consumer"));
        }
        previous = Some(&record.id);
        canonical_uuid(&record.projection_uuid)?;
        canonical_uuid(&record.registration_uuid)?;
        canonical_uuid(&record.checkpoint_uuid)?;
        if record.snapshot_encoded_len == 0
            || record.max_idle_commits == 0
            || record.expires_at_commit_epoch <= record.durable_complete_through_epoch
            || record.snapshot_sha256.len() != 64
            || !record
                .snapshot_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid("invalid receipt or epoch"));
        }
    }
    Ok(())
}

fn canonical_uuid(value: &str) -> Result<Uuid> {
    let uuid = value.parse::<Uuid>().map_err(|_| invalid("invalid UUID"))?;
    if uuid.is_nil() || uuid.to_string() != value {
        return Err(invalid("noncanonical UUID"));
    }
    Ok(uuid)
}

fn digest(bytes: &[u8]) -> String {
    let mut hasher = IntegrityHasher::new();
    hasher.update(bytes);
    hasher.finish().sha256.to_string()
}

fn invalid(detail: &str) -> HawDBError {
    HawDBError::Storage(format!("invalid projection consumer registry: {detail}"))
}

fn bounded_records<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Record>, D::Error> {
    struct Records;
    impl<'de> serde::de::Visitor<'de> for Records {
        type Value = Vec<Record>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("at most 64 consumers")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut records = Vec::new();
            while let Some(record) = seq.next_element::<Record>()? {
                if records.len() == MAX_CONSUMERS {
                    return Err(serde::de::Error::custom("consumer limit exceeded"));
                }
                records.push(record);
            }
            Ok(records)
        }
    }
    deserializer.deserialize_seq(Records)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn payload() -> Payload {
        let uuid = "01950000-0000-7000-8000-000000000001".to_string();
        Payload {
            database_uuid: uuid.clone(),
            consumers: vec![Record {
                checkpoint_uuid: uuid.clone(),
                durable_complete_through_epoch: 3,
                expires_at_commit_epoch: 10,
                id: "main".into(),
                max_idle_commits: 7,
                projection_uuid: uuid.clone(),
                registration_uuid: uuid,
                snapshot_encoded_len: 500,
                snapshot_sha256: "a".repeat(64),
            }],
        }
    }
    fn encode(payload: Payload) -> Vec<u8> {
        let hash = digest(&serde_json::to_vec(&payload).unwrap());
        let mut bytes = serde_json::to_vec(&Envelope {
            protocol: PROTOCOL.into(),
            payload,
            payload_sha256: hash,
        })
        .unwrap();
        bytes.push(b'\n');
        bytes
    }
    #[test]
    fn registry_round_trip_has_canonical_payload_and_complete_file_bound() {
        let bytes = encode(payload());
        assert_eq!(encode(decode(&bytes).unwrap()), bytes);
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("\"payload\":{\"consumers\":[{\"checkpoint_uuid\":"));
        assert!(decode(&vec![b' '; MAX_REGISTRY_BYTES + 1]).is_err());
    }
    #[test]
    fn registry_rejects_duplicate_unknown_fields_versions_and_trailing_data() {
        let text = String::from_utf8(encode(payload())).unwrap();
        for invalid in [
            text.replacen("{", "{\"unknown\":0,", 1),
            text.replacen(
                "\"protocol\":",
                "\"protocol\":\"hawdb-projection-consumers-v1\",\"protocol\":",
                1,
            ),
            text.replacen("\"id\":", "\"id\":\"shadow\",\"id\":", 1),
            text.replacen(
                "hawdb-projection-consumers-v1",
                "hawdb-projection-consumers-v2",
                1,
            ),
            format!("{text}\n"),
            format!("{} {{}}\n", text.trim_end()),
            text.trim_end().to_string(),
        ] {
            assert!(decode(invalid.as_bytes()).is_err(), "accepted {invalid}");
        }
    }
    #[test]
    fn registry_rejects_tampered_payload_and_noncanonical_records() {
        let bytes = encode(payload());
        let altered = String::from_utf8(bytes).unwrap().replace(
            "\"durable_complete_through_epoch\":3",
            "\"durable_complete_through_epoch\":4",
        );
        assert!(decode(altered.as_bytes()).is_err());
        for field in 0..8 {
            let mut payload = payload();
            let record = &mut payload.consumers[0];
            match field {
                0 => record.id = "x".repeat(129),
                1 => record.id = "bad/id".into(),
                2 => record.snapshot_sha256 = "A".repeat(64),
                3 => record.checkpoint_uuid = "bad".into(),
                4 => record.expires_at_commit_epoch = 3,
                5 => record.max_idle_commits = 0,
                6 => record.snapshot_encoded_len = 0,
                _ => record.registration_uuid = "00000000-0000-0000-0000-000000000000".into(),
            }
            assert!(decode(&encode(payload)).is_err());
        }
    }
    #[test]
    fn registry_enforces_sixty_four_records_including_duplicates() {
        let mut payload = payload();
        let record = payload.consumers[0].clone();
        payload.consumers = (0..MAX_CONSUMERS)
            .map(|index| Record {
                id: format!("c{index:03}"),
                ..record.clone()
            })
            .collect();
        assert_eq!(
            decode(&encode(payload.clone())).unwrap().consumers.len(),
            MAX_CONSUMERS
        );
        payload.consumers.push(Record {
            id: "extra".into(),
            ..record
        });
        assert!(decode(&encode(payload.clone())).is_err());
        payload.consumers.truncate(2);
        payload.consumers[1].id = payload.consumers[0].id.clone();
        assert!(decode(&encode(payload)).is_err());
    }
}
