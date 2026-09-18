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

use super::{property_type_name, GraphRagSchemaContext};

pub(super) fn schema_context_fingerprint(context: &GraphRagSchemaContext) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    fingerprint_bytes(&mut hash, context.protocol.as_bytes());
    fingerprint_bytes(&mut hash, &context.computed_at_commit_epoch.to_le_bytes());
    for label in &context.labels {
        fingerprint_bytes(&mut hash, label.name.as_bytes());
        fingerprint_bytes(&mut hash, &label.node_count.to_le_bytes());
    }
    for relationship in &context.relationship_types {
        fingerprint_bytes(&mut hash, relationship.name.as_bytes());
        fingerprint_bytes(&mut hash, &relationship.relationship_count.to_le_bytes());
        fingerprint_bytes(&mut hash, &relationship.distinct_source_count.to_le_bytes());
        fingerprint_bytes(&mut hash, &relationship.distinct_target_count.to_le_bytes());
    }
    for property in &context.properties {
        fingerprint_bytes(&mut hash, property.subject.as_str().as_bytes());
        fingerprint_bytes(&mut hash, property.subject_name.as_bytes());
        fingerprint_bytes(&mut hash, property.name.as_bytes());
        fingerprint_bytes(
            &mut hash,
            property_type_name(property.value_type).as_bytes(),
        );
        fingerprint_bytes(&mut hash, &[u8::from(property.nullable)]);
        fingerprint_bytes(&mut hash, &[u8::from(property.declared)]);
        fingerprint_optional_count(&mut hash, property.distinct_count);
    }
    for route in &context.routes {
        fingerprint_bytes(&mut hash, route.source_label.as_bytes());
        fingerprint_bytes(&mut hash, route.relationship_type.as_bytes());
        fingerprint_bytes(&mut hash, route.target_label.as_bytes());
        fingerprint_bytes(&mut hash, &route.observed_count.to_le_bytes());
        fingerprint_bytes(&mut hash, &route.distinct_source_count.to_le_bytes());
        fingerprint_bytes(&mut hash, &route.distinct_target_count.to_le_bytes());
    }
    for path in &context.common_paths {
        fingerprint_bytes(&mut hash, path.source_label.as_bytes());
        fingerprint_bytes(&mut hash, path.relationship_type.as_bytes());
        fingerprint_bytes(&mut hash, path.target_label.as_bytes());
        fingerprint_bytes(&mut hash, &path.hops.to_le_bytes());
        fingerprint_bytes(&mut hash, &path.observed_count.to_le_bytes());
        fingerprint_bytes(&mut hash, &path.distinct_source_count.to_le_bytes());
        fingerprint_bytes(&mut hash, &path.distinct_target_count.to_le_bytes());
    }
    fingerprint_bytes(&mut hash, &[u8::from(context.truncation.labels)]);
    fingerprint_bytes(
        &mut hash,
        &[u8::from(context.truncation.relationship_types)],
    );
    fingerprint_bytes(&mut hash, &[u8::from(context.truncation.properties)]);
    fingerprint_bytes(&mut hash, &[u8::from(context.truncation.routes)]);
    fingerprint_bytes(&mut hash, &[u8::from(context.truncation.common_paths)]);
    hash
}

fn fingerprint_optional_count(hash: &mut u64, count: Option<u64>) {
    match count {
        Some(count) => {
            fingerprint_bytes(hash, &[1]);
            fingerprint_bytes(hash, &count.to_le_bytes());
        }
        None => fingerprint_bytes(hash, &[0]),
    }
}

fn fingerprint_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    *hash ^= 0xff;
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}
