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

//! Catalog schema helpers used by schema-maintenance operations.

use hawdb_core::{Catalog, TableId, TableKind};

/// Ensures the owning label/rel-type and the table descriptor exist.
pub fn ensure_table_descriptor(catalog: &mut Catalog, kind: TableKind, name: &str) -> TableId {
    match kind {
        TableKind::Node => {
            catalog.get_or_create_label(name);
        }
        TableKind::Relationship => {
            catalog.get_or_create_rel_type(name);
        }
    }
    catalog.get_or_create_table(kind, name)
}

/// Reserves a schema-maintenance budget unit, returning `false` when it would
/// exceed the cap.
pub fn reserve_schema_maintenance_budget(
    used_estimated_operations: &mut usize,
    max_estimated_operations: Option<usize>,
    estimated_operations: usize,
) -> bool {
    let Some(max_estimated_operations) = max_estimated_operations else {
        return true;
    };
    let next = used_estimated_operations.saturating_add(estimated_operations);
    if next > max_estimated_operations {
        return false;
    }
    *used_estimated_operations = next;
    true
}
