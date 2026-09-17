//! Catalog schema helpers used by schema-maintenance operations.

use skein_core::{Catalog, TableId, TableKind};

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
