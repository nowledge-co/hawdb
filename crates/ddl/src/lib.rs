//! Canonical graph DDL command types shared by syntax ASTs and query plans.
//!
//! Frontends re-export these types instead of defining parser-specific copies.
//! Core catalog types remain behind the explicit conversions in [`convert`];
//! this crate does not own catalog execution or persistent encoding.

pub mod convert;
pub mod types;

pub use convert::{object_state_to_core, property_type_to_core, table_kind_to_core};
pub use types::{SchemaObjectState, SchemaPropertyType, SchemaTableKind};
