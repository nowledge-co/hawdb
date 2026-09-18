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

//! Canonical graph DDL command types shared by syntax ASTs and query plans.
//!
//! Frontends re-export these types instead of defining parser-specific copies.
//! Core catalog types remain behind the explicit conversions in [`convert`];
//! this crate does not own catalog execution or persistent encoding.

pub mod convert;
pub mod types;

pub use convert::{object_state_to_core, property_type_to_core, table_kind_to_core};
pub use types::{SchemaObjectState, SchemaPropertyType, SchemaTableKind};
