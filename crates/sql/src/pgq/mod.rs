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

mod binder;
mod catalog;
mod create;
mod ir;
mod lowering;

pub use binder::{bind_postgres_graph_tables, PgqBindError, PgqBindErrorCode};
pub use catalog::{
    PgqBindingContext, PgqCatalog, PropertyGraphCatalog, PropertyGraphElementSchema,
    PropertyGraphSchema,
};
pub use create::{
    bind_postgres_create_property_graph, PgqCreateBindError, PgqCreateBindErrorCode,
    PgqSourceCatalog, PgqSourceColumnSchema, PgqSourceForeignKeySchema, PgqSourceTableSchema,
};
pub use ir::*;
pub use lowering::{
    lower_bound_pgq_graph_table, PgqLoweringError, PgqLoweringErrorCode, PgqLoweringParameters,
};

#[cfg(test)]
mod tests;
