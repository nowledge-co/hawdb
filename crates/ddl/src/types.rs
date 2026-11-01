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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaTableKind {
    Node,
    Relationship,
}

impl SchemaTableKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Relationship => "relationship",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPropertyType {
    Any,
    Bool,
    Int,
    Float,
    String,
    Text,
    List,
    Vector(hawdb_core::VectorDimension),
}

impl SchemaPropertyType {
    pub fn fingerprint_suffix(self) -> std::borrow::Cow<'static, str> {
        match self {
            Self::Any => ":any",
            Self::Bool => ":bool",
            Self::Int => ":int",
            Self::Float => ":float",
            Self::String => ":string",
            Self::Text => ":text",
            Self::List => ":list",
            Self::Vector(dimension) => return format!(":vector({})", dimension.get()).into(),
        }
        .into()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaObjectState {
    DeleteOnly,
    WriteOnly,
    Backfill,
    Validating,
    Public,
    Gc,
}

impl SchemaObjectState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeleteOnly => "delete_only",
            Self::WriteOnly => "write_only",
            Self::Backfill => "backfill",
            Self::Validating => "validating",
            Self::Public => "public",
            Self::Gc => "gc",
        }
    }
}

// Retained query ownership: SchemaTableKind.
hawdb_core::impl_retained_heap_for_copy!(SchemaTableKind);

// Retained query ownership: SchemaPropertyType.
hawdb_core::impl_retained_heap_for_copy!(SchemaPropertyType);

// Retained query ownership: SchemaObjectState.
hawdb_core::impl_retained_heap_for_copy!(SchemaObjectState);
