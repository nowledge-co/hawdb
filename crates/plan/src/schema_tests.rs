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

use std::any::TypeId;

fn assert_shared_type<Parser: 'static, Ast: 'static, Plan: 'static, Ddl: 'static>() {
    let canonical = TypeId::of::<Ddl>();
    assert_eq!(TypeId::of::<Plan>(), canonical, "plan type identity");
    assert_eq!(TypeId::of::<Ast>(), canonical, "AST type identity");
    assert_eq!(
        TypeId::of::<Parser>(),
        canonical,
        "parser re-export identity"
    );
}

#[test]
fn table_kind_is_one_shared_command_type() {
    assert_shared_type::<
        hawdb_cypher::SchemaTableKind,
        hawdb_cypher::ast::SchemaTableKind,
        crate::SchemaTableKind,
        hawdb_ddl::SchemaTableKind,
    >();
}

#[test]
fn property_type_is_one_shared_command_type() {
    assert_shared_type::<
        hawdb_cypher::SchemaPropertyType,
        hawdb_cypher::ast::SchemaPropertyType,
        crate::SchemaPropertyType,
        hawdb_ddl::SchemaPropertyType,
    >();
}

#[test]
fn object_state_is_one_shared_command_type() {
    assert_shared_type::<
        hawdb_cypher::SchemaObjectState,
        hawdb_cypher::ast::SchemaObjectState,
        crate::SchemaObjectState,
        hawdb_ddl::SchemaObjectState,
    >();
}
