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
