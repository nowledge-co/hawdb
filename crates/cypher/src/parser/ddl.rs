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

use hawdb_core::Result;

use super::super::ast::*;
use super::Parser;

const SCHEMA_PROPERTY_TYPES: &[(&str, SchemaPropertyType)] = &[
    ("ANY", SchemaPropertyType::Any),
    ("BOOL", SchemaPropertyType::Bool),
    ("BOOLEAN", SchemaPropertyType::Bool),
    ("INT", SchemaPropertyType::Int),
    ("INTEGER", SchemaPropertyType::Int),
    ("FLOAT", SchemaPropertyType::Float),
    ("DOUBLE", SchemaPropertyType::Float),
    ("STRING", SchemaPropertyType::String),
    ("TEXT", SchemaPropertyType::Text),
    ("LIST", SchemaPropertyType::List),
];

const SCHEMA_TABLE_KINDS: &[(&str, SchemaTableKind)] = &[
    ("NODE", SchemaTableKind::Node),
    ("RELATIONSHIP", SchemaTableKind::Relationship),
];

const SCHEMA_OBJECT_STATES: &[(&str, SchemaObjectState)] = &[
    ("DELETE_ONLY", SchemaObjectState::DeleteOnly),
    ("WRITE_ONLY", SchemaObjectState::WriteOnly),
    ("BACKFILL", SchemaObjectState::Backfill),
    ("VALIDATING", SchemaObjectState::Validating),
    ("PUBLIC", SchemaObjectState::Public),
    ("GC", SchemaObjectState::Gc),
];

impl Parser<'_> {
    pub(super) fn parse_create_statement(&mut self) -> Result<Statement> {
        if self.consume_keyword("CONSTRAINT") {
            return self.parse_create_constraint();
        }
        if self.consume_keyword("INDEX") {
            return self.parse_create_index(false);
        }
        if self.consume_keyword("RANGE") {
            self.expect_keyword("INDEX")?;
            return self.parse_create_index(true);
        }
        if self.consume_keyword("FULLTEXT") {
            self.expect_keyword("INDEX")?;
            return self.parse_create_full_text_index();
        }
        if self.consume_keyword("PROPERTY") {
            return self.parse_create_property();
        }
        if self.consume_keyword("NODE") {
            if self.consume_keyword("TABLE") {
                return Ok(Statement::CreateNodeTable(self.parse_ident()?));
            }
            self.expect_keyword("LABEL")?;
            return Ok(Statement::CreateNodeLabel(self.parse_ident()?));
        }
        if self.consume_keyword("RELATIONSHIP") {
            if self.consume_keyword("TABLE") {
                return Ok(Statement::CreateRelationshipTable(self.parse_ident()?));
            }
            self.expect_keyword("TYPE")?;
            return Ok(Statement::CreateRelationshipType(self.parse_ident()?));
        }
        let source = self.parse_create_node_pattern()?;
        if !self.consume_char('-') {
            return Ok(Statement::CreateNode(source));
        }
        let (rel_type, properties) = self.parse_relationship_pattern()?;
        self.expect_char('-')?;
        self.expect_char('>')?;
        let target = self.parse_create_node_pattern()?;
        Ok(Statement::CreateRelationship(CreateRelationship {
            source,
            rel_type,
            properties,
            target,
        }))
    }

    pub(super) fn parse_create_index(&mut self, range: bool) -> Result<Statement> {
        self.expect_keyword("ON")?;
        self.expect_char(':')?;
        let label = self.parse_ident()?;
        self.expect_char('(')?;
        let mut properties = vec![self.parse_ident()?];
        while self.consume_char(',') {
            properties.push(self.parse_ident()?);
        }
        self.expect_char(')')?;
        if range && properties.len() > 1 {
            return Err(self.error("range index expects one property"));
        }
        if !range && properties.len() > 1 {
            return Ok(Statement::CreateCompositeIndex(CreateCompositeIndex {
                label,
                properties,
            }));
        }
        let property = properties.remove(0);
        let index = CreateIndex { label, property };
        if range {
            Ok(Statement::CreateRangeIndex(index))
        } else {
            Ok(Statement::CreateIndex(index))
        }
    }

    pub(super) fn parse_create_full_text_index(&mut self) -> Result<Statement> {
        self.expect_keyword("ON")?;
        self.expect_char(':')?;
        let label = self.parse_ident()?;
        self.expect_char('(')?;
        let property = self.parse_ident()?;
        if self.consume_char(',') {
            return Err(self.error("fulltext index expects one property"));
        }
        self.expect_char(')')?;
        Ok(Statement::CreateFullTextIndex(CreateIndex {
            label,
            property,
        }))
    }

    pub(super) fn parse_create_property(&mut self) -> Result<Statement> {
        self.expect_keyword("ON")?;
        let table_kind = self.parse_schema_table_kind()?;
        self.expect_keyword("TABLE")?;
        let table = self.parse_ident()?;
        self.expect_char('(')?;
        let property = self.parse_ident()?;
        self.expect_char(')')?;
        self.expect_keyword("TYPE")?;
        let value_type = self.parse_schema_property_type()?;
        let nullable = if self.consume_keyword("NOT") {
            self.expect_keyword("NULL")?;
            false
        } else {
            true
        };
        Ok(Statement::CreateProperty(CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        }))
    }

    pub(super) fn parse_schema_property_type(&mut self) -> Result<SchemaPropertyType> {
        if self.consume_keyword("CHARACTER") {
            self.expect_keyword("VARYING")?;
            return Ok(SchemaPropertyType::String);
        }
        if self.consume_keyword("VARCHAR") {
            return Ok(SchemaPropertyType::String);
        }
        self.parse_keyword_choice(SCHEMA_PROPERTY_TYPES, "expected property type")
    }

    pub(super) fn parse_alter_statement(&mut self) -> Result<Statement> {
        if self.consume_keyword("PROPERTY") {
            return self.parse_alter_property_state();
        }
        let table_kind = self
            .parse_schema_table_kind()
            .map_err(|_| self.error("expected NODE, RELATIONSHIP, or PROPERTY"))?;
        self.expect_keyword("TABLE")?;
        let table = self.parse_ident()?;
        let state = self.parse_set_state_clause()?;
        Ok(Statement::AlterTableState(AlterTableState {
            table_kind,
            table,
            state,
        }))
    }

    pub(super) fn parse_alter_property_state(&mut self) -> Result<Statement> {
        self.expect_keyword("ON")?;
        let table_kind = self.parse_schema_table_kind()?;
        self.expect_keyword("TABLE")?;
        let table = self.parse_ident()?;
        self.expect_char('(')?;
        let property = self.parse_ident()?;
        self.expect_char(')')?;
        let state = self.parse_set_state_clause()?;
        Ok(Statement::AlterPropertyState(AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        }))
    }

    pub(super) fn parse_set_state_clause(&mut self) -> Result<SchemaObjectState> {
        self.expect_keyword("SET")?;
        self.expect_keyword("STATE")?;
        self.parse_schema_object_state()
    }

    pub(super) fn parse_schema_table_kind(&mut self) -> Result<SchemaTableKind> {
        self.parse_keyword_choice(SCHEMA_TABLE_KINDS, "expected NODE or RELATIONSHIP")
    }

    pub(super) fn parse_schema_object_state(&mut self) -> Result<SchemaObjectState> {
        self.parse_keyword_choice(SCHEMA_OBJECT_STATES, "expected schema object state")
    }

    pub(super) fn parse_create_constraint(&mut self) -> Result<Statement> {
        self.expect_keyword("ON")?;
        if self.consume_char('-') {
            return self.parse_create_relationship_constraint();
        }
        self.expect_char(':')?;
        let label = self.parse_ident()?;
        self.expect_char('(')?;
        let property = self.parse_ident()?;
        self.expect_char(')')?;
        self.expect_keyword("ASSERT")?;
        let constraint = CreateIndex { label, property };
        if self.consume_keyword("UNIQUE") {
            return Ok(Statement::CreateUniqueConstraint(constraint));
        }
        if self.consume_keyword("EXISTS") {
            return Ok(Statement::CreateNodePropertyExistsConstraint(constraint));
        }
        if self.consume_keyword("NOT") {
            self.expect_keyword("NULL")?;
            return Ok(Statement::CreateNodePropertyExistsConstraint(constraint));
        }
        Err(self.error("expected UNIQUE, EXISTS, or NOT NULL"))
    }

    pub(super) fn parse_create_relationship_constraint(&mut self) -> Result<Statement> {
        self.expect_char('[')?;
        self.expect_char(':')?;
        let label = self.parse_ident()?;
        self.expect_char('(')?;
        let property = self.parse_ident()?;
        self.expect_char(')')?;
        self.expect_char(']')?;
        self.expect_char('-')?;
        self.expect_char('>')?;
        self.expect_keyword("ASSERT")?;
        let constraint = CreateIndex { label, property };
        if self.consume_keyword("UNIQUE") {
            return Ok(Statement::CreateRelationshipUniqueConstraint(constraint));
        }
        if self.consume_keyword("EXISTS") {
            return Ok(Statement::CreateRelationshipPropertyExistsConstraint(
                constraint,
            ));
        }
        if self.consume_keyword("NOT") {
            self.expect_keyword("NULL")?;
            return Ok(Statement::CreateRelationshipPropertyExistsConstraint(
                constraint,
            ));
        }
        Err(self.error("expected UNIQUE, EXISTS, or NOT NULL"))
    }
}
