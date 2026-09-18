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

use std::collections::BTreeMap;

use hawdb_core::Result;

use super::super::ast::*;
use super::Parser;

impl Parser<'_> {
    pub(super) fn parse_merge_statement(&mut self) -> Result<Statement> {
        let source = self.parse_merge_node_pattern()?;
        if !self.consume_char('-') {
            let mut on_create_sets = Vec::new();
            let mut on_match_sets = Vec::new();
            while self.consume_keyword("ON") {
                if self.consume_keyword("CREATE") {
                    self.expect_keyword("SET")?;
                    on_create_sets = self.parse_set_properties()?;
                } else if self.consume_keyword("MATCH") {
                    self.expect_keyword("SET")?;
                    on_match_sets = self.parse_set_properties()?;
                } else {
                    return Err(self.error("expected CREATE or MATCH"));
                }
            }
            let post_merge_sets = if self.consume_keyword("SET") {
                self.parse_set_properties()?
            } else {
                Vec::new()
            };
            return Ok(Statement::MergeNode(MergeNode {
                variable: source.variable,
                label: source.label,
                properties: source.properties,
                on_create_sets,
                on_match_sets,
                post_merge_sets,
            }));
        }
        let (rel_type, properties) = self.parse_relationship_pattern()?;
        self.expect_char('-')?;
        self.expect_char('>')?;
        let target = self.parse_create_node_pattern()?;
        Ok(Statement::MergeRelationship(CreateRelationship {
            source: CreateNode {
                label: source.label,
                properties: source.properties,
            },
            rel_type,
            properties,
            target,
        }))
    }

    fn parse_merge_node_pattern(&mut self) -> Result<MergeNode> {
        self.skip_ws();
        self.expect_char('(')?;
        self.skip_ws();
        let variable = if self.peek_char() == Some(':') {
            None
        } else {
            Some(self.parse_ident()?)
        };
        self.skip_ws();
        self.expect_char(':')?;
        let label = self.parse_ident()?;
        self.skip_ws();
        let properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.skip_ws();
        self.expect_char(')')?;
        Ok(MergeNode {
            variable,
            label,
            properties,
            on_create_sets: Vec::new(),
            on_match_sets: Vec::new(),
            post_merge_sets: Vec::new(),
        })
    }

    pub(super) fn parse_create_node_pattern(&mut self) -> Result<CreateNode> {
        self.skip_ws();
        self.expect_char('(')?;
        self.skip_ws();
        if self.peek_char() != Some(':') {
            self.parse_ident()?;
            self.skip_ws();
        }
        self.expect_char(':')?;
        let label = self.parse_ident()?;
        self.skip_ws();
        let properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.skip_ws();
        self.expect_char(')')?;
        Ok(CreateNode { label, properties })
    }

    pub(super) fn parse_match_node_pattern(
        &mut self,
    ) -> Result<(String, String, BTreeMap<String, ValueExpression>)> {
        self.skip_ws();
        self.expect_char('(')?;
        self.skip_ws();
        let (variable, label) = if self.consume_char(':') {
            (self.next_anonymous_variable(), self.parse_label_pattern()?)
        } else if self.peek_char() == Some(')') || self.peek_char() == Some('{') {
            (self.next_anonymous_variable(), String::new())
        } else {
            let variable = self.parse_ident()?;
            self.skip_ws();
            let label = if self.consume_char(':') {
                self.parse_label_pattern()?
            } else {
                String::new()
            };
            (variable, label)
        };
        self.skip_ws();
        let properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.skip_ws();
        self.expect_char(')')?;
        Ok((variable, label, properties))
    }

    fn parse_label_pattern(&mut self) -> Result<String> {
        let mut labels = vec![self.parse_ident()?];
        while self.consume_char(':') {
            labels.push(self.parse_ident()?);
        }
        Ok(labels.join(":"))
    }

    pub(super) fn parse_relationship_pattern(
        &mut self,
    ) -> Result<(String, BTreeMap<String, ValueExpression>)> {
        self.expect_char('[')?;
        self.expect_char(':')?;
        let rel_type = self.parse_ident()?;
        self.skip_ws();
        let properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.expect_char(']')?;
        Ok((rel_type, properties))
    }
}
