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

use super::Parser;
use crate::{
    CreatePropertyGraph, EdgeEndpoint, EdgeTableDefinition, ElementExposure, ElementLabel,
    ElementLabelExposure, Identifier, PropertyExposure, Span, SyntaxError, TokenKind,
    VertexTableDefinition,
};

impl Parser<'_> {
    pub(super) fn parse_create_property_graph(
        &mut self,
    ) -> Result<CreatePropertyGraph, SyntaxError> {
        let start = self.expect_keyword("CREATE")?.span.start;
        let temporary = self.consume_keyword("TEMP") || self.consume_keyword("TEMPORARY");
        self.expect_keyword("PROPERTY")?;
        self.expect_keyword("GRAPH")?;
        let name = self.parse_qualified_name()?;
        let mut vertex_tables = Vec::new();
        let mut edge_tables = Vec::new();

        if self.consume_keyword("VERTEX") || self.consume_keyword("NODE") {
            self.expect_keyword("TABLES")?;
            self.expect_kind(TokenKind::LeftParen, "(")?;
            vertex_tables = self.parse_nonempty_comma_separated(
                TokenKind::RightParen,
                Self::parse_vertex_table_definition,
                "at least one vertex table definition",
            )?;
            self.expect_kind(TokenKind::RightParen, ")")?;
        }
        if self.consume_keyword("EDGE") || self.consume_keyword("RELATIONSHIP") {
            self.expect_keyword("TABLES")?;
            self.expect_kind(TokenKind::LeftParen, "(")?;
            edge_tables = self.parse_nonempty_comma_separated(
                TokenKind::RightParen,
                Self::parse_edge_table_definition,
                "at least one edge table definition",
            )?;
            self.expect_kind(TokenKind::RightParen, ")")?;
        }
        if !matches!(self.current().kind, TokenKind::Semicolon | TokenKind::End) {
            return Err(self.unexpected("VERTEX TABLES followed by EDGE TABLES"));
        }

        Ok(CreatePropertyGraph {
            temporary,
            name,
            vertex_tables,
            edge_tables,
            span: Span::new(start, self.previous_end()),
        })
    }

    fn parse_vertex_table_definition(&mut self) -> Result<VertexTableDefinition, SyntaxError> {
        let start = self.current().span.start;
        let table = self.parse_qualified_name()?;
        let alias = self.parse_optional_as_alias()?;
        let key = self.parse_optional_key()?;
        let exposure = self.parse_optional_element_exposure()?;
        Ok(VertexTableDefinition {
            table,
            alias,
            key,
            exposure,
            span: Span::new(start, self.previous_end()),
        })
    }

    fn parse_edge_table_definition(&mut self) -> Result<EdgeTableDefinition, SyntaxError> {
        let start = self.current().span.start;
        let table = self.parse_qualified_name()?;
        let alias = self.parse_optional_as_alias()?;
        let key = self.parse_optional_key()?;
        self.expect_keyword("SOURCE")?;
        let source = self.parse_edge_endpoint()?;
        self.expect_keyword("DESTINATION")?;
        let destination = self.parse_edge_endpoint()?;
        let exposure = self.parse_optional_element_exposure()?;
        Ok(EdgeTableDefinition {
            table,
            alias,
            key,
            source,
            destination,
            exposure,
            span: Span::new(start, self.previous_end()),
        })
    }

    fn parse_edge_endpoint(&mut self) -> Result<EdgeEndpoint, SyntaxError> {
        let start = self.current().span.start;
        if self.consume_keyword("KEY") {
            let key = self.parse_identifier_list()?;
            self.expect_keyword("REFERENCES")?;
            let vertex = self.parse_identifier()?;
            let vertex_key = self.parse_identifier_list()?;
            Ok(EdgeEndpoint {
                key,
                vertex,
                vertex_key,
                span: Span::new(start, self.previous_end()),
            })
        } else {
            let vertex = self.parse_identifier()?;
            Ok(EdgeEndpoint {
                key: Vec::new(),
                vertex,
                vertex_key: Vec::new(),
                span: Span::new(start, self.previous_end()),
            })
        }
    }

    fn parse_optional_as_alias(&mut self) -> Result<Option<Identifier>, SyntaxError> {
        if self.consume_keyword("AS") {
            self.parse_identifier().map(Some)
        } else {
            Ok(None)
        }
    }

    fn parse_optional_key(&mut self) -> Result<Vec<Identifier>, SyntaxError> {
        if self.consume_keyword("KEY") {
            self.parse_identifier_list()
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_optional_element_exposure(&mut self) -> Result<Option<ElementExposure>, SyntaxError> {
        if matches!(
            self.current().kind,
            TokenKind::Comma | TokenKind::RightParen
        ) {
            return Ok(None);
        }
        if !["NO", "PROPERTIES", "LABEL", "DEFAULT"]
            .into_iter()
            .any(|keyword| self.at_keyword(keyword))
        {
            return Err(self.unexpected("LABEL, DEFAULT LABEL, PROPERTIES, or NO PROPERTIES"));
        }
        let start = self.current().span.start;
        let mut labels = Vec::new();
        if self.at_keyword("NO") || self.at_keyword("PROPERTIES") {
            let properties = self.parse_property_exposure()?;
            labels.push(ElementLabelExposure {
                label: ElementLabel::Implicit,
                properties,
                span: Span::new(start, self.previous_end()),
            });
        } else {
            while self.at_keyword("LABEL") || self.at_keyword("DEFAULT") {
                let label_start = self.current().span.start;
                let label = if self.consume_keyword("LABEL") {
                    ElementLabel::Named(self.parse_identifier()?)
                } else {
                    self.expect_keyword("DEFAULT")?;
                    self.expect_keyword("LABEL")?;
                    ElementLabel::Default
                };
                let properties = if self.at_keyword("NO") || self.at_keyword("PROPERTIES") {
                    self.parse_property_exposure()?
                } else {
                    PropertyExposure::AllColumns
                };
                labels.push(ElementLabelExposure {
                    label,
                    properties,
                    span: Span::new(label_start, self.previous_end()),
                });
            }
        }
        Ok(Some(ElementExposure {
            labels,
            span: Span::new(start, self.previous_end()),
        }))
    }

    fn parse_property_exposure(&mut self) -> Result<PropertyExposure, SyntaxError> {
        if self.consume_keyword("NO") {
            self.expect_keyword("PROPERTIES")?;
            return Ok(PropertyExposure::NoProperties);
        }

        self.expect_keyword("PROPERTIES")?;
        if self.consume_keyword("ALL") {
            self.expect_keyword("COLUMNS")?;
            return Ok(PropertyExposure::AllColumns);
        }

        self.expect_kind(TokenKind::LeftParen, "(")?;
        let expressions = self.parse_labeled_expressions("at least one property expression")?;
        self.expect_kind(TokenKind::RightParen, ")")?;
        Ok(PropertyExposure::Expressions(expressions))
    }
}
