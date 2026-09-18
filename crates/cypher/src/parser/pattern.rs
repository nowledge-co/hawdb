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

use std::collections::{BTreeMap, BTreeSet};

use hawdb_core::Result;

use super::super::ast::*;
use super::Parser;

pub(super) struct BoundRelationshipMergePattern {
    pub(super) source_variable: String,
    pub(super) rel_variable: Option<String>,
    pub(super) rel_type: String,
    pub(super) rel_properties: BTreeMap<String, ValueExpression>,
    pub(super) target_variable: String,
}

impl Parser<'_> {
    pub(super) fn consume_match_path_binding_prefix(&mut self) -> Option<String> {
        self.skip_ws();
        let start = self.pos;
        let mut index = start;
        let mut chars = self.input[index..].chars();
        let first = chars.next()?;
        if !is_path_binding_ident_start(first) {
            return None;
        }
        index += first.len_utf8();
        while let Some(ch) = self.input[index..].chars().next() {
            if !is_path_binding_ident_continue(ch) {
                break;
            }
            index += ch.len_utf8();
        }
        let variable = self.input[start..index].to_string();
        index = skip_ascii_whitespace(self.input, index);
        if !self.input[index..].starts_with('=') {
            return None;
        }
        index += '='.len_utf8();
        index = skip_ascii_whitespace(self.input, index);
        if !self.input[index..].starts_with('(') {
            return None;
        }
        self.pos = index;
        Some(variable)
    }

    pub(super) fn next_relationship_pattern_is_all_shortest(&self) -> bool {
        let mut index = skip_ascii_whitespace(self.input, self.pos);
        if !self.input[index..].starts_with('-') {
            return false;
        }
        index += '-'.len_utf8();
        index = skip_ascii_whitespace(self.input, index);
        if !self.input[index..].starts_with('[') {
            return false;
        }
        let Some(end) = self.input[index..].find(']') else {
            return false;
        };
        self.input[index..index + end]
            .to_ascii_uppercase()
            .contains("ALL SHORTEST")
    }

    pub(super) fn parse_post_match_relationship_expand(
        &mut self,
        source_variable: &str,
        source_label: &str,
        source_properties: &BTreeMap<String, ValueExpression>,
    ) -> Result<Option<PostMatchRelationshipExpand>> {
        if self.peek_char() == Some('<') {
            self.expect_char('<')?;
            self.expect_char('-')?;
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            self.expect_char('-')?;
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            return Ok(Some(PostMatchRelationshipExpand {
                source_variable: source_variable.to_string(),
                source_label: source_label.to_string(),
                source_properties: source_properties.clone(),
                expand: RelationshipExpand {
                    variable: rel_variable,
                    rel_type,
                    properties,
                    direction: RelationshipDirection::Incoming,
                    target_variable,
                    target_label,
                    target_properties,
                    min_hops,
                    max_hops,
                },
            }));
        }
        if self.peek_char() == Some('-') {
            self.expect_char('-')?;
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            self.expect_char('-')?;
            let direction = if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            return Ok(Some(PostMatchRelationshipExpand {
                source_variable: source_variable.to_string(),
                source_label: source_label.to_string(),
                source_properties: source_properties.clone(),
                expand: RelationshipExpand {
                    variable: rel_variable,
                    rel_type,
                    properties,
                    direction,
                    target_variable,
                    target_label,
                    target_properties,
                    min_hops,
                    max_hops,
                },
            }));
        }
        Ok(None)
    }

    pub(super) fn parse_bound_relationship_create_pattern(
        &mut self,
    ) -> Result<(String, String, BTreeMap<String, ValueExpression>, String)> {
        self.expect_char('(')?;
        let source_variable = self.parse_ident()?;
        self.expect_char(')')?;
        self.expect_char('-')?;
        self.expect_char('[')?;
        if self.peek_char() != Some(':') {
            self.parse_ident()?;
            self.skip_ws();
        }
        self.expect_char(':')?;
        let rel_type = self.parse_ident()?;
        self.skip_ws();
        let rel_properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.expect_char(']')?;
        self.expect_char('-')?;
        self.expect_char('>')?;
        self.expect_char('(')?;
        let target_variable = self.parse_ident()?;
        self.expect_char(')')?;
        Ok((source_variable, rel_type, rel_properties, target_variable))
    }

    pub(super) fn parse_bound_relationship_merge_pattern(
        &mut self,
    ) -> Result<BoundRelationshipMergePattern> {
        self.expect_char('(')?;
        let source_variable = self.parse_ident()?;
        self.expect_char(')')?;
        self.expect_char('-')?;
        self.expect_char('[')?;
        let rel_variable = if self.peek_char() == Some(':') {
            None
        } else {
            Some(self.parse_ident()?)
        };
        self.expect_char(':')?;
        let rel_type = self.parse_ident()?;
        self.skip_ws();
        let rel_properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.expect_char(']')?;
        self.expect_char('-')?;
        self.expect_char('>')?;
        self.expect_char('(')?;
        let target_variable = self.parse_ident()?;
        self.expect_char(')')?;
        Ok(BoundRelationshipMergePattern {
            source_variable,
            rel_variable,
            rel_type,
            rel_properties,
            target_variable,
        })
    }

    pub(super) fn parse_optional_relationship_expand(
        &mut self,
        scope: &BTreeSet<String>,
    ) -> Result<OptionalRelationshipExpand> {
        let (source_variable, source_label, source_properties) = self.parse_match_node_pattern()?;
        let (rel_variable, rel_type, rel_properties, direction) = if self.consume_char('<') {
            self.expect_char('-')?;
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if min_hops != 1 || max_hops != 1 {
                return Err(self.error("OPTIONAL MATCH supports only one-hop relationships"));
            }
            self.expect_char('-')?;
            (
                rel_variable,
                rel_type,
                properties,
                RelationshipDirection::Incoming,
            )
        } else {
            self.expect_char('-')?;
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if min_hops != 1 || max_hops != 1 {
                return Err(self.error("OPTIONAL MATCH supports only one-hop relationships"));
            }
            self.expect_char('-')?;
            let direction = if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            (rel_variable, rel_type, properties, direction)
        };
        let (target_variable, target_label, target_properties) = self.parse_match_node_pattern()?;
        if scope.contains(&source_variable) {
            return Ok(OptionalRelationshipExpand {
                source_variable,
                source_label,
                expand: RelationshipExpand {
                    variable: rel_variable,
                    rel_type,
                    properties: rel_properties,
                    direction,
                    target_variable,
                    target_label,
                    target_properties,
                    min_hops: 1,
                    max_hops: 1,
                },
            });
        }
        if scope.contains(&target_variable) {
            let direction = match direction {
                RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
                RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
                RelationshipDirection::Undirected => RelationshipDirection::Undirected,
            };
            return Ok(OptionalRelationshipExpand {
                source_variable: target_variable,
                source_label: target_label,
                expand: RelationshipExpand {
                    variable: rel_variable,
                    rel_type,
                    properties: rel_properties,
                    direction,
                    target_variable: source_variable,
                    target_label: source_label,
                    target_properties: source_properties,
                    min_hops: 1,
                    max_hops: 1,
                },
            });
        }
        Err(self.error("OPTIONAL MATCH must reference a bound node variable"))
    }
}

fn skip_ascii_whitespace(input: &str, mut index: usize) -> usize {
    while let Some(ch) = input[index..].chars().next() {
        if !ch.is_whitespace() {
            break;
        }
        index += ch.len_utf8();
    }
    index
}

fn is_path_binding_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_path_binding_ident_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}
