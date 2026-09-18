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

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const QUERY_DIGEST_PROTOCOL_VERSION: u16 = 1;
pub const QUERY_TEXT_HASH_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryIdentity {
    normalized_query: String,
    query_digest: String,
    query_text_hash: String,
}

impl QueryIdentity {
    pub fn new(query_language: &str, query_text: &str) -> Self {
        let query_language = query_language.trim().to_ascii_lowercase();
        let normalized_query = normalize_query(&query_language, query_text);
        let query_digest = versioned_hash(
            "hawdb-query-digest",
            QUERY_DIGEST_PROTOCOL_VERSION,
            "q",
            &[query_language.as_bytes(), normalized_query.as_bytes()],
        );
        let query_text_hash = versioned_hash(
            "hawdb-query-text",
            QUERY_TEXT_HASH_PROTOCOL_VERSION,
            "t",
            &[query_language.as_bytes(), query_text.as_bytes()],
        );
        Self {
            normalized_query,
            query_digest,
            query_text_hash,
        }
    }

    pub fn normalized_query(&self) -> &str {
        &self.normalized_query
    }

    pub fn query_digest(&self) -> &str {
        &self.query_digest
    }

    pub fn query_text_hash(&self) -> &str {
        &self.query_text_hash
    }
}

pub fn normalize_query(query_language: &str, query_text: &str) -> String {
    Normalizer::new(query_language, query_text, NormalizationMode::Identity).normalize()
}

/// Normalizes syntax that does not affect planning while preserving every
/// value and binding name that can change the resulting physical plan.
pub fn normalize_query_for_plan_cache(query_language: &str, query_text: &str) -> String {
    Normalizer::new(query_language, query_text, NormalizationMode::PlanCache).normalize()
}

fn versioned_hash(domain: &str, version: u16, prefix: &str, fields: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, domain.as_bytes());
    hasher.update(version.to_le_bytes());
    for field in fields {
        hash_field(&mut hasher, field);
    }
    let digest = hasher.finalize();
    let mut output = format!("{prefix}{version}:");
    for byte in digest {
        use std::fmt::Write;
        write!(output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

struct Normalizer<'a> {
    query_language: &'a str,
    input: &'a str,
    position: usize,
    parameters: BTreeMap<String, usize>,
    tokens: Vec<String>,
    mode: NormalizationMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NormalizationMode {
    Identity,
    PlanCache,
}

impl<'a> Normalizer<'a> {
    fn new(query_language: &'a str, input: &'a str, mode: NormalizationMode) -> Self {
        Self {
            query_language,
            input,
            position: 0,
            parameters: BTreeMap::new(),
            tokens: Vec::new(),
            mode,
        }
    }

    fn normalize(mut self) -> String {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.advance();
            } else if ch == '$' {
                self.normalize_parameter();
            } else if ch == '\'' || ch == '"' {
                self.normalize_quoted(ch);
            } else if is_number_start(ch, self.peek_after(ch.len_utf8())) {
                self.normalize_number();
            } else if is_identifier_start(ch) {
                self.normalize_word();
            } else {
                self.advance();
                self.push_token("symbol", &ch.to_string());
            }
        }
        while self
            .tokens
            .last()
            .is_some_and(|token| token == "symbol:1:;")
        {
            self.tokens.pop();
        }
        self.tokens.join(" ")
    }

    fn normalize_parameter(&mut self) {
        self.advance();
        let name = self.consume_while(is_identifier_continue);
        if name.is_empty() {
            self.push_token("symbol", "$");
            return;
        }
        if self.mode == NormalizationMode::PlanCache {
            self.push_token("parameter", name);
            return;
        }
        let next_ordinal = self.parameters.len();
        let ordinal = *self
            .parameters
            .entry(name.to_string())
            .or_insert(next_ordinal);
        self.push_token("parameter", &ordinal.to_string());
    }

    fn normalize_quoted(&mut self, quote: char) {
        self.advance();
        let start = self.position;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            if escaped {
                escaped = false;
                self.advance();
                continue;
            }
            if ch == '\\' {
                escaped = true;
                self.advance();
                continue;
            }
            if ch == quote {
                if !self.query_language.eq_ignore_ascii_case("cypher")
                    && self.peek_after(ch.len_utf8()) == Some(quote)
                {
                    self.advance();
                    self.advance();
                    continue;
                }
                let value = &self.input[start..self.position];
                self.advance();
                if quote == '"' && !self.query_language.eq_ignore_ascii_case("cypher") {
                    self.push_token("quoted_identifier", value);
                } else if self.mode == NormalizationMode::PlanCache {
                    self.push_token("literal_value", &format!("{quote}{value}{quote}"));
                } else {
                    self.push_token("literal", "string");
                }
                return;
            }
            self.advance();
        }
        self.push_token("invalid_quoted", &quote.to_string());
    }

    fn normalize_number(&mut self) {
        let start = self.position;
        if self.peek() == Some('-') {
            self.advance();
        }
        self.consume_while(|ch| ch.is_ascii_digit());
        let mut kind = "int";
        if self.peek() == Some('.') {
            kind = "float";
            self.advance();
            self.consume_while(|ch| ch.is_ascii_digit());
        }
        if self.mode == NormalizationMode::PlanCache {
            let value = &self.input[start..self.position];
            self.push_token("literal_value", value);
        } else {
            self.push_token("literal", kind);
        }
    }

    fn normalize_word(&mut self) {
        let word = self.consume_while(is_identifier_continue).to_string();
        let lowercase = word.to_ascii_lowercase();
        if matches!(lowercase.as_str(), "true" | "false") {
            if self.mode == NormalizationMode::PlanCache {
                self.push_token("literal_value", &lowercase);
            } else {
                self.push_token("literal", "bool");
            }
        } else if word.eq_ignore_ascii_case("null") {
            self.push_token("literal", "null");
        } else if is_keyword(&word) {
            self.push_token("keyword", &lowercase);
        } else {
            self.push_token("identifier", &word);
        }
    }

    fn push_token(&mut self, kind: &str, value: &str) {
        self.tokens.push(format!("{kind}:{}:{value}", value.len()));
    }

    fn consume_while(&mut self, predicate: impl Fn(char) -> bool) -> &'a str {
        let start = self.position;
        while self.peek().is_some_and(&predicate) {
            self.advance();
        }
        &self.input[start..self.position]
    }

    fn peek(&self) -> Option<char> {
        self.input[self.position..].chars().next()
    }

    fn peek_after(&self, byte_offset: usize) -> Option<char> {
        self.input[self.position + byte_offset..].chars().next()
    }

    fn advance(&mut self) {
        self.position += self
            .peek()
            .expect("advance requires a character")
            .len_utf8();
    }
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_identifier_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn is_number_start(ch: char, next: Option<char>) -> bool {
    ch.is_ascii_digit() || (ch == '-' && next.is_some_and(|next| next.is_ascii_digit()))
}

fn is_keyword(word: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "all",
        "alter",
        "analyze",
        "and",
        "as",
        "asc",
        "assert",
        "avg",
        "backfill",
        "begin",
        "by",
        "call",
        "case",
        "cast",
        "checkpoint",
        "coalesce",
        "collect",
        "commit",
        "constraint",
        "contains",
        "count",
        "create",
        "current_timestamp",
        "cypher",
        "date_part",
        "delete",
        "delete_only",
        "desc",
        "detach",
        "distinct",
        "else",
        "end",
        "ends",
        "exists",
        "explain",
        "fulltext",
        "gc",
        "in",
        "index",
        "is",
        "label",
        "length",
        "limit",
        "match",
        "max",
        "merge",
        "min",
        "node",
        "nodes",
        "not",
        "offset",
        "on",
        "optional",
        "or",
        "order",
        "properties",
        "property",
        "public",
        "range",
        "relationship",
        "return",
        "rollback",
        "set",
        "shortest",
        "skip",
        "starts",
        "state",
        "system",
        "table",
        "then",
        "timestamp",
        "transaction",
        "type",
        "unique",
        "validating",
        "variable",
        "when",
        "where",
        "with",
        "write_only",
        "yield",
    ];
    KEYWORDS
        .binary_search(&word.to_ascii_lowercase().as_str())
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_ignores_keyword_case_whitespace_and_literals() {
        let first = QueryIdentity::new(
            "cypher",
            "MATCH (m:Memory {id: 'one'}) WHERE m.score >= 1 RETURN m.title",
        );
        let second = QueryIdentity::new(
            "cypher",
            " match (m:Memory { id : \"two\" }) where m.score>=99 return m.title; ",
        );

        assert_eq!(first.normalized_query(), second.normalized_query());
        assert_eq!(first.query_digest(), second.query_digest());
        assert_ne!(first.query_text_hash(), second.query_text_hash());
    }

    #[test]
    fn digest_preserves_identifier_and_literal_type() {
        let memory = QueryIdentity::new("cypher", "MATCH (m:Memory {id: 1}) RETURN m");
        let entity = QueryIdentity::new("cypher", "MATCH (m:Entity {id: 1}) RETURN m");
        let string_id = QueryIdentity::new("cypher", "MATCH (m:Memory {id: '1'}) RETURN m");

        assert_ne!(memory.query_digest(), entity.query_digest());
        assert_ne!(memory.query_digest(), string_id.query_digest());
    }

    #[test]
    fn parameter_names_are_canonicalized_with_correlation_preserved() {
        let first = QueryIdentity::new(
            "cypher",
            "MATCH (m:Memory) WHERE m.id = $id OR m.parent_id = $id RETURN m",
        );
        let renamed = QueryIdentity::new(
            "cypher",
            "MATCH (m:Memory) WHERE m.id = $value OR m.parent_id = $value RETURN m",
        );
        let independent = QueryIdentity::new(
            "cypher",
            "MATCH (m:Memory) WHERE m.id = $left OR m.parent_id = $right RETURN m",
        );

        assert_eq!(first.query_digest(), renamed.query_digest());
        assert_ne!(first.query_digest(), independent.query_digest());
    }

    #[test]
    fn digest_is_domain_separated_by_query_language() {
        let cypher = QueryIdentity::new("cypher", "MATCH (m) RETURN m");
        let case_variant = QueryIdentity::new(" CYPHER ", "MATCH (m) RETURN m");
        let sql = QueryIdentity::new("sql", "MATCH (m) RETURN m");

        assert_eq!(cypher.query_digest(), case_variant.query_digest());
        assert_ne!(cypher.query_digest(), sql.query_digest());
        assert!(cypher.query_digest().starts_with("q1:"));
        assert!(cypher.query_text_hash().starts_with("t1:"));
    }

    #[test]
    fn float_literals_with_or_without_fractional_digits_share_a_shape() {
        let first = QueryIdentity::new("cypher", "MATCH (m) WHERE m.score = 1. RETURN m");
        let second = QueryIdentity::new("cypher", "MATCH (m) WHERE m.score = 2.5 RETURN m");

        assert_eq!(first.query_digest(), second.query_digest());
    }

    #[test]
    fn sql_escaped_quotes_remain_one_literal_shape() {
        let escaped = QueryIdentity::new("sql", "SELECT 'it''s private'");
        let simple = QueryIdentity::new("sql", "SELECT 'public'");

        assert_eq!(escaped.query_digest(), simple.query_digest());
    }

    #[test]
    fn plan_cache_normalization_ignores_keyword_case_whitespace_and_trailing_semicolons() {
        let first = normalize_query_for_plan_cache(
            "cypher",
            "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
        );
        let second = normalize_query_for_plan_cache(
            "cypher",
            "  match (m:Memory)\nwhere m.id=$id\nreturn m.title as title;;; ",
        );

        assert_eq!(first, second);
    }

    #[test]
    fn plan_cache_normalization_preserves_literals_and_parameter_names() {
        let string_one = normalize_query_for_plan_cache("cypher", "RETURN 'one'");
        let string_two = normalize_query_for_plan_cache("cypher", "RETURN 'two'");
        let integer_one = normalize_query_for_plan_cache("cypher", "RETURN 1");
        let integer_two = normalize_query_for_plan_cache("cypher", "RETURN 2");
        let bool_true = normalize_query_for_plan_cache("cypher", "RETURN true");
        let bool_false = normalize_query_for_plan_cache("cypher", "RETURN false");
        let parameter_id = normalize_query_for_plan_cache("cypher", "RETURN $id");
        let parameter_value = normalize_query_for_plan_cache("cypher", "RETURN $value");

        assert_ne!(string_one, string_two);
        assert_ne!(integer_one, integer_two);
        assert_ne!(bool_true, bool_false);
        assert_ne!(parameter_id, parameter_value);
    }
}
