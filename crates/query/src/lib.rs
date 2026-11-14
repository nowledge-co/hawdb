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
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Write;

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
        let query_language = query_language.trim();
        let query_language = if query_language.bytes().any(|byte| byte.is_ascii_uppercase()) {
            Cow::Owned(query_language.to_ascii_lowercase())
        } else {
            Cow::Borrowed(query_language)
        };
        let normalized_query = normalize_query(&query_language, query_text);
        Self::from_normalized(&query_language, query_text, normalized_query)
    }

    /// Computes query identity and its value-preserving plan-cache key together.
    /// The outputs match `new` and `normalize_query_for_plan_cache` respectively.
    pub fn new_with_plan_cache_key(query_language: &str, query_text: &str) -> (Self, String) {
        // `new` trims the language; the standalone plan-cache API does not.
        // Retain both established interpretations for noncanonical callers.
        if query_language != query_language.trim() {
            return (
                Self::new(query_language, query_text),
                normalize_query_for_plan_cache(query_language, query_text),
            );
        }
        let language = if query_language.bytes().any(|byte| byte.is_ascii_uppercase()) {
            Cow::Owned(query_language.to_ascii_lowercase())
        } else {
            Cow::Borrowed(query_language)
        };
        let (identity, plan_cache_key) =
            Normalizer::new(&language, query_text, NormalizationMode::PlanCache).normalize_pair();
        (
            Self::from_normalized(&language, query_text, identity),
            plan_cache_key,
        )
    }

    fn from_normalized(query_language: &str, query_text: &str, normalized_query: String) -> Self {
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
    let mut output = String::with_capacity(
        prefix.len() + decimal_len(usize::from(version)) + 1 + digest.len() * 2,
    );
    write!(output, "{prefix}{version}:").expect("writing to a String cannot fail");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[derive(Clone, Copy)]
enum NormalizedToken<'a> {
    Keyword(&'a str),
    Identifier(&'a str),
    ParameterName(&'a str),
    ParameterOrdinal(usize),
    LiteralShape(&'static str),
    LiteralValue(&'a str),
    LowercaseLiteralValue(&'a str),
    SingleQuotedLiteral(&'a str),
    DoubleQuotedLiteral(&'a str),
    QuotedIdentifier(&'a str),
    Symbol(&'a str),
    InvalidQuote(char),
}

fn decimal_len(value: usize) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

impl NormalizedToken<'_> {
    fn header(&self) -> (&'static str, usize) {
        match self {
            Self::Keyword(value) => ("keyword", value.len()),
            Self::Identifier(value) => ("identifier", value.len()),
            Self::ParameterName(value) => ("parameter", value.len()),
            Self::ParameterOrdinal(value) => ("parameter", decimal_len(*value)),
            Self::LiteralShape(value) => ("literal", value.len()),
            Self::LiteralValue(value) | Self::LowercaseLiteralValue(value) => {
                ("literal_value", value.len())
            }
            Self::SingleQuotedLiteral(value) | Self::DoubleQuotedLiteral(value) => (
                "literal_value",
                value
                    .len()
                    .checked_add(2)
                    .expect("quoted token length overflow"),
            ),
            Self::QuotedIdentifier(value) => ("quoted_identifier", value.len()),
            Self::Symbol(value) => ("symbol", value.len()),
            Self::InvalidQuote(value) => ("invalid_quoted", value.len_utf8()),
        }
    }

    fn serialized_len(&self) -> usize {
        let (kind, value_len) = self.header();
        kind.len()
            .checked_add(2)
            .and_then(|length| length.checked_add(decimal_len(value_len)))
            .and_then(|length| length.checked_add(value_len))
            .expect("normalized token length overflow")
    }

    fn append_to(self, output: &mut String) {
        let (kind, value_len) = self.header();
        write!(output, "{kind}:{value_len}:").expect("writing to a String cannot fail");
        match self {
            Self::Keyword(value) | Self::LowercaseLiteralValue(value) => {
                // These variants come from the scanner's ASCII identifiers.
                for byte in value.bytes() {
                    output.push(char::from(byte.to_ascii_lowercase()));
                }
            }
            Self::ParameterOrdinal(value) => {
                write!(output, "{value}").expect("writing to a String cannot fail");
            }
            Self::SingleQuotedLiteral(value) => {
                output.push('\'');
                output.push_str(value);
                output.push('\'');
            }
            Self::DoubleQuotedLiteral(value) => {
                output.push('"');
                output.push_str(value);
                output.push('"');
            }
            Self::InvalidQuote(value) => output.push(value),
            Self::Identifier(value)
            | Self::ParameterName(value)
            | Self::LiteralShape(value)
            | Self::LiteralValue(value)
            | Self::QuotedIdentifier(value)
            | Self::Symbol(value) => output.push_str(value),
        }
    }
}

struct Normalizer<'a> {
    query_language: &'a str,
    input: &'a str,
    position: usize,
    parameters: BTreeMap<&'a str, usize>,
    tokens: Vec<NormalizedToken<'a>>,
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
        self.scan();
        Self::render(&self.tokens)
    }

    fn normalize_pair(mut self) -> (String, String) {
        self.scan();
        let plan_cache_key = Self::render(&self.tokens);
        for token in &mut self.tokens {
            *token = match *token {
                NormalizedToken::ParameterName(name) => {
                    let next = self.parameters.len();
                    let ordinal = *self.parameters.entry(name).or_insert(next);
                    NormalizedToken::ParameterOrdinal(ordinal)
                }
                NormalizedToken::LiteralValue(value) => {
                    NormalizedToken::LiteralShape(if value.contains('.') { "float" } else { "int" })
                }
                NormalizedToken::LowercaseLiteralValue(_) => NormalizedToken::LiteralShape("bool"),
                NormalizedToken::SingleQuotedLiteral(_)
                | NormalizedToken::DoubleQuotedLiteral(_) => {
                    NormalizedToken::LiteralShape("string")
                }
                token => token,
            };
        }
        (Self::render(&self.tokens), plan_cache_key)
    }

    fn scan(&mut self) {
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
                let start = self.position;
                self.advance();
                self.tokens
                    .push(NormalizedToken::Symbol(&self.input[start..self.position]));
            }
        }
        while self
            .tokens
            .last()
            .is_some_and(|token| matches!(token, NormalizedToken::Symbol(";")))
        {
            self.tokens.pop();
        }
    }

    fn render(tokens: &[NormalizedToken<'_>]) -> String {
        let byte_len = tokens
            .iter()
            .try_fold(tokens.len().saturating_sub(1), |length, token| {
                length.checked_add(token.serialized_len())
            })
            .expect("normalized query length overflow");
        let mut output = String::with_capacity(byte_len);
        for (index, token) in tokens.iter().copied().enumerate() {
            if index != 0 {
                output.push(' ');
            }
            token.append_to(&mut output);
        }
        output
    }

    fn normalize_parameter(&mut self) {
        self.advance();
        let name = self.consume_while(is_identifier_continue);
        if name.is_empty() {
            self.tokens.push(NormalizedToken::Symbol("$"));
            return;
        }
        if self.mode == NormalizationMode::PlanCache {
            self.tokens.push(NormalizedToken::ParameterName(name));
            return;
        }
        let next_ordinal = self.parameters.len();
        let ordinal = *self.parameters.entry(name).or_insert(next_ordinal);
        self.tokens.push(NormalizedToken::ParameterOrdinal(ordinal));
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
                    self.tokens.push(NormalizedToken::QuotedIdentifier(value));
                } else if self.mode == NormalizationMode::PlanCache {
                    self.tokens.push(match quote {
                        '\'' => NormalizedToken::SingleQuotedLiteral(value),
                        '"' => NormalizedToken::DoubleQuotedLiteral(value),
                        _ => unreachable!("quoted normalization requires a quote"),
                    });
                } else {
                    self.tokens.push(NormalizedToken::LiteralShape("string"));
                }
                return;
            }
            self.advance();
        }
        self.tokens.push(NormalizedToken::InvalidQuote(quote));
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
            self.tokens.push(NormalizedToken::LiteralValue(value));
        } else {
            self.tokens.push(NormalizedToken::LiteralShape(kind));
        }
    }

    fn normalize_word(&mut self) {
        let word = self.consume_while(is_identifier_continue);
        if word.eq_ignore_ascii_case("true") || word.eq_ignore_ascii_case("false") {
            if self.mode == NormalizationMode::PlanCache {
                self.tokens
                    .push(NormalizedToken::LowercaseLiteralValue(word));
            } else {
                self.tokens.push(NormalizedToken::LiteralShape("bool"));
            }
        } else if word.eq_ignore_ascii_case("null") {
            self.tokens.push(NormalizedToken::LiteralShape("null"));
        } else if is_keyword(word) {
            self.tokens.push(NormalizedToken::Keyword(word));
        } else {
            self.tokens.push(NormalizedToken::Identifier(word));
        }
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
        .binary_search_by(|keyword| {
            keyword
                .bytes()
                .cmp(word.bytes().map(|byte| byte.to_ascii_lowercase()))
        })
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versioned_hash_preserves_framing_and_lowercase_hex_vectors() {
        // SHA-256 vectors independently generated from length-prefixed UTF-8
        // fields and a little-endian u16 version, including NUL boundaries.
        for (domain, version, prefix, fields, expected) in [
            (
                "hawdb-query-digest",
                1_u16,
                "q",
                ["cypher", ""],
                "q1:89601b19dd789b0119adac19f1f1ab94bdb42e9be6825447e2800b9699526396",
            ),
            (
                "hawdb-query-text",
                1,
                "t",
                ["sql", "SELECT 'it''s private'"],
                "t1:bdfd08955ffda9b649b2a41201faaf7bab5bcf5cd53e3cdccf38d154cd022458",
            ),
            (
                "",
                0,
                "",
                ["", ""],
                "0:659d36ca563ba4622daabb36a71dafaf6060cdcbf89bb12e75426198496d272c",
            ),
            (
                "domain\0boundary",
                u16::MAX,
                "\u{03c0}",
                ["a\0b", "\u{03a9}"],
                "\u{03c0}65535:4cbddd3a3978d0a559d9f269127cf5bc18945380cf688addc6ca2fdb30486a41",
            ),
        ] {
            assert_eq!(
                versioned_hash(domain, version, prefix, &fields.map(str::as_bytes)),
                expected,
            );
        }
    }

    #[test]
    fn identity_preserves_published_protocol_vectors() {
        for (language, text, digest, text_hash) in [
            (
                "cypher",
                "",
                "q1:89601b19dd789b0119adac19f1f1ab94bdb42e9be6825447e2800b9699526396",
                "t1:d084d9e727251ad31b8cdb5d6389d5812986e45fc16d16f6709b7e1fb3c25782",
            ),
            (
                "cypher",
                "MATCH (n:Boundary) WHERE n.id = $id RETURN n.id AS id, n.score AS score, n.title AS title",
                "q1:2c82c0a53fcd28c7a81749ec4101a3efd2f54dcb4fc1f8f33aa1eb03cebbc15c",
                "t1:aee76710fc4272963bce14a5341905eb56a2e354139d6da5ed47fb0b4bd26981",
            ),
            (
                "sql",
                "SELECT id, title FROM memories WHERE id = $1",
                "q1:c67b1e7293af9ae38b0b4477c17e560dac54b64543b8ba8f2e7d56a1e05b42f8",
                "t1:9bcab90876769e1e44528ef99d873f978f6e856ec7f6fd7a79d72022389c4cb7",
            ),
        ] {
            let identity = QueryIdentity::new(language, text);
            assert_eq!(identity.query_digest(), digest);
            assert_eq!(identity.query_text_hash(), text_hash);
        }
    }

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
        for language in [
            "cypher",
            " cypher ",
            "\u{00a0}cypher\u{2003}",
            " CYPHER ",
            "cYpHeR",
        ] {
            assert_eq!(cypher, QueryIdentity::new(language, "MATCH (m) RETURN m"));
        }
        let sql = QueryIdentity::new("sql", "MATCH (m) RETURN m");

        assert_ne!(cypher.query_digest(), sql.query_digest());
        assert!(cypher.query_digest().starts_with("q1:"));
        assert!(cypher.query_text_hash().starts_with("t1:"));
    }

    #[test]
    fn language_canonicalization_preserves_non_ascii_case() {
        let query = "SELECT 1";
        let upper = QueryIdentity::new("\u{03a9}", query);
        let lower = QueryIdentity::new("\u{03c9}", query);
        assert_ne!(upper.query_digest(), lower.query_digest());
        assert_ne!(upper.query_text_hash(), lower.query_text_hash());
        assert_eq!(
            QueryIdentity::new("\u{03a9}SQL", query),
            QueryIdentity::new("\u{03a9}sql", query)
        );
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

    #[test]
    fn token_lengths_use_utf8_bytes_and_preserve_embedded_semicolons() {
        let query = "RETURN '\u{77e5}\u{8bc6}\0\u{1f642}' AS value;;;";
        assert_eq!(
            normalize_query_for_plan_cache("cypher", query),
            "keyword:6:return literal_value:13:'\u{77e5}\u{8bc6}\0\u{1f642}' keyword:2:as identifier:5:value"
        );
        assert_eq!(
            normalize_query("cypher", query),
            "keyword:6:return literal:6:string keyword:2:as identifier:5:value"
        );
        assert_eq!(
            normalize_query_for_plan_cache("cypher", "RETURN 'symbol:1:;' ;;;"),
            "keyword:6:return literal_value:12:'symbol:1:;'"
        );
        assert_eq!(normalize_query("cypher", "; ; ;\n"), "");
        assert_eq!(
            normalize_query("cypher", "\u{1f642};;"),
            "symbol:4:\u{1f642}"
        );
    }

    #[test]
    fn parameter_ordinal_encoding_crosses_decimal_boundaries() {
        let query = (0..101)
            .map(|index| format!("$p{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let expected = (0..101)
            .map(|index| format!("parameter:{}:{index}", index.to_string().len()))
            .collect::<Vec<_>>()
            .join(" symbol:1:, ");
        assert_eq!(normalize_query("cypher", &query), expected);
        let repeated = format!("{query},$p10,$p0");
        assert_eq!(
            normalize_query("cypher", &repeated),
            format!("{expected} symbol:1:, parameter:2:10 symbol:1:, parameter:1:0")
        );
    }

    #[test]
    fn borrowed_literal_descriptors_keep_exact_output_capacity() {
        let literal = "x".repeat(16 * 1024);
        let query = format!("RETURN '{literal}' AS value;;;");
        let identity = normalize_query("cypher", &query);
        assert_eq!(
            identity,
            "keyword:6:return literal:6:string keyword:2:as identifier:5:value"
        );
        assert_eq!(identity.capacity(), identity.len());
        let cache_key = normalize_query_for_plan_cache("cypher", &query);
        assert_eq!(
            cache_key,
            format!(
                "keyword:6:return literal_value:16386:'{literal}' keyword:2:as identifier:5:value"
            )
        );
        assert_eq!(cache_key.capacity(), cache_key.len());
    }

    #[test]
    fn combined_identity_and_plan_key_preserve_both_contracts() {
        let queries = [
            "",
            "; ; ;\n",
            "MATCH (n:Memory) WHERE n.id = $id RETURN n.title LIMIT 1",
            "MATCH (n) RETURN $a, $b, $a, $c, $b",
            "RETURN $1, $2, $1, $01, $, $_, $A, $a",
            "RETURN 1, -1, 1.0, 1., -0.5, 1e2, 1.2e-3",
            "RETURN true, FALSE, null, NULL, n.true, n.false",
            r#"RETURN 'one', "two", 'symbol:1:;', 'it\'s'"#,
            r#"SELECT "Quoted", 'it''s', 42 FROM table_name WHERE id = $id"#,
            "RETURN 'unterminated",
            "RETURN \"unterminated",
            "RETURN '\\",
            "RETURN 日本語, '你好é', $id, 🙂;;;",
            "RETURN (1 + 2), [1, 2], {a: 3}; ;",
            "MATCH (n) // $not_a_binding\nRETURN n",
            "RETURN `quoted name`, '\\n', '\\\\', 'a\nb'",
        ];
        for language in [
            "cypher", "CyPhEr", " cypher ", "sql", "SQL", " sql ", "", "other",
        ] {
            for query in queries {
                let (identity, plan_key) = QueryIdentity::new_with_plan_cache_key(language, query);
                assert_eq!(
                    identity,
                    QueryIdentity::new(language, query),
                    "{language:?}: {query}"
                );
                assert_eq!(
                    plan_key,
                    normalize_query_for_plan_cache(language, query),
                    "{language:?}: {query}"
                );
            }
        }
    }
}
