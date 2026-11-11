// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Engine-owned BM25 postings over one TEXT column of a RowPage table.
//!
//! A search index is a synchronously maintained derived relation: its
//! postings, per-document lengths and corpus statistics are ordinary rows of a
//! hidden RowPage table that the base transaction writes before it commits.
//! WAL replay re-derives them from the same deterministic analyzer, so there is
//! no separately acknowledged projection, watermark or catch-up job.

use super::*;

/// Postings and statistics share one relation keyed by `(term, document)`.
pub const SEARCH_INDEX_TERM_COLUMN: &str = "__hawdb_search_term";
pub const SEARCH_INDEX_DOCUMENT_COLUMN: &str = "__hawdb_search_doc";
pub const SEARCH_INDEX_FREQUENCY_COLUMN: &str = "__hawdb_search_tf";
pub const SEARCH_INDEX_LENGTH_COLUMN: &str = "__hawdb_search_len";
/// Number of physical columns in a search index relation.
pub const SEARCH_INDEX_COLUMN_COUNT: usize = 4;
/// Tokens longer than this many bytes are not indexed (encoded blobs, hashes).
pub const SEARCH_INDEX_MAX_TOKEN_BYTES: usize = 128;

/// The deterministic text analysis bound into a persistent definition. A new
/// analysis is a new variant; an existing one never changes its output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchIndexAnalyzer {
    /// Lowercased Unicode letter/digit words, with full-width ASCII folded to
    /// ASCII; Han, Kana and Hangul runs indexed as character unigrams and
    /// overlapping bigrams.
    CjkNgram,
}

impl SearchIndexAnalyzer {
    pub const DEFAULT: Self = Self::CjkNgram;

    pub fn name(self) -> &'static str {
        match self {
            Self::CjkNgram => "cjk_ngram",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        name.eq_ignore_ascii_case("cjk_ngram")
            .then_some(Self::CjkNgram)
    }

    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::CjkNgram => 1,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        (tag == 1).then_some(Self::CjkNgram)
    }

    /// Index-side tokens in document order, with repetition. The count is
    /// the BM25 document length.
    pub fn index_tokens(self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        analyze(text, |run| match run {
            Run::Word(word) => tokens.push(word),
            Run::Ideographic(chars) => {
                for (position, ch) in chars.iter().enumerate() {
                    tokens.push(ch.to_string());
                    if let Some(next) = chars.get(position + 1) {
                        tokens.push([*ch, *next].iter().collect());
                    }
                }
            }
        });
        tokens
    }

    /// Distinct query-side terms in ascending order. Multi-character
    /// ideographic runs use their bigrams, which are more selective than the
    /// unigrams also present in the index; a lone character uses its unigram.
    pub fn query_terms(self, text: &str) -> Vec<String> {
        let mut terms = BTreeSet::new();
        analyze(text, |run| match run {
            Run::Word(word) => {
                terms.insert(word);
            }
            Run::Ideographic(chars) if chars.len() == 1 => {
                terms.insert(chars[0].to_string());
            }
            Run::Ideographic(chars) => {
                for pair in chars.windows(2) {
                    terms.insert(pair.iter().collect());
                }
            }
        });
        terms.into_iter().collect()
    }
}

/// The search-specific part of a derived relation definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexSpec {
    pub column: String,
    pub analyzer: SearchIndexAnalyzer,
}

enum Run {
    Word(String),
    Ideographic(Vec<char>),
}

fn fold(ch: char) -> char {
    // Full-width ASCII forms (U+FF01..=U+FF5E) and the ideographic space.
    match ch as u32 {
        0xFF01..=0xFF5E => char::from_u32(ch as u32 - 0xFEE0).unwrap_or(ch),
        0x3000 => ' ',
        _ => ch,
    }
}

pub(crate) fn is_ideographic(ch: char) -> bool {
    matches!(
        ch as u32,
        0x1100..=0x11FF // Hangul Jamo
            | 0x2E80..=0x2FDF // CJK radicals, Kangxi radicals
            | 0x3040..=0x309F // Hiragana
            | 0x30A0..=0x30FF // Katakana
            | 0x3130..=0x318F // Hangul compatibility Jamo
            | 0x31F0..=0x31FF // Katakana phonetic extensions
            | 0x3400..=0x4DBF // CJK extension A
            | 0x4E00..=0x9FFF // CJK unified ideographs
            | 0xAC00..=0xD7AF // Hangul syllables
            | 0xF900..=0xFAFF // CJK compatibility ideographs
            | 0x20000..=0x323AF // CJK extensions B..H
    )
}

fn analyze(text: &str, mut emit: impl FnMut(Run)) {
    let mut word = String::new();
    let mut ideographic = Vec::new();
    let flush_word = |word: &mut String, emit: &mut dyn FnMut(Run)| {
        if !word.is_empty() {
            let token = std::mem::take(word);
            if token.len() <= SEARCH_INDEX_MAX_TOKEN_BYTES {
                emit(Run::Word(token));
            }
        }
    };
    let flush_ideographic = |run: &mut Vec<char>, emit: &mut dyn FnMut(Run)| {
        if !run.is_empty() {
            emit(Run::Ideographic(std::mem::take(run)));
        }
    };
    for ch in text.chars().map(fold) {
        if is_ideographic(ch) {
            flush_word(&mut word, &mut emit);
            ideographic.push(ch);
        } else if ch.is_alphanumeric() {
            flush_ideographic(&mut ideographic, &mut emit);
            word.extend(ch.to_lowercase());
        } else {
            flush_word(&mut word, &mut emit);
            flush_ideographic(&mut ideographic, &mut emit);
        }
    }
    flush_word(&mut word, &mut emit);
    flush_ideographic(&mut ideographic, &mut emit);
}

pub(super) fn table_schema(
    name: String,
    definition: &MaterializedViewDefinition,
    spec: &SearchIndexSpec,
    source: &RelationalTableSchema,
) -> Result<RelationalTableSchema, RelationalError> {
    if !definition.columns.is_empty()
        || !definition.group_by.is_empty()
        || definition.filter.is_some()
    {
        return Err(RelationalError::Schema(
            "search index definitions have no projection, grouping or filter".into(),
        ));
    }
    let column = source_column(source, &spec.column)?;
    if column.scalar_type != RelationalScalarType::Text {
        return Err(RelationalError::Schema(format!(
            "search index column {} must be TEXT",
            spec.column
        )));
    }
    let internal = |name: &str, scalar_type| RelationalColumnSchema {
        name: name.into(),
        scalar_type,
        nullable: false,
        default: None,
    };
    let schema = RelationalTableSchema {
        name,
        columns: vec![
            internal(SEARCH_INDEX_TERM_COLUMN, RelationalScalarType::Text),
            internal(SEARCH_INDEX_DOCUMENT_COLUMN, RelationalScalarType::Bytea),
            internal(SEARCH_INDEX_FREQUENCY_COLUMN, RelationalScalarType::BigInt),
            internal(SEARCH_INDEX_LENGTH_COLUMN, RelationalScalarType::BigInt),
        ],
        primary_key: vec![
            SEARCH_INDEX_TERM_COLUMN.into(),
            SEARCH_INDEX_DOCUMENT_COLUMN.into(),
        ],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        materialized_view: Some(Box::new(definition.clone())),
    };
    validate_table_schema(&schema)?;
    Ok(schema)
}

/// The statistics row: `(term = '', document = b'')` carries the indexed
/// document count in the frequency column and the total token count in the
/// length column. Analyzer terms are never empty and encoded primary keys are
/// never empty, so it cannot collide with a posting.
pub fn search_index_statistics_key() -> RelationalKey {
    RelationalKey(vec![
        RelationalValue::Text(String::new()),
        RelationalValue::Bytea(Vec::new()),
    ])
}

/// The primary-key interval holding every posting of one term.
pub fn search_index_term_range(term: &str) -> Result<RelationalPrimaryKeyRange, RelationalError> {
    RelationalPrimaryKeyRange::from_prefix_bounds(
        RelationalKey(vec![RelationalValue::Text(term.to_owned())]),
        None,
        None,
    )
}

fn analyzed_document(
    spec: &SearchIndexSpec,
    source: &RelationalTableSchema,
    row: Option<&RelationalRow>,
) -> Result<(BTreeMap<String, i64>, i64), RelationalError> {
    let Some(row) = row else {
        return Ok((BTreeMap::new(), 0));
    };
    let text = match value(source, row, &spec.column) {
        RelationalValue::Null => return Ok((BTreeMap::new(), 0)),
        RelationalValue::Text(text) => text.as_str(),
        _ => {
            return Err(RelationalError::Corruption(
                "search index source value is not resident TEXT".into(),
            ))
        }
    };
    let tokens = spec.analyzer.index_tokens(text);
    let length = i64::try_from(tokens.len())
        .map_err(|_| RelationalError::Admission("search document is too long".into()))?;
    let mut terms = BTreeMap::new();
    for token in tokens {
        *terms.entry(token).or_insert(0i64) += 1;
    }
    Ok((terms, length))
}

fn posting_key(term: &str, document: &[u8]) -> RelationalKey {
    RelationalKey(vec![
        RelationalValue::Text(term.to_owned()),
        RelationalValue::Bytea(document.to_vec()),
    ])
}

pub(super) struct SearchMaintenance<'a, 'b> {
    pub previous: &'a RelationalState,
    pub next: &'a mut RelationalState,
    pub schema: &'a RelationalTableSchema,
    pub source: &'a RelationalTableSchema,
    pub spec: &'a SearchIndexSpec,
    pub rebuild: bool,
    pub view_keys: &'a mut BTreeSet<RelationalKey>,
    pub overflow_config: RelationalOverflowConfig,
    pub budget: &'a mut MaintenanceBudget<'b>,
}

pub(super) fn maintain(
    maintenance: SearchMaintenance<'_, '_>,
    keys: &BTreeSet<RelationalKey>,
) -> Result<(), RelationalError> {
    let SearchMaintenance {
        previous,
        next,
        schema,
        source,
        spec,
        rebuild,
        view_keys,
        overflow_config,
        budget,
    } = maintenance;
    let mut documents = 0i64;
    let mut tokens = 0i64;
    for key in keys {
        let old = if rebuild {
            None
        } else {
            budget.read(previous, &source.name, key)?
        };
        // Overflowed TEXT is hydrated by the maintenance read before analysis.
        let new = budget.read(next, &source.name, key)?;
        let (old_terms, old_length) = analyzed_document(spec, source, old.as_ref())?;
        let (new_terms, new_length) = analyzed_document(spec, source, new.as_ref())?;
        if old_terms == new_terms {
            continue;
        }
        let document = encode_relational_primary_key(key)?;
        budget.charge(0, document.len())?;
        for term in old_terms.keys() {
            if !new_terms.contains_key(term) {
                write_output(
                    next,
                    schema,
                    posting_key(term, &document),
                    None,
                    view_keys,
                    overflow_config,
                    budget,
                )?;
            }
        }
        for (term, frequency) in &new_terms {
            if old_length == new_length && old_terms.get(term) == Some(frequency) {
                continue;
            }
            budget.charge(0, term.len().saturating_add(document.len()))?;
            let row = RelationalRow::new(vec![
                RelationalValue::Text(term.clone()),
                RelationalValue::Bytea(document.clone()),
                RelationalValue::BigInt(*frequency),
                RelationalValue::BigInt(new_length),
            ]);
            write_output(
                next,
                schema,
                posting_key(term, &document),
                Some(row),
                view_keys,
                overflow_config,
                budget,
            )?;
        }
        documents += i64::from(!new_terms.is_empty()) - i64::from(!old_terms.is_empty());
        tokens = tokens
            .checked_add(new_length - old_length)
            .ok_or_else(|| RelationalError::Constraint("search index length overflow".into()))?;
    }
    if !rebuild && documents == 0 && tokens == 0 {
        return Ok(());
    }
    let key = search_index_statistics_key();
    let (old_documents, old_tokens) = if rebuild {
        (0, 0)
    } else {
        match budget.read(previous, &schema.name, &key)? {
            Some(row) => match row.values() {
                [_, _, RelationalValue::BigInt(documents), RelationalValue::BigInt(tokens)] => {
                    (*documents, *tokens)
                }
                _ => {
                    return Err(RelationalError::Corruption(
                        "invalid search index statistics row".into(),
                    ))
                }
            },
            None => (0, 0),
        }
    };
    let documents = old_documents
        .checked_add(documents)
        .filter(|count| *count >= 0)
        .ok_or_else(|| RelationalError::Corruption("negative search document count".into()))?;
    let tokens = old_tokens
        .checked_add(tokens)
        .filter(|count| *count >= 0)
        .ok_or_else(|| RelationalError::Corruption("negative search token count".into()))?;
    let row = (documents > 0).then(|| {
        RelationalRow::new(vec![
            RelationalValue::Text(String::new()),
            RelationalValue::Bytea(Vec::new()),
            RelationalValue::BigInt(documents),
            RelationalValue::BigInt(tokens),
        ])
    });
    if row.is_none() && (rebuild || old_documents == 0) {
        return Ok(());
    }
    write_output(next, schema, key, row, view_keys, overflow_config, budget)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyzer_lowercases_words_and_folds_full_width_ascii() {
        let analyzer = SearchIndexAnalyzer::CjkNgram;
        assert_eq!(
            analyzer.index_tokens("Fix the HawDB-bm25 index, ＡＢＣ v2!"),
            ["fix", "the", "hawdb", "bm25", "index", "abc", "v2"]
        );
        assert_eq!(analyzer.query_terms("the THE index"), ["index", "the"]);
        assert!(analyzer.index_tokens(" ,.;  ").is_empty());
    }

    #[test]
    fn analyzer_indexes_ideographic_unigrams_and_bigrams() {
        let analyzer = SearchIndexAnalyzer::CjkNgram;
        assert_eq!(
            analyzer.index_tokens("数据库用HawDB"),
            ["数", "数据", "据", "据库", "库", "库用", "用", "hawdb"]
        );
        assert_eq!(analyzer.query_terms("数据库"), ["据库", "数据"]);
        assert_eq!(analyzer.query_terms("库"), ["库"]);
        assert_eq!(
            analyzer.index_tokens("東京タワー 서울"),
            [
                "東", "東京", "京", "京タ", "タ", "タワ", "ワ", "ワー", "ー", "서", "서울", "울"
            ]
        );
    }

    #[test]
    fn analyzer_drops_oversized_tokens_and_keeps_names_stable() {
        let analyzer = SearchIndexAnalyzer::CjkNgram;
        let long = "x".repeat(SEARCH_INDEX_MAX_TOKEN_BYTES + 1);
        assert_eq!(analyzer.index_tokens(&format!("a {long} b")), ["a", "b"]);
        assert_eq!(SearchIndexAnalyzer::parse("CJK_NGRAM"), Some(analyzer));
        assert_eq!(SearchIndexAnalyzer::parse("jieba"), None);
        assert_eq!(
            SearchIndexAnalyzer::from_tag(analyzer.tag()),
            Some(analyzer)
        );
    }
}
