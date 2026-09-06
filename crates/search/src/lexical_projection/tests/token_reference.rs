//! Frozen pre-streaming token oracle from 2b9caa56; do not reuse production recipes.

use crate::cjk_tokenizer::is_cjk_search_char;
use crate::{
    chinese_search_tokens, push_unique_token, trim_doubled_suffix_consonant, IdentifierCharKind,
    SearchAnalyzerLexicon, SearchDocument, TokenSequence, TITLE_TERM_FREQUENCY_WEIGHT,
};

pub(super) fn document_tokens(
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Vec<String> {
    let title_tokens = tokenize_list(&document.title, analyzer_lexicon);
    let mut tokens = Vec::new();
    for _ in 0..TITLE_TERM_FREQUENCY_WEIGHT {
        tokens.extend(title_tokens.iter().cloned());
    }
    tokens.extend(tokenize_list(&document.content, analyzer_lexicon));
    tokens.extend(searchable_metadata_tokens(document, analyzer_lexicon));
    tokens
}

fn searchable_metadata_tokens(
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Vec<String> {
    ["kind", "external_id", "source_id", "space_id"]
        .into_iter()
        .filter_map(|key| document.metadata.get(key))
        .flat_map(|value| tokenize_list(value, analyzer_lexicon))
        .collect()
}

fn tokenize_list(text: &str, analyzer_lexicon: &SearchAnalyzerLexicon) -> Vec<String> {
    let mut tokens = TokenSequence::default();
    let mut previous_part = None::<String>;
    for raw in text.split(|ch: char| !ch.is_alphanumeric() && ch != '_') {
        let parts = identifier_parts(raw);
        if let (Some(previous), Some(first)) = (previous_part.as_ref(), parts.first()) {
            push_analyzed_token(&mut tokens, format!("{previous}_{first}"), analyzer_lexicon);
        }
        tokens.extend(identifier_tokens(raw, analyzer_lexicon));
        if let Some(last) = parts.last() {
            previous_part = Some(last.clone());
        }
    }
    tokens.into_vec()
}

pub(super) fn identifier_tokens(
    raw: &str,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Vec<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Vec::new();
    }
    let mut tokens = TokenSequence::default();
    push_unique_token(&mut tokens, raw.to_lowercase(), analyzer_lexicon);
    for token in chinese_search_tokens(raw) {
        push_analyzed_token(&mut tokens, token, analyzer_lexicon);
    }
    push_cjk_ngram_tokens(&mut tokens, raw, analyzer_lexicon);
    let parts = identifier_parts(raw);
    for part in &parts {
        push_analyzed_token(&mut tokens, part.clone(), analyzer_lexicon);
    }
    for pair in parts.windows(2) {
        push_analyzed_token(&mut tokens, pair.join("_"), analyzer_lexicon);
    }
    tokens.into_vec()
}

fn push_cjk_ngram_tokens(
    tokens: &mut TokenSequence,
    raw: &str,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) {
    let mut run = Vec::new();
    for ch in raw.chars() {
        if is_cjk_search_char(ch) {
            run.push(ch);
        } else {
            push_cjk_ngram_run_tokens(tokens, &run, analyzer_lexicon);
            run.clear();
        }
    }
    push_cjk_ngram_run_tokens(tokens, &run, analyzer_lexicon);
}

fn push_cjk_ngram_run_tokens(
    tokens: &mut TokenSequence,
    run: &[char],
    analyzer_lexicon: &SearchAnalyzerLexicon,
) {
    for width in [2_usize, 3] {
        if run.len() < width {
            continue;
        }
        for window in run.windows(width) {
            push_unique_token(tokens, window.iter().collect(), analyzer_lexicon);
        }
    }
}

fn identifier_parts(raw: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut previous_kind = IdentifierCharKind::Other;
    let chars = raw.chars().collect::<Vec<_>>();
    for (index, ch) in chars.iter().copied().enumerate() {
        if ch == '_' {
            push_identifier_part(&mut parts, &mut current);
            previous_kind = IdentifierCharKind::Other;
            continue;
        }
        let kind = IdentifierCharKind::from_char(ch);
        let next_kind = chars
            .get(index + 1)
            .copied()
            .map(IdentifierCharKind::from_char);
        if !current.is_empty()
            && ((previous_kind == IdentifierCharKind::Lower && kind == IdentifierCharKind::Upper)
                || (previous_kind == IdentifierCharKind::Upper
                    && kind == IdentifierCharKind::Upper
                    && next_kind == Some(IdentifierCharKind::Lower))
                || (previous_kind != IdentifierCharKind::Digit
                    && kind == IdentifierCharKind::Digit)
                || (previous_kind == IdentifierCharKind::Digit
                    && kind != IdentifierCharKind::Digit))
        {
            push_identifier_part(&mut parts, &mut current);
        }
        current.extend(ch.to_lowercase());
        previous_kind = kind;
    }
    push_identifier_part(&mut parts, &mut current);
    parts
}

fn push_identifier_part(parts: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        parts.push(std::mem::take(current));
    }
}

fn push_analyzed_token(
    tokens: &mut TokenSequence,
    token: String,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) {
    push_unique_token(tokens, token.clone(), analyzer_lexicon);
    for normalized in normalize_english_suffixes(&token) {
        push_unique_token(tokens, normalized, analyzer_lexicon);
    }
    for alias in analyzer_lexicon.semantic_aliases(&token) {
        push_unique_token(tokens, alias, analyzer_lexicon);
    }
}

fn normalize_english_suffixes(token: &str) -> Vec<String> {
    if token.len() <= 4 || token.contains('_') || token.chars().any(|ch| ch.is_ascii_digit()) {
        return Vec::new();
    }
    if let Some(stem) = token.strip_suffix("ies")
        && stem.len() >= 2
    {
        return vec![format!("{stem}y")];
    }
    if let Some(stem) = token.strip_suffix("ing")
        && stem.len() >= 3
    {
        return suffix_stem_variants(trim_doubled_suffix_consonant(stem));
    }
    if let Some(stem) = token.strip_suffix("ed")
        && stem.len() >= 3
    {
        return suffix_stem_variants(trim_doubled_suffix_consonant(stem));
    }
    if let Some(stem) = token.strip_suffix('s')
        && stem.len() >= 3
        && !stem.ends_with('s')
    {
        return vec![stem.to_string()];
    }
    Vec::new()
}

fn suffix_stem_variants(stem: &str) -> Vec<String> {
    let mut variants = vec![stem.to_string()];
    if matches!(stem.chars().last(), Some('c' | 'v' | 'z')) {
        variants.push(format!("{stem}e"));
    }
    variants
}
