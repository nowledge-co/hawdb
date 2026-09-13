//! Fallible field and identifier traversal. Identifier deduplication and opaque
//! Jieba analysis still retain whole-run state; this is not a bounded-RSS tokenizer.

use super::cjk_tokenizer::{is_cjk_search_char, visit_chinese_search_tokens};
use super::identifier::{normalize_part, part_slices, IdentifierParts};
use super::{
    normalize_english_suffixes, Result, SearchAnalyzerLexicon, SearchDocument, TokenSequence,
    TITLE_TERM_FREQUENCY_WEIGHT,
};
use std::borrow::Cow;
use std::collections::{hash_map::Entry, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TokenOccurrence {
    // Phrase aliases are deduplicated against every preceding token in a field,
    // including ordinary identifier occurrences, not just other phrase aliases.
    UniqueInField,
    Repeated,
}

pub(super) fn document_token_fields(
    document: &SearchDocument,
) -> impl Iterator<Item = (&str, usize)> {
    [
        (document.title.as_str(), TITLE_TERM_FREQUENCY_WEIGHT),
        (document.content.as_str(), 1),
    ]
    .into_iter()
    .chain(
        ["kind", "external_id", "source_id", "space_id"]
            .into_iter()
            .filter_map(|key| document.metadata.get(key).map(|value| (value.as_str(), 1))),
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenScope {
    Phrase(usize),
    Identifier(usize),
}

pub(super) fn visit_token_list(
    text: &str,
    analyzer: &SearchAnalyzerLexicon,
    mut emit: impl FnMut(String, TokenOccurrence) -> Result<()>,
) -> Result<()> {
    let mut current_scope = None;
    let mut seen = HashMap::new();
    visit_token_events(text, analyzer, |token, scope| {
        if current_scope != Some(scope) {
            seen = HashMap::new();
            current_scope = Some(scope);
        }
        if let Entry::Vacant(entry) = seen.entry(token) {
            let token = entry.key().clone().into_owned();
            entry.insert(());
            emit(
                token,
                match scope {
                    TokenScope::Phrase(_) => TokenOccurrence::UniqueInField,
                    TokenScope::Identifier(_) => TokenOccurrence::Repeated,
                },
            )?;
        }
        Ok(())
    })
}

pub(super) fn collect_token_list(text: &str, analyzer: &SearchAnalyzerLexicon) -> Vec<String> {
    let mut tokens = TokenSequence::default();
    {
        // Reuse collected token IDs instead of retaining a second identifier
        // hash table. Release these markers before materializing output order.
        let mut last_identifier = Vec::new();
        visit_token_events(text, analyzer, |token, scope| {
            let (id, inserted) = if let Some(id) = tokens.token_ids.get(token.as_ref()) {
                (*id, false)
            } else {
                let id = tokens.token_ids.len();
                tokens.token_ids.insert(token.into_owned(), id);
                last_identifier.push(0);
                (id, true)
            };
            let retain = match scope {
                TokenScope::Phrase(_) => inserted,
                TokenScope::Identifier(identifier) => {
                    let retain = last_identifier[id] != identifier;
                    last_identifier[id] = identifier;
                    retain
                }
            };
            if retain {
                tokens.order.push(id);
            }
            Ok(())
        })
        .expect("token collection has no fallible admission");
    }
    tokens.into_vec()
}

fn visit_token_events<'a>(
    text: &'a str,
    analyzer: &SearchAnalyzerLexicon,
    mut emit: impl FnMut(Cow<'a, str>, TokenScope) -> Result<()>,
) -> Result<()> {
    let mut previous_part = None::<Cow<'_, str>>;
    let mut identifier = 0;
    for raw in text.split(|ch: char| !ch.is_alphanumeric() && ch != '_') {
        if raw.is_empty() {
            continue;
        }
        identifier += 1;
        #[cfg(test)]
        IDENTIFIER_VISITS.with(|visits| visits.set(visits.get() + 1));
        let parts = part_slices(raw);
        if let Some(previous) = previous_part.as_ref()
            && let Some(first) = parts.clone().next()
        {
            let mut phrase = TokenEmitter::new(analyzer, |token| {
                emit(token, TokenScope::Phrase(identifier))
            });
            phrase.analyzed(Cow::Owned(format!("{previous}_{}", normalized_part(first))))?;
        }
        if let Some(last) = visit_identifier_tokens(raw, parts, analyzer, |token| {
            emit(token, TokenScope::Identifier(identifier))
        })? {
            previous_part = Some(last);
        }
    }
    Ok(())
}

pub(super) fn identifier_tokens(raw: &str, analyzer: &SearchAnalyzerLexicon) -> Vec<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Vec::new();
    }
    let mut tokens = TokenSequence::default();
    visit_identifier_tokens(raw, part_slices(raw), analyzer, |token| {
        tokens.push_unique(token.into_owned());
        Ok(())
    })
    .expect("token collection has no fallible admission");
    tokens.into_vec()
}

fn visit_identifier_tokens<'a>(
    raw: &'a str,
    parts: IdentifierParts<'a>,
    analyzer: &SearchAnalyzerLexicon,
    emit: impl FnMut(Cow<'a, str>) -> Result<()>,
) -> Result<Option<Cow<'a, str>>> {
    let mut tokens = TokenEmitter::new(analyzer, emit);
    tokens.emit_token(if lowercase_is_identity(raw) {
        Cow::Borrowed(raw)
    } else {
        Cow::Owned(raw.to_lowercase())
    })?;
    // Jieba still owns its whole-run scratch and borrowed token collection.
    visit_chinese_search_tokens(raw, |token| tokens.analyzed(Cow::Borrowed(token)))?;
    for run in raw.split(|ch| !is_cjk_search_char(ch)) {
        for width in [2, 3] {
            let mut starts = [0; 3];
            for (index, (offset, ch)) in run.char_indices().enumerate() {
                starts.copy_within(1..width, 0);
                starts[width - 1] = offset;
                if index + 1 >= width {
                    tokens.emit_token(Cow::Borrowed(&run[starts[0]..offset + ch.len_utf8()]))?;
                }
            }
        }
    }
    for part in parts.clone() {
        tokens.analyzed(normalized_part(part))?;
    }
    // Replay boundaries to retain the historical parts-before-pairs order
    // without keeping an owned vector of every part.
    let mut previous = None::<Cow<'_, str>>;
    for part in parts {
        let part = normalized_part(part);
        if let Some(previous) = previous.as_ref() {
            tokens.analyzed(Cow::Owned(format!("{previous}_{part}")))?;
        }
        previous = Some(part);
    }
    Ok(previous)
}

fn lowercase_is_identity(text: &str) -> bool {
    text.chars()
        .all(|ch| ch.to_lowercase().eq(std::iter::once(ch)))
}

fn normalized_part(part: &str) -> Cow<'_, str> {
    if lowercase_is_identity(part) {
        Cow::Borrowed(part)
    } else {
        Cow::Owned(normalize_part(part))
    }
}

struct TokenEmitter<'analyzer, F> {
    analyzer: &'analyzer SearchAnalyzerLexicon,
    emit: F,
}

impl<'text, 'analyzer, F: FnMut(Cow<'text, str>) -> Result<()>> TokenEmitter<'analyzer, F> {
    fn new(analyzer: &'analyzer SearchAnalyzerLexicon, emit: F) -> Self {
        Self { analyzer, emit }
    }

    fn emit_token(&mut self, token: Cow<'text, str>) -> Result<()> {
        if token.is_empty() || self.analyzer.is_stopword(&token) {
            return Ok(());
        }
        (self.emit)(token)
    }

    fn analyzed(&mut self, token: Cow<'text, str>) -> Result<()> {
        self.emit_token(token.clone())?;
        for normalized in normalize_english_suffixes(&token) {
            self.emit_token(Cow::Owned(normalized))?;
        }
        for alias in self.analyzer.semantic_aliases(&token) {
            self.emit_token(Cow::Owned(alias))?;
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    pub(super) static IDENTIFIER_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
