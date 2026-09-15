//! Fallible traversal with owned token and identifier-dedup admission.
//! Opaque analysis and downstream spill progress have separate ownership scopes.

use super::cjk_tokenizer::{is_cjk_search_char, visit_chinese_search_tokens_with_workspace};
use super::identifier::{normalize_part, part_slices, IdentifierParts};
use super::{
    Result, SearchAnalyzerLexicon, SearchDocument, TokenSequence, TITLE_TERM_FREQUENCY_WEIGHT,
};
mod control;
mod text;
use crate::build_term::Term;
pub(crate) use control::Control;
use control::Dedup;
use std::collections::HashMap;
use text::Text;

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
    emit: impl FnMut(String, TokenOccurrence) -> Result<()>,
) -> Result<()> {
    visit_token_list_with_workspace(text, analyzer, None, emit)
}

pub(super) fn visit_token_list_with_workspace(
    text: &str,
    analyzer: &SearchAnalyzerLexicon,
    workspace: Option<&crate::analyzer_workspace::Workspace>,
    mut emit: impl FnMut(String, TokenOccurrence) -> Result<()>,
) -> Result<()> {
    visit_admitted_token_list(
        text,
        analyzer,
        Control {
            workspace,
            ..Control::default()
        },
        |term, occurrence| emit(term.into_untracked()?, occurrence),
    )
}

pub(crate) fn visit_admitted_token_list(
    text: &str,
    analyzer: &SearchAnalyzerLexicon,
    control: Control<'_>,
    mut emit: impl FnMut(Term, TokenOccurrence) -> Result<()>,
) -> Result<()> {
    let mut current_scope = None;
    let mut seen = Dedup::new();
    visit_token_events(text, analyzer, control, |token, scope| {
        if current_scope != Some(scope) {
            seen = Dedup::new();
            current_scope = Some(scope);
        }
        if let Some(token) = seen.insert(token, control)? {
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
        visit_token_events(text, analyzer, Control::default(), |token, scope| {
            let (id, inserted) = if let Some(id) = tokens.token_ids.get(token.as_str()) {
                (*id, false)
            } else {
                let id = tokens.token_ids.len();
                tokens.token_ids.insert(token.into_untracked()?, id);
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
    analyzer: &'a SearchAnalyzerLexicon,
    control: Control<'_>,
    mut emit: impl FnMut(Text<'a>, TokenScope) -> Result<()>,
) -> Result<()> {
    let mut previous_part = None::<Text<'_>>;
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
            let mut phrase = TokenEmitter::new(analyzer, control, |token| {
                emit(token, TokenScope::Phrase(identifier))
            });
            let first = Text::lowercase(first, true, control)?;
            phrase.analyzed(Text::join(previous.as_str(), "_", first.as_str(), control)?)?;
        }
        if let Some(last) = visit_identifier_tokens(raw, parts, analyzer, control, |token| {
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
    visit_identifier_tokens(
        raw,
        part_slices(raw),
        analyzer,
        Control::default(),
        |token| {
            tokens.push_unique(token.into_untracked()?);
            Ok(())
        },
    )
    .expect("token collection has no fallible admission");
    tokens.into_vec()
}

fn visit_identifier_tokens<'a>(
    raw: &'a str,
    parts: IdentifierParts<'a>,
    analyzer: &'a SearchAnalyzerLexicon,
    control: Control<'_>,
    emit: impl FnMut(Text<'a>) -> Result<()>,
) -> Result<Option<Text<'a>>> {
    let mut tokens = TokenEmitter::new(analyzer, control, emit);
    tokens.emit_token(Text::lowercase(raw, false, control)?)?;
    // Jieba still owns its whole-run scratch and borrowed token collection.
    visit_chinese_search_tokens_with_workspace(raw, control.workspace, |token| {
        tokens.analyzed(Text::Borrowed(token))
    })?;
    for run in raw.split(|ch| !is_cjk_search_char(ch)) {
        for width in [2, 3] {
            let mut starts = [0; 3];
            for (index, (offset, ch)) in run.char_indices().enumerate() {
                starts.copy_within(1..width, 0);
                starts[width - 1] = offset;
                if index + 1 >= width {
                    tokens.emit_token(Text::Borrowed(&run[starts[0]..offset + ch.len_utf8()]))?;
                }
            }
        }
    }
    for part in parts.clone() {
        tokens.analyzed(Text::lowercase(part, true, control)?)?;
    }
    // Replay boundaries to retain the historical parts-before-pairs order
    // without keeping an owned vector of every part.
    let mut previous = None::<Text<'_>>;
    for part in parts {
        let part = Text::lowercase(part, true, control)?;
        if let Some(previous) = previous.as_ref() {
            tokens.analyzed(Text::join(previous.as_str(), "_", part.as_str(), control)?)?;
        }
        previous = Some(part);
    }
    Ok(previous)
}

fn lowercase_is_identity(text: &str) -> bool {
    text.chars()
        .all(|ch| ch.to_lowercase().eq(std::iter::once(ch)))
}

struct TokenEmitter<'analyzer, 'control, F> {
    analyzer: &'analyzer SearchAnalyzerLexicon,
    control: Control<'control>,
    emit: F,
}

impl<'text, 'control, F: FnMut(Text<'text>) -> Result<()>> TokenEmitter<'text, 'control, F> {
    fn new(analyzer: &'text SearchAnalyzerLexicon, control: Control<'control>, emit: F) -> Self {
        TokenEmitter {
            analyzer,
            control,
            emit,
        }
    }

    fn emit_token(&mut self, token: Text<'text>) -> Result<()> {
        self.control.check()?;
        if token.as_str().is_empty() || self.analyzer.is_stopword(token.as_str()) {
            return Ok(());
        }
        (self.emit)(token)
    }

    fn analyzed(&mut self, token: Text<'text>) -> Result<()> {
        self.emit_token(token.clone())?;
        for (stem, tail) in crate::english_suffix_parts(token.as_str())
            .into_iter()
            .flatten()
        {
            self.emit_token(token.suffix(stem.len(), tail, self.control)?)?;
        }
        for alias in self.analyzer.semantic_alias_slices(token.as_str()) {
            self.emit_token(Text::Borrowed(alias))?;
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    pub(super) static IDENTIFIER_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
