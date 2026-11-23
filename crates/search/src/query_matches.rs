// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Matched query terms and spans of one hit in one analyzer pass per field.
//!
//! A hit's matched terms are the query terms among all of its analyzed tokens
//! (title, content and the tokenized metadata fields). Its spans are the title
//! and content words whose own identifier tokens include a query term: one
//! span per distinct term, in word order and then term order. The analyzer
//! already splits a field into these words and emits each word's identifier
//! tokens together, so both come from the same token stream instead of a
//! whole-document pass plus a second analysis of every word (#1917).

use crate::analyzer_stream::{metadata_token_fields, visit_word_tokens, Control};
use crate::{Result, SearchAnalyzerLexicon, SearchDocument};
use std::collections::BTreeSet;

/// One matched span: `text` is `field[start..end]`, a whole word.
pub(crate) struct Span<'d, 'q> {
    pub(crate) field: &'static str,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) text: &'d str,
    pub(crate) term: &'q str,
}

/// Calls `matched` for every token that is a query term (possibly repeatedly;
/// callers keep the distinct ones) and `span` for every matched span in output
/// order. Nothing is visited for an empty query.
pub(crate) fn visit<'d, 'q>(
    terms: &'q BTreeSet<String>,
    document: &'d SearchDocument,
    analyzer: &SearchAnalyzerLexicon,
    control: Control<'_>,
    mut matched: impl FnMut(&'q str) -> Result<()>,
    mut span: impl FnMut(Span<'d, 'q>) -> Result<()>,
) -> Result<()> {
    if terms.is_empty() {
        return Ok(());
    }
    // The word holding the pending span terms, as byte offsets in its field.
    let mut word = None::<(usize, usize)>;
    let mut word_terms = Vec::<&'q str>::new();
    for (field, text) in [
        ("title", document.title.as_str()),
        ("content", document.content.as_str()),
    ] {
        let base = text.as_ptr() as usize;
        visit_word_tokens(text, analyzer, control, |raw, token, phrase| {
            let Some(term) = terms.get(token) else {
                return Ok(());
            };
            matched(term.as_str())?;
            if phrase {
                return Ok(());
            }
            // Words are subslices of the field, emitted in order.
            let start = raw.as_ptr() as usize - base;
            if word.is_some_and(|(pending, _)| pending != start) {
                flush(field, text, &mut word, &mut word_terms, &mut span)?;
            }
            word = Some((start, start + raw.len()));
            word_terms.push(term.as_str());
            Ok(())
        })?;
        flush(field, text, &mut word, &mut word_terms, &mut span)?;
    }
    for (text, _) in metadata_token_fields(&document.metadata) {
        visit_word_tokens(text, analyzer, control, |_, token, _| {
            terms
                .get(token)
                .map_or(Ok(()), |term| matched(term.as_str()))
        })?;
    }
    Ok(())
}

fn flush<'d, 'q>(
    field: &'static str,
    text: &'d str,
    word: &mut Option<(usize, usize)>,
    terms: &mut Vec<&'q str>,
    span: &mut impl FnMut(Span<'d, 'q>) -> Result<()>,
) -> Result<()> {
    let Some((start, end)) = word.take() else {
        return Ok(());
    };
    terms.sort_unstable();
    terms.dedup();
    for term in terms.drain(..) {
        span(Span {
            field,
            start,
            end,
            text: &text[start..end],
            term,
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
