//! Incremental field traversal. Identifier/Jieba analysis remains a whole-run
//! working unit; this is not an arbitrary byte-chunk or bounded-RSS tokenizer.

use super::{
    identifier_parts, identifier_tokens_with_parts, push_analyzed_token, Result,
    SearchAnalyzerLexicon, SearchDocument, TokenSequence, TITLE_TERM_FREQUENCY_WEIGHT,
};

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

pub(super) fn visit_token_list(
    text: &str,
    analyzer: &SearchAnalyzerLexicon,
    mut emit: impl FnMut(String, TokenOccurrence) -> Result<()>,
) -> Result<()> {
    let mut previous_part = None::<String>;
    for raw in text.split(|ch: char| !ch.is_alphanumeric() && ch != '_') {
        if raw.is_empty() {
            continue;
        }
        #[cfg(test)]
        IDENTIFIER_VISITS.with(|visits| visits.set(visits.get() + 1));
        let parts = identifier_parts(raw);
        if let (Some(previous), Some(first)) = (previous_part.as_ref(), parts.first()) {
            let mut phrase = TokenSequence::default();
            push_analyzed_token(&mut phrase, format!("{previous}_{first}"), analyzer);
            for token in phrase.into_vec() {
                emit(token, TokenOccurrence::UniqueInField)?;
            }
        }
        for token in identifier_tokens_with_parts(raw, Some(&parts), analyzer) {
            emit(token, TokenOccurrence::Repeated)?;
        }
        if let Some(last) = parts.last() {
            previous_part = Some(last.clone());
        }
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    pub(super) static IDENTIFIER_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
