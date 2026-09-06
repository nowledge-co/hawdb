//! Borrowed token recipes shared by list and admitted streaming analyzers.

use crate::cjk_tokenizer::is_cjk_search_char;
use crate::{trim_doubled_suffix_consonant, IdentifierCharKind};
use std::iter::Peekable;
use std::str::CharIndices;

pub(super) struct IdentifierParts<'a> {
    raw: &'a str,
    chars: Peekable<CharIndices<'a>>,
    start: usize,
    previous: IdentifierCharKind,
}

impl<'a> IdentifierParts<'a> {
    pub(super) fn new(raw: &'a str) -> Self {
        Self {
            raw,
            chars: raw.char_indices().peekable(),
            start: 0,
            previous: IdentifierCharKind::Other,
        }
    }
}

impl<'a> Iterator for IdentifierParts<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((index, ch)) = self.chars.next() {
            if ch == '_' {
                let start = std::mem::replace(&mut self.start, index + 1);
                self.previous = IdentifierCharKind::Other;
                if start < index {
                    return Some(&self.raw[start..index]);
                }
                continue;
            }
            let kind = IdentifierCharKind::from_char(ch);
            let next = self
                .chars
                .peek()
                .map(|(_, ch)| IdentifierCharKind::from_char(*ch));
            let boundary = index > self.start
                && ((self.previous == IdentifierCharKind::Lower
                    && kind == IdentifierCharKind::Upper)
                    || (self.previous == IdentifierCharKind::Upper
                        && kind == IdentifierCharKind::Upper
                        && next == Some(IdentifierCharKind::Lower))
                    || (self.previous != IdentifierCharKind::Digit
                        && kind == IdentifierCharKind::Digit)
                    || (self.previous == IdentifierCharKind::Digit
                        && kind != IdentifierCharKind::Digit));
            self.previous = kind;
            if boundary {
                let start = std::mem::replace(&mut self.start, index);
                return Some(&self.raw[start..index]);
            }
        }
        let start = std::mem::replace(&mut self.start, self.raw.len());
        (start < self.raw.len()).then(|| &self.raw[start..])
    }
}

pub(super) fn cjk_ngrams(raw: &str) -> impl Iterator<Item = &str> {
    raw.split(|ch| !is_cjk_search_char(ch)).flat_map(|run| {
        [2, 3].into_iter().flat_map(move |width| {
            run.char_indices().filter_map(move |(start, _)| {
                let (last, ch) = run[start..].char_indices().nth(width - 1)?;
                Some(&run[start..start + last + ch.len_utf8()])
            })
        })
    })
}

pub(super) fn suffix_variants(token: &str) -> impl Iterator<Item = (&str, &str)> {
    let variants =
        if token.len() <= 4 || token.contains('_') || token.chars().any(|ch| ch.is_ascii_digit()) {
            [None, None]
        } else if let Some(stem) = token.strip_suffix("ies")
            && stem.len() >= 2
        {
            [Some((stem, "y")), None]
        } else if let Some(stem) = token
            .strip_suffix("ing")
            .or_else(|| token.strip_suffix("ed"))
            && stem.len() >= 3
        {
            let stem = trim_doubled_suffix_consonant(stem);
            [
                Some((stem, "")),
                matches!(stem.chars().last(), Some('c' | 'v' | 'z')).then_some((stem, "e")),
            ]
        } else if let Some(stem) = token.strip_suffix('s')
            && stem.len() >= 3
            && !stem.ends_with('s')
        {
            [Some((stem, "")), None]
        } else {
            [None, None]
        };
    variants.into_iter().flatten()
}
