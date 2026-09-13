//! Borrowed identifier boundaries with one lookahead character and replayable cursors.

#[cfg(test)]
#[path = "identifier/reference.rs"]
pub(super) mod reference;

#[cfg(test)]
thread_local! {
    pub(super) static SPLIT_CURSORS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn identifier_parts(raw: &str) -> Vec<String> {
    part_slices(raw).map(normalize_part).collect()
}

pub(super) fn normalize_part(part: &str) -> String {
    part.chars().flat_map(char::to_lowercase).collect()
}

pub(super) fn part_slices(raw: &str) -> IdentifierParts<'_> {
    #[cfg(test)]
    SPLIT_CURSORS.with(|visits| visits.set(visits.get() + 1));
    IdentifierParts {
        raw,
        chars: raw.char_indices().peekable(),
        start: 0,
        previous_kind: IdentifierCharKind::Other,
    }
}

#[derive(Clone)]
pub(super) struct IdentifierParts<'a> {
    raw: &'a str,
    chars: std::iter::Peekable<std::str::CharIndices<'a>>,
    start: usize,
    previous_kind: IdentifierCharKind,
}

impl<'a> Iterator for IdentifierParts<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((offset, ch)) = self.chars.next() {
            if ch == '_' {
                let start = self.start;
                self.start = offset + 1;
                self.previous_kind = IdentifierCharKind::Other;
                if start != offset {
                    return Some(&self.raw[start..offset]);
                }
                continue;
            }
            let kind = IdentifierCharKind::from_char(ch);
            let next_kind = self
                .chars
                .peek()
                .map(|(_, ch)| IdentifierCharKind::from_char(*ch));
            let boundary = offset != self.start
                && ((self.previous_kind == IdentifierCharKind::Lower
                    && kind == IdentifierCharKind::Upper)
                    || (self.previous_kind == IdentifierCharKind::Upper
                        && kind == IdentifierCharKind::Upper
                        && next_kind == Some(IdentifierCharKind::Lower))
                    || (self.previous_kind != IdentifierCharKind::Digit
                        && kind == IdentifierCharKind::Digit)
                    || (self.previous_kind == IdentifierCharKind::Digit
                        && kind != IdentifierCharKind::Digit));
            self.previous_kind = kind;
            if boundary {
                let start = self.start;
                self.start = offset;
                return Some(&self.raw[start..offset]);
            }
        }
        if self.start == self.raw.len() {
            None
        } else {
            let part = &self.raw[self.start..];
            self.start = self.raw.len();
            Some(part)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentifierCharKind {
    Lower,
    Upper,
    Digit,
    Other,
}

impl IdentifierCharKind {
    fn from_char(ch: char) -> Self {
        if ch.is_ascii_lowercase() {
            Self::Lower
        } else if ch.is_ascii_uppercase() {
            Self::Upper
        } else if ch.is_ascii_digit() {
            Self::Digit
        } else {
            Self::Other
        }
    }
}
