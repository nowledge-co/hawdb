//! Identifier splitting retains output parts, but needs only one lookahead char.

#[cfg(test)]
#[path = "identifier/reference.rs"]
pub(super) mod reference;

#[cfg(test)]
thread_local! {
    pub(super) static SPLIT_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn identifier_parts(raw: &str) -> Vec<String> {
    #[cfg(test)]
    SPLIT_VISITS.with(|visits| visits.set(visits.get() + 1));
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut previous_kind = IdentifierCharKind::Other;
    let mut chars = raw.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '_' {
            push_identifier_part(&mut parts, &mut current);
            previous_kind = IdentifierCharKind::Other;
            continue;
        }
        let kind = IdentifierCharKind::from_char(ch);
        let next_kind = chars.peek().copied().map(IdentifierCharKind::from_char);
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
