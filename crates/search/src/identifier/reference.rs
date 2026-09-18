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

//! Frozen pre-optimization splitter, independent of production classification,
//! lookahead, and part assembly. Shared only by semantic and allocation tests.

pub(crate) fn identifier_parts(raw: &str) -> Vec<String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Kind {
        Lower,
        Upper,
        Digit,
        Other,
    }
    fn kind(ch: char) -> Kind {
        if ch.is_ascii_lowercase() {
            Kind::Lower
        } else if ch.is_ascii_uppercase() {
            Kind::Upper
        } else if ch.is_ascii_digit() {
            Kind::Digit
        } else {
            Kind::Other
        }
    }
    fn push(parts: &mut Vec<String>, current: &mut String) {
        if !current.is_empty() {
            parts.push(std::mem::take(current));
        }
    }
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut previous_kind = Kind::Other;
    let chars = raw.chars().collect::<Vec<_>>();
    for (index, ch) in chars.iter().copied().enumerate() {
        if ch == '_' {
            push(&mut parts, &mut current);
            previous_kind = Kind::Other;
            continue;
        }
        let current_kind = kind(ch);
        let next_kind = chars.get(index + 1).copied().map(kind);
        if !current.is_empty()
            && ((previous_kind == Kind::Lower && current_kind == Kind::Upper)
                || (previous_kind == Kind::Upper
                    && current_kind == Kind::Upper
                    && next_kind == Some(Kind::Lower))
                || (previous_kind != Kind::Digit && current_kind == Kind::Digit)
                || (previous_kind == Kind::Digit && current_kind != Kind::Digit))
        {
            push(&mut parts, &mut current);
        }
        current.extend(ch.to_lowercase());
        previous_kind = current_kind;
    }
    push(&mut parts, &mut current);
    parts
}

#[test]
fn identifier_boundary_sequences_match_the_frozen_splitter() {
    for (raw, expected) in [
        ("", vec![]),
        ("___", vec![]),
        ("__HTTPServer42ID__", vec!["http", "server", "42", "id"]),
        ("aBCd", vec!["a", "b", "cd"]),
        ("A1_b2", vec!["a", "1", "b", "2"]),
        ("\u{130}Index", vec!["i\u{307}index"]),
        ("\u{e9}A\u{ff11}b", vec!["\u{e9}a\u{ff11}b"]),
    ] {
        assert_eq!(identifier_parts(raw), expected, "reference: {raw:?}");
        assert_eq!(
            super::identifier_parts(raw),
            expected,
            "production: {raw:?}"
        );
    }

    // Exhaust every four-character boundary combination, including non-ASCII
    // characters (classified as Other) and expanding Unicode lowercase.
    let alphabet = ['a', 'A', 'B', '1', '_', '\u{4e2d}', '\u{130}', '\u{e9}'];
    for length in 0..=4 {
        for mut index in 0..alphabet.len().pow(length) {
            let mut raw = String::new();
            for _ in 0..length {
                raw.push(alphabet[index % alphabet.len()]);
                index /= alphabet.len();
            }
            assert_eq!(
                super::identifier_parts(&raw),
                identifier_parts(&raw),
                "{raw:?}"
            );
        }
    }
}
