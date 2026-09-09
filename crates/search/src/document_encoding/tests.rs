use super::*;
use std::collections::BTreeMap;

// Keep the previous encoder as an independent wire-format oracle.
fn legacy_encode(document: &SearchDocument) -> String {
    fn hex(value: &str) -> String {
        value.bytes().map(|byte| format!("{byte:02x}")).collect()
    }
    let embedding = document
        .embedding
        .as_ref()
        .map(|values| {
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let metadata = document
        .metadata
        .iter()
        .map(|(key, value)| format!("{}={}", hex(key), hex(value)))
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "doc\t{}\t{}\t{}\t{}\t{}\n",
        hex(&document.id),
        hex(&document.title),
        hex(&document.content),
        embedding,
        metadata,
    )
}

fn check_encoding(document: &SearchDocument) {
    let expected = legacy_encode(document);
    let encoding = DocumentEncoding::new(document).unwrap();
    assert_eq!(encoding.len(), expected.len());
    assert_eq!(encoding.encode().unwrap(), expected);
    assert_eq!(encode_search_document_line(document), expected);
}

#[test]
fn wire_encoding_preserves_empty_fields_unicode_separators_and_float_edges() {
    let mut document = SearchDocument {
        id: String::new(),
        title: String::new(),
        content: String::new(),
        embedding: None,
        metadata: BTreeMap::new(),
    };
    check_encoding(&document);
    document.embedding = Some(Vec::new());
    check_encoding(&document);
    document.id = "\0\t\n;=\u{4e2d}\u{1f980}".to_string();
    document.title = "cafe\u{301}".to_string();
    document.content = "\r\n\u{fffd}\u{10ffff}".to_string();
    document.metadata = BTreeMap::from([
        (String::new(), String::new()),
        (";=\t\0".to_string(), document.id.clone()),
    ]);
    document.embedding = Some(vec![
        0.0,
        -0.0,
        1.0,
        -1.0,
        f32::MIN,
        f32::MAX,
        f32::MIN_POSITIVE,
        f32::from_bits(1),
        f32::INFINITY,
        f32::NEG_INFINITY,
        f32::NAN,
    ]);
    check_encoding(&document);
}

#[test]
fn seeded_wire_encoding_matches_legacy_reference() {
    fn next(state: &mut u64) -> u32 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        (*state >> 32) as u32
    }
    fn string(state: &mut u64) -> String {
        const CHARS: &[char] = &[
            'a',
            'Z',
            '0',
            '\0',
            '\t',
            '\n',
            ';',
            '=',
            '\u{e9}',
            '\u{4e2d}',
            '\u{1f980}',
        ];
        (0..next(state) % 80)
            .map(|_| CHARS[next(state) as usize % CHARS.len()])
            .collect()
    }
    let mut state = 0x392_c0de;
    for case in 0..1024 {
        let document = SearchDocument {
            id: string(&mut state),
            title: string(&mut state),
            content: string(&mut state),
            embedding: (case % 3 != 0).then(|| {
                (0..next(&mut state) % 64)
                    .map(|_| f32::from_bits(next(&mut state)))
                    .collect()
            }),
            metadata: (0..next(&mut state) % 12)
                .map(|_| (string(&mut state), string(&mut state)))
                .collect(),
        };
        check_encoding(&document);
    }
}

#[test]
fn size_counter_rejects_overflow_without_wrapping() {
    let mut length = EncodedLength(usize::MAX);
    assert!(length.write_str("x").is_err());
    assert_eq!(length.0, usize::MAX);
    let mut length = EncodedLength(usize::MAX - 1);
    assert!(length.write_hex("x").is_err());
    assert_eq!(length.0, usize::MAX - 1);
}
