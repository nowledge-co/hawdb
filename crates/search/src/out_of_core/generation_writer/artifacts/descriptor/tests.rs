use super::*;

fn document(id: usize, values: &[(&str, &str)]) -> SearchDocument {
    SearchDocument {
        id: format!("document-{id:05}"),
        title: String::new(),
        content: String::new(),
        embedding: None,
        metadata: values
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect(),
    }
}

fn check(documents: &[SearchDocument], fields: &BTreeSet<String>) {
    let references = documents.iter().collect::<Vec<_>>();
    let expected = SearchSegmentDescriptorEntry::from_documents(7, &references, fields);
    let expected_bytes = super::super::descriptor_working_bytes(&expected) + 96;
    for retained in [0, 731] {
        let limit = retained + expected_bytes;
        let (actual, bytes) = build(7, documents, fields, retained, limit).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(bytes, limit);
        assert!(build(7, documents, fields, retained, limit - 1)
            .unwrap_err()
            .to_string()
            .contains("descriptor working set"));
    }
}

#[test]
fn descriptor_builder_preserves_legacy_fields_ranges_and_exact_budget() {
    let fields: BTreeSet<String> = BTreeSet::from([
        crate::SEARCH_DOCUMENT_ID_FIELD.into(),
        "space_id".into(),
        "kind".into(),
        "labels".into(),
        "metadata.notes".into(),
        "number".into(),
        "created_at".into(),
        "missing".into(),
        "empty".into(),
    ]);
    check(&[], &fields);
    check(&[document(0, &[])], &fields);
    check(
        &[
            document(
                0,
                &[
                    ("kind", "SourceChunk"),
                    ("labels", "[\"Memory\",\"memory\",\"  \u{4e2d}  \",\"\"]"),
                    (
                        "metadata.notes",
                        "[\"\u{039f}\u{03a3}\",\"\u{0130}\",\"12.5\"]",
                    ),
                    ("number", " -0.0 "),
                    ("created_at", "2026-09-14T12:30:00Z"),
                    ("empty", ""),
                ],
            ),
            document(
                1,
                &[
                    ("kind", "chunk"),
                    ("space_id", ""),
                    ("labels", " memory, Memory, ,tag,tag "),
                    ("metadata.notes", "[\"partial\",1]"),
                    ("number", " 1.7976931348623157e308 "),
                    ("created_at", "2021-01-01"),
                ],
            ),
            document(2, &[("space_id", "Custom"), ("number", "NaN")]),
        ],
        &fields,
    );
}

#[test]
fn descriptor_values_preserve_json_fallback_and_presence() {
    let cases = [
        "",
        " ",
        ", ,",
        "[]",
        "[\"\"]",
        "[\" \u{2003} \" ]",
        "[\" a \", \"a\", \" b \" ]",
        "[\"a,b\",\"c\"]",
        "[\"\\uD83E\\uDD80\",\"a\\nb\",\"a\\tb\"]",
        "[\"good\",1]",
        "[\"good\",null]",
        "[\"good\",{}]",
        "[\"good\",[]]",
        "[\"good\",true]",
        "[\"good\",]",
        "[\"good\"] trailing",
        "[\"good\"] []",
        "[\"\\uD800\"]",
        "[\"\\uDD00\"]",
        "[\"\\q\"]",
        "[\"unterminated]",
        "[\"good\"",
        "null",
        "1",
        "{}",
        "\"single\"",
        "a, b ,,, c",
    ];
    for field in ["labels", "metadata.notes", "kind", "note", "space_id"] {
        for raw in cases {
            let source = document(0, &[(field, raw)]);
            let expected = crate::search_document_field_values(&source, field)
                .into_iter()
                .map(Cow::into_owned)
                .collect::<Vec<_>>();
            let mut actual = Vec::new();
            values::visit(&source, field, &mut |value| {
                actual.push(value.to_owned());
                Ok(())
            })
            .unwrap();
            assert_eq!(actual, expected, "field={field}, raw={raw:?}");
            check(&[source], &BTreeSet::from([field.into()]));
        }
    }
}

#[test]
fn descriptor_value_failure_does_not_select_csv_fallback_or_visit_tail() {
    for raw in ["[\"first\",\"second\"]", "first,second", "[\"first\",1]"] {
        let source = document(0, &[("labels", raw)]);
        let mut calls = 0;
        let error = values::visit(&source, "labels", &mut |_| {
            calls += 1;
            Err(SkeinError::Storage(
                "injected descriptor budget failure".into(),
            ))
        })
        .unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(
            error.to_string(),
            SkeinError::Storage("injected descriptor budget failure".into()).to_string()
        );
    }
}

#[test]
fn descriptor_normalization_preserves_unicode_context_and_enum_aliases() {
    let cases = [
        " SourceChunk ",
        "SourceChunk",
        "chunk",
        " Memory ",
        "\u{039f}\u{03a3}",
        "\u{039f}\u{03a3}\u{0301}",
        "\u{039f}\u{03a3}A",
        "\u{03a3}",
        "\u{0130}",
        "\u{212a}",
        "\u{1e9e}",
        "\u{01c5}",
        "\u{2003}x\u{2003}",
    ];
    let mut strings = cases
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    for scalar in (0..=0x10ffff).step_by(257) {
        if let Some(character) = char::from_u32(scalar) {
            strings.push(format!(" \u{039f}{character}\u{03a3}\u{0301} "));
        }
    }
    for field in ["kind", "labels", "metadata.notes", "note", "review_status"] {
        for value in &strings {
            assert_eq!(
                summary_value(field, value),
                crate::search_segment_summary_value(field, value)
            );
        }
    }
}

#[test]
fn seeded_descriptor_builders_match_unique_counts_and_range_summaries() {
    let fields: BTreeSet<String> = BTreeSet::from([
        "labels".into(),
        "metadata.notes".into(),
        "kind".into(),
        "rating".into(),
        "created_at".into(),
        "space_id".into(),
    ]);
    let choices = [
        "",
        "0",
        "-1.25",
        "1e9",
        "NaN",
        "\u{039f}\u{03a3}",
        "\u{0130}",
        " Memory ",
        "2026-09-14",
        "[\"a\",\"A\",\"\"]",
        "[\"x\",1]",
        "a,a,b,,",
        "[\"\\u0061\",\"a,b\"]",
    ];
    let mut state = 0x392_d1c7_u64;
    for case in 0..256 {
        let mut documents = Vec::new();
        for index in 0..case % 9 + 1 {
            let mut source = document(index, &[]);
            for field in &fields {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                if !state.is_multiple_of(5) {
                    source.metadata.insert(
                        field.clone(),
                        choices[(state >> 32) as usize % choices.len()].into(),
                    );
                }
            }
            documents.push(source);
        }
        check(&documents, &fields);
    }
}

#[test]
fn descriptor_budget_stops_before_appending_any_segment_payload() {
    let root = std::env::temp_dir().join(format!(
        "skein-descriptor-builder-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let source = document(0, &[("labels", "[\"a\",\"a\",\"b\"]")]);
    let fields: BTreeSet<String> = BTreeSet::from(["labels".into()]);
    let (_, limit) = build(0, std::slice::from_ref(&source), &fields, 0, u64::MAX).unwrap();
    let options = SearchOutOfCoreGenerationBuildOptions {
        max_descriptor_working_bytes: std::num::NonZeroU64::new(limit).unwrap(),
        ..Default::default()
    };
    let mut builder = SegmentArtifactBuilder::new(&root, 1, &fields, &options).unwrap();
    builder.push(source.clone()).unwrap();
    builder.flush_segment().unwrap();
    let before = [
        builder.document_file.metadata().unwrap().len(),
        builder.metadata_file.metadata().unwrap().len(),
        builder.vector_file.metadata().unwrap().len(),
    ];
    assert!(before.iter().all(|bytes| *bytes > 0));
    builder.push(source).unwrap();
    assert!(builder
        .flush_segment()
        .unwrap_err()
        .to_string()
        .contains("descriptor working set"));
    assert_eq!(
        [
            builder.document_file.metadata().unwrap().len(),
            builder.metadata_file.metadata().unwrap().len(),
            builder.vector_file.metadata().unwrap().len()
        ],
        before
    );
    assert_eq!(builder.descriptor_working_bytes, limit);
    assert_eq!(builder.descriptor.segments.len(), 1);
}
