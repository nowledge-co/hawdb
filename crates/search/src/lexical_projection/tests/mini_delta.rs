use super::*;

pub(super) fn identities(terms: &BTreeMap<String, u32>) -> Vec<(usize, usize, usize)> {
    terms
        .keys()
        .map(|key| {
            (
                key as *const String as usize,
                key.as_ptr() as usize,
                key.capacity(),
            )
        })
        .collect()
}

fn base(delta: &LexicalMiniDelta, id: &str) -> Vec<(usize, usize, usize)> {
    identities(
        &delta
            .upserts
            .get(id)
            .and_then(|document| document.base.as_ref())
            .or_else(|| delta.deletes.get(id))
            .unwrap()
            .terms,
    )
}

fn id_pointer(delta: &LexicalMiniDelta, id: &str) -> usize {
    delta
        .upserts
        .get_key_value(id)
        .map(|(id, _)| id)
        .or_else(|| delta.deletes.get_key_value(id).map(|(id, _)| id))
        .unwrap()
        .as_ptr() as usize
}

#[test]
fn base_terms_reuse_analyzed_tree_nodes_and_strings() {
    let analyzed = analyze_delta_document(
        &document("memory:base", "graph storage", "base memory"),
        &SearchAnalyzerLexicon::default(),
        LexicalProjectionConfig::default(),
    )
    .unwrap();
    let before = identities(&analyzed.frequencies);
    assert!(before.len() >= 4);
    let base = BaseDocumentTerms::from_analyzed(analyzed);
    assert_eq!(identities(&base.terms), before);
}

#[test]
fn mini_delta_moves_base_and_id_through_replace_delete_and_resurrection() {
    let analyzer = SearchAnalyzerLexicon::default();
    let config = LexicalProjectionConfig::default();
    let original = document("memory:base", "graph graph", "storage memory");
    let first = document(&original.id, "vector", "query");
    let next = document(&original.id, "cache", "index");
    let mut delta = LexicalMiniDelta::default();
    delta
        .upsert(&first, Some(&original), &analyzer, config)
        .unwrap();
    let identity = base(&delta, &original.id);
    let id = id_pointer(&delta, &original.id);
    assert!(!identity.is_empty());
    for step in 0..32 {
        match step % 4 {
            0 | 3 => delta
                .upsert(&next, Some(&first), &analyzer, config)
                .unwrap(),
            _ => assert!(delta
                .delete(&original.id, Some(&next), &analyzer, config)
                .unwrap()),
        }
        assert_eq!(base(&delta, &original.id), identity, "base at step {step}");
        assert_eq!(id_pointer(&delta, &original.id), id, "id at step {step}");
        assert_eq!(delta.projected_document_frequency("graph", 1), 0);
        assert_eq!(
            delta
                .projected_corpus(
                    1,
                    u64::from(
                        delta
                            .upserts
                            .get(&original.id)
                            .and_then(|d| d.base.as_ref())
                            .or_else(|| delta.deletes.get(&original.id))
                            .unwrap()
                            .document_len
                    )
                )
                .0,
            usize::from(delta.upserts.contains_key(&original.id))
        );
    }
}

#[test]
fn failed_delta_mutations_preserve_base_identity_and_statistics() {
    let analyzer = SearchAnalyzerLexicon::default();
    let config = LexicalProjectionConfig::default();
    let original = document("memory:base", "graph", "memory");
    let update = document(&original.id, "cache", "index");
    let mut delta = LexicalMiniDelta::default();
    delta
        .upsert(&update, Some(&original), &analyzer, config)
        .unwrap();
    let before = format!("{delta:?}");
    let identity = base(&delta, &original.id);
    let exact = LexicalProjectionConfig {
        mini_delta_bytes: NonZeroU64::new(delta.resident_bytes).unwrap(),
        ..config
    };
    for options in [
        exact,
        LexicalProjectionConfig {
            max_document_source_bytes: NonZeroU64::MIN,
            ..config
        },
    ] {
        assert!(delta
            .upsert(
                &document(&original.id, "a b c d e f g", "extra terms"),
                Some(&update),
                &analyzer,
                options
            )
            .is_err());
        assert_eq!(format!("{delta:?}"), before);
        assert_eq!(base(&delta, &original.id), identity);
    }
    assert!(!delta
        .delete(
            &original.id,
            Some(&update),
            &analyzer,
            LexicalProjectionConfig {
                mini_delta_bytes: NonZeroU64::MIN,
                ..config
            }
        )
        .unwrap());
    assert_eq!(format!("{delta:?}"), before);
    assert_eq!(base(&delta, &original.id), identity);
    delta
        .upsert(&update, Some(&update), &analyzer, exact)
        .unwrap();
    assert_eq!(base(&delta, &original.id), identity);
    assert_eq!(delta.projected_document_frequency("graph", 1), 0);
}

#[test]
fn new_document_replacement_and_delete_never_invent_base_terms() {
    let analyzer = SearchAnalyzerLexicon::default();
    let config = LexicalProjectionConfig::default();
    let first = document("new", "graph", "memory");
    let update = document("new", "vector", "index");
    let mut delta = LexicalMiniDelta::default();
    delta.upsert(&first, None, &analyzer, config).unwrap();
    delta
        .upsert(&update, Some(&first), &analyzer, config)
        .unwrap();
    assert!(delta.upserts["new"].base.is_none());
    assert_eq!(delta.projected_document_frequency("graph", 0), 0);
    assert!(delta
        .delete("new", Some(&update), &analyzer, config)
        .unwrap());
    assert!(delta.upserts.is_empty() && delta.deletes.is_empty());
    assert_eq!(delta.resident_bytes, 0);
    assert_eq!(delta.projected_corpus(0, 0), (0, 0));
}
