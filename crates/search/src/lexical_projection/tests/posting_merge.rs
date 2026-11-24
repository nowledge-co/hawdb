// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn multi_term_frontier_matches_independent_term_scores_bit_for_bit() {
    let root = projection_root("multi-term-frontier");
    fs::create_dir_all(&root).unwrap();
    let reader = super::block_ranges::many_block_reader(&root);
    let mut terms = BTreeSet::from(["rare".into(), "storage".into(), "title".into()]);
    terms.extend((0..32).map(|index| format!("unique{index:04}")));
    let mut expected = BTreeMap::<String, f64>::new();
    let mut postings = 0;
    for term in &terms {
        let report = reader
            .score(
                &BTreeSet::from([term.clone()]),
                &LexicalMiniDelta::default(),
                None,
                |id| Ok(!id.ends_with("0007")),
            )
            .unwrap();
        postings += report.postings_visited;
        for (id, score) in report.scores {
            *expected.entry(id).or_default() += score;
        }
    }
    let actual = reader
        .score(&terms, &LexicalMiniDelta::default(), None, |id| {
            Ok(!id.ends_with("0007"))
        })
        .unwrap();
    let bits = |scores: &BTreeMap<String, f64>| {
        scores
            .iter()
            .map(|(id, score)| (id.clone(), score.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(bits(&actual.scores), bits(&expected));
    assert_eq!(actual.matching_document_count, expected.len());
    assert_eq!(actual.postings_visited, postings);
    let mut ordered = expected.into_iter().collect::<Vec<_>>();
    ordered.sort_unstable_by(|(left_id, left), (right_id, right)| {
        right.total_cmp(left).then_with(|| left_id.cmp(right_id))
    });
    let expected = ordered.into_iter().take(10).collect();
    let retained = reader
        .score(&terms, &LexicalMiniDelta::default(), Some(10), |id| {
            Ok(!id.ends_with("0007"))
        })
        .unwrap();
    assert_eq!(bits(&retained.scores), bits(&expected));
    assert_eq!(
        retained.matching_document_count,
        actual.matching_document_count
    );
    fs::remove_dir_all(root).unwrap();
}
