//! Executable reduction proof for document-local spill. Kept test-only until
//! the admitted run writer and two-pass posting emission are integrated.

use super::*;

/// Summary for one (term, field), over a disjoint subset of occurrence ordinals.
/// Every ordinary occurrence counts. A unique-in-field occurrence counts only
/// when it is the earliest occurrence of either kind in that field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PartialFieldFrequency {
    repeated_weight: u64,
    first_event: Option<(u64, u64)>,
}

impl PartialFieldFrequency {
    fn push(&mut self, ordinal: u64, occurrence: TokenOccurrence, weight: u64) -> Result<()> {
        let mut incoming = Self::default();
        let unique_weight = match occurrence {
            TokenOccurrence::Repeated => {
                incoming.repeated_weight = weight;
                0
            }
            TokenOccurrence::UniqueInField => weight,
        };
        incoming.first_event = Some((ordinal, unique_weight));
        self.merge(incoming)
    }

    fn merge(&mut self, incoming: Self) -> Result<()> {
        let repeated_weight = self
            .repeated_weight
            .checked_add(incoming.repeated_weight)
            .ok_or_else(|| SkeinError::Storage("document term frequency overflow".into()))?;
        if let Some(first) = incoming.first_event {
            if self.first_event.is_none_or(|current| first.0 < current.0) {
                self.first_event = Some(first);
            }
        }
        self.repeated_weight = repeated_weight;
        Ok(())
    }

    fn frequency(self) -> Result<u64> {
        self.repeated_weight
            .checked_add(
                self.first_event
                    .map_or(0, |(_, unique_weight)| unique_weight),
            )
            .ok_or_else(|| SkeinError::Storage("document term frequency overflow".into()))
    }
}

#[derive(Debug, Clone)]
struct Event {
    term: String,
    occurrence: TokenOccurrence,
    field: u8,
    weight: usize,
}

type Frequencies = BTreeMap<String, u32>;
type PartialRun = BTreeMap<(String, u8), PartialFieldFrequency>;

fn reference(events: &[Event]) -> (Frequencies, u32) {
    let mut analysis = DocumentAnalysis::new("spill-proof", Default::default()).unwrap();
    for event in events {
        analysis
            .push(
                event.term.clone(),
                event.occurrence,
                event.field,
                event.weight,
            )
            .unwrap();
    }
    let document = analysis.finish();
    (document.frequencies, document.document_len)
}

fn partial_runs(events: &[Event], split_after: impl Fn(usize) -> bool) -> Vec<PartialRun> {
    let mut runs = Vec::new();
    let mut current = PartialRun::new();
    for (ordinal, event) in events.iter().enumerate() {
        current
            .entry((event.term.clone(), event.field))
            .or_default()
            .push(ordinal as u64, event.occurrence, event.weight as u64)
            .unwrap();
        if split_after(ordinal) {
            runs.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        runs.push(current);
    }
    runs
}

fn merge_runs(mut left: PartialRun, right: PartialRun) -> PartialRun {
    for (key, summary) in right {
        left.entry(key).or_default().merge(summary).unwrap();
    }
    left
}

fn reduce(mut runs: Vec<PartialRun>, reverse: bool) -> (Frequencies, u32) {
    if reverse {
        runs.reverse();
    }
    // Pairwise merges deliberately differ from a sequential traversal and
    // exercise summaries from several prior merge generations.
    while runs.len() > 1 {
        let mut next = Vec::new();
        let mut inputs = runs.into_iter();
        while let Some(left) = inputs.next() {
            next.push(match inputs.next() {
                Some(right) => merge_runs(left, right),
                None => left,
            });
        }
        runs = next;
    }
    let mut frequencies = Frequencies::new();
    let mut document_len = 0u32;
    for ((term, _field), summary) in runs.pop().unwrap_or_default() {
        let frequency = u32::try_from(summary.frequency().unwrap()).unwrap();
        *frequencies.entry(term).or_default() += frequency;
        document_len += frequency;
    }
    (frequencies, document_len)
}

#[test]
fn independent_chunk_tf_sums_are_not_an_exact_reducer() {
    let events = [
        TokenOccurrence::UniqueInField,
        TokenOccurrence::Repeated,
        TokenOccurrence::UniqueInField,
    ]
    .map(|occurrence| Event {
        term: "graph".into(),
        occurrence,
        field: 0,
        weight: 2,
    });
    let exact = reference(&events);
    let naive_len = events
        .chunks(1)
        .map(|chunk| reference(chunk).1)
        .sum::<u32>();
    assert_eq!(exact.1, 4);
    assert_eq!(naive_len, 6);
    assert_ne!(naive_len, exact.1);
    assert_eq!(reduce(partial_runs(&events, |_| true), true), exact);
}

#[test]
fn all_small_occurrence_partitions_match_the_existing_document_analysis() {
    for length in 0..=5usize {
        for pattern in 0..4usize.pow(length as u32) {
            for field_boundary in 0..=length {
                let events = (0..length)
                    .map(|position| {
                        let symbol = (pattern >> (position * 2)) & 3;
                        Event {
                            term: if symbol & 1 == 0 { "alpha" } else { "beta" }.into(),
                            occurrence: if symbol & 2 == 0 {
                                TokenOccurrence::Repeated
                            } else {
                                TokenOccurrence::UniqueInField
                            },
                            field: u8::from(position >= field_boundary),
                            weight: if position < field_boundary { 2 } else { 1 },
                        }
                    })
                    .collect::<Vec<_>>();
                let expected = reference(&events);
                for partition in 0..(1usize << length.saturating_sub(1)) {
                    let runs = partial_runs(&events, |position| partition & (1 << position) != 0);
                    for reverse in [false, true] {
                        assert_eq!(
                            reduce(runs.clone(), reverse),
                            expected,
                            "pattern={pattern}, boundary={field_boundary}, partition={partition}, reverse={reverse}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn real_analyzer_fields_remain_exact_across_spill_and_merge_boundaries() {
    let analyzer = SearchAnalyzerLexicon::default();
    let fields = [
        "graph graph storage graph_storage graph storage",
        "APIClient api client API_client APIClient",
        "\u{4e2d}\u{6587}\u{6570}\u{636e}\u{5e93} \u{4e2d}\u{6587} GraphRAG",
        "\u{20000}\u{20001}\u{20002} FooBAR foo bar",
    ];
    for title in fields {
        for content in fields {
            let document = SearchDocument {
                id: "spill-proof".into(),
                title: title.into(),
                content: content.into(),
                embedding: None,
                metadata: BTreeMap::from([
                    ("kind".into(), "graph graph storage".into()),
                    ("external_id".into(), "GraphRAG_graph_rag".into()),
                    ("space_id".into(), "graph storage".into()),
                ]),
            };
            let mut events = Vec::new();
            for (field, (text, weight)) in document_token_fields(&document).enumerate() {
                visit_token_list(text, &analyzer, |term, occurrence| {
                    events.push(Event {
                        term,
                        occurrence,
                        field: field as u8,
                        weight,
                    });
                    Ok(())
                })
                .unwrap();
            }
            let expected =
                analyze_delta_document(&document, &analyzer, Default::default()).unwrap();
            for chunk_size in 1..=events.len().max(1) {
                for reverse in [false, true] {
                    let actual = reduce(
                        partial_runs(&events, |position| (position + 1) % chunk_size == 0),
                        reverse,
                    );
                    assert_eq!(
                        actual.0, expected.frequencies,
                        "chunk={chunk_size}, reverse={reverse}"
                    );
                    assert_eq!(actual.1, expected.document_len);
                }
            }
        }
    }
}

#[test]
fn summary_overflow_is_checked_and_rejected_merge_is_atomic() {
    let mut summary = PartialFieldFrequency {
        repeated_weight: u64::MAX,
        first_event: Some((1, 0)),
    };
    let before = summary;
    assert!(summary.push(0, TokenOccurrence::Repeated, 1).is_err());
    assert_eq!(summary, before);
    assert_eq!(summary.frequency().unwrap(), u64::MAX);
    summary.push(0, TokenOccurrence::UniqueInField, 1).unwrap();
    assert!(summary.frequency().is_err());
}
