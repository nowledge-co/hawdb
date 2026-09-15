use super::*;
use crate::analyzer_stream::{visit_admitted_token_list, Control};
use skein_core::RuntimeMemoryReservation;

fn context() -> (RuntimeTaskContext, BuildMemory) {
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(64 * 1024 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    (task, memory)
}

#[test]
fn admitted_events_and_every_rejected_prefix_match_the_legacy_oracle() {
    let (task, memory) = context();
    let control = Control {
        memory: Some(&memory),
        task: Some(&task),
        workspace: None,
    };
    for analyzer in analyzers() {
        for text in [
            "Graph __ Graph_graph HTTPServer42Running WAL studies moving indexed",
            "\u{39f}\u{3a3} \u{39f}\u{3a3}Beta \u{130}Index__V2 CAF\u{c9}",
            "\u{77e5}\u{8b58}Graph\u{691c}\u{7d22} raw evidence raw evidence",
        ] {
            let expected = reference_events(text, &analyzer);
            for stop_after in 0..=expected.len() {
                let mut actual = Vec::new();
                let result =
                    visit_admitted_token_list(text, &analyzer, control, |term, occurrence| {
                        if actual.len() == stop_after {
                            return Err(SkeinError::Execution("consumer stopped".into()));
                        }
                        actual.push((term, occurrence));
                        Ok(())
                    });
                assert_eq!(result.is_ok(), stop_after == expected.len());
                assert_eq!(
                    actual
                        .iter()
                        .map(|(term, occurrence)| (term.as_str().to_owned(), *occurrence))
                        .collect::<Vec<_>>(),
                    expected[..stop_after]
                );
                if !actual.is_empty() {
                    assert!(memory.ledger.snapshot().used_bytes > 0);
                    assert!(actual.iter().all(|(term, _)| term.clone_bytes() == 0));
                }
                drop(actual);
                assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            }
        }
    }
}

#[test]
fn admitted_token_cancellation_and_unwind_release_all_local_state() {
    for unwind in [false, true] {
        let (task, memory) = context();
        let mut retained = None;
        let mut callbacks = 0;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            visit_admitted_token_list(
                "HTTPServer Graph another",
                &SearchAnalyzerLexicon::default(),
                Control {
                    memory: Some(&memory),
                    task: Some(&task),
                    workspace: None,
                },
                |term, _| {
                    callbacks += 1;
                    retained = Some(term);
                    if unwind {
                        panic!("consumer unwind");
                    }
                    task.cancellation().cancel();
                    Ok(())
                },
            )
        }));
        assert_eq!(callbacks, 1);
        if unwind {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_err());
        }
        assert_eq!(retained.as_ref().unwrap().as_str(), "httpserver");
        assert!(memory.ledger.snapshot().used_bytes > 0);
        drop(retained);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn resident_node_denial_preserves_the_previous_frequency_state() {
    let (_, memory) = context();
    let mut analysis =
        DocumentAnalysis::new_with_memory("owned", Default::default(), Some(&memory)).unwrap();
    let term = Term::copy("alpha", Some(&memory)).unwrap();
    let before = memory.ledger.snapshot();
    let mut competing = memory
        .input
        .reserve(before.budget_bytes - before.used_bytes - MAP_ENTRY_BYTES + 1)
        .unwrap();
    let error = analysis
        .push_term(term.clone(), TokenOccurrence::Repeated, 0, 2)
        .unwrap_err();
    assert!(error.to_string().contains("query_memory_bytes"));
    assert_eq!(analysis.document_len, 0);
    assert!(analysis.frequencies.is_empty());
    assert_eq!(analysis.resident_bytes, "owned".len() as u64 + 64);
    competing.shrink(1);
    analysis
        .push_term(term.clone(), TokenOccurrence::Repeated, 0, 2)
        .unwrap();
    analysis
        .push_term(term.clone(), TokenOccurrence::UniqueInField, 0, 1)
        .unwrap();
    analysis
        .push_term(term, TokenOccurrence::Repeated, 0, 1)
        .unwrap();
    assert_eq!(analysis.document_len, 3);
    assert_eq!(analysis.frequencies["alpha"].frequency, 3);
    drop(competing);
    let mut consumer = Vec::new();
    document_frequency::AnalyzedDocument::Resident(analysis)
        .visit(Default::default(), |term, frequency, _| {
            assert!(memory.ledger.snapshot().used_bytes >= MAP_ENTRY_BYTES);
            consumer.push((term, frequency));
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, before.used_bytes);
    assert_eq!(consumer[0].0.as_str(), "alpha");
    drop(consumer);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn resident_frequency_drain_releases_remaining_nodes_on_consumer_error_and_unwind() {
    for unwind in [false, true] {
        let (_, memory) = context();
        let mut analysis =
            DocumentAnalysis::new_with_memory("owned", Default::default(), Some(&memory)).unwrap();
        for text in ["alpha", "beta", "gamma"] {
            let term = Term::copy(text, Some(&memory)).unwrap();
            analysis
                .push_term(term, TokenOccurrence::Repeated, 0, 1)
                .unwrap();
        }
        let mut retained = None;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            document_frequency::AnalyzedDocument::Resident(analysis).visit(
                Default::default(),
                |term, _, _| {
                    retained = Some(term);
                    if unwind {
                        panic!("frequency consumer unwind");
                    }
                    Err(SkeinError::Execution("frequency consumer stopped".into()))
                },
            )
        }));
        if unwind {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_err());
        }
        let bytes = memory.ledger.snapshot().used_bytes;
        assert!(bytes > 0 && bytes < MAP_ENTRY_BYTES);
        assert_eq!(retained.as_ref().unwrap().as_str(), "alpha");
        drop(retained);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn admitted_resident_frequencies_match_seeded_legacy_field_semantics() {
    let (task, memory) = context();
    let control = Control {
        memory: Some(&memory),
        task: Some(&task),
        workspace: None,
    };
    let parts = [
        "Graph",
        "graph_graph",
        "RawEvidence",
        "raw evidence",
        "running",
        "studies",
        "moving",
        "\u{39f}\u{3a3}",
        "\u{130}Index",
        "\u{77e5}\u{8b58}",
        "___",
        "HTTPServer42",
    ];
    for analyzer in analyzers() {
        let mut random = 937u64;
        for case in 0..128 {
            let mut text = String::new();
            for _ in 0..32 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                text.push_str(parts[random as usize % parts.len()]);
                text.push(' ');
            }
            let fixture = document("Graph raw evidence", &text);
            let mut analysis =
                DocumentAnalysis::new_with_memory(&fixture.id, Default::default(), Some(&memory))
                    .unwrap();
            for (field, (text, weight)) in document_token_fields(&fixture).enumerate() {
                visit_admitted_token_list(text, &analyzer, control, |term, occurrence| {
                    analysis.push_term(term, occurrence, field as u8, weight)
                })
                .unwrap();
            }
            let mut expected = BTreeMap::<String, u32>::new();
            for term in reference_document_tokens(&fixture, &analyzer) {
                *expected.entry(term).or_default() += 1;
            }
            assert_eq!(
                analysis.document_len,
                expected.values().sum::<u32>(),
                "case={case}"
            );
            let mut actual = BTreeMap::new();
            document_frequency::AnalyzedDocument::Resident(analysis)
                .visit(Default::default(), |term, frequency, _| {
                    actual.insert(term.as_str().to_owned(), frequency);
                    Ok(())
                })
                .unwrap();
            assert_eq!(actual, expected, "case={case}");
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }
}
