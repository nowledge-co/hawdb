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

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-term-policy-internal-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn config() -> LexicalProjectionConfig {
    LexicalProjectionConfig {
        max_term_bytes: NonZeroU64::new(5202).unwrap(),
        ..Default::default()
    }
}

fn document() -> SearchDocument {
    SearchDocument {
        id: "d".into(),
        title: String::new(),
        content: "x".repeat(5202),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}

fn build(root: &Path, config: LexicalProjectionConfig) -> Result<Arc<LexicalProjectionReader>> {
    LexicalProjectionWriter::new(config).write(
        root,
        1,
        None,
        11,
        13,
        [&document()].into_iter(),
        &Default::default(),
    )
}

#[test]
fn term_policy_default_query_count_boundary_remains_usable() {
    let fixture = Fixture::new();
    let source = SearchDocument {
        content: "short".into(),
        ..document()
    };
    let reader = LexicalProjectionWriter::new(Default::default())
        .write(
            &fixture.0,
            1,
            None,
            11,
            13,
            [&source].into_iter(),
            &Default::default(),
        )
        .unwrap();
    let mut query = (0..31)
        .map(|index| format!("query{index:02}"))
        .collect::<BTreeSet<_>>();
    query.insert("short".into());
    assert_eq!(query.len(), reader.config.max_query_terms.get());
    assert_eq!(
        reader
            .score(&query, &Default::default(), None, |_| Ok(true))
            .unwrap()
            .scores
            .len(),
        1
    );
    query.insert("overflow".into());
    assert!(reader
        .score(&query, &Default::default(), None, |_| Ok(true))
        .unwrap_err()
        .to_string()
        .contains("33 terms"));
}

#[test]
fn term_policy_query_and_delta_exact_memory_boundaries() {
    let fixture = Fixture::new();
    let mut reader = build(&fixture.0, config()).unwrap();
    let query = BTreeSet::from([document().content]);
    let blocks = reader
        .manifest
        .blocks
        .iter()
        .filter(|block| block.kind == BlockKind::Postings)
        .collect::<Vec<_>>();
    let query_bytes = blocks.iter().map(|block| block.length).max().unwrap() * 4
        + blocks
            .iter()
            .map(|block| u64::from(block.entry_count))
            .max()
            .unwrap()
            * 4
            * std::mem::size_of::<Posting>() as u64
        + blocks.len() as u64 * 2 * std::mem::size_of::<&BlockDescriptor>() as u64
        + 5202 * 3
        + 32;
    Arc::get_mut(&mut reader).unwrap().config.query_memory_bytes =
        NonZeroU64::new(query_bytes).unwrap();
    assert_eq!(
        reader
            .score(&query, &Default::default(), None, |_| Ok(true))
            .unwrap()
            .scores
            .len(),
        1
    );
    Arc::get_mut(&mut reader).unwrap().config.query_memory_bytes =
        NonZeroU64::new(query_bytes - 1).unwrap();
    assert!(reader
        .score(&query, &Default::default(), None, |_| Ok(true))
        .unwrap_err()
        .to_string()
        .contains("streams require"));

    let source = document();
    let analyzed = analyze_delta_document(&source, &Default::default(), config()).unwrap();
    let bytes = analyzed.resident_bytes;
    let mut delta = Arc::new(LexicalMiniDelta::default());
    let exact = LexicalProjectionConfig {
        mini_delta_bytes: NonZeroU64::new(bytes).unwrap(),
        ..config()
    };
    delta
        .upsert(&source, None, &Default::default(), exact)
        .unwrap();
    let before = delta.resident_bytes;
    let short = LexicalProjectionConfig {
        mini_delta_bytes: NonZeroU64::new(bytes - 1).unwrap(),
        ..config()
    };
    assert!(delta
        .upsert(&source, None, &Default::default(), short)
        .is_err());
    assert_eq!(delta.resident_bytes, before);
    assert_eq!(delta.upserts.len(), 1);
}

#[test]
fn term_policy_query_visitor_preserves_analyzer_terms_and_checks_aliases() {
    let fixture = Fixture::new();
    let reader = build(&fixture.0, config()).unwrap();
    for analyzer in [
        SearchAnalyzerLexicon::default(),
        SearchAnalyzerLexicon::empty()
            .with_normalized_alias_rule(["rag"], ["graph", "raw evidence"])
            .with_stopwords(["stop"]),
    ] {
        for text in [
            "Graph Graph",
            "HTTPServer raw-evidence RAG stop",
            "\u{4e2d}\u{6587} graph",
            "",
            "mixed_\u{e9}",
        ] {
            assert_eq!(
                reader
                    .tokenize_query(text, &analyzer, config().max_term_bytes)
                    .unwrap(),
                crate::tokenize(text, &analyzer)
            );
        }
    }
    let analyzer =
        SearchAnalyzerLexicon::empty().with_normalized_alias_rule(["short"], ["x".repeat(5203)]);
    assert!(reader
        .tokenize_query("short", &analyzer, config().max_term_bytes)
        .unwrap_err()
        .to_string()
        .contains("exceeding 5202"));
}

#[test]
fn term_policy_open_checks_dictionary_interior_and_posting_bounds() {
    let fixture = Fixture::new();
    let reader = build(&fixture.0, config()).unwrap();
    let mut dictionary = reader.manifest.clone();
    // Boundaries alone cannot stand in for the full term dictionary.
    for block in &mut dictionary.blocks {
        if block.kind == BlockKind::Postings {
            block.min_key = "a".into();
            block.max_key = "z".into();
        }
    }
    let error = LexicalProjectionReader::load_manifest_bytes(
        &fixture.0,
        &dictionary.encode(DEFAULT_MAX_MANIFEST_BYTES).unwrap(),
        None,
        11,
        13,
        Default::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("exceeding 4096"));
    let mut bounds = reader.manifest.clone();
    bounds.term_statistics[0].term = "x".into();
    let error = LexicalProjectionReader::load_manifest_bytes(
        &fixture.0,
        &bounds.encode(DEFAULT_MAX_MANIFEST_BYTES).unwrap(),
        None,
        11,
        13,
        Default::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("exceeding 4096"));
    let block = reader
        .manifest
        .blocks
        .iter()
        .find(|block| block.kind == BlockKind::Postings)
        .unwrap();
    let bytes = reader.read_block(block).unwrap();
    assert!(decode_posting_block(&bytes, 1, block, 4096, |_| Ok(())).is_err());
    let mut decoded = Vec::new();
    decode_posting_block(&bytes, 1, block, 5202, |posting| {
        decoded.push(posting);
        Ok(())
    })
    .unwrap();
    assert_eq!(decoded[0].term.len(), 5202);
}

#[test]
fn term_policy_merge_heads_share_a_checked_memory_budget() {
    let fixture = Fixture::new();
    let mut runs = SpillRuns::new(&fixture.0, 1, config());
    let postings = ["a", "b"].map(|id| Posting {
        term: "x".repeat(5202).into(),
        document_id: id.into(),
        term_frequency: 1,
        document_len: 1,
    });
    for posting in &postings {
        runs.spill(&mut vec![posting.clone()]).unwrap();
    }
    let exact_bytes = 2 * Posting::resident_bytes(&postings[0].term, "a");
    let mut actual = Vec::new();
    let exact = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(exact_bytes).unwrap(),
        ..config()
    };
    visit_merged_postings(&runs.paths, exact, |posting| {
        actual.push(posting.clone());
        Ok(())
    })
    .unwrap();
    assert_eq!(actual, postings);
    let short = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(exact_bytes - 1).unwrap(),
        ..config()
    };
    assert!(visit_merged_postings(&runs.paths, short, |_| Ok(())).is_err());
}

#[test]
fn term_policy_does_not_waive_block_spill_or_source_budgets() {
    let source = document();
    let posting = Posting {
        term: source.content.clone().into(),
        document_id: source.id.clone(),
        term_frequency: 1,
        document_len: 1,
    };
    let spill_bytes = RUN_HEADER.len() as u64 + posting.encoded_len();
    let block_bytes = 8 + 8 + 8 + 1 + 4 + posting.encoded_len();
    for (label, exact, short) in [
        (
            "spill",
            LexicalProjectionConfig {
                max_spill_bytes: NonZeroU64::new(spill_bytes).unwrap(),
                ..config()
            },
            LexicalProjectionConfig {
                max_spill_bytes: NonZeroU64::new(spill_bytes - 1).unwrap(),
                ..config()
            },
        ),
        (
            "block",
            LexicalProjectionConfig {
                max_block_bytes: NonZeroU64::new(block_bytes).unwrap(),
                ..config()
            },
            LexicalProjectionConfig {
                max_block_bytes: NonZeroU64::new(block_bytes - 1).unwrap(),
                ..config()
            },
        ),
        (
            "source",
            LexicalProjectionConfig {
                max_document_source_bytes: NonZeroU64::new(5202).unwrap(),
                ..config()
            },
            LexicalProjectionConfig {
                max_document_source_bytes: NonZeroU64::new(5201).unwrap(),
                ..config()
            },
        ),
    ] {
        let good = Fixture::new();
        build(&good.0, exact).unwrap();
        let bad = Fixture::new();
        let error = build(&bad.0, short).unwrap_err();
        assert!(error.to_string().contains(label), "{label}: {error}");
        assert!(!bad.0.join(MANIFEST_FILE).exists());
        assert!(fs::read_dir(&bad.0).unwrap().next().is_none());
    }
}

#[test]
fn term_policy_cancelled_scan_removes_long_term_staging() {
    let fixture = Fixture::new();
    let error = LexicalProjectionWriter::new(config())
        .write_scanned(
            &fixture.0,
            1,
            None,
            11,
            13,
            |consume| {
                consume(&document())?;
                Err(HawDBError::Execution("cancelled scan".into()))
            },
            &Default::default(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("cancelled scan"));
    assert!(fs::read_dir(&fixture.0).unwrap().next().is_none());
}
