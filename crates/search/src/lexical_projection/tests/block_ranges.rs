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

//! Posting-block ranges resolved by binary search (#1940).

use super::*;
use crate::lexical_projection::posting_ranges::MANIFEST_KEY_COMPARISONS;
use hawdb_core::RuntimeCancellationToken;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};

/// Many small posting blocks: unique per-document terms, plus terms whose
/// postings span several blocks.
pub(super) fn many_block_reader(root: &Path) -> Arc<LexicalProjectionReader> {
    let analyzer = SearchAnalyzerLexicon::default();
    let documents = (0..1024u32)
        .map(|index| {
            let mut content = format!("unique{index:04} ");
            if index % 2 == 0 {
                content.push_str("storage ");
            }
            if index % 128 == 0 {
                content.push_str("rare ");
            }
            document(&format!("doc-{index:04}"), "title", &content)
        })
        .collect::<Vec<_>>();
    LexicalProjectionWriter::new(pruning_config(u64::MAX))
        .write(root, 1, Some(7), 11, 13, documents.iter(), &analyzer)
        .unwrap()
}

fn linear_range(reader: &LexicalProjectionReader, term: &str) -> Vec<usize> {
    reader
        .manifest
        .blocks
        .iter()
        .enumerate()
        .filter(|(_, block)| {
            block.kind == BlockKind::Postings
                && block.min_key.as_str() <= term
                && term <= block.max_key.as_str()
        })
        .map(|(index, _)| index)
        .collect()
}

fn comparisons<T>(work: impl FnOnce() -> T) -> (T, usize) {
    let before = MANIFEST_KEY_COMPARISONS.with(std::cell::Cell::get);
    let output = work();
    let after = MANIFEST_KEY_COMPARISONS.with(std::cell::Cell::get);
    (output, after - before)
}

#[test]
fn posting_block_ranges_match_a_linear_manifest_scan() {
    let root = projection_root("block-ranges-linear");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let reader = many_block_reader(&root);
    let postings = reader
        .manifest
        .blocks
        .iter()
        .filter(|block| block.kind == BlockKind::Postings)
        .count();
    assert!(postings > 64, "fixture has {postings} posting blocks");
    let mut probes = vec![
        String::new(),
        "a".to_owned(),
        "rare".to_owned(),
        "storage".to_owned(),
        "unique0512".to_owned(),
        "unique9999".to_owned(),
        "\u{10FFFF}".to_owned(),
    ];
    for block in &reader.manifest.blocks {
        probes.push(block.min_key.clone());
        probes.push(block.max_key.clone());
        probes.push(format!("{}0", block.min_key));
        probes.push(format!("{}0", block.max_key));
    }
    let mut spanning = 0;
    for term in &probes {
        let range = reader.posting_block_range(term);
        assert_eq!(
            range.clone().collect::<Vec<_>>(),
            linear_range(&reader, term),
            "{term:?}"
        );
        spanning += usize::from(range.len() > 1);
    }
    assert!(spanning > 0, "some term spans several posting blocks");
    assert!(reader.posting_block_range("storage").len() > 2);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn query_manifest_work_is_logarithmic_per_term() {
    let root = projection_root("block-ranges-logarithmic");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let reader = many_block_reader(&root);
    let blocks = reader.manifest.blocks.len();
    let config = reader.config;
    let terms = BTreeSet::from([
        "rare".to_owned(),
        "storage".to_owned(),
        "unique0007".to_owned(),
        "unique1000".to_owned(),
        "zzz".to_owned(),
    ]);
    // Two partition points per term, each at most ceil(log2(n)) + 1 probes.
    let per_resolution = terms.len() * 2 * (blocks.ilog2() as usize + 2);
    let (statistics, statistics_comparisons) = comparisons(|| {
        reader
            .query_statistics(&terms, config.max_term_bytes)
            .unwrap()
    });
    assert!(
        statistics_comparisons <= per_resolution,
        "{statistics_comparisons}"
    );
    // Local statistics, admission and every posting stream share one resolution.
    let (local, local_comparisons) = comparisons(|| {
        reader
            .score(&terms, &LexicalMiniDelta::default(), Some(10), |_| Ok(true))
            .unwrap()
    });
    assert!(local_comparisons <= per_resolution, "{local_comparisons}");
    let (global, global_comparisons) = comparisons(|| {
        reader
            .score_with_global_statistics_and_pruning(
                &terms,
                config.max_term_bytes,
                Some(10),
                &statistics,
                true,
                |_| Ok(true),
            )
            .unwrap()
    });
    assert!(global_comparisons <= per_resolution, "{global_comparisons}");
    assert_eq!(local.scores, global.scores);
    // A linear scan would compare every block for every term.
    assert!(
        per_resolution * 8 < blocks * terms.len(),
        "fixture too small to distinguish: {blocks} blocks"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn overlapping_or_decreasing_posting_block_keys_are_refused_at_open() {
    let root = projection_root("block-ranges-order");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let reader = many_block_reader(&root);
    let first_posting = reader
        .manifest
        .blocks
        .iter()
        .position(|block| block.kind == BlockKind::Postings)
        .unwrap();
    // A term spanning a boundary is the only permitted shared key.
    assert!(reader.manifest.validate().is_ok());
    for case in 0..2 {
        let mut body = reader.manifest.clone();
        let index = first_posting + 1;
        let next_min = body.blocks[index + 1].min_key.clone();
        match case {
            // Overlap: this block's range extends past the next block's start.
            0 => body.blocks[index].max_key = format!("{next_min}\u{10FFFF}"),
            // Decreasing maximum while minima stay ordered.
            _ => {
                let max = body.blocks[index + 1].max_key.clone();
                body.blocks[index].max_key = format!("{max}\u{10FFFF}");
            }
        }
        let error = body.validate().unwrap_err();
        assert!(matches!(error, HawDBError::Storage(_)), "{error}");
        assert!(error.to_string().contains("not ordered"), "{error}");
        let checksum = checksum(&serde_json::to_vec(&body).unwrap());
        let bytes = serde_json::to_vec(&ManifestEnvelope { body, checksum }).unwrap();
        let error = ManifestBody::decode(&bytes).unwrap_err();
        assert!(
            error.to_string().contains("not ordered"),
            "case {case}: {error}"
        );
    }
    fs::remove_dir_all(root).unwrap();
}

/// Cancellation is observed before any further posting block is read or
/// decoded: after a cancel, a stream yields at most the postings already
/// decoded from its current block.
#[test]
fn posting_stream_cancellation_stops_at_the_next_block_boundary() {
    let root = projection_root("block-ranges-cancel");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let reader = many_block_reader(&root);
    let range = reader.posting_block_range("storage");
    let blocks = reader.posting_blocks_in(range);
    assert!(blocks.len() > 2);
    let largest_block = blocks
        .iter()
        .map(|block| block.entry_count as usize)
        .max()
        .unwrap();
    let budget = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "stream cancel", budget);
    for cancel_after in [0usize, 1, 37, 200] {
        let cancellation = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(cancellation.clone());
        let context = QueryContext {
            io_scope: None,
            execution: None,
            memory: &account,
            task: &task,
        };
        let encoded = RefCell::new(PostingReadBuffer::default());
        let mut stream =
            TermPostingStream::with_context(&reader, "storage", blocks, Some(context), &encoded);
        let mut taken = 0usize;
        while taken < cancel_after {
            assert!(stream.peek().unwrap().is_some());
            stream.take().unwrap();
            taken += 1;
        }
        cancellation.cancel();
        let mut after_cancel = 0usize;
        let error = loop {
            match stream.peek() {
                Ok(Some(_)) => {
                    stream.take().unwrap();
                    after_cancel += 1;
                }
                Ok(None) => panic!("cancelled stream reached its end"),
                Err(error) => break error,
            }
        };
        assert!(
            matches!(error, HawDBError::RuntimeTaskStopped(_)),
            "{error}"
        );
        assert!(
            after_cancel <= largest_block,
            "{after_cancel} postings after cancel exceed one block ({largest_block})"
        );
        // The bound is the current block's remaining postings, not the doclist.
        assert!(taken + after_cancel < 512);
    }
    fs::remove_dir_all(root).unwrap();
}
