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

#[test]
fn block_max_randomized_filtered_overlay_matches_exhaustive_and_rebuilt() {
    let vocabulary = ["graph", "memory", "index", "query", "checkpoint", "wal"];
    let queries = [
        vec!["rare", "storage"],
        vec!["graph", "storage"],
        vec!["memory", "rare", "wal"],
        vec!["index", "query"],
        vec!["\u{6570}\u{636e}", "storage"],
        vocabulary.to_vec(),
    ];
    let config = pruning_config(4);
    let analyzer = SearchAnalyzerLexicon::default();
    let mut pruned_cases = 0;
    for seed in [292_u64, 7, 0x5eed, 0x1234, 0xc0ffee] {
        let root = projection_root(&format!("random-pruning-{seed}"));
        let rebuilt_root = projection_root(&format!("random-pruning-rebuilt-{seed}"));
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&rebuilt_root).unwrap();
        let mut random = seed;
        let mut next = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        let documents = (0..512)
            .map(|index| {
                let mut content = "storage ".repeat((next() % 9 + 1) as usize);
                for term in vocabulary {
                    let frequency = (next() % 9) as usize;
                    content.push_str(&format!("{term} ").repeat(frequency));
                }
                if index % 64 == 0 {
                    content.push_str(&"rare ".repeat((next() % 12 + 1) as usize));
                }
                if next() % 3 == 0 {
                    content
                        .push_str(&"\u{6570}\u{636e}\u{5e93} ".repeat((next() % 4 + 1) as usize));
                }
                document(&format!("doc-{index:04}"), "title", &content)
            })
            .collect::<Vec<_>>();
        let reader = LexicalProjectionWriter::new(config)
            .write(&root, 1, Some(7), 11, 13, documents.iter(), &analyzer)
            .unwrap();
        assert!(reader.posting_blocks("storage").count() > 2);
        let mut live = documents
            .iter()
            .map(|document| (document.id.clone(), document.clone()))
            .collect::<BTreeMap<_, _>>();
        let empty = LexicalMiniDelta::default();
        let mut delta = Arc::new(LexicalMiniDelta::default());
        for step in 0..12 {
            let id = format!("doc-{:04}", next() % 512);
            let previous = live.get(&id);
            if step % 3 == 0 {
                assert!(delta.delete(&id, previous, &analyzer, config).unwrap());
                live.remove(&id);
            } else {
                let replacement = document(
                    &id,
                    "title",
                    &format!(
                        "{}{}",
                        "rare ".repeat((next() % 40 + 1) as usize),
                        "graph memory storage \u{6570}\u{636e}\u{5e93} "
                            .repeat((next() % 8 + 1) as usize),
                    ),
                );
                delta
                    .upsert(&replacement, previous, &analyzer, config)
                    .unwrap();
                live.insert(id, replacement);
            }
        }
        for index in 0..2 {
            let inserted = document(
                &format!("added-{index:04}"),
                "title",
                "rare rare graph storage \u{6570}\u{636e}\u{5e93}",
            );
            delta.upsert(&inserted, None, &analyzer, config).unwrap();
            live.insert(inserted.id.clone(), inserted);
        }
        let rebuilt = LexicalProjectionWriter::new(config)
            .write(&rebuilt_root, 1, Some(7), 11, 13, live.values(), &analyzer)
            .unwrap();
        for overlay in [false, true] {
            let (changes, reference) = if overlay {
                (delta.as_ref(), rebuilt.as_ref())
            } else {
                (&empty, reader.as_ref())
            };
            for query in &queries {
                let terms = query.iter().map(|term| (*term).to_string()).collect();
                for rank_window in [1, 4, 16] {
                    for filtered in [false, true] {
                        let allowed =
                            |id: &str| {
                                Ok(!filtered
                                    || !id.as_bytes().last().is_some_and(|last| last % 3 == 0))
                            };
                        let exhaustive = reader
                            .score_with_term_limit_and_statistics(
                                &terms,
                                changes,
                                config.max_term_bytes,
                                Some(rank_window),
                                None,
                                false,
                                allowed,
                            )
                            .unwrap();
                        let pruned = reader
                            .score_with_term_limit_and_statistics(
                                &terms,
                                changes,
                                config.max_term_bytes,
                                Some(rank_window),
                                None,
                                true,
                                allowed,
                            )
                            .unwrap();
                        let rebuilt = reference
                            .score(&terms, &empty, Some(rank_window), allowed)
                            .unwrap();
                        let bits = |report: &LexicalQueryReport| {
                            report
                                .scores
                                .iter()
                                .map(|(id, score)| (id.clone(), score.to_bits()))
                                .collect::<BTreeMap<_, _>>()
                        };
                        assert_eq!(
                            bits(&pruned), bits(&exhaustive),
                            "seed={seed} query={query:?} k={rank_window} filtered={filtered} overlay={overlay}"
                        );
                        assert_eq!(
                            bits(&pruned), bits(&rebuilt),
                            "rebuilt seed={seed} query={query:?} k={rank_window} filtered={filtered} overlay={overlay}"
                        );
                        pruned_cases += usize::from(pruned.blocks_skipped > 0);
                    }
                }
            }
        }
        drop(reader);
        drop(rebuilt);
        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(rebuilt_root).unwrap();
    }
    assert!(pruned_cases > 0, "the campaign must exercise block pruning");
}

#[test]
fn block_max_stream_seek_skips_intermediate_payloads() {
    let root = projection_root("pruning-stream-seek");
    fs::create_dir_all(&root).unwrap();
    let config = pruning_config(4);
    let reader = pruning_corpus_reader(&root, config);
    let mut exhaustive = TermPostingStream::new(&reader, "storage");
    let mut pruned = TermPostingStream::new(&reader, "storage");
    assert_eq!(pruned.peek_ordinal().unwrap(), Some(0));
    pruned.skip_to(400).unwrap();
    while exhaustive
        .peek_ordinal()
        .unwrap()
        .is_some_and(|ordinal| ordinal < 400)
    {
        exhaustive.take();
    }
    assert_eq!(
        pruned.peek_ordinal().unwrap(),
        exhaustive.peek_ordinal().unwrap()
    );
    assert!(pruned.blocks_skipped > 0);
    assert!(pruned.postings_visited < exhaustive.postings_visited);
    pruned.skip_to(512).unwrap();
    assert_eq!(pruned.peek_ordinal().unwrap(), None);
    drop(pruned);
    drop(exhaustive);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
