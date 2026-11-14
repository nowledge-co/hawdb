// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

struct Block {
    postings: Vec<(u64, u64)>,
    bounds: Bm25Block,
}

/// An in-memory posting list split into blocks, counting what it decodes.
struct Cursor<'a> {
    blocks: &'a [Block],
    hint: usize,
    position: Option<u64>,
    exact: Option<(usize, usize)>,
    /// The block whose postings are decoded, as a real cursor caches it.
    loaded: Option<usize>,
    decoded: &'a std::cell::Cell<u64>,
    decoded_postings: &'a std::cell::Cell<u64>,
}

impl<'a> Cursor<'a> {
    fn new(
        blocks: &'a [Block],
        decoded: &'a std::cell::Cell<u64>,
        decoded_postings: &'a std::cell::Cell<u64>,
    ) -> Self {
        Self {
            blocks,
            hint: 0,
            position: blocks.first().map(|_| 0),
            exact: None,
            loaded: None,
            decoded,
            decoded_postings,
        }
    }

    fn block_index(&mut self, target: u64) -> Option<usize> {
        while self.hint < self.blocks.len() && self.blocks[self.hint].bounds.last < target {
            self.hint += 1;
        }
        (self.hint < self.blocks.len()).then_some(self.hint)
    }
}

impl Bm25PostingCursor for Cursor<'_> {
    fn position(&self) -> Option<u64> {
        self.position
    }

    fn is_exact(&self) -> bool {
        self.exact.is_some()
    }

    fn block_at(&mut self, target: u64) -> Result<Option<Bm25Block>> {
        Ok(self
            .block_index(target)
            .map(|index| self.blocks[index].bounds))
    }

    fn skip_to(&mut self, target: u64) -> Result<()> {
        let Some(position) = self.position else {
            return Ok(());
        };
        if target <= position {
            return Ok(());
        }
        self.exact = None;
        self.position = self.block_index(target).map(|_| target);
        Ok(())
    }

    fn materialize(&mut self) -> Result<()> {
        let Some(position) = self.position else {
            return Ok(());
        };
        let Some(block) = self.block_index(position) else {
            self.position = None;
            return Ok(());
        };
        if self.loaded != Some(block) {
            self.loaded = Some(block);
            self.decoded.set(self.decoded.get() + 1);
            self.decoded_postings
                .set(self.decoded_postings.get() + self.blocks[block].postings.len() as u64);
        }
        let postings = &self.blocks[block].postings;
        let index = postings.partition_point(|(ordinal, _)| *ordinal < position);
        self.position = Some(postings[index].0);
        self.exact = Some((block, index));
        Ok(())
    }

    fn current(&self) -> Result<(u64, Bm25Block)> {
        let (block, index) = self.exact.expect("materialized");
        Ok((
            self.blocks[block].postings[index].1,
            self.blocks[block].bounds,
        ))
    }
}

struct Documents<'a> {
    lengths: &'a [u64],
    keys: &'a [Vec<u8>],
    admitted: isize,
}

impl Bm25Documents for Documents<'_> {
    fn length(&mut self, ordinal: u64) -> Result<u64> {
        Ok(self.lengths[ordinal as usize])
    }

    fn key(&mut self, ordinal: u64) -> Result<&[u8]> {
        Ok(&self.keys[ordinal as usize])
    }

    fn checkpoint(&mut self) -> Result<()> {
        Ok(())
    }

    fn admit(&mut self, bytes: usize) -> Result<()> {
        self.admitted += bytes as isize;
        Ok(())
    }

    fn release(&mut self, bytes: usize) {
        self.admitted -= bytes as isize;
    }
}

struct Corpus {
    lengths: Vec<u64>,
    keys: Vec<Vec<u8>>,
    /// Per term, ordinal-sorted `(ordinal, frequency)`.
    postings: Vec<Vec<(u64, u64)>>,
}

impl Corpus {
    fn blocks(&self, term: usize, size: usize, rng: &mut Rng, loose: bool) -> Vec<Block> {
        self.postings[term]
            .chunks(size.max(1))
            .map(|chunk| {
                let min_length = chunk
                    .iter()
                    .map(|(ordinal, _)| self.lengths[*ordinal as usize])
                    .min()
                    .unwrap();
                Block {
                    postings: chunk.to_vec(),
                    bounds: Bm25Block {
                        last: chunk.last().unwrap().0,
                        max_frequency: chunk.iter().map(|(_, tf)| *tf).max().unwrap(),
                        // Maintenance keeps a conservative minimum length.
                        min_length: if loose {
                            1 + rng.below(min_length)
                        } else {
                            min_length
                        },
                    },
                }
            })
            .collect()
    }

    fn average(&self) -> f64 {
        self.lengths.iter().sum::<u64>() as f64 / self.lengths.len() as f64
    }

    fn idf(&self, term: usize) -> f64 {
        idf(self.lengths.len() as u64, self.postings[term].len() as u64)
    }

    /// Brute force: every document's score summed in query-term order.
    fn oracle(&self, query: &[usize], k: usize) -> Vec<Bm25Hit> {
        let average = self.average();
        let mut scores = std::collections::BTreeMap::<u64, f64>::new();
        for &term in query {
            let idf = self.idf(term);
            for &(ordinal, frequency) in &self.postings[term] {
                let length = self.lengths[ordinal as usize];
                *scores.entry(ordinal).or_insert(0.0) +=
                    term_score(idf, frequency as f64, length as f64, average);
            }
        }
        let mut hits = scores
            .into_iter()
            .map(|(ordinal, score)| Bm25Hit {
                ordinal,
                key: self.keys[ordinal as usize].clone(),
                score,
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        hits.truncate(k);
        hits
    }
}

fn random_corpus(rng: &mut Rng, documents: usize, vocabulary: usize) -> Corpus {
    let mut postings = vec![Vec::new(); vocabulary];
    let mut lengths = Vec::with_capacity(documents);
    let mut keys = Vec::with_capacity(documents);
    for ordinal in 0..documents as u64 {
        let mut length = 0;
        // Zipf-like: low term ids are common. A few documents repeat a term.
        for (term, list) in postings.iter_mut().enumerate() {
            if rng.below(term as u64 + 2) == 0 {
                let frequency = if rng.below(8) == 0 {
                    1 + rng.below(9)
                } else {
                    1 + rng.below(2)
                };
                list.push((ordinal, frequency));
                length += frequency;
            }
        }
        lengths.push(length + rng.below(20));
        // Keys are unique, like primary keys, but unrelated to ordinals, so
        // equal scores are ordered by key rather than by ordinal.
        let mut key = rng.below(documents as u64 * 4).to_be_bytes().to_vec();
        key.extend_from_slice(&ordinal.to_be_bytes());
        keys.push(key);
    }
    for length in &mut lengths {
        *length = (*length).max(1);
    }
    Corpus {
        lengths,
        keys,
        postings,
    }
}

fn run(
    corpus: &Corpus,
    query: &[usize],
    k: usize,
    block_size: usize,
    rng: &mut Rng,
    loose: bool,
) -> (Bm25TopK, u64, u64) {
    let blocks = query
        .iter()
        .map(|&term| corpus.blocks(term, block_size, rng, loose))
        .collect::<Vec<_>>();
    let decoded = std::cell::Cell::new(0);
    let postings = std::cell::Cell::new(0);
    let average = corpus.average();
    let mut terms = query
        .iter()
        .zip(&blocks)
        .map(|(&term, blocks)| {
            let idf = corpus.idf(term);
            Bm25Term {
                cursor: Cursor::new(blocks, &decoded, &postings),
                idf,
                max_bound: blocks.iter().fold(0.0, |bound: f64, block| {
                    bound.max(term_upper_bound(
                        idf,
                        block.bounds.max_frequency,
                        block.bounds.min_length,
                        average,
                    ))
                }),
            }
        })
        .collect::<Vec<_>>();
    let mut documents = Documents {
        lengths: &corpus.lengths,
        keys: &corpus.keys,
        admitted: 0,
    };
    let top = block_max_wand(&mut terms, average, k, &mut documents).unwrap();
    assert!(documents.admitted >= 0);
    (top, decoded.get(), postings.get())
}

fn assert_bit_exact(actual: &[Bm25Hit], expected: &[Bm25Hit]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.ordinal, expected.ordinal);
        assert_eq!(actual.key, expected.key);
        assert_eq!(actual.score.to_bits(), expected.score.to_bits());
    }
}

/// A small fixed corpus: three terms of different densities over 40
/// documents of varying length, with repeated terms and keys unrelated to
/// ordinals (so equal scores order by key).
fn fixed_corpus() -> Corpus {
    let documents = 40u64;
    let mut postings = vec![Vec::new(); 3];
    let mut lengths = Vec::new();
    for ordinal in 0..documents {
        let mut length = 2 + ordinal % 7;
        for (term, list) in postings.iter_mut().enumerate() {
            let every = [1, 3, 7][term];
            if ordinal % every == 0 {
                let frequency = 1 + (ordinal / every) % [1, 3, 4][term];
                list.push((ordinal, frequency));
                length += frequency;
            }
        }
        lengths.push(length);
    }
    Corpus {
        lengths,
        keys: (0..documents)
            .map(|ordinal| [((ordinal * 17) % 41) as u8, ordinal as u8].to_vec())
            .collect(),
        postings,
    }
}

#[test]
fn block_max_wand_returns_nothing_for_empty_input() {
    let corpus = fixed_corpus();
    let mut rng = Rng(1);
    // No query terms, k = 0, and a term without postings.
    assert!(run(&corpus, &[], 5, 4, &mut rng, false).0.hits.is_empty());
    assert!(run(&corpus, &[0, 1], 0, 4, &mut rng, false)
        .0
        .hits
        .is_empty());
    let empty = Corpus {
        postings: vec![Vec::new()],
        ..fixed_corpus()
    };
    assert!(run(&empty, &[0], 5, 4, &mut rng, false).0.hits.is_empty());
}

#[test]
fn block_max_wand_is_exact_at_block_boundaries() {
    // Blocks of one posting, a size that splits each list unevenly, and one
    // block per list; k below, at and beyond a block and the match count.
    let corpus = fixed_corpus();
    let mut rng = Rng(2);
    let mut check = |query: &[usize], k, block_size| {
        let (top, _, _) = run(&corpus, query, k, block_size, &mut rng, false);
        assert_bit_exact(&top.hits, &corpus.oracle(query, k));
    };
    check(&[0], 1, 1);
    check(&[1, 2], 3, 3);
    check(&[0, 1, 2], 8, 7);
    check(&[0, 1, 2], 41, 64);
}

#[test]
fn block_max_wand_is_exact_with_loose_length_bounds() {
    // Maintenance may leave a block's minimum length below its documents'
    // (after deletes or updates): bounds only loosen, results stay exact.
    let corpus = fixed_corpus();
    let mut rng = Rng(3);
    for (query, k) in [(vec![0], 1), (vec![1, 2], 5), (vec![0, 1, 2], 12)] {
        let (top, _, _) = run(&corpus, &query, k, 3, &mut rng, true);
        assert_bit_exact(&top.hits, &corpus.oracle(&query, k));
    }
}

/// One generated case: a random corpus, query, `k` and block size, compared
/// with the brute-force ranking. Every case derives from its own seed, which
/// the failure message names; replay it with `generated_case(seed)`.
fn generated_case(seed: u64) {
    let mut rng = Rng(seed | 1);
    let documents = 1 + rng.below(300) as usize;
    let vocabulary = 1 + rng.below(12) as usize;
    let corpus = random_corpus(&mut rng, documents, vocabulary);
    let mut query = (0..1 + rng.below(5) as usize)
        .map(|_| rng.below(vocabulary as u64) as usize)
        .filter(|&term| !corpus.postings[term].is_empty())
        .collect::<Vec<_>>();
    query.sort_unstable();
    query.dedup();
    let k = 1 + rng.below(documents as u64 + 2) as usize;
    let block_size = 1 + rng.below(9) as usize;
    let loose = rng.below(3) == 0;
    let (top, _, _) = run(&corpus, &query, k, block_size, &mut rng, loose);
    let expected = corpus.oracle(&query, k);
    let context = format!("seed {seed:#x}: query {query:?} k={k} block_size={block_size}");
    assert_eq!(top.hits.len(), expected.len(), "{context}");
    for (actual, expected) in top.hits.iter().zip(&expected) {
        assert_eq!(
            (actual.ordinal, &actual.key, actual.score.to_bits()),
            (expected.ordinal, &expected.key, expected.score.to_bits()),
            "{context}"
        );
    }
}

/// The generated differential campaign. Manual only, not part of ordinary
/// unit runs or CI: `cargo test --locked -p hawdb-executor --lib
/// bm25::tests::block_max_wand_generated_campaign -- --ignored`.
#[test]
#[ignore = "explicit local differential campaign"]
fn block_max_wand_generated_campaign() {
    let mut seeds = Rng(0x9e37_79b9_7f4a_7c15);
    for _ in 0..400 {
        generated_case(seeds.next());
    }
}

#[test]
fn block_max_wand_handles_ties_and_identical_documents_by_key() {
    // Every document is identical: the ranking is the key order alone.
    let documents = 500;
    let corpus = Corpus {
        lengths: vec![3; documents],
        keys: (0..documents as u64)
            .map(|ordinal| ((ordinal * 7919) % 1009).to_be_bytes().to_vec())
            .collect(),
        postings: vec![(0..documents as u64).map(|ordinal| (ordinal, 2)).collect()],
    };
    let mut rng = Rng(7);
    for k in [1, 2, 17, 128, 499, 500, 600] {
        let (top, _, _) = run(&corpus, &[0], k, 16, &mut rng, false);
        assert_bit_exact(&top.hits, &corpus.oracle(&[0], k));
    }
}

#[test]
fn block_max_wand_visits_a_small_fraction_for_one_row_on_a_large_corpus() {
    // 100,000 documents: a term in every document, usually once; a few
    // documents repeat it. Top-1 must not decode the whole posting list.
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let documents = 100_000u64;
    let mut common = Vec::new();
    let mut rare = Vec::new();
    let mut lengths = Vec::new();
    for ordinal in 0..documents {
        let frequency = if rng.below(2_000) == 0 {
            5 + rng.below(20)
        } else {
            1
        };
        common.push((ordinal, frequency));
        if rng.below(50) == 0 {
            rare.push((ordinal, 1 + rng.below(2)));
        }
        lengths.push(20 + rng.below(30) + frequency);
    }
    let corpus = Corpus {
        lengths,
        keys: (0..documents)
            .map(|ordinal| ordinal.to_be_bytes().to_vec())
            .collect(),
        postings: vec![common, rare],
    };
    let total_blocks = documents.div_ceil(128);
    for (query, k) in [
        (vec![0], 1),
        (vec![0], 10),
        (vec![0, 1], 1),
        (vec![0, 1], 10),
    ] {
        let (top, decoded, postings) = run(&corpus, &query, k, 128, &mut rng, false);
        assert_bit_exact(&top.hits, &corpus.oracle(&query, k));
        let total_postings: u64 = query
            .iter()
            .map(|&term| corpus.postings[term].len() as u64)
            .sum();
        eprintln!(
            "MEASURE query {query:?} k={k}: scored {} documents, decoded {postings} of {total_postings} postings, {decoded} of {total_blocks} blocks",
            top.stats.documents_scored
        );
        // The union is every document; only a fraction is scored.
        assert!(
            top.stats.documents_scored * if k == 1 { 20 } else { 4 } < documents,
            "query {query:?} k={k}: scored {} documents",
            top.stats.documents_scored
        );
        if query.len() == 1 {
            // Alone, the dense term's blocks are skipped without decoding.
            assert!(
                postings * if k == 1 { 20 } else { 4 } < total_postings,
                "query {query:?} k={k}: decoded {postings} of {total_postings} postings, {decoded} of {total_blocks} blocks"
            );
        }
        assert!(top.stats.block_skips > 0);
    }
}

#[test]
fn shared_scorer_matches_the_reference_formula() {
    let idf = idf(10, 3);
    assert_eq!(idf.to_bits(), (1.0f64 + 7.5 / 3.5).ln().to_bits());
    let score = term_score(idf, 2.0, 7.0, 5.0);
    let reference =
        idf * (2.0 * (BM25_K1 + 1.0)) / (2.0 + BM25_K1 * (1.0 - BM25_B + BM25_B * 7.0 / 5.0));
    assert_eq!(score.to_bits(), reference.to_bits());
    assert!(term_upper_bound(idf, 2, 7, 5.0) > score);
    assert!(term_upper_bound(idf, 3, 1, 5.0) > term_score(idf, 3.0, 1.0, 5.0));
}
