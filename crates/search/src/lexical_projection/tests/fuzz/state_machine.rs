use super::*;
use skein_core::{RuntimeMemoryReservation, RuntimeTaskContext};

const TERMS: [&str; 8] = [
    "graph", "memory", "storage", "vector", "query", "index", "cache", "needle",
];

fn random_document(random: &mut Random, id: &str) -> SearchDocument {
    let mut terms = Vec::new();
    for _ in 0..random.index(18) {
        terms.push(TERMS[random.index(TERMS.len())]);
    }
    let title = TERMS[random.index(TERMS.len())];
    document(id, title, &terms.join(" "))
}

// The oracle shares tokenization, but no persisted statistics, delta accounting,
// posting merge, ranking collector or production BM25 scoring implementation.
pub(super) fn reference_scores(
    documents: &BTreeMap<String, SearchDocument>,
    analyzer: &SearchAnalyzerLexicon,
    terms: &BTreeSet<String>,
    excluded: &BTreeSet<String>,
) -> Vec<(String, f64)> {
    let frequencies = documents
        .values()
        .map(|document| {
            let mut counts = BTreeMap::<String, usize>::new();
            for term in document_tokens(document, analyzer) {
                *counts.entry(term).or_default() += 1;
            }
            (document.id.clone(), counts)
        })
        .collect::<BTreeMap<_, _>>();
    let total: usize = frequencies
        .values()
        .flat_map(|counts| counts.values())
        .sum();
    let average = (total as f64 / documents.len().max(1) as f64).max(1.0);
    let mut scores = Vec::new();
    for (id, counts) in &frequencies {
        if excluded.contains(id) {
            continue;
        }
        let length: usize = counts.values().sum();
        let mut score = 0.0;
        for term in terms {
            let Some(&tf) = counts.get(term) else {
                continue;
            };
            let df = frequencies
                .values()
                .filter(|counts| counts.contains_key(term))
                .count();
            let weight =
                (1.0 + (documents.len() as f64 - df as f64 + 0.5) / (df as f64 + 0.5)).ln();
            score += weight * (tf as f64 * (BM25_K1 + 1.0))
                / (tf as f64 + BM25_K1 * (1.0 - BM25_B + BM25_B * length as f64 / average));
        }
        if score > 0.0 {
            scores.push((id.clone(), score));
        }
    }
    scores.sort_by(|(left_id, left), (right_id, right)| {
        right.total_cmp(left).then(left_id.cmp(right_id))
    });
    scores
}

struct Model {
    root: PathBuf,
    reader: Arc<LexicalProjectionReader>,
    documents: BTreeMap<String, SearchDocument>,
    delta: LexicalMiniDelta,
    analyzer: SearchAnalyzerLexicon,
    config: LexicalProjectionConfig,
    cache: Arc<SegmentCache>,
    seed: u64,
}

impl Model {
    fn new(seed: u64) -> Self {
        let root = temporary_root(&format!("state-{seed}"));
        let analyzer = SearchAnalyzerLexicon::default();
        let config = LexicalProjectionConfig {
            target_block_bytes: NonZeroU64::new(512).unwrap(),
            max_block_bytes: NonZeroU64::new(4096).unwrap(),
            build_memory_bytes: NonZeroU64::new(4096).unwrap(),
            max_merge_fan_in: NonZeroUsize::new(3).unwrap(),
            ..LexicalProjectionConfig::default()
        };
        let mut random = Random(seed);
        let documents = (0..257)
            .map(|index| {
                let id = format!("memory:{index:04}");
                let mut document = random_document(&mut random, &id);
                // Force a frequent term across the SIMD/tail/skip boundary while
                // retaining variable frequencies and unequal document lengths.
                document.content.push_str(" graph");
                (id, document)
            })
            .collect::<BTreeMap<_, _>>();
        let cache = Arc::new(SegmentCache::new(32 * 1024));
        let reader = LexicalProjectionWriter::new(config)
            .with_cache(cache.clone())
            .write(&root, 1, None, 11, 13, documents.values(), &analyzer)
            .unwrap();
        assert!(
            reader
                .term_metadata("graph", &mut 0)
                .unwrap()
                .unwrap()
                .skip_offset
                > 0
        );
        Self {
            root,
            reader,
            documents,
            delta: LexicalMiniDelta::default(),
            analyzer,
            config,
            cache,
            seed,
        }
    }

    fn upsert(&mut self, document: SearchDocument) {
        self.delta
            .upsert(
                &document,
                self.documents.get(&document.id),
                &self.analyzer,
                self.config,
            )
            .unwrap();
        self.documents.insert(document.id.clone(), document);
    }

    fn delete(&mut self, id: &str) {
        assert!(self
            .delta
            .delete(id, self.documents.get(id), &self.analyzer, self.config)
            .unwrap());
        self.documents.remove(id);
    }

    fn reopen(&mut self) {
        let previous = self.reader.clone();
        self.reader = LexicalProjectionReader::load_named_with_cache(
            &self.root,
            MANIFEST_FILE,
            None,
            11,
            13,
            self.config,
            self.cache.clone(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(self.reader.generation(), previous.generation());
        assert_eq!(self.reader.artifact_bytes(), previous.artifact_bytes());
        assert_ne!(self.reader.cache_namespace, previous.cache_namespace);
    }

    fn publish(&mut self) {
        let old_reader = self.reader.clone();
        let old_delta = self.delta.clone();
        let terms = BTreeSet::from(["graph".to_string(), "query".to_string()]);
        let old_scores = old_reader
            .score(&terms, &old_delta, None, |_| Ok(true))
            .unwrap()
            .scores;
        self.reader = LexicalProjectionWriter::new(self.config)
            .with_cache(self.cache.clone())
            .write(
                &self.root,
                old_reader.generation() + 1,
                None,
                11,
                13,
                self.documents.values(),
                &self.analyzer,
            )
            .unwrap();
        self.delta = LexicalMiniDelta::default();
        assert_eq!(
            self.reader
                .score(&terms, &self.delta, None, |_| Ok(true))
                .unwrap()
                .scores,
            old_scores
        );
        // A pinned immutable reader and its own delta survive publication.
        assert_eq!(
            old_reader
                .score(&terms, &old_delta, None, |_| Ok(true))
                .unwrap()
                .scores,
            old_scores
        );
        self.reopen();
    }

    fn failed_publication(&mut self, late: bool) {
        let manifest = fs::read(self.root.join(MANIFEST_FILE)).unwrap();
        let generation = self.reader.generation() + 1;
        let mut config = self.config;
        if late {
            config.dictionary_validation_bytes = NonZeroU64::MIN;
        }
        let result = LexicalProjectionWriter::new(config).write_scanned(
            &self.root,
            generation,
            None,
            11,
            13,
            |consume| {
                for (ordinal, document) in self.documents.values().enumerate() {
                    consume(ordinal as u64, document)?;
                    if !late && ordinal == 29 {
                        return Err(SkeinError::Storage(
                            "injected fuzz scan failure".to_string(),
                        ));
                    }
                }
                Ok(())
            },
            &self.analyzer,
        );
        assert!(result.is_err());
        assert_eq!(fs::read(self.root.join(MANIFEST_FILE)).unwrap(), manifest);
        assert!(!self.root.join(artifact_file(generation)).exists());
        assert!(fs::read_dir(&self.root).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with("tmp")));
        self.reopen();
    }

    fn rejected_operations(&mut self) {
        let terms = BTreeSet::from(["graph".to_string()]);
        let context = RuntimeTaskContext::default();
        context.cancellation().cancel();
        assert!(self
            .reader
            .score_with_context(&terms, &self.delta, None, Some(&context), |_| panic!(
                "cancelled query visited a candidate"
            ))
            .is_err());
        let context = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(1, 1));
        assert!(self
            .reader
            .score_with_context(&terms, &self.delta, None, Some(&context), |_| panic!(
                "unadmitted query visited a candidate"
            ))
            .is_err());
        let small_cache = Arc::new(SegmentCache::new(1));
        let reader = LexicalProjectionReader::load_named_with_cache(
            &self.root,
            MANIFEST_FILE,
            None,
            11,
            13,
            self.config,
            small_cache.clone(),
        )
        .unwrap()
        .unwrap();
        let error = reader
            .score(&terms, &self.delta, None, |_| Ok(true))
            .unwrap_err();
        assert!(error.to_string().contains("cache admission failed"));
        assert_eq!(small_cache.snapshot().pinned_bytes, 0);
        assert!(small_cache.snapshot().admission_rejection_count > 0);
        // Delta rejection must not leave statistics or source visibility changed.
        let config = LexicalProjectionConfig {
            mini_delta_bytes: NonZeroU64::MIN,
            ..self.config
        };
        assert!(self
            .delta
            .upsert(
                &document("rejected", "graph", "query"),
                None,
                &self.analyzer,
                config
            )
            .is_err());
        self.cancelled_build();
    }

    fn cancelled_build(&self) {
        let manifest = fs::read(self.root.join(MANIFEST_FILE)).unwrap();
        let next = self.reader.generation() + 1;
        let task = RuntimeTaskContext::default();
        let stop = self.reader.generation() as usize % 16;
        let result = LexicalProjectionWriter::new(self.config)
            .with_context(task.clone())
            .write_scanned(
                &self.root,
                next,
                None,
                11,
                13,
                |consume| {
                    for (ordinal, document) in self.documents.values().enumerate() {
                        consume(ordinal as u64, document)?;
                        if ordinal == stop {
                            task.cancellation().cancel();
                            break;
                        }
                    }
                    Ok(())
                },
                &self.analyzer,
            );
        let error = result.unwrap_err();
        assert!(matches!(error, SkeinError::Execution(_)), "{error}");
        assert!(error.to_string().contains("cancelled"), "{error}");
        assert_eq!(fs::read(self.root.join(MANIFEST_FILE)).unwrap(), manifest);
        assert!(!self.root.join(artifact_file(next)).exists());
        assert!(fs::read_dir(&self.root).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with("tmp")));
    }

    fn check(&self, random: &mut Random, step: usize) {
        let terms = BTreeSet::from([
            "graph".to_string(),
            TERMS[random.index(TERMS.len())].to_string(),
            "not_in_corpus".to_string(),
        ]);
        let modulus = 2 + random.index(5);
        let excluded = self
            .documents
            .keys()
            .enumerate()
            .filter(|(index, _)| index % modulus == 0)
            .map(|(_, id)| id.clone())
            .collect::<BTreeSet<_>>();
        let expected = reference_scores(&self.documents, &self.analyzer, &terms, &excluded);
        let limit = [0, 1, 7, 128, 512][random.index(5)];
        for retained in [None, Some(limit)] {
            let report = self
                .reader
                .score(&terms, &self.delta, retained, |id| {
                    Ok(!excluded.contains(id))
                })
                .unwrap_or_else(|error| panic!("seed={} step={step}: {error}", self.seed));
            assert_eq!(
                report.matching_document_count,
                expected.len(),
                "seed={} step={step}",
                self.seed
            );
            let expected = expected
                .iter()
                .take(retained.unwrap_or(expected.len()))
                .cloned()
                .collect::<BTreeMap<_, _>>();
            assert_eq!(
                report.scores.keys().collect::<Vec<_>>(),
                expected.keys().collect::<Vec<_>>(),
                "seed={} step={step}",
                self.seed
            );
            for (id, expected) in expected {
                assert!(
                    (report.scores[&id] - expected).abs() <= 1e-12 * expected.max(1.0),
                    "seed={} step={step} id={id}",
                    self.seed
                );
            }
            assert_eq!(
                report.bytes_read,
                report.document_bytes_read
                    + report.dictionary_bytes_read
                    + report.posting_bytes_read
            );
            assert_eq!(self.cache.snapshot().pinned_bytes, 0);
            assert!(self.cache.snapshot().resident_bytes <= self.cache.snapshot().capacity_bytes);
        }
    }

    fn finish(self) {
        drop(self.reader);
        assert_eq!(self.cache.snapshot().pinned_bytes, 0);
        fs::remove_dir_all(self.root).unwrap();
    }
}

#[test]
#[ignore = "local production-reader campaign; run the explicit Bazel fuzz suite"]
fn projection_state_machine_campaign() {
    for seed in [206, 18, 0x5eed] {
        let mut model = Model::new(seed);
        let mut random = Random(seed);
        let mut actions = [0; 11];
        model.check(&mut random, 0);
        for cycle in 0..3 {
            let id = format!("memory:{:04}", random.index(257));
            let inserted = format!("inserted:{cycle:04}");
            // Directed cycles ensure every transition is non-vacuous. Payloads,
            // affected keys, queries, filters and rank windows remain seeded.
            for (action, count) in actions.iter_mut().enumerate() {
                match action {
                    0 | 1 | 4 => model.upsert(random_document(&mut random, &id)),
                    2 | 3 => model.delete(&id),
                    5 => model.upsert(random_document(&mut random, &inserted)),
                    6 => model.delete(&inserted),
                    7 => model.reopen(),
                    8 => model.publish(),
                    9 => model.failed_publication(cycle % 2 == 0),
                    _ => model.rejected_operations(),
                }
                *count += 1;
                model.check(&mut random, 1 + cycle * 11 + action);
            }
        }
        assert_eq!(actions, [3; 11]);
        let mut random_actions = [0; 7];
        for step in 0..64 {
            let id = format!("memory:{:04}", random.index(320));
            let action = random.index(random_actions.len());
            match action {
                0 | 1 => model.upsert(random_document(&mut random, &id)),
                2 => model.delete(&id),
                3 => model.reopen(),
                4 => model.publish(),
                5 => model.failed_publication(step % 2 == 0),
                _ => model.rejected_operations(),
            }
            random_actions[action] += 1;
            model.check(&mut random, 34 + step);
        }
        assert!(random_actions.iter().all(|&count| count > 0));
        eprintln!("projection state seed={seed}: directed={actions:?}, random={random_actions:?}, queries=196");
        model.finish();
    }
}
