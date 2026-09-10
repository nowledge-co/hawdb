use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Barrier;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    reader: Arc<LexicalProjectionReader>,
    artifact: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "skein-lexical-positioned-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&root).unwrap();
        let documents = documents();
        let config = LexicalProjectionConfig {
            target_block_bytes: NonZeroU64::new(128).unwrap(),
            max_block_bytes: NonZeroU64::new(4096).unwrap(),
            ..Default::default()
        };
        let reader = LexicalProjectionWriter::new(config)
            .write(
                &root,
                1,
                None,
                11,
                13,
                documents.iter(),
                &SearchAnalyzerLexicon::default(),
            )
            .unwrap();
        let artifact = fs::read(root.join(&reader.manifest.artifact_file)).unwrap();
        assert!(reader.manifest.blocks.len() > 16);
        Self {
            root,
            reader,
            artifact,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn documents() -> Vec<SearchDocument> {
    (0..32)
        .map(|index| SearchDocument {
            id: format!("document:{index:04}"),
            title: format!("graph entry {index}"),
            content: format!(
                "{} token{index} \u{e9} \u{4e2d}\u{6587}",
                [
                    "alpha storage",
                    "beta memory",
                    "gamma retrieval",
                    "delta cache"
                ][index % 4]
            ),
            embedding: None,
            metadata: BTreeMap::from([("space".to_string(), format!("group{}", index % 3))]),
        })
        .collect()
}

#[test]
#[cfg(unix)]
fn lexical_block_reads_do_not_mutate_the_shared_file_cursor() {
    let fixture = Fixture::new();
    let mut observer = fixture.reader.file.try_clone().unwrap();
    for block in &fixture.reader.manifest.blocks {
        observer.seek(SeekFrom::Start(7)).unwrap();
        let bytes = fixture.reader.read_block(block).unwrap();
        assert_eq!(
            bytes,
            fixture.artifact[block.offset as usize..(block.offset + block.length) as usize]
        );
        assert_eq!(
            observer.stream_position().unwrap(),
            7,
            "block {} changed the shared cursor",
            block.block_id
        );
    }
}

fn concurrent_blocks(iterations: usize, workers: usize) {
    let fixture = Fixture::new();
    let barrier = Barrier::new(workers);
    std::thread::scope(|scope| {
        let fixture = &fixture;
        let barrier = &barrier;
        let handles = (0..workers)
            .map(|worker| {
                scope.spawn(move || {
                    barrier.wait();
                    let mut state = 0x392_u64 + worker as u64;
                    for iteration in 0..iterations {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        let block = &fixture.reader.manifest.blocks
                            [state as usize % fixture.reader.manifest.blocks.len()];
                        let actual = fixture.reader.read_block(block).unwrap_or_else(|error| {
                            panic!(
                                "worker {worker}, iteration {iteration}, block {}: {error}",
                                block.block_id
                            )
                        });
                        assert_eq!(
                            actual,
                            fixture.artifact
                                [block.offset as usize..(block.offset + block.length) as usize]
                        );
                        std::thread::yield_now();
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }
    });
}

#[test]
fn concurrent_lexical_block_reads_match_independent_artifact_slices() {
    concurrent_blocks(128, 8);
}

#[test]
fn lexical_positioned_reads_keep_admission_and_integrity_checks() {
    let fixture = Fixture::new();
    let mut block = fixture.reader.manifest.blocks.last().unwrap().clone();
    block.checksum ^= 1;
    assert!(fixture
        .reader
        .read_block(&block)
        .unwrap_err()
        .to_string()
        .contains("checksum mismatch"));
    block.length = fixture.reader.config.max_block_bytes.get() + 1;
    assert!(fixture
        .reader
        .read_block(&block)
        .unwrap_err()
        .to_string()
        .contains("read budget"));
    block = fixture.reader.manifest.blocks.last().unwrap().clone();
    block.offset = u64::MAX;
    assert!(fixture.reader.read_block(&block).is_err());
    block = fixture.reader.manifest.blocks.last().unwrap().clone();
    let file = fs::OpenOptions::new()
        .write(true)
        .open(fixture.root.join(&fixture.reader.manifest.artifact_file))
        .unwrap();
    file.set_len(block.offset + block.length - 1).unwrap();
    assert!(fixture.reader.read_block(&block).is_err());
}

#[test]
#[ignore = "explicit local concurrent lexical read campaign"]
fn concurrent_lexical_read_differential_campaign() {
    concurrent_blocks(2048, 12);
}

#[test]
#[cfg(feature = "full-text-search")]
fn public_concurrent_queries_preserve_results_while_a_new_generation_publishes() {
    use crate::{
        SearchIndex, SearchMode, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
        SearchQueryOptions,
    };

    let root = std::env::temp_dir().join(format!(
        "skein-lexical-public-concurrency-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let sources = documents();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for document in &sources {
        writer.push(document.clone()).unwrap();
    }
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let queries = [
        "graph",
        "alpha",
        "beta",
        "gamma",
        "delta",
        "storage memory",
        "cache retrieval",
    ];
    let options = SearchQueryOptions {
        limit: 32,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    let expected = queries
        .iter()
        .map(|query| {
            reader
                .search_with_options(query, None, SearchMode::Text, options.clone())
                .unwrap()
                .result
        })
        .collect::<Vec<_>>();
    let mut resident = SearchIndex::default();
    for document in &sources {
        resident.upsert(document.clone()).unwrap();
    }
    for (query, expected) in queries.iter().zip(&expected) {
        let reference =
            resident.search_with_options(query, None, SearchMode::Text, options.clone());
        assert_eq!(expected.total_hits, reference.total_hits);
        assert_eq!(
            expected
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_score))
                .collect::<Vec<_>>(),
            reference
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_score))
                .collect::<Vec<_>>()
        );
    }
    let barrier = Barrier::new(9);
    std::thread::scope(|scope| {
        let handles = (0..8)
            .map(|worker| {
                let reader = &reader;
                let barrier = &barrier;
                let expected = &expected;
                let options = &options;
                scope.spawn(move || {
                    barrier.wait();
                    for iteration in 0..32 {
                        let index = (worker + iteration) % queries.len();
                        let actual = reader
                            .search_with_options(
                                queries[index],
                                None,
                                SearchMode::Text,
                                options.clone(),
                            )
                            .unwrap();
                        assert_eq!(actual.result, expected[index]);
                    }
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        for mut document in sources {
            document.content = "replacement generation".to_string();
            writer.push(document).unwrap();
        }
        writer.finish().unwrap();
        for handle in handles {
            handle.join().unwrap();
        }
    });
    let current = SearchOutOfCoreReader::open(&root).unwrap();
    assert!(current.generation() > reader.generation());
    assert_eq!(
        current
            .search_with_options("alpha", None, SearchMode::Text, options.clone())
            .unwrap()
            .result
            .total_hits,
        0
    );
    assert_eq!(
        reader
            .search_with_options("alpha", None, SearchMode::Text, options)
            .unwrap()
            .result,
        expected[1]
    );
    drop(current);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
