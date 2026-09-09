use super::*;
use std::sync::Barrier;
use std::time::Instant;

#[test]
#[ignore = "local release-mode multi-identity cached reader scaling comparison"]
fn sharded_cache_reader_benchmark() {
    const IDENTITIES: usize = 64;
    const READS_PER_THREAD: usize = 50_000;
    let config = StableIdentityMappingConfig {
        page_bytes: NonZeroUsize::new(16 * 1024).unwrap(),
        max_value_bytes: NonZeroUsize::new(512).unwrap(),
        ..StableIdentityMappingConfig::default()
    };
    let cache = Arc::new(SegmentCache::new((IDENTITIES * 16 * 1024) as u64));
    let mut readers = Vec::new();
    let mut paths = Vec::new();
    for index in 0..IDENTITIES {
        let path = test_path("sharded-cache-benchmark");
        let output =
            StableIdentityMappingWriter::publish(&path, 1, entries(&mapping(32)), config).unwrap();
        assert_eq!(output.header.page_count, 1);
        let reader = StableIdentityMappingReader::open_with_cache(
            &path,
            config,
            Arc::clone(&cache),
            StoreId(index as u128 + 1),
        )
        .unwrap();
        let (value, _) = reader
            .lookup(
                StableIdentityKey::node(17),
                StableIdentityReadLimits::default(),
            )
            .unwrap();
        assert_eq!(value, Some(Value::String("node-17".to_string())));
        readers.push(reader);
        paths.push(path);
    }
    assert_eq!(cache.snapshot().entry_count, IDENTITIES);
    for threads in [1, 2, 4, 8] {
        for distributed in [true, false] {
            let barrier = Barrier::new(threads + 1);
            let elapsed = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..threads)
                    .map(|worker| {
                        let readers = &readers;
                        let barrier = &barrier;
                        scope.spawn(move || {
                            let checks = crate::cache::PAGE_INTEGRITY_CHECKS.get();
                            barrier.wait();
                            for iteration in 0..READS_PER_THREAD {
                                let index = if distributed {
                                    (iteration * 17 + worker * 7) % IDENTITIES
                                } else {
                                    0
                                };
                                let (value, report) = readers[index]
                                    .lookup(
                                        StableIdentityKey::node(17),
                                        StableIdentityReadLimits::default(),
                                    )
                                    .unwrap();
                                assert_eq!(report.storage_bytes_read, 0);
                                assert_eq!(report.cache_hits, 1);
                                assert!(value.is_some());
                                std::hint::black_box(value);
                            }
                            assert_eq!(crate::cache::PAGE_INTEGRITY_CHECKS.get(), checks);
                        })
                    })
                    .collect();
                let started = Instant::now();
                barrier.wait();
                for handle in handles {
                    handle.join().unwrap();
                }
                started.elapsed()
            });
            eprintln!("sharded-cache-reader threads={threads} distributed={distributed} reads={} elapsed_ms={:.3} reads_per_second={:.0}", threads * READS_PER_THREAD, elapsed.as_secs_f64() * 1000.0, (threads * READS_PER_THREAD) as f64 / elapsed.as_secs_f64());
        }
    }
    assert_eq!(cache.snapshot().pinned_bytes, 0);
    drop(readers);
    for path in paths {
        remove_mapping_fixture(&path, &[1]);
    }
}
