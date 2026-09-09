use super::*;
use crate::cache::PAGE_INTEGRITY_CHECKS;
use crate::{
    content_digest, ManifestGeneration, RepresentationKind, SegmentCache, SegmentCacheKey, StoreId,
};
use std::sync::Arc;

fn id(value: u64) -> IndexPageId {
    IndexPageId::new(NonZeroU64::new(value).unwrap())
}

fn pages(seed: u64) -> [ImmutableIndexPage; 4] {
    let width = 2 + seed as usize % 127;
    let rows = vec![
        IndexRowId::new(vec![1; width]),
        IndexRowId::new(vec![2; width]),
    ];
    [
        ImmutableIndexPageBody::Root(IndexRootPage {
            identity: IndexIdentity {
                namespace: "documents".into(),
                name: format!("index-{seed}"),
            },
            schema_digest: skein_integrity::integrity_digest(&seed.to_le_bytes()).sha256,
            child: id(2),
            height: 1,
        }),
        ImmutableIndexPageBody::Interior(IndexInteriorPage {
            entries: vec![
                IndexInteriorEntry {
                    upper_bound: vec![1; width],
                    child: id(2),
                },
                IndexInteriorEntry {
                    upper_bound: vec![2; width],
                    child: id(3),
                },
            ],
        }),
        ImmutableIndexPageBody::Leaf(IndexLeafPage {
            entries: vec![IndexLeafEntry {
                key: vec![1; width],
                posting: IndexLeafPosting::Inline(rows.clone()),
            }],
        }),
        ImmutableIndexPageBody::Posting(IndexPostingPage {
            next: None,
            row_ids: rows,
        }),
    ]
    .map(|body| ImmutableIndexPage {
        generation: seed.max(1),
        source_commit_epoch: seed,
        page_id: id(1),
        body,
    })
}

fn cache_key(slot: &[u8], generation: u64) -> SegmentCacheKey {
    SegmentCacheKey {
        store_id: StoreId(1),
        manifest_generation: ManifestGeneration(generation),
        segment_id: 1,
        content_digest: content_digest(slot),
        representation: RepresentationKind::RelationalIndexPageSlot,
    }
}

fn check_seed(seed: u64) {
    let limits = ImmutableIndexPageLimits {
        max_page_bytes: NonZeroUsize::new(1024).unwrap(),
        ..ImmutableIndexPageLimits::default()
    };
    for page in pages(seed) {
        let slot: Arc<[u8]> = page.encode_slot(limits).unwrap().into();
        let key = cache_key(&slot, page.generation);
        let cache = SegmentCache::new(1024);
        let raw = cache.insert(key, Arc::clone(&slot)).unwrap();
        assert!(!raw.page_integrity_verified());
        let before = PAGE_INTEGRITY_CHECKS.get();
        assert_eq!(
            ImmutableIndexPage::decode_cached_slot(&raw, limits).unwrap(),
            page
        );
        assert_eq!(PAGE_INTEGRITY_CHECKS.get(), before + 1);
        drop(raw);
        let verified = cache.insert_page_verified(key, Arc::clone(&slot)).unwrap();
        for reader_limits in [
            limits,
            ImmutableIndexPageLimits {
                max_page_bytes: NonZeroUsize::new(1023).unwrap(),
                ..limits
            },
            ImmutableIndexPageLimits {
                max_entries: NonZeroUsize::new(1).unwrap(),
                ..limits
            },
            ImmutableIndexPageLimits {
                max_key_bytes: NonZeroUsize::new(1).unwrap(),
                ..limits
            },
            ImmutableIndexPageLimits {
                max_row_id_bytes: NonZeroUsize::new(1).unwrap(),
                ..limits
            },
            ImmutableIndexPageLimits {
                max_identity_bytes: NonZeroUsize::new(1).unwrap(),
                ..limits
            },
            ImmutableIndexPageLimits {
                max_inline_postings: NonZeroUsize::new(1).unwrap(),
                ..limits
            },
        ] {
            let expected = ImmutableIndexPage::decode_slot(&slot, reader_limits);
            let before = PAGE_INTEGRITY_CHECKS.get();
            assert_eq!(
                ImmutableIndexPage::decode_cached_slot(&verified, reader_limits),
                expected
            );
            assert_eq!(PAGE_INTEGRITY_CHECKS.get(), before);
        }

        let payload_end = PAGE_HEADER_BYTES + read_u64(&slot[40..48]) as usize;
        for offset in [48, 52, payload_end - 1, slot.len() - 1] {
            let mut corrupt = slot.to_vec();
            corrupt[offset] ^= 1 << (seed % 8);
            if offset == payload_end - 1 {
                // Recomputing CRC alone must never authorize a forged payload.
                let mut hasher = IntegrityHasher::new();
                hasher.update(&corrupt[..48]);
                hasher.update(&corrupt[PAGE_HEADER_BYTES..payload_end]);
                corrupt[48..52].copy_from_slice(&hasher.finish().crc32c.get().to_le_bytes());
            }
            let expected = ImmutableIndexPage::decode_slot(&corrupt, limits);
            assert!(expected.is_err());
            let poison_cache = SegmentCache::new(1024);
            let raw = poison_cache
                .insert(cache_key(&corrupt, page.generation), corrupt)
                .unwrap();
            assert_eq!(
                ImmutableIndexPage::decode_cached_slot(&raw, limits),
                expected
            );
        }

        let exported = verified.into_arc();
        drop(slot);
        assert_eq!(cache.snapshot().pinned_bytes, 1024);
        drop(exported);
        assert_eq!(cache.snapshot().pinned_bytes, 0);
        let replacement = vec![0; 1024];
        drop(
            cache
                .insert(
                    SegmentCacheKey {
                        segment_id: 2,
                        ..cache_key(&replacement, page.generation)
                    },
                    replacement,
                )
                .unwrap(),
        );
        assert!(cache.get(&key).is_none());
        let fresh_slot = page.encode_slot(limits).unwrap();
        let raw = cache.insert(key, fresh_slot).unwrap();
        assert!(!raw.page_integrity_verified());
        let before = PAGE_INTEGRITY_CHECKS.get();
        assert_eq!(
            ImmutableIndexPage::decode_cached_slot(&raw, limits).unwrap(),
            page
        );
        assert_eq!(PAGE_INTEGRITY_CHECKS.get(), before + 1);
    }
}

#[test]
fn verified_page_cache_matches_full_decode_and_reader_limits() {
    for seed in [0, 1, 7, 126, 127, 128, u64::MAX] {
        check_seed(seed);
    }
}

#[test]
#[ignore = "local deterministic cache provenance and codec differential campaign"]
fn verified_page_cache_differential_campaign() {
    let mut seed = 0x196_cace_f00d_u64;
    for _ in 0..1024 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        check_seed(seed);
    }
    eprintln!("verified page cache campaign: 4096 generated pages, seven reader policies, four corruption modes, promotion and eviction");
}
