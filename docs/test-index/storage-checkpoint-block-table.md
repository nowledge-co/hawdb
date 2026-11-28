# Checkpoint block table codec (Phase 1B) (#2020)

Owner: `checkpoint_block_table::tests`
Lane: `//crates/storage:hawdb_storage_tests` (existing storage unit lane)
Replay: `cargo test --locked -p hawdb-storage --lib checkpoint_block_table`

The block table is the immutable ordered list of content-addressed checkpoint
blocks that a future generation manifest will bind. Tests cover empty and
multi-kind tables, strict kind/start/part ordering, contiguous parts,
non-overlapping buckets, inclusive `u64::MAX` endpoints, entry-count/encoded-byte
limit consistency, block-reference kind/version/length/digest bounds,
spill-namespace binding and zero-namespace rejection, table object identity,
corrupt magic/version/reserved/count/truncation/trailing bytes/digest, and the
decode path with a re-signed invalid block length. The codec is not connected to checkpoint publication or manifest
selection yet; Phase 1B read/write integration remains in #2020.
