# Checkpoint producer inode sharing

Issue: https://github.com/nowledge-co/hawdb/issues/1385.

The manual `examples/checkpoint_inode_sharing.rs` probe compares unchanged main
`c5e7dea8466cd3b9eab7252d8e124d3368bb85ae` with this change on macOS arm64,
Rust 1.99.1, locked dependencies and the repository release profile. Both runs
use new scratch databases: 1,000 `Note` nodes, integer IDs, short names and
256-byte bodies. They checkpoint twice, add one node, checkpoint again and
reopen. Both reopen checks return exactly 1,001 nodes.

| State | Logical path bytes, both | Main unique inode bytes | Shared unique inode bytes | Reduction |
|---|---:|---:|---:|---:|
| First checkpoint | 695,603 | 690,977 | 353,986 | 48.77% |
| Second, unchanged checkpoint | 1,361,808 | 1,361,416 | 692,058 | 49.17% |
| One added node, third checkpoint | 1,362,485 | 1,362,093 | 692,634 | 49.15% |
| Reopened | 1,362,485 | 1,029,627 | 692,634 | 32.73% |

Main's reopen already remounts some current aliases. The improvement after
reopen is therefore smaller than immediately after publication. The new path
also avoids the initial staging payload copy; the power-loss recorder guard
asserts that sealed publication issues no staging payload writes.

Logical bytes sum every regular-file path. Unique inode bytes deduplicate
`(st_dev, st_ino)` before summing file lengths. On Unix the probe separately
reports `st_blocks * 512`, deduplicated by the same identity: after the second
checkpoint those sums were 1,822,720 and 950,272 bytes. They are filesystem
accounting, not free-space deltas or measurements of shared APFS extents.
Non-Unix inode/allocation metrics are `null`. This fixture has no relationships
and does not qualify the reported 1.7 GB LDBC workload or checkpoint latency.

Reproduce each revision with a distinct, nonexistent destination:

```sh
cargo run --locked --release --example checkpoint_inode_sharing -- "$PROBE_ROOT"
```

Use the identical probe source for the main baseline. The implementation
changes publication ownership only; file bytes, names, object references and
persistent-format version remain unchanged. Existing readers and current/previous
generation retention remain intact. Cross-device/unsupported sharing retains
a verified independent copy. Public caller-owned sources keep copying.
