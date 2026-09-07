# Publication-lock ownership

The generation writer acquires its private publication lease with the existing
`BuildMemory` and task context. Canonical root storage, the lock-file path and
the in-process registry node share that build root; they do not create a second
independent admission limit. The legacy resident checkpoint wrapper still has no
operation-wide `BuildMemory` contract and is not evidence of resident checkpoint
admission.

## Native paths

The canonical path remains the in-process identity. Lexical normalization alone
would not preserve symlink aliases. Physical directory IDs would introduce a
different platform contract, so this change keeps the pinned standard library's
native resolution calls in a small private adapter:

- Unix reserves the NUL-terminated Rust input before constructing it, then calls
  `realpath` with a null output. A private RAII owner frees the libc result on
  every exit. After measuring that native result, the adapter reserves the Rust
  output before copying it. No fixed `PATH_MAX` bound or lossy UTF-8 conversion
  replaces native semantics.
- Windows opens the directory with access mode zero and backup semantics, then
  calls `GetFinalPathNameByHandleW` with the same DOS-name mode as `canonicalize`.
  The first buffer is a 512-unit stack array. A larger result reserves each wide
  allocation before construction, retaining the old allocation's lease until
  replacement. The output conversion reserves `12 * max(utf16_units, 8)` bytes
  for the pinned `OsString::from_wide` growth and old/new allocation overlap.
  Actual retained capacity remains charged with the resulting `PathBuf`.

The Win32 API reports successful lengths without the terminating NUL and required
buffer lengths with it. See the [native API contract](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfinalpathnamebyhandlew).
An intervening rename can change the required size; retries retain both old and
replacement wide buffers and check cancellation. The pinned Rust 1.97.1 sources
for `sys/fs/{unix,windows}.rs`, `alloc::wtf8` and native path joins qualify these
requested-capacity bounds.

This is **not** allocator/RSS or whole-filesystem accounting. The libc resolver's
own temporary allocation and std's native open-path conversion remain outside
this Rust-owned capacity ledger. OS calls themselves are not interruptible.
The budgeted adapter targets Unix and Windows; other targets return an explicit
unsupported error. The direct platform dependencies reuse the already locked
`libc 0.2.186` and `windows-sys 0.61.2`, without dependency version upgrades.

## Registry and rollback

The process registry uses individually owned linked-list nodes rather than a
`HashSet` with retained shared capacity. The canonical path moves into its node
without cloning. Each node preadmits its value and two pointer slots, matching
the pinned standard-library node layout. Lookup and removal are linear in the
number of active publishers; this is outside the query hot path.

A registration guard stores only a checked monotonic ID. Removal extracts the
matching node without allocating, then drops its path and leases. Unrelated
publishers retain their own originating build roots. Empty registry state holds
no per-publisher allocation or spare array capacity.

Acquisition prepares both paths before insertion and checks cancellation before
opening the sticky lock file. Open errors, failed OS locking and cancellation
after OS acquisition all close any open handle and remove registration. Success
keeps both the OS lock and registration until the publication lease drops; the
file handle closes before in-process registration is removed. The sticky lock
file is neither truncated nor deleted. The existing publication commit gate,
manifest-last ordering and v1 bytes are unchanged.

## Verification

Normal regressions cover native path parity, exact/one-short output and node
budgets, a live competing owner, charge lifetime after the `BuildMemory` handle
drops, independent publishers, aliases, cancellation before open and after OS
locking, open failure, and retained lock-file contents. A subprocess proves OS
mutual exclusion independently of the process registry and reacquisition after
drop. Its ignored fixture is invoked only by the parent regression.

A generation integration test verifies that the acquired publisher remains
charged to the writer's real root through downstream admission failure. Every
old published artifact and the reopened generation remain unchanged; staging,
registry state and build charges are released.

The manual campaign uses seed `0x20610cc`: 128 generated nested native paths,
including Unicode and paths beyond the initial Windows wide-buffer size. Each
has exact and one-short budgets with a 137-byte competing owner, an alias
contender, cancellation before open and after locking, full charge release,
reacquisition and unchanged sticky contents. It is not added to CI.

Negative controls must compile and fail the matching assertion for a separate
writer budget root, missing canonical-output admission, missing node admission,
missing in-process registration, missing OS locking and missing rollback. Restore
the exact source hash before final positive verification.

```bash
cargo test -p skein-search --all-features publish
cargo test -p skein-search --no-default-features build_memory::path::canonical
cargo test -p skein-search --all-features publisher_admission_campaign -- --ignored --nocapture
cargo test -p skein-search --no-default-features publisher_admission_campaign -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/search:skein_search_publisher_admission_fuzz_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Local macOS execution and Windows/Linux type checking are separate evidence from
native platform execution. Returned public cleanup/output/reader ownership,
resident delta maps, combined component limits, tokenizer/Jieba workspaces and
representative-corpus qualification remain full-issue gates.
