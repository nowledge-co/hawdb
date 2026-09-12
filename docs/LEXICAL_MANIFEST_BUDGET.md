# Lexical manifest byte admission

The host can select the encoded lexical manifest budget through the embedded
`SearchOutOfCoreGenerationWriter`. Existing constructors and option-struct
literals retain their behavior; the default remains 256 MiB.

```rust
pub fn max_lexical_manifest_bytes(&self) -> NonZeroU64;
pub fn set_max_lexical_manifest_bytes(&mut self, max_bytes: NonZeroU64) -> Result<()>;
```

The setter accepts nonzero limits up to the local `isize::MAX`. A rejected value
leaves the previous selection intact. It can be called before or after `push`;
the last value selected before `finish` applies to the complete staged corpus.
The private encoder checks the exact V1 envelope size before reserving output,
and the internal post-build reopen uses the same budget. A failed build drops
the stage and leaves the active generation unchanged.

`SearchOutOfCoreConfig::max_lexical_manifest_bytes` remains the reader control.
Both the bounded artifact read and private lexical loader enforce that cap on
the captured bytes. A generous writer cap does not require an equally generous
reader cap if the actual manifest is small. The artifact never raises its
reader's admission limit.

`prepare_delta` copies the source reader's cap into its private writer, alongside
the term policy. Reopen the source reader with a different configuration to
select a different update cap. A reader may accept an arbitrarily large `u64`
ceiling for a small artifact, but preparing a writer from a ceiling above local
`isize::MAX` fails and cleans up the stage. The reader configuration alone does
not allocate its entire budget.

Generation-number recovery after active-manifest corruption also observes the
selected cap. Malformed candidates do not supply a generation number. Budget or
I/O errors fail recovery instead of hiding existing generations and allowing
their numbers to be reused. Standalone, fully resident search retains its
default budget.

## Complete-corpus qualification

The owner approved this additive interface and a 512 MiB manifest cap on
September 13, 2026. The qualification harness must apply that cap to both writer
and reader, with the previously approved 1 MiB term policy:

```rust
use skein::{
    SearchLexicalTermPolicy, SearchOutOfCoreConfig,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
};
use std::num::NonZeroU64;

let manifest_budget = NonZeroU64::new(512 * 1024 * 1024).unwrap();
let term_policy = SearchLexicalTermPolicy::new(NonZeroU64::new(1024 * 1024).unwrap())?;
let mut writer = SearchOutOfCoreGenerationWriter::create_with_term_policy(
    &root, Default::default(), term_policy,
)?;
writer.set_max_lexical_manifest_bytes(manifest_budget)?;
// Push the complete source in ascending UTF-8 document ID order, then finish.
let report = writer.finish()?;
let reader = SearchOutOfCoreReader::open_with_term_policy(
    &root,
    SearchOutOfCoreConfig {
        max_lexical_manifest_bytes: manifest_budget,
        ..Default::default()
    },
    Default::default(),
    term_policy,
)?;
```

This is an encoded-byte budget, not a process-RSS ceiling. The decoded
dictionary/descriptors, source records, analyzer, blocks, spill and result
buffers retain separate admission or resident-memory obligations. The term and
document-source defaults, V1 format, and release policy are unchanged.

The API itself does not satisfy [#206](https://github.com/nowledge-co/skein/issues/206)
or [#325](https://github.com/nowledge-co/skein/issues/325). Their complete identical
334,844-document corpus, source hashes, actual posting/dictionary/mapping extents,
BM25/reopen parity, and original compression criterion still need qualification.
No filtered or truncated corpus substitutes for that evidence.

## Verification

Coverage includes setter rollback, exact/one-short publication and read bounds,
private captured-byte admission, internal reopen, prepared-update inheritance,
cancelled stages, pinned old readers, and corruption recovery without generation
reuse. A 64-case local lifecycle campaign varies content and exact byte limits.

```sh
cargo test --locked --offline -p skein-search --all-features
cargo test --locked --offline -p skein-search --all-features manifest_budget -- --include-ignored
bazel test //crates/search:presubmit_tests //:skein_unit_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The lifecycle fuzz target remains manual and local; no CI fuzz job is added.
