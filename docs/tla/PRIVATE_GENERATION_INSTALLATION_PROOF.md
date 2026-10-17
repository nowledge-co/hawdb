# Dependency installation and component selection

This component separates two internal backend steps: installing a complete
immutable dependency closure and writing its selecting component manifest. It
exposes no public installed candidate or registered-consumer activation API.
The existing wrappers still execute both steps immediately under their caller's
publication exclusion. The [consumer candidate-directory design](CONSUMER_INSTALLED_ACTIVATION_DESIGN.md)
remains a proposal, including complete C/E mapping, governed reader construction,
root identity, small selector/receipt and persistent cleanup custody.

## Source argument

In [single-generation publication](../../crates/search/src/out_of_core/generation_writer/publication.rs),
`install_generation` performs the existing admission, checksum, metadata encoding,
link/copy, verification and dependency synchronization sequence. It returns the
same encoded manifest and publication report in `InstalledGeneration`.
`publish_generation` immediately consumes that value; its private `publish` writes
the same `OUT_OF_CORE_MANIFEST_FILE` through the existing `GenerationIo::write`
and returns the report only after success. The input artifact ownership and the
caller's target exclusion remain alive across both calls.

In [partitioned publication](../../crates/search/src/out_of_core/generation_writer/initial/install.rs),
`install` retains the existing preflight and first expected-head check before any
real-root dependency installation. `InstalledInitial` owns the encoded manifest,
retained-generation sets and their memory lease, and the completed report values.
Its private `publish` performs the second expected-head check immediately before
the unchanged head write. Report fields change only after that write succeeds;
the existing `publish` wrapper immediately composes these two steps. No expected
base, encoded format, dependency barrier or default durability policy changes.

The extraction adds no heap allocation or I/O operation. It can release temporary
decoded metadata before the head write where the former combined function kept
it until return; it does not promise identical allocation peaks or cancellation
scheduling. That disposal still occurs under the existing caller's ownership.
It does not establish a short source fence or implement the activation model's
NoHeavyUnderFence premise. No native source/target validation-to-selection fence
is shortened by this change.

An installed value is deliberately not cleanup or publication authority. It
holds neither the actual root lease nor complete ownership of all real-root
aliases. Only the existing enclosing wrappers use it in production code. A later
consumer constructor must retain its actual operation/root authority, prepare
and govern its reader, and transfer candidate directory/resource ownership before
exposing a queued installed state. Generic registered-root rejection is retained.

A failed head write may have renamed the selector before directory synchronization
failed. The extraction does not add rollback cleanup: dependencies remain, as in
the existing path. An installed value must not gain a Drop implementation that
unconditionally deletes those files. Repeated rejected/aborted raw installation
can also leave future-generation aliases that ordinary cleanup conservatively
keeps. Bounded directory cleanup, uncertain-outcome retention, post-crash debt
admission and committed view/receipt roots remain native implementation work.
The finite activation/receipt models assume those capabilities; they do not prove
this backend split supplies them.

## Regression and qualification

The new native boundary regression installs actual partition dependencies while
asserting the old selector bytes and old view remain unchanged. It then exercises
three outcomes: successful explicit selection, cancellation after installation
without changing the old selector, and a real competing publisher whose winning
head cannot be overwritten by stale selection. The fixture removes its temporary
root explicitly; it is not evidence for native cancellation cleanup custody.

On fixed runtime/test/model tree `94d9677ba46ded3fd1ece162c89dcb4d446d4d66`,
the complete vector Search library passed 882 tests, zero failures and 20 existing
ignored tests (369.99 seconds). The complete Bazel Search target also passed
882 tests, zero failures and 20 ignored (412.18 seconds test body, 413.1 seconds
reported target). Focused prepared, initial and incremental-ownership selections
passed 12, nine and six tests. The installed-boundary regression passed all three
scenarios in one test. Strict locked workspace/all-target/all-feature Clippy passed.

The facade's existing Search power-loss selection ran with `--features test-support`
and passed all 11 tests, zero failures, 2,034 filtered (744.47 seconds), including
incremental, partitioned, compaction, acknowledgement, ancestry and applicable
symlink cases. It exercises generic component publication across bounded lost,
torn and reordered writes; it does not qualify the unimplemented consumer selector
or full source WAL/catalog/registry/GC composition. The first invocation omitted
the feature and selected zero tests; it is retained and excluded from coverage.
These concurrent-run timings are observations, not benchmark qualification.

The three mandatory unfiltered local fuzz suites passed 98/98 targets, 33
executed and 65 cached (1,617.268 seconds wall, 354.46 seconds critical path).
All 294 log/XML/cache-status files are retained; all XML reports were checked
for failures/errors. Complete tree comparison verified that final primary
runtime/test/model bytes match the fixed qualified tree, with differences only
in four Markdown design/proof files.

The primary minimal installed-boundary test passed all three outcomes in one
test, with 723 filtered (3.20 seconds). Required WASM library/portable-test and
playground-example strict Clippy passed with documented Clang/pinned LLVM ar.
No browser run is claimed. Primary formatting, strict locked workspace Clippy
and all-file prek passed; installed commit-hook outcomes are retained with the
publication receipt. Runs used pinned Rust 1.97.1 on macOS arm64.

A separate uncommitted layout probe used the actual library artifact from that
fixed tree: private C construction and an E update preserved every original D
root file and receipt, retained native root exclusion after consumer closure,
and reopened D after preparation-token release. Synthetic source epochs,
ordinary ungoverned readers and explicit fixture removal limit that result to
layout feasibility; it supplies no native source closure, activation or cleanup
custody. Its initial associated-function call and reserved metadata expectation
were corrected while retaining full equality assertions and adverse logs.

Native C-ready/E activation, governed reader residency, actual Mem adoption and
all original physical-resource and writer/fairness/group-fsync gates remain open.
