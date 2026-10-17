# Consumer mutation custody

This component protects the existing native consumer owner while the C/E
activation protocol remains under development. It does not certify closed
source mappings, logical incarnation continuity, durable operation receipts,
Mem adoption or post-crash installed-candidate cleanup.

## Runtime argument

`consumer/operation.rs` stores one atomic word in the existing authority Arc:
47 generation bits, one admission-open bit and 16 reference bits. An idle
acquisition advances the generation with checked arithmetic and owns one scope
reference. Preparation leaves admission closed. Apply and checkpoint open
admission and stamp the index with that generation. Every sensitive method
retains a non-cloneable `MutationPermit` for its entire invocation. Admission
checks generation, open state and reference capacity in one atomic update.

Each index also carries a private Weak association with its original authority.
Moving an entire index moves that association. Owner operations check pointer
identity before mutation, checkpoint identity changes or directory publication.
Numeric generations from different owners are never interchangeable. This
association is runtime provenance, not a persisted logical incarnation.

Scope destruction atomically closes admission and removes only the scope
reference. It revokes the original authority even when safe Rust callbacks swap
the index occupying the borrowed slot. An already admitted method retains a
strong Arc and a reference until completion. Thus the next operation remains
busy and the native lease remains live after scope/owner closure. New operations
cannot overlap those references; old stamps cannot revive in a later generation.
Generation and reference exhaustion refuse rather than wrap. There is no extra
per-operation allocation or independently configured resource governor.

## Inductive representation argument

For each live Authority, let g be its checked generation, S the number of live
operation scopes (0 or 1), A the admitted mutation permits and R the encoded
reference count. The representation invariant is R = S + |A|, OPEN implies S=1,
and every member of A carries g and a strong reference to that same Authority.
This argument concerns accepted transitions, not scheduling fairness.

Initially S=R=0 and A is empty. Acquisition requires R=0 and OPEN=0 and performs
checked g+STEP and R=1 in one successful atomic update. Conservation therefore
implies that no previous permit survives a generation advance. Preparation
never sets OPEN. Mutation opening sets OPEN while the unique scope reference
exists. Admission atomically checks OPEN, matching g, nonzero R and remaining
reference capacity, then adds exactly one R and one owned permit. Closing a
scope atomically clears OPEN and subtracts exactly one R; it does not release
references belonging to admitted methods. Completing a permit subtracts one R
and removes exactly that permit. Failed/exhausted updates change no state.

The old and new generation are separated by an idle state with no permits.
Generations do not wrap: checked addition refuses at the real 47-bit boundary.
Reference admission refuses at 65,535 rather than carrying into OPEN/generation.
Drop uses the same generation and a positive count; the non-cloneable scope and
permit types discharge uniqueness of each decrement under Rust ownership.

A borrowed index selects its original Authority through a private Weak, never
through a Boolean in the borrowed slot. Whole-value moves preserve that Weak.
Owner methods compare it against their own Arc before accepting the index.
A retained Weak keeps its original allocation alive even after its strong count
falls to zero, so allocator address reuse cannot turn it into a newly created
runtime owner. This runtime allocation property does not supply durable target,
source/schema or host binding incarnations across rebind/reopen.

Weak upgrade transiently pins the native lease before admission; refusal drops
that pin. Accepted permits keep it until their method completes. Thus scope
closure and owner closure cannot release the native lease while admitted I/O
continues. A competing conforming native owner cannot acquire until all strong
pins disappear. Weak-upgrade transients can delay acquisition, not authorize an
unprotected mutation. This relies on the native lease platform assumptions and
has no fairness or bounded-wait conclusion.

The finite TLC model checks this representation for two epochs/two permits and
independent defective transitions; it is not an unbounded machine-checked proof
of Rust. Exact integer exhaustion and native lifetime/move/race tests supply
additional evidence. Directory relocation and logical incarnation continuity
remain separate proof obligations and must be composed before full C/E activation.

## Finite model and correspondence

[HawDBConsumerMutationCustody](HawDBConsumerMutationCustody.tla) explores two
generations and two admitted methods, including scope closure, an old retained
stamp, whole-index movement, owner destruction and a competing native owner.
Begin, Admit, Close and Finish abstract the linearization points of their
single-word atomic updates; native exclusion assumes conforming lease users.
Move conservatively allows a foreign index with a coincident numeric stamp.
The model's retained stamp represents an escaped value independently of the
currently borrowed slot. It does not model Rust aliasing or byte-level storage.

Bazel checked all 116 reachable states (433 generated, depth 12) for reference
conservation, revoked admission, owner association, absence of epoch overlap,
native exclusion and admission requiring a live scope. Five independently
configured negative controls produced the required counterexamples: ignoring
generation, permitting admission after closure, releasing busy state before
admitted work completes, omitting the native Arc pin, and skipping owner identity.
This is bounded exploration plus a source argument, not a mechanized refinement
proof of Rust or an unbounded temporal progress theorem. The real 47/16-bit
exhaustion boundaries are covered by native boundary tests and checked arithmetic,
rather than by making a small wrapping counter appear safe in the model.

Native tests additionally cover moved-index cleanup under a different live owner,
retained work after owner closure, rejection of checkpoint/apply/preparation while
work is outstanding, zero cleanup scans/deletes under that retained lease, stale
stamps across preparation and a later mutation, exact exhaustion boundaries and
64 racing admission/revocation attempts. The race test samples scheduling; the
finite atomic model explores both orders. Existing cancellation/unwind tests
remain applicable.

## Component qualification and limits

Runtime/test/model tree `9495dc1f8ef7c2d377b2752baf3e6422cbb41a24` remained fixed
through qualification on macOS arm64 with pinned Rust 1.97.1. The published
component adds Markdown source-correspondence/evidence updates afterward; no
runtime, test, model, configuration or build-rule bytes change.

- Native kernel consumer selection: 27 vector-profile tests and 26 minimal tests.
- Full Search Bazel library: 893 passed, zero failed, 20 existing ignored tests
  (235.50 seconds). Storage: 1,956 passed, zero failed, 30 existing ignored tests
  (799.74 seconds, exact-action cache). Both had zero filtered tests.
- Internal facade consumer lifecycle/preparation: 25 passed in both default
  (1,981 filtered, 205.26 seconds) and minimal (1,865 filtered, 128.88 seconds)
  profiles. External facade Bazel contracts passed three tests per profile.
- All 15 selected native/model Bazel targets passed. The two models and nine
  controls have retained module/configuration byte equality, pinned tool hash,
  exact TLC arguments and named invariant outcomes, including cache hits.
- Search component physical-loss/torn/reordering selection: 11 passed, zero
  failed, 2,034 filtered (544.51 seconds), with `test-support` enabled. These
  exercise generic Search publication; they do not prove the future consumer
  selector/C..E protocol or every platform/directory-relocation crash outcome.
- Required unfiltered local fuzz: 98/98 targets passed, 81 executed and 17 cached
  (835.064 seconds wall, 249.02 seconds critical path). All 98 XML files were
  checked; 294 log/XML/cache-status files are retained.
- Formatting, locked workspace/all-target/all-feature strict Clippy and all-file
  prek passed. Strict Storage/Search all-target/all-feature cross-checks passed
  for Linux GNU and Windows GNU; the required minimal WASM library/portable-test
  strict check passed with Clang and the pinned LLVM archiver. These cross-checks
  are compilation evidence; no Windows/Linux execution or browser run is claimed.

Relevant Cargo selections were `cargo test --locked -p hawdb-search --features
vector-search --lib consumer::`, its `--no-default-features` variant, `cargo test
--locked -p hawdb --lib api::search_projection_consumer::tests::`, the corresponding
minimal facade command, and `cargo test --locked -p hawdb --features test-support
--lib api::tests::power_loss::search_projection::`. Required local fuzz used
`bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests
//:hawdb_linux_ci_fuzz_smoke_test` without a test filter. Bazel resolver options
used a temporary public-crate cache with `CARGO_BAZEL_ISOLATED=false`, an explicit
scratch `CARGO_HOME` and `CARGO_NET_OFFLINE=true`; all archived crate checksums
matched the unchanged lockfiles. No host configuration/credentials were copied.

An earlier final-source Bazel attempt timed out during dependency fetching at
601 seconds (exit 48), before running tests. Resuming that failed check with the
verified cache preserved test selection, versions, features and assertions. Seven
already-passed final-source formatting/lint/target/minimal/prek records were
retained with verified log hashes. A preceding 840fbf2 runtime passed its native
matrix but was superseded by the directory-relocation fix; it did not qualify
this later source. The selected model evidence output group was explicitly
materialized: a cached marker alone had left an empty evidence directory.

The initial new native test incorrectly expected ProjectionBusy from native root
acquisition; acquisition actually reports Storage("another search projection
publication is active"). The test checks that exact refusal and zero scan/delete
behavior. The initial generation-negative model lacked a retained stale stamp;
its missing counterexample prompted explicit stamp retention. Those adverse runs
remain recorded, rather than being counted as successful qualification.

Full native C/E, actual Mem adoption, logical incarnation/ABA continuity, governed
read views, durable receipts/cleanup, composed physical/resource qualification and
the original writer/fairness/fsync performance gates remain open. These timings
are test observations, not benchmark qualification.

The ordinary-handle marker audit reproduced a second native bypass: an ordinary
SearchIndex opened before consumer registration could write a maintenance marker
while the registered consumer retained its native lock. The regression first
failed at that write. Marker I/O now retains either a consumer permit or a native
unregistered-directory lease, checking the durable binding under exclusion before
changing resident markers or files. Refusal is tested both while the consumer is
live and after it closes. Initialization establishes its private runtime owner
and scoped generation before invoking the initializer, then verifies owner/path
association before binding/checkpoint. Its marker methods reuse that actual lease;
ordinary and consumer-owned successful marker writes remain covered. The finite
model concerns scope/guard custody; these additional marker and initializer
filesystem paths are covered by source argument and native regressions.

Directory relocation now has a separate [source argument and native regression](CONSUMER_DIRECTORY_RELOCATION_PROOF.md):
a rename follows the actual native root before later fallible work, failed
publication revokes owner operations, and the uncertain destination is retained.
The combined source still needs complete qualification before publication.
