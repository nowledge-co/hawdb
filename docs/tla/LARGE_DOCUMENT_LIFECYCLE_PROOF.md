# Large-document lifecycle proof scope

This records the bounded protocol and the current build/read/mutation implementation for
[the large-document specification](../specs/LARGE_DOCUMENT_LIFECYCLE_SPEC.md),
tracked by [#392](https://github.com/nowledge-co/hawdb/issues/392).
The public `push_reader`, ordered mutation, candidate-result and verified-body
APIs are integrated. See the [delivery matrix](../STREAMED_DOCUMENT_LIFECYCLE.md)
for supported entrypoints, resource profiles and remaining gates. Source and
token defaults remain unchanged; these changes do not establish Mem readiness.

## Finite protocol

`HawDBLargeDocumentLifecycle.tla` contains one candidate, one competing
publication, one old reader, and a two-slot descriptor domain. External pressure
may consume every unreserved slot. Capture and preparation acquire a slot before
work; the publication slot remains held across validation, data flush, rename,
and selector synchronization. Abandonment releases native capacity but retains
private-stage ownership until cleanup can obtain capacity.

The model separates volatile selection from its synchronized selector. A crash
before selector synchronization may recover either complete generation; after
synchronization it retains the committed generation. A response can be lost
without rollback. Uncertain post-rename failures retain evidence and require
recovery, rather than guessing whether to discard the candidate.

| Model boundary | Rust boundary and remaining refinement |
| --- | --- |
| Capture / seal | `SearchOutOfCoreGenerationWriter::push_reader` validates UTF-8, declared length, optional CRC32c and successful EOF while capturing one private spool frame. Failure poisons the writer. `finish_with_artifacts` flushes and synchronizes the complete spool before artifact construction. |
| Exact analysis | `analyzer_stream::reader::visit_reader` retains an unfinished identifier and the previous normalized part; both owned and streamed analysis call `visit_raw_identifier`. The operation ledger admits input growth, deduplication, retained terms, and qualified native workspace. |
| Encoded frame | `document_encoding::streamed::write_frame` shares the production prefix/suffix/hex grammar, reads the body once, and patches the existing checksum field only after complete source validation. The writer commits record counts and document identity only after the complete receipt; incomplete frames cannot publish. |
| Validation / flush / fence | `SpoolSource::scan_records` validates complete frames and retains bounded headers plus immutable encoded ranges. Shared external frequency reduction and direct-to-artifact compression prepare the existing wire grammar before `finish_with_artifacts` reaches the unchanged publication fence. |
| Cancellation and stale base | Existing task checkpoints and expected-generation validation remain authoritative. Streaming input, frame validation, frequency reduction, copying and compression observe the same task. Further public-path fault qualification remains required. |
| Selected-body validation | `hydration::read_validated` shares the existing full-range, compressed and inflated checksums with the prototype. Its admitted path reserves buffers and the existing native decoder before allocation; only a complete receipt can admit the private selected output. `SearchVerifiedBody` borrows the originating reader and retains its anonymous output, header lease and optional governor admission through transfer and drop. Successful EOF verifies the transferred size and checksum. |
| FD rejection / cleanup retry | The input fixture fills four counted descriptors before unlink. The Unix selected-body fixture fills six descriptors before scan, reopen, unlink and publication, verifies typed denial and unchanged bytes, then borrows two reserved slots across nested rename and directory synchronization while a competing thread occupies the remaining capacity. Old handles retain old bytes; abandoned bytes and their path lease stay owned until cleanup succeeds. Pending private stages retain only accounted ownership metadata, an additional governor/process-policy memory reservation and conservative disk reservation, releasing the work permit, FD domain and scan workspace. Explicit retry and subsequent same-root creation reserve fresh workspace and descriptor admission. A competing-thread test exhausts the real project between scan and unlink; additional regressions cover governor release, different-limit reopen, automatic retry and permanent-failure evidence. The primitive publication fixture does not qualify Windows publication. |
| Pins / uncertain publication / recovery | The existing manifest-last publication fence and generation pins remain authoritative. Streamed mutations retain target content identities and compaction preserves retractions for unselected segments. `HawDBSearchMutationPublication` supplies the adjacent repeated-replacement, deletion, compaction and stale-CAS obligations; the integration tests retain an old reader across these operations. |

Safety invariants cover complete selected content, old-reader retention,
private-stage ownership, validation before observable delivery, acknowledged
commit preservation, and descriptor admission. Each unsafe-transition control
must violate its named invariant. Two intentionally false witness invariants
require reachable lost-response and cleanup-after-pressure-release traces.

This finite model checks safety and reachability, not unconditional progress.
External pressure may persist forever, leaving cleanup debt. Eventual cleanup
would require fair scheduling, eventual capacity, and successful filesystem
operations. `owner` represents logical responsibility for retained private
bytes; its post-crash value is a recovery obligation, not a surviving Rust lease.
It does not represent a retained host work permit. The finite model and its
recorded state counts are unchanged by the review follow-up; it does not prove
the process-wide registry cannot fill or that cross-process stage recovery exists.

Data flush and selector synchronization abstract the platform's persistence
barriers. The model does not simulate individual torn bytes, corrupt checksums,
write reordering below a completed barrier, host source snapshot consistency,
real allocator/RSS behavior, analyzer correctness, or complete mutable search
visibility. Rust corruption/fault-injection tests and the adjacent mutation model
are separate evidence; a TLC pass is not physical power-loss certification.

## Input proof and remaining gates

The input tests compare exact token events across byte boundaries, including
UTF-8, underscores, identifier phrases, aliases, and CJK. They reject oversized
indivisible identifiers without changing their analyzer. Native CJK work runs in
the existing joined workspace owner. An 8 MiB generated field exercises analysis
under a 64 KiB operation reservation without preallocating the input or output.
A 128 MiB generated capture similarly writes through bounded scratch to a counted
file and verifies all bytes on reread. These remain independent component proofs; the public integration evidence below
is required in addition.

Small complete frame fixtures compare the streamed and existing owned wire
bytes, lengths, and checksums. Encoded capability overflow rejects before reading
or writing. Source EOF, checksum, UTF-8, minimum-unit, callback, and cancellation
failures remain explicit. Captured prefixes and incomplete frames are private,
never successfully delivered output.

A generated 128 MiB selected body is decoded into a counted private file under
a 4 MiB operation ceiling. The producer never constructs a full raw or hex body;
its bounded encoding chunk and compressed fixture are separate test owners.
Complete reread verifies size and checksum. Small fixtures exercise every hex
buffer boundary, legacy line endings, corrupted unselected syntax/UTF-8, late
checksums, exact scratch admission, cancellation and sink failure. The returned
prototype header retains its memory lease until dropped. This component fixture is distinct from the public build/read integration.

## Shared result boundary

`SearchResultSet<H = SearchHit>` and `SearchOutOfCoreOutput<H = SearchHit>` keep
existing owned-result defaults and use one result/report envelope for candidates.
`SearchOutOfCoreReader::search_candidates_with_options` and
`search_candidates_with_context` return `SearchOutOfCoreCandidate` values. Each
contains the unchanged scored candidate, reader generation and exact visible
content-segment identity. The originating reader must remain alive for later
version-specific reads; copying a candidate does not retain a generation pin.

Both modes call `search_with_materializer`. Candidate construction and legacy
`hydrate_hits` are its only downstream alternatives. Filtering, ACL checks,
vector strategy, text corpus statistics, score fusion, top-k, matching counts,
tie ordering and pagination execute before that choice. Materialization cannot
refill or reorder the page. Candidate output does not request body, matched-term
or matched-span materialization. Score-entry limits remain authoritative;
full-output hydration limits remain with the owned mode.

Regression fixtures compare complete score tuples, ordered IDs, candidate-set
and retriever reports, filters, matching counts, generation identities and
replacement/delete visibility, including a retained old reader. ACL builds use
the same checks with visibility scopes. A one-byte owned-output allowance rejects
legacy hydration while the same ranked candidate page still succeeds without a
body range read. This is an output capability, not a relaxed indexing limit.

## Integrated lifecycle contracts and remaining work

`SearchDocumentHeader` owns ID, title, metadata and embedding; `SearchDocumentBody`
declares body bytes and an optional CRC32c checksum. `push_reader` consumes the
host reader once and preserves the existing spool/segment grammar. The host pins
its source snapshot through capture. Header fields and indivisible analyzer units
remain explicitly admitted resident values.

The build retains segment headers and immutable spool ranges rather than full
bodies. Lexical analysis reads bounded hex windows and shares the same external
frequency reducer with owned input. Document compression writes directly into
its private artifact and inserts the existing envelope with bounded file copies;
it never retains a complete compressed document segment. Metadata and vector
sidecars retain their independently admitted header-only representation.

Public Linux tests generate 8, 32 and 128 MiB bodies without preallocating them,
build under a 16 MiB operation reservation, reopen, query candidates, and verify
complete byte-for-byte transfer under a 4 MiB read reservation. The bounded source
contains repeated searchable words; this is not a high-cardinality retraction or
continuous-CJK qualification. The owned-hit API rejects its deliberately one-byte
output budget while candidate ranking succeeds. Token/source/encoded limits are
explicitly selected for these fixtures; defaults are unchanged.

`SearchOutOfCoreMutationWriter` accepts strictly ordered upserts and deletes
against a borrowed base reader. The existing target-bound protocol resolves each
old content version. Full-segment validation stages one old body into a reused
private file; the shared external frequency reducer emits sorted exact terms
into a bounded term file. Mutation publication streams the existing JSON grammar
instead of constructing a complete encoded run or contribution set.

Reopen preflights the complete run's checksum and maximum JSON scalar, decodes
bounded entry headers, and retains immutable term-array ranges. Each range has
its own integrity check on subsequent reads. Arbitrary supported JSON field
order, whitespace and escaping do not change canonical envelope validation.
Target validation independently reanalyzes each exact old version and compares
the ordered term stream, document length and encoded-document digest. Reader
limits control reanalysis memory, spill, tokens and headers; a writer cannot
raise those limits through an artifact. The reanalysis allowance is additional
to explicitly limited retained reader metadata, not a total-reader RSS claim.

Compaction scans each selected source segment once, stages one visible body at a
time, and feeds the same writer. Later corruption invalidates the entire private
candidate. No selected body or complete term map becomes resident. The public
32 MiB fixture exercises append, compaction, repeated replacement, delete,
restore, reopen, candidate visibility and a retained original reader. Its build
reservation is 16 MiB, reanalysis allowance is 8 MiB, and verified transfer uses
4 MiB. A separate 32 MiB fixture has 65,536 distinct generated tokens; its term
run exceeds 1 MiB and reopens with a 64 KiB mutation-header/workspace budget plus
the 8 MiB reanalysis allowance. These are fixed qualification profiles, not
automatically selected product defaults or complete throughput measurements.

`open_verified_body` validates the exact candidate generation/content version,
then the complete required source segment, including every unselected suffix.
Only then is a `SearchVerifiedBody` returned. Its Linux `O_TMPFILE` output has no
name or unlink phase; unsupported platforms/filesystems return an explicit error.
An output owns its descriptor and private disk occupancy until drop, and a
`SearchGenerationAdmission` wrapper retains the shared host permit through that
lifetime. Consumer completion still requires successful EOF, not just some bytes.

Private generation stages use a fixed-capacity owner registry admitted before
creation. Counted flat-directory cleanup closes each four-entry batch before
unlink. Failure preserves the primary operation result and keeps cleanup memory,
conservative disk reservation and governor admission alive. Explicit retries are
bounded and expose the typed descriptor cause. Successful publication reports
pending stages without rollback. Registry state is process-local: reconstructing
orphan-stage debt after process restart remains a recovery obligation, not a claim
that Rust permits survive a crash. Conservative retained disk reservations are
not measurements of live file bytes or cumulative write amplification.

The descriptor work in #829, #839 and #841 has been merged from main. This
implementation retains the counted vector bridge, typed errors and shared path
capacity bounds while adding explicit private-stage cleanup ownership. Additional
public failure/pressure/feature/platform tests and private-stage crash recovery
remain qualification gaps. The resident `SearchIndex` checkpoint and
mini-delta entrypoints retain their resident contract; the new source path uses
durable immutable generation publication. Stage 4 requires supported-path and
host-performance evidence before changing any policy defaults.

## Model receipt

The bounded TLC run checks 1,417 distinct states (8,773 generated) with no safety
violation. Six negative controls each violate their named invariant:
`ActiveComplete`, `PinnedRetained`, `NoUnvalidatedDelivery`, `OwnershipRetained`,
`CommittedNotRolledBack`, and `DescriptorBound`. The two witness configurations
violate `NoLostReply` and `NoCleanupRetry`, establishing the intended reachable
traces. State counts are receipt values for this model/configuration, not a
coverage bound on the eventual Rust implementation.

Reproduce with:

```console
bazel test //docs/tla:HawDBLargeDocumentLifecycle_check //docs/tla:large_document_controls
```

Bazel emits each source/configuration, TLC log, Java identity, jar checksum,
arguments and result under the target's `.run.tlc-evidence` directory. Inspect
the named invariant in every negative-control log; merely obtaining any failed
model is not sufficient. No liveness or physical power-loss claim is made.

The adjacent `HawDBSearchMutationPublication` baseline checks 629 distinct states
(3,267 generated). Five negative controls violate their named invariants:
`VisibilityMatchesLogical`, `StaleNeverPublished`, `ActiveClosureDurable`,
`PinnedClosureRetained`, and `TargetsRemainBound`. The repeated-replacement and
compaction witnesses violate `NoRepeatedReplacement` and `NoCompaction`.
This model binds retractions to exact versions and checks closure selection;
the lifecycle model supplies capture, validation and deferred-cleanup boundaries.
Their mapping assumes that Rust's successful preparation denotes a complete
exact retraction and that its publication fence implements the modeled CAS.
These are separate finite checks, not a mechanically proved composition.

The mutation-model receipt used pinned TLC 1.7.4, SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`,
OpenJDK 11.0.32.1, two workers and a 512 MiB heap. It was invoked directly with
the repository's unchanged source/configurations because the default Bazel
repository cache lacked `rules_rust` package files. Baseline and all seven
controls were checked for their specific success or invariant names. This
fallback does not count as passing Bazel, Cargo/Bazel parity, or local fuzz.
