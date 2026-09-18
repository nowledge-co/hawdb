# Opaque search analyzer ownership

This stage of [#392](https://github.com/nowledge-co/hawdb/issues/392) extends the
private generation resource foundation. A generation containing Han characters
in an analyzed field builds its fused lexical/segment/vector artifacts on one
operation-owned worker. ASCII-only generations retain their existing execution
path. Query-time analysis and the public query contract are unchanged.

The checkpoint lexical writer and mini-delta upsert/delete analysis also use this
workspace when an analyzed field contains Han characters. The checkpoint writer
borrows its input iterator on one scoped worker, using the same build accounts
as artifact construction. It joins before returning the reader. A mini-delta
analysis joins before its result can enter the copy-on-write delta maps. Replacing
a base document can require two sequential analyses; each releases its native TLS
before the next begins. Source admission still precedes mini-delta analysis.

These existing facade entrypoints retain their default task context. A context
with no memory reservation has no aggregate operation ceiling; this change does
not reinterpret the logical posting or mini-delta limits as such a ceiling.
The private writer honors an explicitly supplied reservation, and all three paths
retain the input-dependent workspace leases through join. Shared host admission,
facade context configuration and query-time analyzer ownership remain separate
contracts. No public API, persisted encoding or default limit changes here.

Mini-delta analysis failure still invalidates the optional lexical projection and
lets the existing mutation/query fallback proceed. Checkpoint analyzer failure
does not publish a lexical reader. The enclosing checkpoint keeps its existing
snapshot and publication boundaries; this does not introduce a new transaction
boundary around those artifacts.

The worker borrows the operation's three existing accounts. It admits a 2 MiB
native stack, a 4 KiB Rust thread bookkeeping allowance and the captured closure
and result sizes before spawning. One worker stays within either nonzero task
execution ceiling while its caller waits. This is not a shared process scheduler.
The stack is a named allowance; OS thread metadata and allocator overhead are not
modeled. Public construction now uses the integrated
[context contract](SEARCH_GENERATION_CONTEXT.md).

## Ownership and cancellation

The parent's join guard retains the workspace and thread leases until explicit
native join completes. A lease held only by the worker would end before its TLS
destructors. Ordinary scoped-thread cleanup also runs too late if the parent
unwinds and releases its leases first. The guard joins in its destructor, including
when both threads panic. Worker errors preserve the previous generation and clean
the private stage through existing publication ownership.

Before work begins, a fixed unknown two-Han word initializes the worker's HMM and
skip regex under a separate constructor reservation. The immutable default Jieba
dictionary remains a shared process owner, including its first lazy initialization;
its residency belongs to #186 rather than an individual operation.

Every `cut_for_search` call admits its input-dependent capacity before entering
Jieba. HMM vectors retain a character-count high-water reservation across shorter
calls and until native join. Call scratch covers old/replacement coexistence and
stays live through the returned token iterator and its consumer. Consumer errors,
unwind and cancellation drop that iterator before its scratch lease. Checkpoints
surround opaque calls and recur every 1,024 output tokens; an in-progress Jieba
call is not internally interruptible.

## Qualified dependency envelope

The model is tied to Rust 1.97.1, Jieba 0.10.3, regex 1.13.1,
regex-automata 0.4.16 and regex-syntax 0.8.11. Exact dependency requirements prevent
a downstream compatible-version update from silently changing the opaque layout
or algorithm. Requalify the model when updating these requirements. Bounds use
8-byte words and cover the supported 32/64-bit layouts. Rust allocation requests,
including spare capacity and conservative replacement overlap, are the metric;
these are not process RSS bounds.

For input byte length B and scalar count C, the pinned `cut` implementation owns
word slices, token output, a byte-indexed route, sparse DAG edges/start positions
and touched-start positions. Search mode additionally owns expanded token output
and word character offsets. Each original token spans disjoint characters; its
two/three-gram expansion produces at most twice that span's scalar count. The
immutable default dictionary has 349,045 distinct words and at most seven nested
matching prefixes, so the DAG requires at most eight entries per scalar including
its sentinel. The dictionary SHA-256 is
`139519822fe8ab9e10d9d07e68ea0451045380aedaf54ecc51e2a28c6b42a13f`.

`bounds::invocation` covers each initial allocation and all geometric growth.
`bounds::hmm_retained` covers four Viterbi states per scalar, predecessor states,
the best path and character offsets. The call allowance includes a second HMM
envelope while old and replacement buffers coexist.

The fixed skip pattern is `([a-zA-Z0-9]+(?:.\d+)?%?)`. Its HIR has 11 nodes,
76 class ranges and 273 UTF-8 transition edges. Construction accounts for AST/HIR
and parser worklists; the 10,000-entry UTF-8 bounded table and 1,000-entry suffix
table; mutable/immutable NFA overlap, cached transition payloads, ID remapping and
empty-state rewrites. Node/header allowances cover the pinned layouts, captures,
regex cache pool and fallback engines' fixed metadata. The pattern contains no
Unicode word-boundary assertion and has no literal prefilter.

The forward/reverse lazy NFAs have 46/188 states. Exhaustive byte/EOI closure from
all anchoring and look-behind conditions reaches 54/356 tagged lazy DFA states,
including 105,370 transitions. Physical cache bounds include 128-column table
capacity, state-vector and hash-table growth, shared encoded state payloads,
sparse sets, epsilon DFS, start states and the reusable encoding buffer. Each
bound is below the unchanged 2 MiB cache policy, so this complete graph cannot
clear the cache or trigger input-sized backtracking/PikeVM fallback during the
fixed `find_iter` path. Reported `memory_usage()` alone undercounts owned capacity.

Cargo feature unification cannot enable full DFA construction here: the 46-state
forward NFA exceeds the meta engine's default 30-state threshold. The optional
one-pass attempt is bounded separately by one state per NFA state plus DEAD,
its transition table, ID worklist/map/remap, DFS and sparse set. The fixed pattern
is not one-pass; the regression verifies this rejection. Neither feature union
nor a different input changes these fixed pattern/configuration facts.

## Verification and remaining scope

Permanent tests compare all generated artifact bytes against the prior execution
path, then reopen and hydrate every document. Budget denial preserves the complete
active generation and removes its private stage. Actual ledger tests cover shared
root pressure, repeated work, consumer failure, cooperative cancellation, worker
panic, parent unwind and simultaneous panic; TLS observations precede the final
lease release.

Entrypoint regressions observe native TLS exit through real checkpoint,
checkpoint-with-report, upsert and delete calls. They require the thread lease to
remain live at TLS exit and the operation accounts to drain after their owners
drop. They also cover checkpoint reservation denial and cancellation before file
creation, failed mini-delta analysis with its existing mutation fallback, retained
snapshots, complete reopen, and the unchanged inline path for non-Han input.

The independent allocator target uses the production formulas and real pinned
Jieba/regex. It checks construction, repeated growth, short-after-long inputs and
zero tracked live bytes after native join, with rare Han, dictionary words, mixed
Unicode digits and supplementary Han. Its allocation header follows tracked
payloads across thread-local destruction and excludes untracked harness work.
Observed peaks are regression evidence; the component formulas are the admission
bounds. The test does not infer a universal bound by rounding a sampled peak.

```sh
cargo test -p hawdb-search
cargo test -p hawdb-search --no-default-features --lib
cargo test -p hawdb-search --test analyzer_workspace_allocation --features regex-automata/dfa-build,regex-automata/dfa-onepass
cargo clippy -p hawdb-search --all-targets -- -D warnings
bazel test //crates/search:all //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

HawDB-owned normalized strings, identifier deduplication and resident frequencies
now retain their own admission; see [the token ownership contract](SEARCH_TOKEN_OWNERSHIP.md).
Spill/merge ownership, outer publication and delta hydration are integrated through
the [context facade](SEARCH_GENERATION_CONTEXT.md). The 4 MiB source guard and
existing finite term and manifest policies remain. Shared host governance,
large-source support and the original #206 corpus gate remain separate work.
