# Injectable Zstd implementations

Issue Number: ref #1988

## Scope and ownership

The host may install one process-wide compression backend before the first
compression operation. Installation and first use race through one `OnceLock`:
exactly one backend wins, and subsequent replacement is refused. Existing
contexts, background tasks and process-wide dictionary caches never switch
implementations. No environment variables or dynamic library lookup select it.

The default `builtin-zstd` feature supplies the existing custom-memory adapter.
Disabling it removes Zstd from the library's normal dependency graph. A host
then registers its own backend through the facade's typed Rust API. An operation
requiring compression without a backend returns an error; it does not emit raw
bytes labelled as Zstd. Missing-provider attempts do not prevent later explicit
installation. Installing a backend does not create or modify a database.

The contract belongs to `hawdb-storage::compression` and is re-exported by the
HawDB facade. A separate crate is unnecessary: the implementation owns the
existing storage codec boundary and checkpoint admission types.

## Codec contract

All implementations encode and decode ordinary Zstd frames. This is an
implementation substitution, not a new persistent codec or format version.
Persisted data must remain readable after a backend change across process
restarts. Modern concatenated and skippable frames retain their existing
semantics; pre-1.0 legacy frames remain refused.

Object-safe encoder/decoder contexts consume bounded input/output slices and
report consumed bytes, produced bytes and completion. The wrapper validates
reported positions before slicing, rejects stalled operations and incomplete
frames, and poisons a context after a failure. A decoder may be reset explicitly;
an encoder failure cannot be retried into a successful publication.

Encoders support streaming, flush, end-of-frame, pledged content size and
sequential independent frames. Finish reports failures before returning the
sink. Decoders support streaming, frame boundaries, reset and retained workspace
accounting. Each context is owned, `Send + Sync`, and independent of other
contexts created by the same shared factory.

## Resource admission

Workspace bounds belong to the implementation, not a global Zstd-version
constant in Search. Factories expose checked compressor, decoder-context and
frame-buffer bounds. Bounds include backend-owned allocations and must cover
peak live ownership, including replacement overlap if the implementation does
not free before replacement. Search reserves before construction/growth and
checks retained decoder memory after each step.

Checkpoint factories receive the existing `CheckpointWorkContext`; a host
implementation must acquire and retain its memory permits before allocation,
release physical ownership before refunding them, and preserve typed refusal.
The default adapter continues using Rust allocation callbacks with exact native
allocation charging. Host implementations own their allocator and bound
correctness; the interface does not intercept arbitrary third-party malloc.

ABI/version qualification remains private to the built-in adapter. A host
backend does not need to use `zstd-sys`, or the built-in adapter's native version.

## Implementation and verification sequence

1. Introduce the process registry and backend-neutral streaming contract, then
   adapt the existing native implementation without changing its allocation
   callbacks. Preserve codec failure/cleanup regressions.
2. Route all production storage/Search codec and frame-header operations through
   that boundary. Replace Search's implementation-specific bounds with provider
   bounds. Keep budget-denial and incomplete-publication behavior.
3. Make `builtin-zstd` optional, propagate features through the facade and
   internal dependencies, and preserve built-in codec selection in bindings and
   existing default/minimal test lanes. Add an independent external-provider
   consumer and a normal-dependency graph check without the built-in backend.
4. Verify initialization races, replacement refusal, cross-implementation frame
   compatibility, malformed progress, truncated streams, exact/short budgets,
   failure cleanup and native/WASM default behavior. Update TEST_INDEX and
   allocator documentation. Run the required final formatting/strict Clippy
   checks before committing; source checks and final-head CI remain separate.

## Non-goals

Per-database backend selection, arbitrary compression formats, dynamic plugins,
dictionary injection, native worker configuration and runtime backend replacement
are outside this change. No existing persistent format is changed.
