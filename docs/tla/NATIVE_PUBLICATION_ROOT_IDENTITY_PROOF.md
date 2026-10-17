# Native publication-root binding

This component strengthens acquisition of the existing Search publication lease.
It is a physical binding check, not a logical target incarnation, source snapshot,
consumer C-ready/E session, publication receipt or durable directory owner.
The [consumer installed-activation design](CONSUMER_INSTALLED_ACTIVATION_DESIGN.md)
still requires those contracts and their composition.

## Source argument and limits

The shared [native identity implementation](../../crates/storage/src/native_file_identity.rs)
uses Unix device/inode identity and Windows volume serial/full 128-bit file ID.
The Windows implementation is moved from power-loss capture without changing
its native query, structure layout or identity width. Power-loss capture reuses
that same implementation; no persistent encoding is changed.

[File::matches_native_path](../../crates/storage/src/file_io.rs) compares the held
native file's identity with the current path lookup. Equal contents are not
identity. A native Unix handle uses path metadata rather than reopening/closing
the same inode: POSIX process-owned record locks may disappear when any matching
descriptor closes. Windows uses a separately counted validation handle and its
existing per-handle lock semantics. Unsupported queries return errors; they do
not fall back to path or byte equality. The comparison follows native path
resolution and grants no reservation of that path across later operations.

[Publication acquisition](../../crates/search/src/out_of_core/publish_lease.rs)
keeps its existing process registration and native file lock. Before returning
the lease, Unix rechecks that the actual directory still matches its registered
device/inode key; every supported native platform compares the lock pathname
with the held file. A mismatch refuses acquisition. The owned lease then closes
its native descriptor before releasing local registration, including refusal and
cancellation paths. A later attempt can acquire the corrected current namespace.
The caller retains its existing admitted native path scratch across these checks.

On Unix, directory replacement between registration and opening the lock file
must refuse rather than retain the old registration key beside a new native lock.
Windows has no local directory-registration key: replacement before opening may
bind the newly opened current root, subject to separate logical target checks.
Replacement of the lock pathname after native locking must refuse on either
platform. It cannot turn a lock on the old file into authority for a new one.

These are checked observations under the existing native identity and conforming
lease-user assumptions. They are not a security boundary against arbitrary raw
filesystem mutation after the checks, a content-integrity certificate or ABA
protection for a host binding. A native C constructor must capture trusted root
context and the source/target/binding incarnations, then validate them at fenced
selection. Neither this Boolean file comparison nor a matching path/hash/Arc can
substitute for that protocol or mint a qualified C..E End.

Validation also needs admission while the ownership descriptor is live: Unix
uses existing transient metadata admission; Windows opens a counted validation
handle. An insufficient descriptor budget returns an error before granting the
lease. No data-file installation, selector change, default durability change,
busy retry, second governor or uncounted descriptor fallback is introduced.

## Native evidence and qualification status

The positive Storage selection passed three tests, including same-byte copies,
hard-link aliases, rename/replacement while the original file stays open, missing
paths and child-process exclusion. One case forces `F_SETLK` on Unix rather than
assuming the standard lock API uses that implementation. The child must execute
one exact test and observe the parent's lock. The Search lease selection passed
nine tests, including two deterministic acquisition-namespace races and existing
thread, alias, admission/cancellation and retained-registration checks.

Two isolated native controls demonstrate the regressions. Ignoring both binding
checks caused both injected acquisition-race tests to fail. Replacing Unix path
metadata with a second open/close caused the child-process `F_SETLK` test to fail,
showing that file identity alone is insufficient if the check loses exclusion.
The production source was restored byte-for-byte, then the three/nine positive
selections and strict locked workspace/all-target/all-feature Clippy passed.
These are native counterexamples and a conditional source argument, not a new
mechanized refinement theorem or a proof of the complete activation model.

The combined component's [qualification](CONSUMER_MUTATION_CUSTODY_PROOF.md)
uses runtime/test/model tree `9495dc1f8ef7c2d377b2752baf3e6422cbb41a24`:
full Storage/Search native Bazel coverage, both facade profiles, strict native /
Linux GNU / Windows GNU / minimal WASM compilation checks, 11 generic Search
physical-fault tests and all 98 required unfiltered local fuzz targets passed.
All retained model input/tool/argument hashes and test XML files were checked.
Final primary formatting/lint/prek checks are repeated before committing the
source-correspondence/evidence Markdown updates.

No Windows/Linux execution, browser run, physical-resource or performance
qualification is claimed. Native C/E, actual Mem, vector/schema/incarnation,
complete composed power-loss and writer/fairness/group-fsync gates remain open.
