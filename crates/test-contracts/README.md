# Repository-only checkpoint contracts

This workspace member is unpublished (`publish = false`). Its dependency-free
fixture library is Bazel `testonly`, and the facade package excludes this
directory from its archive. Production packages must not depend on it.
The publication script rejects a publishable test-contract package or a normal
or build dependency on it; development dependencies remain allowed.

The shared fixture allocates an independently owned temporary project for each
case. It never adopts an existing directory and removes only its own project
after the case closes its database handles.

The default contract executable contains the five original manual-checkpoint
cases and six WAL-recovery cases. The separate minimal executable retains those
five manual-checkpoint cases and all six WAL cases already available under Cargo
without default features. Every original assertion, recovery negative control
and durability setting remains.
Both Bazel carriers use the existing checked profile, original 300-second limit,
one libtest thread and exclusive scheduling.

The integration carriers remain required by the existing root suite; their
`manual` tags prevent duplicate implicit discovery by the crate lane. The crate
presubmit owns the lightweight fixture-isolation unit test. The old root Bazel
labels are single-member `test_suite` wrappers and do not compile additional
binaries.

```sh
cargo test --locked -p hawdb-test-contracts
cargo test --locked -p hawdb-test-contracts --no-default-features
bazel test //:hawdb_manual_checkpoint_tests //:hawdb_manual_checkpoint_minimal_tests
```

Private automatic-checkpoint state-machine tests and pause hooks remain in the
facade unit binary. This crate adds no production test-hook API. Allocator, RSS,
and crash-subprocess contracts retain their own existing process boundaries.
