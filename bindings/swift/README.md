# HawDB for Swift

An embedded Swift 6 package over HawDB's existing C ABI. The repository-root
`Package.swift` exports the `HawDB` library product and imports the canonical
[`hawdb_ffi.h`](../ffi/include/hawdb_ffi.h) through a SwiftPM system library
target. No Rust crate, subprocess, or runtime library loader is added.
See SwiftPM's [system library documentation](https://github.com/swiftlang/swift-package-manager/blob/main/Sources/PackageManagerDocs/Documentation.docc/Dependencies/AddingSystemLibraryDependency.md)
for the module-map integration.

## Install and link

Add this repository as a package dependency at a revision containing the Swift
binding, and add `.product(name: "HawDB", package: "hawdb")` to your target's
dependencies. For a local checkout:

```swift
dependencies: [.package(path: "/path/to/hawdb")],
targets: [
    .executableTarget(
        name: "MyApp",
        dependencies: [.product(name: "HawDB", package: "hawdb")]
    )
]
```

SwiftPM builds the Swift code; the host supplies a matching HawDB C ABI library.
Use a [prebuilt release archive](../../docs/PREBUILT_LIBRARIES.md), or build
from the same source revision as the package:

```sh
bazel build //bindings/ffi:hawdb_ffi
library_dir="$(bazel info bazel-bin)/bindings/ffi"
swift build --package-path /path/to/MyApp \
  -Xlinker -L -Xlinker "$library_dir" \
  -Xlinker -rpath -Xlinker "$library_dir"
```

Alternatively, `cargo build --locked --manifest-path bindings/ffi/Cargo.toml`
produces the library under `bindings/ffi/target/debug`. The FFI crate is an
independent workspace. Use that directory in the linker flags instead.
For an unpacked release archive, point the flags at its `lib` directory.

The source package supports macOS 13+, iOS/iPadOS 15+, and Linux with Swift 6+.
The Native Bindings workflow owns macOS and Ubuntu conformance using its
existing Bazel-built shared library. Check each prebuilt archive's manifest for its
actual deployment/glibc floor; a Swift package platform declaration cannot
lower a native binary's minimum OS version. Build from source if necessary.
When distributing an application, bundle the matching library and configure
its runtime search path for the installed layout. Xcode hosts configure Library
Search Paths, Runpath Search Paths, and embedding/signing in their app target.
Static linking instead requires the platform-specific native dependencies
listed in the archive's manifest. For iOS, use the static XCFramework package
below. watchOS, tvOS, visionOS, Mac Catalyst, and Windows are outside this scope.

HawDB's on-disk format carries **no compatibility promise** until HawDB declares
one. An engine upgrade may require recreating development databases.

## iOS and XCFramework

An [XCFramework](https://help.apple.com/xcode/mac/current/en.lproj/dev6f6ac218b.html)
is Apple's container for platform-specific native libraries and headers. Its
device and simulator builds are distinct, even when both use arm64. HawDB's
Apple builder packages these slices:

| Rust target | Platform | Architecture |
| --- | --- | --- |
| `aarch64-apple-ios` | iPhone/iPad device | arm64 |
| `aarch64-apple-ios-sim` | iOS simulator | arm64 |
| `x86_64-apple-ios` | iOS simulator | x86-64 |

Use a Mac with **full Xcode**, including the iPhoneOS/iPhoneSimulator SDKs;
Command Line Tools alone cannot build or qualify iOS. From the repository root:

```sh
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
python3 bindings/swift/build-apple.py
```

The builder uses the pinned Rust toolchain, the repository-wide release
profile, default engine features, and iOS deployment target 15.0. It builds
three static C ABI libraries sequentially with two Cargo jobs, combines only
the simulator archives with `lipo`, and calls `xcodebuild -create-xcframework`.
It refuses incomplete slices or a header differing from the canonical ABI.
Native system-library/framework link requirements come from each build's
`rustc --print=native-static-libs`; unknown flags or differing requirements
fail packaging rather than being silently dropped.

The output at `target/swift-apple/package/HawDB` is a self-contained Swift
package: `CHawDB.xcframework`, the same Swift sources and XCTest suite, and a
generated `Package.swift` with a binary target. In Xcode, choose **Add Local
Package**, select that directory, and add the `HawDB` product to your app.
The Swift Apple workflow also uploads this package after successful device
compilation and simulator tests. These are CI artifacts, not an automatic
GitHub Release publication. `manifest.json` records source revision/dirty
state, Rust/Xcode versions, targets/profile, dependencies and file hashes.

Use this generated package instead of adding both the source package and binary
package to the same app. There is one `HawDB` Swift module and one `CHawDB`
native module. The static library is linked into the app; it is not a desktop
dylib to load or embed. No Rust compiler is needed to consume the built package.

Choose a database path in your app's writable Application Support directory,
not the application bundle:

```swift
import Foundation
import HawDB

let support = try FileManager.default.url(
    for: .applicationSupportDirectory, in: .userDomainMask,
    appropriateFor: nil, create: true
)
let database = try Database(path: support.appendingPathComponent("knowledge").path)
```

Calls are synchronous; schedule database work off the main UI thread. Close
handles when appropriate for your host's lifecycle. Default commit durability
is unchanged, and no database operation depends on a background helper process.
Simulator tests establish simulator behavior; physical-device execution,
App Store submission, suspension/termination, device file-protection policy,
and power-loss qualification remain separate host acceptance work.

## Queries and values

```swift
import HawDB

let database = try Database(path: "./knowledge")
try database.query("CREATE (:Person {name: $name})", parameters: ["name": .string("Ada")])
let people = try database.query("MATCH (p:Person) RETURN p.name AS name")
print(people.columns, people.rows)

try database.querySQL("CREATE TABLE events (id BIGINT PRIMARY KEY, title TEXT)")
try database.querySQL(
    "INSERT INTO events (id, title) VALUES ($1, $2)",
    parameters: [.int(9_007_199_254_740_993), .string("Created")]
)
let events = try database.querySQL("SELECT id, title FROM events WHERE id = $1", parameters: [.int(9_007_199_254_740_993)])
try database.close()
```

`Database.inMemory()` opens an independent in-memory store with native defaults.
`Database.version()` returns the linked engine version. Query calls return an
owned `QueryResult` with ordered `columns` and `rows`; duplicate rows and column
names are retained. Results remain valid after database closure. Each call
copies the native result and frees every received buffer exactly once, including
on decode failure.

`Value` is a `Codable`, `Equatable`, `Sendable` JSON value: `.null`, `.bool`,
`.int(Int64)`, `.float(Double)`, `.string`, `.list`, and `.map`. Integer tokens
within the signed 64-bit range decode without passing through `Double`.
The native parameter encoder preserves `.float(1.0)` as a floating-point JSON
token instead of letting Foundation normalize it to an integer. Foundation's
integer-first result decoding represents integral floating-point numbers as
`.int` when they fit; result values do not preserve that numeric type distinction.
Non-finite input numbers are refused before entering native
code. Cypher parameter names and map keys are preserved verbatim.

Native tagged values remain explicit maps in results and parameters:

```swift
let bytes = Value.binary(Data([0, 1, 255]))
let id = Value.uuid(UUID())
let exact = Value.decimal("12345678901234567890.123456789")
let vector = Value.vector([1.25, -2.5])
let date = Value.map(["$date": .string("2026-11-14")])
```

Import `Foundation` for `Data` and `UUID`. Temporal tags are `$date`,
`$localtime`, `$localdatetime`, `$datetime`, and `$duration`, with ISO 8601
string payloads. The engine validates native tags and Decimal precision;
Decimal text never passes through `Double`. See the
[C ABI value contract](../ffi/README.md#value-encoding).

## Options and errors

```swift
let database = try Database(
    path: "./knowledge",
    options: OpenOptions(
        readOnly: false,
        durability: .syncOnEveryWrite,
        maxReadResultRows: 10_000,
        maxReadIntermediateRows: 100_000,
        maxReadResultPayloadBytes: 8_388_608,
        relationalMonotonicAppendFastPath: false
    )
)
```

Every option is optional and defaults to the native setting. `nil` row limits
omit the option: the result ceiling defaults to unlimited and the intermediate
ceiling inherits the result ceiling. A zero row limit disables that ceiling;
the payload limit must be positive. Budget exhaustion throws, never returns
partial rows. Options are validated before creating the database path.
In-memory construction currently uses native defaults and takes no options.

Durability defaults to `.syncOnEveryWrite`, which synchronizes acknowledged
commits. Explicit `.syncOnCheckpoint` may lose recent acknowledged transactions
after power failure, while preserving transaction atomicity. Branch publication
remains power-loss safe. See [C ABI options](../ffi/README.md#options).

Engine failures throw `HawDBError`, retaining `kind`, `message`, and optional
`resourceBudget` metadata with unsigned 64-bit `requested`, `used`, and `budget`
counts. Handle misuse throws `closed`, `transaction_open`, or `no_transaction`;
a malformed native response throws `invalid_response`.

## Transactions and ownership

```swift
let inserted = try database.transaction { transaction in
    try transaction.querySQL("INSERT INTO events (id, title) VALUES ($1, $2)", parameters: [.int(42), .string("Atomic")])
    try transaction.query("CREATE (:Event {id: $id})", parameters: ["id": .int(42)])
    return 42
}
```

The synchronous body commits on normal return and rolls back on throw. It
returns the body's value; a failed statement propagating out of the body rolls
back all prior writes. If rollback itself fails, `TransactionRollbackError`
retains both the original and rollback errors. A failed commit is not retried
or reported as a rollback: publication may already have happened. Reconcile
against a reopened database or retry idempotently; see
[transaction outcomes](../ffi/README.md#transactions).

`Database` is `Sendable`: a recursive lock serializes queries, transactions, and
close. A transaction holds the lock throughout its synchronous body. Another
thread waits until the body finishes; do not synchronously wait inside that
body for another thread to use the same database. `Transaction` is deliberately
not `Sendable`. Autocommit queries, nested transactions, and `close()` from
inside the body throw `transaction_open`. An escaped transaction refuses later
use with `no_transaction`, including during a subsequent transaction.

`close()` is idempotent, subsequent queries fail safely, and deinitialization
also closes the handle. Separate handles provide independent serialization.
Maintenance/backup, retained-columnar access, and async APIs are not wrapped yet.

## Verification

The Native Bindings Linux/macOS matrix runs the XCTest suite and an independent
downstream SwiftPM consumer against the same C ABI library as Go and Bun.
From the repository root, local replay is:

```sh
bazel build //bindings/ffi:hawdb_ffi
library_dir="$(bazel info bazel-bin)/bindings/ffi"
swift test -j 2 -Xswiftc -warnings-as-errors \
  -Xlinker -L -Xlinker "$library_dir" \
  -Xlinker -rpath -Xlinker "$library_dir"
swift run --package-path bindings/swift/Tests/Consumer -j 2 \
  -Xswiftc -warnings-as-errors \
  -Xlinker -L -Xlinker "$library_dir" \
  -Xlinker -rpath -Xlinker "$library_dir"
```

The `Swift Apple` workflow separately runs portable package refusal controls,
builds all three iOS native slices, runs strict Clippy for all three targets,
compiles the Swift product for a generic iOS device, and runs the complete
XCTest suite on an available iPhone simulator with parallel test execution
disabled. After building the Apple package, replay with full Xcode:

```sh
cd target/swift-apple/package/HawDB
xcodebuild -scheme HawDB -destination 'generic/platform=iOS' CODE_SIGNING_ALLOWED=NO build
xcodebuild -scheme HawDB -destination 'platform=iOS Simulator,name=iPhone 16' \
  -parallel-testing-enabled NO CODE_SIGNING_ALLOWED=NO test
```

Select a simulator installed with your Xcode. The native build logs and
simulator `.xcresult` are uploaded separately from the usable package. The
portable packager controls run with `python3 bindings/swift/test_build_apple.py`
from the repository root. Shared Darwin exclusive-rename and process-RSS unit
contracts remain owned by their existing storage/QoS targets; iOS uses the same
Apple implementations, without weakening directory publication or multiplying
Darwin's byte-valued peak RSS by 1024.

The Swift layer adds no third-party dependencies. A first Unix build includes
Swift Foundation/module compilation and the existing C ABI engine build. The
iOS lane additionally cross-compiles the three release-profile archives and
target-specific Clippy metadata; these costs are separate from simulator test
execution. No timing or remote CI speedup is claimed.
