// swift-tools-version: 6.0
// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import PackageDescription

let package = Package(
  name: "HawDB",
  platforms: [.macOS(.v13), .iOS(.v15)],
  products: [.library(name: "HawDB", targets: ["HawDB"])],
  targets: [
    .systemLibrary(name: "CHawDB", path: "bindings/swift/Sources/CHawDB"),
    .target(
      name: "HawDB", dependencies: ["CHawDB"],
      path: "bindings/swift/Sources/HawDB"
    ),
    .testTarget(
      name: "HawDBTests", dependencies: ["HawDB"],
      path: "bindings/swift/Tests/HawDBTests"
    ),
  ]
)
