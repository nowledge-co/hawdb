// swift-tools-version: 6.0
// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import PackageDescription

let package = Package(
  name: "HawDBConsumer",
  platforms: [.macOS(.v13)],
  dependencies: [.package(name: "HawDB", path: "../../../..")],
  targets: [
    .executableTarget(
      name: "HawDBConsumer",
      dependencies: [.product(name: "HawDB", package: "HawDB")]
    )
  ]
)
