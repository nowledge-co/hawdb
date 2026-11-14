// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import Dispatch
import Foundation
import HawDB
import XCTest

final class DatabaseTests: XCTestCase {
  private func temporaryPath() throws -> URL {
    let path = FileManager.default.temporaryDirectory.appendingPathComponent(
      "hawdb-swift-\(UUID())")
    try FileManager.default.createDirectory(at: path, withIntermediateDirectories: false)
    addTeardownBlock { try FileManager.default.removeItem(at: path) }
    return path.appendingPathComponent("database")
  }

  private func assertKind<T>(
    _ kind: String, _ body: @autoclosure () throws -> T,
    file: StaticString = #filePath, line: UInt = #line
  ) {
    XCTAssertThrowsError(try body(), file: file, line: line) { error in
      XCTAssertEqual((error as? HawDBError)?.kind, kind, file: file, line: line)
    }
  }

  func testSQLParametersPreserveInt64UnicodeAndNulls() throws {
    let database = try Database.inMemory()
    defer { try? database.close() }
    try database.querySQL("CREATE TABLE values_test (id BIGINT PRIMARY KEY, text_value TEXT)")
    let text = "Ada\0\u{1F680}"
    try database.querySQL(
      "INSERT INTO values_test (id, text_value) VALUES ($1, $2)",
      parameters: [.int(.max), .string(text)])
    try database.querySQL(
      "INSERT INTO values_test (id, text_value) VALUES ($1, $2)", parameters: [.int(.min), .null])
    let result = try database.querySQL("SELECT id, text_value FROM values_test ORDER BY id")
    XCTAssertEqual(result.columns, ["id", "text_value"])
    XCTAssertEqual(result.rows, [[.int(.min), .null], [.int(.max), .string(text)]])
    XCTAssertEqual(
      try database.querySQL("SELECT id FROM values_test WHERE id = $1", parameters: [.int(0)]).rows,
      [])
  }

  func testCypherParametersAndTaggedValues() throws {
    let database = try Database.inMemory()
    defer { try? database.close() }
    let values: [Value] = [
      .bool(true), .null, .int(9_007_199_254_740_993), .float(1.25),
      .string("\u{4E16}\u{754C}\0"), .list([]), .map([:]),
      .map(["camelCase": .string("preserved")]), .binary(Data([0, 255])),
      .uuid(try XCTUnwrap(UUID(uuidString: "12345678-1234-1234-1234-123456789abc"))),
      .decimal("12345678901234567890.123456789"),
      .map(["$date": .string("2026-11-14")]),
    ]
    for value in values {
      XCTAssertEqual(
        try database.query(
          "UNWIND [0] AS ignored RETURN $myValue AS value", parameters: ["myValue": value]
        ).rows,
        [[value]])
    }
    try database.query(
      "CREATE (:Person {name: $personName})", parameters: ["personName": .string("Ada")])
    XCTAssertEqual(
      try database.query("MATCH (p:Person) RETURN p.name AS name").rows, [[.string("Ada")]])
  }

  func testNativeErrorLeavesHandleUsable() throws {
    let database = try Database.inMemory()
    defer { try? database.close() }
    try database.querySQL("CREATE TABLE keys_test (id BIGINT PRIMARY KEY)")
    try database.querySQL("INSERT INTO keys_test (id) VALUES (1)")
    assertKind("constraint", try database.querySQL("INSERT INTO keys_test (id) VALUES (1)"))
    assertKind(
      "invalid_argument",
      try database.query(
        "UNWIND [0] AS ignored RETURN $value AS value",
        parameters: ["value": .decimal("invalid")]))
    XCTAssertEqual(try database.querySQL("SELECT id FROM keys_test").rows, [[.int(1)]])
  }

  func testCloseIsIdempotentAndResultsOutliveHandle() throws {
    let database = try Database.inMemory()
    let result = try database.query("UNWIND [42] AS answer RETURN answer")
    try database.close()
    try database.close()
    XCTAssertEqual(result.rows, [[.int(42)]])
    assertKind("closed", try database.query("UNWIND [1] AS value RETURN value"))
    assertKind("closed", try database.transaction { _ in 1 })
    XCTAssertFalse(try Database.version().isEmpty)
  }

  func testPersistentReopenAndReadOnly() throws {
    let path = try temporaryPath().path
    let database = try Database(path: path)
    try database.querySQL("CREATE TABLE events (id BIGINT PRIMARY KEY)")
    try database.querySQL("INSERT INTO events (id) VALUES (42)")
    try database.close()
    let reopened = try Database(path: path, options: OpenOptions(readOnly: true))
    defer { try? reopened.close() }
    XCTAssertEqual(try reopened.querySQL("SELECT id FROM events").rows, [[.int(42)]])
    XCTAssertThrowsError(try reopened.querySQL("INSERT INTO events (id) VALUES (43)"))
  }

  func testInvalidOptionsDoNotCreateDatabase() throws {
    let path = try temporaryPath()
    assertKind(
      "invalid_argument",
      try Database(path: path.path, options: OpenOptions(maxReadResultPayloadBytes: 0)))
    XCTAssertFalse(FileManager.default.fileExists(atPath: path.path))
  }

  func testReadBudgetsRefuseInsteadOfReturningPartialRows() throws {
    let path = try temporaryPath().path
    let database = try Database(path: path)
    try database.querySQL("CREATE TABLE bounded (id BIGINT PRIMARY KEY)")
    try database.querySQL("INSERT INTO bounded (id) VALUES (1), (2)")
    try database.close()
    let bounded = try Database(
      path: path, options: OpenOptions(maxReadResultRows: 1, maxReadIntermediateRows: 10))
    defer { try? bounded.close() }
    XCTAssertThrowsError(try bounded.querySQL("SELECT id FROM bounded"))
    XCTAssertEqual(try bounded.querySQL("SELECT COUNT(*) FROM bounded").rows, [[.int(2)]])
  }

  func testPayloadBudgetRefusesWithoutPoisoningHandle() throws {
    let path = try temporaryPath().path
    let database = try Database(path: path)
    try database.querySQL("CREATE TABLE payload_values (id BIGINT PRIMARY KEY, body TEXT)")
    try database.querySQL(
      "INSERT INTO payload_values (id, body) VALUES ($1, $2)",
      parameters: [.int(1), .string(String(repeating: "x", count: 4096))])
    try database.close()
    let bounded = try Database(path: path, options: OpenOptions(maxReadResultPayloadBytes: 1024))
    defer { try? bounded.close() }
    assertKind("read_budget_exceeded", try bounded.querySQL("SELECT body FROM payload_values"))
    XCTAssertEqual(try bounded.querySQL("SELECT id FROM payload_values").rows, [[.int(1)]])
  }

  func testDurabilityAndAppendOptionsAreScopedToOpen() throws {
    let path = try temporaryPath().path
    let relaxed = try Database(
      path: path,
      options: OpenOptions(durability: .syncOnCheckpoint, relationalMonotonicAppendFastPath: true))
    let branch = try relaxed.querySQL("SHOW CURRENT BRANCH")
    let index = try XCTUnwrap(branch.columns.firstIndex(of: "durability"))
    XCTAssertEqual(branch.rows.first?[index], .string("sync_on_checkpoint"))
    try relaxed.querySQL("CREATE TABLE append_values (id BIGINT PRIMARY KEY)")
    for id in 1...8 {
      try relaxed.querySQL(
        "INSERT INTO append_values (id) VALUES ($1)", parameters: [.int(Int64(id))])
    }
    try relaxed.querySQL("INSERT INTO append_values (id) VALUES (0)")
    assertKind("constraint", try relaxed.querySQL("INSERT INTO append_values (id) VALUES (1)"))
    try relaxed.close()
    let defaults = try Database(path: path)
    defer { try? defaults.close() }
    let current = try defaults.querySQL("SHOW CURRENT BRANCH")
    let currentIndex = try XCTUnwrap(current.columns.firstIndex(of: "durability"))
    XCTAssertEqual(current.rows.first?[currentIndex], .string("sync_on_every_write"))
    XCTAssertEqual(try defaults.querySQL("SELECT COUNT(*) FROM append_values").rows, [[.int(9)]])
  }

  func testTransactionsCommitSQLAndCypherAndExpireEscapedHandles() throws {
    let path = try temporaryPath().path
    let database = try Database(path: path)
    try database.querySQL("CREATE TABLE atomic_values (id BIGINT PRIMARY KEY)")
    var escaped: Transaction?
    let returned = try database.transaction { transaction in
      escaped = transaction
      try transaction.querySQL("INSERT INTO atomic_values (id) VALUES ($1)", parameters: [.int(42)])
      try transaction.query("CREATE (:Atomic {name: $name})", parameters: ["name": .string("Ada")])
      XCTAssertEqual(try transaction.querySQL("SELECT id FROM atomic_values").rows, [[.int(42)]])
      assertKind("transaction_open", try database.querySQL("SELECT id FROM atomic_values"))
      assertKind("transaction_open", try database.close())
      assertKind("transaction_open", try database.transaction { _ in 0 })
      return 7
    }
    XCTAssertEqual(returned, 7)
    let expired = try XCTUnwrap(escaped)
    assertKind("no_transaction", try expired.query("UNWIND [1] AS value RETURN value"))
    try database.transaction { _ in
      assertKind("no_transaction", try expired.querySQL("SELECT id FROM atomic_values"))
    }
    try database.close()
    let reopened = try Database(path: path)
    defer { try? reopened.close() }
    XCTAssertEqual(try reopened.querySQL("SELECT id FROM atomic_values").rows, [[.int(42)]])
    XCTAssertEqual(
      try reopened.query("MATCH (n:Atomic) RETURN n.name AS name").rows, [[.string("Ada")]])
  }

  func testTransactionThrowAndFailedStatementRollBack() throws {
    enum BodyError: Error { case stop }
    let database = try Database.inMemory()
    defer { try? database.close() }
    try database.querySQL("CREATE TABLE rollback_values (id BIGINT PRIMARY KEY)")
    XCTAssertThrowsError(
      try database.transaction { transaction in
        try transaction.querySQL("INSERT INTO rollback_values (id) VALUES (1)")
        throw BodyError.stop
      }
    ) { XCTAssertTrue($0 is BodyError) }
    assertKind(
      "constraint",
      try database.transaction { transaction in
        try transaction.querySQL("INSERT INTO rollback_values (id) VALUES (2)")
        try transaction.querySQL("INSERT INTO rollback_values (id) VALUES (2)")
      })
    XCTAssertEqual(try database.querySQL("SELECT id FROM rollback_values").rows, [])
    _ = try database.transaction { try $0.querySQL("INSERT INTO rollback_values (id) VALUES (3)") }
    XCTAssertEqual(try database.querySQL("SELECT id FROM rollback_values").rows, [[.int(3)]])
  }

  func testDatabaseDeinitClosesPersistentHandle() throws {
    let path = try temporaryPath().path
    do {
      let database = try Database(path: path)
      try database.querySQL("CREATE TABLE released (id BIGINT PRIMARY KEY)")
    }
    let reopened = try Database(path: path)
    try reopened.close()
  }

  func testConcurrentCallsAndCloseDoNotUseFreedHandle() throws {
    let database = try Database.inMemory()
    DispatchQueue.concurrentPerform(iterations: 32) { index in
      if index == 16 {
        try? database.close()
      } else {
        do {
          let result = try database.query(
            "UNWIND [0] AS ignored RETURN $index AS value",
            parameters: ["index": .int(Int64(index))])
          XCTAssertEqual(result.rows, [[.int(Int64(index))]])
        } catch {
          XCTAssertEqual((error as? HawDBError)?.kind, "closed")
        }
      }
    }
    assertKind("closed", try database.query("UNWIND [1] AS value RETURN value"))
  }

  func testConcurrentTransactionsSerializeCompleteBodies() throws {
    let database = try Database.inMemory()
    defer { try? database.close() }
    try database.querySQL("CREATE TABLE concurrent_values (id BIGINT PRIMARY KEY)")
    DispatchQueue.concurrentPerform(iterations: 8) { index in
      do {
        try database.transaction { transaction in
          for offset in 0...1 {
            try transaction.querySQL(
              "INSERT INTO concurrent_values (id) VALUES ($1)",
              parameters: [.int(Int64(index * 2 + offset))])
          }
        }
      } catch {
        XCTFail("Concurrent transaction failed: \(error)")
      }
    }
    XCTAssertEqual(
      try database.querySQL("SELECT COUNT(*) FROM concurrent_values").rows, [[.int(16)]])
  }
}
