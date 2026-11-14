// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import Foundation
import XCTest

@testable import HawDB

final class ValueTests: XCTestCase {
  func testLosslessIntegerAndNestedValueCodec() throws {
    let value: Value = .map([
      "camelCase": .list([
        .int(.min), .int(.max), .int(9_007_199_254_740_993),
        .bool(true), .bool(false), .null, .float(1.25), .string("\0\u{1F680}"),
        .list([]), .map([:]),
      ])
    ])
    let encoded = try Native.encode(value)
    XCTAssertTrue(encoded.contains("camelCase"))
    XCTAssertEqual(try JSONDecoder().decode(Value.self, from: Data(encoded.utf8)), value)
  }

  func testNativeTagsKeepTheirExactShape() throws {
    let id = try XCTUnwrap(UUID(uuidString: "12345678-1234-1234-1234-123456789abc"))
    let values: [Value] = [
      .binary(Data([0, 1, 255])), .uuid(id), .decimal("12345678901234567890.123456789"),
      .vector([1.25, -2.5]), .map(["$date": .string("2026-11-14")]),
      .map(["$localtime": .string("12:34:56")]),
      .map(["$localdatetime": .string("2026-11-14T12:34:56")]),
      .map(["$datetime": .string("2026-11-14T12:34:56Z")]),
      .map(["$duration": .string("P1D")]),
    ]
    for value in values {
      XCTAssertEqual(
        try JSONDecoder().decode(Value.self, from: JSONEncoder().encode(value)), value
      )
    }
  }

  func testNonFiniteParametersAreRejected() {
    for number in [Double.nan, .infinity, -.infinity] {
      XCTAssertThrowsError(try Native.encode(Value.list([.float(number)]))) { error in
        XCTAssertEqual((error as? HawDBError)?.kind, "invalid_argument")
      }
    }
  }

  func testExplicitFloatParametersKeepFloatingPointTokens() throws {
    XCTAssertEqual(try Native.encode([Value.float(1.0), .float(-0.0)]), "[1.0,-0.0]")
    XCTAssertEqual(try Native.encode(["camelCase": Value.float(1.0)]), "{\"camelCase\":1.0}")
    let encoded = try Native.encode(
      Value.list([.float(.greatestFiniteMagnitude), .float(.leastNonzeroMagnitude)]))
    XCTAssertNoThrow(try JSONSerialization.jsonObject(with: Data(encoded.utf8)))
  }

  func testResultCodecPreservesDuplicateColumnsAndRows() throws {
    let json = """
      {"columns":["same","same"],"rows":[[1,2],[1,2],[null,3]]}
      """
    let result = try JSONDecoder().decode(QueryResult.self, from: Data(json.utf8))
    XCTAssertEqual(result.columns, ["same", "same"])
    XCTAssertEqual(result.rows, [[.int(1), .int(2)], [.int(1), .int(2)], [.null, .int(3)]])
  }

  func testOptionsKeepNativeDefaultsAndExactUnsignedLimits() throws {
    XCTAssertEqual(try Native.encode(OpenOptions()), "{}")
    let encoded = try Native.encode(
      OpenOptions(
        readOnly: true, durability: .syncOnCheckpoint,
        maxReadResultRows: .max, maxReadIntermediateRows: 0,
        maxReadResultPayloadBytes: 1024, relationalMonotonicAppendFastPath: true
      ))
    XCTAssertTrue(encoded.contains("18446744073709551615"))
    let object = try XCTUnwrap(
      JSONSerialization.jsonObject(with: Data(encoded.utf8)) as? [String: Any])
    XCTAssertEqual(
      Set(object.keys),
      Set([
        "read_only", "durability", "max_read_result_rows", "max_read_intermediate_rows",
        "max_read_result_payload_bytes", "relational_monotonic_append_fast_path",
      ]))
    XCTAssertEqual(object["durability"] as? String, "sync_on_checkpoint")
  }

  func testResourceErrorsRetainUnsignedPrecision() throws {
    let json = """
      {"kind":"resource_budget_exceeded","message":"full","resource_budget":{
      "resource":"memory","scope":"query","owner":"sort","memory_class":null,
      "requested":18446744073709551615,"used":9007199254740993,"budget":0,
      "budget_option":"host_limit"}}
      """
    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase
    let error = try decoder.decode(HawDBError.self, from: Data(json.utf8))
    XCTAssertEqual(error.kind, "resource_budget_exceeded")
    XCTAssertEqual(error.resourceBudget?.requested, .max)
    XCTAssertEqual(error.resourceBudget?.used, 9_007_199_254_740_993)
    XCTAssertEqual(error.resourceBudget?.budgetOption, "host_limit")
  }
}
