// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import CHawDB
import Foundation

enum Native {
  // JSONEncoder strips the fraction from integral Doubles. The native decoder
  // distinguishes integer and float tokens, so preserve explicit float inputs.
  static func encode(_ value: Value) throws -> String {
    switch value {
    case .null: return "null"
    case .bool(let value): return value ? "true" : "false"
    case .int(let value): return String(value)
    case .float(let value):
      guard value.isFinite else {
        throw HawDBError(
          kind: "invalid_argument", message: "Non-finite parameters are not supported")
      }
      return String(value)
    case .string(let value): return try encode(value)
    case .list(let values):
      return "[" + (try values.map { try encode($0) }).joined(separator: ",") + "]"
    case .map(let values):
      let entries = try values.map { key, value in
        try encode(key) + ":" + encode(value)
      }
      return "{" + entries.joined(separator: ",") + "}"
    }
  }

  static func encode(_ values: [Value]) throws -> String { try encode(Value.list(values)) }

  static func encode(_ values: [String: Value]) throws -> String { try encode(Value.map(values)) }

  static func encode(_ value: some Encodable) throws -> String {
    String(decoding: try JSONEncoder().encode(value), as: UTF8.self)
  }

  static func bytes<T>(_ text: String, _ body: (UnsafePointer<CChar>, UInt) throws -> T) rethrows
    -> T
  {
    try text.withCString { try body($0, UInt(text.utf8.count)) }
  }

  static func data(_ buffer: HawdbBuffer) throws -> Data {
    guard let pointer = buffer.ptr, buffer.len > 0, buffer.len <= UInt(Int.max) else {
      throw HawDBError(kind: "invalid_response", message: "HawDB returned an empty buffer")
    }
    return Data(bytes: pointer, count: Int(buffer.len))
  }

  static func error(_ buffer: HawdbBuffer) -> HawDBError {
    do {
      let decoder = JSONDecoder()
      decoder.keyDecodingStrategy = .convertFromSnakeCase
      return try decoder.decode(HawDBError.self, from: data(buffer))
    } catch {
      return HawDBError(kind: "invalid_response", message: "Invalid native error: \(error)")
    }
  }

  static func open(_ body: (UnsafeMutablePointer<HawdbBuffer>) -> OpaquePointer?) throws
    -> OpaquePointer
  {
    var errorBuffer = HawdbBuffer()
    defer { hawdb_buffer_free(&errorBuffer) }
    guard let handle = body(&errorBuffer) else { throw error(errorBuffer) }
    return handle
  }

  static func check(_ body: (UnsafeMutablePointer<HawdbBuffer>) -> Bool) throws {
    var errorBuffer = HawdbBuffer()
    defer { hawdb_buffer_free(&errorBuffer) }
    guard body(&errorBuffer) else { throw error(errorBuffer) }
  }

  static func result(
    _ body: (UnsafeMutablePointer<HawdbBuffer>, UnsafeMutablePointer<HawdbBuffer>) -> Bool
  ) throws -> QueryResult {
    var resultBuffer = HawdbBuffer()
    var errorBuffer = HawdbBuffer()
    defer {
      hawdb_buffer_free(&resultBuffer)
      hawdb_buffer_free(&errorBuffer)
    }
    guard body(&resultBuffer, &errorBuffer) else { throw error(errorBuffer) }
    do {
      return try JSONDecoder().decode(QueryResult.self, from: data(resultBuffer))
    } catch {
      throw HawDBError(kind: "invalid_response", message: "Invalid query result: \(error)")
    }
  }
}
