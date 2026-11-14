// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// The ordinary C ABI's JSON values, including tagged native values as objects.
public indirect enum Value: Codable, Equatable, Sendable {
  case null
  case bool(Bool)
  case int(Int64)
  case float(Double)
  case string(String)
  case list([Value])
  case map([String: Value])

  public static func binary(_ data: Data) -> Value {
    .map(["$binary": .string(data.base64EncodedString())])
  }

  public static func uuid(_ value: UUID) -> Value {
    .map(["$uuid": .string(value.uuidString.lowercased())])
  }

  /// Decimal text is validated by HawDB without conversion through Double.
  public static func decimal(_ text: String) -> Value {
    .map(["$decimal": .string(text)])
  }

  public static func vector(_ coordinates: [Float]) -> Value {
    .map(["$vector": .list(coordinates.map { .float(Double($0)) })])
  }

  public init(from decoder: any Decoder) throws {
    let container = try decoder.singleValueContainer()
    if container.decodeNil() {
      self = .null
    } else if let value = try? container.decode(Bool.self) {
      self = .bool(value)
    } else if let value = try? container.decode(Int64.self) {
      self = .int(value)
    } else if let value = try? container.decode(Double.self) {
      self = .float(value)
    } else if let value = try? container.decode(String.self) {
      self = .string(value)
    } else if let value = try? container.decode([Value].self) {
      self = .list(value)
    } else {
      self = .map(try container.decode([String: Value].self))
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.singleValueContainer()
    switch self {
    case .null: try container.encodeNil()
    case .bool(let value): try container.encode(value)
    case .int(let value): try container.encode(value)
    case .float(let value):
      guard value.isFinite else {
        throw HawDBError(
          kind: "invalid_argument", message: "Non-finite parameters are not supported")
      }
      try container.encode(value)
    case .string(let value): try container.encode(value)
    case .list(let values): try container.encode(values)
    case .map(let values): try container.encode(values)
    }
  }

}

/// Rows retain column order, duplicate column names and duplicate rows.
public struct QueryResult: Decodable, Equatable, Sendable {
  public let columns: [String]
  public let rows: [[Value]]
}
