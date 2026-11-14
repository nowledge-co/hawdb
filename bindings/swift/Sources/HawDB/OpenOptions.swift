// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

public struct OpenOptions: Encodable, Sendable {
  public enum Durability: String, Encodable, Sendable {
    case syncOnEveryWrite = "sync_on_every_write"
    /// May lose recent acknowledged transactions after power failure.
    case syncOnCheckpoint = "sync_on_checkpoint"
  }

  public var readOnly: Bool?
  public var durability: Durability?
  public var maxReadResultRows: UInt64?
  public var maxReadIntermediateRows: UInt64?
  public var maxReadResultPayloadBytes: UInt64?
  public var relationalMonotonicAppendFastPath: Bool?

  private enum CodingKeys: String, CodingKey {
    case readOnly = "read_only"
    case durability
    case maxReadResultRows = "max_read_result_rows"
    case maxReadIntermediateRows = "max_read_intermediate_rows"
    case maxReadResultPayloadBytes = "max_read_result_payload_bytes"
    case relationalMonotonicAppendFastPath = "relational_monotonic_append_fast_path"
  }

  /// Omitted options preserve the native defaults, including safe durability.
  public init(
    readOnly: Bool? = nil,
    durability: Durability? = nil,
    maxReadResultRows: UInt64? = nil,
    maxReadIntermediateRows: UInt64? = nil,
    maxReadResultPayloadBytes: UInt64? = nil,
    relationalMonotonicAppendFastPath: Bool? = nil
  ) {
    self.readOnly = readOnly
    self.durability = durability
    self.maxReadResultRows = maxReadResultRows
    self.maxReadIntermediateRows = maxReadIntermediateRows
    self.maxReadResultPayloadBytes = maxReadResultPayloadBytes
    self.relationalMonotonicAppendFastPath = relationalMonotonicAppendFastPath
  }
}
