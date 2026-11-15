// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import Foundation

public struct HawDBError: Error, Decodable, Equatable, Sendable, CustomStringConvertible {
  public let kind: String
  public let message: String
  public let resourceBudget: ResourceBudget?

  public struct ResourceBudget: Decodable, Equatable, Sendable {
    public let resource: String
    public let scope: String
    public let owner: String
    public let memoryClass: String?
    public let requested: UInt64
    public let used: UInt64
    public let budget: UInt64
    public let budgetOption: String
  }

  public var description: String { "\(kind): \(message)" }

  init(kind: String, message: String) {
    self.kind = kind
    self.message = message
    resourceBudget = nil
  }
}

/// Both errors are retained if rollback itself fails after the body throws.
public struct TransactionRollbackError: Error {
  public let originalError: any Error
  public let rollbackError: any Error
}
