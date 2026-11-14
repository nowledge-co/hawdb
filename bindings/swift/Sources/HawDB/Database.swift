// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

import CHawDB
import Foundation

/// Synchronous embedded access. Calls and close are serialized per handle.
public final class Database: @unchecked Sendable {
  private let lock = NSRecursiveLock()
  private var handle: OpaquePointer?
  private var activeTransaction: UUID?

  private init(handle: OpaquePointer) { self.handle = handle }

  public convenience init(path: String, options: OpenOptions = OpenOptions()) throws {
    let encoded = try Native.encode(options)
    let handle = try Native.bytes(path) { pathPointer, pathLength in
      try Native.bytes(encoded) { optionsPointer, optionsLength in
        try Native.open {
          hawdb_open(pathPointer, pathLength, optionsPointer, optionsLength, $0)
        }
      }
    }
    self.init(handle: handle)
  }

  public static func inMemory() throws -> Database {
    Database(handle: try Native.open { hawdb_open_in_memory($0) })
  }

  public static func version() throws -> String {
    var buffer = HawdbBuffer()
    defer { hawdb_buffer_free(&buffer) }
    hawdb_version(&buffer)
    guard let version = String(data: try Native.data(buffer), encoding: .utf8) else {
      throw HawDBError(kind: "invalid_response", message: "Invalid version UTF-8")
    }
    return version
  }

  deinit { if let handle { hawdb_close(handle) } }

  /// Idempotent. Closing inside a transaction body is refused.
  public func close() throws {
    try locked {
      guard activeTransaction == nil else { throw transactionOpen() }
      if let handle {
        self.handle = nil
        hawdb_close(handle)
      }
    }
  }

  @discardableResult
  public func query(_ cypher: String, parameters: [String: Value] = [:]) throws -> QueryResult {
    try query(cypher, encoded: Native.encode(parameters), sql: false, transaction: nil)
  }

  @discardableResult
  public func querySQL(_ sql: String, parameters: [Value] = []) throws -> QueryResult {
    try query(sql, encoded: Native.encode(parameters), sql: true, transaction: nil)
  }

  /// Commits on return and rolls back on throw. The body must be synchronous.
  /// A failed commit can have an unknown outcome; do not blindly retry it.
  public func transaction<T>(_ body: (Transaction) throws -> T) throws -> T {
    try locked {
      let handle = try requireHandle()
      guard activeTransaction == nil else { throw transactionOpen() }
      try Native.check { hawdb_begin_transaction(handle, $0) }
      let token = UUID()
      activeTransaction = token
      defer { activeTransaction = nil }
      let value: T
      do {
        value = try body(Transaction(database: self, token: token))
      } catch {
        do {
          try Native.check { hawdb_rollback_transaction(handle, $0) }
        } catch let rollbackError {
          throw TransactionRollbackError(originalError: error, rollbackError: rollbackError)
        }
        throw error
      }
      // Native commit consumes the transaction even when it fails.
      _ = try Native.result { hawdb_commit_transaction(handle, $0, $1) }
      return value
    }
  }

  fileprivate func query(
    _ statement: String, encoded: String, sql: Bool, transaction: UUID?
  ) throws -> QueryResult {
    try locked {
      if let transaction {
        guard activeTransaction == transaction else {
          throw HawDBError(kind: "no_transaction", message: "The transaction has finished")
        }
      } else if activeTransaction != nil {
        throw transactionOpen()
      }
      let handle = try requireHandle()
      let function =
        transaction == nil
        ? (sql ? hawdb_query_sql : hawdb_query)
        : (sql ? hawdb_transaction_query_sql : hawdb_transaction_query)
      return try Native.bytes(statement) { statementPointer, statementLength in
        try Native.bytes(encoded) { parametersPointer, parametersLength in
          try Native.result {
            function(
              handle, statementPointer, statementLength, parametersPointer, parametersLength, $0, $1
            )
          }
        }
      }
    }
  }

  private func requireHandle() throws -> OpaquePointer {
    guard let handle else { throw HawDBError(kind: "closed", message: "The database is closed") }
    return handle
  }

  private func transactionOpen() -> HawDBError {
    HawDBError(kind: "transaction_open", message: "The database has an open transaction")
  }

  private func locked<T>(_ body: () throws -> T) rethrows -> T {
    lock.lock()
    defer { lock.unlock() }
    return try body()
  }
}

/// Valid only during its database's transaction body, including across later transactions.
public final class Transaction {
  private let database: Database
  private let token: UUID

  fileprivate init(database: Database, token: UUID) {
    self.database = database
    self.token = token
  }

  @discardableResult
  public func query(_ cypher: String, parameters: [String: Value] = [:]) throws -> QueryResult {
    try database.query(cypher, encoded: Native.encode(parameters), sql: false, transaction: token)
  }

  @discardableResult
  public func querySQL(_ sql: String, parameters: [Value] = []) throws -> QueryResult {
    try database.query(sql, encoded: Native.encode(parameters), sql: true, transaction: token)
  }
}
