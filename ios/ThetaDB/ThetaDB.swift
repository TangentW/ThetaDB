//
//  ThetaDB.swift
//  ThetaDB
//
//  Created by Tangent on 2023/9/12.
//

import Foundation
import ThetaDBFFI

public struct Error: Swift.Error, CustomStringConvertible {
    /// All possible error cases that can be return by API calls in ThetaDB.
    public enum Code {
        /// A Rust panic was generated.
        case panic
        /// An error occured during an I/O operation.
        case io
        /// The input (e.g., key, value) is invalid.
        case inputInvalid
        /// The database file is not in the expected format or state.
        case fileUnexpected
        /// The database is corrupted.
        case dbCorrupted
        /// Unexpected FFI call error.
        case unexpected
    }

    public let code: Code
    public let description: String
}

/// The options for configuring a ThetaDB instance.
public struct Options {
    /// The size of a page in the ThetaDB.
    ///
    /// By default, it is the operating system's memory page size. And the minimum page size
    /// is 4 KB.
    public var pageSize: UInt32?

    /// Decide whether to force synchronization on every commit of the read write transaction.
    ///
    /// If it is true, every commit of the read write transaction will be immediately followed
    /// by a sync operation. If it is false, sync operation will be performed according to the
    /// operating system's internal logic.
    public var forceSync: Bool

    /// The capacity of the memory pool. Represents the number of pages that can be reused.
    ///
    /// By default, it is 4.
    public var mempoolCapacity: UInt64

    @inlinable
    public init(pageSize: UInt32? = nil, forceSync: Bool = false, mempoolCapacity: UInt64 = 4) {
        self.pageSize = pageSize
        self.forceSync = forceSync
        self.mempoolCapacity = mempoolCapacity
    }

    @inlinable
    var raw: ThetaDBOptions {
        .init(page_size: pageSize ?? 0, force_sync: forceSync ? 1 : 0, mempool_capacity: mempoolCapacity)
    }
}

// MARK: - DB

/// The main database class, all entry points are here.
public final class ThetaDB {
    @usableFromInline
    let db: UnsafeMutableRawPointer

    /// Open a ThetaDB instance at the given file path with the provided options.
    @inlinable
    public init(path: String, options: Options = .init()) throws {
        var path = path
        db = try path.withBytesRef { path in
            try ffiCall { thetadb_new(path, options.raw, $0) }
        }
    }

    deinit {
        try? ffiCall { thetadb_dealloc(db, $0) }
    }
}

public extension ThetaDB {
    /// Check if the ThetaDB contains a given key.
    @inlinable
    func contains(_ key: String) throws -> Bool {
        var key = key
        return try key.withBytesRef { key in
            try ffiCall { thetadb_contains(db, key, $0) } != 0
        }
    }

    /// Get the value associated with a given key.
    @inlinable
    func get(_ key: String) throws -> Data? {
        var key = key
        return try key.withBytesRef { key in
            try Data(bytes: ffiCall { thetadb_get(db, key, $0) })
        }
    }

    /// Insert or update a key-value pair into the ThetaDB.
    @inlinable
    func put(_ value: Data, for key: String) throws {
        var key = key
        try key.withBytesRef { key in
            try value.withBytesRef { value in
                try ffiCall { thetadb_put(db, key, value, $0) }
            }
        }
    }

    /// Delete a key-value pair from the ThetaDB.
    @inlinable
    func delete(_ key: String) throws {
        var key = key
        try key.withBytesRef { key in
            try ffiCall { thetadb_delete(db, key, $0) }
        }
    }

    /// Start a read-only transaction.
    @inlinable
    func beginTx() throws -> Tx {
        try .init(self)
    }

    /// Start a read-write transaction.
    @inlinable
    func beginTxMut() throws -> TxMut {
        try .init(self)
    }

    /// Perform a read-only transaction using closure on the ThetaDB.
    @inlinable
    func view<R>(body: (Tx) throws -> R) throws -> R {
        try body(beginTx())
    }

    /// Perform a read-write transaction using closure on the ThetaDB.
    @inlinable
    func update<R>(body: (TxMut) throws -> R) throws -> R {
        let tx = try beginTxMut()
        let result = try body(tx)
        try tx.commit()
        return result
    }

    /// Get the cursor pointing to the first record in the ThetaDB.
    @inlinable
    func firstCursor() throws -> Cursor {
        try .first(self)
    }

    /// Get the cursor pointing to the last record in the ThetaDB.
    @inlinable
    func lastCursor() throws -> Cursor {
        try .last(self)
    }

    /// Get the cursor pointing to the specific record in the ThetaDB with the given key.
    @inlinable
    func cursor(key: String) throws -> Cursor {
        try .key(self, key: key)
    }
}

// MARK: - Tx

/// Represents the read-only transaction in ThetaDB.
public final class Tx {
    @usableFromInline
    let db: ThetaDB

    @usableFromInline
    let tx: UnsafeMutableRawPointer

    /// Start a read-only transaction.
    @inlinable
    public init(_ db: ThetaDB) throws {
        tx = try ffiCall { thetadb_begin_tx(db.db, $0) }
        self.db = db
    }

    deinit {
        try? ffiCall { thetadb_tx_dealloc(tx, $0) }
    }
}

public extension Tx {
    /// Check if the ThetaDB contains a given key.
    @inlinable
    func contains(_ key: String) throws -> Bool {
        var key = key
        return try key.withBytesRef { key in
            try ffiCall { thetadb_tx_contains(tx, key, $0) } != 0
        }
    }

    /// Get the value associated with a given key.
    @inlinable
    func get(_ key: String) throws -> Data? {
        var key = key
        return try key.withBytesRef { key in
            try Data(bytes: ffiCall { thetadb_tx_get(tx, key, $0) })
        }
    }
}

// MARK: - TxMut

/// Represents the read-write transaction in ThetaDB.
public final class TxMut {
    @usableFromInline
    let db: ThetaDB

    @usableFromInline
    var rawTx: UnsafeMutableRawPointer?

    /// Start a read-only transaction.
    @inlinable
    public init(_ db: ThetaDB) throws {
        rawTx = try ffiCall { thetadb_begin_tx_mut(db.db, $0) }
        self.db = db
    }

    @inlinable
    var tx: UnsafeMutableRawPointer {
        guard let tx = rawTx else {
            fatalError("the transaction has been committed")
        }
        return tx
    }

    deinit {
        guard let tx = rawTx else { return }
        try? ffiCall { thetadb_tx_mut_dealloc(tx, $0) }
    }
}

public extension TxMut {
    /// Check if the ThetaDB contains a given key.
    @inlinable
    func contains(_ key: String) throws -> Bool {
        var key = key
        return try key.withBytesRef { key in
            try ffiCall { thetadb_tx_mut_contains(tx, key, $0) } != 0
        }
    }

    /// Get the value associated with a given key.
    @inlinable
    func get(_ key: String) throws -> Data? {
        var key = key
        return try key.withBytesRef { key in
            try Data(bytes: ffiCall { thetadb_tx_mut_get(tx, key, $0) })
        }
    }

    /// Insert or update a key-value pair into the ThetaDB.
    @inlinable
    func put(_ value: Data, for key: String) throws {
        var key = key
        try key.withBytesRef { key in
            try value.withBytesRef { value in
                try ffiCall { thetadb_tx_mut_put(tx, key, value, $0) }
            }
        }
    }

    /// Delete a key-value pair from the ThetaDB.
    @inlinable
    func delete(_ key: String) throws {
        var key = key
        try key.withBytesRef { key in
            try ffiCall { thetadb_tx_mut_delete(tx, key, $0) }
        }
    }

    /// Commit the read-write transaction, which means it has done all its work.
    @inlinable
    func commit() throws {
        try ffiCall { thetadb_tx_mut_commit(tx, $0) }
        rawTx = nil
    }
}

// MARK: - Cursor

/// Represents a cursor for navigating through the ThetaDB.
public final class Cursor {
    @usableFromInline
    let db: ThetaDB

    @usableFromInline
    let cursor: UnsafeMutableRawPointer

    @inlinable
    init(db: ThetaDB, cursor: UnsafeMutableRawPointer) {
        self.db = db
        self.cursor = cursor
    }

    /// Get the cursor pointing to the first record in the ThetaDB.
    @inlinable
    static func first(_ db: ThetaDB) throws -> Self {
        let cursor = try ffiCall { thetadb_first_cursor(db.db, $0) }!
        return .init(db: db, cursor: cursor)
    }

    /// Get the cursor pointing to the last record in the ThetaDB.
    @inlinable
    static func last(_ db: ThetaDB) throws -> Self {
        let cursor = try ffiCall { thetadb_last_cursor(db.db, $0) }!
        return .init(db: db, cursor: cursor)
    }

    /// Get the cursor pointing to the specific record in the ThetaDB with the given key.
    @inlinable
    static func key(_ db: ThetaDB, key: String) throws -> Self {
        var key = key
        return try key.withBytesRef { key in
            let cursor = try ffiCall { thetadb_cursor_from_key(db.db, key, $0) }!
            return .init(db: db, cursor: cursor)
        }
    }

    deinit {
        try? ffiCall { thetadb_cursor_dealloc(cursor, $0) }
    }
}

public extension Cursor {
    /// Gets the key of the current record pointed by the cursor.
    @inlinable
    func key() throws -> String? {
        try String(bytes: ffiCall { thetadb_cursor_key(cursor, $0) })
    }

    /// Gets the value of the current record pointed by the cursor.
    @inlinable
    func value() throws -> Data? {
        try Data(bytes: ffiCall { thetadb_cursor_value(cursor, $0) })
    }

    /// Gets the key-value pair of the current record pointed by the cursor.
    @inlinable
    func keyValue() throws -> (String, Data)? {
        let (keyBytes, valueBytes) = try ffiCall {
            var (key, value) = (FFIBytes(), FFIBytes())
            thetadb_cursor_key_value(cursor, &key, &value, $0)
            return (key, value)
        }
        guard let key = String(bytes: keyBytes), let value = Data(bytes: valueBytes) else {
            return nil
        }
        return (key, value)
    }

    /// Moves the cursor to the next record.
    @inlinable
    @discardableResult
    func next() throws -> Bool {
        try ffiCall { thetadb_cursor_next(cursor, $0) } != 0
    }

    /// Moves the cursor to the previous record.
    @inlinable
    @discardableResult
    func previous() throws -> Bool {
        try ffiCall { thetadb_cursor_prev(cursor, $0) } != 0
    }
}

// MARK: - FFI Call Utils

@usableFromInline
func ffiCall<T>(call: (UnsafeMutablePointer<FFICallState>) -> T?) throws -> T {
    var callState = FFICallState()
    let result = call(&callState)

    if let error = Error(callState: callState) {
        throw error
    }
    guard let result = result else {
        throw Error(code: .unexpected, description: "unexpected nil result from FFI call")
    }

    return result
}

private extension Error {
    init?(callState: FFICallState) {
        switch callState.code {
        case FFICallSuccess:
            return nil
        case FFICallPanic:
            code = .panic
        case FFICallErrIO:
            code = .io
        case FFICallErrInputInvalid:
            code = .inputInvalid
        case FFICallErrFileUnexpected:
            code = .fileUnexpected
        case FFICallErrDBCorrupted:
            code = .dbCorrupted
        default:
            code = .unexpected
        }
        description = String(bytes: callState.err_desc) ?? "unexpected error"
    }
}

extension String {
    @usableFromInline
    init?(bytes: FFIBytes) {
        guard let data = Data(bytes: bytes) else { return nil }
        self.init(data: data, encoding: .utf8)
    }

    @usableFromInline
    mutating func withBytesRef<R>(_ body: (FFIBytesRef) throws -> R) rethrows -> R {
        try withUTF8 {
            guard let pointer = $0.baseAddress else {
                throw Error(code: .inputInvalid, description: "cannot fetch string pointer")
            }
            guard let length = UInt32(exactly: $0.count) else {
                throw Error(code: .inputInvalid, description: "string length cannot fit into u32")
            }

            let bytesRef = FFIBytesRef(ptr: pointer, length: length)
            return try body(bytesRef)
        }
    }
}

extension Data {
    @usableFromInline
    init?(bytes: FFIBytes) {
        guard let pointer = bytes.ptr else { return nil }
        self.init(bytesNoCopy: pointer, count: Int(bytes.length), deallocator: .custom { _, _ in
            try! ffiCall { thetadb_bytes_dealloc(bytes, $0) }
        })
    }

    @usableFromInline
    func withBytesRef<R>(_ body: (FFIBytesRef) throws -> R) throws -> R {
        try withUnsafeBytes {
            guard let pointer = $0.baseAddress else {
                throw Error(code: .inputInvalid, description: "cannot fetch data pointer")
            }
            guard let length = UInt32(exactly: $0.count) else {
                throw Error(code: .inputInvalid, description: "data length cannot fit into u32")
            }

            let bytesRef = FFIBytesRef(ptr: pointer, length: length)
            return try body(bytesRef)
        }
    }
}
