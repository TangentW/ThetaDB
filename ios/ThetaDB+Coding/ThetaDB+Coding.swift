//
//  ThetaDB+Coding.swift
//  ThetaDB
//
//  Created by Tangent on 2023/9/12.
//

import Combine
import Foundation

#if !COCOAPODS
    import ThetaDB
#endif

public extension ThetaDB {
    /// Get the value associated with a given key, by decoding from the given decoder.
    @inlinable
    func get<T, C>(_ key: String, with coder: C) throws -> T?
        where T: Decodable, C: TopLevelDecoder, C.Input == Data
    {
        guard let data = try get(key) else { return nil }
        return try coder.decode(T.self, from: data)
    }

    /// Insert or update a key-value pair into the ThetaDB, the value will be encoded
    /// into the given encoder.
    @inlinable
    func put<T, C>(_ value: T, for key: String, with coder: C) throws
        where T: Encodable, C: TopLevelEncoder, C.Output == Data
    {
        let data = try coder.encode(value)
        try put(data, for: key)
    }
}

public extension Tx {
    /// Get the value associated with a given key, by decoding from the given decoder.
    @inlinable
    func get<T, C>(_ key: String, with coder: C) throws -> T?
        where T: Decodable, C: TopLevelDecoder, C.Input == Data
    {
        guard let data = try get(key) else { return nil }
        return try coder.decode(T.self, from: data)
    }
}

public extension TxMut {
    /// Get the value associated with a given key, by decoding from the given decoder.
    @inlinable
    func get<T, C>(_ key: String, with coder: C) throws -> T?
        where T: Decodable, C: TopLevelDecoder, C.Input == Data
    {
        guard let data = try get(key) else { return nil }
        return try coder.decode(T.self, from: data)
    }

    /// Insert or update a key-value pair into the ThetaDB, the value will be encoded
    /// into the given encoder.
    @inlinable
    func put<T, C>(_ value: T, for key: String, with coder: C) throws
        where T: Encodable, C: TopLevelEncoder, C.Output == Data
    {
        let data = try coder.encode(value)
        try put(data, for: key)
    }
}

public extension Cursor {
    /// Gets the value of the current record pointed by the cursor, by decoding from
    /// the given decoder.
    @inlinable
    func value<T, C>(with coder: C) throws -> T?
        where T: Decodable, C: TopLevelDecoder, C.Input == Data
    {
        guard let data = try value() else { return nil }
        return try coder.decode(T.self, from: data)
    }

    /// Gets the key-value pair of the current record pointed by the cursor, by decoding
    /// from the given decoder.
    @inlinable
    func keyValue<T, C>(with coder: C) throws -> (String, T)?
        where T: Decodable, C: TopLevelDecoder, C.Input == Data
    {
        guard let (key, data) = try keyValue() else { return nil }
        return try (key, coder.decode(T.self, from: data))
    }
}
