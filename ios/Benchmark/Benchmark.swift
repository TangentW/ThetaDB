//
//  Benchmark.swift
//  ThetaDB
//
//  Created by Tangent on 2023/9/23.
//

@testable import ThetaDB
import XCTest

// Try 10000 times.
let times = 10000

/// With 5000 keys.
let keysCount: UInt32 = 5000

var randomKey: String {
    String(arc4random_uniform(keysCount))
}

var randomPath: String {
    let name = String(arc4random())
    let documents = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).last!
    return documents + "/" + name
}

final class WriteBenchmark: XCTestCase {
    func testWrite() {
        let path = randomPath
        defer {
            try! FileManager.default.removeItem(atPath: path)
        }

        let db = try! ThetaDB(path: path)

        measure {
            for i in 1 ... times {
                let value = Data(repeating: 1, count: i)
                try! db.put(value, for: randomKey)
            }
        }
    }

    func testWriteInTransaction() {
        let path = randomPath
        defer {
            try! FileManager.default.removeItem(atPath: path)
        }

        let db = try! ThetaDB(path: path)

        measure {
            try! db.update { tx in
                for i in 1 ... times {
                    let value = Data(repeating: 1, count: i)
                    try! tx.put(value, for: randomKey)
                }
            }
        }
    }
}

final class ReadBenchmark: XCTestCase {
    func testReadFirstKey() {
        let path = makeDB()
        defer {
            try! FileManager.default.removeItem(atPath: path)
        }

        measure {
            let db = try! ThetaDB(path: path)
            _ = try! db.get(randomKey)
        }
    }

    func testRead() {
        let path = makeDB()
        defer {
            try! FileManager.default.removeItem(atPath: path)
        }

        let db = try! ThetaDB(path: path)

        measure {
            for _ in 0 ..< times {
                _ = try! db.get(randomKey)
            }
        }
    }

    func testReadInTransaction() {
        let path = makeDB()
        defer {
            try! FileManager.default.removeItem(atPath: path)
        }

        let db = try! ThetaDB(path: path)

        measure {
            try! db.view { tx in
                for _ in 0 ..< times {
                    _ = try! tx.get(randomKey)
                }
            }
        }
    }

    func makeDB() -> String {
        let path = randomPath
        let db = try! ThetaDB(path: path)

        // Put random data into database.
        for i in 1 ... times {
            let value = Data(repeating: 1, count: Int(i))
            try! db.put(value, for: randomKey)
        }

        return path
    }
}
