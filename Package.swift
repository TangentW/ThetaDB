// swift-tools-version: 5.8
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "ThetaDB",
    platforms: [.iOS(.v13)],
    products: [
        .library(
            name: "ThetaDB",
            targets: ["ThetaDB"]
        ),
        .library(
            name: "ThetaDBCoding",
            targets: ["ThetaDBCoding"]
        ),
    ],
    targets: [
        .target(
            name: "ThetaDB",
            dependencies: ["ThetaDBFFI"],
            path: "ios/ThetaDB",
            sources: ["ThetaDB.swift"]
        ),
        .target(
            name: "ThetaDBCoding",
            dependencies: ["ThetaDB"],
            path: "ios/ThetaDB+Coding",
            sources: ["ThetaDB+Coding.swift"]
        ),
        .binaryTarget(
            name: "ThetaDBFFI",
            path: "ios/ThetaDBFFI.xcframework"
        ),
    ]
)
