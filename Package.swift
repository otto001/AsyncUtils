// swift-tools-version: 5.10
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "AsyncUtils",
    platforms: [.iOS(.v15), .macOS(.v13), .watchOS(.v6)],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "AsyncUtils",
            targets: ["AsyncUtils"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-collections", .upToNextMajor(from: "1.0.0")),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "AsyncUtils",
            dependencies: [.product(name: "DequeModule", package: "swift-collections")]),
        .testTarget(
            name: "AsyncUtilsTests",
            dependencies: ["AsyncUtils"]),
    ]
)
