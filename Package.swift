// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "ChromaTest",
    platforms: [.macOS(.v10_15)],
    dependencies: [
        .package(path: "../swift_bindings/Chroma")
    ],
    targets: [
        .executableTarget(
            name: "ChromaTest",
            dependencies: ["Chroma"]
        )
    ]
) 