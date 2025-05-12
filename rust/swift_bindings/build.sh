#!/bin/bash
# Swift bindings build script for Chroma
set -e  # Exit immediately if a command exits with a non-zero status

# Set variables
NAME="chroma_swift"
BASE_DIR=$(pwd)
OUT_DIR="${BASE_DIR}/out"
HEADERPATH="${OUT_DIR}/${NAME}FFI.h"
TARGETDIR="${BASE_DIR}/target"
OUTDIR="${BASE_DIR}/../swift_package"
RELDIR="release"
STATIC_LIB_NAME="lib${NAME}.a"
NEW_HEADER_DIR="${OUT_DIR}/include"

# Create directories
mkdir -p "${OUT_DIR}"
mkdir -p "${OUTDIR}"
mkdir -p "${OUTDIR}/Sources"

# Build for macOS
echo "Building for macOS..."
cargo build --release --manifest-path="$(pwd)/Cargo.toml"

# Generate bindings using UniFFI
echo "Generating Swift bindings..."
cargo run --bin uniffi-bindgen --manifest-path="$(pwd)/Cargo.toml" -- generate --library "$(pwd)/target/release/lib${NAME}.dylib" --language swift --out-dir "$(pwd)/out"

# Check if files were generated successfully
if [ ! -f "${HEADERPATH}" ]; then
    echo "Error: Header file ${HEADERPATH} was not generated."
    exit 1
fi

if [ ! -f "${OUT_DIR}/${NAME}FFI.modulemap" ]; then
    echo "Error: Module map ${OUT_DIR}/${NAME}FFI.modulemap was not generated."
    exit 1
fi

if [ ! -f "${OUT_DIR}/${NAME}.swift" ]; then
    echo "Error: Swift file ${OUT_DIR}/${NAME}.swift was not generated."
    exit 1
fi

# Create header directory
mkdir -p "${NEW_HEADER_DIR}"
cp "${HEADERPATH}" "${NEW_HEADER_DIR}/"
cp "${OUT_DIR}/${NAME}FFI.modulemap" "${NEW_HEADER_DIR}/module.modulemap"

# Remove previous framework if it exists
rm -rf "${OUTDIR}/${NAME}_framework.xcframework"

# Create XCFramework
echo "Creating XCFramework..."
xcodebuild -create-xcframework \
    -library "${TARGETDIR}/${RELDIR}/${STATIC_LIB_NAME}" \
    -headers "${NEW_HEADER_DIR}" \
    -output "${OUTDIR}/${NAME}_framework.xcframework"

# Copy Swift file to the output directory
echo "Copying Swift bindings to output directory..."
cp "${OUT_DIR}/${NAME}.swift" "${OUTDIR}/Sources/"

# Create Swift package manifest
echo "Creating Swift Package Manager manifest..."
cat > "${OUTDIR}/Package.swift" << EOL
// swift-tools-version:5.10
import PackageDescription

let package = Package(
    name: "ChromaSwift",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13)
    ],
    products: [
        .library(
            name: "ChromaSwift",
            targets: ["ChromaSwift"]
        ),
    ],
    dependencies: [],
    targets: [
        .target(
            name: "ChromaSwift",
            dependencies: ["${NAME}_framework"],
            path: "Sources"
        ),
        .binaryTarget(
            name: "${NAME}_framework",
            path: "${NAME}_framework.xcframework"
        )
    ]
)
EOL

echo "Creating README for Swift package..."
cat > "${OUTDIR}/README.md" << EOL
# ChromaSwift

Swift bindings for the Chroma vector database in-memory components.

## Installation

Add this package to your Xcode project using Swift Package Manager.

## Usage

\`\`\`swift
import ChromaSwift

// Initialize Chroma
initialize_chroma()

// Create a collection
let collection = try ChromaCollection(
    name: "my_collection", 
    dimension: 384, 
    distance_function: .Cosine
)

// Add embeddings
try collection.add(
    ids: ["id1", "id2"],
    embeddings: [[0.1, 0.2, 0.3, ...], [0.4, 0.5, 0.6, ...]],
    metadatas: ["{\"text\": \"document 1\"}", "{\"text\": \"document 2\"}"]
)

// Query embeddings
let results = try collection.query(
    query_embedding: [0.1, 0.2, 0.3, ...],
    n_results: 2
)

// Retrieve embeddings
let embeddings = try collection.get(ids: ["id1"])

// Count embeddings
let count = try collection.count()

// Delete embeddings
try collection.delete(ids: ["id1"])
\`\`\`
EOL

# Clean up temporary directories
rm -rf "${NEW_HEADER_DIR}"

echo "Swift bindings generation complete!"
echo "Framework and Swift files are available in ${OUTDIR}"
