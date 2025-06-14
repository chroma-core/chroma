#!/bin/bash
# Swift bindings build script for Chroma – now builds for macOS & iOS (device + simulator)
set -euo pipefail

############################################
# ─────────── Configuration ───────────────
############################################
NAME="chroma_swift"                   # Base library name produced by Cargo
PACKAGE_NAME="Chroma"                 # Swift‑package name / output folder
BASE_DIR="$(pwd)"
OUT_DIR="${BASE_DIR}/out"             # UniFFI output (headers, .swift, modulemap)
TARGET_DIR="${BASE_DIR}/target"       # Cargo build artefacts
RELEASE_DIR="release"
HEADER_FILE="${OUT_DIR}/${NAME}FFI.h"
INCLUDE_TMP="${OUT_DIR}/include"      # Temp header folder for the XCFramework
XCFRAMEWORK_NAME="${NAME}_framework.xcframework"

############################################
# ─────────── Rust target list ────────────
############################################
RUST_TARGETS=(
  "aarch64-apple-darwin"         # macOS Apple Silicon
  "x86_64-apple-darwin"          # macOS Intel
  "aarch64-apple-ios"            # iOS device
  "x86_64-apple-ios"             # iOS simulator (Intel)
  "aarch64-apple-ios-sim"        # iOS simulator (Apple Silicon)
)

############################################
# ─────── Install missing targets ─────────
############################################
echo "🔧 Ensuring required Rust targets are installed…"
for T in "${RUST_TARGETS[@]}"; do
  rustup target add "$T" &>/dev/null || true
done

############################################
# ───────────── Build Rust ────────────────
############################################
echo "🦀 Building static libraries…"
for TARGET in "${RUST_TARGETS[@]}"; do
  echo "  • $TARGET"
  if [[ "$TARGET" == "aarch64-apple-ios-sim" ]]; then
    export BINDGEN_EXTRA_CLANG_ARGS="-target arm64-apple-ios-simulator -isysroot $(xcrun --sdk iphonesimulator --show-sdk-path)"
  else
    unset BINDGEN_EXTRA_CLANG_ARGS
  fi
  cargo build --manifest-path "$BASE_DIR/Cargo.toml" --release --target "$TARGET"
done

############################################
# ──────── Generate Swift bindings ────────
############################################
echo "🪄 Generating UniFFI Swift bindings…"
mkdir -p "$OUT_DIR"
cargo run --bin uniffi-bindgen \
  --manifest-path "$BASE_DIR/Cargo.toml" \
  -- generate --library "${TARGET_DIR}/aarch64-apple-darwin/${RELEASE_DIR}/lib${NAME}.dylib" \
  --language swift \
  --out-dir "$OUT_DIR"

if [[ ! -f "$HEADER_FILE" ]]; then
  echo "❌  UniFFI failed to generate $HEADER_FILE" ; exit 1
fi

############################################
# ───── Create fat/universal libraries ────
############################################
echo "📦 Creating universal (fat) libs…"
mkdir -p "$OUT_DIR/universal"

# macOS (arm64 + x86_64)
lipo -create -output "$OUT_DIR/universal/lib${NAME}_macOS.a" \
  "$TARGET_DIR/aarch64-apple-darwin/${RELEASE_DIR}/lib${NAME}.a" \
  "$TARGET_DIR/x86_64-apple-darwin/${RELEASE_DIR}/lib${NAME}.a"

# iOS Simulator (arm64 + x86_64)
# Note: Apple Silicon simulators use aarch64-apple-ios-sim, Intel simulators use x86_64-apple-ios
lipo -create -output "$OUT_DIR/universal/lib${NAME}_iOS-sim.a" \
  "$TARGET_DIR/aarch64-apple-ios-sim/${RELEASE_DIR}/lib${NAME}.a" \
  "$TARGET_DIR/x86_64-apple-ios/${RELEASE_DIR}/lib${NAME}.a"

# iOS Device (just aarch64)
cp "$TARGET_DIR/aarch64-apple-ios/${RELEASE_DIR}/lib${NAME}.a" "$OUT_DIR/universal/lib${NAME}_iOS.a"

############################################
# ───────── Create XCFramework ────────────
############################################
echo "🏗️  Building XCFramework…"
rm -rf "$PACKAGE_NAME/$XCFRAMEWORK_NAME" "$INCLUDE_TMP"
mkdir -p "$INCLUDE_TMP"
cp "$HEADER_FILE" "$INCLUDE_TMP/"
cp "$OUT_DIR/${NAME}FFI.modulemap" "$INCLUDE_TMP/module.modulemap"

xcodebuild -create-xcframework \
  -library "$OUT_DIR/universal/lib${NAME}_macOS.a" -headers "$INCLUDE_TMP" \
  -library "$OUT_DIR/universal/lib${NAME}_iOS.a" -headers "$INCLUDE_TMP" \
  -library "$OUT_DIR/universal/lib${NAME}_iOS-sim.a" -headers "$INCLUDE_TMP" \
  -output "$PACKAGE_NAME/$XCFRAMEWORK_NAME"

############################################
# ─────────── Swift Package  ──────────────
############################################
echo "📦 Preparing Swift package…"
mkdir -p "$PACKAGE_NAME/Sources"
cp "$OUT_DIR/${NAME}.swift" "$PACKAGE_NAME/Sources/"

cat > "$PACKAGE_NAME/Package.swift" <<EOF
// swift-tools-version:5.10
import PackageDescription

let package = Package(
    name: "$PACKAGE_NAME",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13)
    ],
    products: [
        .library(name: "$PACKAGE_NAME", targets: ["$PACKAGE_NAME"])
    ],
    targets: [
        .target(
            name: "$PACKAGE_NAME",
            dependencies: ["${NAME}_framework"],
            path: "Sources",
            linkerSettings: [.linkedFramework("SystemConfiguration")]
        ),
        .binaryTarget(
            name: "${NAME}_framework",
            path: "$XCFRAMEWORK_NAME"
        )
    ]
)
EOF

############################################
# ───────────── House‑keeping ─────────────
############################################
rm -rf "$INCLUDE_TMP"
echo "✅ Build complete – XCFramework and Swift package are in  →  $PACKAGE_NAME"
