# Chroma Swift Bindings

This crate builds the UniFFI bindings and XCFramework used by the `chroma-swift` package. It supports macOS (Intel + Apple Silicon) and iOS (device + simulator).

## Prerequisites

Install the required Rust targets once:

```bash
rustup target add aarch64-apple-ios x86_64-apple-ios aarch64-apple-ios-sim aarch64-apple-darwin
```

Ensure you have Xcode command-line tools installed so `xcodebuild`, `lipo`, and `swift` are available.

## Building the XCFramework

1. From this directory run:
   ```bash
   ./build_swift_package.sh
   ```
2. The script:
   - Builds static libraries for all Apple targets
   - Runs UniFFI to generate Swift bindings
   - Produces universal libraries and an `chroma_swift_framework.xcframework`
   - Writes the generated Swift package into `Chroma/`

## Local development workflow

1. Build the bindings and XCFramework: `cd rust/swift_bindings && ./build_swift_package.sh`
2. Point `../chroma-swift/Package.swift` at the locally built XCFramework: from the `chroma-swift` repo run `./scripts/use_local_framework.sh` (expects `chroma-swift` checked out next to this repo).
3. Build/run your app from `chroma-swift` as usual.
4. When you want to go back to using a released binary, run `./scripts/use_release_framework.sh <download-url> <checksum>` (grab the zip URL + checksum from the GitHub release you uploaded).

## Publishing a new binary (manual)

If you want to ship an updated XCFramework, do the above build, zip `Chroma/chroma_swift_framework.xcframework`, upload the zip to a GitHub release, then update `chroma-swift/Package.swift` with the GitHub release URL and `swift package compute-checksum` output (use `./scripts/use_release_framework.sh <url> <checksum>` from the `chroma-swift` repo to rewrite the manifest).

Keeping the steps here avoids extra automation and makes local vs release usage explicit.
