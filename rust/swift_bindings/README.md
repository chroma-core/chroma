# Chroma Swift Bindings

Make sure the following targets are added:

`rustup target add aarch64-apple-ios x86_64-apple-ios aarch64-apple-ios-sim aarch64-apple-darwin`

- Run ./build_swift_package.sh
- This will compile the cross-platform Rust library, and create a new Swift package (Chroma) that includes the library + Swift bindings.
