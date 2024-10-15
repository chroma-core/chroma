# Readme

This folder houses the Rust code for the query and compactor nodes. It is a standard rust crate managed using cargo.

## Testing

In CI, we use [Nextest](https://nexte.st/) to as our test runner to both speed up tests and to easily segment tests into different categories. (At time of writing, Nextest is about 33% faster than `cargo test`.)

While it is not required to use Nextest locally, we recommend it for better output, faster test runs, and the ability to selectively run tests.

### Running with `cargo nextest`

1. Install Nextest with `cargo install nextest` to install from source, or grab a [prebuilt binary](https://nexte.st/docs/installation/pre-built-binaries).
2. Run `cargo nextest run` to run most tests.
3. Start the Tilt stack and run `cargo nextest run --profile k8s_integration` to run tests that require the Tilt stack.

### Running with `cargo test`

Start the Tilt stack and run `cargo test` to run all tests.

## Building

`cargo build`

## Rust version

Use rust 1.81.0 or greater.
