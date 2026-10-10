#!/usr/bin/env bash
set -euo pipefail

retry() {
    local attempt=1
    local max_attempts=5
    local delay=5

    until "$@"; do
        local status=$?
        if (( attempt >= max_attempts )); then
            echo "::error::Command failed after ${max_attempts} attempts: $*"
            return "$status"
        fi

        echo "::warning::Command failed with exit code ${status}; retrying in ${delay}s (attempt ${attempt}/${max_attempts}): $*"
        sleep "$delay"
        attempt=$((attempt + 1))
        delay=$((delay * 2))
    done
}

toolchain="${RUST_TOOLCHAIN:-stable}"
targets="${RUST_TARGETS:-}"

rustup set profile minimal

# Rust's dist server occasionally times out before a download starts. Keep the
# normal "latest stable" behavior, but do not fail CI if an installed toolchain
# can be used after transient update failures.
if retry rustup update "$toolchain"; then
    :
elif rustup run "$toolchain" rustc --version > /dev/null 2>&1; then
    echo "::warning::Could not update Rust toolchain '${toolchain}'; using the installed toolchain"
else
    echo "::error::Could not install Rust toolchain '${toolchain}' and no installed copy is available"
    exit 1
fi

rustup default "$toolchain"

if [[ -n "$targets" ]]; then
    for target in ${targets//,/ }; do
        retry rustup target add "$target"
    done
fi

rustc --version
cargo --version
