#!/bin/bash

# Script to verify and fix arrow patch setup
set -e

echo "Checking Chroma arrow patch setup..."

# Check if we're in the chroma repository root
if [ ! -f "Cargo.toml" ] || ! grep -q "chroma-" Cargo.toml; then
    echo "Error: This script must be run from the Chroma repository root"
    exit 1
fi

# Check if patched directory exists
if [ ! -d "patched/arrow-arith" ]; then
    echo "Error: patched/arrow-arith directory not found"
    echo "This usually means the repository wasn't cloned properly"
    echo "Try: git clone --recursive <repository-url>"
    exit 1
fi

# Check if patched Cargo.toml exists
if [ ! -f "patched/arrow-arith/Cargo.toml" ]; then
    echo "Error: patched/arrow-arith/Cargo.toml not found"
    exit 1
fi

# Check if source files exist
if [ ! -f "patched/arrow-arith/src/lib.rs" ]; then
    echo "Error: patched arrow source files not found"
    exit 1
fi

# Verify the patch configuration in main Cargo.toml
if ! grep -q 'arrow-arith.*path.*patched/arrow-arith' Cargo.toml; then
    echo "Error: Arrow patch configuration not found in Cargo.toml"
    exit 1
fi

echo "✅ Arrow patch setup is correct"

# Try to build the patched crate
echo "Testing patched arrow-arith build..."
cd patched/arrow-arith
if cargo check --quiet; then
    echo "✅ Patched arrow-arith builds successfully"
else
    echo "❌ Patched arrow-arith failed to build"
    exit 1
fi

echo "✅ All checks passed!"
