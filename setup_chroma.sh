#!/bin/bash

# Setup script for Chroma repository after cloning
set -e

echo "Setting up Chroma repository..."

# Check if we're in the chroma repository root
if [ ! -f "Cargo.toml" ] || ! grep -q "chroma-" Cargo.toml; then
    echo "Error: This script must be run from the Chroma repository root"
    exit 1
fi

# Ensure all git files are properly checked out
echo "Ensuring all files are checked out..."
git checkout HEAD -- .

# Verify the arrow patch is present
if [ ! -d "patched/arrow-arith" ]; then
    echo "❌ Error: Arrow patch directory missing"
    echo "Trying to restore from git..."
    
    # Try to restore the patched directory from git
    git checkout HEAD -- patched/
    
    if [ ! -d "patched/arrow-arith" ]; then
        echo "❌ Failed to restore arrow patch from git"
        echo "You may need to re-clone the repository"
        exit 1
    fi
fi

echo "✅ Arrow patch directory found"

# Clean any previous build artifacts
echo "Cleaning previous build artifacts..."
cargo clean

# Update rust toolchain if needed
echo "Updating Rust toolchain..."
rustup update

# Build the patched arrow-arith crate first
echo "Building patched arrow-arith..."
cd patched/arrow-arith
cargo build
cd ../..

# Build the main project
echo "Building Chroma..."
cargo build

echo "✅ Chroma setup complete!"
echo ""
echo "You can now run:"
echo "  cargo build         # Build the project"
echo "  cargo test          # Run tests"
echo "  ./check_arrow_patch.sh  # Verify arrow patch"
