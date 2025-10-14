#!/bin/bash
set -x

# Script to generate Rust operator constants from Go source
# Run this whenever go/pkg/sysdb/metastore/db/dbmodel/constants.go changes

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Generating Rust operator constants from Go source..."

cd "$WORKSPACE_ROOT"

# Temporarily modify build.rs to enable code generation
BUILD_RS="rust/types/build.rs"
BUILD_RS_BACKUP="$BUILD_RS.backup"

# Backup the original build.rs
cp "$BUILD_RS" "$BUILD_RS_BACKUP"

# Add the module and function call back temporarily
sed -i.tmp '1i\
mod operator_codegen;' "$BUILD_RS"
sed -i.tmp '/Note: Operator constants/,/This avoids/d' "$BUILD_RS"
sed -i.tmp 's|Ok(())|// Generate operator constants from Go source\n    operator_codegen::generate_operator_constants()?;\n\n    Ok(())|' "$BUILD_RS"
rm -f "$BUILD_RS.tmp"

# Run cargo build to trigger generation
echo "Running code generation..."
cargo build -p chroma-types --quiet 2>&1 || {
    # Restore on error
    mv "$BUILD_RS_BACKUP" "$BUILD_RS"
    echo "Error: Code generation failed"
    exit 1
}

# Restore the original build.rs
mv "$BUILD_RS_BACKUP" "$BUILD_RS"

# Find and copy the generated file
GENERATED_FILE=$(find target/debug/build/chroma-types-*/out/operators_generated.rs -type f 2>/dev/null | head -1)

if [ -z "$GENERATED_FILE" ]; then
    echo "Error: Could not find generated file"
    exit 1
fi

TARGET_FILE="$WORKSPACE_ROOT/rust/types/src/operators_generated.rs"
cp "$GENERATED_FILE" "$TARGET_FILE"

echo "✓ Generated $TARGET_FILE"
echo ""
echo "The file has been updated. Please review and commit the changes."
