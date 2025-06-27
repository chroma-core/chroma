# Arrow Patch Setup for Chroma

This repository uses a patched version of Apache Arrow's `arrow-arith` crate. This document explains how to set up the project correctly.

## Background

Chroma uses a custom-patched version of Apache Arrow's arithmetic kernels located in `patched/arrow-arith/`. This allows us to include custom optimizations and bug fixes that aren't yet available in the upstream Arrow libraries.

## Setup Instructions

### For New Repository Clones

1. **Clone the repository normally:**
   ```bash
   git clone <repository-url>
   cd chroma
   ```

2. **Run the setup script:**
   ```bash
   ./setup_chroma.sh
   ```

3. **Verify the setup:**
   ```bash
   ./check_arrow_patch.sh
   ```

### Manual Setup (if scripts fail)

1. **Verify the patched directory exists:**
   ```bash
   ls -la patched/arrow-arith/
   ```

2. **If missing, restore from git:**
   ```bash
   git checkout HEAD -- patched/
   ```

3. **Clean and rebuild:**
   ```bash
   cargo clean
   cd patched/arrow-arith
   cargo build
   cd ../..
   cargo build
   ```

## Troubleshooting

### Error: "could not find arrow-arith"

This usually means the `patched/arrow-arith` directory is missing or incomplete.

**Solution:**
```bash
git status patched/
git checkout HEAD -- patched/
./check_arrow_patch.sh
```

### Error: "failed to build arrow-arith"

The patched crate might have dependency issues.

**Solution:**
```bash
cd patched/arrow-arith
cargo update
cargo build
cd ../..
cargo build
```

### Error: "no such file or directory: patched/arrow-arith"

The patch directory wasn't properly cloned.

**Solution:**
```bash
# Check if files are tracked in git
git ls-files patched/

# If they exist, restore them
git checkout HEAD -- patched/

# If they don't exist, you may need to re-clone
git clone <repository-url> chroma-fresh
```

## How the Patch Works

The patch is implemented using Cargo's `[patch.crates-io]` feature in the root `Cargo.toml`:

```toml
[patch.crates-io]
arrow-arith = { version = "52.2.0", path = "patched/arrow-arith" }
```

This tells Cargo to use our local patched version instead of the crates.io version for any crate that depends on `arrow-arith`.

## Files Structure

```
patched/
└── arrow-arith/
    ├── Cargo.toml          # Patch crate configuration
    └── src/
        ├── lib.rs          # Module declarations
        ├── aggregate.rs    # Aggregation operations
        ├── arithmetic.rs   # Basic arithmetic
        ├── arity.rs        # Unary/binary operations
        ├── bitwise.rs      # Bitwise operations
        ├── boolean.rs      # Boolean logic
        ├── numeric.rs      # Numeric kernels
        └── temporal.rs     # Date/time operations
```

## Updating the Patch

If you need to modify the arrow patch:

1. Edit files in `patched/arrow-arith/src/`
2. Test your changes: `cd patched/arrow-arith && cargo test`
3. Build the main project: `cd ../.. && cargo build`
4. Commit the changes: `git add patched/ && git commit -m "Update arrow patch"`

## Alternative: Using Original Arrow

If you want to temporarily use the original Arrow crate instead of the patch:

1. Comment out the patch in `Cargo.toml`:
   ```toml
   [patch.crates-io]
   # arrow-arith = { version = "52.2.0", path = "patched/arrow-arith" }
   ```

2. Clean and rebuild:
   ```bash
   cargo clean
   cargo build
   ```

Note: This may cause compilation errors if the code relies on custom functionality in the patch.
