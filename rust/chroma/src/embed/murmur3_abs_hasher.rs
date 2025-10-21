use std::io::Cursor;

use murmur3::murmur3_32;

use crate::embed::TokenHasher;

/// Hasher that uses MurmurHash3 with absolute value,
/// matching Python's abs(mmh3.hash(token)) behavior.
///
/// Python's mmh3.hash() returns a signed i32, and fastembed uses abs() to convert
/// negative values to positive. This hasher replicates that behavior.
pub struct Murmur3AbsHasher {
    seed: u32,
}

impl Murmur3AbsHasher {
    /// Create a new hasher with seed 0 (matching fastembed default).
    pub fn new() -> Self {
        Self { seed: 0 }
    }

    /// Create a new hasher with a custom seed.
    pub fn with_seed(seed: u32) -> Self {
        Self { seed }
    }
}

impl Default for Murmur3AbsHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenHasher for Murmur3AbsHasher {
    fn hash(&self, token: &str) -> u32 {
        // Hash the token bytes with murmur3
        let hash_result = murmur3_32(&mut Cursor::new(token.as_bytes()), self.seed)
            .expect("murmur3_32 should not fail on in-memory data");

        // Convert to signed i32, then take absolute value
        // This matches Python's: abs(mmh3.hash(token, seed=0))
        let hash_signed = hash_result as i32;
        hash_signed.unsigned_abs()
    }
}
