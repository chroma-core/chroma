use std::io::Cursor;

use murmur3::murmur3_32;

use crate::embed::TokenHasher;

/// Hasher that uses MurmurHash3 with absolute value.
///
/// This matches Python's abs(mmh3.hash(token)) behavior, where the signed i32
/// hash is converted to its absolute value.
///
/// The seed field is public for direct customization.
#[derive(Default)]
pub struct Murmur3AbsHasher {
    /// Seed value for MurmurHash3 (typically 0).
    pub seed: u32,
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
