use thiserror::Error;

/// A token instance is a unique value containing a trigram, an offset ID, and optionally a position within a document.
/// These three attributes are packed into a single u128 value:
/// - The trigram is a 63-bit value, packed into the top 64 bits.
/// - The offset ID is a 32-bit value, packed into the next 32 bits.
/// - The position is a 32-bit value, packed into the bottom 32 bits. The highest bit is used as a flag to indicate whether the position is present (meaning that the maximum position value is 2^31, not 2^32).
///
/// It is laid out like this to make it trivial to sort a list of token instances by trigram, offset ID, and then position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TokenInstance(u128);

/// Unicode characters only use 21 bits, so we can encode a trigram in 21 * 3 = 63 bits (a u64).
/// Returns None if the string contains a null terminator.
#[inline(always)]
fn pack_trigram(s: &str) -> Option<u64> {
    let mut u = 0u64;
    for (i, c) in s.chars().take(3).enumerate() {
        if c == '\0' {
            return None;
        }

        let shift = (2 - i) * 21;
        u |= (c as u64) << shift;
    }

    Some(u)
}

fn encode_utf8_unchecked(c: u32, buf: &mut [u8]) -> usize {
    if c == 0 {
        0
    } else if c < 0x80 {
        buf[0] = c as u8;
        1
    } else if c < 0x800 {
        buf[0] = (0xC0 | (c >> 6)) as u8;
        buf[1] = (0x80 | (c & 0x3F)) as u8;
        2
    } else if c < 0x10000 {
        buf[0] = (0xE0 | (c >> 12)) as u8;
        buf[1] = (0x80 | ((c >> 6) & 0x3F)) as u8;
        buf[2] = (0x80 | (c & 0x3F)) as u8;
        3
    } else {
        buf[0] = (0xF0 | (c >> 18)) as u8;
        buf[1] = (0x80 | ((c >> 12) & 0x3F)) as u8;
        buf[2] = (0x80 | ((c >> 6) & 0x3F)) as u8;
        buf[3] = (0x80 | (c & 0x3F)) as u8;
        4
    }
}

#[inline(always)]
fn unpack_trigram(u: u64) -> String {
    let c0 = ((u >> 42) & 0x1F_FFFF) as u32;
    let c1 = ((u >> 21) & 0x1F_FFFF) as u32;
    let c2 = (u & 0x1F_FFFF) as u32;

    // Preallocate the maximum possible size (3 chars * 4 bytes each)
    let mut s = String::with_capacity(12);

    unsafe {
        // Directly get a mutable reference to the internal buffer
        let v = s.as_mut_vec();
        let len0 = v.len();

        // Ensure the buffer has enough capacity
        v.set_len(len0 + 12);

        // Encode the codepoints directly into the buffer
        let bytes_written_c0 = encode_utf8_unchecked(c0, &mut v[len0..]);
        let bytes_written_c1 = encode_utf8_unchecked(c1, &mut v[len0 + bytes_written_c0..]);
        let bytes_written_c2 =
            encode_utf8_unchecked(c2, &mut v[len0 + bytes_written_c0 + bytes_written_c1..]);

        // Set the correct length after writing
        let total_bytes = bytes_written_c0 + bytes_written_c1 + bytes_written_c2;
        v.set_len(len0 + total_bytes);
    }

    s
}

#[derive(Debug, Error)]
pub enum TokenInstanceEncodeError {
    #[error("Token contains null terminator")]
    NullTerminator,
}

impl TokenInstance {
    pub const MAX: Self = Self(u128::MAX);

    #[inline(always)]
    pub fn encode(
        token: &str,
        offset_id: u32,
        position: Option<u32>,
    ) -> Result<Self, TokenInstanceEncodeError> {
        match pack_trigram(token) {
            Some(packed) => Ok(TokenInstance(
                ((packed as u128) << 64)
                    | ((offset_id as u128) << 32)
                    | (position.map(|o| o | (1 << 31)).unwrap_or(0) as u128),
            )),
            None => Err(TokenInstanceEncodeError::NullTerminator),
        }
    }

    #[inline(always)]
    pub fn omit_position(&self) -> Self {
        // clear bottom 32 bits
        TokenInstance(self.0 & (u128::MAX ^ (u32::MAX as u128)))
    }

    #[inline(always)]
    pub fn get_token(&self) -> String {
        unpack_trigram((self.0 >> 64) as u64)
    }

    #[inline(always)]
    pub fn get_offset_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline(always)]
    pub fn get_position(&self) -> Option<u32> {
        let position = self.0 as u32;
        if position & (1 << 31) != 0 {
            return Some(position & !(1 << 31));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
      #[test]
      fn test_pack_unpack_trigram(token in "\\PC{3}", offset_id in 0..u32::MAX, position in proptest::option::of((0..u32::MAX).prop_map(|v| v >> 1))) {
        let encoded = TokenInstance::encode(&token, offset_id, position).unwrap();
        let decoded_token = encoded.get_token();
        let decoded_offset_id = encoded.get_offset_id();
        let decoded_position = encoded.get_position();

        prop_assert_eq!(token, decoded_token);
        prop_assert_eq!(offset_id, decoded_offset_id);
        prop_assert_eq!(position, decoded_position);
      }

      #[test]
      fn test_omit_position(token in "\\PC{3}", offset_id in 0..u32::MAX, position1 in proptest::option::of(0..u32::MAX), position2 in proptest::option::of(0..u32::MAX)) {
        let encoded1 = TokenInstance::encode(&token, offset_id, position1).unwrap();
        let encoded2 = TokenInstance::encode(&token, offset_id, position2).unwrap();

        assert_eq!(encoded1.omit_position(), encoded2.omit_position(), "Omitting position should make two token instances equal");
        assert_eq!(encoded1.omit_position().get_token(), encoded1.get_token(), "Omitting position should not change the token");
        assert_eq!(encoded1.omit_position().get_offset_id(), encoded1.get_offset_id(), "Omitting position should not change the offset ID");
      }
    }
}
