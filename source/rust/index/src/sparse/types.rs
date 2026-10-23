use base64::{prelude::BASE64_STANDARD, DecodeError, Engine};
use thiserror::Error;

pub const DEFAULT_BLOCK_SIZE: u32 = 128;

// NOTE: This is a temporary hack to store dimension id in prefix of blockfile.
// This should be removed once we have generic prefix type.

pub const DIMENSION_PREFIX: &str = "DIM";

#[derive(Debug, Error)]
pub enum Base64DecodeError {
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error("Unable to convert bytes to u32")]
    Parse,
}

pub fn encode_u32(value: u32) -> String {
    BASE64_STANDARD.encode(value.to_le_bytes())
}

pub fn decode_u32(code: &str) -> Result<u32, Base64DecodeError> {
    let le_bytes: [u8; 4] = BASE64_STANDARD
        .decode(code)?
        .try_into()
        .map_err(|_| Base64DecodeError::Parse)?;
    Ok(u32::from_le_bytes(le_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_u32() {
        assert_eq!(
            decode_u32(&encode_u32(42)).expect("Encoding should be valid"),
            42
        );
    }
}
