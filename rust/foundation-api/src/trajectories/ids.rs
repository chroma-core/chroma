#![allow(missing_docs)]

use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::error::TrajectoryError;
use super::limits::{ITEM_ID_WIDTH, TID_WIDTH};

/// Encode a UUID into the fixed-width base36 trajectory identifier used in keys.
///
/// # Errors
///
/// Returns [`TrajectoryError::SizeLimit`] if the configured trajectory id width
/// cannot represent a UUID.
pub fn uuid_to_tid(uuid: Uuid) -> Result<String, TrajectoryError> {
    encode_be_bytes_base36(uuid.as_bytes(), TID_WIDTH)
}

/// Decode a fixed-width base36 trajectory identifier into a UUID.
///
/// # Errors
///
/// Returns [`TrajectoryError::InvalidKey`] when `tid` has the wrong width, contains a
/// non-base36 digit, or does not fit into a UUID.
pub fn tid_to_uuid(tid: &str) -> Result<Uuid, TrajectoryError> {
    if tid.len() != TID_WIDTH {
        return Err(TrajectoryError::InvalidKey(format!(
            "tid must be {TID_WIDTH} bytes, got {}: {tid}",
            tid.len()
        )));
    }
    let bytes = decode_fixed_base36_to_be_bytes(tid, 16)?;
    let uuid_bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        TrajectoryError::InvalidKey(format!("tid did not decode to 16 bytes: {tid}"))
    })?;
    Ok(Uuid::from_bytes(uuid_bytes))
}

/// Compute the fixed-width base36 SHA-256 identifier for arbitrary bytes.
///
/// # Errors
///
/// Returns [`TrajectoryError::SizeLimit`] if the configured item id width
/// cannot represent a SHA-256 digest.
pub fn sha256_base36(bytes: &[u8]) -> Result<String, TrajectoryError> {
    encode_be_bytes_base36(&sha256_bytes(bytes), ITEM_ID_WIDTH)
}

/// Encode an ordinal index into a fixed-width lower-case base36 component.
pub(crate) fn encode_index(index: usize, width: usize) -> Result<String, TrajectoryError> {
    let value = u64::try_from(index)?;
    encode_be_bytes_base36(&value.to_be_bytes(), width)
}

/// Decode a fixed-width lower-case base36 index component.
pub(crate) fn decode_index(text: &str, width: usize) -> Result<usize, TrajectoryError> {
    if text.len() != width {
        return Err(TrajectoryError::InvalidKey(format!(
            "index must be {width} bytes, got {}: {text}",
            text.len()
        )));
    }
    let bytes = decode_fixed_base36_to_be_bytes(text, 8)?;
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        TrajectoryError::InvalidKey(format!("index did not decode to 8 bytes: {text}"))
    })?;
    Ok(usize::try_from(u64::from_be_bytes(bytes))?)
}

/// Render bytes as lower-case hexadecimal text.
pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// Compute the raw SHA-256 digest for bytes.
pub(crate) fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Encode a big-endian byte string as fixed-width lower-case base36 text.
pub(crate) fn encode_be_bytes_base36(
    bytes: &[u8],
    width: usize,
) -> Result<String, TrajectoryError> {
    let mut number = bytes.to_vec();
    let mut digits = Vec::new();

    while number.iter().any(|byte| *byte != 0) {
        let mut remainder = 0u16;
        for byte in &mut number {
            let value = remainder
                .checked_mul(256)
                .and_then(|value| value.checked_add(u16::from(*byte)))
                .ok_or_else(|| {
                    TrajectoryError::InvalidValue(
                        "base36 encoder overflowed while dividing bytes".to_string(),
                    )
                })?;
            let quotient = value.checked_div(36).ok_or_else(|| {
                TrajectoryError::InvalidValue(
                    "base36 encoder attempted division by zero".to_string(),
                )
            })?;
            *byte = u8::try_from(quotient)?;
            remainder = value.checked_rem(36).ok_or_else(|| {
                TrajectoryError::InvalidValue(
                    "base36 encoder attempted remainder by zero".to_string(),
                )
            })?;
        }
        digits.push(base36_digit(u8::try_from(remainder)?)?);
    }

    if digits.is_empty() {
        digits.push('0');
    }
    if digits.len() > width {
        return Err(TrajectoryError::SizeLimit(format!(
            "base36 value needs {} digits, width is {width}",
            digits.len()
        )));
    }

    let padding = width.checked_sub(digits.len()).ok_or_else(|| {
        TrajectoryError::SizeLimit(format!(
            "base36 value needs {} digits, width is {width}",
            digits.len()
        ))
    })?;
    let mut out = String::with_capacity(width);
    for _ in 0..padding {
        out.push('0');
    }
    for digit in digits.iter().rev() {
        out.push(*digit);
    }
    Ok(out)
}

/// Decode lower-case base36 text into a fixed-length big-endian byte string.
pub(crate) fn decode_fixed_base36_to_be_bytes(
    text: &str,
    output_len: usize,
) -> Result<Vec<u8>, TrajectoryError> {
    let mut bytes = vec![0u8; output_len];
    for byte in text.bytes() {
        let digit = decode_base36_digit(byte)?;
        let mut carry = u16::from(digit);
        for output_byte in bytes.iter_mut().rev() {
            let value = u16::from(*output_byte)
                .checked_mul(36)
                .and_then(|value| value.checked_add(carry))
                .ok_or_else(|| {
                    TrajectoryError::InvalidKey(
                        "base36 decoder overflowed while multiplying bytes".to_string(),
                    )
                })?;
            let low_byte = value.checked_rem(256).ok_or_else(|| {
                TrajectoryError::InvalidKey(
                    "base36 decoder attempted remainder by zero".to_string(),
                )
            })?;
            *output_byte = u8::try_from(low_byte)?;
            carry = value.checked_div(256).ok_or_else(|| {
                TrajectoryError::InvalidKey("base36 decoder attempted division by zero".to_string())
            })?;
        }
        if carry != 0 {
            return Err(TrajectoryError::InvalidKey(format!(
                "base36 value overflows {output_len} bytes: {text}"
            )));
        }
    }
    Ok(bytes)
}

/// Convert a base36 digit value into its lower-case ASCII character.
fn base36_digit(value: u8) -> Result<char, TrajectoryError> {
    match value {
        0..=9 => Ok(char::from(b'0'.checked_add(value).ok_or_else(|| {
            TrajectoryError::InvalidValue(format!("base36 digit overflow: {value}"))
        })?)),
        10..=35 => {
            let offset = value.checked_sub(10).ok_or_else(|| {
                TrajectoryError::InvalidValue(format!("base36 digit underflow: {value}"))
            })?;
            Ok(char::from(b'a'.checked_add(offset).ok_or_else(|| {
                TrajectoryError::InvalidValue(format!("base36 digit overflow: {value}"))
            })?))
        }
        _ => Err(TrajectoryError::InvalidValue(format!(
            "base36 digit out of range: {value}"
        ))),
    }
}

/// Decode one lower-case ASCII base36 digit into its numeric value.
fn decode_base36_digit(byte: u8) -> Result<u8, TrajectoryError> {
    match byte {
        b'0'..=b'9' => byte.checked_sub(b'0').ok_or_else(|| {
            TrajectoryError::InvalidKey(format!(
                "invalid lower-case base36 digit {:?}",
                char::from(byte)
            ))
        }),
        b'a'..=b'z' => byte
            .checked_sub(b'a')
            .and_then(|value| value.checked_add(10))
            .ok_or_else(|| {
                TrajectoryError::InvalidKey(format!(
                    "invalid lower-case base36 digit {:?}",
                    char::from(byte)
                ))
            }),
        _ => Err(TrajectoryError::InvalidKey(format!(
            "invalid lower-case base36 digit {:?}",
            char::from(byte)
        ))),
    }
}
