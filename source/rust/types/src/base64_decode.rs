use base64::{engine::general_purpose, Engine as _};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Base64DecodeError {
    #[error("Invalid base64 string: {0}")]
    InvalidBase64(#[from] base64::DecodeError),
    #[error("Invalid byte length: {byte_length} bytes cannot be converted to f32 values (must be divisible by 4)")]
    InvalidByteLength { byte_length: usize },
    #[error("Failed to convert embedding {embedding_index} to byte array")]
    EmbeddingConversionFailed { embedding_index: usize },
}

impl ChromaError for Base64DecodeError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum EmbeddingsPayload {
    JsonArrays(Vec<Vec<f32>>),
    Base64Binary(Vec<String>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum UpdateEmbeddingsPayload {
    JsonArrays(Vec<Option<Vec<f32>>>),
    Base64Binary(Vec<Option<String>>),
}

pub fn decode_embeddings(
    embeddings: EmbeddingsPayload,
) -> Result<Vec<Vec<f32>>, Base64DecodeError> {
    match embeddings {
        EmbeddingsPayload::Base64Binary(base64_strings) => {
            Ok(decode_base64_embeddings(&base64_strings)?)
        }
        EmbeddingsPayload::JsonArrays(arrays) => Ok(arrays),
    }
}

pub fn maybe_decode_update_embeddings(
    embeddings: Option<UpdateEmbeddingsPayload>,
) -> Result<Option<Vec<Option<Vec<f32>>>>, Base64DecodeError> {
    match embeddings {
        Some(UpdateEmbeddingsPayload::Base64Binary(base64_data)) => {
            Ok(Some(decode_base64_update_embeddings(&base64_data)?))
        }
        Some(UpdateEmbeddingsPayload::JsonArrays(arrays)) => Ok(Some(arrays)),
        None => Ok(None),
    }
}

pub fn decode_base64_embeddings(
    base64_strings: &Vec<String>,
) -> Result<Vec<Vec<f32>>, Base64DecodeError> {
    let mut result = Vec::with_capacity(base64_strings.len());

    for base64_str in base64_strings {
        let floats = decode_base64_embedding(base64_str)?;

        result.push(floats);
    }

    Ok(result)
}

pub fn decode_base64_update_embeddings(
    base64_data: &Vec<Option<String>>,
) -> Result<Vec<Option<Vec<f32>>>, Base64DecodeError> {
    let mut result = Vec::with_capacity(base64_data.len());

    for base64_str in base64_data {
        if let Some(base64_str) = base64_str {
            let floats = decode_base64_embedding(base64_str)?;

            result.push(Some(floats));
        } else {
            result.push(None);
        }
    }

    Ok(result)
}

pub fn decode_base64_embedding(base64_str: &String) -> Result<Vec<f32>, Base64DecodeError> {
    let bytes = general_purpose::STANDARD.decode(base64_str)?;

    let float_count = bytes.len() / 4;
    if bytes.len() % 4 != 0 {
        return Err(Base64DecodeError::InvalidByteLength {
            byte_length: bytes.len(),
        });
    }

    let mut floats = Vec::with_capacity(float_count);
    for (embedding_index, chunk) in bytes.chunks_exact(4).enumerate() {
        let float_bytes: [u8; 4] = chunk
            .try_into()
            .map_err(|_| Base64DecodeError::EmbeddingConversionFailed { embedding_index })?;
        // handles little endian encoding
        floats.push(f32::from_le_bytes(float_bytes));
    }

    Ok(floats)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "testing")]
    use proptest::prelude::*;

    #[test]
    fn test_invalid_base64_returns_error() {
        let invalid_base64 = "invalid!@#$".to_string();
        let result = decode_base64_embedding(&invalid_base64);
        assert!(matches!(result, Err(Base64DecodeError::InvalidBase64(_))));
    }

    #[test]
    fn test_invalid_byte_length_returns_error() {
        // This is valid base64 but encodes 3 bytes (not divisible by 4)
        let invalid_length_base64 = "YWJj".to_string(); // "abc" = 3 bytes
        let result = decode_base64_embedding(&invalid_length_base64);
        assert!(matches!(
            result,
            Err(Base64DecodeError::InvalidByteLength { byte_length: 3 })
        ));
    }

    #[test]
    fn test_get_embeddings_propagates_error() {
        let invalid_embeddings = EmbeddingsPayload::Base64Binary(vec!["invalid!@#$".to_string()]);
        let result = decode_embeddings(invalid_embeddings);

        assert!(matches!(result, Err(Base64DecodeError::InvalidBase64(_))));
    }

    #[test]
    fn test_valid_base64_decoding() {
        // Valid base64 encoding 4 bytes (1 f32)
        let valid_base64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            1.0f32.to_le_bytes(),
        );
        let result = decode_base64_embedding(&valid_base64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![1.0f32]);
    }

    #[test]
    fn test_multiple_embeddings_with_one_invalid() {
        let valid_base64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            1.0f32.to_le_bytes(),
        );
        let embeddings =
            EmbeddingsPayload::Base64Binary(vec![valid_base64, "invalid!@#$".to_string()]);

        let result = decode_embeddings(embeddings);
        assert!(matches!(result, Err(Base64DecodeError::InvalidBase64(_))));
    }

    #[test]
    fn test_decode_base64_embedding() {
        let valid_base64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            1.0f32.to_le_bytes(),
        );
        let result = decode_base64_embedding(&valid_base64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![1.0f32]);
    }

    #[test]
    fn test_decode_base64_update_embeddings() {
        let valid_base64s: Vec<Option<String>> = vec![
            Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                1.0f32.to_le_bytes(),
            )),
            Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                2.0f32.to_le_bytes(),
            )),
            None,
            Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                3.0f32.to_le_bytes(),
            )),
            None,
        ];
        let result = decode_base64_update_embeddings(&valid_base64s);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec![
                Some(vec![1.0f32]),
                Some(vec![2.0f32]),
                None,
                Some(vec![3.0f32]),
                None,
            ]
        );
    }

    #[test]
    fn test_decode_base64_embeddings() {
        let valid_base64s = vec![
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                1.0f32.to_le_bytes(),
            ),
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                2.0f32.to_le_bytes(),
            ),
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                3.0f32.to_le_bytes(),
            ),
        ];
        let result = decode_base64_embeddings(&valid_base64s);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec![vec![1.0f32], vec![2.0f32], vec![3.0f32]]
        );
    }

    #[cfg(feature = "testing")]
    fn encode_floats_to_base64(floats: &[f32]) -> String {
        let mut bytes = Vec::with_capacity(floats.len() * 4);
        for &f in floats {
            bytes.extend_from_slice(&f.to_le_bytes());
        }
        general_purpose::STANDARD.encode(&bytes)
    }

    #[cfg(feature = "testing")]
    fn embeddings_strategy() -> impl Strategy<Value = Vec<Vec<f32>>> {
        any::<Vec<Vec<f32>>>()
    }

    #[cfg(feature = "testing")]
    proptest! {
        #[test]
        fn test_decode_base64_embeddings_prop(embeddings in embeddings_strategy()) {
            let base64_strings = embeddings.iter().map(|e| encode_floats_to_base64(e)).collect();
            let result = decode_base64_embeddings(&base64_strings).unwrap();
            for (original, decoded) in embeddings.iter().zip(result.iter()) {
                prop_assert_eq!(original, decoded);
            }
        }
    }
}
