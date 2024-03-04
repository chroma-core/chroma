use super::{
    ConversionError, Operation, OperationConversionError, ScalarEncoding,
    ScalarEncodingConversionError, SeqId, UpdateMetadata, UpdateMetadataValueConversionError,
};
use crate::{
    chroma_proto,
    errors::{ChromaError, ErrorCodes},
};

use chroma_proto::RecordLog;
use chroma_proto::SubmitEmbeddingRecord;
use num_bigint::BigInt;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct EmbeddingRecord {
    pub(crate) id: String,
    pub(crate) seq_id: SeqId,
    pub(crate) embedding: Option<Vec<f32>>, // NOTE: we only support float32 embeddings for now
    pub(crate) encoding: Option<ScalarEncoding>,
    pub(crate) metadata: Option<UpdateMetadata>,
    pub(crate) operation: Operation,
    pub(crate) collection_id: Uuid,
}

pub(crate) type SubmitEmbeddingRecordWithSeqId = (chroma_proto::SubmitEmbeddingRecord, SeqId);

#[derive(Error, Debug)]
pub(crate) enum EmbeddingRecordConversionError {
    #[error("Invalid UUID")]
    InvalidUuid,
    #[error(transparent)]
    DecodeError(#[from] ConversionError),
    #[error(transparent)]
    OperationConversionError(#[from] OperationConversionError),
    #[error(transparent)]
    ScalarEncodingConversionError(#[from] ScalarEncodingConversionError),
    #[error(transparent)]
    UpdateMetadataValueConversionError(#[from] UpdateMetadataValueConversionError),
    #[error(transparent)]
    VectorConversionError(#[from] VectorConversionError),
}

impl_base_convert_error!(EmbeddingRecordConversionError, {
    EmbeddingRecordConversionError::InvalidUuid => ErrorCodes::InvalidArgument,
    EmbeddingRecordConversionError::OperationConversionError(inner) => inner.code(),
    EmbeddingRecordConversionError::ScalarEncodingConversionError(inner) => inner.code(),
    EmbeddingRecordConversionError::UpdateMetadataValueConversionError(inner) => inner.code(),
    EmbeddingRecordConversionError::VectorConversionError(inner) => inner.code(),
});

impl TryFrom<SubmitEmbeddingRecordWithSeqId> for EmbeddingRecord {
    type Error = EmbeddingRecordConversionError;

    fn try_from(
        proto_submit_with_seq_id: SubmitEmbeddingRecordWithSeqId,
    ) -> Result<Self, Self::Error> {
        let proto_submit = proto_submit_with_seq_id.0;
        let seq_id = proto_submit_with_seq_id.1;
        let op = match proto_submit.operation.try_into() {
            Ok(op) => op,
            Err(e) => return Err(EmbeddingRecordConversionError::OperationConversionError(e)),
        };

        let collection_uuid = match Uuid::try_parse(&proto_submit.collection_id) {
            Ok(uuid) => uuid,
            Err(_) => return Err(EmbeddingRecordConversionError::InvalidUuid),
        };

        let (embedding, encoding) = match proto_submit.vector {
            Some(proto_vector) => match proto_vector.try_into() {
                Ok((embedding, encoding)) => (Some(embedding), Some(encoding)),
                Err(e) => return Err(EmbeddingRecordConversionError::VectorConversionError(e)),
            },
            // If there is no vector, there is no encoding
            None => (None, None),
        };

        let metadata: Option<UpdateMetadata> = match proto_submit.metadata {
            Some(proto_metadata) => match proto_metadata.try_into() {
                Ok(metadata) => Some(metadata),
                Err(e) => {
                    return Err(
                        EmbeddingRecordConversionError::UpdateMetadataValueConversionError(e),
                    )
                }
            },
            None => None,
        };

        Ok(EmbeddingRecord {
            id: proto_submit.id,
            seq_id: seq_id,
            embedding: embedding,
            encoding: encoding,
            metadata: metadata,
            operation: op,
            collection_id: collection_uuid,
        })
    }
}

impl TryFrom<RecordLog> for EmbeddingRecord {
    type Error = EmbeddingRecordConversionError;

    fn try_from(record_log: RecordLog) -> Result<Self, Self::Error> {
        let proto_submit = record_log
            .record
            .ok_or(EmbeddingRecordConversionError::DecodeError(
                ConversionError::DecodeError,
            ))?;

        let seq_id = BigInt::from(record_log.log_id);
        let op = match proto_submit.operation.try_into() {
            Ok(op) => op,
            Err(e) => return Err(EmbeddingRecordConversionError::OperationConversionError(e)),
        };

        let collection_uuid = match Uuid::try_parse(&proto_submit.collection_id) {
            Ok(uuid) => uuid,
            Err(_) => return Err(EmbeddingRecordConversionError::InvalidUuid),
        };

        let (embedding, encoding) = match proto_submit.vector {
            Some(proto_vector) => match proto_vector.try_into() {
                Ok((embedding, encoding)) => (Some(embedding), Some(encoding)),
                Err(e) => return Err(EmbeddingRecordConversionError::VectorConversionError(e)),
            },
            // If there is no vector, there is no encoding
            None => (None, None),
        };

        let metadata: Option<UpdateMetadata> = match proto_submit.metadata {
            Some(proto_metadata) => match proto_metadata.try_into() {
                Ok(metadata) => Some(metadata),
                Err(e) => {
                    return Err(
                        EmbeddingRecordConversionError::UpdateMetadataValueConversionError(e),
                    )
                }
            },
            None => None,
        };

        Ok(EmbeddingRecord {
            id: proto_submit.id,
            seq_id: seq_id,
            embedding: embedding,
            encoding: encoding,
            metadata: metadata,
            operation: op,
            collection_id: collection_uuid,
        })
    }
}

/*
===========================================
Vector
===========================================
*/
impl TryFrom<chroma_proto::Vector> for (Vec<f32>, ScalarEncoding) {
    type Error = VectorConversionError;

    fn try_from(proto_vector: chroma_proto::Vector) -> Result<Self, Self::Error> {
        let out_encoding: ScalarEncoding = match proto_vector.encoding.try_into() {
            Ok(encoding) => encoding,
            Err(e) => return Err(VectorConversionError::ScalarEncodingConversionError(e)),
        };

        if out_encoding != ScalarEncoding::FLOAT32 {
            // We only support float32 embeddings for now
            return Err(VectorConversionError::UnsupportedEncoding);
        }

        let out_vector = vec_to_f32(&proto_vector.vector);
        match (out_vector, out_encoding) {
            (Ok(vector), encoding) => Ok((vector.to_vec(), encoding)),
            _ => Err(VectorConversionError::DecodeError(
                ConversionError::DecodeError,
            )),
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum VectorConversionError {
    #[error("Invalid byte length, must be divisible by 4")]
    InvalidByteLength,
    #[error(transparent)]
    ScalarEncodingConversionError(#[from] ScalarEncodingConversionError),
    #[error("Unsupported encoding")]
    UnsupportedEncoding,
    #[error(transparent)]
    DecodeError(#[from] ConversionError),
}

impl_base_convert_error!(VectorConversionError, {
    VectorConversionError::InvalidByteLength => ErrorCodes::InvalidArgument,
    VectorConversionError::UnsupportedEncoding => ErrorCodes::InvalidArgument,
    VectorConversionError::ScalarEncodingConversionError(inner) => inner.code(),
});

/// Converts a vector of bytes to a vector of f32s
/// # WARNING
/// - This will only work if the machine is little endian since protobufs are little endian
/// - TODO: convert to big endian if the machine is big endian
/// # Notes
/// This method internally uses unsafe code to convert the bytes to f32s
fn vec_to_f32(bytes: &[u8]) -> Result<&[f32], VectorConversionError> {
    // Transmutes a vector of bytes into vector of f32s

    if bytes.len() % 4 != 0 {
        return Err(VectorConversionError::InvalidByteLength);
    }

    unsafe {
        let (pre, mid, post) = bytes.align_to::<f32>();
        if pre.len() != 0 || post.len() != 0 {
            return Err(VectorConversionError::InvalidByteLength);
        }
        return Ok(mid);
    }
}

fn f32_to_vec(vector: &[f32]) -> Vec<u8> {
    unsafe {
        std::slice::from_raw_parts(
            vector.as_ptr() as *const u8,
            vector.len() * std::mem::size_of::<f32>(),
        )
    }
    .to_vec()
}

impl TryFrom<(Vec<f32>, ScalarEncoding, usize)> for chroma_proto::Vector {
    type Error = VectorConversionError;

    fn try_from(
        (vector, encoding, dimension): (Vec<f32>, ScalarEncoding, usize),
    ) -> Result<Self, Self::Error> {
        let proto_vector = chroma_proto::Vector {
            vector: f32_to_vec(&vector),
            encoding: encoding as i32,
            dimension: dimension as i32,
        };
        Ok(proto_vector)
    }
}

/*
===========================================
Vector Embedding Record
===========================================
*/

#[derive(Debug)]
pub(crate) struct VectorEmbeddingRecord {
    pub(crate) id: String,
    pub(crate) seq_id: SeqId,
    pub(crate) vector: Vec<f32>,
}

/*
===========================================
Vector Query Result
===========================================
 */

#[derive(Debug)]
pub(crate) struct VectorQueryResult {
    pub(crate) id: String,
    pub(crate) seq_id: SeqId,
    pub(crate) distance: f32,
    pub(crate) vector: Option<Vec<f32>>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use num_bigint::BigInt;

    use super::*;
    use crate::{chroma_proto, types::UpdateMetadataValue};

    fn as_byte_view(input: &[f32]) -> Vec<u8> {
        unsafe {
            std::slice::from_raw_parts(
                input.as_ptr() as *const u8,
                input.len() * std::mem::size_of::<f32>(),
            )
        }
        .to_vec()
    }

    #[test]
    fn test_embedding_record_try_from() {
        let mut metadata = chroma_proto::UpdateMetadata {
            metadata: HashMap::new(),
        };
        metadata.metadata.insert(
            "foo".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(42)),
            },
        );
        let proto_vector = chroma_proto::Vector {
            vector: as_byte_view(&[1.0, 2.0, 3.0]),
            encoding: chroma_proto::ScalarEncoding::Float32 as i32,
            dimension: 3,
        };
        let proto_submit = chroma_proto::SubmitEmbeddingRecord {
            id: "00000000-0000-0000-0000-000000000000".to_string(),
            vector: Some(proto_vector),
            metadata: Some(metadata),
            operation: chroma_proto::Operation::Add as i32,
            collection_id: "00000000-0000-0000-0000-000000000000".to_string(),
        };
        let converted_embedding_record: EmbeddingRecord =
            EmbeddingRecord::try_from((proto_submit, BigInt::from(42))).unwrap();
        assert_eq!(converted_embedding_record.id, Uuid::nil().to_string());
        assert_eq!(converted_embedding_record.seq_id, BigInt::from(42));
        assert_eq!(
            converted_embedding_record.embedding,
            Some(vec![1.0, 2.0, 3.0])
        );
        assert_eq!(
            converted_embedding_record.encoding,
            Some(ScalarEncoding::FLOAT32)
        );
        let metadata = converted_embedding_record.metadata.unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("foo").unwrap(), &UpdateMetadataValue::Int(42));
        assert_eq!(converted_embedding_record.operation, Operation::Add);
        assert_eq!(converted_embedding_record.collection_id, Uuid::nil());
    }

    #[test]
    fn test_embedding_record_try_from_record_log() {
        let mut metadata = chroma_proto::UpdateMetadata {
            metadata: HashMap::new(),
        };
        metadata.metadata.insert(
            "foo".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(42)),
            },
        );
        let proto_vector = chroma_proto::Vector {
            vector: as_byte_view(&[1.0, 2.0, 3.0]),
            encoding: chroma_proto::ScalarEncoding::Float32 as i32,
            dimension: 3,
        };
        let proto_submit = chroma_proto::SubmitEmbeddingRecord {
            id: "00000000-0000-0000-0000-000000000000".to_string(),
            vector: Some(proto_vector),
            metadata: Some(metadata),
            operation: chroma_proto::Operation::Add as i32,
            collection_id: "00000000-0000-0000-0000-000000000000".to_string(),
        };
        let record_log = chroma_proto::RecordLog {
            log_id: 42,
            record: Some(proto_submit),
        };
        let converted_embedding_record: EmbeddingRecord =
            EmbeddingRecord::try_from(record_log).unwrap();
        assert_eq!(converted_embedding_record.id, Uuid::nil().to_string());
        assert_eq!(converted_embedding_record.seq_id, BigInt::from(42));
        assert_eq!(
            converted_embedding_record.embedding,
            Some(vec![1.0, 2.0, 3.0])
        );
        assert_eq!(
            converted_embedding_record.encoding,
            Some(ScalarEncoding::FLOAT32)
        );
        let metadata = converted_embedding_record.metadata.unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("foo").unwrap(), &UpdateMetadataValue::Int(42));
        assert_eq!(converted_embedding_record.operation, Operation::Add);
        assert_eq!(converted_embedding_record.collection_id, Uuid::nil());
    }
}
