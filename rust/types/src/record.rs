use super::{
    ConversionError, Operation, OperationConversionError, ScalarEncoding,
    ScalarEncodingConversionError, UpdateMetadata, UpdateMetadataValue,
    UpdateMetadataValueConversionError,
};
use crate::chroma_proto;
use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct OperationRecord {
    pub id: String,
    pub embedding: Option<Vec<f32>>, // NOTE: we only support float32 embeddings for now so this ignores the encoding
    pub encoding: Option<ScalarEncoding>,
    pub metadata: Option<UpdateMetadata>,
    // Document is implemented in the python code as a special key "chroma:document" in the metadata
    // This is ugly and clunky. In the rust code we choose to make it a separate field and
    // only let that concept live in the transport layer
    pub document: Option<String>,
    pub operation: Operation,
}

#[derive(Clone, Debug)]
pub struct LogRecord {
    pub log_offset: i64,
    pub record: OperationRecord,
}

#[derive(Error, Debug)]
pub enum RecordConversionError {
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

impl_base_convert_error!(RecordConversionError, {
    RecordConversionError::InvalidUuid => ErrorCodes::InvalidArgument,
    RecordConversionError::OperationConversionError(inner) => inner.code(),
    RecordConversionError::ScalarEncodingConversionError(inner) => inner.code(),
    RecordConversionError::UpdateMetadataValueConversionError(inner) => inner.code(),
    RecordConversionError::VectorConversionError(inner) => inner.code(),
});

impl TryFrom<chroma_proto::OperationRecord> for OperationRecord {
    type Error = RecordConversionError;

    fn try_from(
        operation_record_proto: chroma_proto::OperationRecord,
    ) -> Result<Self, Self::Error> {
        let operation = match operation_record_proto.operation.try_into() {
            Ok(op) => op,
            Err(e) => return Err(RecordConversionError::OperationConversionError(e)),
        };

        let (embedding, encoding) = match operation_record_proto.vector {
            Some(proto_vector) => match proto_vector.try_into() {
                Ok((embedding, encoding)) => (Some(embedding), Some(encoding)),
                Err(e) => return Err(RecordConversionError::VectorConversionError(e)),
            },
            // If there is no vector, there is no encoding
            None => (None, None),
        };

        let (metadata, document) = match operation_record_proto.metadata {
            Some(proto_metadata) => match UpdateMetadata::try_from(proto_metadata) {
                Ok(mut metadata) => {
                    let document = metadata.remove("chroma:document");
                    match document {
                        Some(UpdateMetadataValue::Str(document)) => {
                            (Some(metadata), Some(document))
                        }
                        _ => (Some(metadata), None),
                    }
                }
                Err(e) => return Err(RecordConversionError::UpdateMetadataValueConversionError(e)),
            },
            None => (None, None),
        };

        Ok(OperationRecord {
            id: operation_record_proto.id,
            embedding,
            encoding,
            metadata,
            document,
            operation,
        })
    }
}

impl TryFrom<chroma_proto::LogRecord> for LogRecord {
    type Error = RecordConversionError;

    fn try_from(log_record_proto: chroma_proto::LogRecord) -> Result<Self, Self::Error> {
        let record = match log_record_proto.record {
            Some(proto_record) => OperationRecord::try_from(proto_record)?,
            None => {
                return Err(RecordConversionError::DecodeError(
                    ConversionError::DecodeError,
                ))
            }
        };
        Ok(LogRecord {
            log_offset: log_record_proto.log_offset,
            record,
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
pub enum VectorConversionError {
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
        if !pre.is_empty() || !post.is_empty() {
            return Err(VectorConversionError::InvalidByteLength);
        }
        Ok(mid)
    }
}

fn f32_to_vec(vector: &[f32]) -> Vec<u8> {
    unsafe {
        std::slice::from_raw_parts(vector.as_ptr() as *const u8, std::mem::size_of_val(vector))
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
pub struct VectorEmbeddingRecord {
    pub id: String,
    pub vector: Vec<f32>,
}

/*
===========================================
Vector Query Result
===========================================
 */

#[derive(Debug)]
pub struct VectorQueryResult {
    pub id: String,
    pub distance: f32,
    pub vector: Option<Vec<f32>>,
}

/*
===========================================
Get Vector Results
===========================================
*/

#[derive(Debug)]
pub struct GetVectorsResult {
    pub ids: Vec<String>,
    pub vectors: Vec<Vec<f32>>,
}

/*
===========================================
Metadata Embedding Record
===========================================
*/

#[derive(Debug)]
pub struct MetadataEmbeddingRecord {
    pub id: String,
    pub metadata: UpdateMetadata,
}

impl From<MetadataEmbeddingRecord> for chroma_proto::MetadataEmbeddingRecord {
    fn from(record: MetadataEmbeddingRecord) -> Self {
        chroma_proto::MetadataEmbeddingRecord {
            id: record.id,
            metadata: Some(record.metadata.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{chroma_proto, UpdateMetadataValue};
    use std::collections::HashMap;
    use uuid::Uuid;

    fn as_byte_view(input: &[f32]) -> Vec<u8> {
        unsafe {
            std::slice::from_raw_parts(input.as_ptr() as *const u8, std::mem::size_of_val(input))
        }
        .to_vec()
    }

    #[test]
    fn test_operation_record_try_from() {
        let mut metadata = chroma_proto::UpdateMetadata {
            metadata: HashMap::new(),
        };
        metadata.metadata.insert(
            "foo".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::IntValue(42)),
            },
        );

        // Insert a chroma:document field
        metadata.metadata.insert(
            "chroma:document".to_string(),
            chroma_proto::UpdateMetadataValue {
                value: Some(chroma_proto::update_metadata_value::Value::StringValue(
                    "document_contents".to_string(),
                )),
            },
        );

        let proto_vector = chroma_proto::Vector {
            vector: as_byte_view(&[1.0, 2.0, 3.0]),
            encoding: chroma_proto::ScalarEncoding::Float32 as i32,
            dimension: 3,
        };
        let proto_submit = chroma_proto::OperationRecord {
            id: "00000000-0000-0000-0000-000000000000".to_string(),
            vector: Some(proto_vector),
            metadata: Some(metadata),
            operation: chroma_proto::Operation::Add as i32,
        };
        let converted_operation_record = OperationRecord::try_from(proto_submit).unwrap();
        assert_eq!(converted_operation_record.id, Uuid::nil().to_string());
        assert_eq!(
            converted_operation_record.embedding,
            Some(vec![1.0, 2.0, 3.0])
        );
        assert_eq!(
            converted_operation_record.encoding,
            Some(ScalarEncoding::FLOAT32)
        );
        assert_eq!(
            converted_operation_record.document,
            Some("document_contents".to_string())
        );
        let metadata = converted_operation_record.metadata.unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("foo").unwrap(), &UpdateMetadataValue::Int(42));
        assert_eq!(converted_operation_record.operation, Operation::Add);

        // Ensure metadata no longer has the document field
        assert_eq!(metadata.get("chroma:document"), None);
    }

    #[test]
    fn test_log_record_try_from_record_log() {
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
        let proto_submit = chroma_proto::OperationRecord {
            id: "00000000-0000-0000-0000-000000000000".to_string(),
            vector: Some(proto_vector),
            metadata: Some(metadata),
            operation: chroma_proto::Operation::Add as i32,
        };
        let record_log = chroma_proto::LogRecord {
            log_offset: 42,
            record: Some(proto_submit),
        };
        let converted_log_record = LogRecord::try_from(record_log).unwrap();
        assert_eq!(converted_log_record.record.id, Uuid::nil().to_string());
        assert_eq!(converted_log_record.log_offset, 42);
        assert_eq!(
            converted_log_record.record.embedding,
            Some(vec![1.0, 2.0, 3.0])
        );
        assert_eq!(
            converted_log_record.record.encoding,
            Some(ScalarEncoding::FLOAT32)
        );
        let metadata = converted_log_record.record.metadata.unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("foo").unwrap(), &UpdateMetadataValue::Int(42));
        assert_eq!(converted_log_record.record.operation, Operation::Add);
    }
}
