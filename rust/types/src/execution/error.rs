use super::operator::ScanToProtoError;
use crate::{
    CollectionConversionError, MetadataValueConversionError, SegmentConversionError,
    VectorConversionError, WhereConversionError,
};
use thiserror::Error;
use tonic::Status;

#[derive(Debug, Error)]
pub enum QueryConversionError {
    #[error("Error parsing collection: {0}")]
    Collection(#[from] CollectionConversionError),
    #[error("Error decoding field: {0}")]
    Field(String),
    #[error("Error parsing metadata: {0}")]
    Metadata(#[from] MetadataValueConversionError),
    #[error("Error parsing segment: {0}")]
    Segment(#[from] SegmentConversionError),
    #[error("Error parsing vector: {0}")]
    Vector(#[from] VectorConversionError),
    #[error("Error parsing where clause: {0}")]
    Where(#[from] WhereConversionError),
    #[error("Error parsing scan: {0}")]
    Scan(#[from] ScanToProtoError),
    #[error("Validation error: {0}")]
    Validation(String),
}

impl QueryConversionError {
    pub fn field(name: impl ToString) -> Self {
        Self::Field(name.to_string())
    }

    pub fn validation(msg: impl ToString) -> Self {
        Self::Validation(msg.to_string())
    }
}

impl From<QueryConversionError> for Status {
    fn from(value: QueryConversionError) -> Self {
        Self::invalid_argument(value.to_string())
    }
}
