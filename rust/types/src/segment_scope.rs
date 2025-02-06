use crate::{chroma_proto, ConversionError};
use chroma_error::{ChromaError, ErrorCodes};

use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub enum SegmentScope {
    VECTOR,
    METADATA,
    RECORD,
    SQLITE,
}

impl From<SegmentScope> for String {
    fn from(scope: SegmentScope) -> String {
        match scope {
            SegmentScope::VECTOR => "VECTOR".to_string(),
            SegmentScope::METADATA => "METADATA".to_string(),
            SegmentScope::RECORD => "RECORD".to_string(),
            SegmentScope::SQLITE => "SQLITE".to_string(),
        }
    }
}

impl TryFrom<&str> for SegmentScope {
    type Error = SegmentScopeConversionError;

    fn try_from(scope: &str) -> Result<Self, Self::Error> {
        match scope {
            "VECTOR" => Ok(SegmentScope::VECTOR),
            "METADATA" => Ok(SegmentScope::METADATA),
            "RECORD" => Ok(SegmentScope::RECORD),
            "SQLITE" => Ok(SegmentScope::SQLITE),
            _ => Err(SegmentScopeConversionError::InvalidScope),
        }
    }
}

#[derive(Error, Debug)]
pub enum SegmentScopeConversionError {
    #[error("Invalid segment scope, valid scopes are: Vector, Metadata")]
    InvalidScope,
    #[error(transparent)]
    DecodeError(#[from] ConversionError),
}

impl_base_convert_error!(SegmentScopeConversionError, {
    SegmentScopeConversionError::InvalidScope => ErrorCodes::InvalidArgument,
});

impl From<chroma_proto::SegmentScope> for SegmentScope {
    fn from(value: chroma_proto::SegmentScope) -> Self {
        match value {
            chroma_proto::SegmentScope::Vector => Self::VECTOR,
            chroma_proto::SegmentScope::Metadata => Self::METADATA,
            chroma_proto::SegmentScope::Record => Self::RECORD,
            chroma_proto::SegmentScope::Sqlite => Self::SQLITE,
        }
    }
}

impl From<SegmentScope> for chroma_proto::SegmentScope {
    fn from(value: SegmentScope) -> Self {
        match value {
            SegmentScope::VECTOR => Self::Vector,
            SegmentScope::METADATA => Self::Metadata,
            SegmentScope::RECORD => Self::Record,
            SegmentScope::SQLITE => Self::Sqlite,
        }
    }
}

impl TryFrom<i32> for SegmentScope {
    type Error = SegmentScopeConversionError;

    fn try_from(scope: i32) -> Result<Self, Self::Error> {
        let maybe_scope = chroma_proto::SegmentScope::try_from(scope);
        match maybe_scope {
            Ok(scope) => match scope {
                chroma_proto::SegmentScope::Vector => Ok(SegmentScope::VECTOR),
                chroma_proto::SegmentScope::Metadata => Ok(SegmentScope::METADATA),
                chroma_proto::SegmentScope::Record => Ok(SegmentScope::RECORD),
                chroma_proto::SegmentScope::Sqlite => Ok(SegmentScope::SQLITE),
            },
            Err(_) => Err(SegmentScopeConversionError::InvalidScope),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_scope_try_from() {
        let proto_scope = chroma_proto::SegmentScope::Vector;
        let converted_scope: SegmentScope = proto_scope.into();
        assert_eq!(converted_scope, SegmentScope::VECTOR);

        let proto_scope = chroma_proto::SegmentScope::Metadata;
        let converted_scope: SegmentScope = proto_scope.into();
        assert_eq!(converted_scope, SegmentScope::METADATA);

        let proto_scope = chroma_proto::SegmentScope::Sqlite;
        let converted_scope: SegmentScope = proto_scope.into();
        assert_eq!(converted_scope, SegmentScope::SQLITE);

        let proto_scope = chroma_proto::SegmentScope::Record;
        let converted_scope: SegmentScope = proto_scope.into();
        assert_eq!(converted_scope, SegmentScope::RECORD);
    }
}
