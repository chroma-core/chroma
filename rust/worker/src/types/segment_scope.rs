use super::ConversionError;
use crate::{
    chroma_proto,
    errors::{ChromaError, ErrorCodes},
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SegmentScope {
    VECTOR,
    METADATA,
    RECORD,
    SQLITE,
}

#[derive(Error, Debug)]
pub(crate) enum SegmentScopeConversionError {
    #[error("Invalid segment scope, valid scopes are: Vector, Metadata")]
    InvalidScope,
    #[error(transparent)]
    DecodeError(#[from] ConversionError),
}

impl_base_convert_error!(SegmentScopeConversionError, {
    SegmentScopeConversionError::InvalidScope => ErrorCodes::InvalidArgument,
});

impl TryFrom<chroma_proto::SegmentScope> for SegmentScope {
    type Error = SegmentScopeConversionError;

    fn try_from(scope: chroma_proto::SegmentScope) -> Result<Self, Self::Error> {
        match scope {
            chroma_proto::SegmentScope::Vector => Ok(SegmentScope::VECTOR),
            chroma_proto::SegmentScope::Metadata => Ok(SegmentScope::METADATA),
            chroma_proto::SegmentScope::Record => Ok(SegmentScope::RECORD),
            chroma_proto::SegmentScope::Sqlite => Ok(SegmentScope::SQLITE),
            _ => Err(SegmentScopeConversionError::InvalidScope),
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
                _ => Err(SegmentScopeConversionError::InvalidScope),
            },
            Err(_) => Err(SegmentScopeConversionError::DecodeError(
                ConversionError::DecodeError,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_scope_try_from() {
        let proto_scope = chroma_proto::SegmentScope::Vector;
        let converted_scope: SegmentScope = proto_scope.try_into().unwrap();
        assert_eq!(converted_scope, SegmentScope::VECTOR);

        let proto_scope = chroma_proto::SegmentScope::Metadata;
        let converted_scope: SegmentScope = proto_scope.try_into().unwrap();
        assert_eq!(converted_scope, SegmentScope::METADATA);

        let proto_scope = chroma_proto::SegmentScope::Sqlite;
        let converted_scope: SegmentScope = proto_scope.try_into().unwrap();
        assert_eq!(converted_scope, SegmentScope::SQLITE);

        let proto_scope = chroma_proto::SegmentScope::Record;
        let converted_scope: SegmentScope = proto_scope.try_into().unwrap();
        assert_eq!(converted_scope, SegmentScope::RECORD);
    }
}
