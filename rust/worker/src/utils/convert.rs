use std::str::FromStr;

use chroma_types::{chroma_proto, CollectionUuid, ConversionError};

use crate::compactor::{OneOffCompactMessage, RebuildMessage};

impl TryFrom<chroma_proto::CompactRequest> for OneOffCompactMessage {
    type Error = ConversionError;

    fn try_from(value: chroma_proto::CompactRequest) -> Result<Self, ConversionError> {
        Ok(Self {
            collection_ids: value
                .ids
                .ok_or(ConversionError::DecodeError)?
                .ids
                .into_iter()
                .map(|id| CollectionUuid::from_str(&id))
                .collect::<Result<_, _>>()
                .map_err(|_| ConversionError::DecodeError)?,
        })
    }
}

impl TryFrom<chroma_proto::RebuildRequest> for RebuildMessage {
    type Error = ConversionError;

    fn try_from(value: chroma_proto::RebuildRequest) -> Result<Self, ConversionError> {
        Ok(Self {
            collection_ids: value
                .ids
                .ok_or(ConversionError::DecodeError)?
                .ids
                .into_iter()
                .map(|id| CollectionUuid::from_str(&id))
                .collect::<Result<_, _>>()
                .map_err(|_| ConversionError::DecodeError)?,
        })
    }
}
