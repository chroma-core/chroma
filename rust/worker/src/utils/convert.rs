use std::str::FromStr;

use chroma_types::{chroma_proto, CollectionUuid, ConversionError, SegmentScope};

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
        let collection_ids = value
            .ids
            .ok_or(ConversionError::DecodeError)?
            .ids
            .into_iter()
            .map(|id| CollectionUuid::from_str(&id))
            .collect::<Result<_, _>>()
            .map_err(|_| ConversionError::DecodeError)?;

        let segment_scopes = value
            .segment_scopes
            .into_iter()
            .map(|s| SegmentScope::try_from(s).map_err(|_| ConversionError::DecodeError))
            .collect::<Result<_, _>>()?;

        Ok(Self {
            collection_ids,
            segment_scopes,
        })
    }
}
