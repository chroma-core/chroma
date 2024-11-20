use chroma_types::{
    chroma_proto::{self, GetResult, KnnBatchResult, KnnResult},
    ConversionError, ScalarEncoding, Where,
};

use crate::execution::operators::{
    filter::FilterOperator,
    knn::KnnOperator,
    knn_projection::{KnnProjectionOperator, KnnProjectionOutput, KnnProjectionRecord},
    limit::LimitOperator,
    projection::{ProjectionOperator, ProjectionOutput, ProjectionRecord},
};

impl TryFrom<chroma_proto::FilterOperator> for FilterOperator {
    type Error = ConversionError;

    fn try_from(value: chroma_proto::FilterOperator) -> Result<Self, ConversionError> {
        let where_metadata_clause = value
            .r#where
            .map(|w| w.try_into())
            .transpose()
            .map_err(|_| ConversionError::DecodeError)?;
        let where_document_clause = value
            .where_document
            .map(|w| w.try_into())
            .transpose()
            .map_err(|_| ConversionError::DecodeError)?;
        let where_clause = match (where_metadata_clause, where_document_clause) {
            (Some(wc), Some(wdc)) => Some(Where::conjunction(vec![wc, wdc])),
            (Some(c), None) | (None, Some(c)) => Some(c),
            _ => None,
        };

        Ok(Self {
            query_ids: value.ids.map(|uids| uids.ids),
            where_clause,
        })
    }
}

impl From<chroma_proto::LimitOperator> for LimitOperator {
    fn from(value: chroma_proto::LimitOperator) -> Self {
        Self {
            skip: value.skip,
            fetch: value.fetch,
        }
    }
}

impl From<chroma_proto::ProjectionOperator> for ProjectionOperator {
    fn from(value: chroma_proto::ProjectionOperator) -> Self {
        Self {
            document: value.document,
            embedding: value.embedding,
            metadata: value.metadata,
        }
    }
}

impl TryFrom<chroma_proto::KnnProjectionOperator> for KnnProjectionOperator {
    type Error = ConversionError;

    fn try_from(value: chroma_proto::KnnProjectionOperator) -> Result<Self, ConversionError> {
        Ok(Self {
            projection: value.projection.ok_or(ConversionError::DecodeError)?.into(),
            distance: value.distance,
        })
    }
}

impl TryFrom<ProjectionRecord> for chroma_proto::ProjectionRecord {
    type Error = ConversionError;

    fn try_from(value: ProjectionRecord) -> Result<Self, ConversionError> {
        Ok(Self {
            id: value.id,
            document: value.document,
            embedding: value
                .embedding
                .map(|embedding| {
                    let embedding_dimension = embedding.len();
                    chroma_proto::Vector::try_from((
                        embedding,
                        ScalarEncoding::FLOAT32,
                        embedding_dimension,
                    ))
                })
                .transpose()
                .map_err(|_| ConversionError::DecodeError)?,
            metadata: value.metadata.map(|metadata| metadata.into()),
        })
    }
}

impl TryFrom<ProjectionOutput> for GetResult {
    type Error = ConversionError;

    fn try_from(value: ProjectionOutput) -> Result<Self, ConversionError> {
        Ok(Self {
            records: value
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<KnnProjectionRecord> for chroma_proto::KnnProjectionRecord {
    type Error = ConversionError;

    fn try_from(value: KnnProjectionRecord) -> Result<Self, ConversionError> {
        Ok(chroma_proto::KnnProjectionRecord {
            record: Some(value.record.try_into()?),
            distance: value.distance,
        })
    }
}

impl TryFrom<KnnProjectionOutput> for KnnResult {
    type Error = ConversionError;

    fn try_from(value: KnnProjectionOutput) -> Result<Self, ConversionError> {
        Ok(KnnResult {
            records: value
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

pub fn from_proto_knn(knn: chroma_proto::KnnOperator) -> Result<Vec<KnnOperator>, ConversionError> {
    knn.embeddings
        .into_iter()
        .map(|embedding| match embedding.try_into() {
            Ok((embedding, _)) => Ok(KnnOperator {
                embedding,
                fetch: knn.fetch,
            }),
            Err(_) => Err(ConversionError::DecodeError),
        })
        .collect()
}

pub fn to_proto_knn_batch_result(
    results: Vec<KnnProjectionOutput>,
) -> Result<KnnBatchResult, ConversionError> {
    Ok(KnnBatchResult {
        results: results
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?,
    })
}
