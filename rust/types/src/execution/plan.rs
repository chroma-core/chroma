use crate::chroma_proto;

use super::{
    error::QueryConversionError,
    operator::{Filter, KnnBatch, KnnProjection, Limit, Projection, Scan},
};

/// The `Count` plan shoud ouutput the total number of records in the collection
pub struct Count {
    pub scan: Scan,
}

impl TryFrom<chroma_proto::CountPlan> for Count {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::CountPlan) -> Result<Self, Self::Error> {
        Ok(Self {
            scan: value
                .scan
                .ok_or(QueryConversionError::field("scan"))?
                .try_into()?,
        })
    }
}

impl From<Count> for chroma_proto::CountPlan {
    fn from(value: Count) -> Self {
        Self {
            scan: Some(value.scan.into()),
        }
    }
}

/// The `Get` plan should output records matching the specified filter and limit in the collection
pub struct Get {
    pub scan: Scan,
    pub filter: Filter,
    pub limit: Limit,
    pub proj: Projection,
}

impl TryFrom<chroma_proto::GetPlan> for Get {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::GetPlan) -> Result<Self, Self::Error> {
        Ok(Self {
            scan: value
                .scan
                .ok_or(QueryConversionError::field("scan"))?
                .try_into()?,
            filter: value
                .filter
                .ok_or(QueryConversionError::field("filter"))?
                .try_into()?,
            limit: value
                .limit
                .ok_or(QueryConversionError::field("limit"))?
                .into(),
            proj: value
                .projection
                .ok_or(QueryConversionError::field("projection"))?
                .into(),
        })
    }
}

impl TryFrom<Get> for chroma_proto::GetPlan {
    type Error = QueryConversionError;

    fn try_from(value: Get) -> Result<Self, Self::Error> {
        Ok(Self {
            scan: Some(value.scan.into()),
            filter: Some(value.filter.try_into()?),
            limit: Some(value.limit.into()),
            projection: Some(value.proj.into()),
        })
    }
}

/// The `Knn` plan should output records nearest to the target embeddings that matches the specified filter
pub struct Knn {
    pub scan: Scan,
    pub filter: Filter,
    pub knn: KnnBatch,
    pub proj: KnnProjection,
}

impl TryFrom<chroma_proto::KnnPlan> for Knn {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::KnnPlan) -> Result<Self, Self::Error> {
        Ok(Self {
            scan: value
                .scan
                .ok_or(QueryConversionError::field("scan"))?
                .try_into()?,
            filter: value
                .filter
                .ok_or(QueryConversionError::field("filter"))?
                .try_into()?,
            knn: value
                .knn
                .ok_or(QueryConversionError::field("knn"))?
                .try_into()?,
            proj: value
                .projection
                .ok_or(QueryConversionError::field("projection"))?
                .try_into()?,
        })
    }
}

impl TryFrom<Knn> for chroma_proto::KnnPlan {
    type Error = QueryConversionError;

    fn try_from(value: Knn) -> Result<Self, Self::Error> {
        Ok(Self {
            scan: Some(value.scan.into()),
            filter: Some(value.filter.try_into()?),
            knn: Some(value.knn.try_into()?),
            projection: Some(value.proj.into()),
        })
    }
}
