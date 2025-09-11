use super::{
    error::QueryConversionError,
    operator::{
        Filter, KnnBatch, KnnProjection, Limit, Projection, Rank, Scan, ScanToProtoError, Select,
    },
};
use crate::chroma_proto;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::{
    openapi::{
        schema::{Schema, SchemaType},
        ArrayBuilder, Object, ObjectBuilder, RefOr, Type,
    },
    PartialSchema,
};
use validator::Validate;

#[derive(Error, Debug)]
pub enum PlanToProtoError {
    #[error("Failed to convert scan to proto: {0}")]
    Scan(#[from] ScanToProtoError),
}

/// The `Count` plan shoud ouutput the total number of records in the collection
#[derive(Clone)]
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

impl TryFrom<Count> for chroma_proto::CountPlan {
    type Error = PlanToProtoError;

    fn try_from(value: Count) -> Result<Self, Self::Error> {
        Ok(Self {
            scan: Some(value.scan.try_into()?),
        })
    }
}

/// The `Get` plan should output records matching the specified filter and limit in the collection
#[derive(Clone, Debug)]
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
            scan: Some(value.scan.try_into()?),
            filter: Some(value.filter.try_into()?),
            limit: Some(value.limit.into()),
            projection: Some(value.proj.into()),
        })
    }
}

/// The `Knn` plan should output records nearest to the target embeddings that matches the specified filter
#[derive(Clone, Debug)]
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
            scan: Some(value.scan.try_into()?),
            filter: Some(value.filter.try_into()?),
            knn: Some(value.knn.try_into()?),
            projection: Some(value.proj.into()),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Validate)]
pub struct SearchPayload {
    #[serde(default)]
    pub filter: Filter,
    #[serde(default)]
    pub rank: Rank,
    #[serde(default)]
    pub limit: Limit,
    #[serde(default)]
    pub select: Select,
}

impl PartialSchema for SearchPayload {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::Object))
                .description(Some("Payload for hybrid search"))
                .property(
                    "filter",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Type(Type::Object))
                        .description(Some("Filter criteria for search"))
                        .property(
                            "query_ids",
                            ArrayBuilder::new()
                                .items(Object::with_type(SchemaType::Type(Type::String))),
                        )
                        .property(
                            "where_clause",
                            Object::with_type(SchemaType::Type(Type::Object)),
                        ),
                )
                .property(
                    "rank",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Type(Type::Object))
                        .description(Some("Ranking expression for hybrid search"))
                        .additional_properties(Some(Schema::Object(Object::with_type(
                            SchemaType::Type(Type::Object),
                        )))),
                )
                .property(
                    "limit",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Type(Type::Object))
                        .property("skip", Object::with_type(SchemaType::Type(Type::Integer)))
                        .property("fetch", Object::with_type(SchemaType::Type(Type::Integer)))
                        .required("skip"),
                )
                .property(
                    "select",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Type(Type::Object))
                        .property(
                            "fields",
                            ArrayBuilder::new()
                                .items(Object::with_type(SchemaType::Type(Type::String))),
                        )
                        .required("fields"),
                )
                .build(),
        ))
    }
}

impl utoipa::ToSchema for SearchPayload {}

impl TryFrom<chroma_proto::SearchPayload> for SearchPayload {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::SearchPayload) -> Result<Self, Self::Error> {
        Ok(Self {
            filter: value
                .filter
                .ok_or(QueryConversionError::field("filter"))?
                .try_into()?,
            rank: value
                .rank
                .ok_or(QueryConversionError::field("rank"))?
                .try_into()?,
            limit: value
                .limit
                .ok_or(QueryConversionError::field("limit"))?
                .into(),
            select: value
                .select
                .ok_or(QueryConversionError::field("select"))?
                .try_into()?,
        })
    }
}

impl TryFrom<SearchPayload> for chroma_proto::SearchPayload {
    type Error = QueryConversionError;

    fn try_from(value: SearchPayload) -> Result<Self, Self::Error> {
        Ok(Self {
            filter: Some(value.filter.try_into()?),
            rank: Some(value.rank.try_into()?),
            limit: Some(value.limit.into()),
            select: Some(value.select.try_into()?),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Search {
    pub scan: Scan,
    pub payloads: Vec<SearchPayload>,
}

impl TryFrom<chroma_proto::SearchPlan> for Search {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::SearchPlan) -> Result<Self, Self::Error> {
        Ok(Self {
            scan: value
                .scan
                .ok_or(QueryConversionError::field("scan"))?
                .try_into()?,
            payloads: value
                .payloads
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<Search> for chroma_proto::SearchPlan {
    type Error = QueryConversionError;

    fn try_from(value: Search) -> Result<Self, Self::Error> {
        Ok(Self {
            scan: Some(value.scan.try_into()?),
            payloads: value
                .payloads
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}
