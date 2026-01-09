use super::{
    error::QueryConversionError,
    operator::{
        Filter, GroupBy, KnnBatch, KnnProjection, Limit, Projection, Rank, Scan, ScanToProtoError,
        Select,
    },
};
use crate::{
    chroma_proto,
    operator::{Key, RankExpr},
    validators::{validate_group_by, validate_rank, validate_search_payload},
    Where,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
#[cfg(feature = "utoipa")]
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

/// A search payload for the hybrid search API.
///
/// Combines filtering, ranking, pagination, and field selection into a single query.
/// Use the builder methods to construct complex searches with a fluent interface.
///
/// # Examples
///
/// ## Basic vector search
///
/// ```
/// use chroma_types::plan::SearchPayload;
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
///
/// let search = SearchPayload::default()
///     .rank(RankExpr::Knn {
///         query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///         key: Key::Embedding,
///         limit: 100,
///         default: None,
///         return_rank: false,
///     })
///     .limit(Some(10), 0)
///     .select([Key::Document, Key::Score]);
/// ```
///
/// ## Filtered search
///
/// ```
/// use chroma_types::plan::SearchPayload;
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
///
/// let search = SearchPayload::default()
///     .r#where(
///         Key::field("status").eq("published")
///             & Key::field("year").gte(2020)
///     )
///     .rank(RankExpr::Knn {
///         query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///         key: Key::Embedding,
///         limit: 200,
///         default: None,
///         return_rank: false,
///     })
///     .limit(Some(5), 0)
///     .select([Key::Document, Key::Score, Key::field("title")]);
/// ```
///
/// ## Hybrid search with custom ranking
///
/// ```
/// use chroma_types::plan::SearchPayload;
/// use chroma_types::operator::{RankExpr, QueryVector, Key};
///
/// let dense = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::Embedding,
///     limit: 200,
///     default: None,
///     return_rank: false,
/// };
///
/// let sparse = RankExpr::Knn {
///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
///     key: Key::field("sparse_embedding"),
///     limit: 200,
///     default: None,
///     return_rank: false,
/// };
///
/// let search = SearchPayload::default()
///     .rank(dense * 0.7 + sparse * 0.3)
///     .limit(Some(10), 0)
///     .select([Key::Document, Key::Score]);
/// ```
#[derive(Clone, Debug, Default, Deserialize, Serialize, Validate)]
#[validate(schema(function = "validate_search_payload"))]
pub struct SearchPayload {
    #[serde(default)]
    pub filter: Filter,
    #[serde(default)]
    #[validate(custom(function = "validate_rank"))]
    pub rank: Rank,
    #[serde(default)]
    #[validate(custom(function = "validate_group_by"))]
    pub group_by: GroupBy,
    #[serde(default)]
    pub limit: Limit,
    #[serde(default)]
    pub select: Select,
}

impl SearchPayload {
    /// Sets pagination parameters.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of results to return (None = no limit)
    /// * `offset` - Number of results to skip
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    ///
    /// // First page: results 0-9
    /// let search = SearchPayload::default().limit(Some(10), 0);
    ///
    /// // Second page: results 10-19
    /// let search = SearchPayload::default().limit(Some(10), 10);
    /// ```
    pub fn limit(mut self, limit: Option<u32>, offset: u32) -> Self {
        self.limit.limit = limit;
        self.limit.offset = offset;
        self
    }

    /// Sets the ranking expression for scoring and ordering results.
    ///
    /// # Arguments
    ///
    /// * `expr` - A ranking expression (typically Knn or a combination of expressions)
    ///
    /// # Examples
    ///
    /// ## Simple KNN ranking
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// let search = SearchPayload::default()
    ///     .rank(RankExpr::Knn {
    ///         query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///         key: Key::Embedding,
    ///         limit: 100,
    ///         default: None,
    ///         return_rank: false,
    ///     });
    /// ```
    ///
    /// ## Weighted combination
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// let knn1 = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///     key: Key::Embedding,
    ///     limit: 100,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// let knn2 = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.2, 0.3, 0.4]),
    ///     key: Key::field("other_embedding"),
    ///     limit: 100,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// let search = SearchPayload::default()
    ///     .rank(knn1 * 0.8 + knn2 * 0.2);
    /// ```
    pub fn rank(mut self, expr: RankExpr) -> Self {
        self.rank.expr = Some(expr);
        self
    }

    /// Selects which fields to include in the results.
    ///
    /// # Arguments
    ///
    /// * `keys` - Fields to include (e.g., Document, Score, Metadata, or custom fields)
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::Key;
    ///
    /// // Select predefined fields
    /// let search = SearchPayload::default()
    ///     .select([Key::Document, Key::Score]);
    ///
    /// // Select metadata fields
    /// let search = SearchPayload::default()
    ///     .select([Key::field("title"), Key::field("author")]);
    ///
    /// // Mix predefined and custom fields
    /// let search = SearchPayload::default()
    ///     .select([Key::Document, Key::Score, Key::field("title")]);
    /// ```
    pub fn select<I, T>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Key>,
    {
        self.select.keys = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the filter expression for narrowing results.
    ///
    /// # Arguments
    ///
    /// * `where` - A Where expression for filtering
    ///
    /// # Examples
    ///
    /// ## Simple equality filter
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::Key;
    ///
    /// let search = SearchPayload::default()
    ///     .r#where(Key::field("status").eq("published"));
    /// ```
    ///
    /// ## Numeric comparisons
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::Key;
    ///
    /// let search = SearchPayload::default()
    ///     .r#where(Key::field("year").gte(2020));
    /// ```
    ///
    /// ## Combining filters
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::Key;
    ///
    /// let search = SearchPayload::default()
    ///     .r#where(
    ///         Key::field("status").eq("published")
    ///             & Key::field("year").gte(2020)
    ///             & Key::field("category").is_in(vec!["tech", "science"])
    ///     );
    /// ```
    ///
    /// ## Document content filtering
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::Key;
    ///
    /// let search = SearchPayload::default()
    ///     .r#where(Key::Document.contains("machine learning"));
    /// ```
    pub fn r#where(mut self, r#where: Where) -> Self {
        self.filter.where_clause = Some(r#where);
        self
    }

    /// Groups results by metadata keys and aggregates within each group.
    ///
    /// # Arguments
    ///
    /// * `group_by` - GroupBy configuration with keys and aggregation
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::{GroupBy, Aggregate, Key};
    ///
    /// // Top 3 best documents per category
    /// let search = SearchPayload::default()
    ///     .group_by(GroupBy {
    ///         keys: vec![Key::field("category")],
    ///         aggregate: Some(Aggregate::MinK {
    ///             keys: vec![Key::Score],
    ///             k: 3,
    ///         }),
    ///     });
    /// ```
    pub fn group_by(mut self, group_by: GroupBy) -> Self {
        self.group_by = group_by;
        self
    }
}

#[cfg(feature = "utoipa")]
impl PartialSchema for SearchPayload {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::Object))
                .property(
                    "filter",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Type(Type::Object))
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
                .property("rank", Object::with_type(SchemaType::Type(Type::Object)))
                .property(
                    "group_by",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Type(Type::Object))
                        .property(
                            "keys",
                            ArrayBuilder::new()
                                .items(Object::with_type(SchemaType::Type(Type::String))),
                        )
                        .property(
                            "aggregate",
                            Object::with_type(SchemaType::Type(Type::Object)),
                        ),
                )
                .property(
                    "limit",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Type(Type::Object))
                        .property("offset", Object::with_type(SchemaType::Type(Type::Integer)))
                        .property("limit", Object::with_type(SchemaType::Type(Type::Integer))),
                )
                .property(
                    "select",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Type(Type::Object))
                        .property(
                            "keys",
                            ArrayBuilder::new()
                                .items(Object::with_type(SchemaType::Type(Type::String))),
                        ),
                )
                .build(),
        ))
    }
}

#[cfg(feature = "utoipa")]
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
            group_by: value
                .group_by
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
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
            group_by: Some(value.group_by.try_into()?),
            limit: Some(value.limit.into()),
            select: Some(value.select.try_into()?),
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadLevel {
    /// Read from both the index and the write-ahead log (default).
    /// Provides full consistency with all committed writes visible.
    #[default]
    IndexAndWal,
    /// Read only from the index, skipping the write-ahead log.
    /// Provides eventual consistency - recent uncommitted writes may not be visible.
    IndexOnly,
}

impl From<chroma_proto::ReadLevel> for ReadLevel {
    fn from(value: chroma_proto::ReadLevel) -> Self {
        match value {
            chroma_proto::ReadLevel::IndexAndWal => ReadLevel::IndexAndWal,
            chroma_proto::ReadLevel::IndexOnly => ReadLevel::IndexOnly,
        }
    }
}

impl From<ReadLevel> for chroma_proto::ReadLevel {
    fn from(value: ReadLevel) -> Self {
        match value {
            ReadLevel::IndexAndWal => chroma_proto::ReadLevel::IndexAndWal,
            ReadLevel::IndexOnly => chroma_proto::ReadLevel::IndexOnly,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Search {
    pub scan: Scan,
    pub payloads: Vec<SearchPayload>,
    pub read_level: ReadLevel,
}

impl TryFrom<chroma_proto::SearchPlan> for Search {
    type Error = QueryConversionError;

    fn try_from(value: chroma_proto::SearchPlan) -> Result<Self, Self::Error> {
        let read_level = value.read_level().into();
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
            read_level,
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
            read_level: chroma_proto::ReadLevel::from(value.read_level).into(),
        })
    }
}
