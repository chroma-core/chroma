use std::{
    collections::{BTreeMap, HashMap},
    num::TryFromIntError,
};

use chroma_error::{ChromaError, ErrorCodes};
use chroma_sqlite::{
    db::SqliteDb,
    table::{EmbeddingFulltextSearch, EmbeddingMetadata, Embeddings, MaxSeqId},
};
use chroma_types::{
    operator::{CountResult, Filter, GetResult, Limit, Projection, ProjectionRecord, Scan},
    plan::{Count, Get},
    BooleanOperator, CompositeExpression, DocumentExpression, DocumentOperator, LogRecord,
    Metadata, MetadataComparison, MetadataExpression, MetadataSetValue, MetadataValue,
    MetadataValueConversionError, Operation, OperationRecord, PrimitiveOperator, SegmentUuid,
    SetOperator, UpdateMetadata, Where, CHROMA_DOCUMENT_KEY,
};
use sea_query::{
    Alias, DeleteStatement, Expr, Func, InsertStatement, Nullable, OnConflict, Query, SimpleExpr,
    SqliteQueryBuilder, UpdateStatement,
};
use sea_query_binder::SqlxBinder;
use sqlx::{Row, Sqlite, Transaction};
use thiserror::Error;

const SUBQ_ALIAS: &str = "filter_limit_subq";

#[derive(Debug, Error)]
pub enum SqliteMetadataError {
    #[error("Invalid log offset: {0}")]
    LogOffset(#[from] TryFromIntError),
    #[error("Invalid metadata value: {0}")]
    MetadataValue(#[from] MetadataValueConversionError),
    #[error(transparent)]
    SeaQuery(#[from] sea_query::error::Error),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

impl ChromaError for SqliteMetadataError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

pub struct SqliteMetadataWriter {
    pub db: SqliteDb,
}

impl SqliteMetadataWriter {
    fn add_embedding_stmt(
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Query::insert()
            .into_table(Embeddings::Table)
            .columns([
                Embeddings::SegmentId,
                Embeddings::EmbeddingId,
                Embeddings::SeqId,
            ])
            .values([segment_id.to_string().into(), user_id.into(), seq_id.into()])?
            .on_conflict(
                OnConflict::columns([Embeddings::SegmentId, Embeddings::EmbeddingId])
                    .do_nothing()
                    .to_owned(),
            )
            .returning(Query::returning().columns([Embeddings::Id, Embeddings::SeqId]))
            .to_owned())
    }

    async fn add_embedding(
        tx: &mut Transaction<'static, Sqlite>,
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<Option<u32>, SqliteMetadataError> {
        let (add_emb_stmt, values) =
            Self::add_embedding_stmt(segment_id, seq_id, user_id)?.build_sqlx(SqliteQueryBuilder);
        let result = sqlx::query_with(&add_emb_stmt, values)
            .fetch_one(&mut **tx)
            .await?;
        Ok((result.try_get::<u64, _>(1)? == seq_id).then_some(result.try_get(0)?))
    }

    fn update_embedding_stmt(
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> UpdateStatement {
        Query::update()
            .table(Embeddings::Table)
            .and_where(
                Expr::col(Embeddings::SegmentId)
                    .eq(segment_id.to_string())
                    .and(Expr::col(Embeddings::EmbeddingId).eq(user_id)),
            )
            .value(Embeddings::SeqId, seq_id)
            .returning_col(Embeddings::Id)
            .to_owned()
    }

    async fn update_embedding(
        tx: &mut Transaction<'static, Sqlite>,
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<u32, SqliteMetadataError> {
        let (update_emb_stmt, values) =
            Self::update_embedding_stmt(segment_id, seq_id, user_id).build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&update_emb_stmt, values)
            .fetch_one(&mut **tx)
            .await?
            .try_get(0)?)
    }

    fn upsert_embedding_stmt(
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Self::add_embedding_stmt(segment_id, seq_id, user_id)?
            .on_conflict(
                OnConflict::columns([Embeddings::SegmentId, Embeddings::EmbeddingId])
                    .update_columns([Embeddings::SeqId])
                    .to_owned(),
            )
            .to_owned())
    }

    async fn upsert_embedding(
        tx: &mut Transaction<'static, Sqlite>,
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<u32, SqliteMetadataError> {
        let (upsert_emb_stmt, values) = Self::upsert_embedding_stmt(segment_id, seq_id, user_id)?
            .build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&upsert_emb_stmt, values)
            .fetch_one(&mut **tx)
            .await?
            .try_get(0)?)
    }

    fn delete_embedding_stmt(segment_id: SegmentUuid, user_id: String) -> DeleteStatement {
        Query::delete()
            .from_table(Embeddings::Table)
            .and_where(
                Expr::col(Embeddings::SegmentId)
                    .eq(segment_id.to_string())
                    .and(Expr::col(Embeddings::EmbeddingId).eq(user_id)),
            )
            .returning_col(Embeddings::Id)
            .to_owned()
    }

    async fn delete_embedding(
        tx: &mut Transaction<'static, Sqlite>,
        segment_id: SegmentUuid,
        user_id: String,
    ) -> Result<Option<u32>, SqliteMetadataError> {
        let (delete_emb_stmt, values) =
            Self::delete_embedding_stmt(segment_id, user_id).build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&delete_emb_stmt, values)
            .fetch_optional(&mut **tx)
            .await?
            .map(|r| r.try_get(0))
            .transpose()?)
    }

    fn upsert_metadata_stmt(
        id: u32,
        metadata: Metadata,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        let mut stmt = Query::insert();
        stmt.into_table(EmbeddingMetadata::Table)
            .columns([
                EmbeddingMetadata::Id,
                EmbeddingMetadata::Key,
                EmbeddingMetadata::StringValue,
                EmbeddingMetadata::IntValue,
                EmbeddingMetadata::FloatValue,
                EmbeddingMetadata::BoolValue,
            ])
            .on_conflict(
                OnConflict::columns([EmbeddingMetadata::Id, EmbeddingMetadata::Key])
                    .update_columns([
                        EmbeddingMetadata::StringValue,
                        EmbeddingMetadata::IntValue,
                        EmbeddingMetadata::FloatValue,
                        EmbeddingMetadata::BoolValue,
                    ])
                    .to_owned(),
            );
        for (key, val) in metadata {
            stmt.values(match val {
                MetadataValue::Bool(b) => [
                    id.into(),
                    key.into(),
                    String::null().into(),
                    i32::null().into(),
                    f32::null().into(),
                    b.into(),
                ],
                MetadataValue::Int(i) => [
                    id.into(),
                    key.into(),
                    String::null().into(),
                    i.into(),
                    f32::null().into(),
                    bool::null().into(),
                ],
                MetadataValue::Float(f) => [
                    id.into(),
                    key.into(),
                    String::null().into(),
                    i32::null().into(),
                    f.into(),
                    bool::null().into(),
                ],
                MetadataValue::Str(s) => [
                    id.into(),
                    key.into(),
                    s.into(),
                    i32::null().into(),
                    f32::null().into(),
                    bool::null().into(),
                ],
            })?;
        }
        Ok(stmt)
    }

    async fn upsert_metadata(
        tx: &mut Transaction<'static, Sqlite>,
        id: u32,
        metadata: Metadata,
    ) -> Result<(), SqliteMetadataError> {
        let (upsert_meta_stmt, meta_values) =
            Self::upsert_metadata_stmt(id, metadata)?.build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&upsert_meta_stmt, meta_values)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    fn delete_metadata_stmt(id: u32) -> DeleteStatement {
        Query::delete()
            .from_table(EmbeddingMetadata::Table)
            .and_where(Expr::col(EmbeddingMetadata::Id).eq(id))
            .to_owned()
    }

    async fn delete_metadata(
        tx: &mut Transaction<'static, Sqlite>,
        id: u32,
    ) -> Result<(), SqliteMetadataError> {
        let (delete_meta_stmt, meta_values) =
            Self::delete_metadata_stmt(id).build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&delete_meta_stmt, meta_values)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    fn delete_metadata_by_key_stmt(id: u32, keys: Vec<String>) -> DeleteStatement {
        Query::delete()
            .from_table(EmbeddingMetadata::Table)
            .and_where(
                Expr::col(EmbeddingMetadata::Id)
                    .eq(id)
                    .and(Expr::col(EmbeddingMetadata::Key).is_in(keys)),
            )
            .to_owned()
    }

    async fn delete_metadata_by_key(
        tx: &mut Transaction<'static, Sqlite>,
        id: u32,
        keys: Vec<String>,
    ) -> Result<(), SqliteMetadataError> {
        let (delete_meta_by_key_stmt, meta_values) =
            Self::delete_metadata_by_key_stmt(id, keys).build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&delete_meta_by_key_stmt, meta_values)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn update_metadata(
        tx: &mut Transaction<'static, Sqlite>,
        id: u32,
        metadata: UpdateMetadata,
    ) -> Result<(), SqliteMetadataError> {
        let mut deleted_keys = Vec::new();
        let mut metadata_not_null = HashMap::new();
        for (key, value) in metadata {
            match (&value).try_into() {
                Ok(val) => {
                    metadata_not_null.insert(key, val);
                }
                Err(_) => deleted_keys.push(key),
            }
        }
        Self::delete_metadata_by_key(tx, id, deleted_keys).await?;
        Self::upsert_metadata(tx, id, metadata_not_null).await?;
        Ok(())
    }

    fn upsert_document_stmt(
        id: u32,
        document: String,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Query::insert()
            .into_table(EmbeddingFulltextSearch::Table)
            .columns([
                EmbeddingFulltextSearch::Rowid,
                EmbeddingFulltextSearch::StringValue,
            ])
            .values([id.into(), document.into()])?
            .on_conflict(
                OnConflict::column(EmbeddingFulltextSearch::Rowid)
                    .update_column(EmbeddingFulltextSearch::StringValue)
                    .to_owned(),
            )
            .to_owned())
    }

    async fn upsert_document(
        tx: &mut Transaction<'static, Sqlite>,
        id: u32,
        document: String,
    ) -> Result<(), SqliteMetadataError> {
        let (upsert_doc_stmt, values) =
            Self::upsert_document_stmt(id, document)?.build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&upsert_doc_stmt, values)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    fn delete_document_stmt(id: u32) -> DeleteStatement {
        Query::delete()
            .from_table(EmbeddingFulltextSearch::Table)
            .and_where(Expr::col(EmbeddingFulltextSearch::Rowid).eq(id))
            .to_owned()
    }

    async fn delete_document(
        tx: &mut Transaction<'static, Sqlite>,
        id: u32,
    ) -> Result<(), SqliteMetadataError> {
        let (delete_doc_stmt, values) =
            Self::delete_document_stmt(id).build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&delete_doc_stmt, values)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    fn upsert_max_seq_id_stmt(
        segment_id: SegmentUuid,
        seq_id: u64,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Query::insert()
            .into_table(MaxSeqId::Table)
            .columns([MaxSeqId::SegmentId, MaxSeqId::SeqId])
            .values([segment_id.to_string().into(), seq_id.into()])?
            .on_conflict(
                OnConflict::column(MaxSeqId::SegmentId)
                    .update_column(MaxSeqId::SeqId)
                    .to_owned(),
            )
            .to_owned())
    }

    async fn upsert_max_seq_id(
        tx: &mut Transaction<'static, Sqlite>,
        segment_id: SegmentUuid,
        seq_id: u64,
    ) -> Result<(), SqliteMetadataError> {
        let (upsert_max_seq_id_stmt, values) =
            Self::upsert_max_seq_id_stmt(segment_id, seq_id)?.build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&upsert_max_seq_id_stmt, values)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    pub async fn apply_materialized_logs(
        &self,
        logs: Vec<LogRecord>,
        segment_id: SegmentUuid,
    ) -> Result<(), SqliteMetadataError> {
        if logs.is_empty() {
            return Ok(());
        }
        let mut tx = self.db.get_conn().begin().await?;
        let mut max_seq_id = u64::MIN;
        for LogRecord {
            log_offset,
            record:
                OperationRecord {
                    id,
                    metadata,
                    document,
                    operation,
                    ..
                },
        } in logs
        {
            let log_offset_unsigned = log_offset.try_into()?;
            max_seq_id = max_seq_id.max(log_offset_unsigned);
            match operation {
                Operation::Add => {
                    if let Some(offset_id) =
                        Self::add_embedding(&mut tx, segment_id, log_offset_unsigned, id).await?
                    {
                        if let Some(meta) = metadata {
                            Self::update_metadata(&mut tx, offset_id, meta).await?;
                        }

                        if let Some(doc) = document {
                            Self::upsert_document(&mut tx, offset_id, doc).await?;
                        }
                    }
                }
                Operation::Update => {
                    let offset_id =
                        Self::update_embedding(&mut tx, segment_id, log_offset_unsigned, id)
                            .await?;

                    if let Some(meta) = metadata {
                        Self::update_metadata(&mut tx, offset_id, meta).await?;
                    }

                    if let Some(doc) = document {
                        Self::upsert_document(&mut tx, offset_id, doc).await?;
                    }
                }
                Operation::Upsert => {
                    let offset_id =
                        Self::upsert_embedding(&mut tx, segment_id, log_offset_unsigned, id)
                            .await?;

                    if let Some(meta) = metadata {
                        Self::update_metadata(&mut tx, offset_id, meta).await?;
                    }

                    if let Some(doc) = document {
                        Self::upsert_document(&mut tx, offset_id, doc).await?;
                    }
                }
                Operation::Delete => {
                    if let Some(offset_id) = Self::delete_embedding(&mut tx, segment_id, id).await?
                    {
                        Self::delete_metadata(&mut tx, offset_id).await?;
                        Self::delete_document(&mut tx, offset_id).await?;
                    }
                }
            }
        }

        Self::upsert_max_seq_id(&mut tx, segment_id, max_seq_id).await?;

        Ok(tx.commit().await?)
    }
}

trait IntoSqliteExpr {
    /// Evaluate to a binary integer (0/1) indicating boolean value
    /// We cannot directly use a boolean value because `Any` and `All` aggregation does not exist
    /// We need to use `Min` and `Max` as a workaround
    /// In SQLite, boolean can be implicitly treated as binary integer
    fn eval(&self) -> SimpleExpr;

    fn one() -> SimpleExpr {
        Expr::value(1)
    }
}

impl IntoSqliteExpr for Where {
    fn eval(&self) -> SimpleExpr {
        match self {
            Where::Composite(expr) => expr.eval(),
            Where::Document(expr) => expr.eval(),
            Where::Metadata(expr) => expr.eval(),
        }
    }
}

impl IntoSqliteExpr for CompositeExpression {
    fn eval(&self) -> SimpleExpr {
        let mut expr = Self::one();
        for child in &self.children {
            expr = expr.mul(match self.operator {
                BooleanOperator::And => child.eval(),
                BooleanOperator::Or => Self::one().sub(child.eval()),
            })
        }
        match self.operator {
            BooleanOperator::And => expr,
            BooleanOperator::Or => Self::one().sub(expr),
        }
    }
}

impl IntoSqliteExpr for DocumentExpression {
    fn eval(&self) -> SimpleExpr {
        let doc_col = Expr::col((
            EmbeddingFulltextSearch::Table,
            EmbeddingFulltextSearch::StringValue,
        ));
        match self.operator {
            DocumentOperator::Contains => doc_col.like(format!("%{}%", self.text)),
            DocumentOperator::NotContains => doc_col
                .clone()
                .not_like(format!("%{}%", self.text))
                .or(doc_col.is_null()),
        }
    }
}

impl IntoSqliteExpr for MetadataExpression {
    fn eval(&self) -> SimpleExpr {
        let key_cond =
            Expr::col((EmbeddingMetadata::Table, EmbeddingMetadata::Key)).eq(self.key.to_string());
        match &self.comparison {
            MetadataComparison::Primitive(op, val) => {
                let (col, sval) = match val {
                    MetadataValue::Bool(b) => (EmbeddingMetadata::BoolValue, Expr::val(*b)),
                    MetadataValue::Int(i) => (EmbeddingMetadata::IntValue, Expr::val(*i)),
                    MetadataValue::Float(f) => (EmbeddingMetadata::FloatValue, Expr::val(*f)),
                    MetadataValue::Str(s) => (EmbeddingMetadata::StringValue, Expr::val(s)),
                };
                let scol = Expr::col((EmbeddingMetadata::Table, col));
                match op {
                    PrimitiveOperator::Equal => Expr::expr(key_cond.and(scol.eq(sval))).max(),
                    PrimitiveOperator::NotEqual => {
                        Expr::expr(key_cond.and(scol.eq(sval)).not()).min()
                    }
                    PrimitiveOperator::GreaterThan => Expr::expr(key_cond.and(scol.gt(sval))).max(),
                    PrimitiveOperator::GreaterThanOrEqual => {
                        Expr::expr(key_cond.and(scol.gte(sval))).max()
                    }
                    PrimitiveOperator::LessThan => Expr::expr(key_cond.and(scol.lt(sval))).max(),
                    PrimitiveOperator::LessThanOrEqual => {
                        Expr::expr(key_cond.and(scol.lte(sval))).max()
                    }
                }
            }
            MetadataComparison::Set(op, vals) => {
                let (col, svals) = match vals {
                    MetadataSetValue::Bool(bs) => (
                        EmbeddingMetadata::BoolValue,
                        bs.iter().cloned().map(Expr::val).collect::<Vec<_>>(),
                    ),
                    MetadataSetValue::Int(is) => (
                        EmbeddingMetadata::IntValue,
                        is.iter().cloned().map(Expr::val).collect::<Vec<_>>(),
                    ),
                    MetadataSetValue::Float(fs) => (
                        EmbeddingMetadata::FloatValue,
                        fs.iter().cloned().map(Expr::val).collect::<Vec<_>>(),
                    ),
                    MetadataSetValue::Str(ss) => (
                        EmbeddingMetadata::StringValue,
                        ss.iter().cloned().map(Expr::val).collect::<Vec<_>>(),
                    ),
                };
                let scol = Expr::col((EmbeddingMetadata::Table, col));
                match op {
                    SetOperator::In => Expr::expr(scol.is_in(svals)).max(),
                    SetOperator::NotIn => Expr::expr(scol.is_in(svals).not()).min(),
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqliteMetadataReader {
    pub db: SqliteDb,
}

impl SqliteMetadataReader {
    pub fn new(db: SqliteDb) -> Self {
        Self { db }
    }

    pub async fn count(
        &self,
        Count {
            scan: Scan {
                collection_and_segments,
            },
        }: Count,
    ) -> Result<CountResult, SqliteMetadataError> {
        let (sql, values) = Query::select()
            .expr(Func::count(Expr::col(Embeddings::Id)))
            .from(Embeddings::Table)
            .and_where(
                Expr::col(Embeddings::SegmentId)
                    .eq(collection_and_segments.metadata_segment.id.to_string()),
            )
            .build_sqlx(SqliteQueryBuilder);

        // Count should yield exactly one row with exactly one column
        Ok(sqlx::query_with(&sql, values)
            .fetch_one(self.db.get_conn())
            .await?
            .try_get(0)?)
    }

    pub async fn get(
        &self,
        Get {
            scan: Scan {
                collection_and_segments,
            },
            filter: Filter {
                query_ids,
                where_clause,
            },
            limit: Limit { skip, fetch },
            proj: Projection {
                document, metadata, ..
            },
        }: Get,
    ) -> Result<GetResult, SqliteMetadataError> {
        let mut filter_limit_query = Query::select();
        filter_limit_query.columns([
            (Embeddings::Table, Embeddings::Id),
            (Embeddings::Table, Embeddings::EmbeddingId),
        ]);
        filter_limit_query.from(Embeddings::Table).and_where(
            Expr::col((Embeddings::Table, Embeddings::SegmentId))
                .eq(collection_and_segments.metadata_segment.id.to_string()),
        );

        if let Some(ids) = &query_ids {
            filter_limit_query
                .cond_where(Expr::col((Embeddings::Table, Embeddings::EmbeddingId)).is_in(ids));
        }

        if let Some(whr) = &where_clause {
            filter_limit_query
                .left_join(
                    EmbeddingMetadata::Table,
                    Expr::col((Embeddings::Table, Embeddings::Id))
                        .equals((EmbeddingMetadata::Table, EmbeddingMetadata::Id)),
                )
                .left_join(
                    EmbeddingFulltextSearch::Table,
                    Expr::col((Embeddings::Table, Embeddings::Id)).equals((
                        EmbeddingFulltextSearch::Table,
                        EmbeddingFulltextSearch::Rowid,
                    )),
                )
                .add_group_by([
                    Expr::col((Embeddings::Table, Embeddings::Id)).into(),
                    Expr::col((
                        EmbeddingFulltextSearch::Table,
                        EmbeddingFulltextSearch::StringValue,
                    ))
                    .into(),
                ])
                .cond_having(whr.eval());
        }

        filter_limit_query
            .order_by((Embeddings::Table, Embeddings::Id), sea_query::Order::Asc)
            .offset(skip as u64)
            .limit(fetch.unwrap_or(u32::MAX) as u64);

        let alias = Alias::new(SUBQ_ALIAS);
        let mut projection_query = Query::select();
        projection_query
            .column((alias.clone(), Embeddings::EmbeddingId))
            .from_subquery(filter_limit_query, alias.clone());

        if document || metadata {
            projection_query
                .left_join(
                    EmbeddingMetadata::Table,
                    Expr::col((alias.clone(), Embeddings::Id))
                        .equals((EmbeddingMetadata::Table, EmbeddingMetadata::Id)),
                )
                .columns(
                    [
                        EmbeddingMetadata::Key,
                        EmbeddingMetadata::StringValue,
                        EmbeddingMetadata::IntValue,
                        EmbeddingMetadata::FloatValue,
                        EmbeddingMetadata::BoolValue,
                    ]
                    .map(|c| (EmbeddingMetadata::Table, c)),
                );
        }

        let (sql, values) = projection_query.build_sqlx(SqliteQueryBuilder);

        let rows = sqlx::query_with(&sql, values)
            .fetch_all(self.db.get_conn())
            .await?;

        let mut records = BTreeMap::new();

        for row in rows {
            let id: String = row.try_get(0)?;
            let record = records.entry(id.clone()).or_insert(ProjectionRecord {
                id,
                document: None,
                embedding: None,
                metadata: metadata.then_some(HashMap::new()),
            });

            if document || metadata {
                if let Ok(key) = row.try_get::<String, _>(1) {
                    if let (true, Ok(doc)) = (
                        document && key.starts_with(CHROMA_DOCUMENT_KEY),
                        row.try_get(2),
                    ) {
                        record.document = Some(doc);
                    }

                    if let Some(metadata) = record.metadata.as_mut() {
                        if let Ok(Some(s)) = row.try_get(2) {
                            metadata.insert(key.clone(), MetadataValue::Str(s));
                        } else if let Ok(Some(i)) = row.try_get(3) {
                            metadata.insert(key.clone(), MetadataValue::Int(i));
                        } else if let Ok(Some(f)) = row.try_get(4) {
                            metadata.insert(key.clone(), MetadataValue::Float(f));
                        } else if let Ok(Some(b)) = row.try_get(5) {
                            metadata.insert(key, MetadataValue::Bool(b));
                        }
                    }
                }
            }
        }

        Ok(GetResult {
            records: records.into_values().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{
        operator::{Filter, Limit, Projection, Scan},
        plan::{Count, Get},
        BooleanOperator, CompositeExpression, DocumentExpression, MetadataComparison,
        MetadataExpression, MetadataValue, PrimitiveOperator, Where,
    };

    use crate::test::TestSegment;

    use super::SqliteMetadataReader;

    #[tokio::test]
    async fn test_count() {
        let metadata_reader = SqliteMetadataReader {
            db: get_new_sqlite_db().await,
        };

        metadata_reader
            .count(Count {
                scan: Scan {
                    collection_and_segments: TestSegment::default().into(),
                },
            })
            .await
            .expect("Count should not fail");
    }
    #[tokio::test]
    async fn test_get() {
        let metadata_reader = SqliteMetadataReader {
            db: get_new_sqlite_db().await,
        };

        let _result = metadata_reader
            .get(Get {
                scan: Scan {
                    collection_and_segments: TestSegment::default().into(),
                },
                filter: Filter {
                    query_ids: None,
                    where_clause: Some(Where::Composite(CompositeExpression {
                        operator: BooleanOperator::Or,
                        children: vec![
                            Where::Metadata(MetadataExpression {
                                key: "age".into(),
                                comparison: MetadataComparison::Primitive(
                                    PrimitiveOperator::GreaterThan,
                                    MetadataValue::Int(0),
                                ),
                            }),
                            Where::Document(DocumentExpression {
                                operator: chroma_types::DocumentOperator::NotContains,
                                text: "fish".into(),
                            }),
                        ],
                    })),
                },
                limit: Limit {
                    skip: 0,
                    fetch: None,
                },
                proj: Projection {
                    document: true,
                    embedding: false,
                    metadata: true,
                },
            })
            .await
            .expect("Get should not fail");
    }
}
