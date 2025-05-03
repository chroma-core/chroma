use std::{
    collections::{BTreeMap, HashMap},
    num::TryFromIntError,
};

use chroma_error::{ChromaError, ErrorCodes};
use chroma_sqlite::{
    db::SqliteDb,
    helpers::{delete_metadata, update_metadata},
    table::{EmbeddingFulltextSearch, EmbeddingMetadata, Embeddings, MaxSeqId},
};
use chroma_types::{
    operator::{
        CountResult, Filter, GetResult, Limit, Projection, ProjectionOutput, ProjectionRecord, Scan,
    },
    plan::{Count, Get},
    BooleanOperator, Chunk, CompositeExpression, DocumentExpression, DocumentOperator, LogRecord,
    MetadataComparison, MetadataExpression, MetadataSetValue, MetadataValue,
    MetadataValueConversionError, Operation, OperationRecord, PrimitiveOperator, SegmentUuid,
    SetOperator, UpdateMetadataValue, Where, CHROMA_DOCUMENT_KEY,
};
use sea_query::{
    DeleteStatement, Expr, ExprTrait, Func, InsertStatement, OnConflict, Query, SimpleExpr,
    SqliteQueryBuilder, UpdateStatement,
};
use sea_query_binder::SqlxBinder;
use sqlx::{Row, Sqlite, Transaction};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteMetadataError {
    #[error("Invalid log offset: {0}")]
    LogOffset(#[from] TryFromIntError),
    #[error("Invalid metadata value: {0}")]
    MetadataValue(#[from] MetadataValueConversionError),
    #[error("Could not update metadata table: {0}")]
    UpdateMetadata(#[from] chroma_sqlite::helpers::MetadataError),
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
    pub fn new(db: SqliteDb) -> Self {
        Self { db }
    }

    fn add_record_stmt(
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
            .returning_col(Embeddings::Id)
            .to_owned())
    }

    async fn add_record<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<Option<u32>, SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (add_rec_stmt, values) =
            Self::add_record_stmt(segment_id, seq_id, user_id)?.build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&add_rec_stmt, values)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.try_get(0))
            .transpose()?)
    }

    fn update_record_stmt(
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

    async fn update_record<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<Option<u32>, SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (update_rec_stmt, values) =
            Self::update_record_stmt(segment_id, seq_id, user_id).build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&update_rec_stmt, values)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.try_get(0))
            .transpose()?)
    }

    fn upsert_record_stmt(
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Self::add_record_stmt(segment_id, seq_id, user_id)?
            .on_conflict(
                OnConflict::columns([Embeddings::SegmentId, Embeddings::EmbeddingId])
                    .update_columns([Embeddings::SeqId])
                    .to_owned(),
            )
            .to_owned())
    }

    async fn upsert_record<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        seq_id: u64,
        user_id: String,
    ) -> Result<u32, SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (upsert_rec_stmt, values) =
            Self::upsert_record_stmt(segment_id, seq_id, user_id)?.build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&upsert_rec_stmt, values)
            .fetch_one(&mut *tx)
            .await?
            .try_get(0)?)
    }

    fn delete_record_stmt(segment_id: SegmentUuid, user_id: String) -> DeleteStatement {
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

    async fn delete_record<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        user_id: String,
    ) -> Result<Option<u32>, SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (delete_rec_stmt, values) =
            Self::delete_record_stmt(segment_id, user_id).build_sqlx(SqliteQueryBuilder);
        Ok(sqlx::query_with(&delete_rec_stmt, values)
            .fetch_optional(&mut *tx)
            .await?
            .map(|r| r.try_get(0))
            .transpose()?)
    }

    fn add_document_stmt(
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
            .to_owned())
    }

    async fn add_document<C>(
        tx: &mut C,
        id: u32,
        document: String,
    ) -> Result<(), SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (add_doc_stmt, values) =
            Self::add_document_stmt(id, document)?.build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&add_doc_stmt, values)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    fn delete_document_stmt(id: u32) -> DeleteStatement {
        Query::delete()
            .from_table(EmbeddingFulltextSearch::Table)
            .and_where(Expr::col(EmbeddingFulltextSearch::Rowid).eq(id))
            .to_owned()
    }

    async fn delete_document<C>(tx: &mut C, id: u32) -> Result<(), SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (delete_doc_stmt, values) =
            Self::delete_document_stmt(id).build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&delete_doc_stmt, values)
            .execute(&mut *tx)
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

    async fn upsert_max_seq_id<C>(
        tx: &mut C,
        segment_id: SegmentUuid,
        seq_id: u64,
    ) -> Result<(), SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        let (upsert_max_seq_id_stmt, values) =
            Self::upsert_max_seq_id_stmt(segment_id, seq_id)?.build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&upsert_max_seq_id_stmt, values)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>, SqliteMetadataError> {
        Ok(self.db.get_conn().begin().await?)
    }

    pub async fn apply_logs<C>(
        &self,
        logs: Chunk<LogRecord>,
        segment_id: SegmentUuid,
        tx: &mut C,
    ) -> Result<(), SqliteMetadataError>
    where
        for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
    {
        if logs.is_empty() {
            return Ok(());
        }
        let mut max_seq_id = u64::MIN;
        for (
            LogRecord {
                log_offset,
                record:
                    OperationRecord {
                        id,
                        metadata,
                        document,
                        operation,
                        ..
                    },
            },
            _,
        ) in logs.iter()
        {
            let log_offset_unsigned = (*log_offset).try_into()?;
            max_seq_id = max_seq_id.max(log_offset_unsigned);
            let mut metadata_owned = metadata.clone();
            if let Some(doc) = document {
                let mut doc_embedded_meta = metadata_owned.unwrap_or_default();
                doc_embedded_meta.insert(
                    CHROMA_DOCUMENT_KEY.to_string(),
                    UpdateMetadataValue::Str(doc.clone()),
                );
                metadata_owned = Some(doc_embedded_meta);
            }
            match operation {
                Operation::Add => {
                    if let Some(offset_id) =
                        Self::add_record(tx, segment_id, log_offset_unsigned, id.clone()).await?
                    {
                        if let Some(meta) = metadata_owned {
                            update_metadata::<EmbeddingMetadata, _, _>(tx, offset_id, meta).await?;
                        }

                        if let Some(doc) = document {
                            Self::add_document(tx, offset_id, doc.clone()).await?;
                        }
                    }
                }
                Operation::Update => {
                    if let Some(offset_id) =
                        Self::update_record(tx, segment_id, log_offset_unsigned, id.clone()).await?
                    {
                        if let Some(meta) = metadata_owned {
                            update_metadata::<EmbeddingMetadata, _, _>(tx, offset_id, meta).await?;
                        }

                        if let Some(doc) = document {
                            Self::delete_document(tx, offset_id).await?;
                            Self::add_document(tx, offset_id, doc.clone()).await?;
                        }
                    }
                }
                Operation::Upsert => {
                    let offset_id =
                        Self::upsert_record(tx, segment_id, log_offset_unsigned, id.clone())
                            .await?;

                    if let Some(meta) = metadata_owned {
                        update_metadata::<EmbeddingMetadata, _, _>(tx, offset_id, meta).await?;
                    }

                    if let Some(doc) = document {
                        Self::delete_document(tx, offset_id).await?;
                        Self::add_document(tx, offset_id, doc.clone()).await?;
                    }
                }
                Operation::Delete => {
                    if let Some(offset_id) = Self::delete_record(tx, segment_id, id.clone()).await?
                    {
                        delete_metadata::<EmbeddingMetadata, _, _>(tx, offset_id).await?;
                        Self::delete_document(tx, offset_id).await?;
                    }
                }
            }
        }

        Self::upsert_max_seq_id(tx, segment_id, max_seq_id).await?;

        Ok(())
    }
}

trait IntoSqliteExpr {
    /// Evaluate to a binary integer (0/1) indicating boolean value
    /// We cannot directly use a boolean value because `Any` and `All` aggregation does not exist
    /// We need to use `Min` and `Max` as a workaround
    /// In SQLite, boolean can be implicitly treated as binary integer
    fn eval(&self) -> SimpleExpr;

    // fn one() -> SimpleExpr {
    //     Expr::value(1)
    // }
}

impl IntoSqliteExpr for Where {
    fn eval(&self) -> SimpleExpr {
        match self {
            Where::Composite(expr) => expr.eval(),
            Where::Document(expr) => expr.eval(),
            Where::Metadata(expr) => {
                // Local chroma is mixing the usage of int and float
                match &expr.comparison {
                    MetadataComparison::Primitive(op, MetadataValue::Int(i)) => {
                        let alt = MetadataExpression {
                            key: expr.key.clone(),
                            comparison: MetadataComparison::Primitive(
                                op.clone(),
                                MetadataValue::Float(*i as f64),
                            ),
                        };
                        match op {
                            PrimitiveOperator::NotEqual => expr.eval().and(alt.eval()),
                            _ => expr.eval().or(alt.eval()),
                        }
                    }
                    MetadataComparison::Primitive(op, MetadataValue::Float(f)) => {
                        let alt = MetadataExpression {
                            key: expr.key.clone(),
                            comparison: MetadataComparison::Primitive(
                                op.clone(),
                                MetadataValue::Int(*f as i64),
                            ),
                        };
                        match op {
                            PrimitiveOperator::NotEqual => expr.eval().and(alt.eval()),
                            _ => expr.eval().or(alt.eval()),
                        }
                    }
                    MetadataComparison::Set(op, MetadataSetValue::Int(is)) => {
                        let alt = MetadataExpression {
                            key: expr.key.clone(),
                            comparison: MetadataComparison::Set(
                                op.clone(),
                                MetadataSetValue::Float(
                                    is.iter().cloned().map(|i| i as f64).collect(),
                                ),
                            ),
                        };
                        match op {
                            SetOperator::In => expr.eval().or(alt.eval()),
                            SetOperator::NotIn => expr.eval().and(alt.eval()),
                        }
                    }
                    MetadataComparison::Set(op, MetadataSetValue::Float(fs)) => {
                        let alt = MetadataExpression {
                            key: expr.key.clone(),
                            comparison: MetadataComparison::Set(
                                op.clone(),
                                MetadataSetValue::Int(
                                    fs.iter().cloned().map(|f| f as i64).collect(),
                                ),
                            ),
                        };
                        match op {
                            SetOperator::In => expr.eval().or(alt.eval()),
                            SetOperator::NotIn => expr.eval().and(alt.eval()),
                        }
                    }
                    _ => expr.eval(),
                }
            }
        }
    }
}

impl IntoSqliteExpr for CompositeExpression {
    fn eval(&self) -> SimpleExpr {
        match self.operator {
            BooleanOperator::And => {
                // Start with a literal true expression
                let mut expr = SimpleExpr::Value(sea_query::Value::Bool(Some(true)));
                for child in &self.children {
                    // And with each child evaluation
                    expr = Expr::expr(expr).and(child.eval());
                }
                expr
            }
            BooleanOperator::Or => {
                // Start with a literal false expression
                let mut expr = SimpleExpr::Value(sea_query::Value::Bool(Some(false)));
                for child in &self.children {
                    // Or with each child evaluation
                    expr = Expr::expr(expr).or(child.eval());
                }
                expr
            }
        }
    }
}

impl IntoSqliteExpr for DocumentExpression {
    fn eval(&self) -> SimpleExpr {
        let pattern = format!("%{}%", self.pattern.replace("%", ""));
        let subquery = Query::select()
            .expr(Expr::val(1))
            .from(EmbeddingFulltextSearch::Table)
            .and_where(
                Expr::col(EmbeddingFulltextSearch::Rowid)
                    .equals((Embeddings::Table, Embeddings::Id)),
            )
            .and_where(Expr::col(EmbeddingFulltextSearch::StringValue).like(pattern))
            .to_owned();
        match self.operator {
            DocumentOperator::Contains => Expr::exists(subquery),
            DocumentOperator::NotContains => Expr::exists(subquery).not(),
            DocumentOperator::Matches => todo!("Implement Regex matching. The result must be a not-nullable boolean (use `<expr>.is(true)`)"),
            DocumentOperator::NotMatches => todo!("Implement negated Regex matching. This must be exact opposite of Regex matching (use `<expr>.not()`)"),
        }
    }
}

impl IntoSqliteExpr for MetadataExpression {
    fn eval(&self) -> SimpleExpr {
        let key_cond = Expr::col((EmbeddingMetadata::Table, EmbeddingMetadata::Key))
            .eq(self.key.to_string())
            .is(true);
        match &self.comparison {
            MetadataComparison::Primitive(op, val) => {
                let (col, sval) = match val {
                    MetadataValue::Bool(b) => (EmbeddingMetadata::BoolValue, Expr::val(*b)),
                    MetadataValue::Int(i) => (EmbeddingMetadata::IntValue, Expr::val(*i)),
                    MetadataValue::Float(f) => (EmbeddingMetadata::FloatValue, Expr::val(*f)),
                    MetadataValue::Str(s) => (EmbeddingMetadata::StringValue, Expr::val(s)),
                };
                let scol = Expr::col((EmbeddingMetadata::Table, col));
                let condition = match op {
                    PrimitiveOperator::Equal => scol.eq(sval.clone()),
                    PrimitiveOperator::NotEqual => scol.eq(sval.clone()).not(),
                    PrimitiveOperator::GreaterThan => scol.gt(sval.clone()),
                    PrimitiveOperator::GreaterThanOrEqual => scol.gte(sval.clone()),
                    PrimitiveOperator::LessThan => scol.lt(sval.clone()),
                    PrimitiveOperator::LessThanOrEqual => scol.lte(sval.clone()),
                };

                // Create base EXISTS subquery
                let mut subquery_builder = Query::select();
                let subquery = subquery_builder
                    .expr(Expr::val(1))
                    .from(EmbeddingMetadata::Table)
                    .and_where(
                        Expr::col(EmbeddingMetadata::Id)
                            .equals((Embeddings::Table, Embeddings::Id)),
                    )
                    .and_where(key_cond.clone())
                    .and_where(condition)
                    .to_owned();

                let base_expr = if matches!(op, PrimitiveOperator::NotEqual) {
                    Expr::exists(subquery).not()
                } else {
                    Expr::exists(subquery)
                };

                // For numeric values, we need to check both int and float columns due to
                // potential type ambiguity in the database
                if matches!(val, MetadataValue::Int(_) | MetadataValue::Float(_)) {
                    let (alt_col, alt_val) = match val {
                        MetadataValue::Int(i) => {
                            (EmbeddingMetadata::FloatValue, Expr::val(*i as f64))
                        }
                        MetadataValue::Float(f) => {
                            (EmbeddingMetadata::IntValue, Expr::val(*f as i64))
                        }
                        _ => unreachable!(),
                    };
                    let alt_scol = Expr::col((EmbeddingMetadata::Table, alt_col));

                    // Create alternative condition
                    let alt_condition = match op {
                        PrimitiveOperator::Equal => alt_scol.eq(alt_val),
                        PrimitiveOperator::NotEqual => alt_scol.eq(alt_val).not(),
                        PrimitiveOperator::GreaterThan => alt_scol.gt(alt_val),
                        PrimitiveOperator::GreaterThanOrEqual => alt_scol.gte(alt_val),
                        PrimitiveOperator::LessThan => alt_scol.lt(alt_val),
                        PrimitiveOperator::LessThanOrEqual => alt_scol.lte(alt_val),
                    };

                    // Create alternative EXISTS subquery
                    let mut alt_subquery_builder = Query::select();
                    let alt_subquery = alt_subquery_builder
                        .expr(Expr::val(1))
                        .from(EmbeddingMetadata::Table)
                        .and_where(
                            Expr::col(EmbeddingMetadata::Id)
                                .equals((Embeddings::Table, Embeddings::Id)),
                        )
                        .and_where(key_cond)
                        .and_where(alt_condition)
                        .to_owned();

                    let alt_expr = if matches!(op, PrimitiveOperator::NotEqual) {
                        Expr::exists(alt_subquery).not()
                    } else {
                        Expr::exists(alt_subquery)
                    };

                    // Combine expressions based on operator
                    match op {
                        PrimitiveOperator::NotEqual => base_expr.and(alt_expr),
                        _ => base_expr.or(alt_expr),
                    }
                } else {
                    // For non-numeric values, just return the base expression
                    base_expr
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

                if !svals.is_empty() {
                    // Build the exists subquery
                    let mut subquery_builder = Query::select();
                    let subquery = subquery_builder
                        .expr(Expr::val(1))
                        .from(EmbeddingMetadata::Table)
                        .and_where(
                            Expr::col(EmbeddingMetadata::Id)
                                .equals((Embeddings::Table, Embeddings::Id)),
                        )
                        .and_where(key_cond.clone())
                        .and_where(scol.is_in(svals.clone()))
                        .to_owned();

                    match op {
                        SetOperator::In => Expr::exists(subquery),
                        SetOperator::NotIn => Expr::exists(subquery).not(),
                    }
                } else {
                    // Handle empty set case
                    match op {
                        SetOperator::In => Expr::value(false), // Empty IN set always false
                        SetOperator::NotIn => Expr::value(true), // Empty NOT IN set always true
                    }
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

    pub async fn current_max_seq_id(
        &self,
        segment_id: &SegmentUuid,
    ) -> Result<u64, SqliteMetadataError> {
        let (sql, values) = Query::select()
            .column(MaxSeqId::SeqId)
            .from(MaxSeqId::Table)
            .and_where(Expr::col(MaxSeqId::SegmentId).eq(segment_id.to_string()))
            .build_sqlx(SqliteQueryBuilder);
        let row_opt = sqlx::query_with(&sql, values)
            .fetch_optional(self.db.get_conn())
            .await?;
        Ok(row_opt
            .map(|row| row.try_get::<u64, _>(0))
            .transpose()?
            .unwrap_or_default())
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
        let count = sqlx::query_with(&sql, values)
            .fetch_one(self.db.get_conn())
            .await?
            .try_get(0)?;
        Ok(CountResult {
            count,
            pulled_log_bytes: 0,
        })
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
        let mut id_subquery = Query::select()
            .column((Embeddings::Table, Embeddings::Id))
            .from(Embeddings::Table)
            .and_where(
                Expr::col((Embeddings::Table, Embeddings::SegmentId))
                    .eq(collection_and_segments.metadata_segment.id.to_string()),
            )
            .to_owned();

        if let Some(ids) = &query_ids {
            id_subquery
                .and_where(Expr::col((Embeddings::Table, Embeddings::EmbeddingId)).is_in(ids));
        }

        if let Some(whr) = &where_clause {
            id_subquery
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
                .distinct()
                .and_where(whr.eval());
        }

        id_subquery
            .order_by((Embeddings::Table, Embeddings::Id), sea_query::Order::Asc)
            .offset(skip as u64)
            .limit(fetch.unwrap_or(u32::MAX) as u64);

        let mut data_query = Query::select();

        data_query
            .column((Embeddings::Table, Embeddings::Id))
            .column((Embeddings::Table, Embeddings::EmbeddingId))
            .from(Embeddings::Table)
            .and_where(Expr::col((Embeddings::Table, Embeddings::Id)).in_subquery(id_subquery));

        if document || metadata {
            data_query
                .left_join(
                    EmbeddingMetadata::Table,
                    Expr::col((Embeddings::Table, Embeddings::Id))
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

        let (sql, values) = data_query.build_sqlx(SqliteQueryBuilder);
        let rows = sqlx::query_with(&sql, values)
            .fetch_all(self.db.get_conn())
            .await?;

        let mut records_map = BTreeMap::new();

        for row in rows {
            let offset_id: u32 = row.try_get(0)?;
            let user_id: String = row.try_get(1)?;
            let record = records_map
                .entry(offset_id)
                .or_insert_with(|| ProjectionRecord {
                    id: user_id.clone(),
                    document: None,
                    embedding: None,
                    metadata: (document || metadata).then_some(HashMap::new()),
                });

            if document || metadata {
                if let Ok(key) = row.try_get::<String, _>(2) {
                    if let Some(metadata) = record.metadata.as_mut() {
                        // Optimize metadata extraction with early returns
                        if let Ok(Some(s)) = row.try_get(3) {
                            metadata.insert(key, MetadataValue::Str(s));
                            continue;
                        }
                        if let Ok(Some(i)) = row.try_get(4) {
                            metadata.insert(key, MetadataValue::Int(i));
                            continue;
                        }
                        if let Ok(Some(f)) = row.try_get(5) {
                            metadata.insert(key, MetadataValue::Float(f));
                            continue;
                        }
                        if let Ok(Some(b)) = row.try_get(6) {
                            metadata.insert(key, MetadataValue::Bool(b));
                        }
                    }
                }
            }
        }

        // Convert to final result format
        Ok(GetResult {
            pulled_log_bytes: 0,
            result: ProjectionOutput {
                records: records_map
                    .into_values()
                    .map(|mut rec| {
                        if let Some(mut meta) = rec.metadata.take() {
                            if let Some(MetadataValue::Str(doc)) = meta.remove(CHROMA_DOCUMENT_KEY)
                            {
                                rec.document = Some(doc);
                            }
                            if !meta.is_empty() {
                                rec.metadata = Some(meta)
                            }
                        }
                        rec
                    })
                    .collect(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{
        operator::{Filter, Limit, Projection, Scan},
        plan::{Count, Get},
        strategies::{any_collection_data_and_where_filter, TestCollectionData},
        Chunk, LogRecord,
    };
    use proptest::prelude::*;
    use tokio::runtime::Runtime;

    use crate::test::TestReferenceSegment;

    use super::{SqliteMetadataReader, SqliteMetadataWriter};

    proptest! {
        #[test]
        fn test_count(
            test_data in any::<TestCollectionData>()
        ) {
            let runtime = Runtime::new().expect("Should be able to start tokio runtime");
            let mut ref_seg = TestReferenceSegment::default();
            let sqlite_seg_writer = SqliteMetadataWriter {
                db: runtime.block_on(get_new_sqlite_db())
            };

            let metadata_seg_id = test_data.collection_and_segments.metadata_segment.id;
            ref_seg.apply_logs(test_data.logs.clone(), metadata_seg_id);
            let mut tx = runtime.block_on(sqlite_seg_writer.begin()).expect("Should be able to start transaction");
            let data: Chunk<LogRecord> = Chunk::new(test_data.logs.clone().into());
            runtime.block_on(sqlite_seg_writer.apply_logs(data, metadata_seg_id, &mut *tx)).expect("Should be able to apply logs");
            runtime.block_on(tx.commit()).expect("Should be able to commit log");

            let sqlite_seg_reader = SqliteMetadataReader {
                db: sqlite_seg_writer.db
            };
            let plan = Count { scan: Scan { collection_and_segments: test_data.collection_and_segments.clone() }};
            let ref_count = ref_seg.count(plan.clone()).expect("Count should not fail").count;
            let sqlite_count = runtime.block_on(sqlite_seg_reader.count(plan)).expect("Count should not fail").count;
            assert_eq!(sqlite_count, ref_count);
        }
    }

    proptest! {
        #[test]
        fn test_get(
            (test_data, where_clause) in any_collection_data_and_where_filter()
        ) {
            let runtime = Runtime::new().expect("Should be able to start tokio runtime");
            let mut ref_seg = TestReferenceSegment::default();
            let sqlite_seg_writer = SqliteMetadataWriter {
                db: runtime.block_on(get_new_sqlite_db())
            };

            let metadata_seg_id = test_data.collection_and_segments.metadata_segment.id;
            ref_seg.apply_logs(test_data.logs.clone(), metadata_seg_id);
            let mut tx = runtime.block_on(sqlite_seg_writer.begin()).expect("Should be able to start transaction");
            let data: Chunk<LogRecord> = Chunk::new(test_data.logs.clone().into());
            runtime.block_on(sqlite_seg_writer.apply_logs(data, metadata_seg_id, &mut *tx)).expect("Should be able to apply logs");
            runtime.block_on(tx.commit()).expect("Should be able to commit log");

            let sqlite_seg_reader = SqliteMetadataReader {
                db: sqlite_seg_writer.db
            };

            let plan = Get {
                scan: Scan {
                    collection_and_segments: test_data.collection_and_segments.clone(),
                },
                filter: Filter {
                    query_ids: None,
                    where_clause: Some(where_clause.clause),
                },
                limit: Limit {
                    skip: 3,
                    fetch: Some(6),
                },
                proj: Projection {
                    document: true,
                    embedding: false,
                    metadata: true,
                },
            };
            let ref_get = ref_seg.get(plan.clone()).expect("Get should not fail");
            let sqlite_get = runtime.block_on(sqlite_seg_reader.get(plan)).expect("Get should not fail");
            assert_eq!(sqlite_get, ref_get);
        }
    }
}
