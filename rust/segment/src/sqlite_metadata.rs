use std::collections::{BTreeMap, HashMap};

use chroma_error::{ChromaError, ErrorCodes};
use chroma_sqlite::{
    db::SqliteDb,
    table::{EmbeddingFulltextSearch, EmbeddingMetadata, Embeddings},
};
use chroma_types::{
    operator::{CountResult, Filter, GetResult, Limit, Projection, ProjectionRecord, Scan},
    plan::{Count, Get},
    BooleanOperator, CompositeExpression, DocumentExpression, DocumentOperator,
    MaterializedLogOperation, Metadata, MetadataComparison, MetadataExpression, MetadataSetValue,
    MetadataValue, PrimitiveOperator, SegmentUuid, SetOperator, Where, CHROMA_DOCUMENT_KEY,
};
use sea_query::{
    Alias, DeleteStatement, Expr, Func, InsertStatement, Nullable, OnConflict, Query, SimpleExpr,
    SqliteQueryBuilder,
};
use sea_query_binder::SqlxBinder;
use sqlx::Row;
use thiserror::Error;

use crate::types::MaterializeLogsResult;

const SUBQ_ALIAS: &str = "filter_limit_subq";

#[derive(Debug, Error)]
pub enum SqliteMetadataError {
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
    fn add_embedding(
        segment_id: SegmentUuid,
        seq_id: u32,
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
            .returning_col(Embeddings::Id)
            .to_owned())
    }

    fn upsert_embedding(
        segment_id: SegmentUuid,
        seq_id: u32,
        user_id: String,
    ) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Self::add_embedding(segment_id, seq_id, user_id)?
            .on_conflict(
                OnConflict::columns([Embeddings::SegmentId, Embeddings::EmbeddingId])
                    .update_columns([Embeddings::SeqId])
                    .to_owned(),
            )
            .to_owned())
    }

    fn delete_embedding(segment_id: SegmentUuid, user_id: String) -> DeleteStatement {
        Query::delete()
            .from_table(Embeddings::Table)
            .and_where(
                Expr::col(Embeddings::SegmentId)
                    .eq(segment_id.to_string())
                    .and(Expr::col(Embeddings::EmbeddingId).eq(user_id)),
            )
            .to_owned()
    }

    fn upsert_metadata(
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
                        EmbeddingMetadata::Id,
                        EmbeddingMetadata::Key,
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

    fn delete_metadata(id: u32) -> DeleteStatement {
        Query::delete()
            .from_table(EmbeddingMetadata::Table)
            .and_where(Expr::col(EmbeddingMetadata::Id).eq(id))
            .to_owned()
    }

    fn delete_metadata_by_key(id: u32, keys: Vec<String>) -> DeleteStatement {
        Query::delete()
            .from_table(EmbeddingMetadata::Table)
            .and_where(
                Expr::col(EmbeddingMetadata::Id)
                    .eq(id)
                    .and(Expr::col(EmbeddingMetadata::Key).is_in(keys)),
            )
            .to_owned()
    }

    fn add_document(id: u32, document: String) -> Result<InsertStatement, SqliteMetadataError> {
        Ok(Query::insert()
            .into_table(EmbeddingFulltextSearch::Table)
            .columns([
                EmbeddingFulltextSearch::Rowid,
                EmbeddingFulltextSearch::StringValue,
            ])
            .values([id.into(), document.into()])?
            .to_owned())
    }

    pub async fn apply_materialized_logs(
        &self,
        logs: MaterializeLogsResult,
        segment_id: SegmentUuid,
    ) -> Result<(), SqliteMetadataError> {
        let mut tx = self.db.get_conn().begin().await?;
        for log in logs.iter() {
            match log.get_operation() {
                MaterializedLogOperation::Initial => {
                    unreachable!("Initial operation should be internal to log materializer")
                }
                MaterializedLogOperation::AddNew => todo!(),
                MaterializedLogOperation::OverwriteExisting => todo!(),
                MaterializedLogOperation::UpdateExisting => todo!(),
                MaterializedLogOperation::DeleteExisting => todo!(),
            }
        }

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
