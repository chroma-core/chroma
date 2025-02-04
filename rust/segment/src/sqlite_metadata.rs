use std::collections::{BTreeMap, HashMap};

use chroma_sqlite::{
    db::SqliteDb,
    table::{EmbeddingFulltextSearch, EmbeddingMetadata, Embeddings},
};
use chroma_types::{
    operator::{CountResult, Filter, GetResult, Limit, Projection, ProjectionRecord, Scan},
    plan::{Count, Get},
    BooleanOperator, CompositeExpression, DocumentExpression, DocumentOperator, MetadataComparison,
    MetadataExpression, MetadataSetValue, MetadataValue, PrimitiveOperator, SetOperator, Where,
    CHROMA_DOCUMENT_KEY,
};
use sea_query::{Cond, Expr, Func, IntoCondition, Query, SqliteQueryBuilder};
use sea_query_binder::SqlxBinder;
use sqlx::Row;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteMetadataError {
    #[error(transparent)]
    Sqlite(#[from] sqlx::Error),
}

trait IntoSqliteCondition {
    fn eval(&self) -> Cond;
}

impl IntoSqliteCondition for Where {
    fn eval(&self) -> Cond {
        match self {
            Where::Composite(expr) => expr.eval(),
            Where::Document(expr) => expr.eval(),
            Where::Metadata(expr) => expr.eval(),
        }
    }
}

impl IntoSqliteCondition for CompositeExpression {
    fn eval(&self) -> Cond {
        let mut binder = match self.operator {
            BooleanOperator::And => Cond::all(),
            BooleanOperator::Or => Cond::any(),
        };
        for child in &self.children {
            binder = binder.add(child.eval())
        }
        binder
    }
}

impl IntoSqliteCondition for DocumentExpression {
    fn eval(&self) -> Cond {
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
        .into_condition()
    }
}

impl IntoSqliteCondition for MetadataExpression {
    fn eval(&self) -> Cond {
        let key_cond = Expr::col((EmbeddingMetadata::Table, EmbeddingMetadata::Key))
            .eq(Expr::val(self.key.to_string()));
        let val_cond = match &self.comparison {
            MetadataComparison::Primitive(op, val) => {
                let (col, sval) = match val {
                    MetadataValue::Bool(b) => (EmbeddingMetadata::BoolValue, Expr::val(*b)),
                    MetadataValue::Int(i) => (EmbeddingMetadata::IntValue, Expr::val(*i)),
                    MetadataValue::Float(f) => (EmbeddingMetadata::FloatValue, Expr::val(*f)),
                    MetadataValue::Str(s) => (EmbeddingMetadata::StringValue, Expr::val(s)),
                };
                let scol = Expr::col((EmbeddingMetadata::Table, col));
                match op {
                    PrimitiveOperator::Equal => scol.eq(sval),
                    PrimitiveOperator::NotEqual => scol.clone().ne(sval).or(scol.is_null()),
                    PrimitiveOperator::GreaterThan => scol.gt(sval),
                    PrimitiveOperator::GreaterThanOrEqual => scol.gte(sval),
                    PrimitiveOperator::LessThan => scol.lt(sval),
                    PrimitiveOperator::LessThanOrEqual => scol.lte(sval),
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
                    SetOperator::In => scol.is_in(svals),
                    SetOperator::NotIn => scol.clone().is_in(svals).or(scol.is_null()),
                }
            }
        };
        key_cond.and(val_cond).into_condition()
    }
}

pub struct SqliteMetadataReader {
    pub db: SqliteDb,
}

impl SqliteMetadataReader {
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
        let mut query = Query::select();
        query.column((Embeddings::Table, Embeddings::EmbeddingId));
        if document || metadata {
            query.columns(
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

        query.from(Embeddings::Table).and_where(
            Expr::col((Embeddings::Table, Embeddings::SegmentId))
                .eq(collection_and_segments.metadata_segment.id.to_string()),
        );

        if document || metadata || where_clause.is_some() {
            query
                .left_join(
                    EmbeddingMetadata::Table,
                    Expr::col((Embeddings::Table, Embeddings::Id))
                        .eq(Expr::col((EmbeddingMetadata::Table, EmbeddingMetadata::Id))),
                )
                .left_join(
                    EmbeddingFulltextSearch::Table,
                    Expr::col((Embeddings::Table, Embeddings::Id)).eq(Expr::col((
                        EmbeddingFulltextSearch::Table,
                        EmbeddingFulltextSearch::Rowid,
                    ))),
                );
        }

        if let Some(ids) = &query_ids {
            query.cond_where(Expr::col((Embeddings::Table, Embeddings::EmbeddingId)).is_in(ids));
        }

        if let Some(whr) = &where_clause {
            query.cond_where(whr.eval());
        }

        query
            .order_by((Embeddings::Table, Embeddings::Id), sea_query::Order::Asc)
            .offset(skip as u64);

        if let Some(limit) = fetch {
            query.limit(limit as u64);
        }

        let (sql, values) = query.build_sqlx(SqliteQueryBuilder);
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
                        if let Ok(s) = row.try_get(2) {
                            metadata.insert(key.clone(), MetadataValue::Str(s));
                        }
                        if let Ok(i) = row.try_get(3) {
                            metadata.insert(key.clone(), MetadataValue::Int(i));
                        }
                        if let Ok(f) = row.try_get(4) {
                            metadata.insert(key.clone(), MetadataValue::Float(f));
                        }
                        if let Ok(b) = row.try_get(5) {
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

        metadata_reader
            .get(Get {
                scan: Scan {
                    collection_and_segments: TestSegment::default().into(),
                },
                filter: Filter {
                    query_ids: Some(vec!["Cat".into(), "Dog".into()]),
                    where_clause: Some(Where::Composite(CompositeExpression {
                        operator: BooleanOperator::Or,
                        children: vec![
                            Where::Metadata(MetadataExpression {
                                key: "age".into(),
                                comparison: MetadataComparison::Primitive(
                                    PrimitiveOperator::NotEqual,
                                    MetadataValue::Int(1),
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
                    skip: 2,
                    fetch: Some(6),
                },
                proj: Projection {
                    document: true,
                    embedding: false,
                    metadata: false,
                },
            })
            .await
            .expect("Count should not fail");
    }
}
