use crate::table::MetadataTable;
use chroma_error::{ChromaError, WrappedSqlxError};
use chroma_types::{CollectionUuid, Metadata, MetadataValue, UpdateMetadata};
use sea_query::{Expr, Iden, InsertStatement, OnConflict, SimpleExpr, SqliteQueryBuilder};
use sea_query::{Nullable, Query};
use sea_query_binder::SqlxBinder;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("Error constructing query: {0}")]
    QueryError(#[from] sea_query::error::Error),
    #[error("Error executing query: {0}")]
    SqlxError(#[from] WrappedSqlxError),
}

impl ChromaError for MetadataError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            MetadataError::QueryError(_) => chroma_error::ErrorCodes::Internal,
            MetadataError::SqlxError(e) => e.code(),
        }
    }
}

fn construct_upsert_metadata_stmt<
    Table: MetadataTable + Iden + 'static,
    Id: Into<SimpleExpr> + Clone,
>(
    id: Id,
    metadata: Metadata,
) -> Result<InsertStatement, sea_query::error::Error> {
    let mut stmt = Query::insert();
    stmt.into_table(Table::table_name())
        .columns([
            Table::id_column(),
            Table::key_column(),
            Table::str_value_column(),
            Table::int_value_column(),
            Table::float_value_column(),
            Table::bool_value_column(),
        ])
        .on_conflict(
            OnConflict::columns([Table::id_column(), Table::key_column()])
                .update_columns([
                    Table::str_value_column(),
                    Table::int_value_column(),
                    Table::float_value_column(),
                    Table::bool_value_column(),
                ])
                .to_owned(),
        );
    for (key, val) in metadata {
        stmt.values(match val {
            MetadataValue::Bool(b) => [
                id.clone().into(),
                key.into(),
                String::null().into(),
                i32::null().into(),
                f32::null().into(),
                b.into(),
            ],
            MetadataValue::Int(i) => [
                id.clone().into(),
                key.into(),
                String::null().into(),
                i.into(),
                f32::null().into(),
                bool::null().into(),
            ],
            MetadataValue::Float(f) => [
                id.clone().into(),
                key.into(),
                String::null().into(),
                i32::null().into(),
                f.into(),
                bool::null().into(),
            ],
            MetadataValue::Str(s) => [
                id.clone().into(),
                key.into(),
                s.into(),
                i32::null().into(),
                f32::null().into(),
                bool::null().into(),
            ],
            MetadataValue::SparseVector(_) => {
                todo!("Sparse vector is not yet supported for local")
            }
        })?;
    }
    Ok(stmt)
}

pub async fn update_metadata<
    Table: MetadataTable + Iden + 'static,
    Id: Into<SimpleExpr> + Clone,
    C,
>(
    conn: &mut C,
    id: Id,
    metadata: UpdateMetadata,
) -> Result<(), MetadataError>
where
    for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
{
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
    if !deleted_keys.is_empty() {
        let (sql, values) = Query::delete()
            .from_table(Table::table_name())
            .and_where(
                Expr::col(Table::id_column())
                    .eq(id.clone())
                    .and(Expr::col(Table::key_column()).is_in(deleted_keys)),
            )
            .to_owned()
            .build_sqlx(SqliteQueryBuilder);

        sqlx::query_with(&sql, values)
            .execute(&mut *conn)
            .await
            .map_err(WrappedSqlxError)?;
    }
    if !metadata_not_null.is_empty() {
        let (sql, values) = construct_upsert_metadata_stmt::<Table, Id>(id, metadata_not_null)?
            .build_sqlx(SqliteQueryBuilder);

        sqlx::query_with(&sql, values)
            .execute(conn)
            .await
            .map_err(WrappedSqlxError)?;
    }
    Ok(())
}

pub async fn delete_metadata<
    Table: MetadataTable + Iden + 'static,
    Id: Into<SimpleExpr> + Clone,
    C,
>(
    conn: &mut C,
    id: Id,
) -> Result<(), MetadataError>
where
    for<'connection> &'connection mut C: sqlx::Executor<'connection, Database = sqlx::Sqlite>,
{
    let (sql, values) = Query::delete()
        .from_table(Table::table_name())
        .and_where(Expr::col(Table::id_column()).eq(id))
        .to_owned()
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(conn)
        .await
        .map_err(WrappedSqlxError)?;

    Ok(())
}

pub fn get_embeddings_queue_topic_name(
    log_tenant: &str,
    log_topic_namespace: &str,
    collection_id: CollectionUuid,
) -> String {
    format!(
        "persistent://{}/{}/{}",
        log_tenant, log_topic_namespace, collection_id
    )
}
