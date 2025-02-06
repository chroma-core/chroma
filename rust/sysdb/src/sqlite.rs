use chroma_error::{ChromaError, WrappedSqlxError};
use chroma_sqlite::db::SqliteDb;
use chroma_sqlite::helpers::update_metadata;
use chroma_sqlite::table;
use chroma_types::{
    Collection, CollectionAndSegments, CollectionMetadataUpdate, CollectionUuid,
    CreateCollectionError, CreateCollectionResponse, CreateDatabaseError, CreateDatabaseResponse,
    CreateTenantError, CreateTenantResponse, Database, DeleteCollectionError, DeleteDatabaseError,
    DeleteDatabaseResponse, GetCollectionWithSegmentsError, GetCollectionsError, GetDatabaseError,
    GetSegmentsError, GetTenantError, GetTenantResponse, ListDatabasesError, Metadata,
    MetadataValue, Segment, SegmentScope, SegmentType, SegmentUuid, UpdateCollectionError,
};
use futures::TryStreamExt;
use sea_query_binder::SqlxBinder;
use sqlx::error::ErrorKind;
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

//////////////////////// SqliteSysDb ////////////////////////

#[derive(Debug, Clone)]
#[allow(dead_code)]
/// A wrapper around a SqliteDb that accesses the SysDB
/// This is the database that stores metadata about databases, tenants, and collections etc
/// ## Notes
/// - The SqliteSysDb should be "Shareable" - it should be possible to clone it and use it in multiple threads
///     without having divergent state
pub struct SqliteSysDb {
    db: SqliteDb,
}

impl SqliteSysDb {
    #[allow(dead_code)]
    pub fn new(db: SqliteDb) -> Self {
        Self { db }
    }

    ////////////////////////// Database Methods ////////////////////////
    #[allow(dead_code)]
    pub(crate) async fn create_database(
        &self,
        id: uuid::Uuid,
        name: &str,
        tenant: &str,
    ) -> Result<CreateDatabaseResponse, CreateDatabaseError> {
        sqlx::query("INSERT INTO databases (id, name, tenant_id) VALUES ($1, $2, $3)")
            .bind(id.to_string())
            .bind(name)
            .bind(tenant)
            .execute(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(ref db_err)
                    if db_err.kind() == ErrorKind::UniqueViolation =>
                {
                    CreateDatabaseError::AlreadyExists(name.to_string())
                }
                _ => CreateDatabaseError::Internal(e.into()),
            })?;

        Ok(CreateDatabaseResponse {})
    }

    pub(crate) async fn get_database(
        &self,
        name: &str,
        tenant: &str,
    ) -> Result<Database, GetDatabaseError> {
        sqlx::query("SELECT id, name, tenant_id FROM databases WHERE name = $1 AND tenant_id = $2")
            .bind(name)
            .bind(tenant)
            .fetch_one(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => GetDatabaseError::NotFound(name.to_string()),
                _ => GetDatabaseError::Internal(e.into()),
            })
            .and_then(|row| {
                let id = Uuid::from_str(row.get::<&str, _>(0))
                    .map_err(|e| GetDatabaseError::InvalidID(e.to_string()))?;
                Ok(Database {
                    id,
                    name: row.get(1),
                    tenant: row.get(2),
                })
            })
    }

    pub(crate) async fn delete_database(
        &self,
        database_name: String,
        tenant: String,
    ) -> Result<DeleteDatabaseResponse, DeleteDatabaseError> {
        sqlx::query("DELETE FROM databases WHERE name = $1 AND tenant_id = $2")
            .bind(&database_name)
            .bind(tenant)
            .execute(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => DeleteDatabaseError::NotFound(database_name),
                _ => DeleteDatabaseError::Internal(e.into()),
            })?;

        Ok(DeleteDatabaseResponse {})
    }

    pub(crate) async fn list_databases(
        &self,
        tenant_id: String,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Vec<Database>, ListDatabasesError> {
        let mut rows = sqlx::query(
            r#"
                SELECT id, name, tenant_id
                FROM databases
                WHERE tenant_id = $1
                ORDER BY name
                LIMIT $2 OFFSET $3
            "#,
        )
        .bind(tenant_id)
        .bind(limit.unwrap_or(u32::MAX))
        .bind(offset)
        .fetch(self.db.get_conn());

        let mut databases = Vec::new();
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| ListDatabasesError::Internal(e.into()))?
        {
            let id = Uuid::from_str(row.get::<&str, _>(0))
                .map_err(|e| ListDatabasesError::InvalidID(e.to_string()))?;
            databases.push(Database {
                id,
                name: row.get(1),
                tenant: row.get(2),
            });
        }

        Ok(databases)
    }

    ////////////////////////// Tenant Methods ////////////////////////

    pub(crate) async fn create_tenant(
        &self,
        name: String,
    ) -> Result<CreateTenantResponse, CreateTenantError> {
        sqlx::query("INSERT INTO tenants (id) VALUES ($1)")
            .bind(&name)
            .execute(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(ref db_err)
                    if db_err.kind() == ErrorKind::UniqueViolation =>
                {
                    CreateTenantError::AlreadyExists(name.clone())
                }
                _ => CreateTenantError::Internal(e.into()),
            })?;

        Ok(CreateTenantResponse {})
    }

    pub(crate) async fn get_tenant(&self, name: &str) -> Result<GetTenantResponse, GetTenantError> {
        sqlx::query("SELECT id FROM tenants WHERE id = $1")
            .bind(name)
            .fetch_one(self.db.get_conn())
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => GetTenantError::NotFound(name.to_string()),
                _ => GetTenantError::Internal(e.into()),
            })
            .map(|row| GetTenantResponse { name: row.get(0) })
    }

    ////////////////////////// Collection Methods ////////////////////////

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_collection(
        &self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
        name: String,
        segments: Vec<Segment>,
        configuration_json: serde_json::Value,
        metadata: Option<Metadata>,
        dimension: Option<i32>,
        get_or_create: bool,
    ) -> Result<CreateCollectionResponse, CreateCollectionError> {
        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| CreateCollectionError::Internal(e.into()))?;

        let mut existing_collections = self
            .get_collections_with_conn(
                &mut *tx,
                None,
                Some(name.clone()),
                Some(tenant.clone()),
                Some(database.clone()),
                None,
                0,
            )
            .await
            .map_err(CreateCollectionError::Get)?;

        if let Some(collection) = existing_collections.pop() {
            if get_or_create {
                return Ok(collection);
            } else {
                return Err(CreateCollectionError::AlreadyExists(name.to_string()));
            }
        }

        // Look up database
        let database_result =
            sqlx::query("SELECT id FROM databases WHERE name = $1 AND tenant_id = $2")
                .bind(&database)
                .bind(&tenant)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| match e {
                    sqlx::Error::RowNotFound => {
                        CreateCollectionError::DatabaseNotFound(database.clone())
                    }
                    _ => CreateCollectionError::Internal(e.into()),
                })?;
        let database_id = database_result.get::<&str, _>(0);

        sqlx::query(
            r#"
            INSERT INTO collections
                (id, name, config_json_str, dimension, database_id)
            VALUES ($1, $2, $3, $4, $5)
        "#,
        )
        .bind(collection_id.to_string())
        .bind(&name)
        .bind(
            serde_json::to_string(&configuration_json)
                .map_err(CreateCollectionError::Configuration)?,
        )
        .bind(dimension.unwrap_or_default())
        .bind(database_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| CreateCollectionError::Internal(e.into()))?;

        if let Some(metadata) = metadata.clone() {
            update_metadata::<table::CollectionMetadata, _, _>(
                &mut *tx,
                collection_id.to_string(),
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect(),
            )
            .await
            .map_err(|e| e.boxed())?;
        }

        for segment in segments {
            self.create_segment_with_tx(&mut *tx, segment)
                .await
                .map_err(|e| CreateCollectionError::Internal(e.boxed()))?;
        }

        tx.commit()
            .await
            .map_err(|e| CreateCollectionError::Internal(e.into()))?;

        Ok(CreateCollectionResponse {
            collection_id,
            name,
            tenant,
            database,
            configuration_json,
            metadata,
            dimension,
            log_position: 0,
            total_records_post_compaction: 0,
            version: 0,
        })
    }

    pub(crate) async fn update_collection(
        &self,
        collection_id: CollectionUuid,
        name: Option<String>,
        metadata: Option<CollectionMetadataUpdate>,
        dimension: Option<u32>,
    ) -> Result<(), UpdateCollectionError> {
        let mut query = sea_query::Query::update();
        let mut query = query.table(table::Collections::Table).and_where(
            sea_query::Expr::col((table::Collections::Table, table::Collections::Id))
                .eq(collection_id.to_string()),
        );

        if let Some(name) = name {
            query = query.value(table::Collections::Name, name.to_string());
        }

        if let Some(dimension) = dimension {
            query = query.value(table::Collections::Dimension, dimension);
        }

        let (sql, values) = query.build_sqlx(sea_query::SqliteQueryBuilder);

        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| UpdateCollectionError::Internal(e.into()))?;

        let result = sqlx::query_with(&sql, values)
            .execute(&mut *tx)
            .await
            .map_err(|e| UpdateCollectionError::Internal(e.into()))?;
        if result.rows_affected() == 0 {
            return Err(UpdateCollectionError::CollectionNotFound);
        }

        if let Some(metadata) = metadata {
            match metadata {
                CollectionMetadataUpdate::ResetMetadata => {
                    return Err(UpdateCollectionError::MetadataResetUnsupported);
                }
                CollectionMetadataUpdate::UpdateMetadata(metadata) => {
                    update_metadata::<table::CollectionMetadata, _, _>(
                        &mut *tx,
                        collection_id.to_string(),
                        metadata,
                    )
                    .await
                    .map_err(|e| e.boxed())?;
                }
            }
        }

        tx.commit()
            .await
            .map_err(|e| UpdateCollectionError::Internal(e.into()))?;

        Ok(())
    }

    async fn create_segment_with_tx<C>(
        &self,
        conn: &mut C,
        segment: Segment,
    ) -> Result<(), Box<dyn ChromaError>>
    where
        for<'a> &'a mut C: sqlx::Executor<'a, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            r#"
            INSERT INTO segments (id, type, scope, collection) VALUES ($1, $2, $3, $4)
        "#,
        )
        .bind(segment.id.to_string())
        .bind(String::from(segment.r#type))
        .bind(String::from(segment.scope))
        .bind(segment.collection.to_string())
        .execute(&mut *conn)
        .await
        .map_err(|e| WrappedSqlxError(e).boxed())?;

        if let Some(metadata) = segment.metadata {
            update_metadata::<table::SegmentMetadata, _, _>(
                conn,
                segment.id.to_string(),
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect(),
            )
            .await
            .map_err(|e| e.boxed())?;
        }

        Ok(())
    }

    pub(crate) async fn get_collections(
        &self,
        collection_id: Option<CollectionUuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        self.get_collections_with_conn(
            self.db.get_conn(),
            collection_id,
            name,
            tenant,
            database,
            limit,
            offset,
        )
        .await
    }

    pub(crate) async fn delete_collection(
        &self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
        segment_ids: Vec<SegmentUuid>,
    ) -> Result<(), DeleteCollectionError> {
        let mut tx = self
            .db
            .get_conn()
            .begin()
            .await
            .map_err(|e| DeleteCollectionError::Internal(e.into()))?;

        let deleted_rows = sqlx::query(
            r#"
            DELETE FROM collections
            WHERE id = $1
            AND database_id = (SELECT id FROM databases WHERE name = $2 AND tenant_id = $3)
            RETURNING id
        "#,
        )
        .bind(collection_id.to_string())
        .bind(&database)
        .bind(&tenant)
        .execute(&mut *tx)
        .await
        .map_err(|e| DeleteCollectionError::Internal(e.into()))?;

        if deleted_rows.rows_affected() == 0 {
            return Err(DeleteCollectionError::NotFound);
        }

        // Delete segments
        let (sql, values) = sea_query::Query::delete()
            .from_table(table::Segments::Table)
            .and_where(
                sea_query::Expr::col((table::Segments::Table, table::Segments::Id))
                    .is_in(segment_ids.iter().map(|id| id.to_string())),
            )
            .build_sqlx(sea_query::SqliteQueryBuilder);

        sqlx::query_with(&sql, values)
            .execute(&mut *tx)
            .await
            .map_err(|e| DeleteCollectionError::Internal(e.into()))?;

        // Delete segment metadata
        sqlx::query(
            r#"
            DELETE FROM segment_metadata
            WHERE segment_id IN (SELECT id FROM segments WHERE collection = $1)
            "#,
        )
        .bind(collection_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| DeleteCollectionError::Internal(e.into()))?;

        // Delete collection metadata
        sqlx::query(
            r#"
            DELETE FROM collection_metadata
            WHERE collection_id = $1
            "#,
        )
        .bind(collection_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| DeleteCollectionError::Internal(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| DeleteCollectionError::Internal(e.into()))?;

        Ok(())
    }

    pub(crate) async fn get_segments(
        &self,
        id: Option<SegmentUuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: CollectionUuid,
    ) -> Result<Vec<Segment>, GetSegmentsError> {
        todo!()
    }

    pub(crate) async fn get_collection_with_segments(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionAndSegments, GetCollectionWithSegmentsError> {
        let collections = self
            .get_collections_with_conn(
                self.db.get_conn(),
                Some(collection_id),
                None,
                None,
                None,
                None,
                0,
            )
            .await
            .map_err(|e| e.boxed())?;
        let collection = collections
            .first()
            .ok_or(GetCollectionWithSegmentsError::NotFound)?;

        let segments = self
            .get_segments_with_conn(self.db.get_conn(), collection_id)
            .await?;

        let metadata_segment = segments
            .iter()
            .find(|s| s.scope == SegmentScope::METADATA)
            .ok_or(GetCollectionWithSegmentsError::Field(
                "Missing metadata segment".to_string(),
            ))?;

        let vector_segment = segments
            .iter()
            .find(|s| s.scope == SegmentScope::VECTOR)
            .ok_or(GetCollectionWithSegmentsError::Field(
                "Missing vector segment".to_string(),
            ))?;

        Ok(CollectionAndSegments {
            collection: collection.clone(),
            metadata_segment: metadata_segment.clone(),
            vector_segment: vector_segment.clone(),
            record_segment: metadata_segment.clone(), // single node Chroma does not have a record segment
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_collections_with_conn<'a, C>(
        &self,
        conn: C,
        collection_id: Option<CollectionUuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Vec<Collection>, GetCollectionsError>
    where
        C: sqlx::Executor<'a, Database = sqlx::Sqlite>,
    {
        let (sql, values) = sea_query::Query::select()
            .from(table::Collections::Table)
            .inner_join(
                table::Databases::Table,
                sea_query::Expr::col((table::Databases::Table, table::Databases::Id))
                    .equals((table::Collections::Table, table::Collections::DatabaseId)),
            )
            .left_join(
                table::CollectionMetadata::Table,
                sea_query::Expr::col((
                    table::CollectionMetadata::Table,
                    table::CollectionMetadata::CollectionId,
                ))
                .equals((table::Collections::Table, table::Collections::Id)),
            )
            .cond_where(
                sea_query::Cond::all()
                    .add_option(name.map(|name| {
                        sea_query::Expr::col((table::Collections::Table, table::Collections::Name))
                            .eq(name)
                    }))
                    .add_option(database.map(|database| {
                        sea_query::Expr::col((table::Databases::Table, table::Databases::Name))
                            .eq(database)
                    }))
                    .add_option(
                        tenant.map(|tenant| {
                            sea_query::Expr::col(table::Databases::TenantId).eq(tenant)
                        }),
                    )
                    .add_option(collection_id.map(|collection_id| {
                        sea_query::Expr::col((table::Collections::Table, table::Collections::Id))
                            .eq(collection_id.to_string())
                    })),
            )
            .limit(limit.unwrap_or(u32::MAX).into()) // SQLite requires that limit is always set if offset is provided
            .offset(offset.into())
            .column((table::Collections::Table, table::Collections::Id))
            .column((table::Collections::Table, table::Collections::Name))
            .column((table::Collections::Table, table::Collections::ConfigJsonStr))
            .column((table::Collections::Table, table::Collections::Dimension))
            .column((table::Databases::Table, table::Databases::TenantId))
            .column((table::Databases::Table, table::Databases::Name))
            .columns([
                table::CollectionMetadata::Key,
                table::CollectionMetadata::StrValue,
                table::CollectionMetadata::IntValue,
                table::CollectionMetadata::FloatValue,
                table::CollectionMetadata::BoolValue,
            ])
            .build_sqlx(sea_query::SqliteQueryBuilder);

        let mut rows = sqlx::query_with(&sql, values).fetch(conn);
        let mut rows_by_collection_id: HashMap<CollectionUuid, Vec<SqliteRow>> = HashMap::new();

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| GetCollectionsError::Internal(e.into()))?
        {
            let collection_id = CollectionUuid::from_str(row.get::<&str, _>(0))
                .map_err(GetCollectionsError::CollectionId)?;

            if let Some(entry) = rows_by_collection_id.get_mut(&collection_id) {
                entry.push(row);
            } else {
                rows_by_collection_id.insert(collection_id, vec![row]);
            }
        }

        let collections = rows_by_collection_id
            .into_iter()
            .filter_map(|(collection_id, rows)| {
                if rows.is_empty() {
                    // should never happen
                    return None;
                }

                let metadata = self.metadata_from_rows(rows.iter());
                let first_row = rows.first().unwrap();

                let configuration_json =
                    match serde_json::from_str::<serde_json::Value>(first_row.get::<&str, _>(2))
                        .map_err(GetCollectionsError::Configuration)
                    {
                        Ok(configuration_json) => configuration_json,
                        Err(e) => return Some(Err(e)),
                    };

                Some(Ok(Collection {
                    collection_id,
                    configuration_json,
                    metadata,
                    total_records_post_compaction: 0,
                    version: 0,
                    log_position: 0,
                    dimension: first_row.get(3),
                    name: first_row.get(1),
                    tenant: first_row.get(4),
                    database: first_row.get(5),
                }))
            })
            .collect::<Result<Vec<_>, GetCollectionsError>>()?;

        Ok(collections)
    }

    async fn get_segments_with_conn<'a, C>(
        &self,
        conn: C,
        collection_id: CollectionUuid,
    ) -> Result<Vec<Segment>, GetSegmentsError>
    where
        C: sqlx::Executor<'a, Database = sqlx::Sqlite>,
    {
        let (sql, values) = sea_query::Query::select()
            .from(table::Segments::Table)
            .left_join(
                table::SegmentMetadata::Table,
                sea_query::Expr::col((
                    table::SegmentMetadata::Table,
                    table::SegmentMetadata::SegmentId,
                ))
                .equals((table::Segments::Table, table::Segments::Id)),
            )
            .and_where(
                sea_query::Expr::col((table::Segments::Table, table::Segments::Collection))
                    .eq(collection_id.to_string()),
            )
            .column((table::Segments::Table, table::Segments::Id))
            .column((table::Segments::Table, table::Segments::Type))
            .column((table::Segments::Table, table::Segments::Scope))
            .columns([
                table::SegmentMetadata::Key,
                table::SegmentMetadata::StrValue,
                table::SegmentMetadata::IntValue,
                table::SegmentMetadata::FloatValue,
                table::SegmentMetadata::BoolValue,
            ])
            .build_sqlx(sea_query::SqliteQueryBuilder);

        let mut rows = sqlx::query_with(&sql, values).fetch(conn);
        let mut rows_by_segment_id: HashMap<SegmentUuid, Vec<SqliteRow>> = HashMap::new();

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| WrappedSqlxError(e).boxed())?
        {
            let segment_id = SegmentUuid::from_str(row.get::<&str, _>(0))
                .map_err(GetSegmentsError::SegmentConversion)?;

            if let Some(entry) = rows_by_segment_id.get_mut(&segment_id) {
                entry.push(row);
            } else {
                rows_by_segment_id.insert(segment_id, vec![row]);
            }
        }

        let segments = rows_by_segment_id
            .into_iter()
            .filter_map(|(segment_id, rows)| {
                if rows.is_empty() {
                    // should never happen
                    return None;
                }

                let metadata = self.metadata_from_rows(rows.iter());
                let first_row = rows.first().unwrap();

                let segment_type = match SegmentType::try_from(first_row.get::<&str, _>(1))
                    .map_err(GetSegmentsError::SegmentConversion)
                {
                    Ok(segment_type) => segment_type,
                    Err(err) => return Some(Err(err)),
                };

                let segment_scope = match SegmentScope::try_from(first_row.get::<&str, _>(2))
                    .map_err(GetSegmentsError::UnknownScope)
                {
                    Ok(scope) => scope,
                    Err(err) => return Some(Err(err)),
                };

                Some(Ok(Segment {
                    id: segment_id,
                    r#type: segment_type,
                    scope: segment_scope,
                    collection: collection_id,
                    metadata,
                    file_path: HashMap::new(),
                }))
            })
            .collect::<Result<Vec<_>, GetSegmentsError>>()?;

        Ok(segments)
    }

    // TODO: reuse logic from metadata reader
    fn metadata_from_rows<'row>(
        &self,
        rows: impl Iterator<Item = &'row SqliteRow>,
    ) -> Option<Metadata> {
        let metadata: Metadata = rows
            .map(|row| {
                let key = row.get::<&str, _>("key");

                if let Some(str_value) = row.get::<Option<String>, _>("str_value") {
                    (key.to_string(), MetadataValue::Str(str_value))
                } else if let Some(int_value) = row.get::<Option<i64>, _>("int_value") {
                    (key.to_string(), MetadataValue::Int(int_value))
                } else if let Some(float_value) = row.get::<Option<f64>, _>("float_value") {
                    (key.to_string(), MetadataValue::Float(float_value))
                } else if let Some(bool_value) = row.get::<Option<bool>, _>("bool_value") {
                    (key.to_string(), MetadataValue::Bool(bool_value))
                } else {
                    // should never happen
                    (key.to_string(), MetadataValue::Str("".to_string()))
                }
            })
            .collect();

        if metadata.is_empty() {
            None
        } else {
            Some(metadata)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{
        SegmentScope, SegmentType, SegmentUuid, UpdateMetadata, UpdateMetadataValue,
    };

    #[tokio::test]
    async fn test_create_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);
        let db_id = uuid::Uuid::new_v4();
        sysdb
            .create_database(db_id, "test", "default_tenant")
            .await
            .unwrap();

        // Second call should fail
        let result = sysdb
            .create_database(uuid::Uuid::new_v4(), "test", "default_tenant")
            .await;

        matches!(result, Err(CreateDatabaseError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_get_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        // Get non-existent database
        let result = sysdb.get_database("test", "default_tenant").await;
        matches!(result, Err(GetDatabaseError::NotFound(_)));

        let db_id = uuid::Uuid::new_v4();
        sysdb
            .create_database(db_id, "test", "default_tenant")
            .await
            .unwrap();

        let database = sysdb.get_database("test", "default_tenant").await.unwrap();
        assert_eq!(database.id, db_id);
    }

    #[tokio::test]
    async fn test_delete_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        // Delete non-existent database
        let result = sysdb
            .delete_database("test".to_string(), "default_tenant".to_string())
            .await;
        matches!(result, Err(DeleteDatabaseError::NotFound(_)));

        let db_id = uuid::Uuid::new_v4();
        sysdb
            .create_database(db_id, "test", "default_tenant")
            .await
            .unwrap();

        // Delete database
        sysdb
            .delete_database("test".to_string(), "default_tenant".to_string())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_list_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        // List default databases
        let databases = sysdb
            .list_databases("default_tenant".to_string(), None, 0)
            .await
            .unwrap();
        assert_eq!(databases.len(), 1);

        // Create database and list again
        let db_id = uuid::Uuid::new_v4();
        sysdb
            .create_database(db_id, "test", "default_tenant")
            .await
            .unwrap();

        let databases = sysdb
            .list_databases("default_tenant".to_string(), None, 0)
            .await
            .unwrap();
        assert_eq!(databases.len(), 2);

        // Offset list by 1 and limit to 1 result
        let databases = sysdb
            .list_databases("default_tenant".to_string(), Some(1), 1)
            .await
            .unwrap();
        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0].name, "test");
    }

    #[tokio::test]
    async fn test_create_tenant() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        // Create tenant
        sysdb.create_tenant("new_tenant".to_string()).await.unwrap();

        // Second call should fail
        let result = sysdb.create_tenant("new_tenant".to_string()).await;
        matches!(result, Err(CreateTenantError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_get_tenant() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        // Get non-existent tenant
        let result = sysdb.get_tenant("test").await;
        matches!(result, Err(GetTenantError::NotFound(_)));

        // Create tenant
        sysdb.create_tenant("new_tenant".to_string()).await.unwrap();

        // Get tenant
        let tenant = sysdb.get_tenant("new_tenant").await.unwrap();
        assert_eq!(tenant.name, "new_tenant");
    }

    #[tokio::test]
    async fn test_create_collection() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        let mut collection_metadata = Metadata::new();
        collection_metadata.insert("key1".to_string(), MetadataValue::Str("value1".to_string()));
        collection_metadata.insert("key2".to_string(), MetadataValue::Int(42));
        collection_metadata.insert("key3".to_string(), MetadataValue::Float(42.0));
        collection_metadata.insert("key4".to_string(), MetadataValue::Bool(true));

        let collection_id = CollectionUuid::new();
        let segments = vec![Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        }];
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                serde_json::Value::Null,
                Some(collection_metadata.clone()),
                None,
                false,
            )
            .await
            .unwrap();

        let collections = sysdb
            .get_collections(Some(collection_id), None, None, None, None, 0)
            .await
            .unwrap();
        let collection = collections.first().unwrap();

        assert_eq!(collection.name, "test_collection");
        assert_eq!(collection.metadata.as_ref().unwrap().len(), 4);
        assert_eq!(collection.metadata, Some(collection_metadata));
    }

    #[tokio::test]
    async fn test_create_collection_fails_for_duplicate_name() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        let collection_id = CollectionUuid::new();
        let segments = vec![Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        }];
        let result = sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                serde_json::Value::Null,
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.name, "test_collection");

        // Should fail when attempting to create with the same name
        let result = sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments,
                serde_json::Value::Null,
                None,
                None,
                false,
            )
            .await;
        matches!(result, Err(CreateCollectionError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_create_collection_get_or_create() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        let collection_id = CollectionUuid::new();
        let segments = vec![Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        }];
        let result = sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                serde_json::Value::Null,
                None,
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.name, "test_collection");

        // Should return existing collection
        let result = sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                CollectionUuid::new(),
                "test_collection".to_string(),
                vec![],
                serde_json::Value::Null,
                None,
                None,
                true,
            )
            .await
            .unwrap();
        assert_eq!(result.collection_id, collection_id);
    }

    #[tokio::test]
    async fn test_update_collection() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        let collection_id = CollectionUuid::new();
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                vec![],
                serde_json::Value::Null,
                None,
                None,
                false,
            )
            .await
            .unwrap();

        let mut metadata: UpdateMetadata = HashMap::new();
        metadata.insert(
            "key1".to_string(),
            UpdateMetadataValue::Str("value1".to_string()),
        );

        sysdb
            .update_collection(
                collection_id,
                Some("new_name".to_string()),
                Some(CollectionMetadataUpdate::UpdateMetadata(metadata)),
                Some(1024),
            )
            .await
            .unwrap();

        let collections = sysdb
            .get_collections(Some(collection_id), None, None, None, None, 0)
            .await
            .unwrap();
        let collection = collections.first().unwrap();
        assert_eq!(collection.name, "new_name");
        assert_eq!(collection.dimension, Some(1024));
        let metadata = collection.metadata.as_ref().unwrap();
        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata.get("key1").unwrap(),
            &MetadataValue::Str("value1".to_string())
        );
    }

    #[tokio::test]
    async fn test_delete_collection() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        let collection_id = CollectionUuid::new();
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                vec![],
                serde_json::Value::Null,
                None,
                None,
                false,
            )
            .await
            .unwrap();

        // Delete non-existent collection
        let result = sysdb
            .delete_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                CollectionUuid::new(),
                vec![],
            )
            .await;
        matches!(result, Err(DeleteCollectionError::NotFound));

        // Delete collection
        sysdb
            .delete_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                vec![],
            )
            .await
            .unwrap();

        // Should no longer exist
        let result = sysdb
            .get_collections(Some(collection_id), None, None, None, None, 0)
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_get_collection_with_segments() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);

        let mut collection_metadata = Metadata::new();
        collection_metadata.insert("key1".to_string(), MetadataValue::Str("value1".to_string()));
        collection_metadata.insert("key2".to_string(), MetadataValue::Int(42));
        collection_metadata.insert("key3".to_string(), MetadataValue::Float(42.0));
        collection_metadata.insert("key4".to_string(), MetadataValue::Bool(true));

        let segment_metadata = collection_metadata.clone();

        let collection_id = CollectionUuid::new();
        let segments = vec![
            Segment {
                id: SegmentUuid::new(),
                r#type: SegmentType::BlockfileMetadata,
                scope: SegmentScope::METADATA,
                collection: collection_id,
                metadata: Some(segment_metadata),
                file_path: HashMap::new(),
            },
            Segment {
                id: SegmentUuid::new(),
                r#type: SegmentType::HnswDistributed,
                scope: SegmentScope::VECTOR,
                collection: collection_id,
                metadata: None,
                file_path: HashMap::new(),
            },
        ];
        sysdb
            .create_collection(
                "default_tenant".to_string(),
                "default_database".to_string(),
                collection_id,
                "test_collection".to_string(),
                segments.clone(),
                serde_json::Value::Null,
                Some(collection_metadata.clone()),
                None,
                false,
            )
            .await
            .unwrap();

        let collection_and_segments = sysdb
            .get_collection_with_segments(collection_id)
            .await
            .unwrap();

        assert_eq!(collection_and_segments.collection.name, "test_collection");
        assert_eq!(collection_and_segments.metadata_segment.id, segments[0].id);
        assert_eq!(
            collection_and_segments.metadata_segment.metadata,
            segments[0].metadata
        );
    }
}
