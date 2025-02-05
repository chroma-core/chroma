use chroma_sqlite::db::SqliteDb;
use chroma_types::{
    Collection, CollectionUuid, CreateDatabaseError, CreateDatabaseResponse, CreateTenantError,
    CreateTenantResponse, Database, DeleteDatabaseError, DeleteDatabaseResponse, GetDatabaseError,
    GetTenantError, GetTenantResponse, ListDatabasesError, Metadata, Segment,
};
use futures::TryStreamExt;
use sqlx::error::ErrorKind;
use sqlx::Row;
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

    // TODO: real error
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
    pub(crate) async fn _create_collection(
        &self,
        // TODO: unify all id types on wrappers
        _id: Option<CollectionUuid>,
        _name: &str,
        _segments: Vec<Segment>,
        _metadata: Option<&Metadata>,
        _dimension: Option<i32>,
        _get_or_create: bool,
        _tenant: Option<&str>,
        _database: Option<&str>,
    ) -> Result<(Collection, bool), String> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;

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
}
