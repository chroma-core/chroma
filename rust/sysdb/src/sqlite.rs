use std::str::FromStr;

use chroma_sqlite::db::SqliteDb;
use chroma_types::{
    Collection, CollectionUuid, CreateDatabaseError, CreateDatabaseResponse, Database,
    DeleteDatabaseError, DeleteDatabaseResponse, GetDatabaseError, Metadata, Segment, Tenant,
};
use sqlx::error::ErrorKind;
use sqlx::Row;
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

    ////////////////////////// Tenant Methods ////////////////////////

    pub(crate) async fn _create_tenant(&self, _name: &str) -> Result<Tenant, String> {
        unimplemented!();
    }

    pub(crate) async fn _get_tenant(&self, _name: &str) -> Result<Tenant, String> {
        unimplemented!();
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

        let database = sysdb.get_database("test", "default_tenant").await.unwrap();
        assert_eq!(database.name, "test");

        // Delete database
        sysdb
            .delete_database("test".to_string(), "default_tenant".to_string())
            .await
            .unwrap();

        // Second call should fail
        let result = sysdb
            .delete_database("test".to_string(), "default_tenant".to_string())
            .await;
        matches!(result, Err(DeleteDatabaseError::NotFound(_)));

        // TODO: Add tests
        // test same id or name
        // custom tenant

        // let db = sysdb
        //     .get_database("test", "default_tenant")
        //     .await
        //     .expect("Database to be created");
        // assert_eq!(db.name, "test");
        // assert_eq!(db.tenant, "default_tenant");
        // assert_eq!(db.id, db_id);
    }
}
