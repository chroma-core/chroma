use chroma::sqlite::{MigrationHash, MigrationMode, SqliteDb};
use chroma_types::{
    Collection, CollectionUuid, Database, Metadata, MetadataValue, Segment, Tenant,
};
use sqlx::sqlite::SqliteRow;
use sqlx::{error::ErrorKind, Executor};
use sqlx::{Column, Row};
use std::collections::HashMap;
use std::path::PathBuf;

//////////////////////// SqliteSysDb ////////////////////////

#[derive(Debug)]
pub struct SqliteSysDb {
    db: SqliteDb,
}

impl SqliteSysDb {
    fn new(db: SqliteDb) -> Self {
        Self { db }
    }

    ////////////////////////// Database Methods ////////////////////////

    // TODO: real error
    pub(crate) async fn create_database(
        &self,
        id: uuid::Uuid,
        name: &str,
        tenant: &str,
    ) -> Result<(), String> {
        let query = "INSERT INTO databases (id, name, tenant_id) VALUES ($1, $2, $3)";

        let conn = self.db.get_conn();
        let query = sqlx::query(query)
            .bind(id.to_string())
            .bind(name)
            .bind(tenant.unwrap_or(DEFAULT_TENANT));

        // TODO: error
        let mut tx = conn.begin().await.map_err(|e| e.to_string())?;

        let res = tx.execute(query).await;
        match res {
            Ok(_) => {}
            Err(e) => match e {
                sqlx::Error::Database(ref db_err) => {
                    if db_err.kind() == ErrorKind::UniqueViolation {
                        // TODO: real error
                        return Err(format!(
                            "Database {} already exists for tenant {}",
                            name,
                            tenant.unwrap_or("default_tenant")
                        ));
                    }
                }
                _ => {
                    return Err(e.to_string());
                }
            },
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    pub(crate) async fn get_database(&self, name: &str, tenant: &str) -> Result<Database, String> {
        unimplemented!();
    }

    ////////////////////////// Tenant Methods ////////////////////////

    pub(crate) async fn create_tenant(&self, name: &str) -> Result<Tenant, String> {
        unimplemented!();
    }

    pub(crate) async fn get_tenant(&self, name: &str) -> Result<Tenant, String> {
        unimplemented!();
    }

    ////////////////////////// Collection Methods ////////////////////////

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_collection(
        &self,
        // TODO: unify all id types on wrappers
        id: Option<CollectionUuid>,
        name: &str,
        segments: Vec<Segment>,
        metadata: Option<&Metadata>,
        dimension: Option<i32>,
        get_or_create: bool,
        tenant: Option<&str>,
        database: Option<&str>,
    ) -> Result<(Collection, bool), String> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma::sqlite::test_utils::get_new_sqlite_db;
    use chroma_types::{SegmentScope, SegmentType, SegmentUuid};

    #[tokio::test]
    async fn test_create_database() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);
        let db_id = uuid::Uuid::new_v4();
        sysdb.create_database(db_id, "test", None).await.unwrap();

        // Second call should fail
        let result = sysdb
            .create_database(uuid::Uuid::new_v4(), "test", None)
            .await;

        // TODO:
        // test same id or name
        // custom tenant

        // TODO: real error
        assert_eq!(
            result,
            Err("Database test already exists for tenant default_tenant".to_string())
        );

        let db = sysdb
            .get_database("test", None)
            .await
            .expect("Database to be created");
        assert_eq!(db.name, "test");
        assert_eq!(db.tenant, "default_tenant");
        assert_eq!(db.id, db_id);
    }
}
