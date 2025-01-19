use crate::sqlite::{MigrationHash, MigrationMode, SqliteDb};
use chroma_types::{Database, Tenant};
use sqlx::Row;
use sqlx::{error::ErrorKind, Executor};
use std::path::PathBuf;

//////////////////////// SqliteSysDb ////////////////////////

const DEFAULT_TENANT: &str = "default_tenant";
#[derive(Debug)]
pub struct SqliteSysDb {
    db: SqliteDb,
}

impl SqliteSysDb {
    fn new(db: SqliteDb) -> Self {
        Self { db }
    }

    // TODO: THIS IS JUST FOR TESTING
    // REMOVE IN FAVOR OF CONFIG PARSING
    pub async fn new_hack_test(path: &str) -> Self {
        let sqlite_db_config = crate::sqlite::SqliteDBConfig {
            url: path.to_string(),
            migrations_root_dir: PathBuf::from(
                "/Users/hammad/Documents/chroma/chromadb/migrations",
            ),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let db = SqliteDb::try_from_config(&sqlite_db_config).await.unwrap();
        Self::new(db)
    }

    // TODO: real error
    // TODO: shouldn't need to be pub since enum should wrap
    // TODO: maybe this should not be optional?
    pub async fn create_database(
        &self,
        id: uuid::Uuid,
        name: &str,
        tenant: Option<&str>,
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

    pub async fn get_database(&self, name: &str, tenant: Option<&str>) -> Result<Database, String> {
        let query_str =
            "SELECT id, name, tenant_id FROM databases WHERE name = $1 AND tenant_id = $2";
        let conn = self.db.get_conn();
        let query = sqlx::query(query_str)
            .bind(name)
            .bind(tenant.unwrap_or(DEFAULT_TENANT));
        let row = conn.fetch_one(query).await.map_err(|e| e.to_string())?;
        // TODO: error
        Ok(Database {
            id: uuid::Uuid::parse_str(row.get("id")).map_err(|e| e.to_string())?,
            name: row.get("name"),
            tenant: row.get("tenant_id"),
        })
    }

    pub async fn create_tenant(&self, name: &str) -> Result<Tenant, String> {
        let query = "INSERT INTO tenants (id) VALUES ($1)";

        let conn = self.db.get_conn();
        let query = sqlx::query(query).bind(name);
        let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
        let res = tx.execute(query).await;
        match res {
            Ok(_) => {}
            Err(e) => match e {
                sqlx::Error::Database(ref db_err) => {
                    if db_err.kind() == ErrorKind::UniqueViolation {
                        // TODO: real error
                        return Err(format!("Tenant {} already exists", name));
                    }
                }
                _ => {
                    return Err(e.to_string());
                }
            },
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(Tenant {
            id: name.to_string(),
            // last_compaction_time is a distributed system concept only
            last_compaction_time: 0,
        })
    }

    pub async fn get_tenant(&self, name: &str) -> Result<Tenant, String> {
        let query_str = "SELECT id FROM tenants WHERE id = $1";
        let conn = self.db.get_conn();
        let query = sqlx::query(query_str).bind(name);
        let row = conn.fetch_one(query).await.map_err(|e| e.to_string())?;
        Ok(Tenant {
            id: row.get("id"),
            last_compaction_time: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::sqlite::tests::get_new_sqlite_db;

    use super::*;

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

    #[tokio::test]
    async fn test_create_get_tenant() {
        let db = get_new_sqlite_db().await;
        let sysdb = SqliteSysDb::new(db);
        sysdb
            .create_tenant("test")
            .await
            .expect("Tenant to be created");

        // Second call should fail
        let result = sysdb.create_tenant("test").await;

        // TODO: real error
        assert_eq!(result, Err("Tenant test already exists".to_string()));

        let tenant = sysdb.get_tenant("test").await.unwrap();
        assert_eq!(tenant.id, "test");
    }
}
