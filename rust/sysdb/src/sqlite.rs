use sqlx::{query, sqlite::SqlitePool};
use sqlx::{Executor, Row};
use std::path::PathBuf;

//////////////////////// SqliteDb ////////////////////////////

#[derive(Clone)]
struct SqliteDBConfig {
    url: String,
    // TODO: change this to something bundled with binary
    migrations_root_dir: PathBuf,
}

struct SqliteDb {
    conn: SqlitePool,
    config: SqliteDBConfig,
}

impl SqliteDb {
    pub async fn try_from_config(config: &SqliteDBConfig) -> Result<Self, String> {
        // TODO: error type
        let conn = SqlitePool::connect(&config.url)
            .await
            .map_err(|e| e.to_string())?;
        let db = Self {
            conn,
            config: config.clone(),
        };
        db.initialize_migrations().await?;
        Ok(db)
    }

    async fn initialize_migrations(&self) -> Result<(), String> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS migrations (
                dir TEXT NOT NULL,
                version INTEGER NOT NULL,
                filename TEXT NOT NULL,
                sql TEXT NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (dir, version)
            )
        "#;
        sqlx::query(query)
            .execute(&self.conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Get existing migrations for a given directory
    /// Arguments:
    /// - dir_name: str - The name of the directory that contains the migrations
    /// Returns:
    /// - Vec<Migration> - A list of migrations
    /// Notes
    /// - dir_name has to be held constant for a given migration directory
    /// - The migrations are sorted by version in ascending order
    /// - The dir_name is consistent with the python implementation
    async fn get_existing_migrations(&self, dir: &MigrationDir) -> Vec<Migration> {
        let query = r#"
            SELECT dir, version, filename, sql, hash
            FROM migrations
            WHERE dir = $1
            ORDER BY version ASC
        "#;
        let rows = sqlx::query(query)
            .bind(dir.as_str())
            .fetch_all(&self.conn)
            .await
            .expect("Expect it to be fetched");

        let mut migrations = Vec::new();
        for row in rows {
            let dir: String = row.get("dir");
            let version: i32 = row.get("version");
            let filename: String = row.get("filename");
            let sql: String = row.get("sql");
            let hash: String = row.get("hash");
            migrations.push(Migration {
                dir,
                version,
                filename,
                sql,
                hash,
            });
        }
        migrations
    }
}

struct Migration {
    dir: String,
    filename: String,
    version: i32,
    sql: String,
    hash: String,
}

enum MigrationDir {
    SysDb,
    MetaDb,
    EmbeddingsQueue,
}

const migration_dirs: [MigrationDir; 3] = [
    MigrationDir::SysDb,
    MigrationDir::MetaDb,
    MigrationDir::EmbeddingsQueue,
];

impl MigrationDir {
    fn as_str(&self) -> &str {
        match self {
            Self::SysDb => "sysdb",
            Self::MetaDb => "metadb",
            Self::EmbeddingsQueue => "embeddings_queue",
        }
    }
}

//////////////////////// SqliteSysDb ////////////////////////

struct SqliteSysDb {
    conn: SqlitePool,
}

impl SqliteSysDb {
    pub fn new(conn: SqlitePool) -> Self {
        Self { conn }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    fn test_migration_dir() -> PathBuf {
        let migration_dir = "/Users/hammad/Documents/chroma/chromadb/migrations";
        PathBuf::from(migration_dir)
    }

    fn test_db_path() -> String {
        // TODO: return bundled path
        "/Users/hammad/Documents/chroma/chroma/chroma.sqlite3".to_string()
    }

    #[tokio::test]
    async fn test_sqlite_db() {
        let config = SqliteDBConfig {
            url: "sqlite::memory:".to_string(),
            migrations_root_dir: test_migration_dir(),
        };
        let db = SqliteDb::try_from_config(&config)
            .await
            .expect("Expect it to be created");

        // Check if migrations table exists
        let query = r#"
            SELECT name FROM sqlite_master WHERE type='table' AND name='migrations'
        "#;
        let row = sqlx::query(query)
            .fetch_one(&db.conn)
            .await
            .expect("Expect it to be fetched");
        let name: String = row.get("name");
        assert_eq!(name, "migrations");
    }

    #[tokio::test]
    async fn test_migrations() {
        let config = SqliteDBConfig {
            url: test_db_path(),
            migrations_root_dir: test_migration_dir(),
        };
        let db = SqliteDb::try_from_config(&config)
            .await
            .expect("Expect it to be created");

        // Check if migrations table exists
        let query = r#"
            SELECT name FROM sqlite_master WHERE type='table' AND name='migrations'
        "#;
        let row = sqlx::query(query)
            .fetch_one(&db.conn)
            .await
            .expect("Expect it to be fetched");
        let name: String = row.get("name");
        assert_eq!(name, "migrations");
    }

    #[tokio::test]
    async fn test_migrations_exist() {
        let config = SqliteDBConfig {
            url: test_db_path(),
            migrations_root_dir: test_migration_dir(),
        };
        let db = SqliteDb::try_from_config(&config)
            .await
            .expect("Expect it to be created");
        for dir in migration_dirs {
            let migrations = db.get_existing_migrations(&dir).await;
            let on_disk_path = test_migration_dir().join(dir.as_str());
            // See how many files are in the directory
            let files = std::fs::read_dir(on_disk_path).unwrap();
            let num_files = files.count();
            assert_eq!(migrations.len(), num_files);
        }
    }

    #[tokio::test]
    async fn test_sqlite_sysdb() {
        let conn = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("Expect it to be connected");
        let sysdb = SqliteSysDb::new(conn);
    }
}
