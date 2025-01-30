use crate::config::{MigrationHash, MigrationMode, SqliteDBConfig};
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use sqlx::{Executor, Row};

// TODO:
// - support memory mode, add concurrency tests
struct SqliteDb {
    conn: SqlitePool,
    config: SqliteDBConfig,
    filename_regex: regex::Regex,
}

impl SqliteDb {
    pub async fn try_from_config(config: &SqliteDBConfig) -> Result<Self, String> {
        // TODO: error type
        // TODO: copy all other pragmas from python and add basic tests
        let options = SqliteConnectOptions::new()
            .filename(&config.url)
            // Due to a bug in the python code, foreign_keys is turned off
            // The python code enabled it in a transaction, however,
            // https://www.sqlite.org/pragma.html states that foreign_keys
            // is a no-op in a transaction. In order to be able to run our migrations
            // we turn it off
            .pragma("foreign_keys", "OFF")
            .pragma("case_sensitive_like", "ON")
            .create_if_missing(true);
        let conn = SqlitePool::connect_with(options)
            .await
            .map_err(|e| e.to_string())?;

        // TODO: error type
        let filename_regex =
            regex::Regex::new(r"(\d+)-(.+)\.(.+)\.sql").map_err(|e| e.to_string())?;

        let db = Self {
            conn,
            config: config.clone(),
            filename_regex,
        };

        db.validate_migrations_root_dir()?;
        db.initialize_migrations_table().await?;
        match config.migration_mode {
            MigrationMode::Apply => {
                let mut all_unapplied_migrations = Vec::new();
                for dir in migration_dirs.iter() {
                    let applied_migrations = db.get_existing_migrations(dir).await;
                    let source_migrations = db.get_source_migrations(dir).await?;
                    let unapplied = db.validate_migrations_and_get_unapplied(
                        applied_migrations,
                        source_migrations,
                    )?;
                    all_unapplied_migrations.extend(unapplied);
                }
                db.apply_migrations(all_unapplied_migrations).await?;
            }
            MigrationMode::Validate => {
                // TODO: Test this
                if !db.has_initialized_migrations().await {
                    return Err("Migrations table not initialized".to_string());
                }
                for dir in migration_dirs.iter() {
                    let applied_migrations = db.get_existing_migrations(dir).await;
                    let source_migrations = db.get_source_migrations(dir).await?;
                    let unapplied = db.validate_migrations_and_get_unapplied(
                        applied_migrations,
                        source_migrations,
                    )?;
                    if !unapplied.is_empty() {
                        return Err("Unapplied migrations found".to_string());
                    }
                }
            }
        }
        Ok(db)
    }

    //////////////////////// Migrations ////////////////////////

    // TODO: Real error
    /// Apply all migrations in a transaction
    /// Arguments:
    /// - migrations: Vec<Migration> - The migrations to apply
    async fn apply_migrations(&self, migrations: Vec<Migration>) -> Result<(), String> {
        let mut tx = self.conn.begin().await.map_err(|e| e.to_string())?;
        for migration in migrations {
            println!("Applying migration: {}", migration.filename);
            // Apply the migration
            tx.execute("PRAGMA foreign_keys = ON")
                .await
                .map_err(|e| e.to_string())?;
            tx.execute(sqlx::query(&migration.sql))
                .await
                .map_err(|e| e.to_string())?;
            println!("Applied migration: {}", migration.filename);

            // Bookkeeping
            let query = r#"
                INSERT INTO migrations (dir, version, filename, sql, hash)
                VALUES ($1, $2, $3, $4, $5)
            "#;
            let query = sqlx::query(query)
                .bind(&migration.dir)
                .bind(migration.version)
                .bind(&migration.filename)
                .bind(&migration.sql)
                .bind(&migration.hash);
            tx.execute(query).await.map_err(|e| e.to_string())?;
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Validate the migrations root directory
    fn validate_migrations_root_dir(&self) -> Result<(), String> {
        // TODO: replace with ChromaError
        for dir in migration_dirs.iter() {
            let path = self.config.migrations_root_dir.join(dir.as_str());
            if !path.exists() {
                return Err(format!("Migration directory {:?} does not exist", path));
            }
        }
        Ok(())
    }

    // TODO: Real error
    /// Validate migration sequence and get the migrations that need to be applied
    /// Arguments:
    /// - applied_migrations: Vec<Migration> - The migrations that have been applied, in ascending version order
    /// - source_migrations: Vec<Migration> - The migrations that are on disk, in ascending version order
    /// Returns:
    /// - Vec<Migration> - The migrations that need to be applied
    fn validate_migrations_and_get_unapplied(
        &self,
        applied_migrations: Vec<Migration>,
        source_migrations: Vec<Migration>,
    ) -> Result<Vec<Migration>, String> {
        for (db_migration, source_migration) in
            applied_migrations.iter().zip(source_migrations.iter())
        {
            if db_migration.version != source_migration.version {
                return Err(format!(
                    "Inconsistent version: db={}, source={}",
                    db_migration.version, source_migration.version
                ));
            }
            if db_migration.hash != source_migration.hash {
                return Err(format!(
                    "Inconsistent hash: db={}, source={}",
                    db_migration.hash, source_migration.hash
                ));
            }
        }

        let unapplied = source_migrations[applied_migrations.len()..].to_vec();
        Ok(unapplied)
    }

    /// Initialize the migrations table
    /// Note:
    /// - This function is idempotent
    async fn initialize_migrations_table(&self) -> Result<(), String> {
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

    /// Check if the migrations table has been initialized
    /// Returns:
    /// - bool - True if the migrations table has been initialized
    async fn has_initialized_migrations(&self) -> bool {
        let query = r#"
            SELECT name FROM sqlite_master WHERE type='table' AND name='migrations'
        "#;
        let row = sqlx::query(query)
            .fetch_one(&self.conn)
            .await
            .expect("Expect it to be fetched");
        let name: String = row.get("name");
        name == "migrations" // Sanity check
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

    // TODO: REAL ERROR
    /// Get the migrations that are on disk
    /// Arguments:
    /// - dir: str - The name of the directory that contains the migrations
    /// Returns:
    /// - Vec<Migration> - A list of migrations found on disk, sorted by version in ascending order
    /// Notes:
    /// - Uses the migrations_root_dir of this SqlDB instance
    async fn get_source_migrations(&self, dir: &MigrationDir) -> Result<Vec<Migration>, String> {
        let on_disk_path = self.config.migrations_root_dir.join(dir.as_str());
        let mut migrations = Vec::new();
        let mut read_dir = tokio::fs::read_dir(on_disk_path)
            .await
            .map_err(|e| e.to_string())?;

        while let Some(entry) = read_dir.next_entry().await.map_err(|e| e.to_string())? {
            let path = entry.path();
            let filename = match path.file_name() {
                Some(filename) => filename,
                None => return Err("Filename is None".to_string()),
            };
            let filename = match filename.to_str() {
                Some(filename) => filename,
                None => return Err("Filename is not valid".to_string()),
            };
            let (version, _) = self.parse_migration_filename(filename)?;
            let sql = tokio::fs::read_to_string(&path)
                .await
                .map_err(|e| e.to_string())?;
            let hash = match self.config.hash_type {
                MigrationHash::SHA256 => {
                    let mut hasher = Sha256::new();
                    hasher.update(sql.as_bytes());
                    format!("{:x}", hasher.finalize())
                }
                MigrationHash::MD5 => {
                    let hash = md5::compute(sql.as_bytes());
                    format!("{:x}", hash)
                }
            };
            migrations.push(Migration {
                dir: dir.as_str().to_string(),
                version,
                filename: filename.to_string(),
                sql,
                hash,
            });
        }
        // TODO: Make a Vec<Migration> wrapper type that enforces sorting
        migrations.sort_by(|a, b| a.version.cmp(&b.version));
        Ok(migrations)
    }

    // Parse the migration filename
    // Arguments:
    // - filename: str - The filename of the migration
    // Returns:
    // - (i32, str) - The version and scope of the migration
    // Notes
    // - Format is <version>-<name>.<scope>.sql
    // - e.g, 00001-users.sqlite.sql
    // - scope is unused, it is legacy from the python implementation. It is
    // written but never read
    fn parse_migration_filename(&self, filename: &str) -> Result<(i32, String), String> {
        let regex_match = self.filename_regex.captures(filename);
        let groups = match regex_match {
            Some(groups) => groups,
            // TODO: Error
            None => return Err(format!("Invalid migration filename: {}", filename)),
        };

        // Parse version
        let version = match groups.get(1) {
            Some(version) => version,
            None => return Err("Failed to find version".to_string()),
        };
        let version = match version.as_str().parse::<i32>() {
            Ok(version) => version,
            Err(e) => return Err(e.to_string()),
        };

        // Parse scope
        let scope = match groups.get(3) {
            Some(scope) => scope,
            None => return Err("Failed to find scope".to_string()),
        };
        let scope = scope.as_str().to_string();

        Ok((version, scope))
    }
}

#[derive(Clone)]
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
    use crate::config::MigrationHash;
    use sqlx::Row;
    use tempfile::tempdir;

    //////////////////////// Test Helpers ////////////////////////

    fn test_migration_dir() -> PathBuf {
        let migration_dir = "/Users/hammad/Documents/chroma/chromadb/migrations";
        PathBuf::from(migration_dir)
    }

    fn existing_test_db_path() -> String {
        // TODO: return bundled path
        "/Users/hammad/Documents/chroma/chroma/chroma.sqlite3".to_string()
    }

    fn new_test_db_path() -> String {
        // TODO: Make tmpfile work
        // let dir = tempdir().expect("Expect it to be created");
        // let path = dir.path().join("chroma.sqlite3");
        let path = "/Users/hammad/Documents/chroma/chroma/chromaTEST.sqlite3".to_string();
        // remove the file if it exists
        std::fs::remove_file(&path).unwrap_or_default();
        path
    }

    //////////////////////// SqliteDb ////////////////////////

    #[tokio::test]
    async fn test_sqlite_db() {
        let config = SqliteDBConfig {
            url: "sqlite::memory:".to_string(),
            migrations_root_dir: test_migration_dir(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
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
    async fn test_migrations_validate_on_existing_db() {
        let config: SqliteDBConfig = SqliteDBConfig {
            url: existing_test_db_path(),
            migrations_root_dir: test_migration_dir(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Validate,
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
    async fn test_migrations_get_applied_on_new_db() {
        let config = SqliteDBConfig {
            url: new_test_db_path(),
            migrations_root_dir: test_migration_dir(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
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

    // TODO: more tests
    // - add test migrations
    // - tamper with one and test
    // - add new migration and test
    // - reorder migrations

    //////////////////////// SqliteSysDb ////////////////////////

    #[tokio::test]
    async fn test_sqlite_sysdb() {
        let conn = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("Expect it to be connected");
        let sysdb = SqliteSysDb::new(conn);
    }
}
