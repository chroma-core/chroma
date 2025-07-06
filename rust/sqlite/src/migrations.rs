use crate::config::MigrationHash;
use core::str;
use regex::Regex;
use rust_embed::Embed;
use sha2::{Digest, Sha256};
use std::{borrow::Cow, sync::LazyLock};
use thiserror::Error;

///////////// Migration Types //////////////

// A migration is a single SQL file that is executed to update the database schema
// ## Fields
// - dir: The directory where the migration file is located. One of "sysdb", "metadb", "embeddings_queue"
// - filename: The name of the migration file
// - version: The version of the migration file
// - sql: The SQL content of the migration file
// - hash: The hash of the migration file content
// ## Note
// - Due to legacy naming from the python codebase, the "log" table is known
// as "embeddings_queue" in the Rust codebase. Only in the sql files is it referred to as "embeddings_queue"
// Elsewhere in our code we should refer to it as "log"
#[derive(Clone, Debug)]
pub(crate) struct Migration {
    pub(crate) dir: String,
    pub(crate) filename: String,
    pub(crate) version: i32,
    pub(crate) sql: String,
    pub(crate) hash: String,
}

impl Migration {
    pub(crate) fn new(
        dir: String,
        filename: String,
        version: i32,
        sql: String,
        hash: String,
    ) -> Self {
        Self {
            dir,
            filename,
            version,
            sql,
            hash,
        }
    }
}

// A migration dir is a directory that contains migration files
// for a given subsystem
pub(crate) enum MigrationDir {
    SysDb,
    MetaDb,
    EmbeddingsQueue,
}

pub(crate) const MIGRATION_DIRS: [MigrationDir; 3] = [
    MigrationDir::SysDb,
    MigrationDir::MetaDb,
    MigrationDir::EmbeddingsQueue,
];

#[derive(Error, Debug)]
pub enum GetSourceMigrationsError {
    #[error(transparent)]
    ParseMigrationFilenameError(#[from] ParseMigrationFilenameError),
    #[error("{0}")]
    NoSuchMigrationFile(String),
    #[error("Failed to get migration file: {0}")]
    FailedToGetMigrationFile(String),
}

impl MigrationDir {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::SysDb => "sysdb",
            Self::MetaDb => "metadb",
            Self::EmbeddingsQueue => "embeddings_queue",
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Cow<'static, str>>> {
        match self {
            Self::SysDb => Box::new(SysDbMigrationsFolder::iter()),
            Self::MetaDb => Box::new(MetaDbMigrationsFolder::iter()),
            Self::EmbeddingsQueue => Box::new(EmbeddingsQueueMigrationsFolder::iter()),
        }
    }

    fn get_file(&self, name: &str) -> Option<rust_embed::EmbeddedFile> {
        match self {
            Self::SysDb => SysDbMigrationsFolder::get(name),
            Self::MetaDb => MetaDbMigrationsFolder::get(name),
            Self::EmbeddingsQueue => EmbeddingsQueueMigrationsFolder::get(name),
        }
    }

    /// Get the migrations that are on disk
    /// Arguments:
    /// - migration_hash: MigrationHash - The hash function to use for the migration files
    /// ## Returns:
    /// - Vec<Migration> - A list of migrations found on disk, sorted by version in ascending order
    /// ## Notes:
    /// - Uses the migrations_root_dir of this SqlDB instance
    pub(crate) fn get_source_migrations(
        &self,
        migration_hash: &MigrationHash,
    ) -> Result<Vec<Migration>, GetSourceMigrationsError> {
        let mut migrations = Vec::new();

        for migration_name in self.iter() {
            let (version, _) = parse_migration_filename(&migration_name)
                .map_err(GetSourceMigrationsError::ParseMigrationFilenameError)?;
            let sql = match self.get_file(&migration_name) {
                Some(sql) => str::from_utf8(&sql.data)
                    .map_err(|_| {
                        GetSourceMigrationsError::FailedToGetMigrationFile(
                            migration_name.to_string(),
                        )
                    })?
                    // Remove CR character on Windows
                    .replace(
                        str::from_utf8(&[13]).expect("CR is valid ASCII character"),
                        "",
                    ),
                None => {
                    return Err(GetSourceMigrationsError::NoSuchMigrationFile(
                        migration_name.to_string(),
                    ))
                }
            };
            let hash = match migration_hash {
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
            migrations.push(Migration::new(
                self.as_str().to_string(),
                migration_name.to_string(),
                version,
                sql,
                hash,
            ));
        }

        migrations.sort_by(|a, b| a.version.cmp(&b.version));
        Ok(migrations)
    }
}

///////////// MigrationDir Helpers //////////////

#[derive(Error, Debug)]
pub enum ParseMigrationFilenameError {
    #[error("Invalid migration filename: {0}")]
    InvalidMigrationFilename(String),
    #[error("Failed to find version")]
    FailedToFindVersion,
    #[error("Failed to find scope")]
    FailedToFindScope,
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
fn parse_migration_filename(filename: &str) -> Result<(i32, String), ParseMigrationFilenameError> {
    let regex_match = MIGRATION_FILENAME_REGEX.captures(filename);
    let groups = match regex_match {
        Some(groups) => groups,
        None => {
            return Err(ParseMigrationFilenameError::InvalidMigrationFilename(
                filename.to_string(),
            ))
        }
    };

    // Parse version
    let version = match groups.get(1) {
        Some(version) => version,
        None => return Err(ParseMigrationFilenameError::FailedToFindVersion),
    };
    let version = match version.as_str().parse::<i32>() {
        Ok(version) => version,
        Err(e) => {
            return Err(ParseMigrationFilenameError::InvalidMigrationFilename(
                e.to_string(),
            ))
        }
    };

    // Parse scope
    let scope = match groups.get(3) {
        Some(scope) => scope,
        None => return Err(ParseMigrationFilenameError::FailedToFindScope),
    };
    let scope = scope.as_str().to_string();

    Ok((version, scope))
}

static MIGRATION_FILENAME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\d+)-(.+)\.(.+)\.sql").expect("Failed to compile regex"));

///////////// Rust Embed Migrations Data //////////////
// The migration files are embedded in the binary using the `rust_embed` crate
// These are internal to this file and should not be used elsewhere

#[derive(Embed)]
#[folder = "./migrations/sysdb/"]
#[include = "*.sql"]
struct SysDbMigrationsFolder;

#[derive(Embed)]
#[folder = "./migrations/metadb/"]
#[include = "*.sql"]
struct MetaDbMigrationsFolder;

#[derive(Embed)]
#[folder = "./migrations/embeddings_queue/"]
#[include = "*.sql"]
struct EmbeddingsQueueMigrationsFolder;
