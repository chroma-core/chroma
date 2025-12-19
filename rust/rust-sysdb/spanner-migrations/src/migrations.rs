//! Migration types and embedded migration files for Spanner.

use core::str;
use regex::Regex;
use rust_embed::Embed;
use sha2::{Digest, Sha256};
use std::{borrow::Cow, sync::LazyLock};
use thiserror::Error;

///////////// Migration Types //////////////

#[derive(Clone, Debug)]
pub struct Migration {
    pub dir: String,
    pub filename: String,
    pub version: i32,
    pub sql: String,
    pub hash: String,
}

impl Migration {
    pub fn new(dir: String, filename: String, version: i32, sql: String, hash: String) -> Self {
        Self {
            dir,
            filename,
            version,
            sql,
            hash,
        }
    }
}

pub enum MigrationDir {
    SpannerSysDb,
}

pub const MIGRATION_DIRS: [MigrationDir; 1] = [MigrationDir::SpannerSysDb];

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
    pub fn as_str(&self) -> &str {
        match self {
            Self::SpannerSysDb => "spanner_sysdb",
        }
    }

    fn parse_migration_filename(
        filename: &str,
    ) -> Result<(i32, String), ParseMigrationFilenameError> {
        let regex_match = MIGRATION_FILENAME_REGEX.captures(filename);
        let groups = match regex_match {
            Some(groups) => groups,
            None => {
                return Err(ParseMigrationFilenameError::InvalidMigrationFilename(
                    filename.to_string(),
                ))
            }
        };

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

        let scope = match groups.get(3) {
            Some(scope) => scope.as_str().to_string(),
            None => return Err(ParseMigrationFilenameError::FailedToFindVersion),
        };

        Ok((version, scope))
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Cow<'static, str>>> {
        match self {
            Self::SpannerSysDb => Box::new(SpannerSysDbMigrationsFolder::iter()),
        }
    }

    fn get_file(&self, name: &str) -> Option<rust_embed::EmbeddedFile> {
        match self {
            Self::SpannerSysDb => SpannerSysDbMigrationsFolder::get(name),
        }
    }

    pub fn get_source_migrations(&self) -> Result<Vec<Migration>, GetSourceMigrationsError> {
        let mut migrations = Vec::new();

        for migration_name in self.iter() {
            let (version, _) = Self::parse_migration_filename(&migration_name)
                .map_err(GetSourceMigrationsError::ParseMigrationFilenameError)?;
            let sql = match self.get_file(&migration_name) {
                Some(sql) => str::from_utf8(&sql.data)
                    .map_err(|_| {
                        GetSourceMigrationsError::FailedToGetMigrationFile(
                            migration_name.to_string(),
                        )
                    })?
                    // Remove CR character on Windows, copied from rust/sqlite/src/migrations.rs
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
            let mut hasher = Sha256::new();
            hasher.update(sql.as_bytes());
            let hash = format!("{:x}", hasher.finalize());
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
}

/// Regex for parsing migration filenames.
/// Format: `{version}-{description}.spanner.sql`
/// Examples:
///   - `0001-create_tenants.spanner.sql` -> version=1, description="create_tenants", scope="spanner"
///   - `0002-add_users_table.spanner.sql` -> version=2, description="add_users_table", scope="spanner"
static MIGRATION_FILENAME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\d+)-(.+)\.(spanner)\.sql").expect("Failed to compile regex"));

///////////// Rust Embed Migrations Data //////////////

#[derive(Embed)]
#[folder = "migrations/"]
#[include = "*.sql"]
struct SpannerSysDbMigrationsFolder;
