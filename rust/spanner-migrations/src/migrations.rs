//! Migration types and embedded migration files for Spanner.

use core::str;
use regex::Regex;
use rust_embed::Embed;
use sha2::{Digest, Sha256};
use std::{borrow::Cow, collections::HashMap, sync::LazyLock};
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
    #[error("Migration manifest validation failed: {0}")]
    ManifestValidationError(String),
}

impl MigrationDir {
    /// Returns SQL migration file names only
    fn sql_migration_files(&self) -> Vec<String> {
        self.iter()
            .filter(|name| name.ends_with(".sql"))
            .map(|name| name.to_string())
            .collect()
    }

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

    /// Load the migrations.sum manifest and return a map of filename -> expected hash
    fn load_manifest(&self) -> Result<HashMap<String, String>, GetSourceMigrationsError> {
        let manifest_file = self.get_file("migrations.sum").ok_or_else(|| {
            GetSourceMigrationsError::ManifestValidationError(
                "migrations.sum file not found - run: cargo run --bin spanner_migration -- --generate-sum".to_string(),
            )
        })?;

        let manifest_content = str::from_utf8(&manifest_file.data).map_err(|_| {
            GetSourceMigrationsError::ManifestValidationError(
                "Failed to parse migrations.sum as UTF-8".to_string(),
            )
        })?;

        let mut manifest = HashMap::new();
        for line in manifest_content.lines() {
            let line = line.trim();
            // Skip comments and empty lines
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 2 {
                return Err(GetSourceMigrationsError::ManifestValidationError(format!(
                    "Invalid manifest line: {}",
                    line
                )));
            }
            manifest.insert(parts[0].to_string(), parts[1].to_string());
        }
        Ok(manifest)
    }

    /// Validate migrations against the manifest
    fn validate_manifest(
        &self,
        migrations: &[Migration],
        manifest: &HashMap<String, String>,
    ) -> Result<(), GetSourceMigrationsError> {
        // Check that all manifest entries have corresponding migration files
        for (filename, expected_hash) in manifest {
            let migration = migrations.iter().find(|m| m.filename == *filename);
            match migration {
                Some(m) => {
                    if m.hash != *expected_hash {
                        return Err(GetSourceMigrationsError::ManifestValidationError(format!(
                            "Hash mismatch for {}: manifest={}, actual={}, you might have to regenerate migrations.sum with: cargo run --bin spanner_migration -- --generate-sum",
                            filename, expected_hash, m.hash
                        )));
                    }
                }
                None => {
                    return Err(GetSourceMigrationsError::ManifestValidationError(format!(
                        "Migration file {} listed in manifest but not found, you might have to regenerate migrations.sum with: cargo run --bin spanner_migration -- --generate-sum",
                        filename
                    )));
                }
            }
        }

        // Check that all migration files are listed in manifest
        for migration in migrations {
            if !manifest.contains_key(&migration.filename) {
                return Err(GetSourceMigrationsError::ManifestValidationError(format!(
                    "Migration file {} not listed in migrations.sum - regenerate the manifest",
                    migration.filename
                )));
            }
        }

        Ok(())
    }

    pub fn get_source_migrations(&self) -> Result<Vec<Migration>, GetSourceMigrationsError> {
        let mut migrations = Vec::new();

        for migration_name in self.sql_migration_files() {
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

        // Validate against manifest
        let manifest = self.load_manifest()?;
        self.validate_manifest(&migrations, &manifest)?;

        Ok(migrations)
    }

    /// Generate manifest content for all migrations (for updating migrations.sum)
    pub fn generate_manifest(&self) -> String {
        let mut lines = vec![
            "# Spanner migrations manifest - DO NOT EDIT MANUALLY".to_string(),
            "# Format: {filename} {sha256_hash}".to_string(),
            "# This file protects against merge conflicts and forgotten migration files."
                .to_string(),
            "# Run `cargo run --bin spanner_migration -- --generate-sum` to regenerate."
                .to_string(),
            String::new(),
        ];

        let mut entries: Vec<(String, String)> = Vec::new();
        for migration_name in self.sql_migration_files() {
            if let Some(file) = self.get_file(&migration_name) {
                if let Ok(sql) = str::from_utf8(&file.data) {
                    let sql = sql.replace(
                        str::from_utf8(&[13]).expect("CR is valid ASCII character"),
                        "",
                    );
                    let mut hasher = Sha256::new();
                    hasher.update(sql.as_bytes());
                    let hash = format!("{:x}", hasher.finalize());
                    entries.push((migration_name.to_string(), hash));
                }
            }
        }

        // Sort by filename for consistent output
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        for (filename, hash) in entries {
            lines.push(format!("{} {}", filename, hash));
        }

        lines.join("\n")
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
#[include = "migrations.sum"]
struct SpannerSysDbMigrationsFolder;
