//! Migration types and embedded migration files for Spanner.

use core::str;
use regex::Regex;
use rust_embed::Embed;
use sha2::{Digest, Sha256};
use std::{borrow::Cow, collections::HashMap, sync::LazyLock};
use thiserror::Error;

///////////// Migration Types //////////////

#[derive(Clone, Debug)]
pub struct Manifest {
    pub migrations: HashMap<String, String>,
}

impl Manifest {
    pub fn new(migrations: HashMap<String, String>) -> Self {
        Self { migrations }
    }
}

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

/// Validates that a string is a valid SHA256 hash (64 hex characters)
fn validate_sha256_hash(hash: &str, context: &str) -> Result<(), GetSourceMigrationsError> {
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(GetSourceMigrationsError::ManifestValidationError(format!(
            "Invalid SHA256 hash format for {}: {}, expected 64-character hex string",
            context, hash
        )));
    }
    Ok(())
}

fn format_sha256_hash(hash: &[u8]) -> String {
    hash.iter().map(|byte| format!("{:02x}", byte)).collect()
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
        let mut files: Vec<String> = self
            .iter()
            .filter(|name| name.ends_with(".sql"))
            .map(|name| name.to_string())
            .collect();
        files.sort();
        files
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

    /// Load the migrations.sum manifest and return a Manifest struct
    fn load_manifest(&self) -> Result<Manifest, GetSourceMigrationsError> {
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
        let lines_iter = manifest_content.lines().map(|l| l.trim());

        for line in lines_iter {
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

            validate_sha256_hash(parts[1], &format!("migration {}", parts[0]))?;
            manifest.insert(parts[0].to_string(), parts[1].to_string());
        }

        Ok(Manifest::new(manifest))
    }

    /// Validate migrations against the manifest
    fn validate_manifest(
        &self,
        migrations: &[Migration],
        manifest: &Manifest,
    ) -> Result<(), GetSourceMigrationsError> {
        // Check that all manifest entries have corresponding migration files
        for (filename, expected_running_hash) in &manifest.migrations {
            let migration = migrations.iter().find(|m| m.filename == *filename);
            match migration {
                Some(m) => {
                    if m.hash != *expected_running_hash {
                        return Err(GetSourceMigrationsError::ManifestValidationError(format!(
                            "Running hash mismatch for {}: manifest={}, actual={}, you might have to regenerate migrations.sum with: cargo run --bin spanner_migration -- --generate-sum",
                            filename, expected_running_hash, m.hash
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
            if !manifest.migrations.contains_key(&migration.filename) {
                return Err(GetSourceMigrationsError::ManifestValidationError(format!(
                    "Migration file {} not listed in migrations.sum - regenerate the manifest",
                    migration.filename
                )));
            }
        }

        Ok(())
    }

    /// Shared method to convert migration files to Migration structs with rolling hashes
    fn create_migrations_from_files(&self) -> Result<Vec<Migration>, GetSourceMigrationsError> {
        let mut migrations = Vec::new();
        let mut previous_hash: Option<Vec<u8>> = None;

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
            if let Some(prev_hash) = &previous_hash {
                hasher.update(prev_hash);
            }
            hasher.update(sql.as_bytes());
            let rolling_hash = hasher.finalize();
            let rolling_hash_hex = format_sha256_hash(&rolling_hash);
            previous_hash = Some(rolling_hash.to_vec());

            migrations.push(Migration::new(
                self.as_str().to_string(),
                migration_name.to_string(),
                version,
                sql,
                rolling_hash_hex,
            ));
        }

        Ok(migrations)
    }

    pub fn get_source_migrations(&self) -> Result<Vec<Migration>, GetSourceMigrationsError> {
        let migrations = self.create_migrations_from_files()?;

        let manifest = self.load_manifest()?;
        self.validate_manifest(&migrations, &manifest)?;

        Ok(migrations)
    }

    /// Generate manifest content for all migrations (for updating migrations.sum)
    pub fn generate_manifest(&self) -> Result<String, GetSourceMigrationsError> {
        let mut lines = vec![
            "# Spanner migrations manifest - DO NOT EDIT MANUALLY".to_string(),
            "# Format: {filename} {running_sha256_hash}".to_string(),
            "# This file protects against merge conflicts and forgotten migration files."
                .to_string(),
            "# Run `cargo run --bin spanner_migration -- --generate-sum` to regenerate."
                .to_string(),
            String::new(),
        ];

        let migrations = self.create_migrations_from_files()?;

        let rolling_hashes: Vec<(String, String)> = migrations
            .into_iter()
            .map(|m| (m.filename, m.hash))
            .collect();

        for (filename, hash) in rolling_hashes {
            lines.push(format!("{} {}", filename, hash));
        }

        Ok(lines.join("\n"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_sha256_hash_produces_64_char_hex() {
        let empty_hash = [
            0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
            0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
            0x78, 0x52, 0xb8, 0x55,
        ];

        let result = format_sha256_hash(&empty_hash);

        assert_eq!(result.len(), 64);

        assert!(result.chars().all(|c| c.is_ascii_hexdigit()));

        assert_eq!(
            result,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_format_sha256_hash_with_different_values() {
        let test_hash = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ];

        let result = format_sha256_hash(&test_hash);

        assert_eq!(result.len(), 64);

        assert!(result.chars().all(|c| c.is_ascii_hexdigit()));

        assert_eq!(
            result,
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
    }
}
