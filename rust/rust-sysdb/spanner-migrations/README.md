# Spanner Migrations

Schema migrations for Spanner database.

## Adding a New Migration

1. Create a new SQL file in `migrations/`:
   ```
   {version}-{description}.spanner.sql
   ```
   Example: `0002-create_databases.spanner.sql`

   NOTE: THE MIGRATION FILE MUST HAVE ONLY ONE DDL STATEMENT AND MUST BE IDEMPOTENT.

2. Write your Spanner DDL in the file (one statement per file).

3. Regenerate the manifest (from `rust/rust-sysdb/spanner-migrations/` directory):
   ```bash
   cd rust/rust-sysdb/spanner-migrations
   cargo run --bin spanner_migration -- --generate-sum
   ```
   This writes directly to `migrations/migrations.sum`.

4. Commit both the new migration file and updated `migrations.sum`.

## Manifest Validation

The `migrations.sum` file protects against:
- Forgotten migration files during git push
- Accidental modifications to existing migrations
- Merge conflicts (one line per migration)

If validation fails, regenerate the manifest and commit both the migration and updated `migrations.sum`.

## Running Migrations

```bash
# Apply migrations (default mode)
cargo run --bin spanner_migration

# Validate migrations are applied
cargo run --bin spanner_migration  # with migration_mode: validate in config
```

## File Format

- **Filename**: `{version}-{description}.spanner.sql`
  - `version`: Zero-padded number (e.g., `0001`, `0002`)
  - `description`: snake_case description
  - Extension: `.spanner.sql`

- **Manifest**: `migrations.sum`
  - Format: `{filename} {sha256_hash}`
  - Comments start with `#`
