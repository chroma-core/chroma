# Set up Local Postgres

- Tilt up for postgres
    - `tilt up`
- Set postgres ENV Vars
    Several tests (such as record_log_service_test.go) require the following environment variables to be set:
    - `export POSTGRES_HOST=localhost`
    - `export POSTGRES_PORT=5432`
- Atlas schema migration
    - Generate a migration after making changes to gorm [~/chroma/go]: `atlas migrate diff --env dev --dev-url "docker://postgres/15/dev?search_path=public"`
    - If you need to manually apply schema changes [~/chroma/go]: `atlas --env dev migrate apply --url "postgres://chroma:chroma@localhost:5432/chroma?sslmode=disable"`
