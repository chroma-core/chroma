# Set up Local Postgres

- Install Postgres on Mac
    - `brew install postgresql@14`
- Start & Stop
    - `brew services start postgresql`
    - `brew services stop postgresql`
- create testing db
    - terminal: `psql postgres`
    - postgres=# `create role chroma with login password 'chroma';`
    - postgres=# `alter role chroma with superuser;`
    - postgres=# `create database chroma;`
- Set postgres ENV Vars
    Several tests (such as record_log_service_test.go) require the following environment variables to be set:
    - `export POSTGRES_HOST=localhost`
    - `export POSTGRES_PORT=5432`
- Atlas schema migration
    - [~/chroma/go]: `atlas migrate diff --env dev`
    - [~/chroma/go]: `atlas --env dev migrate apply --url "postgres://chroma:chroma@localhost:5432/chroma?sslmode=disable"`
