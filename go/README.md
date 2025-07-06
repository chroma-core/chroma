# Chroma Golang Codebase

## Set up Local Postgres

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

## Building

The biggest challenge to getting the project to build correctly is ensuring you have the correct versions for Protobuf. Refer to the "source of truth" for the version in `Dockerfile`. Note, you need all three of these:

- `protoc`
- `protoc-gen-go`
- `protoc-gen-go-grpc`

You can start by downloading the version of `protoc` from the [release page](https://github.com/protocolbuffers/protobuf/releases). Ensure that you copy the `protoc` binary to `/usr/local/bin` or add it to your `GOPATH/bin`.

ALSO, ensure you have copied the `/include` directory of the release to `../include` relative to wherever you installed the binary.

Then, to install the plugins, run the `go install` commands from the `Dockerfile`. The exact commands are not here because we would be duplicating where versions live if we did. The `Dockerfile` is the source of truth for the versions.

Once those are all installed, you can run `make build` to build the project and most importantly, the generated protobuf files which your IDE will complain about until they are generated.

## Schema Migrations

From the directory with the migrations/ and schema/ directories, you can generate a new schema by
changing the files in schema directly and running this command:

```
atlas migrate diff --dir file://migrations --to file://schema --dev-url 'docker://postgres/15/dev?search_path=public'
```
