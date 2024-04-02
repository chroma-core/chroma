
env "dev" {
  url = "postgresql://chroma:chroma@postgres.chroma.svc.cluster.local:5432/log?sslmode=disable"
  migration {
    dir = "file://migrations"
  }
}
env "prod" {
  url = getenv("DB_URL")
  migration {
    dir = "file://migrations"
  }
}
