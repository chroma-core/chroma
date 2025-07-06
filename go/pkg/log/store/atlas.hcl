env "prod" {
  url = getenv("CHROMA_DB_LOG_URL")
  migration {
    dir = "file://migrations"
  }
}
