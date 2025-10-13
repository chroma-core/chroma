target "rust-log-service" {
  dockerfile = "rust/Dockerfile"
  target = "log_service"
  tags = [ "rust-log-service:ci" ]
}

target "heap-tender-service" {
  dockerfile = "rust/Dockerfile"
  target = "heap_tender_service"
  tags = [ "heap-tender-service:ci" ]
}

target "sysdb" {
  dockerfile = "go/Dockerfile"
  target = "sysdb"
  tags = [ "sysdb:ci" ]
}

target "sysdb-migration" {
  dockerfile = "go/Dockerfile.migration"
  target = "sysdb-migration"
  tags = [ "sysdb-migration:ci" ]
}

target "rust-frontend-service" {
  dockerfile = "rust/Dockerfile"
  target = "cli"
  tags = [ "rust-frontend-service:ci" ]
}

target "query-service" {
  dockerfile = "rust/Dockerfile"
  target = "query_service"
  tags = [ "query-service:ci" ]
}

target "compactor-service" {
  dockerfile = "rust/Dockerfile"
  target = "compaction_service"
  tags = [ "compactor-service:ci" ]
}

target "garbage-collector" {
  dockerfile = "rust/Dockerfile"
  target = "garbage_collector"
  tags = [ "garbage-collector:ci" ]
}

target "load-service" {
  dockerfile = "rust/Dockerfile"
  target = "load_service"
  tags = [ "load-service:ci" ]
}


group "default" {
  targets = [
    "rust-log-service",
    "heap-tender-service",
    "sysdb",
    "sysdb-migration",
    "rust-frontend-service",
    "query-service",
    "compactor-service",
    "garbage-collector",
    "load-service"
  ]
}
