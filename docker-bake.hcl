target "log-service" {
  dockerfile = "go/Dockerfile"
  target = "logservice"
  tags = [ "log-service:ci" ]
}

target "log-service-migration" {
  dockerfile = "go/Dockerfile.migration"
  target = "logservice-migration"
}

target "rust-log-service" {
  dockerfile = "rust/Dockerfile"
  target = "log_service"
}

target "sysdb" {
  dockerfile = "go/Dockerfile"
  target = "sysdb"
}

target "sysdb-migration" {
  dockerfile = "go/Dockerfile.migration"
  target = "sysdb-migration"
}

target "rust-frontend-service" {
  dockerfile = "rust/Dockerfile"
  target = "cli"
}

target "query-service" {
  dockerfile = "rust/Dockerfile"
  target = "query_service"
}

target "compactor-service" {
  dockerfile = "rust/Dockerfile"
  target = "compaction_service"
}

target "garbage-collector" {
  dockerfile = "rust/Dockerfile"
  target = "garbage_collector"
}

target "load-service" {
  dockerfile = "rust/Dockerfile"
  target = "load_service"
}


group "default" {
  targets = [
    "log-service",
    "log-service-migration",
    "rust-log-service",
    "sysdb",
    "sysdb-migration",
    "rust-frontend-service",
    "query-service",
    "compactor-service",
    "garbage-collector",
    "load-service"
  ]
}
