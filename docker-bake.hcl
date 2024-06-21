group "default" {
  targets = [
    "logservice",
    "logservice-migration",
    "sysdb",
    "frontend-service",
    "query-service",
    "compaction-service",
  ]
}

target "logservice" {
  dockerfile = "go/Dockerfile"
  target = "logservice"
}

target "logservice-migration" {
  dockerfile = "go/Dockerfile.migration"
  target = "logservice-migration"
}

target "sysdb" {
  dockerfile = "go/Dockerfile"
  target = "sysdb"
}

target "frontend-service" {
  dockerfile = "Dockerfile"
}

target "query-service" {
  dockerfile = "rust/worker/Dockerfile"
  target = "query_service"
}

target "compaction-service" {
  dockerfile = "rust/worker/Dockerfile"
  target = "compaction_service"
}
