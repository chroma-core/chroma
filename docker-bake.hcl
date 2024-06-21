group "default" {
  targets = [
    "logservice",
    "logservice-migration",
    "sysdb-migration",
    "sysdb",
    "frontend-service",
    "query-service",
    "compaction-service",
  ]
}

target "logservice" {
  dockerfile = "go/Dockerfile"
  target = "logservice"
  tags = ["logservice"]
}

target "logservice-migration" {
  dockerfile = "go/Dockerfile.migration"
  target = "logservice-migration"
  tags = ["logservice-migration"]
}

target "sysdb-migration" {
  dockerfile = "go/Dockerfile.migration"
  target = "sysdb-migration"
  tags = ["sysdb-migration"]
}

target "sysdb" {
  dockerfile = "go/Dockerfile"
  target = "sysdb"
  tags = ["sysdb"]
}

target "frontend-service" {
  dockerfile = "Dockerfile"
  tags = ["frontend-service"]
}

target "query-service" {
  dockerfile = "rust/worker/Dockerfile"
  target = "query_service"
  tags = ["query-service"]
}

target "compaction-service" {
  dockerfile = "rust/worker/Dockerfile"
  target = "compaction_service"
  tags = ["compaction-service"]
}
