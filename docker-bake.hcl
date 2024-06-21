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
  tags = ["local:log-service"]
}

target "logservice-migration" {
  dockerfile = "go/Dockerfile.migration"
  target = "logservice-migration"
  tags = ["local:logservice-migration"]
}

target "sysdb-migration" {
  dockerfile = "go/Dockerfile.migration"
  target = "sysdb-migration"
  tags = ["local:sysdb-migration"]
}

target "sysdb" {
  dockerfile = "go/Dockerfile"
  target = "sysdb"
  tags = ["local:sysdb"]
}

target "frontend-service" {
  dockerfile = "Dockerfile"
  tags = ["local:frontend-service"]
}

target "query-service" {
  dockerfile = "rust/worker/Dockerfile"
  target = "query_service"
  tags = ["local:query-service"]
}

target "compaction-service" {
  dockerfile = "rust/worker/Dockerfile"
  target = "compaction_service"
  tags = ["local:compaction-service"]
}
