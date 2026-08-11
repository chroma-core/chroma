// All rust/Dockerfile targets must pass identical args so they share a
// single builder stage in the buildkit cache. A differing arg (even one
// that only affects the log_service binary) forks the builder into two
// cache keys and compiles the entire workspace twice.
//
// NOTE: action.yaml discovers targets by grepping '^target' in this file,
// so a shared inheritance block would itself get built — hence the args
// are repeated on each target instead.

target "rust-log-service" {
  dockerfile = "rust/Dockerfile"
  target = "log_service"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "rust-log-service:ci" ]
}

target "rust-sysdb-service" {
  dockerfile = "rust/Dockerfile"
  target = "sysdb_service"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "rust-sysdb-service:ci" ]
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

target "rust-sysdb-migration" {
  dockerfile = "rust/Dockerfile"
  target = "rust-sysdb-migration"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "rust-sysdb-migration:ci" ]
}

target "rust-frontend-service" {
  dockerfile = "rust/Dockerfile"
  target = "cli"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "rust-frontend-service:ci" ]
}

target "query-service" {
  dockerfile = "rust/Dockerfile"
  target = "query_service"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "query-service:ci" ]
}

target "compactor-service" {
  dockerfile = "rust/Dockerfile"
  target = "compaction_service"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "compactor-service:ci" ]
}

target "garbage-collector" {
  dockerfile = "rust/Dockerfile"
  target = "garbage_collector"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "garbage-collector:ci" ]
}

target "load-service" {
  dockerfile = "rust/Dockerfile"
  target = "load_service"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "load-service:ci" ]
}

target "work-queue-service" {
  dockerfile = "rust/Dockerfile"
  target = "work_queue_service"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "work-queue-service:ci" ]
}

target "fn-consumer" {
  dockerfile = "rust/Dockerfile"
  target = "fn_consumer"
  args = {
    LOG_SERVICE_CARGO_FEATURES = "faults"
  }
  tags = [ "fn-consumer:ci" ]
}

group "default" {
  targets = [
    "rust-log-service",
    "rust-sysdb-service",
    "sysdb",
    "sysdb-migration",
    "rust-sysdb-migration",
    "rust-frontend-service",
    "query-service",
    "compactor-service",
    "garbage-collector",
    "load-service",
    "work-queue-service",
    "fn-consumer"
  ]
}
