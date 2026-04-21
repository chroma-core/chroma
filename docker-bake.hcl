# Bake targets for Chroma service images published by this repo.
# Context is the repo root; each target maps to a stage in rust/Dockerfile,
# go/Dockerfile, or go/Dockerfile.migration.
variable "LOCAL_BUILD" {}
variable "REGISTRY_AWS" {}
variable "REGISTRY_GCP" {}
variable "REGISTRY_DOCKERHUB" {}
variable "COMMIT_SHORT_SHA" {}
variable "ADDRESS_SANITIZER" {}
variable "ENABLE_AVX512" {}

target "compactor-service" {
  context    = "."
  dockerfile = "rust/Dockerfile"
  args = {
    "RELEASE_MODE"      = "1"
    "ENABLE_AVX512"     = "${ENABLE_AVX512}"
    "ADDRESS_SANITIZER" = "${ADDRESS_SANITIZER}"
  }
  target = "compaction_service"
  tags = LOCAL_BUILD == "true" ? ["compactor-service:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/compactor-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/compactor-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/compactor-service:${COMMIT_SHORT_SHA}",
  ]
}

target "rust-frontend-service-oss" {
  context    = "."
  dockerfile = "rust/Dockerfile"
  args = {
    "RELEASE_MODE" = "1"
  }
  target = "cli"
  tags = LOCAL_BUILD == "true" ? ["rust-frontend-service-oss:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/rust-frontend-service-oss:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/rust-frontend-service-oss:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/rust-frontend-service-oss:${COMMIT_SHORT_SHA}",
  ]
}

target "rust-log-service" {
  context    = "."
  dockerfile = "rust/Dockerfile"
  args = {
    "RELEASE_MODE"      = "1"
    "ADDRESS_SANITIZER" = "${ADDRESS_SANITIZER}"
  }
  target = "log_service"
  tags = LOCAL_BUILD == "true" ? ["rust-log-service:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/rust-log-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/rust-log-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/rust-log-service:${COMMIT_SHORT_SHA}",
  ]
}

target "heap-tender-service" {
  context    = "."
  dockerfile = "rust/Dockerfile"
  args = {
    "RELEASE_MODE"      = "1"
    "ADDRESS_SANITIZER" = "${ADDRESS_SANITIZER}"
  }
  target = "heap_tender_service"
  tags = LOCAL_BUILD == "true" ? ["heap-tender-service:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/heap-tender-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/heap-tender-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/heap-tender-service:${COMMIT_SHORT_SHA}",
  ]
}

target "query-service" {
  context    = "."
  dockerfile = "rust/Dockerfile"
  args = {
    "RELEASE_MODE"      = "1"
    "ENABLE_AVX512"     = "${ENABLE_AVX512}"
    "ADDRESS_SANITIZER" = "${ADDRESS_SANITIZER}"
  }
  target = "query_service"
  tags = LOCAL_BUILD == "true" ? ["query-service:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/query-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/query-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/query-service:${COMMIT_SHORT_SHA}",
  ]
}

target "garbage-collector" {
  context    = "."
  dockerfile = "rust/Dockerfile"
  args = {
    "RELEASE_MODE"      = "1"
    "ADDRESS_SANITIZER" = "${ADDRESS_SANITIZER}"
  }
  target = "garbage_collector"
  tags = LOCAL_BUILD == "true" ? ["garbage-collector-service:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/garbage-collector-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/garbage-collector-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/garbage-collector-service:${COMMIT_SHORT_SHA}",
  ]
}

target "sysdb-migration" {
  context    = "."
  dockerfile = "go/Dockerfile.migration"
  target     = "sysdb-migration"
  tags = LOCAL_BUILD == "true" ? ["sysdb-migration:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/sysdb-migration:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/sysdb-migration:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/sysdb-migration:${COMMIT_SHORT_SHA}",
  ]
}

target "sysdb-service" {
  context    = "."
  dockerfile = "go/Dockerfile"
  target     = "sysdb"
  tags = LOCAL_BUILD == "true" ? ["sysdb-service:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/sysdb-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/sysdb-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/sysdb-service:${COMMIT_SHORT_SHA}",
  ]
}

target "rust-sysdb-migration" {
  context    = "."
  dockerfile = "rust/Dockerfile"
  args = {
    "RELEASE_MODE"      = "1"
    "ADDRESS_SANITIZER" = "${ADDRESS_SANITIZER}"
  }
  target = "rust-sysdb-migration"
  tags = LOCAL_BUILD == "true" ? ["rust-sysdb-migration:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/rust-sysdb-migration:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/rust-sysdb-migration:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/rust-sysdb-migration:${COMMIT_SHORT_SHA}",
  ]
}

target "rust-sysdb-service" {
  context    = "."
  dockerfile = "rust/Dockerfile"
  args = {
    "RELEASE_MODE"      = "1"
    "ADDRESS_SANITIZER" = "${ADDRESS_SANITIZER}"
  }
  target = "sysdb_service"
  tags = LOCAL_BUILD == "true" ? ["rust-sysdb-service:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/rust-sysdb-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/rust-sysdb-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/rust-sysdb-service:${COMMIT_SHORT_SHA}",
  ]
}

target "load-service" {
  context    = "."
  dockerfile = "rust/Dockerfile"
  args = {
    "RELEASE_MODE"      = "1"
    "ADDRESS_SANITIZER" = "${ADDRESS_SANITIZER}"
  }
  target = "load_service"
  tags = LOCAL_BUILD == "true" ? ["load-service:${COMMIT_SHORT_SHA}"] : [
    "${REGISTRY_AWS}/load-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_GCP}/load-service:${COMMIT_SHORT_SHA}",
    "${REGISTRY_DOCKERHUB}/load-service:${COMMIT_SHORT_SHA}",
  ]
}

group "default" {
  targets = [
    "compactor-service",
    "rust-frontend-service-oss",
    "rust-log-service",
    "heap-tender-service",
    "query-service",
    "garbage-collector",
    "sysdb-migration",
    "sysdb-service",
    "rust-sysdb-migration",
    "rust-sysdb-service",
    "load-service",
  ]
}
