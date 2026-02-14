.PHONY: build test lint clean docker proto log_db_clean log_db_generate log_db_migration

PROTOC_BIN_PATH=$(if $(shell which protoc),$(shell which protoc),"${GOPATH}/bin/protoc")
PROTOC_GEN_GO_BIN_PATH := $(if $(shell which protoc-gen-go),$(shell which protoc-gen-go),"${GOPATH}/bin/protoc-gen-go")
PROTOC_GEN_GO_GRPC_BIN_PATH := $(if $(shell which protoc-gen-go-grpc),$(shell which protoc-gen-go-grpc),"${GOPATH}/bin/protoc-gen-go-grpc")

proto:
	@echo "Generating gRPC code for Golang..."
	@mkdir -p pkg/proto/coordinatorpb pkg/proto/logservicepb
	@$(PROTOC_BIN_PATH) \
		-I../idl/ \
		--go_out=pkg/proto/coordinatorpb \
		--go_opt paths=source_relative \
		--plugin protoc-gen-go="$(PROTOC_GEN_GO_BIN_PATH)" \
		--go-grpc_out=pkg/proto/coordinatorpb \
		--go-grpc_opt paths=source_relative \
		--plugin protoc-gen-go-grpc="$(PROTOC_GEN_GO_GRPC_BIN_PATH)" \
		../idl/chromadb/proto/chroma.proto \
		../idl/chromadb/proto/coordinator.proto \
		../idl/chromadb/proto/logservice.proto \
		../idl/chromadb/proto/heapservice.proto
	@mv pkg/proto/coordinatorpb/chromadb/proto/logservice*.go pkg/proto/logservicepb/
	@mv pkg/proto/coordinatorpb/chromadb/proto/*.go pkg/proto/coordinatorpb/
	@rm -rf pkg/proto/coordinatorpb/chromadb
	@echo "Done"

build: proto
	go build -v -o bin/coordinator ./cmd/coordinator/

# -p 1 runs tests sequentially. If run in parallel tests are pretty flaky in CI.
# I believe this is caused by go/script/migrate_up_test.sh, which creates the possibility (observed during CI runs) of running multiple `atlas schema apply --dev-url "docker://postgres/15/dev" ...` in parallel.
# This command spins up a new Docker container, so my theory is that there's a race condition inside atlas when used this way.
# Instead of specifying -p 1, it might be possible to add a cross-process lock to guarantee that only one instance of atlas is running at a time.
test: build
	go test -p 1 -race -cover ./...


lint:
	#brew install golangci-lint
	golangci-lint run

clean:
	rm -f bin/chroma
	rm -rf pkg/proto/coordinatorpb pkg/proto/logservicepb

docker:
	docker build -t chroma-coordinator:latest .

### LOG SERVICE
DATABABASE_LOG_DIR := pkg/log/store

log_db_clean:
	rm -rf $(DATABABASE_LOG_DIR)/db

.PHONY: quota_db_generate
log_db_generate: log_db_clean
	sqlc generate -f $(DATABABASE_LOG_DIR)/sqlc.yaml

log_db_migration:
	 atlas migrate diff initial --to file://$(DATABABASE_LOG_DIR)/schema --dev-url "docker://postgres/15/dev"  --format '{{ sql . "  " }}' --dir file://$(DATABABASE_LOG_DIR)/migrations
