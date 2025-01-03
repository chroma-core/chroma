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
		../idl/chromadb/proto/logservice.proto
	@mv pkg/proto/coordinatorpb/chromadb/proto/logservice*.go pkg/proto/logservicepb/
	@mv pkg/proto/coordinatorpb/chromadb/proto/*.go pkg/proto/coordinatorpb/
	@rm -rf pkg/proto/coordinatorpb/chromadb
	@echo "Done"

build: proto
	go build -v -o bin/coordinator ./cmd/coordinator/
	go build -v -o bin/logservice ./cmd/logservice/

test: build
	go test -race -cover ./...


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
