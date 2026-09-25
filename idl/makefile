.PHONY: proto

# Fall back to the Go bin directory when the plugins are not on PATH. Ask the
# toolchain for it rather than reading the shell's ${GOPATH}: that variable is
# unset on most machines, which silently made the fallback "/bin/protoc-gen-go".
GO_BIN_DIR := $(shell go env GOPATH)/bin

PROTOC_GEN_GO_BIN_PATH := $(if $(shell which protoc-gen-go),$(shell which protoc-gen-go),$(GO_BIN_DIR)/protoc-gen-go)
PROTOC_GEN_GO_GRPC_BIN_PATH := $(if $(shell which protoc-gen-go-grpc),$(shell which protoc-gen-go-grpc),$(GO_BIN_DIR)/protoc-gen-go-grpc)

proto_python:
	@echo "Generating gRPC code for python..."
	@python3 -m grpc_tools.protoc -I ./ --python_out=. --pyi_out=. --grpc_python_out=. \
	    ./chromadb/proto/chroma.proto \
	    ./chromadb/proto/coordinator.proto \
	    ./chromadb/proto/logservice.proto \
	    ./chromadb/proto/heapservice.proto \
	    ./chromadb/proto/query_executor.proto
	@mv chromadb/proto/*.py ../chromadb/proto/
	@mv chromadb/proto/*.pyi ../chromadb/proto/
	@echo "Done"

proto_go:
	@echo "Generating gRPC code for golang..."
	@# `go/pkg/proto/` is gitignored in full, so on a fresh clone these
	@# directories do not exist and protoc fails before generating anything.
	@mkdir -p ../go/pkg/proto/coordinatorpb ../go/pkg/proto/logservicepb
	@protoc \
		--go_out=../go/pkg/proto/coordinatorpb \
		--go_opt paths=source_relative \
		--plugin protoc-gen-go="${PROTOC_GEN_GO_BIN_PATH}" \
    	--go-grpc_out=../go/pkg/proto/coordinatorpb \
    	--go-grpc_opt paths=source_relative \
    	--plugin protoc-gen-go-grpc="${PROTOC_GEN_GO_GRPC_BIN_PATH}" \
			chromadb/proto/chroma.proto \
			chromadb/proto/coordinator.proto \
			chromadb/proto/logservice.proto \
			chromadb/proto/heapservice.proto
	@mv ../go/pkg/proto/coordinatorpb/chromadb/proto/logservice*.go ../go/pkg/proto/logservicepb/
	@mv ../go/pkg/proto/coordinatorpb/chromadb/proto/*.go ../go/pkg/proto/coordinatorpb/
	@rm -rf ../go/pkg/proto/coordinatorpb/chromadb
	@echo "Done"
