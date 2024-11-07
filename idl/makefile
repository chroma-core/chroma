.PHONY: proto

PROTOC_GEN_GO_BIN_PATH := $(if $(shell which protoc-gen-go),$(shell which protoc-gen-go),"${GOPATH}/bin/protoc-gen-go")
PROTOC_GEN_GO_GRPC_BIN_PATH := $(if $(shell which protoc-gen-go-grpc),$(shell which protoc-gen-go-grpc),"${GOPATH}/bin/protoc-gen-go-grpc")

proto_python:
	@echo "Generating gRPC code for python..."
	@python -m grpc_tools.protoc -I ./ --python_out=. --pyi_out=. --grpc_python_out=. \
	    ./chromadb/proto/chroma.proto \
	    ./chromadb/proto/coordinator.proto \
	    ./chromadb/proto/logservice.proto \
	    ./chromadb/proto/query_executor.proto
	@mv chromadb/proto/*.py ../chromadb/proto/
	@mv chromadb/proto/*.pyi ../chromadb/proto/
	@echo "Done"

proto_go:
	@echo "Generating gRPC code for golang..."
	@protoc \
		--go_out=../go/pkg/proto/coordinatorpb \
		--go_opt paths=source_relative \
		--plugin protoc-gen-go="${PROTOC_GEN_GO_BIN_PATH}" \
    	--go-grpc_out=../go/pkg/proto/coordinatorpb \
    	--go-grpc_opt paths=source_relative \
    	--plugin protoc-gen-go-grpc="${PROTOC_GEN_GO_GRPC_BIN_PATH}" \
			chromadb/proto/chroma.proto \
			chromadb/proto/coordinator.proto \
			chromadb/proto/logservice.proto
	@mv ../go/pkg/proto/coordinatorpb/chromadb/proto/logservice*.go ../go/pkg/proto/logservicepb/
	@mv ../go/pkg/proto/coordinatorpb/chromadb/proto/*.go ../go/pkg/proto/coordinatorpb/
	@rm -rf ../go/pkg/proto/coordinatorpb/chromadb
	@echo "Done"
