.PHONY: proto

proto_python:
	@echo "Generating gRPC code for python..."
	@python3 -m grpc_tools.protoc -I ./ --python_out=. --pyi_out=. --grpc_python_out=. ./chromadb/proto/*.proto
	@mv chromadb/proto/*.py ../chromadb/proto/
	@mv chromadb/proto/*.pyi ../chromadb/proto/
	@echo "Done"

proto_go:
	@echo "Generating gRPC code for golang..."
	@protoc \
		--go_out=../go/pkg/proto/coordinatorpb \
		--go_opt paths=source_relative \
		--plugin protoc-gen-go="${GOPATH}/bin/protoc-gen-go" \
    	--go-grpc_out=../go/pkg/proto/coordinatorpb \
    	--go-grpc_opt paths=source_relative \
    	--plugin protoc-gen-go-grpc="${GOPATH}/bin/protoc-gen-go-grpc" \
	    chromadb/proto/*.proto
	@mv ../go/pkg/proto/coordinatorpb/chromadb/proto/logservice*.go ../go/pkg/proto/logservicepb/
	@mv ../go/pkg/proto/coordinatorpb/chromadb/proto/*.go ../go/pkg/proto/coordinatorpb/
	@rm -rf ../go/pkg/proto/coordinatorpb/chromadb
	@echo "Done"
