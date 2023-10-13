.PHONY: proto

proto:
	@echo "Generating gRPC code..."
	@python -m grpc_tools.protoc -I ./ --python_out=. --pyi_out=. --grpc_python_out=. ./chromadb/proto/*.proto
	@mv chromadb/proto/*.py ../chromadb/proto/
	@mv chromadb/proto/*.pyi ../chromadb/proto/
	@echo "Done"
