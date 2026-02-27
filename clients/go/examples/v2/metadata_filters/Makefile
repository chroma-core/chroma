.PHONY: chroma-install
chroma-install:
	@echo "Installing Chroma CLI..."
	curl -sSL https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh | bash

.PHONY: chroma-run
chroma-run:
	@echo "Running Chroma server..."
	chroma run --path ./chroma-data

.PHONY: chroma-docker
chroma-docker:
	@echo "Running Chroma server in Docker..."
	docker run -p 8000:8000 -d chromadb/chroma:latest

.PHONY: run
run: chroma-docker
	@echo "Running the example..."
	go run main.go


.PHONY: lint-fix
lint-fix:
	golangci-lint run --fix ./...