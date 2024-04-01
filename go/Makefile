.PHONY: build
build:
	go build -v -o bin/coordinator ./cmd/coordinator/
	go build -v -o bin/logservice ./cmd/logservice/

test: build
	go test -race -cover ./...


lint:
	#brew install golangci-lint
	golangci-lint run

clean:
	rm -f bin/chroma

docker:
	docker build -t chroma-coordinator:latest .


DATABABASE_LOG_DIR := database/log

.PHONY: sqlc-install
sqlc-install:
	@which sqlc > /dev/null || go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

log_db_clean:
	rm -rf $(DATABABASE_LOG_DIR)/db

.PHONY: quota_db_generate
log_db_generate: sqlc-install log_db_clean
	sqlc generate -f $(DATABABASE_LOG_DIR)/sqlc.yaml

log_db_migration:
	 atlas migrate diff initial --to file://$(DATABABASE_LOG_DIR)/schema --dev-url "docker://postgres/15/dev"  --format '{{ sql . "  " }}' --dir file://$(DATABABASE_LOG_DIR)/migrations