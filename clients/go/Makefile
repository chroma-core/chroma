
build:
	go build -v ./...

.PHONY: gotestsum-bin
gotestsum-bin:
	go install gotest.tools/gotestsum@latest

.PHONY: test
test: gotestsum-bin
	gotestsum \
        --format short-verbose \
        --rerun-fails=1 \
        --packages="./..." \
        --junitfile unit.xml \
        -- \
        -v \
        -coverprofile=coverage.out \
        -timeout=30m

.PHONY: test-cloud
test-cloud: gotestsum-bin
	gotestsum \
        --format short-verbose \
        --rerun-fails=1 \
        --packages="./..." \
        --junitfile unit-cloud.xml \
        -- \
        -p=1 \
        -v \
        -tags=cloud \
        -coverprofile=coverage-cloud.out \
        -timeout=30m

.PHONY: test-ef
test-ef: gotestsum-bin
	gotestsum \
		--format short-verbose \
		--rerun-fails=1 \
		--packages="./..." \
		--junitfile unit-ef.xml \
		-- \
		-v \
		-tags=ef \
		-coverprofile=coverage-ef.out \
		-timeout=30m

.PHONY: test-ef-cloud
test-ef-cloud: gotestsum-bin
	gotestsum \
		--format short-verbose \
		--rerun-fails=1 \
		--packages="./..." \
		--junitfile unit-ef-cloud.xml \
		-- \
		-v \
		-tags=ef,cloud \
		-coverprofile=coverage-ef-cloud.out \
		-timeout=30m

.PHONY: test-crosslang
test-crosslang: gotestsum-bin setup-python-venv
	gotestsum \
		--format short-verbose \
		--rerun-fails=1 \
		--packages="./..." \
		--junitfile unit-crosslang.xml \
		-- \
		-v \
		-tags=crosslang \
		-coverprofile=coverage-crosslang.out \
		-timeout=30m

.PHONY: setup-python-venv
setup-python-venv:
	@if [ ! -d ".venv" ]; then \
		python3 -m venv .venv; \
	fi
	.venv/bin/pip install -r requirements-test.txt

.PHONY: lint
lint:
	golangci-lint run

.PHONY: lint-fix
lint-fix:
	golangci-lint run --fix ./...

.PHONY: clean-lint-cache
clean-lint-cache:
	golangci-lint cache clean


.PHONY: server
server:
	sh ./scripts/chroma_server.sh
