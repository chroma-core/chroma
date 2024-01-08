.PHONY: build
build:
	go build -v -o bin/chroma ./cmd

test: build
	go test -cover -race ./...

lint:
	#brew install golangci-lint
	golangci-lint run

clean:
	rm -f bin/chroma

docker:
	docker build -t chroma-coordinator:latest .
