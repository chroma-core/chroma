.PHONY: build
build:
	go build -v -o bin/coordinator ./cmd/coordinator/
	go build -v -o bin/logservice ./cmd/logservice/

test: build
	go test -async -cover ./...

lint:
	#brew install golangci-lint
	golangci-lint run

clean:
	rm -f bin/chroma

docker:
	docker build -t chroma-coordinator:latest .
