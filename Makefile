.PHONY: test lint tidy

test:
	go test -race -count=1 -timeout 180s ./...

lint:
	go vet ./...
	golangci-lint run ./...

tidy:
	go mod tidy
