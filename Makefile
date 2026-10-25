MODULES := . natsq

.PHONY: test lint tidy

test:
	@for m in $(MODULES); do (cd $$m && go test -race -count=1 ./...) || exit 1; done

lint:
	@for m in $(MODULES); do (cd $$m && go vet ./... && golangci-lint run ./...) || exit 1; done

tidy:
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done
