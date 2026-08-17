.PHONY: test build demo
test:
	CGO_ENABLED=0 go test ./...
build:
	CGO_ENABLED=0 go build -o bin/media-transcoder-pool ./cmd/module
demo:
	./scripts/local-single-worker-demo.sh
