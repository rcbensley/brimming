VERSION = $(shell git describe --tags --always --dirty)

IMAGE_NAME = brimming:$(VERSION)

all: fmt vet test build image

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test ./...

build:
	@CGO_ENABLED=0 go build -ldflags "-s -X main.Version=${VERSION}" -o brimming

.PHONY: release
release: build
ifndef GITHUB_TOKEN
	@echo GITHUB_TOKEN is not set
else
	@goreleaser --rm-dist
endif


.PHONY: image
image:
	podman build . -t $(IMAGE_NAME)
