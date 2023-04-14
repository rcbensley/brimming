VERSION = $(shell git describe --tags --always --dirty)
IMAGE_NAME = brimming:$(VERSION)
BIN_NAME := brimming
ALL_ARCH := amd64 arm64
ALL_OS := linux darwin

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

.PHONY: image
image:
	podman build . -t $(IMAGE_NAME)

.PHONY: release
release:
	@gh release delete $(VERSION) -y
	@gh release create $(VERSION) --generate-notes
	@rm -Rf dist
	@mkdir dist
	@for o in $(ALL_OS); do \
		for a in $(ALL_ARCH); do \
			package_file="$(BIN_NAME)-$${o}-$${a}.tar.gz"; \
			echo "Creating $${package_file}"; \
			rm -f $${package_file} \
			&& CGO_ENABLED=0 GOOS=$${o} GOARCH=$${a} go build --ldflags '-extldflags "-static"' -o build/$(BIN_NAME) brimming.go \
				&& tar -czf dist/$${package_file} $(BIN_NAME) \
				&& rm -f $(BIN_NAME) \
				&& gh release upload $(VERSION) dist/$${package_file} --clobber; \
		done \
	done
