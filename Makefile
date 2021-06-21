VERSION = $(shell git describe --tags --always --dirty)

DOCKER_REPO= stickyricky/brimming
DOCKER_IMAGE_NAME = $(DOCKER_REPO):$(VERSION)
DOCKER_LATEST_IMAGE_NAME = $(DOCKER_REPO):latest

all: fmt vet build push

build:
	go build -o brimming

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: release
release: build
ifndef GITHUB_TOKEN
	@echo GITHUB_TOKEN is not set
else
	@goreleaser --rm-dist
endif


.PHONY: build-image
build-image:
	docker build . -t $(DOCKER_IMAGE_NAME)

.PHONY: push
push: build-image
	docker image tag $(DOCKER_IMAGE_NAME) $(DOCKER_IMAGE_NAME)
	docker image tag $(DOCKER_IMAGE_NAME) $(DOCKER_LATEST_IMAGE_NAME)
	docker push $(DOCKER_IMAGE_NAME)
	docker push $(DOCKER_LATEST_IMAGE_NAME)