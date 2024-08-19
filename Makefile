# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=bigtable_server
BINARY_UNIX=$(BINARY_NAME)_unix

all: test build

build:
	$(GOBUILD) -o $(BINARY_NAME) -v ./cmd/server

test:
	$(GOTEST) -v ./...

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_UNIX)

run:
	$(GOBUILD) -o $(BINARY_NAME) -v ./cmd/server
	./$(BINARY_NAME)

deps:
	$(GOGET) github.com/cockroachdb/pebble

# Cross compilation
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BINARY_UNIX) -v ./cmd/server

build-window:
	GOOS=windows GOARCH=amd64 go build -o bigtable_server.exe ./cmd/server

docker-build:
	docker build -t $(BINARY_NAME):latest .

.PHONY: all build test clean run deps build-linux docker-build