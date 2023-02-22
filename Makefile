# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
BINARY_NAME=ptor
COMMIT=$(shell git rev-parse --short HEAD)
DATE=$(shell git log -1 --format=%ci)
Version=0.1.0

# Do not set CGO_ENABLED=0
# sqlite3 will throw error, if you set this

all: build
build: 
		$(GOBUILD) -o $(BINARY_NAME) -v -ldflags="-X 'main.Version=${Version}' -X 'main.GitCommit=${COMMIT}' -X 'main.CommitDate=${DATE}'"
test: 
		$(GOTEST) -v ./...
clean: 
		$(GOCLEAN)
		rm -f $(BINARY_NAME)
