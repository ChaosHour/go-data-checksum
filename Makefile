.PHONY: all build clean test lint help

# Variables
APP_NAME := go-data-checksum
BUILD_DIR := bin
CMD_DIR := cmd/checksum
MAIN_FILE := $(CMD_DIR)/main.go
GO_FILES := $(shell find . -name "*.go" -type f -not -path "./vendor/*")

VERSION := $(shell cat RELEASE_VERSION 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.AppVersion=$(VERSION)"
BUILD_FLAGS := -trimpath $(LDFLAGS)

# Go commands
GO_BUILD := go build $(BUILD_FLAGS)
GO_TEST := go test
GO_LINT := golangci-lint run
GO_CLEAN := go clean
GO_MOD := go mod

# Default target
all: build

# Create build directory if it doesn't exist
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Build the binary
build: $(BUILD_DIR)
	$(GO_BUILD) -o $(BUILD_DIR)/$(APP_NAME) $(MAIN_FILE)
	@echo "Binary built at $(BUILD_DIR)/$(APP_NAME)"

# Run tests
test:
	$(GO_TEST) ./...

# Run linter
lint:
	$(GO_LINT) ./...

# Clean build artifacts
clean:
	rm -rf $(BUILD_DIR)
	$(GO_CLEAN)

# Ensure dependencies are up to date
deps:
	$(GO_MOD) tidy
	$(GO_MOD) verify

# Show help
help:
	@echo "Available targets:"
	@echo "  all:    Build the application (default)"
	@echo "  build:  Build the application"
	@echo "  test:   Run tests"
	@echo "  lint:   Run linter"
	@echo "  clean:  Clean build artifacts"
	@echo "  deps:   Ensure dependencies are up to date"
	@echo "  help:   Show this help message"
