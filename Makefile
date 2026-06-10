.PHONY: all build build-all package package-all install uninstall start stop restart status clean test help

BINARY_NAME := taibai
VERSION ?= $(shell grep '^version =' Cargo.toml | head -1 | sed 's/.*"\([^"]*\)".*/\1/')
BUILD_TIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

DEFAULT_OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
DEFAULT_ARCH := $(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')

BUILD_DIR := build
DIST_DIR := dist
SCRIPTS_DIR := scripts

PREFIX ?= /usr/local/taibai

all: build

build:
	@echo "Building $(BINARY_NAME) for $(DEFAULT_OS)/$(DEFAULT_ARCH)..."
	@$(SCRIPTS_DIR)/build.sh build

build-all:
	@echo "Building $(BINARY_NAME) for all platforms..."
	@$(SCRIPTS_DIR)/build.sh build -all

package:
	@echo "Packaging $(BINARY_NAME) for $(DEFAULT_OS)/$(DEFAULT_ARCH)..."
	@$(SCRIPTS_DIR)/build.sh package

package-all:
	@echo "Packaging $(BINARY_NAME) for all platforms..."
	@$(SCRIPTS_DIR)/build.sh package -all

install:
	@echo "Installing $(BINARY_NAME) to $(PREFIX)..."
	@$(SCRIPTS_DIR)/install.sh -prefix=$(PREFIX)

uninstall:
	@echo "Uninstalling $(BINARY_NAME)..."
	@$(SCRIPTS_DIR)/uninstall.sh -prefix=$(PREFIX)

uninstall-purge:
	@echo "Uninstalling $(BINARY_NAME) (including data)..."
	@$(SCRIPTS_DIR)/uninstall.sh -prefix=$(PREFIX) -purge-data

start:
	@$(SCRIPTS_DIR)/service.sh start

stop:
	@$(SCRIPTS_DIR)/service.sh stop

restart:
	@$(SCRIPTS_DIR)/service.sh restart

status:
	@$(SCRIPTS_DIR)/service.sh status

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR) $(DIST_DIR) target/release target/debug
	@echo "Clean complete."

test:
	@echo "Running tests..."
	@cargo test

test-all:
	@echo "Running all tests including benchmarks..."
	@cargo test --all-features

bench:
	@echo "Running benchmarks..."
	@cargo bench

help:
	@echo "Taibai Build Tool - Build, package, and deployment utility"
	@echo ""
	@echo "Usage: make <target> [options]"
	@echo ""
	@echo "Build targets:"
	@echo "  build          Build binary for current platform ($(DEFAULT_OS)/$(DEFAULT_ARCH))"
	@echo "  build-all      Build binaries for all platforms (linux/amd64, linux/arm64, darwin/amd64, darwin/arm64)"
	@echo "  package        Create distribution package for current platform"
	@echo "  package-all    Create distribution packages for all platforms"
	@echo ""
	@echo "Installation targets:"
	@echo "  install        Install taibai to PREFIX (default: /usr/local/taibai)"
	@echo "                 Usage: make install PREFIX=/opt/taibai"
	@echo "  uninstall      Remove taibai installation (keeps data directory)"
	@echo "  uninstall-purge Remove taibai installation including data directory"
	@echo ""
	@echo "Service management (requires installed taibai):"
	@echo "  start          Start taibai service"
	@echo "  stop           Stop taibai service"
	@echo "  restart        Restart taibai service"
	@echo "  status         Check taibai service status"
	@echo ""
	@echo "Development targets:"
	@echo "  test           Run unit tests"
	@echo "  test-all       Run all tests including benchmarks"
	@echo "  bench          Run benchmarks"
	@echo "  clean          Remove build artifacts"
	@echo ""
	@echo "Options:"
	@echo "  VERSION        Set version (default: $(VERSION))"
	@echo "  PREFIX         Set installation prefix (default: /usr/local/taibai)"
	@echo ""
	@echo "Examples:"
	@echo "  make build                          # Build for current platform"
	@echo "  make build-all                      # Build for all platforms"
	@echo "  make package                        # Package for current platform"
	@echo "  sudo make install                   # Install to /usr/local/taibai"
	@echo "  sudo make install PREFIX=/opt/taibai # Install to /opt/taibai"
	@echo "  make start                          # Start the service"