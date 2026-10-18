#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BINARY_NAME="taibai"
BUILD_DIR="$PROJECT_ROOT/build"
DIST_DIR="$PROJECT_ROOT/dist"

VERSION="${VERSION:-$(grep '^version =' "$PROJECT_ROOT/Cargo.toml" | head -1 | sed 's/.*"\([^"]*\)".*/\1/')}"
BUILD_TIME="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
GIT_COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"

DEFAULT_OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
DEFAULT_ARCH="$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')"

PLATFORMS=(
    "linux:amd64"
    "linux:arm64"
    "darwin:amd64"
    "darwin:arm64"
)

print_help() {
    cat << EOF
Taibai Build Tool - Cross-platform build and packaging utility

Usage: ./scripts/build.sh <command> [options]

Commands:
  build     Build the binary for current or specified platform
            Options: -os=<os> -arch=<arch> -all
            Examples:
              ./scripts/build.sh build
              ./scripts/build.sh build -os=linux -arch=amd64
              ./scripts/build.sh build -all

  package   Create distribution package (tar.gz)
            Options: -os=<os> -arch=<arch> -all
            Examples:
              ./scripts/build.sh package
              ./scripts/build.sh package -all

  clean     Remove build artifacts

  help      Show this help message

Supported platforms:
  - linux/amd64, linux/arm64
  - darwin/amd64, darwin/arm64

Environment variables:
  VERSION   Override version (default: $VERSION)
EOF
}

parse_args() {
    TARGET_OS="$DEFAULT_OS"
    TARGET_ARCH="$DEFAULT_ARCH"
    BUILD_ALL=false

    for arg in "$@"; do
        case "$arg" in
            -os=*)
                TARGET_OS="${arg#-os=}"
                ;;
            -arch=*)
                TARGET_ARCH="${arg#-arch=}"
                ;;
            -all)
                BUILD_ALL=true
                ;;
        esac
    done
}

resolve_target_dir() {
    # cargo 的 target 目录会被环境变量或 cargo 配置（含 ~/.cargo/config.toml 的
    # 全局共享目录）挪走，产物别死认 $PROJECT_ROOT/target
    if [ -n "$CARGO_TARGET_DIR" ]; then
        echo "$CARGO_TARGET_DIR"
        return
    fi
    local cfg td
    for cfg in "$PROJECT_ROOT/.cargo/config.toml" "$HOME/.cargo/config.toml"; do
        if [ -f "$cfg" ]; then
            td="$(sed -n 's/^[[:space:]]*target-dir[[:space:]]*=[[:space:]]*["'\'']\{0,1\}\([^"'\''#]*\).*/\1/p' "$cfg" | head -1 | tr -d '[:space:]')"
            if [ -n "$td" ]; then
                echo "$td"
                return
            fi
        fi
    done
    echo "$PROJECT_ROOT/target"
}

get_rust_target() {
    local os="$1"
    local arch="$2"
    
    case "$os:$arch" in
        linux:amd64)   echo "x86_64-unknown-linux-gnu" ;;
        linux:arm64)   echo "aarch64-unknown-linux-gnu" ;;
        darwin:amd64)  echo "x86_64-apple-darwin" ;;
        darwin:arm64)  echo "aarch64-apple-darwin" ;;
        *)             echo "unknown" ;;
    esac
}

build_binary() {
    local target_os="$1"
    local target_arch="$2"
    local target_triple="$(get_rust_target "$target_os" "$target_arch")"
    
    if [ "$target_triple" = "unknown" ]; then
        echo "Error: Unknown platform $target_os/$target_arch"
        exit 1
    fi

    echo "Building $BINARY_NAME for $target_os/$target_arch ($target_triple)..."
    
    local out_dir="$BUILD_DIR/${target_os}_${target_arch}"
    mkdir -p "$out_dir"
    
    export TAIBAI_VERSION="$VERSION"
    export TAIBAI_BUILD_TIME="$BUILD_TIME"
    export TAIBAI_GIT_COMMIT="$GIT_COMMIT"
    
    if [ "$target_os" = "$DEFAULT_OS" ] && [ "$target_arch" = "$DEFAULT_ARCH" ]; then
        cargo build --release
        cp -f "$(resolve_target_dir)/release/$BINARY_NAME" "$out_dir/$BINARY_NAME"
    else
        rustup target add "$target_triple" 2>/dev/null || true
        cargo build --release --target "$target_triple" || {
            echo "Error: Build failed for $target_os/$target_arch"
            echo "Hint: You may need to install the target with: rustup target add $target_triple"
            exit 1
        }
        cp -f "$(resolve_target_dir)/$target_triple/release/$BINARY_NAME" "$out_dir/$BINARY_NAME"
    fi
    
    chmod +x "$out_dir/$BINARY_NAME"
    
    local size="$(ls -lh "$out_dir/$BINARY_NAME" | awk '{print $5}')"
    echo "Built: $out_dir/$BINARY_NAME ($size)"
}

build_all() {
    for platform in "${PLATFORMS[@]}"; do
        local os="${platform%%:*}"
        local arch="${platform#*:}"
        build_binary "$os" "$arch"
    done
}

create_package() {
    local target_os="$1"
    local target_arch="$2"
    
    local src_dir="$BUILD_DIR/${target_os}_${target_arch}"
    local pkg_name="${BINARY_NAME}-${VERSION}-${target_os}-${target_arch}"
    
    if [ ! -f "$src_dir/$BINARY_NAME" ]; then
        echo "Error: Binary not found at $src_dir/$BINARY_NAME"
        echo "Run './scripts/build.sh build -os=$target_os -arch=$target_arch' first"
        exit 1
    fi
    
    echo "Packaging for $target_os/$target_arch..."
    
    mkdir -p "$DIST_DIR"
    
    local tmp_dir="$(mktemp -d)"
    local pkg_dir="$tmp_dir/$pkg_name"
    mkdir -p "$pkg_dir/conf"
    
    cp -f "$src_dir/$BINARY_NAME" "$pkg_dir/"
    cp -f "$PROJECT_ROOT/conf/example.yaml" "$pkg_dir/conf/taibai.yaml"
    
    if [ -f "$PROJECT_ROOT/README.md" ]; then
        cp -f "$PROJECT_ROOT/README.md" "$pkg_dir/"
    fi
    
    cp -f "$SCRIPT_DIR/install.sh" "$pkg_dir/"
    chmod +x "$pkg_dir/install.sh"
    
    cp -f "$SCRIPT_DIR/uninstall.sh" "$pkg_dir/"
    chmod +x "$pkg_dir/uninstall.sh"
    
    tar -czf "$DIST_DIR/${pkg_name}.tar.gz" -C "$tmp_dir" "$pkg_name"
    
    rm -rf "$tmp_dir"
    
    local size="$(ls -lh "$DIST_DIR/${pkg_name}.tar.gz" | awk '{print $5}')"
    echo "Created: $DIST_DIR/${pkg_name}.tar.gz ($size)"
}

package_all() {
    for platform in "${PLATFORMS[@]}"; do
        local os="${platform%%:*}"
        local arch="${platform#*:}"
        create_package "$os" "$arch"
    done
}

clean() {
    echo "Cleaning build artifacts..."
    rm -rf "$BUILD_DIR" "$DIST_DIR"
    cargo clean 2>/dev/null || true
    echo "Clean complete."
}

main() {
    if [ $# -lt 1 ]; then
        print_help
        exit 0
    fi
    
    cd "$PROJECT_ROOT"
    
    command="$1"
    shift
    
    case "$command" in
        build)
            parse_args "$@"
            if [ "$BUILD_ALL" = true ]; then
                build_all
            else
                build_binary "$TARGET_OS" "$TARGET_ARCH"
            fi
            ;;
        package)
            parse_args "$@"
            if [ "$BUILD_ALL" = true ]; then
                build_all
                package_all
            else
                build_binary "$TARGET_OS" "$TARGET_ARCH"
                create_package "$TARGET_OS" "$TARGET_ARCH"
            fi
            ;;
        clean)
            clean
            ;;
        help|--help|-h)
            print_help
            ;;
        *)
            echo "Unknown command: $command"
            print_help
            exit 1
            ;;
    esac
}

main "$@"