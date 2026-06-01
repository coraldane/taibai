# Monax

A columnar database server wire-compatible with taibai-go, implemented in Rust.

## Features

- Columnar storage optimized for OLAP queries
- Wire protocol compatible with taibai-go
- High-performance batch import
- TLS encryption support
- Compression support (zstd)
- Cross-platform deployment (Linux, macOS)

## Quick Start

### Build

```bash
# Build for current platform
make build

# Or using scripts directly
./scripts/build.sh build

# Build for all platforms (linux/amd64, linux/arm64, darwin/amd64, darwin/arm64)
make build-all
```

### Package

```bash
# Create distribution package for current platform
make package

# Package for all platforms
make package-all
```

### Install

```bash
# Install to default location (/usr/local/monax)
sudo make install

# Install to custom location
sudo make install PREFIX=/opt/monax
```

### Run

```bash
# Start server
make start

# Or run directly
./target/release/monax --config conf/example.yaml

# Check server status
make status

# Stop server
make stop
```

## Project Structure

```
monax/
├── Makefile              # Build system entry point
├── Cargo.toml            # Rust package manifest
├── build.rs              # Build script (version embedding)
├── conf/
│   └── example.yaml      # Example configuration
├── scripts/
│   ├── build.sh          # Cross-platform build script
│   ├── install.sh        # System installation script
│   ├── uninstall.sh      # Uninstallation script
│   └── service.sh        # Service management script
├── src/
│   ├── main.rs           # Main entry point
│   ├── lib.rs            # Library root
│   ├── version.rs        # Version info module
│   ├── common/           # Common utilities
│   ├── net/              # Network layer
│   ├── protocol/         # Wire protocol
│   ├── sql/              # SQL execution
│   ├── storage/          # Storage engine
│   └── types/            # Type definitions
├── build/                # Build outputs
└── dist/                 # Distribution packages
```

## Build Commands

| Command | Description |
|---------|-------------|
| `make build` | Build for current platform |
| `make build-all` | Build for all platforms |
| `make package` | Create package for current platform |
| `make package-all` | Create packages for all platforms |
| `make install` | Install to system |
| `make uninstall` | Remove installation (keeps data) |
| `make uninstall-purge` | Remove everything including data |
| `make start` | Start service |
| `make stop` | Stop service |
| `make restart` | Restart service |
| `make status` | Check service status |
| `make test` | Run tests |
| `make clean` | Clean build artifacts |
| `make help` | Show all commands |

## Installation Directory Structure

After installation, the following directories are created:

```
/usr/local/monax/
├── bin/                  # Binary files
│   └── monax
├── conf/                 # Configuration files
│   └── monax.yaml
├── data/                 # Database files
├── log/                  # Log files
│   └── monax.log
└── run/                  # PID files
    └── monax.pid
```

## Service Management

### Linux (systemd)

```bash
sudo systemctl start monax    # Start service
sudo systemctl stop monax     # Stop service
sudo systemctl status monax   # Check status
sudo systemctl enable monax   # Enable auto-start
sudo journalctl -u monax -f   # View logs
```

### macOS (launchd)

```bash
sudo launchctl load /Library/LaunchDaemons/com.monax.server.plist    # Start
sudo launchctl unload /Library/LaunchDaemons/com.monax.server.plist  # Stop
sudo launchctl list | grep monax                                      # Status
tail -f /usr/local/monax/log/stdout.log                               # Logs
```

## Configuration

Configuration file: `conf/monax.yaml` (or `conf/example.yaml` as template)

```yaml
server:
  host: "0.0.0.0"
  port: 5151
  max_message_size: 67108864  # 64MB

storage:
  data_dir: "/usr/local/monax/data"
  default_database: "main"
  flush_threshold_rows: 65536
  min_segment_seal_size_mb: 500

buffer:
  batch_size: 4096

compression:
  enabled: true

tls:
  enabled: false
  cert_path: ""
  key_path: ""

log:
  level: "info"
```

## Cross-Compilation

For cross-compilation, ensure you have the required Rust targets:

```bash
# Add targets
rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-unknown-linux-gnu
rustup target add x86_64-apple-darwin
rustup target add aarch64-apple-darwin

# Build for specific platform
./scripts/build.sh build -os=linux -arch=amd64
./scripts/build.sh build -os=darwin -arch=arm64
```

## Version Information

Version information is embedded at build time:

```bash
# View version
./target/release/monax --version

# Set custom version during build
VERSION=1.0.0 make build
```

Built version includes:
- Version number (from Cargo.toml or VERSION env)
- Git commit hash
- Build timestamp
- Target platform
- Build profile

## Development

```bash
# Run in development mode
cargo run --release -- --config conf/example.yaml

# Run tests
cargo test

# Run benchmarks
cargo bench

# Check code
cargo clippy
cargo fmt --check
```

## License

MIT or Apache-2.0