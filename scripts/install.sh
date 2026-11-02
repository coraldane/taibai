#!/bin/bash
set -e

BINARY_NAME="taibai"
PREFIX="/usr/local/taibai"
INSTALL_SERVICE=true
CONFIG_FILE=""
DATA_DIR=""

print_help() {
    cat << EOF
Taibai Installation Script

Usage: sudo ./scripts/install.sh [options]

Options:
  -prefix=PATH      Installation prefix (default: /usr/local/taibai)
  -config=PATH     Path to existing config file (optional)
  -data-dir=PATH   Data directory path (default: PREFIX/data)
  -no-service      Skip systemd/launchd service installation

Examples:
  sudo ./scripts/install.sh
  sudo ./scripts/install.sh -prefix=/opt/taibai
  sudo ./scripts/install.sh -data-dir=/data/taibai -no-service

Installation directories:
  <prefix>/bin/     - Binary files
  <prefix>/conf/    - Configuration files
  <prefix>/logs/    - Log files
  <prefix>/run/     - PID files
  <prefix>/data/    - Data files
EOF
}

for arg in "$@"; do
    case "$arg" in
        -prefix=*)
            PREFIX="${arg#-prefix=}"
            ;;
        -config=*)
            CONFIG_FILE="${arg#-config=}"
            ;;
        -data-dir=*)
            DATA_DIR="${arg#-data-dir=}"
            ;;
        -no-service)
            INSTALL_SERVICE=false
            ;;
        -h|--help|help)
            print_help
            exit 0
            ;;
    esac
done

if [ "$EUID" -ne 0 ]; then
    echo "Error: This script must be run as root. Use sudo."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

OS="$(uname -s)"
ARCH="$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')"

BIN_DIR="$PREFIX/bin"
CONF_DIR="$PREFIX/conf"
LOG_DIR="$PREFIX/logs"
RUN_DIR="$PREFIX/run"
DATA_DIR="${DATA_DIR:-$PREFIX/data}"

echo "========================================="
echo "Taibai Database Server Installer"
echo "========================================="
echo "Platform: $OS/$ARCH"
echo "Install prefix: $PREFIX"
echo ""

find_binary() {
    if [ -f "$SCRIPT_DIR/../build/${OS}_${ARCH}/$BINARY_NAME" ]; then
        echo "$SCRIPT_DIR/../build/${OS}_${ARCH}/$BINARY_NAME"
        return 0
    fi
    
    if [ -f "$PROJECT_ROOT/target/release/$BINARY_NAME" ]; then
        echo "$PROJECT_ROOT/target/release/$BINARY_NAME"
        return 0
    fi
    
    if [ -f "$SCRIPT_DIR/$BINARY_NAME" ]; then
        echo "$SCRIPT_DIR/$BINARY_NAME"
        return 0
    fi
    
    return 1
}

BINARY_PATH="$(find_binary)"
if [ -z "$BINARY_PATH" ]; then
    echo "Binary not found. Building from source..."
    "$SCRIPT_DIR/build.sh" build
    BINARY_PATH="$(find_binary)"
    if [ -z "$BINARY_PATH" ]; then
        echo "Error: Binary build failed"
        exit 1
    fi
fi

echo "Binary: $BINARY_PATH"
echo ""

echo "Creating directories..."
mkdir -p "$BIN_DIR"
mkdir -p "$CONF_DIR"
mkdir -p "$LOG_DIR"
mkdir -p "$RUN_DIR"
mkdir -p "$DATA_DIR"

echo "Installing binary..."
cp -f "$BINARY_PATH" "$BIN_DIR/taibai"
chmod 755 "$BIN_DIR/taibai"
echo "  -> $BIN_DIR/taibai"

echo "Installing configuration..."
CONFIG_DEST="$CONF_DIR/taibai.yaml"
if [ -n "$CONFIG_FILE" ] && [ -f "$CONFIG_FILE" ]; then
    cp -f "$CONFIG_FILE" "$CONFIG_DEST"
    echo "  -> $CONFIG_DEST (from $CONFIG_FILE)"
elif [ -f "$SCRIPT_DIR/../conf/example.yaml" ]; then
    if [ ! -f "$CONFIG_DEST" ]; then
        cp -f "$SCRIPT_DIR/../conf/example.yaml" "$CONFIG_DEST"
        echo "  -> $CONFIG_DEST"
    else
        echo "  -> Config exists, skipping: $CONFIG_DEST"
    fi
else
    if [ ! -f "$CONFIG_DEST" ]; then
        cat > "$CONFIG_DEST" << EOF
# Taibai Database Server Configuration
server:
  host: "0.0.0.0"
  port: 5151
  max_message_size: 67108864

storage:
  data_dir: "$DATA_DIR"
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
EOF
        echo "  -> $CONFIG_DEST (generated)"
    else
        echo "  -> Config exists, skipping: $CONFIG_DEST"
    fi
fi

if [ -f "$CONFIG_DEST" ]; then
    if grep -q "data_dir:" "$CONFIG_DEST" 2>/dev/null; then
        if [ "$OS" = "Darwin" ]; then
            sed -i '' "s|data_dir:.*|data_dir: \"$DATA_DIR\"|" "$CONFIG_DEST" 2>/dev/null || true
        else
            sed -i "s|data_dir:.*|data_dir: \"$DATA_DIR\"|" "$CONFIG_DEST" 2>/dev/null || true
        fi
    fi
fi

if [ "$INSTALL_SERVICE" = true ]; then
    echo "Installing system service..."
    
    if [ "$OS" = "Linux" ]; then
        cat > /etc/systemd/system/taibai.service << EOF
[Unit]
Description=Taibai Columnar Database Server
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=root
Group=root
# jemalloc 参数：background_thread 让 decay 在进程空闲时也持续推进，
# 否则导入等大分配高峰结束后，已释放的 extent 会长期滞留在 RSS 里。
# 注意：tikv-jemalloc 以 _rjem_ 前缀编译，实际读取的是 _RJEM_MALLOC_CONF
# （配置文件名也是 /etc/_rjem_malloc.conf）。两个都设，避免依赖构建细节。
Environment="MALLOC_CONF=background_thread:true,dirty_decay_ms:500,muzzy_decay_ms:0,narenas:16"
Environment="_RJEM_MALLOC_CONF=background_thread:true,dirty_decay_ms:500,muzzy_decay_ms:0,narenas:16"
# 内存护栏：MemoryHigh 触发软限回收（勿低于实际工作集，否则会持续节流），
# MemoryMax 兜底防止异常增长拖垮整机。
MemoryHigh=32G
MemoryMax=64G
ExecStart=$BIN_DIR/taibai --config $CONF_DIR/taibai.yaml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
LimitNPROC=65536
StandardOutput=append:$LOG_DIR/taibai.log
StandardError=append:$LOG_DIR/taibai.log
PIDFile=$RUN_DIR/taibai.pid
WorkingDirectory=$PREFIX

[Install]
WantedBy=multi-user.target
EOF
        systemctl daemon-reload
        echo "  -> Created systemd service: /etc/systemd/system/taibai.service"
    elif [ "$OS" = "Darwin" ]; then
        cat > /Library/LaunchDaemons/com.taibai.server.plist << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.taibai.server</string>
    <key>ProgramArguments</key>
    <array>
        <string>$BIN_DIR/taibai</string>
        <string>--config</string>
        <string>$CONF_DIR/taibai.yaml</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/stderr.log</string>
    <key>WorkingDirectory</key>
    <string>$PREFIX</string>
    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
    <key>HardResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
</dict>
</plist>
EOF
        chmod 644 /Library/LaunchDaemons/com.taibai.server.plist
        echo "  -> Created launchd service: /Library/LaunchDaemons/com.taibai.server.plist"
    fi
fi

echo ""
echo "========================================="
echo "Installation Complete!"
echo "========================================="
echo ""
echo "Directories:"
echo "  Binary:  $BIN_DIR/taibai"
echo "  Config:  $CONF_DIR/taibai.yaml"
echo "  Data:    $DATA_DIR"
echo "  Logs:    $LOG_DIR"
echo "  PID:     $RUN_DIR"
echo ""

if [ "$INSTALL_SERVICE" = true ]; then
    echo "Service Management:"
    if [ "$OS" = "Linux" ]; then
        echo "  Start:   sudo systemctl start taibai"
        echo "  Stop:    sudo systemctl stop taibai"
        echo "  Status:  sudo systemctl status taibai"
        echo "  Enable:  sudo systemctl enable taibai"
        echo "  Logs:    sudo journalctl -u taibai -f"
    elif [ "$OS" = "Darwin" ]; then
        echo "  Start:   sudo launchctl load /Library/LaunchDaemons/com.taibai.server.plist"
        echo "  Stop:    sudo launchctl unload /Library/LaunchDaemons/com.taibai.server.plist"
        echo "  Status:  sudo launchctl list | grep taibai"
        echo "  Logs:    tail -f $LOG_DIR/stdout.log"
    fi
else
    echo "Manual Start:"
    echo "  $BIN_DIR/taibai --config $CONF_DIR/taibai.yaml"
fi

echo ""
echo "To uninstall: sudo ./scripts/uninstall.sh"