#!/bin/bash
set -e

PREFIX="/usr/local/taibai"
PURGE_DATA=false
KEEP_CONFIG=false

print_help() {
    cat << EOF
Taibai Uninstallation Script

Usage: sudo ./scripts/uninstall.sh [options]

Options:
  -prefix=PATH      Installation prefix (default: /usr/local/taibai)
  -purge-data       Also remove data directory (default: keep data)
  -keep-config      Keep configuration files (default: remove)

Examples:
  sudo ./scripts/uninstall.sh                    # Remove installation, keep data
  sudo ./scripts/uninstall.sh -purge-data        # Remove everything including data
  sudo ./scripts/uninstall.sh -prefix=/opt/taibai # Uninstall from custom location

Notes:
  - Data directory is preserved by default for safety
  - Use -purge-data to completely remove all files
EOF
}

for arg in "$@"; do
    case "$arg" in
        -prefix=*)
            PREFIX="${arg#-prefix=}"
            ;;
        -purge-data)
            PURGE_DATA=true
            ;;
        -keep-config)
            KEEP_CONFIG=true
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

OS="$(uname -s)"

echo "========================================="
echo "Taibai Database Server Uninstaller"
echo "========================================="
echo "Install prefix: $PREFIX"
echo "Purge data: $PURGE_DATA"
echo ""

stop_service() {
    echo "Stopping service..."
    
    if [ "$OS" = "Linux" ]; then
        systemctl stop taibai 2>/dev/null || true
        echo "  -> Stopped systemd service"
    elif [ "$OS" = "Darwin" ]; then
        launchctl unload /Library/LaunchDaemons/com.taibai.server.plist 2>/dev/null || true
        echo "  -> Stopped launchd service"
    fi
}

remove_service() {
    echo "Removing system service..."
    
    if [ "$OS" = "Linux" ]; then
        systemctl disable taibai 2>/dev/null || true
        rm -f /etc/systemd/system/taibai.service
        systemctl daemon-reload
        echo "  -> Removed systemd service"
    elif [ "$OS" = "Darwin" ]; then
        rm -f /Library/LaunchDaemons/com.taibai.server.plist
        echo "  -> Removed launchd service"
    fi
}

remove_files() {
    echo "Removing files..."
    
    BIN_DIR="$PREFIX/bin"
    CONF_DIR="$PREFIX/conf"
    LOG_DIR="$PREFIX/log"
    RUN_DIR="$PREFIX/run"
    DATA_DIR="$PREFIX/data"
    
    if [ "$PURGE_DATA" = true ]; then
        echo "  -> Removing entire installation: $PREFIX"
        rm -rf "$PREFIX"
    else
        if [ "$KEEP_CONFIG" = false ]; then
            rm -rf "$CONF_DIR"
            echo "  -> Removed: $CONF_DIR"
        else
            echo "  -> Keeping config: $CONF_DIR"
        fi
        
        rm -rf "$BIN_DIR"
        echo "  -> Removed: $BIN_DIR"
        
        rm -rf "$LOG_DIR"
        echo "  -> Removed: $LOG_DIR"
        
        rm -rf "$RUN_DIR"
        echo "  -> Removed: $RUN_DIR"
        
        echo "  -> Keeping data: $DATA_DIR"
        echo "     (Use -purge-data to remove data as well)"
    fi
}

stop_service
remove_service
remove_files

echo ""
echo "========================================="
echo "Uninstallation Complete"
echo "========================================="

if [ "$PURGE_DATA" = false ]; then
    echo ""
    echo "Note: Data directory was preserved at:"
    echo "  $PREFIX/data"
    echo ""
    echo "To completely remove data, run:"
    echo "  rm -rf $PREFIX/data"
fi

echo ""