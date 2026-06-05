#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PREFIX="/usr/local/monax"
BIN_DIR="$PREFIX/bin"
CONF_DIR="$PREFIX/conf"
LOG_DIR="$PREFIX/log"
RUN_DIR="$PREFIX/run"

DEV_BIN="$PROJECT_ROOT/target/release/monax"
DEV_CONF="$PROJECT_ROOT/conf/example.yaml"
DEV_PID="$PROJECT_ROOT/monax.pid"

OS="$(uname -s)"

is_installed() {
    [ -f "$BIN_DIR/monax" ] || [ -f "${BIN_DIR}.exe" ]
}

is_development() {
    [ -f "$DEV_BIN" ]
}

is_service_installed() {
    if [ "$OS" = "Linux" ]; then
        [ -f /etc/systemd/system/monax.service ]
    elif [ "$OS" = "Darwin" ]; then
        [ -f /Library/LaunchDaemons/com.monax.server.plist ]
    else
        false
    fi
}

start_installed() {
    echo "Starting monax service..."
    
    if [ "$OS" = "Linux" ]; then
        systemctl start monax
        echo "Service started via systemd"
    elif [ "$OS" = "Darwin" ]; then
        launchctl load /Library/LaunchDaemons/com.monax.server.plist
        echo "Service started via launchd"
    fi
    
    sleep 1
    status
}

start_development() {
    echo "Starting monax in development mode..."
    
    if [ ! -f "$DEV_BIN" ]; then
        echo "Binary not found at $DEV_BIN"
        echo "Building..."
        cd "$PROJECT_ROOT"
        cargo build --release
    fi
    
    if [ -f "$DEV_PID" ]; then
        local old_pid="$(cat "$DEV_PID")"
        if kill -0 "$old_pid" 2>/dev/null; then
            echo "Stopping existing process (PID: $old_pid)..."
            kill "$old_pid" 2>/dev/null || true
            sleep 1
        fi
        rm -f "$DEV_PID"
    fi
    
    cd "$PROJECT_ROOT"
    nohup "$DEV_BIN" --config "$DEV_CONF" > "$LOG_DIR/monax.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$DEV_PID"
    
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
        echo "Started monax (PID: $pid)"
        echo "Logs: $LOG_DIR/monax.log"
    else
        echo "Failed to start monax"
        exit 1
    fi
}

stop_installed() {
    echo "Stopping monax service..."
    
    if [ "$OS" = "Linux" ]; then
        systemctl stop monax || true
        echo "Service stopped via systemd"
    elif [ "$OS" = "Darwin" ]; then
        launchctl unload /Library/LaunchDaemons/com.monax.server.plist || true
        echo "Service stopped via launchd"
    fi
}

stop_development() {
    echo "Stopping monax (development mode)..."
    
    if [ -f "$DEV_PID" ]; then
        local pid="$(cat "$DEV_PID")"
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping process (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            
            local count=0
            while kill -0 "$pid" 2>/dev/null && [ $count -lt 10 ]; do
                sleep 1
                count=$((count + 1))
            done
            
            if kill -0 "$pid" 2>/dev/null; then
                echo "Force killing process..."
                kill -9 "$pid" 2>/dev/null || true
            fi
            
            echo "Process stopped"
        else
            echo "Process not running ( stale PID file)"
        fi
        rm -f "$DEV_PID"
    else
        echo "No PID file found"
        
        local running_pid="$(pgrep -f 'monax.*--config' 2>/dev/null | head -1)"
        if [ -n "$running_pid" ]; then
            echo "Found running process (PID: $running_pid)"
            kill "$running_pid" 2>/dev/null || true
            echo "Process stopped"
        fi
    fi
}

status_installed() {
    echo "Checking monax service status..."
    
    if [ "$OS" = "Linux" ]; then
        systemctl status monax --no-pager || true
    elif [ "$OS" = "Darwin" ]; then
        local status="$(launchctl list | grep com.monax.server || echo 'Not running')"
        echo "$status"
        
        if echo "$status" | grep -q "com.monax.server"; then
            echo "Service is running"
        else
            echo "Service is not running"
        fi
    fi
}

status_development() {
    echo "Checking monax status (development mode)..."
    
    if [ -f "$DEV_PID" ]; then
        local pid="$(cat "$DEV_PID")"
        if kill -0 "$pid" 2>/dev/null; then
            echo "Running (PID: $pid)"
            
            local port="$(grep -E '^\s*port:' "$DEV_CONF" | sed 's/.*:\s*//' | tr -d '\"')"
            if command -v nc &> /dev/null && nc -z localhost "$port" 2>/dev/null; then
                echo "Listening on port $port"
            fi
            
            echo "Logs: $LOG_DIR/monax.log"
        else
            echo "Not running (stale PID: $pid)"
            rm -f "$DEV_PID"
        fi
    else
        local running_pid="$(pgrep -f 'monax.*--config' 2>/dev/null | head -1)"
        if [ -n "$running_pid" ]; then
            echo "Running (PID: $running_pid - no PID file)"
        else
            echo "Not running"
        fi
    fi
}

print_help() {
    cat << EOF
Monax Service Management

Usage: ./scripts/service.sh <command>

Commands:
  start     Start the monax service
  stop      Stop the monax service  
  restart   Restart the monax service
  status    Check service status

Modes:
  - If monax is installed (binary at $BIN_DIR/monax), uses systemd/launchd
  - If in development mode (binary at $DEV_BIN), runs directly

Environment:
  Development mode is used when:
    - Binary exists at: $DEV_BIN
    - No installed binary found

  Installed mode is used when:
    - Binary exists at: $BIN_DIR/monax
EOF
}

main() {
    if [ $# -lt 1 ]; then
        print_help
        exit 0
    fi
    
    mkdir -p "$LOG_DIR"
    
    command="$1"
    
    case "$command" in
        start)
            if is_service_installed && [ "$EUID" -eq 0 ]; then
                start_installed
            elif is_installed && [ "$EUID" -eq 0 ]; then
                start_installed
            elif is_development; then
                start_development
            else
                echo "Error: No monax binary found"
                echo "Either install monax or build it with: cargo build --release"
                exit 1
            fi
            ;;
        stop)
            if is_service_installed && [ "$EUID" -eq 0 ]; then
                stop_installed
            elif is_installed && [ "$EUID" -eq 0 ]; then
                stop_installed
            elif [ -f "$DEV_PID" ]; then
                stop_development
            else
                echo "No service to stop"
            fi
            ;;
        restart)
            $0 stop
            sleep 2
            $0 start
            ;;
        status)
            if is_service_installed; then
                status_installed
            elif [ -f "$DEV_PID" ] || pgrep -f 'monax' >/dev/null 2>&1; then
                status_development
            else
                echo "No monax process found"
            fi
            ;;
        help|-h|--help)
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