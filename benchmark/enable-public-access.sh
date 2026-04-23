#!/usr/bin/env bash
# Periodically enable public network access on the storage account.
# Azure tenant policy may re-disable it; this script runs in a loop
# to ensure it stays enabled during benchmark execution.
#
# Usage:
#   ./benchmark/enable-public-access.sh                    # foreground, every 5 min
#   ./benchmark/enable-public-access.sh --bg               # background daemon, every 5 min
#   ./benchmark/enable-public-access.sh --bg 120           # background, every 2 min
#   ./benchmark/enable-public-access.sh --stop             # stop background daemon
#   ./benchmark/enable-public-access.sh --status           # check if daemon is running
#   ./benchmark/enable-public-access.sh 300 stgelb2        # foreground, custom interval + account

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE="${SCRIPT_DIR}/.enable-public-access.pid"
LOGFILE="${SCRIPT_DIR}/.enable-public-access.log"

stop_daemon() {
    if [[ -f "$PIDFILE" ]]; then
        local pid
        pid=$(cat "$PIDFILE")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            rm -f "$PIDFILE"
            echo "Stopped daemon (PID $pid)"
        else
            rm -f "$PIDFILE"
            echo "Stale PID file removed (process $pid not running)"
        fi
    else
        echo "No daemon running (no PID file)"
    fi
}

check_status() {
    if [[ -f "$PIDFILE" ]]; then
        local pid
        pid=$(cat "$PIDFILE")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Daemon running (PID $pid)"
            echo "Log: $LOGFILE"
            echo "--- last 5 log lines ---"
            tail -5 "$LOGFILE" 2>/dev/null || true
            return 0
        else
            rm -f "$PIDFILE"
            echo "Stale PID file removed (process $pid not running)"
            return 1
        fi
    else
        echo "No daemon running"
        return 1
    fi
}

run_loop() {
    local interval="$1"
    local storage_account="$2"
    local resource_group="$3"
    
    echo "[$(date '+%H:%M:%S')] Monitoring public network access for '${storage_account}' every ${interval}s"
    
    while true; do
        current=$(az storage account show \
            --name "$storage_account" \
            --resource-group "$resource_group" \
        --query "publicNetworkAccess" -o tsv 2>/dev/null || echo "UNKNOWN")
        
        if [[ "$current" != "Enabled" ]]; then
            echo "[$(date '+%H:%M:%S')] Public access is '${current}' — re-enabling..."
            az storage account update \
            --name "$storage_account" \
            --resource-group "$resource_group" \
            --public-network-access Enabled \
            --output none 2>&1 && \
            echo "[$(date '+%H:%M:%S')] Public access re-enabled." || \
            echo "[$(date '+%H:%M:%S')] ERROR: Failed to enable public access."
        else
            echo "[$(date '+%H:%M:%S')] Public access is Enabled — OK"
        fi
        
        sleep "$interval"
    done
}

# Parse command
case "${1:-}" in
    --stop)
        stop_daemon
        exit 0
    ;;
    --status)
        check_status
        exit $?
    ;;
    --bg)
        shift
        INTERVAL="${1:-300}"
        STORAGE_ACCOUNT="${2:-stgelb}"
        RESOURCE_GROUP="${3:-rg-elb-koc}"
        
        # Stop existing daemon if running
        if [[ -f "$PIDFILE" ]]; then
            old_pid=$(cat "$PIDFILE")
            if kill -0 "$old_pid" 2>/dev/null; then
                echo "Stopping existing daemon (PID $old_pid)..."
                kill "$old_pid"
            fi
            rm -f "$PIDFILE"
        fi
        
        # Launch in background
        nohup bash -c "$(declare -f run_loop); run_loop '$INTERVAL' '$STORAGE_ACCOUNT' '$RESOURCE_GROUP'" \
        >> "$LOGFILE" 2>&1 &
        echo $! > "$PIDFILE"
        echo "Daemon started (PID $!) — interval ${INTERVAL}s, account ${STORAGE_ACCOUNT}"
        echo "Log: $LOGFILE"
        echo "Stop: $0 --stop"
        exit 0
    ;;
    *)
        INTERVAL="${1:-300}"
        STORAGE_ACCOUNT="${2:-stgelb}"
        RESOURCE_GROUP="${3:-rg-elb-koc}"
        echo "Press Ctrl+C to stop"
        run_loop "$INTERVAL" "$STORAGE_ACCOUNT" "$RESOURCE_GROUP"
    ;;
esac
