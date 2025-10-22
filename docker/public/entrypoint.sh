#!/bin/sh

set -e

# Trap SIGTERM to gracefully shutdown
handle_term() {
    echo "Shutting down spqr-router gracefully..."
    ps aux | grep '[s]pqr-router' | awk '{print $2}' | xargs -r kill -TERM 2>/dev/null || true
    wait -n 2>/dev/null || true
    exit 0
}

trap 'handle_term' TERM INT

# Use provided config paths or defaults
CONFIG_PATH="${ROUTER_CONFIG:-/etc/spqr/config/router.yaml}"
COORD_CONFIG_PATH="${COORDINATOR_CONFIG:-/etc/spqr/config/coordinator.yaml}"
LOG_PATH="${ROUTER_LOG:-/var/log/spqr-router.log}"

# Create log directory if it doesn't exist
mkdir -p "$(dirname "$LOG_PATH")"

# Replace host in config if ROUTER_HOST is set
if [ -f "$CONFIG_PATH" ] && [ -n "$ROUTER_HOST" ]; then
    sed "s/host:.*/host: '${ROUTER_HOST}'/g" -i "$CONFIG_PATH"
fi

# Start spqr-router
exec spqr-router run \
    --config "$CONFIG_PATH" \
    --coordinator-config "$COORD_CONFIG_PATH" \
    >> "$LOG_PATH" 2>&1
