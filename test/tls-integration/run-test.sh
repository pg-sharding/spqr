#!/bin/bash
# Verifies that shard TLS configuration is persisted through the
# coordinator and used by the router when manage_shards_by_coordinator=true.
#
# Usage:
#   ./run-test.sh          # run test
#   ./run-test.sh cleanup  # just clean up
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()   { echo -e "${GREEN}[TEST]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail()  { echo -e "${RED}[FAIL]${NC} $*"; }

cleanup() {
    log "Cleaning up..."
    docker compose down -v --remove-orphans 2>/dev/null || true
    rm -rf certs/
}

if [ "${1:-}" = "cleanup" ]; then
    cleanup
    exit 0
fi

trap cleanup EXIT

log "Generating TLS certificates..."
rm -rf certs/
bash generate-certs.sh "$SCRIPT_DIR/certs"
chmod 600 certs/server.key certs/router.key

log "Building SPQR from local source..."
docker compose build spqr-build

log "Starting services..."

docker compose up -d etcd shard1 shard2
docker compose up -d --wait etcd shard1 shard2

log "Starting coordinator..."
docker compose up -d spqr-coordinator

log "Waiting for coordinator..."
for i in $(seq 1 60); do
    if docker compose exec -T spqr-coordinator bash -c 'echo > /dev/tcp/localhost/7003' 2>/dev/null; then
        log "Coordinator is ready"
        break
    fi
    if [ "$i" -eq 60 ]; then
        fail "Coordinator did not become ready in time"
        docker compose logs spqr-coordinator
        exit 1
    fi
    sleep 2
done

sleep 5

log "Starting router..."
docker compose up -d spqr-router

log "Waiting for router..."
for i in $(seq 1 60); do
    if docker compose exec -T spqr-router bash -c 'echo > /dev/tcp/localhost/6432' 2>/dev/null; then
        log "Router is ready"
        break
    fi
    if [ "$i" -eq 60 ]; then
        fail "Router did not become ready in time"
        docker compose logs spqr-router
        exit 1
    fi
    sleep 2
done

sleep 5

log "Verifying TLS config in etcd..."

ETCD_DATA=$(docker compose exec -T etcd etcdctl get --prefix /shards/ 2>&1)
echo "$ETCD_DATA"

if echo "$ETCD_DATA" | grep -q '"sslmode":"require"'; then
    log "TLS config (sslmode=require) is persisted in etcd"
else
    fail "TLS config NOT found in etcd"
    exit 1
fi

log "Setting up distribution and key ranges..."
docker compose exec -T shard1 psql \
  "postgresql://spqr-console@spqr-router:7432/spqr-console?sslmode=require" \
  -f /init.sql

log "Restarting router to pick up new metadata..."
docker compose restart spqr-router
sleep 10

for i in $(seq 1 30); do
    if docker compose exec -T spqr-router bash -c 'echo > /dev/tcp/localhost/6432' 2>/dev/null; then
        break
    fi
    sleep 2
done
sleep 3

log "Creating tables and testing queries..."
docker compose exec -T shard1 psql -U user1 -d db1 \
  -c "CREATE TABLE IF NOT EXISTS test_tls (id INT PRIMARY KEY, data TEXT);"
docker compose exec -T shard2 psql -U user1 -d db1 \
  -c "CREATE TABLE IF NOT EXISTS test_tls (id INT PRIMARY KEY, data TEXT);"

docker compose exec -T shard1 psql \
  "postgresql://user1@spqr-router:6432/db1?sslmode=require" \
  -c "INSERT INTO test_tls (id, data) VALUES (1, 'hello from shard 1');" || {
    fail "INSERT to shard1 through router failed"
    docker compose logs --tail=30 spqr-router
    exit 1
}

docker compose exec -T shard1 psql \
  "postgresql://user1@spqr-router:6432/db1?sslmode=require" \
  -c "INSERT INTO test_tls (id, data) VALUES (20, 'hello from shard 2');" || {
    fail "INSERT to shard2 through router failed"
    docker compose logs --tail=30 spqr-router
    exit 1
}

RESULT1=$(docker compose exec -T shard1 psql \
  "postgresql://user1@spqr-router:6432/db1?sslmode=require" \
  -tA -c "SELECT data FROM test_tls WHERE id = 1;")

RESULT2=$(docker compose exec -T shard1 psql \
  "postgresql://user1@spqr-router:6432/db1?sslmode=require" \
  -tA -c "SELECT data FROM test_tls WHERE id = 20;")

if [ "$RESULT1" = "hello from shard 1" ]; then
    log "Query to shard1 returned correct data: '$RESULT1'"
else
    fail "Query to shard1 returned unexpected: '$RESULT1'"
    exit 1
fi

if [ "$RESULT2" = "hello from shard 2" ]; then
    log "Query to shard2 returned correct data: '$RESULT2'"
else
    fail "Query to shard2 returned unexpected: '$RESULT2'"
    exit 1
fi

log "Verifying TLS on router-to-shard connections..."

SHARD1_SSL=$(docker compose exec -T shard1 psql -U user1 -d db1 -tA \
  -c "SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL;")

SHARD2_SSL=$(docker compose exec -T shard2 psql -U user1 -d db1 -tA \
  -c "SELECT count(*) FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) WHERE ssl = true AND client_addr IS NOT NULL;")

if [ "$SHARD1_SSL" -gt 0 ] 2>/dev/null; then
    log "Shard1 has $SHARD1_SSL active TLS connection(s) from router"
else
    fail "No TLS connections found on shard1"
    exit 1
fi

if [ "$SHARD2_SSL" -gt 0 ] 2>/dev/null; then
    log "Shard2 has $SHARD2_SSL active TLS connection(s) from router"
else
    fail "No TLS connections found on shard2"
    exit 1
fi

docker compose exec -T shard1 psql -U user1 -d db1 \
  -c "SELECT pid, ssl, version, cipher FROM pg_stat_ssl WHERE ssl = true AND pid IN (SELECT pid FROM pg_stat_activity WHERE client_addr IS NOT NULL);"

log "TLS integration test passed"
