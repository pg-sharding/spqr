#!/bin/bash
# Generates self-signed TLS certificates for the integration test.
set -euo pipefail

CERT_DIR="${1:-$(dirname "$0")/certs}"
mkdir -p "$CERT_DIR"

# --- CA ---
openssl req -new -x509 -days 365 -nodes \
  -subj "/CN=SPQR Test CA" \
  -keyout "$CERT_DIR/ca.key" \
  -out "$CERT_DIR/ca.crt" \
  2>/dev/null

# --- Server certificate (used by PostgreSQL shards) ---
# SAN includes all possible hostnames the shards may use
openssl req -new -nodes \
  -subj "/CN=shard" \
  -keyout "$CERT_DIR/server.key" \
  -out "$CERT_DIR/server.csr" \
  2>/dev/null

cat > "$CERT_DIR/server_ext.cnf" <<EOF
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = shard1
DNS.2 = shard2
DNS.3 = localhost
IP.1 = 127.0.0.1
EOF

openssl x509 -req -days 365 \
  -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
  -in "$CERT_DIR/server.csr" \
  -extfile "$CERT_DIR/server_ext.cnf" -extensions v3_req \
  -out "$CERT_DIR/server.crt" \
  2>/dev/null

# --- Router frontend certificate (for client→router TLS) ---
openssl req -new -nodes \
  -subj "/CN=spqr-router" \
  -keyout "$CERT_DIR/router.key" \
  -out "$CERT_DIR/router.csr" \
  2>/dev/null

cat > "$CERT_DIR/router_ext.cnf" <<EOF
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = spqr-router
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

openssl x509 -req -days 365 \
  -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
  -in "$CERT_DIR/router.csr" \
  -extfile "$CERT_DIR/router_ext.cnf" -extensions v3_req \
  -out "$CERT_DIR/router.crt" \
  2>/dev/null

# Fix permissions (PostgreSQL requires key files to be mode 600)
chmod 600 "$CERT_DIR"/*.key

# Clean up CSR and temp files
rm -f "$CERT_DIR"/*.csr "$CERT_DIR"/*.cnf "$CERT_DIR"/*.srl

echo "Certificates generated in $CERT_DIR"
ls -la "$CERT_DIR"
