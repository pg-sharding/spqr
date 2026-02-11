# Multi-stage Dockerfile for production SPQR router image
# This creates a minimal image with only the router binary and runtime dependencies

# Stage 1: Build the router binary
FROM golang:1.26-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make gcc musl-dev

WORKDIR /build

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source code
COPY . .

# Build the router binary with version info
ARG GIT_REVISION=unknown
ARG SPQR_VERSION=unknown
RUN go build -ldflags "-X github.com/pg-sharding/spqr/pkg.GitRevision=${GIT_REVISION} -X github.com/pg-sharding/spqr/pkg.SpqrVersion=${SPQR_VERSION} -w -s" -o spqr-router ./cmd/router

# Stage 2: Create minimal runtime image
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    postgresql-client \
    && rm -rf /var/cache/apk/*

# Create non-root user for running the router
RUN addgroup -g 1000 spqr && \
    adduser -D -u 1000 -G spqr spqr

# Create directories for configuration and logs
RUN mkdir -p /etc/spqr /var/log/spqr /var/run/spqr && \
    chown -R spqr:spqr /etc/spqr /var/log/spqr /var/run/spqr

# Copy the router binary from builder
COPY --from=builder /build/spqr-router /usr/local/bin/spqr-router
RUN chmod +x /usr/local/bin/spqr-router

# Copy default configuration
COPY examples/router.yaml /etc/spqr/router.yaml.example

WORKDIR /var/run/spqr

# Switch to non-root user
USER spqr

# Expose default ports
# 6432 - router port
# 7432 - admin console port
# 7000 - gRPC API port
EXPOSE 6432 7432 7000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD psql -h localhost -p 7432 -U demo -d demo -c "SHOW shards;" || exit 1

# Default command
ENTRYPOINT ["/usr/local/bin/spqr-router"]
CMD ["run", "--config", "/etc/spqr/router.yaml"]
