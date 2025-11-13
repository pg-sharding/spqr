# GitHub Copilot Instructions for SPQR

## Project Overview

SPQR (Stateless Postgres Query Router) is a production-ready system for horizontal scaling of PostgreSQL via sharding. The project provides routing and query distribution across multiple PostgreSQL shards.

## Technology Stack

- **Language**: Go 1.25.0
- **Database**: SPQR acts as a proxy between applications and PostgreSQL clusters (shards). It supports any PostgreSQL version. Testing is performed against PostgreSQL versions 13-17.
- **Testing**: Standard Go testing, godog for BDD, Docker Compose for integration tests
- **Build Tool**: Make
- **Linting**: golangci-lint

## Code Style and Conventions

### General Guidelines

- Follow standard Go conventions and idioms
- Use `gofmt` for code formatting (enforced by CI)
- Run `golangci-lint` before committing (enforced by CI)
- Use `goimports` for import management (enforced by CI)
- Keep dependencies tidy with `go mod tidy` (enforced by CI)

### Naming Conventions

- Use descriptive names for variables and functions
- Follow Go naming conventions (camelCase for unexported, PascalCase for exported)
- Interface names should describe behavior (e.g., `Coordinator`, `Interactor`)

### Code Organization

- Separate interfaces from implementations
- Keep business logic in `pkg/` directory
- Main executables in `cmd/` directory (router, coordinator, balancer, etc.)
- Tests alongside code files with `_test.go` suffix
- Mock implementations in `mock/` subdirectories

## Testing

### Test Types

1. **Unit Tests**: Use standard Go testing framework
   - Run with: `make unittest`
   - Place tests in `*_test.go` files
   - Use `testify/assert` for assertions
   - Use `go.uber.org/mock/gomock` for mocking

2. **Regression Tests**: Database-driven tests
   - Run with: `make regress`
   - Require Docker Compose
   - Test against multiple PostgreSQL versions (13-17)

3. **Feature Tests**: BDD-style tests using godog
   - Run with: `make feature_test`
   - Located in `test/feature/`
   - Use Gherkin syntax

4. **Integration Tests**: Driver compatibility tests
   - GORM: `make gorm_regress`
   - JDBC: `make jdbc_regress`
   - XProto: `make xproto_regress`

### Test Guidelines

- Write tests for new functionality
- Maintain existing test coverage
- Use mocks for external dependencies
- Keep tests fast and isolated
- Test edge cases and error conditions

## Build and Development

### Building

```bash
make deps          # Download dependencies
make build         # Build all components: router, coordinator, balancer, mover, worldmock, workloadreplay, spqr-dump, and coordctl
make build_router  # Build router component
make build_coordinator  # Build coordinator component
make build_balancer     # Build balancer component
make build_coorctl      # Build coordctl (coordinator control CLI)
make build_mover        # Build mover component
make build_worldmock    # Build worldmock component
make build_workloadreplay  # Build workloadreplay component
make build_spqrdump     # Build spqr-dump component
```

### Running

```bash
make run                                      # Quick Docker-based example with router, coordinator, and shards
spqr-router run --config router-config.yaml  # Run router with config file
spqr-coordinator run --config coord-config.yaml  # Run coordinator with config file
spqr-balancer run --config balancer-config.yaml  # Run balancer with config file
coorctl                                       # Coordinator control CLI tool

# Example configs are in examples/ directory:
# - router.yaml, coordinator.yaml, balancer.yaml
# - 2shardproxy.yaml, 4shardproxy.yaml for multi-shard setups
```

### Linting

```bash
make fmt           # Format code
make fmtcheck      # Check formatting
make lint          # Run golangci-lint
```

## Key Architectural Patterns

### Router

- Handles client connections and query routing
- Stateless design for horizontal scalability
- Routes queries to appropriate shards based on sharding rules

### Coordinator

- Manages metadata and cluster configuration
- Coordinates shard topology changes
- Provides consistent configuration across routers

### Balancer

- Distributes connections across router instances
- Monitors router health and availability
- Note: Balancer is now part of the coordinator but temporarily supported as a separate executable

### Key Packages

- `pkg/models/kr`: Key range models for sharding
- `pkg/clientinteractor`: Client interaction interfaces
- `pkg/meta`: Metadata management
- `router/qrouter`: Query routing logic
- `coordinator/coordinator`: Coordinator implementation

## Common Tasks

### Adding a New Feature

1. Implement the feature with minimal changes
2. Add unit tests alongside the code
3. Add integration/feature tests if needed
4. Update documentation if API changes
5. Run linters and tests before committing
6. Ensure backward compatibility

### Fixing Bugs

1. Write a test that reproduces the bug
2. Fix the issue with minimal changes
3. Verify the fix with tests
4. Check for similar issues in related code

### Modifying Metadata Schema

SPQR does not connect to databases as a typical application. It acts as a proxy between applications and PostgreSQL shards.

For sharding metadata management:
1. SPQR has its own metadata database called QDB (Query Database)
2. QDB implementations: `memqdb.go` (in-memory) or `etcdqdb.go` (ETCD-backed)
3. The ETCD cluster stores metadata that coordinators and routers use
4. When modifying metadata schema:
   - Update QDB interface and implementations
   - Add migration logic if needed
   - Update tests to reflect schema changes
   - Test with both memqdb and etcdqdb implementations

## Dependencies

### Adding Dependencies

1. Use `go get` to add dependency
2. Run `go mod tidy` to clean up
3. Run `go mod vendor` to vendor dependencies
4. Commit both `go.mod` and `go.sum` changes

### Updating Dependencies

1. Update version in `go.mod`
2. Run `go mod tidy` and `go mod vendor`
3. Test thoroughly after updates
4. Check for breaking changes

## CI/CD

### GitHub Actions Workflows

- **build.yaml**: Builds all components
- **tests.yaml**: Runs all test suites (unit, regress, feature)
- **linters.yaml**: Runs code quality checks
- **codeql.yml**: Security scanning

### Before Pushing

1. Format code: `make fmt`
2. Run linters: `make lint`
3. Run unit tests: `make unittest`
4. Build: `make build`

All checks must pass in CI before merging.

## Docker

### Images

- `spqr-base-image`: Base SPQR image
- `spqr-shard-image`: PostgreSQL shard image
- Multiple PostgreSQL versions supported (13-17)

### Docker Compose

- Development environment: `docker-compose.yaml`
- Test environments in `test/*/docker-compose.yaml`

## Best Practices

1. **Make minimal changes**: Only modify what's necessary
2. **Test thoroughly**: Run relevant tests before committing
3. **Follow existing patterns**: Match the style of surrounding code
4. **Document changes**: Update docs for user-facing changes
5. **Think about compatibility**: Consider PostgreSQL version compatibility
6. **Use interfaces**: Prefer interfaces for testability and flexibility
7. **Handle errors**: Always check and handle errors appropriately
8. **Consider concurrency**: Use proper synchronization for shared state
9. **Optimize carefully**: Profile before optimizing
10. **Security first**: Consider security implications of changes

## Resources

- [Documentation](https://docs.pg-sharding.tech)
- [Telegram Chat](https://t.me/+jMGhyjwicpI3ZWQy)
- [GitHub Issues](https://github.com/pg-sharding/spqr/issues)
