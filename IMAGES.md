# Release

Create GitHub release via <https://github.com/pg-sharding/spqr/releases>

## Docker Image Publishing

SPQR automatically publishes Docker images to `pgsharding/spqr-router` on Docker Hub <https://github.com/pg-sharding/spqr/pull/1681>.

Promote a nightly build using `crane` (preserves multi-platform images: `linux/amd64` + `linux/arm64`):

```bash
# Install crane (one-time)
brew install crane

# Login to Docker Hub
crane auth login docker.io -u pgsharding

# Promote a specific version
crane copy pgsharding/spqr-router:nightly-2.9.11-2479-d79f2128 pgsharding/spqr-router:2.9.12

# For the latest release, also update latest/stable tags
crane copy pgsharding/spqr-router:nightly-2.9.11-2479-d79f2128 pgsharding/spqr-router:latest
crane copy pgsharding/spqr-router:nightly-2.9.11-2479-d79f2128 pgsharding/spqr-router:stable
```

### Finding the nightly tag for a release

Nightly tags use the **commit hash**, not the tag object hash. For annotated git tags, use `^{commit}`:

```bash
# Get the commit hash for a release tag
git rev-parse --short 2.9.12^{commit}
# d79f2128

# Search for the nightly image with that hash
crane ls pgsharding/spqr-router | grep d79f2128
# nightly-2.9.11-2479-d79f2128
```

Note: the nightly tag name contains the **previous** git tag (e.g. `nightly-2.9.11-...` for the `2.9.12` release), because the nightly was built before the new release tag was created.

### Why crane instead of docker tag/push?

`docker pull` + `docker tag` + `docker push` only copies the image for the **current platform** (e.g. only `arm64` on Apple Silicon). `crane copy` copies the entire multi-platform manifest including all architectures.

### Why this approach?

- Every build is tested as nightly first
- Promote any nightly to stable when ready
- Simple - no complex release workflows
- Rollback-friendly - keep old nightly images

## Version Format

- **Nightly**: `2.7.3-1751-07a36a95` (tag-commit_count-hash)
- **Release** (after promotion): `2.7.3`, `latest`, `stable`

This matches `spqr-router --version` output.
