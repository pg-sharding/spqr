# Release

Create GitHub release via https://github.com/pg-sharding/spqr/releases

## Docker Image Publishing

SPQR automatically publishes Docker images to `pgsharding/spqr-router` on Docker Hub https://github.com/pg-sharding/spqr/pull/1681.

By default all public images are nightly. When you want to mark a nightly build as a stable release:

```bash
# 1. Pull the nightly image you want to promote
docker pull pgsharding/spqr-router:nightly-2.7.3-1751-07a36a95

# 2. Login to Docker Hub
docker login -u pgsharding

# 3. Tag it as latest and versioned release
docker tag pgsharding/spqr-router:nightly-2.7.3-1751-07a36a95 pgsharding/spqr-router:latest
docker tag pgsharding/spqr-router:nightly-2.7.3-1751-07a36a95 pgsharding/spqr-router:2.7.3
docker tag pgsharding/spqr-router:nightly-2.7.3-1751-07a36a95 pgsharding/spqr-router:stable

# 4. Push the new tags
docker push pgsharding/spqr-router:latest
docker push pgsharding/spqr-router:2.7.3
docker push pgsharding/spqr-router:stable
```

Why this approach?
- Every build is tested as nightly first
- Promote any nightly to stable when ready
- Simple - no complex release workflows
- Rollback-friendly - keep old nightly images

## Version Format

- **Nightly**: `2.7.3-1751-07a36a95` (tag-commitcount-hash)
- **Release** (after promotion): `2.7.3`, `latest`, `stable`

This matches `spqr-router --version` output.
