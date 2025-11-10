# Release

Create GitHub release via https://github.com/pg-sharding/spqr/releases

## Docker Image Publishing

SPQR automatically publishes Docker images to `pgsharding/spqr-router` on Docker Hub https://github.com/pg-sharding/spqr/pull/1681.

Promote a nightly build with the helper script:

```bash
./script/promote-nightly.sh nightly-2.8.0-1777-e2479243
```

The script logs in to Docker Hub, retags the nightly image to `latest`, `stable`, and the derived version (e.g. `2.8.0`), then pushes the tags.

Manual flow (if you prefer):

```bash
# 1. Pull the nightly image you want to promote
docker pull pgsharding/spqr-router:nightly-2.8.0-1777-e2479243

# 2. Login to Docker Hub
docker login -u pgsharding

# 3. Tag it as latest and versioned release
docker tag pgsharding/spqr-router:nightly-2.8.0-1777-e2479243 pgsharding/spqr-router:latest
docker tag pgsharding/spqr-router:nightly-2.8.0-1777-e2479243 pgsharding/spqr-router:2.8.0
docker tag pgsharding/spqr-router:nightly-2.8.0-1777-e2479243 pgsharding/spqr-router:stable

# 4. Push the new tags
docker push pgsharding/spqr-router:latest
docker push pgsharding/spqr-router:2.8.0
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
